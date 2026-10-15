import { afterEach, describe, expect, it, vi } from 'vitest';
import { resolveLegacyQueryPolicy } from '../src/mcp/query-policy';
import { QueryPool, type PoolWorker } from '../src/mcp/query-pool';
import type { ToolResult } from '../src/mcp/tools';

const ok: ToolResult = { content: [{ type: 'text', text: 'real completion' }] };
class ControlledWorker implements PoolWorker {
  calls: any[] = [];
  private listeners = new Map<string, (value: any) => void>();
  on(event: string, listener: (value: any) => void): void { this.listeners.set(event, listener); }
  emit(event: string, value: any): void { this.listeners.get(event)?.(value); }
  postMessage(value: any): void { this.calls.push(value); }
  terminate(): Promise<number> { return Promise.resolve(0); }
  complete(id: number): void { this.emit('message', { type: 'result', id, result: ok }); }
}
afterEach(() => { vi.useRealTimers(); vi.unstubAllEnvs(); });

describe('independent pooled waiting override', () => {
  it('removes the busy deadline without changing mode capacity or retention', () => {
    for (const [mode, maximum] of [['direct', 2], ['writable-fallback', 2], ['shared-daemon', 7]] as const) {
      const policy = resolveLegacyQueryPolicy({ busyTimeoutMs: ' unlimited ' }, mode, 8);
      expect(policy).toMatchObject({ concurrency: { kind: 'finite', maximum }, deadline: { kind: 'absent' },
        retention: 'ordinary', classification: 'custom', preset: 'default' });
    }
    expect(resolveLegacyQueryPolicy({ busyTimeoutMs: 'unlimited' }, 'read-only-fallback', 8).deadline).toEqual({ kind: 'inapplicable' });
    expect(resolveLegacyQueryPolicy({ poolSize: '0', busyTimeoutMs: 'unlimited' }, 'direct', 8).deadline).toEqual({ kind: 'inapplicable' });
  });
  it('preserves safe finite timer boundaries and cannot overflow an invalid legacy value into immediate expiry', () => {
    for (const raw of ['1000', '2147483647']) {
      expect(resolveLegacyQueryPolicy({ busyTimeoutMs: raw }, 'direct', 8).deadline).toEqual({ kind: 'finite', milliseconds: Number(raw) });
    }
    for (const raw of ['2147483648', '9007199254740991', 'Infinity']) {
      expect(resolveLegacyQueryPolicy({ busyTimeoutMs: raw }, 'direct', 8).deadline).toEqual({ kind: 'finite', milliseconds: 45000 });
    }
  });
  it('describes actual total admission time without claiming execution was interrupted', async () => {
    vi.useFakeTimers(); vi.setSystemTime(0);
    const worker = new ControlledWorker();
    const pool = new QueryPool({ root: null, size: 1, softTimeoutMs: 1000, createWorker: () => worker });
    worker.emit('message', { type: 'ready', ok: true });
    try {
      const pending = pool.run('codegraph_search', {});
      // A delayed event loop must describe elapsed admission time, not just the
      // configured timeout. The executing worker still owns its occupied slot.
      vi.setSystemTime(200);
      await vi.advanceTimersByTimeAsync(1000);
      const result = await pending;
      expect(result.isError).toBeFalsy();
      expect(result.content[0].text).toContain('1.200s after admission (including queueing and execution)');
      expect(result.content[0].text).toContain('retry this exact call');
      expect(result.content[0].text).not.toMatch(/healthy|interrupted|cancelled|queue is full/i);
      expect(pool.activity.executing).toBe(1);
      worker.complete(worker.calls[0].id);
      expect(pool.activity.executing).toBe(0);
    } finally { await pool.destroy(); }
  });
  it('creates no busy timer and returns a completion after the former backstop', async () => {
    vi.useFakeTimers(); vi.setSystemTime(0);
    vi.stubEnv('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', 'unlimited');
    const worker = new ControlledWorker();
    const pool = new QueryPool({ root: null, size: 1, createWorker: () => worker });
    worker.emit('message', { type: 'ready', ok: true });
    try {
      let answered = false;
      const pending = pool.run('codegraph_search', {}).then(result => { answered = true; return result; });
      expect(vi.getTimerCount()).toBe(0);
      await vi.advanceTimersByTimeAsync(47000);
      expect(answered).toBe(false);
      expect(pool.activity.executing).toBe(1);
      worker.complete(worker.calls[0].id);
      expect(await pending).toEqual(ok);
      expect(pool.execution.policy.retention).toBe('ordinary');
    } finally { await pool.destroy(); }
  });
  it.each([1000, 2147483647])('waits until the supported finite boundary %i without overflow', async milliseconds => {
    vi.useFakeTimers(); vi.setSystemTime(0);
    const worker = new ControlledWorker();
    const pool = new QueryPool({ root: null, size: 1, softTimeoutMs: milliseconds, createWorker: () => worker });
    worker.emit('message', { type: 'ready', ok: true });
    try {
      let answered = false;
      const pending = pool.run('codegraph_search', {}).then(result => { answered = true; return result; });
      await vi.advanceTimersByTimeAsync(milliseconds - 1);
      expect(answered).toBe(false);
      await vi.advanceTimersByTimeAsync(1);
      expect((await pending).isError).toBeFalsy();
      expect(pool.activity.executing).toBe(1);
    } finally { await pool.destroy(); }
  });
  it('counts queue and execution together without resetting the admission timer on dispatch', async () => {
    vi.useFakeTimers(); vi.setSystemTime(0);
    const worker = new ControlledWorker();
    const pool = new QueryPool({ root: null, size: 1, softTimeoutMs: 1000, createWorker: () => worker });
    worker.emit('message', { type: 'ready', ok: true });
    try {
      const first = pool.run('codegraph_search', {});
      await vi.advanceTimersByTimeAsync(500);
      const queued = pool.run('codegraph_search', {});
      await vi.advanceTimersByTimeAsync(400);
      worker.complete(worker.calls[0].id);
      expect(await first).toEqual(ok);
      expect(worker.calls).toHaveLength(2);
      await vi.advanceTimersByTimeAsync(600);
      expect((await queued).content[0].text).toContain('1.000s after admission');
      expect(pool.activity.executing).toBe(1);
    } finally { await pool.destroy(); }
  });
  it('never executes an expired queued caller and ignores repeated late results while the next call owns the slot', async () => {
    vi.useFakeTimers(); vi.setSystemTime(0);
    const worker = new ControlledWorker();
    const pool = new QueryPool({ root: null, size: 1, softTimeoutMs: 1000, createWorker: () => worker });
    worker.emit('message', { type: 'ready', ok: true });
    try {
      const first = pool.run('codegraph_search', {});
      const expired = pool.run('codegraph_search', {});
      await vi.advanceTimersByTimeAsync(1000);
      expect((await first).isError).toBeFalsy();
      expect((await expired).isError).toBeFalsy();
      expect(pool.activity).toMatchObject({ queued: 0, executing: 1 });
      expect(worker.calls).toHaveLength(1);
      worker.complete(1);
      const next = pool.run('codegraph_search', {});
      for (let i = 0; i < 3; i++) worker.complete(1);
      expect(worker.calls.map(call => call.id)).toEqual([1, 3]);
      expect(pool.activity.executing).toBe(1);
      worker.complete(3);
      expect(await next).toEqual(ok);
      expect(pool.activity).toMatchObject({ queued: 0, executing: 0 });
    } finally { await pool.destroy(); }
  });
  it('keeps the original deadline across recovery and does not retry execution whose caller expired', async () => {
    vi.useFakeTimers(); vi.setSystemTime(0);
    const workers: ControlledWorker[] = [];
    const pool = new QueryPool({ root: null, size: 1, softTimeoutMs: 1000, createWorker: () => {
      const worker = new ControlledWorker(); workers.push(worker); return worker;
    } });
    workers[0].emit('message', { type: 'ready', ok: true });
    try {
      const pending = pool.run('codegraph_search', {});
      await vi.advanceTimersByTimeAsync(600);
      workers[0].emit('error', new Error('controlled worker failure'));
      workers[1].emit('message', { type: 'ready', ok: true });
      expect(workers[1].calls[0].id).toBe(workers[0].calls[0].id);
      await vi.advanceTimersByTimeAsync(400);
      expect((await pending).content[0].text).toContain('1.000s after admission');
      workers[1].emit('error', new Error('controlled late worker failure'));
      workers[2].emit('message', { type: 'ready', ok: true });
      expect(workers[2].calls).toEqual([]);
      expect(pool.activity).toMatchObject({ queued: 0, executing: 0 });
      workers[1].complete(1);
      workers[1].emit('exit', 1);
      expect(pool.activity).toMatchObject({ queued: 0, executing: 0 });
    } finally { await pool.destroy(); }
  });
  it.each(['completion-first', 'expiry-first'] as const)('settles once when %s wins the deadline race', async order => {
    vi.useFakeTimers(); vi.setSystemTime(0);
    const worker = new ControlledWorker();
    const pool = new QueryPool({ root: null, size: 1, softTimeoutMs: 1000, createWorker: () => worker });
    worker.emit('message', { type: 'ready', ok: true });
    try {
      let answers = 0;
      const pending = pool.run('codegraph_search', {}).then(result => { answers++; return result; });
      if (order === 'completion-first') worker.complete(1);
      await vi.advanceTimersByTimeAsync(1000);
      worker.complete(1); worker.complete(1);
      const result = await pending;
      expect(answers).toBe(1);
      expect(result.content[0].text).toContain(order === 'completion-first' ? 'real completion' : 'after admission');
      expect(pool.activity).toMatchObject({ queued: 0, executing: 0 });
    } finally { await pool.destroy(); }
  });
  it('settles permanent capacity failure without any busy timer', async () => {
    vi.stubEnv('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', 'unlimited');
    const pool = new QueryPool({ root: null, size: 1, createWorker: () => { throw new Error('controlled unavailable capacity'); } });
    try {
      const result = await pool.run('codegraph_search', {});
      expect(result.isError).toBe(true);
      expect(result.content[0].text).toContain('capacity is unavailable');
      expect(pool.state).toBe('failed');
    } finally { await pool.destroy(); }
  });
});
