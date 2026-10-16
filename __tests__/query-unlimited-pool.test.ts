import { afterEach, describe, expect, it, vi } from 'vitest';
import { EventEmitter } from 'events';
import { resolveLegacyQueryPolicy, resolveQueryPolicy, type QueryExecutionMode } from '../src/mcp/query-policy';
import { createQueryExecutionContext } from '../src/mcp/query-context';
import { QueryPool, type PoolWorker } from '../src/mcp/query-pool';

class ControlledWorker extends EventEmitter implements PoolWorker {
  calls: any[] = [];
  exits = 0;
  postMessage(message: unknown): void { this.calls.push(message); }
  terminate(): Promise<number> { this.exits++; return Promise.resolve(0); }
  ready(): void { this.emit('message', { type: 'ready', ok: true }); }
  complete(): void {
    const call = this.calls.at(-1);
    this.emit('message', { type: 'result', id: call.id, result: { content: [{ type: 'text', text: 'completed graph query' }] } });
  }
}
const execution = (poolSize = 'unlimited', busyTimeoutMs?: string) => createQueryExecutionContext(
  resolveLegacyQueryPolicy({ poolSize, busyTimeoutMs }, 'direct', 8), { readOnly: false, writer: true, synchronization: true });
afterEach(() => vi.useRealTimers());

describe('independent unlimited pool selection', () => {
  it.each(['direct', 'writable-fallback', 'read-only-fallback', 'shared-daemon'] as QueryExecutionMode[])
  ('selects unbounded pooled capacity and idle retirement while retaining finite waiting in %s', mode => {
    expect(resolveLegacyQueryPolicy({ poolSize: ' unlimited ' }, mode, 8, { poolEnabled: true })).toMatchObject({
      preset: 'default', source: 'legacy', classification: 'custom', concurrency: { kind: 'unlimited' },
      deadline: { kind: 'finite', milliseconds: 45000 }, retention: 'retire-idle',
    });
  });
});

describe('unused query worker retirement', () => {
  it('retires unused peers while a healthy timer-free query continues on its own occupied worker', async () => {
    vi.useFakeTimers();
    const workers: ControlledWorker[] = [];
    const pool = new QueryPool({ root: null, execution: execution('unlimited', 'unlimited'), createWorker: () => {
      const worker = new ControlledWorker(); workers.push(worker); return worker;
    } });
    const busy = pool.run('codegraph_search', { query: 'long healthy query' });
    try {
      workers[0].ready();
      const fast = pool.run('codegraph_search', { query: 'short query' });
      workers[1].ready(); workers[1].complete(); await fast;
      await vi.advanceTimersByTimeAsync(30000);
      expect(workers[1].exits).toBe(1);
      expect(workers[0].exits).toBe(0);
      expect(pool.liveWorkers).toBe(1);
      expect(pool.activity.executing).toBe(1);
      workers[0].complete(); await busy;
    } finally { workers[0].complete(); await busy; await pool.destroy(); }
  });
  it('retires ready unused workers after the declared 30-second grace and recreates on demand', async () => {
    vi.useFakeTimers();
    const workers: ControlledWorker[] = [];
    const pool = new QueryPool({ root: null, execution: execution(), createWorker: () => {
      const worker = new ControlledWorker(); workers.push(worker); return worker;
    } });
    try {
      workers[0].ready();
      const first = pool.run('codegraph_search', { query: 'originalSymbol' });
      workers[0].complete();
      expect((await first).content[0].text).toBe('completed graph query');
      await vi.advanceTimersByTimeAsync(29999);
      expect(pool.liveWorkers).toBe(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(pool.liveWorkers).toBe(0);
      expect(workers[0].exits).toBe(1);
      const second = pool.run('codegraph_search', { query: 'originalSymbol' });
      expect(workers).toHaveLength(2);
      expect(workers[1].calls).toHaveLength(0);
      workers[1].ready(); workers[1].complete();
      expect((await second).content[0].text).toBe('completed graph query');
    } finally { await pool.destroy(); }
  });
  it.each(['2', '0'])('does not make timer-only unlimited waiting retire ordinary finite/disabled selection %s', async poolSize => {
    vi.useFakeTimers();
    const policy = resolveLegacyQueryPolicy({ poolSize, busyTimeoutMs: 'unlimited' }, 'direct', 8);
    expect(policy.retention).toBe('ordinary');
    if (poolSize === '0') { expect(policy.concurrency).toEqual({ kind: 'disabled' }); return; }
    const worker = new ControlledWorker();
    const pool = new QueryPool({ root: null, execution: execution(poolSize, 'unlimited'), createWorker: () => worker });
    try {
      worker.ready();
      await vi.advanceTimersByTimeAsync(45000);
      expect(pool.liveWorkers).toBe(1);
      expect(worker.exits).toBe(0);
    } finally { await pool.destroy(); }
  });
  it('preserves actual execution after its finite caller expires and starts a new grace only after completion', async () => {
    vi.useFakeTimers();
    const worker = new ControlledWorker();
    const pool = new QueryPool({ root: null, execution: execution('unlimited', '1000'), createWorker: () => worker });
    try {
      worker.ready();
      const result = pool.run('codegraph_search', {});
      await vi.advanceTimersByTimeAsync(31000);
      expect((await result).content[0].text).toContain('after admission');
      expect(pool.activity.executing).toBe(1);
      expect(worker.exits).toBe(0);
      worker.complete();
      await vi.advanceTimersByTimeAsync(29999);
      expect(pool.liveWorkers).toBe(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(pool.liveWorkers).toBe(0);
    } finally { worker.complete(); await pool.destroy(); }
  });
  it('queues concurrent demand during retirement without reusing or oversubscribing its finite reserved slot', async () => {
    vi.useFakeTimers();
    const workers: ControlledWorker[] = [];
    let exit!: (code: number) => void;
    const policy = resolveQueryPolicy({ mode: 'unlimited', poolSize: '1' }, 'direct', 8);
    expect(policy.classification).toBe('custom');
    const pool = new QueryPool({ root: null, execution: createQueryExecutionContext(policy,
      { readOnly: false, writer: true, synchronization: true }), createWorker: () => {
      const worker = new ControlledWorker(); workers.push(worker);
      if (workers.length === 1) worker.terminate = () => new Promise(resolve => { exit = resolve; });
      return worker;
    } });
    try {
      workers[0].ready();
      await vi.advanceTimersByTimeAsync(30000);
      expect(pool.activity.retiring).toBe(1);
      const first = pool.run('codegraph_search', {});
      const second = pool.run('codegraph_search', {});
      expect(workers).toHaveLength(1);
      expect(workers[0].calls).toHaveLength(0);
      exit(0); await vi.advanceTimersByTimeAsync(0);
      expect(workers).toHaveLength(2);
      workers[1].ready(); workers[1].complete();
      expect((await first).content[0].text).toBe('completed graph query');
      workers[1].complete();
      expect((await second).content[0].text).toBe('completed graph query');
      expect(workers[1].calls.map(call => call.id)).toEqual([1, 2]);
    } finally { exit?.(0); await pool.destroy(); }
  });
});
