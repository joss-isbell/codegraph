import { describe, expect, it } from 'vitest';
import { QueryPool } from '../src/mcp/query-pool';
import { createQueryExecutionContext } from '../src/mcp/query-context';
import { resolveQueryPolicy } from '../src/mcp/query-policy';
import { Worker } from 'worker_threads';
import * as path from 'path';
import * as fs from 'fs';
import * as os from 'os';
import { CodeGraph } from '../src';
import { ToolHandler } from '../src/mcp/tools';
import { execFile } from 'child_process';
import { promisify } from 'util';
import { pathToFileURL } from 'url';
import { Client } from './query-unlimited-mcp-fixture';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { removeSpawnLog, settleLosingCandidates } from './daemon-candidates';

const execution = () => createQueryExecutionContext(resolveQueryPolicy({ poolSize: '2', busyTimeoutMs: 'unlimited' }, 'direct', 8),
  { readOnly: false, writer: true, synchronization: true });
const record = (value: unknown) => {
  if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'capacity-facts.jsonl'), JSON.stringify(value) + '\n');
};
function realWorker(mode?: string, facts?: unknown[]): Worker {
  const worker = new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: { mode } });
  const threadId = worker.threadId;
  worker.on('message', message => facts?.push({ threadId, ...message }));
  worker.on('error', error => facts?.push({ threadId, event: 'error', message: error.message }));
  worker.on('exit', code => facts?.push({ threadId, event: 'exit', code }));
  return worker;
}

describe('pooled capacity without a busy timer', () => {
  it('settles all demand when worker creation permanently fails and refuses later admissions', async () => {
    let attempts = 0;
    const pool = new QueryPool({ root: null, execution: execution(), createWorker: () => { attempts++; throw new Error('resource unavailable'); } });
    try {
      const results = await Promise.all(Array.from({ length: 4 }, () => pool.run('codegraph_search', {})));
      expect(results.every(result => result.isError && /capacity|unavailable|failed/i.test(result.content[0].text))).toBe(true);
      expect(pool.healthy).toBe(false);
      expect(attempts).toBeLessThanOrEqual(12);
      const finalAttempts = attempts;
      expect((await pool.run('codegraph_search', {})).isError).toBe(true);
      expect(attempts).toBe(finalAttempts);
    } finally { await pool.destroy(); }
  }, 1000);
  it('bounds never-ready starts and settles the queued request independently of a busy timer', async () => {
    let created = 0;
    const pool = new QueryPool({ root: null, execution: execution(), startSettleMs: 100,
      createWorker: () => { created++; return new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: { mode: 'never-ready' } }); } });
    try {
      const result = await pool.run('codegraph_search', {});
      expect(result.isError).toBe(true);
      expect(created).toBeLessThanOrEqual(12);
      expect(pool.healthy).toBe(false);
      expect((await pool.run('codegraph_search', {})).isError).toBe(true);
    } finally { await pool.destroy(); }
  }, 3000);
  it('settles a real dispatch clone failure while unrelated queued work still completes', async () => {
    const pool = new QueryPool({ root: null, execution: execution(),
      createWorker: () => new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: {} }) });
    try {
      const failed = pool.run('codegraph_search', { uncloneable: () => undefined });
      const good = pool.run('codegraph_search', { query: 'valid' });
      expect((await failed).isError).toBe(true);
      expect((await good).content[0].text).toMatch(/^worker:\d+:job:2$/);
      expect(pool.healthy).toBe(true);
    } finally { await pool.destroy(); }
  }, 3000);
  it.each(['exit-start', 'error-start', 'open-failure', 'exit-call', 'error-call'])
  ('recovers from real %s loss without disturbing unrelated demand', async mode => {
    let created = 0;
    const workers: Worker[] = [];
    const facts: unknown[] = [];
    const pool = new QueryPool({ root: null, execution: execution(), createWorker: () => {
      const worker = realWorker(created++ === 0 ? mode : undefined, facts);
      workers.push(worker);
      return worker;
    } });
    try {
      const results = await Promise.all(Array.from({ length: 4 }, () => pool.run('codegraph_search', {})));
      expect(results.every(result => !result.isError && /^worker:\d+:job:\d+$/.test(result.content[0].text))).toBe(true);
      expect(pool.healthy).toBe(true);
      expect(new Set(results.map(result => result.content[0].text.split(':job:')[1])).size).toBe(4);
    } finally { await pool.destroy(); }
    expect(workers.every(worker => worker.threadId === -1)).toBe(true);
    record({ scenario: 'recovered', mode, created, facts, finalActivity: pool.activity });
  }, 10000);
  it.each(['exit-start', 'error-start', 'open-failure', 'exit-call', 'error-call'])
  ('settles all admissions after real %s recovery exhausts and cannot resurrect', async mode => {
    const workers: Worker[] = [];
    const facts: unknown[] = [];
    const pool = new QueryPool({ root: null, execution: execution(), createWorker: () => {
      const worker = realWorker(mode, facts);
      workers.push(worker);
      return worker;
    } });
    let settlements = 0;
    try {
      const results = await Promise.all(Array.from({ length: 5 }, () => pool.run('codegraph_search', {}).then(result => { settlements++; return result; })));
      expect(results.every(result => result.isError)).toBe(true);
      // Poison running jobs can exhaust their per-job retry before the pool's
      // global recovery budget. Further demand must eventually trip that budget.
      while (pool.healthy) expect((await pool.run('codegraph_search', {})).isError).toBe(true);
      expect(pool.state).toBe('failed');
      expect(pool.ready).toBe(false);
      expect(workers.length).toBeLessThanOrEqual(13);
      const count = workers.length;
      expect((await pool.run('codegraph_search', {})).isError).toBe(true);
      await new Promise(resolve => setTimeout(resolve, 50));
      expect(workers.length).toBe(count);
      expect(settlements).toBe(5);
    } finally { await Promise.all([pool.destroy(), pool.destroy()]); }
    expect(pool.state).toBe('destroyed');
    expect(pool.activity).toEqual({ queued: 0, executing: 0, starting: 0, retiring: 0 });
    expect(workers.every(worker => worker.threadId === -1)).toBe(true);
    expect((await pool.run('codegraph_search', {})).isError).toBe(true);
    record({ scenario: 'exhausted', mode, created: workers.length, settlements, facts, finalActivity: pool.activity });
  }, 15000);
  it('ignores real duplicate handshakes and stale result ids without releasing another job slot', async () => {
    const pool = new QueryPool({ root: null, size: 1, createWorker: () => new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: { mode: 'duplicate' } }) });
    try {
      const results = await Promise.all(Array.from({ length: 12 }, () => pool.run('codegraph_search', {})));
      expect(results.map(result => result.content[0].text.split(':job:')[1])).toEqual(Array.from({ length: 12 }, (_, index) => String(index + 1)));
      expect(pool.liveWorkers).toBe(1);
    } finally { await pool.destroy(); }
  }, 10000);
});

describe('dispatcher keeps configured pooling authoritative', () => {
  it.each(['initial', 'retry'])('keeps valid explicit reads on real workers after %s default open fails', async mode => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-failed-default-')));
    const output = path.join(process.env.VALIDATION_OUT ?? root, `failed-default-${mode}.json`);
    try {
      await promisify(execFile)(process.execPath, ['--liftoff-only', path.join(__dirname, 'fixtures/query-failed-default.cjs'), root, mode, output],
        { timeout: 15000, maxBuffer: 1024 * 1024, env: { ...process.env, CODEGRAPH_QUERY_POOL_SIZE: '2' } });
      const facts = JSON.parse(fs.readFileSync(output, 'utf8'));
      expect(facts.recognized).toBe(true);
      expect(facts.defaultLoaded).toBe(false);
      expect(facts.result.isError).toBeFalsy();
      expect(facts.result.content[0].text).toContain('survivingExplicitSymbol');
      expect(facts.state.actual.dispatch).toBe('worker');
      expect(facts.events.some((event: any) => event.event === 'result')).toBe(true);
      expect(facts.events.filter((event: any) => event.event === 'spawn').length).toBe(facts.events.filter((event: any) => event.event === 'exit').length);
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  }, 20000);
  it.each(['default', 'explicit'])('queues the cold %s project read and keeps status on the owner', async route => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-cold-dispatch-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    const symbols = ['alphaColdCapacitySymbol', 'betaColdCapacitySymbol'];
    const observations = path.join(fixture, 'workers.jsonl');
    const events = (): any[] => {
      if (!fs.existsSync(observations)) return [];
      if (fs.statSync(observations).size > 1024 * 1024) throw new Error('Cold dispatch observation overflow');
      return fs.readFileSync(observations, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
    };
    let client: Client | undefined, primary: unknown;
    const cleanup: Array<{ phase: string; failure: string | null }> = [];
    try {
      for (const [index, root] of roots.entries()) {
        fs.mkdirSync(root);
        fs.writeFileSync(path.join(root, 'app.ts'), `export function ${symbols[index]}() { return ${index + 41}; }\n`);
        const graph = await CodeGraph.init(root);
        try { await graph.indexAll(); } finally { graph.close(); }
      }
      client = new Client(roots[0], 'direct', observations, path.join(fixture, 'unused-release'), 'finite-no-deadline', { noWatch: true });
      expect((await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {},
        clientInfo: { name: 'cold-capacity', version: '1' }, rootUri: pathToFileURL(roots[0]).href })).serverInfo.name).toBe('codegraph');
      const selected = route === 'explicit' ? 1 : 0;
      const args = route === 'explicit' ? { projectPath: roots[selected] } : {};
      const status = async () => parseQueryDiagnostics(await client!.request('tools/call', { name: 'codegraph_status', arguments: args }));
      const cold = await status();
      expect(cold.project).toEqual({ route, root: roots[selected], index: fs.realpathSync(path.join(roots[selected], '.codegraph/codegraph.db')) });
      expect(cold.effective).toMatchObject({ executionMode: 'direct', concurrency: { kind: 'finite', maximum: 2 }, deadline: { kind: 'absent' } });
      expect(cold.ownership).toMatchObject({ pid: client.child.pid, resourceScope: 'local-engine', capability: 'read-write' });
      expect(cold.lifecycle).toMatchObject({ state: 'cold', dispatch: 'worker', workers: 0, queued: 0, executing: 0 });
      expect(events().filter(event => event.event === 'spawn' || event.event === 'call-start')).toEqual([]);

      // No default warm-up: the selected route is this host's first worker read.
      const result = await client.query(symbols[selected], route === 'explicit' ? roots[selected] : undefined);
      expect(result.isError).toBeFalsy();
      expect(result.content[0].text).toContain(symbols[selected]);
      expect(result.content[0].text).toContain('app.ts:1');
      expect(result.content[0].text).not.toContain(symbols[1 - selected]);
      const active = await status();
      expect(active.project).toEqual(cold.project); expect(active.ownership).toEqual(cold.ownership);
      expect(active.effective).toEqual(cold.effective);
      expect(active.lifecycle).toMatchObject({ state: 'active', dispatch: 'worker', ready: true, workers: 1, queued: 0, executing: 0 });
      const observed = events(), starts = observed.filter(event => event.event === 'call-start');
      expect(starts).toEqual([expect.objectContaining({ query: symbols[selected], pid: client.child.pid,
        context: expect.objectContaining({ project: cold.project }) })]);
      expect(starts[0].threadId).toBeGreaterThan(0);
      expect(observed.filter(event => event.event === 'spawn')).toEqual([expect.objectContaining({ threadId: starts[0].threadId })]);
      expect(observed.filter(event => event.event === 'result')).toHaveLength(1);
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (phase: string, action: () => Promise<unknown>) => {
        try { await action(); cleanup.push({ phase, failure: null }); }
        catch (error) { cleanup.push({ phase, failure: String(error).slice(0, 2048) }); }
      };
      if (client) await attempt('native-client-close', () => client!.close());
      for (const root of roots) await attempt('losing-candidates-' + path.basename(root), () => settleLosingCandidates(root, () => process.pid));
      await attempt('all-workers-exited', async () => {
        const observed = events();
        expect(observed.filter(event => event.event === 'exit')).toHaveLength(observed.filter(event => event.event === 'spawn').length);
      });
      record({ scenario: 'cold-public-dispatch', route, roots, events: events(), capture: client?.capture(),
        replies: client?.replies, primary: primary instanceof Error ? primary.stack : String(primary ?? ''), cleanup });
      if (cleanup.every(item => item.failure === null)) {
        for (const root of roots) removeSpawnLog(root);
        fs.rmSync(fixture, { recursive: true, force: true });
      } else if (!primary) throw new Error('Cold dispatch cleanup incomplete: ' + JSON.stringify(cleanup));
    }
  }, 60000);
  it.each([true, false])('does not escape to owner execution with zero live workers, recoverable=%s', async recoverable => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-empty-dispatch-')));
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function coldSymbol() { return 42; }\n');
    const cg = await CodeGraph.init(root);
    await cg.indexAll();
    const context = execution();
    const handler = new ToolHandler(cg, context);
    let created = 0;
    const pool = new QueryPool({ root: null, execution: context, createWorker: () => {
      if (++created === 1 || !recoverable) throw new Error('resource unavailable');
      return new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: {} });
    } });
    handler.setQueryPool(pool);
    try {
      expect(pool.liveWorkers).toBe(0);
      const result = await handler.execute('codegraph_search', { query: 'coldSymbol' });
      if (recoverable) expect(result.content[0].text).toMatch(/^worker:\d+:job:1$/);
      else {
        expect(result.isError).toBe(true);
        expect(result.content[0].text).toContain('capacity is unavailable');
        expect((await handler.execute('codegraph_search', { query: 'coldSymbol' })).isError).toBe(true);
      }
    } finally { await pool.destroy(); await handler.closeAll(); cg.close(); fs.rmSync(root, { recursive: true, force: true }); }
  }, 10000);
});

describe('exact finite worker capacity', () => {
  it.each([3, 20])('runs real jobs to the exact demand ceiling %i without exceeding it', async maximum => {
    const gate = new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT);
    const state = new Int32Array(gate);
    let created = 0;
    let starting = 0;
    let peakStarting = 0;
    const pool = new QueryPool({ root: null, size: maximum, createWorker: () => {
      created++;
      peakStarting = Math.max(peakStarting, ++starting);
      const worker = new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: { gate, readyDelay: 15 } });
      worker.on('message', message => { if (message.type === 'ready') starting--; });
      return worker;
    } });
    const demand = maximum + 4;
    const calls = Array.from({ length: demand }, () => pool.run('codegraph_search', {}));
    try {
      const until = Date.now() + 5000;
      while (Atomics.load(state, 0) < maximum && Date.now() < until) await new Promise(resolve => setTimeout(resolve, 10));
      expect(pool.size).toBe(maximum);
      expect(Atomics.load(state, 1)).toBe(maximum);
      Atomics.store(state, 3, 1);
      Atomics.notify(state, 3);
      const results = await Promise.all(calls);
      expect(results.every(result => !result.isError && /^worker:\d+:job:\d+$/.test(result.content[0].text))).toBe(true);
      expect(Atomics.load(state, 2)).toBe(demand);
      expect(Atomics.load(state, 4)).toBe(demand);
      expect(Atomics.load(state, 1)).toBe(maximum);
      expect(created).toBe(maximum);
      expect(peakStarting).toBeLessThanOrEqual(2);
      record({ scenario: 'finite-capacity', maximum, demand, created, peakStarting, actualStarts: state[2], actualCompletions: state[4], peakOverlap: state[1] });
    } finally { Atomics.store(state, 3, 1); Atomics.notify(state, 3); await Promise.all(calls); await pool.destroy(); }
  }, 15000);
  it('treats the largest supported finite maximum as a demand ceiling, without eager allocation', async () => {
    let created = 0;
    const pool = new QueryPool({ root: null, size: Number.MAX_SAFE_INTEGER, createWorker: () => {
      created++;
      return new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: {} });
    } });
    try {
      expect(pool.size).toBe(Number.MAX_SAFE_INTEGER);
      expect((await pool.run('codegraph_search', {})).isError).toBeFalsy();
      expect(created).toBe(1);
    } finally { await pool.destroy(); }
  }, 10000);
});

describe('finite admission deadline retains real execution ownership', () => {
  it('expires queued and running callers once, keeps the running slot, and never executes expired queued work', async () => {
    const gate = new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT);
    const state = new Int32Array(gate);
    const pool = new QueryPool({ root: null, size: 1, softTimeoutMs: 180,
      createWorker: () => new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: { gate } }) });
    let answered = 0;
    try {
      const first = pool.run('codegraph_search', {}).then(result => { answered++; return result; });
      const until = Date.now() + 2000;
      while (Atomics.load(state, 0) === 0 && Date.now() < until) await new Promise(resolve => setTimeout(resolve, 5));
      expect(Atomics.load(state, 0)).toBe(1);
      const second = pool.run('codegraph_search', {}).then(result => { answered++; return result; });
      for (const result of await Promise.all([first, second])) {
        expect(result.isError).toBeFalsy();
        expect(result.content[0].text).toContain('including queueing and execution');
      }
      expect(pool.activity).toMatchObject({ queued: 0, executing: 1 });
      expect(state[2]).toBe(1);
      Atomics.store(state, 3, 1);
      Atomics.notify(state, 3);
      const third = await pool.run('codegraph_search', {});
      expect(third.content[0].text).toMatch(/^worker:\d+:job:3$/);
      expect(state[2]).toBe(2);
      expect(state[4]).toBe(2);
      expect(answered).toBe(2);
    } finally { Atomics.store(state, 3, 1); Atomics.notify(state, 3); await pool.destroy(); }
  }, 5000);
});
