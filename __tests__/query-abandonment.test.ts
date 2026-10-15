import { describe, expect, it } from 'vitest';
import { Worker } from 'worker_threads';
import * as path from 'path';
import * as fs from 'fs';
import * as os from 'os';
import * as net from 'net';
import { CodeGraph } from '../src';
import { MCPSession } from '../src/mcp/session';
import { SocketTransport } from '../src/mcp/transport';
import { QueryPool } from '../src/mcp/query-pool';
import { createQueryExecutionContext, createQueryRequestContext } from '../src/mcp/query-context';
import { resolveQueryPolicy } from '../src/mcp/query-policy';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean, timeout = 5000): Promise<void> {
  const end = Date.now() + timeout;
  while (!check()) { if (Date.now() > end) throw new Error('Worker did not reach the declared state'); await sleep(10); }
}
const execution = () => createQueryExecutionContext(resolveQueryPolicy({ poolSize: '1', busyTimeoutMs: 'unlimited' }, 'shared-daemon', 8),
  { readOnly: false, writer: true, synchronization: true });
const record = (facts: unknown) => {
  if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'abandonment-facts.jsonl'), JSON.stringify(facts) + '\n');
};

describe('abandoned pooled requests', () => {
  it('gives intentionally stopped synchronous work its provisional drain then awaits its owned worker exit', async () => {
    const gate = new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT);
    const state = new Int32Array(gate);
    let exited = false;
    const pool = new QueryPool({ root: null, execution: execution(), createWorker: () => {
      const worker = new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: { gate } });
      worker.on('exit', () => { exited = true; }); return worker;
    } });
    const call = pool.run('codegraph_search', {});
    await until(() => state[2] === 1);
    const start = Date.now();
    const stopping = pool.destroy();
    expect((await call).content[0].text).toContain('shutting down');
    await sleep(100);
    expect(pool.activity.executing).toBe(1);
    expect(exited).toBe(false);
    await stopping;
    expect(Date.now() - start).toBeGreaterThanOrEqual(14500);
    expect(Date.now() - start).toBeLessThan(22000);
    expect(exited).toBe(true);
    expect(pool.activity).toEqual({ queued: 0, executing: 0, starting: 0, retiring: 0 });
    expect(pool.liveWorkers).toBe(0);
    expect((await pool.run('codegraph_search', {})).content[0].text).toContain('shutting down');
    record({ scenario: 'intentional-stuck-cleanup', elapsedMs: Date.now() - start, exited, finalActivity: pool.activity });
  }, 30000);
  it('drains healthy admitted work naturally without a busy timer before routine handover', async () => {
    const gate = new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT);
    const state = new Int32Array(gate);
    const pool = new QueryPool({ root: null, execution: execution(), createWorker: () =>
      new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: { gate } }) });
    const running = pool.run('codegraph_search', {});
    await until(() => state[2] === 1);
    let drained = false;
    try {
      const natural = pool.drainNaturally().then(() => { drained = true; });
      expect((await pool.run('codegraph_search', {})).content[0].text).toMatch(/draining|closing|shutting down/i);
      await sleep(100);
      expect(drained).toBe(false);
      expect(pool.activity.executing).toBe(1);
      expect(pool.liveWorkers).toBe(1);
      Atomics.store(state, 3, 1); Atomics.notify(state, 3);
      expect((await running).content[0].text).toMatch(/^worker:/);
      await natural;
      expect(drained).toBe(true);
      expect(pool.activity).toEqual({ queued: 0, executing: 0, starting: 0, retiring: 0 });
    } finally { Atomics.store(state, 3, 1); Atomics.notify(state, 3); await pool.destroy(); }
  }, 10000);
  it('removes one session queued request while another real running request completes', async () => {
    const gate = new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT);
    const state = new Int32Array(gate);
    const authority = execution();
    const pool = new QueryPool({ root: null, execution: authority, createWorker: () =>
      new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: { gate } }) });
    const running = pool.run('codegraph_search', {}, createQueryRequestContext(authority, 'surviving', 1));
    await until(() => state[2] === 1);
    const queued = pool.run('codegraph_search', {}, createQueryRequestContext(authority, 'departing', 1));
    try {
      pool.cancelSession('departing');
      expect((await queued).content[0].text).toMatch(/cancelled|abandoned/i);
      expect(pool.activity).toMatchObject({ queued: 0, executing: 1 });
      Atomics.store(state, 3, 1); Atomics.notify(state, 3);
      expect((await running).content[0].text).toMatch(/^worker:/);
      expect(state[2]).toBe(1);
      expect(state[4]).toBe(1);
    } finally {
      Atomics.store(state, 3, 1); Atomics.notify(state, 3);
      await pool.destroy();
    }
  }, 10000);

  it('settles executing cancellation once without freeing its synchronous worker early', async () => {
    const gate = new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT);
    const state = new Int32Array(gate);
    const authority = execution();
    const pool = new QueryPool({ root: null, execution: authority, createWorker: () =>
      new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: { gate, mode: 'duplicate' } }) });
    const controller = new AbortController();
    let outcomes = 0;
    const abandoned = pool.run('codegraph_search', {}, createQueryRequestContext(authority, 'departing', 1), controller.signal)
      .then(result => { outcomes++; return result; });
    await until(() => state[2] === 1);
    const survivor = pool.run('codegraph_search', {}, createQueryRequestContext(authority, 'surviving', 1));
    try {
      controller.abort();
      expect((await abandoned).content[0].text).toMatch(/cancelled|abandoned/i);
      expect(pool.activity).toMatchObject({ queued: 1, executing: 1 });
      expect(state[2]).toBe(1);
      Atomics.store(state, 3, 1); Atomics.notify(state, 3);
      expect((await survivor).content[0].text).toMatch(/^worker:/);
      expect(state[2]).toBe(2);
      expect(state[4]).toBe(2);
      expect(outcomes).toBe(1);
      expect(pool.activity).toMatchObject({ queued: 0, executing: 0 });
    } finally {
      Atomics.store(state, 3, 1); Atomics.notify(state, 3);
      await pool.destroy();
    }
  }, 10000);

  it('tears down only abandoned stuck execution after its allowance while shared peers survive', async () => {
    const gate = new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT);
    const state = new Int32Array(gate);
    const authority = createQueryExecutionContext(resolveQueryPolicy({ poolSize: '2', busyTimeoutMs: 'unlimited' }, 'shared-daemon', 8),
      { readOnly: false, writer: true, synchronization: true });
    const workers: Worker[] = [];
    let exits = 0;
    const pool = new QueryPool({ root: null, execution: authority, createWorker: () => {
      const worker = new Worker(path.join(__dirname, 'fixtures/query-capacity-worker.cjs'), { workerData: { gate } });
      workers.push(worker); worker.on('exit', () => { exits++; }); return worker;
    } });
    const controller = new AbortController();
    const departed = pool.run('codegraph_search', {}, createQueryRequestContext(authority, 'departing', 1), controller.signal);
    const surviving = pool.run('codegraph_search', {}, createQueryRequestContext(authority, 'surviving', 1));
    await until(() => state[2] === 2);
    const start = Date.now();
    controller.abort();
    expect((await departed).content[0].text).toContain('cancelled');
    const queued = pool.run('codegraph_search', {}, createQueryRequestContext(authority, 'surviving', 2));
    try {
      await sleep(100);
      expect(exits).toBe(0);
      expect(pool.activity).toMatchObject({ executing: 2, queued: 1 });
      await until(() => state[2] === 3, 22000);
      expect(Date.now() - start).toBeGreaterThanOrEqual(14500);
      expect(exits).toBe(1);
      expect(pool.healthy).toBe(true);
      expect(pool.activity.executing).toBe(2);
      Atomics.store(state, 3, 1); Atomics.notify(state, 3);
      expect((await surviving).content[0].text).toMatch(/^worker:/);
      expect((await queued).content[0].text).toMatch(/^worker:/);
    } finally { Atomics.store(state, 3, 1); Atomics.notify(state, 3); await pool.destroy(); }
    expect(exits).toBe(workers.length);
    expect(pool.activity).toEqual({ queued: 0, executing: 0, starting: 0, retiring: 0 });
    record({ scenario: 'targeted-abandoned-cleanup', elapsedMs: Date.now() - start, workers: workers.length, exits, finalActivity: pool.activity });
  }, 30000);
});

describe('actual MCP cancellation and disconnect', () => {
  it.each(['running-cancel', 'queued-cancel', 'running-disconnect', 'queued-disconnect', 'session-stop'])
  ('suppresses %s delivery and history while the other shared session with the same id survives', async signal => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-cancel-socket-')));
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function cancellationSymbol() { return 1; }\n');
    const cg = await CodeGraph.init(root); await cg.indexAll(); cg.close();
    const { MCPEngine } = require('../dist/mcp/engine');
    const engine = new MCPEngine({ watch: false, queryPolicy: execution().policy });
    await engine.ensureInitialized(root);
    const gate = new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT);
    const state = new Int32Array(gate);
    const pool = new QueryPool({ root, execution: engine.getQueryExecutionContext(), createWorker: () =>
      new Worker(path.join(__dirname, 'fixtures/query-abandonment-worker.cjs'), { workerData: { gate, root, execution: engine.getQueryExecutionContext() } }) });
    engine.getToolHandler().setQueryPool(pool);
    const sessions: MCPSession[] = [];
    const server = net.createServer(socket => {
      const session = new MCPSession(new SocketTransport(socket), engine, { explicitProjectPath: root });
      sessions.push(session); session.start();
    });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    const port = (server.address() as net.AddressInfo).port;
    const first = net.createConnection(port, '127.0.0.1');
    const second = net.createConnection(port, '127.0.0.1');
    let firstOut = '', secondOut = '';
    first.on('data', chunk => { firstOut += chunk; }); second.on('data', chunk => { secondOut += chunk; });
    const send = (socket: net.Socket, value: object) => socket.write(JSON.stringify({ jsonrpc: '2.0', ...value }) + '\n');
    await until(() => sessions.length === 2);
    try {
      const queued = signal.startsWith('queued');
      send(queued ? second : first, { id: 1, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query: 'cancellationSymbol' } } });
      await until(() => state[2] === 1);
      send(queued ? first : second, { id: 1, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query: 'cancellationSymbol' } } });
      await until(() => pool.activity.queued === 1);
      if (signal.endsWith('disconnect')) first.destroy();
      else if (signal === 'session-stop') sessions[0].stop();
      else send(first, { method: 'notifications/cancelled', params: { requestId: 1, reason: 'fixture host abandoned its request' } });
      await sleep(100);
      expect(pool.activity.executing).toBe(1);
      Atomics.store(state, 3, 1); Atomics.notify(state, 3);
      await until(() => secondOut.includes('worker:'));
      await sleep(50);
      expect(firstOut).toBe('');
      expect(secondOut.match(/"id":1/g)).toHaveLength(1);
      expect(sessions[0].getExploreSessionState().view().projects).toHaveLength(0);
      expect(sessions[1].getExploreSessionState().view().projects[0].callCount).toBe(1);
      expect(state[2]).toBe(queued ? 1 : 2);
      record({ scenario: signal, victimResponses: firstOut.length, survivingResponses: 1, victimHistory: 0, survivingHistory: 1, starts: state[2] });
    } finally {
      Atomics.store(state, 3, 1); Atomics.notify(state, 3);
      first.destroy(); second.destroy(); for (const session of sessions) session.stop();
      await pool.destroy(); await engine.stop();
      await new Promise<void>(resolve => server.close(() => resolve()));
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15000);
});
