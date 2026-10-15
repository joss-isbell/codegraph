import { describe, expect, it, vi } from 'vitest';
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
import { MCPEngine } from '../src/mcp/engine';
import * as freshness from '../src/mcp/index-freshness';

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

async function connectSessions(engine: MCPEngine, root: string) {
  const sessions: MCPSession[] = [];
  const server = net.createServer(socket => {
    const session = new MCPSession(new SocketTransport(socket), engine, { explicitProjectPath: root });
    sessions.push(session); session.start();
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  const sockets = [0, 1].map(() => net.createConnection((server.address() as net.AddressInfo).port, '127.0.0.1'));
  const output = ['', ''];
  sockets.forEach((socket, index) => socket.on('data', chunk => { output[index] += chunk; }));
  await until(() => sessions.length === 2);
  return { sessions, output, send: (index: number, message: object) => sockets[index].write(JSON.stringify({ jsonrpc: '2.0', ...message }) + '\n'),
    close: async () => {
      sockets.forEach(socket => socket.destroy()); sessions.forEach(session => session.stop());
      await new Promise<void>(resolve => server.close(() => resolve()));
    } };
}

describe('abandoned pooled requests', () => {
  it.each([1, 2])('fails queued and new timer-free demand once all %s finite slots have failed owned cleanup', async maximum => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-cancel-reserved-')));
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function cancellationSymbol() { return 1; }\n');
    const cg = await CodeGraph.init(root); await cg.indexAll(); cg.close();
    const authority = createQueryExecutionContext(resolveQueryPolicy({ poolSize: String(maximum), busyTimeoutMs: 'unlimited' }, 'shared-daemon', 8),
      { readOnly: false, writer: true, synchronization: true });
    const gate = new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT), state = new Int32Array(gate);
    const nativeTeardowns: Array<() => Promise<number>> = [];
    let rejections = 0, abandonedOutcomes = 0, queuedOutcomes = 0, newOutcomes = 0;
    const pool = new QueryPool({ root, execution: authority, createWorker: () => {
      const worker = new Worker(path.join(__dirname, 'fixtures/query-abandonment-worker.cjs'), { workerData: { gate, root, execution: authority } });
      nativeTeardowns.push(worker.terminate.bind(worker));
      worker.terminate = () => { rejections++; return Promise.reject(new Error('controlled reserved-slot cleanup failure')); };
      return worker;
    } });
    const controllers = Array.from({ length: maximum }, () => new AbortController());
    const abandoned = controllers.map((controller, id) => pool.run('codegraph_explore', { query: 'cancellationSymbol' },
      createQueryRequestContext(authority, `departing-${id}`, 1), controller.signal).then(result => { abandonedOutcomes++; return result; }));
    try {
      await until(() => state[2] === maximum);
      const queued = pool.run('codegraph_explore', { query: 'cancellationSymbol' }, createQueryRequestContext(authority, 'surviving', 1))
        .then(result => { queuedOutcomes++; return result; });
      controllers.forEach(controller => controller.abort());
      expect((await Promise.all(abandoned)).every(result => result.content[0].text.includes('cancelled'))).toBe(true);
      await until(() => rejections === maximum, 22000);
      const unavailable = await queued;
      expect(unavailable.isError).toBe(true); expect(unavailable.content[0].text).toMatch(/capacity.*unavailable/i);
      const next = await pool.run('codegraph_explore', {}, createQueryRequestContext(authority, 'surviving', 2))
        .then(result => { newOutcomes++; return result; });
      expect(next.isError).toBe(true); expect(next.content[0].text).toMatch(/capacity.*unavailable/i);
      expect(pool.healthy).toBe(false); expect(pool.state).toBe('failed');
      expect(nativeTeardowns).toHaveLength(maximum); expect(pool.liveWorkers).toBe(maximum);
      expect(pool.activity).toEqual({ queued: 0, executing: maximum, starting: 0, retiring: maximum });
      expect(abandonedOutcomes).toBe(maximum); expect(queuedOutcomes).toBe(1); expect(newOutcomes).toBe(1);
      const stopped = pool.destroy();
      await expect(stopped).rejects.toThrow('controlled reserved-slot cleanup failure');
      expect(pool.destroy()).toBe(stopped); expect(rejections).toBe(maximum);
      expect(pool.liveWorkers).toBe(maximum);
      record({ scenario: 'all-finite-slots-cleanup-failed', maximum, rejections, abandonedOutcomes, queuedOutcomes, newOutcomes,
        created: nativeTeardowns.length, retainedWorkers: pool.liveWorkers, retainedActivity: pool.activity, destroyFailed: true });
    } finally {
      await Promise.all(nativeTeardowns.map(terminate => terminate()));
      await pool.destroy().catch(() => undefined);
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 30000);

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
  it('keeps another shared session and its history alive when targeted owned termination rejects', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-cancel-reject-')));
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function cancellationSymbol() { return 1; }\n');
    const cg = await CodeGraph.init(root); await cg.indexAll(); cg.close();
    const { MCPEngine: BuiltEngine } = require('../dist/mcp/engine');
    const engine = new BuiltEngine({ watch: false, queryPolicy: resolveQueryPolicy({ poolSize: '2', busyTimeoutMs: 'unlimited' }, 'shared-daemon', 8) });
    await engine.ensureInitialized(root);
    const gates = [new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT), new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT)];
    const states = gates.map(gate => new Int32Array(gate));
    const workers: Worker[] = [];
    let nativeTerminate!: () => Promise<number>, rejected = 0, exits = 0;
    const pool = new QueryPool({ root, execution: engine.getQueryExecutionContext(), createWorker: () => {
      const index = workers.length;
      const worker = new Worker(path.join(__dirname, 'fixtures/query-abandonment-worker.cjs'), { workerData: { gate: gates[index], root, execution: engine.getQueryExecutionContext() } });
      workers.push(worker); worker.on('exit', () => { exits++; });
      if (index === 0) {
        nativeTerminate = worker.terminate.bind(worker);
        worker.terminate = () => { rejected++; return Promise.reject(new Error('controlled owned termination rejection')); };
      }
      return worker;
    } });
    engine.getToolHandler().setQueryPool(pool);
    const connection = await connectSessions(engine, root);
    const request = (id: number) => ({ id, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query: 'cancellationSymbol' } } });
    try {
      connection.send(0, request(1)); await until(() => states[0][2] === 1);
      connection.send(1, request(1)); await until(() => states[1][2] === 1);
      connection.send(0, { method: 'notifications/cancelled', params: { requestId: 1 } });
      await until(() => rejected === 1, 22000);
      expect(pool.healthy).toBe(true);
      expect(pool.activity).toEqual({ queued: 0, executing: 2, starting: 0, retiring: 1 });
      expect(pool.liveWorkers).toBe(2); expect(exits).toBe(0);
      connection.send(1, request(2)); await until(() => pool.activity.queued === 1);
      expect(workers).toHaveLength(2); // the unexited retirement still owns its finite slot
      Atomics.store(states[1], 3, 1); Atomics.notify(states[1], 3);
      await until(() => connection.output[1].split('\n').filter(Boolean).length === 2);
      expect(connection.output[0]).toBe('');
      expect(connection.output[1]).toContain('cancellationSymbol');
      expect(connection.sessions[0].getExploreSessionState().view().projects).toHaveLength(0);
      expect(connection.sessions[1].getExploreSessionState().view().projects[0].callCount).toBe(2);
      // A known teardown failure is an explicit handover failure, not an
      // unbounded wait for healthy execution or permission to stop its peers.
      await expect(pool.drainNaturally()).rejects.toThrow('owned worker cleanup incomplete');
      expect(exits).toBe(0);
      const stopped = pool.destroy();
      await expect(stopped).rejects.toThrow('controlled owned termination rejection');
      expect(pool.destroy()).toBe(stopped);
      expect(exits).toBe(1); expect(rejected).toBe(1);
      expect(pool.liveWorkers).toBe(1);
      expect(pool.activity).toEqual({ queued: 0, executing: 1, starting: 0, retiring: 1 });
      record({ scenario: 'targeted-cleanup-rejected', rejected, peerResponses: 2, peerHistory: 2, victimHistory: 0,
        exits, retainedWorkers: pool.liveWorkers, retainedActivity: pool.activity, destroyFailed: true, naturalHandoverFailed: true });
    } finally {
      await connection.close();
      // This test owns the injected rejection; explicitly use its saved native
      // teardown for fixture cleanup, without claiming the pool's stop passed.
      if (nativeTerminate) await nativeTerminate();
      await pool.destroy().catch(() => undefined); await engine.stop();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 35000);

  it('does not start cancelled status measurement after explicit project activation while a peer completes', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-cancel-activation-')));
    const explicit = path.join(root, 'explicit'); fs.mkdirSync(explicit);
    for (const project of [root, explicit]) {
      fs.writeFileSync(path.join(project, 'app.ts'), 'export function cancellationSymbol() { return 1; }\n');
      const cg = await CodeGraph.init(project); await cg.indexAll(); cg.close();
    }
    const engine = new MCPEngine({ watch: false, queryPolicy: resolveQueryPolicy({ poolSize: '0' }, 'direct', 8) });
    await engine.ensureInitialized(root);
    let release!: () => void, entered = false;
    const gate = new Promise<void>(resolve => { release = resolve; });
    engine.getToolHandler().setProjectLifecycle({ open: (_root, open) => open(),
      activate: async cg => { entered = true; await gate; await cg.sync(); }, release: cg => { cg.close(); } });
    const measurement = vi.spyOn(freshness, 'measurePendingChanges');
    const connection = await connectSessions(engine, root);
    try {
      connection.send(0, { id: 1, method: 'tools/call', params: { name: 'codegraph_status', arguments: { projectPath: explicit } } });
      await until(() => entered);
      connection.send(1, { id: 1, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { projectPath: explicit, query: 'cancellationSymbol' } } });
      connection.send(0, { method: 'notifications/cancelled', params: { requestId: 1 } });
      await sleep(50); release();
      await until(() => connection.output[1].includes('cancellationSymbol'));
      expect(measurement).not.toHaveBeenCalled();
      expect(connection.output[0]).toBe('');
      expect(connection.sessions[0].getExploreSessionState().view().projects).toHaveLength(0);
      expect(connection.sessions[1].getExploreSessionState().view().projects[0].callCount).toBe(1);
      // Call through the real status measurement once to prove the observation
      // detects its owner seam, with no mock SQLite or moved status authority.
      connection.send(1, { id: 2, method: 'tools/call', params: { name: 'codegraph_status', arguments: { projectPath: explicit } } });
      await until(() => connection.output[1].includes('Changes since index'), 12000);
      expect(measurement).toHaveBeenCalledTimes(1);
      record({ scenario: 'activation-cancelled-status', cancelledMeasurements: 0, peerMeasurements: 1, peerHistory: 1, victimHistory: 0 });
    } finally {
      release(); measurement.mockRestore(); await connection.close(); await engine.stop();
      fs.rmSync(root, { recursive: true, force: true });
    }
  }, 20000);

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
