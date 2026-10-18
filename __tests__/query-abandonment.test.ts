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
import type { MCPEngine } from '../src/mcp/engine';
import { pathToFileURL } from 'url';
import { Client } from './query-unlimited-mcp-fixture';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { getWriterPidPath, markWriterReady, readWriterLock, releaseWriterLock, tryAcquireWriterLock } from '../src/mcp/writer-lock';
import { removeSpawnLog, settleLosingCandidates } from './daemon-candidates';

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
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-cancel-activation-')));
    const root = path.join(fixture, 'alpha'), explicit = path.join(fixture, 'beta');
    const observations = path.join(fixture, 'workers.jsonl'), cancelledId = 'cold-explicit-status';
    const events = (): any[] => {
      if (!fs.existsSync(observations)) return [];
      if (fs.statSync(observations).size > 1024 * 1024) throw new Error('Cold activation observation overflow');
      return fs.readFileSync(observations, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
    };
    let client: Client | undefined, owner: CodeGraph | undefined, ownsWriter = false, primary: unknown;
    const cleanup: Array<{ phase: string; failure: string | null }> = [];
    try {
      for (const [project, source] of [[root, 'export function alphaForeignCancellationSymbol() { return 17; }\n'],
        [explicit, 'export function betaCancellationPeerSymbol() { return 29; }\n']]) {
        fs.mkdirSync(project); fs.writeFileSync(path.join(project, 'app.ts'), source);
        const graph = await CodeGraph.init(project);
        try { await graph.indexAll(); } finally { graph.close(); }
      }
      // The test process is a genuine live native writer. Its ordinary atomic
      // readiness publication, not a replacement lifecycle, releases catch-up.
      const acquired = tryAcquireWriterLock(explicit, 'direct');
      expect(acquired.kind).toBe('acquired'); ownsWriter = acquired.kind === 'acquired';
      owner = await CodeGraph.open(explicit);
      expect(readWriterLock(explicit)).toMatchObject({ pid: process.pid, mode: 'direct', ready: false });
      const index = fs.realpathSync(path.join(explicit, '.codegraph/codegraph.db'));
      client = new Client(root, 'direct', observations, path.join(fixture, 'unused-release'), 'finite-no-deadline', {
        preload: path.join(__dirname, 'fixtures/query-cold-activation-preload.cjs'),
        environment: { QUERY_COLD_ACTIVATION_WRITER: getWriterPidPath(explicit), QUERY_COLD_ACTIVATION_INDEX: index },
      });
      expect((await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {},
        clientInfo: { name: 'cold-activation-cancellation', version: '1' }, rootUri: pathToFileURL(root).href })).serverInfo.name).toBe('codegraph');
      const cold = parseQueryDiagnostics(await client.request('tools/call', { name: 'codegraph_status', arguments: {} }));
      expect(cold.lifecycle).toMatchObject({ state: 'cold', workers: 0 });
      expect(events().filter(event => event.event === 'explicit-native-open')).toEqual([]);
      client.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: cancelledId, method: 'tools/call',
        params: { name: 'codegraph_status', arguments: { projectPath: explicit } } }) + '\n');
      // A genuine RO SQLite open followed by later ready:false native reads
      // proves the cold activation is awaiting its live owner asynchronously.
      await until(() => {
        const observed = events(), opened = observed.find(event => event.event === 'explicit-native-open' && event.readOnly);
        return !!opened && observed.some(event => event.event === 'explicit-writer-observed'
          && event.owner.pid === process.pid && event.owner.ready === false
          && BigInt(event.monotonicNs) - BigInt(opened.monotonicNs) >= 20_000_000n);
      }, 12000);
      expect(client.replies.filter(reply => reply.id === cancelledId)).toEqual([]);
      const peer = client.request('tools/call', { name: 'codegraph_explore',
        arguments: { projectPath: explicit, query: 'betaCancellationPeerSymbol' } });
      void peer.catch(() => {});
      client.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId: cancelledId } }) + '\n');
      await client.request('ping');
      expect(client.replies.filter(reply => reply.id === cancelledId)).toEqual([]);
      expect(events().filter(event => event.event === 'session-history')).toEqual([]);
      markWriterReady(explicit);
      expect(readWriterLock(explicit)).toMatchObject({ pid: process.pid, ready: true });
      const result = await peer;
      expect(result.isError).toBeFalsy();
      expect(result.content[0].text).toContain('export function betaCancellationPeerSymbol() { return 29; }');
      expect(result.content[0].text).toContain('app.ts');
      expect(result.content[0].text).not.toContain('alphaForeignCancellationSymbol');
      const status = await client.request('tools/call', { name: 'codegraph_status', arguments: { projectPath: explicit } });
      expect(status.content[0].text).toContain('Changes since index');
      const active = parseQueryDiagnostics(status);
      expect(active.project).toEqual({ route: 'explicit', root: explicit, index });
      expect(active.ownership).toEqual(cold.ownership); expect(active.ownership.pid).toBe(client.child.pid);
      expect(active.lifecycle).toMatchObject({ state: 'active', dispatch: 'worker', workers: 1, queued: 0, executing: 0 });
      await client.request('ping');
      expect(client.replies.filter(reply => reply.id === cancelledId)).toEqual([]);
      expect(events().filter(event => event.event === 'session-history')).toEqual([
        expect.objectContaining({ query: 'betaCancellationPeerSymbol', projectRoot: explicit, index: 1 })]);
      expect(events().filter(event => event.event === 'call-start')).toEqual([
        expect.objectContaining({ query: 'betaCancellationPeerSymbol', context: expect.objectContaining({ project: active.project }) })]);
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (phase: string, action: () => Promise<unknown>) => {
        try { await action(); cleanup.push({ phase, failure: null }); }
        catch (error) { cleanup.push({ phase, failure: String(error).slice(0, 2048) }); }
      };
      if (ownsWriter) markWriterReady(explicit);
      if (client) await attempt('native-client-close', () => client!.close());
      for (const project of [root, explicit]) await attempt('losing-candidates-' + path.basename(project), () => settleLosingCandidates(project, () => process.pid));
      await attempt('native-writer-close', async () => { owner?.close(); owner = undefined; });
      if (!owner && ownsWriter) await attempt('writer-release', async () => {
        releaseWriterLock(explicit); expect(readWriterLock(explicit)).toBeNull();
      });
      await attempt('all-workers-exited', async () => {
        const observed = events();
        expect(observed.filter(event => event.event === 'exit')).toHaveLength(observed.filter(event => event.event === 'spawn').length);
      });
      record({ scenario: 'activation-cancelled-public-status', root, explicit, cancelledId, events: events(),
        capture: client?.capture(), replies: client?.replies, primary: primary instanceof Error ? primary.stack : String(primary ?? ''), cleanup });
      if (cleanup.every(item => item.failure === null)) {
        for (const project of [root, explicit]) removeSpawnLog(project);
        fs.rmSync(fixture, { recursive: true, force: true });
      } else if (!primary) throw new Error('Cold activation cleanup incomplete: ' + JSON.stringify(cleanup));
    }
  }, 60000);

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
