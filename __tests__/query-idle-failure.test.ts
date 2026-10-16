import { describe, expect, it } from 'vitest';
import { Worker } from 'worker_threads';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as net from 'net';
import { CodeGraph } from '../src';
import { QueryPool } from '../src/mcp/query-pool';
import { resolveQueryPolicy } from '../src/mcp/query-policy';
import { createQueryExecutionContext, createQueryRequestContext } from '../src/mcp/query-context';
import { MCPSession } from '../src/mcp/session';
import { SocketTransport } from '../src/mcp/transport';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean, milliseconds = 10000) {
  const end = Date.now() + milliseconds;
  while (!check()) { if (Date.now() > end) throw new Error('failed-idle worker phase did not settle'); await sleep(10); }
}
const release = (gate: SharedArrayBuffer) => { Atomics.store(new Int32Array(gate), 0, 1); Atomics.notify(new Int32Array(gate), 0); };
const record = (facts: unknown) => { if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT,
  'idle-failure-facts.jsonl'), JSON.stringify(facts) + '\n'); };
async function project() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-idle-failure-')));
  fs.writeFileSync(path.join(root, 'app.ts'), 'export function healthySymbol() {}\nexport function idleSymbol() {}\n');
  const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
  return root;
}

describe('owned idle-worker cleanup failures', () => {
  it('keeps healthy actual MCP execution and both session histories when background idle teardown rejects', async () => {
    const root = await project();
    const { MCPEngine: BuiltEngine } = require('../dist/mcp/engine');
    const engine = new BuiltEngine({ watch: false, queryPolicy: resolveQueryPolicy({ mode: 'unlimited', poolSize: '2' }, 'shared-daemon', 8) });
    await engine.ensureInitialized(root);
    const execution = engine.getQueryExecutionContext();
    const healthyGate = new SharedArrayBuffer(4);
    const workers: Worker[] = [], observations: any[] = [];
    let nativeIdleTerminate!: () => Promise<number>, rejected = 0;
    const pool = new QueryPool({ root, execution, createWorker: () => {
      const worker = new Worker(path.join(__dirname, 'fixtures/query-idle-sqlite-worker.cjs'), { workerData: { root, execution,
        gates: { healthySymbol: healthyGate } } });
      const index = workers.length, threadId = worker.threadId; workers.push(worker);
      worker.on('message', message => observations.push({ threadId, at: process.hrtime.bigint().toString(), ...message }));
      worker.on('exit', code => observations.push({ type: 'exit', threadId, code }));
      if (index === 1) {
        nativeIdleTerminate = worker.terminate.bind(worker);
        worker.terminate = () => { rejected++; return Promise.reject(new Error('controlled idle teardown rejection')); };
      }
      return worker;
    } });
    engine.getToolHandler().setQueryPool(pool);
    const sessions: MCPSession[] = [], output = ['', ''];
    const server = net.createServer(socket => { const session = new MCPSession(new SocketTransport(socket), engine,
      { explicitProjectPath: root }); sessions.push(session); session.start(); });
    await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
    const sockets = [0, 1].map(() => net.createConnection((server.address() as net.AddressInfo).port, '127.0.0.1'));
    sockets.forEach((socket, index) => socket.on('data', chunk => { output[index] += chunk; }));
    const send = (index: number, id: number, query: string) => sockets[index].write(JSON.stringify({ jsonrpc: '2.0', id,
      method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query } } }) + '\n');
    let primaryFailure: unknown;
    try {
      await until(() => sessions.length === 2);
      send(0, 1, 'healthySymbol');
      await until(() => observations.some(row => row.event === 'start' && row.query === 'healthySymbol'));
      send(1, 1, 'idleSymbol'); await until(() => output[1].includes('idleSymbol'));
      await until(() => rejected === 1, 45000);
      expect(output[0]).toBe('');
      expect(pool.healthy).toBe(true);
      expect(pool.activity).toEqual({ queued: 0, executing: 1, starting: 0, retiring: 1 });
      expect(pool.liveWorkers).toBe(2);
      expect(workers).toHaveLength(2);
      expect(observations.filter(row => row.type === 'exit')).toHaveLength(0);
      expect(sessions[0].getExploreSessionState().view().projects).toHaveLength(0);
      expect(sessions[1].getExploreSessionState().view().projects[0].callCount).toBe(1);
      send(1, 2, 'idleSymbol'); await until(() => pool.activity.queued === 1);
      expect(workers).toHaveLength(2); // failed idle teardown retains its finite slot
      release(healthyGate);
      await until(() => output[0].split('\n').filter(Boolean).length === 1 && output[1].split('\n').filter(Boolean).length === 2);
      expect(output[0]).toContain('healthySymbol');
      expect(JSON.parse(output[0].trim()).result.isError).toBeFalsy();
      expect(sessions[0].getExploreSessionState().view().projects[0].callCount).toBe(1);
      expect(sessions[1].getExploreSessionState().view().projects[0].callCount).toBe(2);
      await expect(pool.drainNaturally()).rejects.toThrow('owned worker cleanup incomplete');
      expect(observations.filter(row => row.type === 'exit')).toHaveLength(0);
      const stopped = pool.destroy();
      await expect(stopped).rejects.toThrow('controlled idle teardown rejection');
      expect(pool.destroy()).toBe(stopped);
      expect(rejected).toBe(1);
      expect(workers[0].threadId).toBe(-1);
      expect(pool.liveWorkers).toBe(1);
      expect(pool.activity).toEqual({ queued: 0, executing: 0, starting: 0, retiring: 1 });
      record({ scenario: 'idle-cleanup-rejected-survivor', rejected, survivorResponses: 1, survivorHistory: 1,
        idleResponses: 2, idleHistory: 2, nativeExits: 1, retainedWorkers: pool.liveWorkers, retainedActivity: pool.activity,
        naturalHandoverFailed: true, destroyFailed: true, observations });
    } catch (error) { primaryFailure = error; throw error; }
    finally {
      let cleanupFailure: unknown;
      try {
        release(healthyGate);
        sockets.forEach(socket => socket.destroy()); sessions.forEach(session => session.stop());
        await new Promise<void>(resolve => server.close(() => resolve()));
        // Native fixture cleanup does not make the rejected pool stop pass.
        if (nativeIdleTerminate) await nativeIdleTerminate();
        await pool.destroy().catch(() => undefined); await engine.stop();
        await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      } catch (error) { cleanupFailure = error; }
      finally {
        try { record({ scenario: 'idle-cleanup-rejected-survivor-fixture-cleanup', root, observations,
          actualExited: workers.every(worker => worker.threadId === -1), retainedActivity: pool.activity,
          primaryFailure: primaryFailure ? String(primaryFailure) : null, cleanupFailure: cleanupFailure ? String(cleanupFailure) : null }); }
        catch (ledgerError) { if (!primaryFailure && !cleanupFailure) throw ledgerError; process.stderr.write(`Idle-failure ledger also failed: ${String(ledgerError)}\n`); }
      }
      if (cleanupFailure) { if (!primaryFailure) throw cleanupFailure; process.stderr.write(`Idle-failure cleanup also failed: ${String(cleanupFailure)}\n`); }
    }
  }, 75000);

  it.each([1, 2])('settles timer-free queued/new demand once all %s finite idle slots have failed owned cleanup', async maximum => {
    const root = await project();
    const execution = createQueryExecutionContext(resolveQueryPolicy({ mode: 'unlimited', poolSize: String(maximum) }, 'shared-daemon', 8),
      { readOnly: false, writer: true, synchronization: true });
    const workers: Worker[] = [], nativeTeardowns: Array<() => Promise<number>> = [], rejections: Array<() => void> = [];
    const gate = new SharedArrayBuffer(4), observations: any[] = [];
    let queuedOutcomes = 0, newOutcomes = 0;
    let primaryFailure: unknown;
    const pool = new QueryPool({ root, execution, createWorker: () => {
      const worker = new Worker(path.join(__dirname, 'fixtures/query-idle-sqlite-worker.cjs'), { workerData: { root, execution,
        gates: { healthySymbol: gate } } });
      workers.push(worker); nativeTeardowns.push(worker.terminate.bind(worker));
      worker.on('message', message => observations.push(message));
      worker.terminate = () => new Promise((_resolve, reject) => rejections.push(() => reject(new Error('controlled finite idle cleanup failure'))));
      return worker;
    } });
    try {
      const initial = Promise.all(Array.from({ length: maximum }, (_, id) => pool.run('codegraph_search', { query: 'healthySymbol' },
        createQueryRequestContext(execution, 'initial', id))));
      await until(() => observations.filter(row => row.event === 'start').length === maximum);
      release(gate); for (const result of await initial) expect(result.content[0].text).toContain('app.ts:1');
      await until(() => rejections.length === maximum, 45000);
      const queued = [0, 1].map(id => pool.run('codegraph_search', { query: 'idleSymbol' }, createQueryRequestContext(execution, 'queued', id))
        .then(result => { queuedOutcomes++; return result; }));
      expect(pool.activity).toEqual({ queued: 2, executing: 0, starting: 0, retiring: maximum });
      expect(workers).toHaveLength(maximum);
      rejections.forEach(reject => reject());
      for (const result of await Promise.all(queued)) expect(result.content[0].text).toContain('all configured worker slots remain reserved');
      const newest = await pool.run('codegraph_search', { query: 'idleSymbol' }).then(result => { newOutcomes++; return result; });
      expect(newest.content[0].text).toContain('all configured worker slots remain reserved');
      expect(pool.healthy).toBe(false); expect(pool.state).toBe('failed');
      expect(queuedOutcomes).toBe(2); expect(newOutcomes).toBe(1);
      expect(workers).toHaveLength(maximum);
      expect(workers.every(worker => worker.threadId !== -1)).toBe(true);
      await expect(pool.drainNaturally()).rejects.toThrow('owned worker cleanup incomplete');
      const stopped = pool.destroy(); await expect(stopped).rejects.toThrow('controlled finite idle cleanup failure');
      expect(pool.destroy()).toBe(stopped); expect(rejections).toHaveLength(maximum);
      expect(pool.liveWorkers).toBe(maximum);
      expect(pool.activity).toEqual({ queued: 0, executing: 0, starting: 0, retiring: maximum });
      record({ scenario: 'idle-cleanup-finite-reserved', maximum, spawned: workers.length, rejections: rejections.length,
        queuedOutcomes, newOutcomes, retainedActivity: pool.activity, nativeExits: 0, naturalHandoverFailed: true, destroyFailed: true });
    } catch (error) { primaryFailure = error; throw error; }
    finally {
      let cleanupFailure: unknown;
      try {
        release(gate); rejections.forEach(reject => reject());
        const nativeResults = await Promise.allSettled(nativeTeardowns.map(stop => stop()));
        await pool.destroy().catch(() => undefined);
        const failedNative = nativeResults.find((result): result is PromiseRejectedResult => result.status === 'rejected');
        if (failedNative) throw failedNative.reason;
        await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      } catch (error) { cleanupFailure = error; }
      finally {
        try { record({ scenario: 'idle-cleanup-finite-reserved-fixture-cleanup', maximum, root,
          actualExited: workers.every(worker => worker.threadId === -1), retainedActivity: pool.activity,
          primaryFailure: primaryFailure ? String(primaryFailure) : null, cleanupFailure: cleanupFailure ? String(cleanupFailure) : null }); }
        catch (ledgerError) { if (!primaryFailure && !cleanupFailure) throw ledgerError; process.stderr.write(`Reserved-idle ledger also failed: ${String(ledgerError)}\n`); }
      }
      if (cleanupFailure) { if (!primaryFailure) throw cleanupFailure; process.stderr.write(`Reserved-idle cleanup also failed: ${String(cleanupFailure)}\n`); }
    }
  }, 65000);
});
