import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { Worker } from 'worker_threads';
import { CodeGraph, getDatabasePath } from '../src';
import { QueryPool } from '../src/mcp/query-pool';
import { resolveQueryPolicy } from '../src/mcp/query-policy';
import { createQueryExecutionContext, createQueryRequestContext, routeQueryRequest } from '../src/mcp/query-context';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean, ms: number) {
  const end = Date.now() + ms;
  while (!check()) { if (Date.now() > end) throw new Error('mixed real-worker phase did not settle'); await sleep(10); }
}
const release = (gate: SharedArrayBuffer) => { Atomics.store(new Int32Array(gate), 0, 1); Atomics.notify(new Int32Array(gate), 0); };

describe('independent retirement of real unused SQLite workers', () => {
  it.each(['healthy', 'expired', 'abandoned'] as const)
  ('retires the unused peer without ending actual %s execution', async phase => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-idle-mixed-')));
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function healthySymbol() {}\nexport function abandonedSymbol() {}\nexport function idleSymbol() {}\n');
    const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
    const execution = createQueryExecutionContext(resolveQueryPolicy({ poolSize: 'unlimited',
      busyTimeoutMs: phase === 'expired' ? '1000' : 'unlimited' }, 'read-only-fallback', 8),
      { readOnly: true, writer: false, synchronization: false });
    const healthyGate = new SharedArrayBuffer(4), abandonedGate = new SharedArrayBuffer(4);
    const workers: Worker[] = [], observations: any[] = [];
    const pool = new QueryPool({ root, execution, createWorker: () => {
      const worker = new Worker(path.join(__dirname, 'fixtures/query-idle-sqlite-worker.cjs'), { workerData: { root, execution,
        gates: { healthySymbol: healthyGate, abandonedSymbol: abandonedGate } } });
      const threadId = worker.threadId; workers.push(worker);
      worker.on('message', message => observations.push({ threadId, at: process.hrtime.bigint().toString(), ...message }));
      worker.on('exit', code => observations.push({ type: 'exit', threadId, at: process.hrtime.bigint().toString(), code }));
      return worker;
    } });
    const context = (id: number, session: string) => routeQueryRequest(createQueryRequestContext(execution, session, id), 'default', root, getDatabasePath(root));
    let healthyOutcomes = 0, abandonedOutcomes = 0;
    let primaryFailure: unknown;
    const healthy = pool.run('codegraph_search', { query: 'healthySymbol' }, context(1, 'survivor')).then(result => { healthyOutcomes++; return result; });
    const controller = new AbortController();
    let abandoned: Promise<any> | undefined;
    try {
      await until(() => observations.some(row => row.event === 'start' && row.query === 'healthySymbol'), 10000);
      if (phase === 'abandoned') {
        abandoned = pool.run('codegraph_search', { query: 'abandonedSymbol' }, context(1, 'departed'), controller.signal)
          .then(result => { abandonedOutcomes++; return result; });
        await until(() => observations.some(row => row.event === 'start' && row.query === 'abandonedSymbol'), 10000);
      }
      const idle = await pool.run('codegraph_search', { query: 'idleSymbol' }, context(2, 'survivor'));
      expect(idle.content[0].text).toContain('app.ts:3');
      const idleStart = observations.find(row => row.event === 'start' && row.query === 'idleSymbol');
      const healthyStart = observations.find(row => row.event === 'start' && row.query === 'healthySymbol');
      if (phase === 'abandoned') { controller.abort(); expect((await abandoned!).content[0].text).toContain('cancelled'); }
      if (phase === 'expired') { expect((await healthy).content[0].text).toContain('after admission'); expect(healthyOutcomes).toBe(1); }
      await until(() => observations.some(row => row.type === 'exit' && row.threadId === idleStart.threadId), 45000);
      const idleExit = observations.find(row => row.type === 'exit' && row.threadId === idleStart.threadId);
      const idleResult = observations.find(row => row.type === 'result' && row.threadId === idleStart.threadId && row.id === idleStart.id);
      expect(Number(BigInt(idleExit.at) - BigInt(idleResult.at)) / 1e6).toBeGreaterThanOrEqual(30000);
      expect(observations.some(row => row.type === 'exit' && row.threadId === healthyStart.threadId)).toBe(false);
      expect(pool.liveWorkers).toBe(1);
      expect(pool.activity.executing).toBe(1);
      if (phase !== 'expired') expect(healthyOutcomes).toBe(0);
      if (phase === 'abandoned') {
        const victim = observations.find(row => row.event === 'start' && row.query === 'abandonedSymbol');
        expect(observations.some(row => row.type === 'exit' && row.threadId === victim.threadId)).toBe(true);
        expect(abandonedOutcomes).toBe(1);
      }
      release(healthyGate);
      const result = await healthy;
      if (phase !== 'expired') expect(result.content[0].text).toContain('app.ts:1');
      await until(() => pool.activity.executing === 0, 10000);
      expect(healthyOutcomes).toBe(1);
      await pool.destroy();
      expect(workers.every(worker => worker.threadId === -1)).toBe(true);
      expect(pool.activity).toEqual({ queued: 0, executing: 0, starting: 0, retiring: 0 });
      if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'idle-mixed-facts.jsonl'), JSON.stringify({
        phase, root, observations, healthyOutcomes, abandonedOutcomes, finalActivity: pool.activity, idleGraceMs: 30000,
      }) + '\n');
    } catch (error) { primaryFailure = error; throw error; }
    finally {
      let cleanupFailure: unknown;
      try {
        release(healthyGate); release(abandonedGate); await healthy; await abandoned; await pool.destroy();
        if (!workers.every(worker => worker.threadId === -1)) throw new Error('mixed-idle fixture still owns live native workers; preserve its files');
        await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      } catch (error) { cleanupFailure = error; }
      finally {
        try {
          if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'idle-mixed-cleanup.jsonl'), JSON.stringify({
            phase, root, observations, finalActivity: pool.activity, actualExited: workers.every(worker => worker.threadId === -1),
            primaryFailure: primaryFailure ? String(primaryFailure) : null, cleanupFailure: cleanupFailure ? String(cleanupFailure) : null,
          }) + '\n');
        } catch (ledgerError) { if (!primaryFailure && !cleanupFailure) throw ledgerError; process.stderr.write(`Mixed-idle ledger also failed: ${String(ledgerError)}\n`); }
      }
      if (cleanupFailure) { if (!primaryFailure) throw cleanupFailure; process.stderr.write(`Mixed-idle cleanup also failed: ${String(cleanupFailure)}\n`); }
    }
  }, 120000);
});
