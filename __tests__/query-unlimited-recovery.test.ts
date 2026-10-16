import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { Worker } from 'worker_threads';
import { CodeGraph, getDatabasePath } from '../src';
import { QueryPool } from '../src/mcp/query-pool';
import { resolveQueryPolicy, type QueryExecutionMode } from '../src/mcp/query-policy';
import { createQueryExecutionContext, createQueryRequestContext, routeQueryRequest } from '../src/mcp/query-context';

async function until(check: () => boolean, milliseconds: number) {
  const deadline = Date.now() + milliseconds;
  while (!check()) { if (Date.now() > deadline) throw new Error('independent recovery observation deadline'); await new Promise(resolve => setTimeout(resolve, 10)); }
}

describe('unlimited capacity recovery without a busy timer', () => {
  it.each(['direct', 'writable-fallback', 'read-only-fallback', 'shared-daemon'] as QueryExecutionMode[])
  ('recovers actual startup loss to a real SQLite result in %s', async mode => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-unlimited-recovery-')));
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function recoveredSymbol() {}\n');
    const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
    const readOnly = mode === 'read-only-fallback';
    const execution = createQueryExecutionContext(resolveQueryPolicy({ poolSize: 'unlimited', busyTimeoutMs: 'unlimited' }, mode, 8),
      { readOnly, writer: !readOnly, synchronization: !readOnly });
    const workers: Worker[] = [], observations: any[] = [];
    const pool = new QueryPool({ root, execution, warmWorker: false, createWorker: () => {
      const file = workers.length ? path.resolve(__dirname, '../dist/mcp/query-worker.js') : path.join(__dirname, 'fixtures/query-capacity-worker.cjs');
      const worker = new Worker(file, { workerData: workers.length ? { root, execution } : { mode: 'exit-start' } });
      const threadId = worker.threadId; workers.push(worker);
      worker.on('message', message => observations.push({ threadId, ...message }));
      worker.on('exit', code => observations.push({ type: 'exit', threadId, code }));
      return worker;
    } });
    let primaryFailure: unknown;
    try {
      const snapshots = [pool.observation];
      expect(pool.observation).toMatchObject({ state: 'cold', ready: false, workers: 0 });
      const run = (i: number) => pool.run('codegraph_search', { query: 'recoveredSymbol' },
        routeQueryRequest(createQueryRequestContext(execution, `session-${i}`, 1), 'default', root, getDatabasePath(root)));
      const first = run(0);
      void first.catch(() => {});
      snapshots.push(pool.observation);
      expect(pool.observation).toMatchObject({ state: 'starting', ready: false, starting: 1 });
      await until(() => pool.observation.state === 'recovering', 10000);
      const createdDuringRecovery = workers.length;
      for (let index = 0; index < 20; index++) snapshots.push(pool.observation);
      expect(pool.observation).toMatchObject({ state: 'recovering', ready: false });
      expect(workers).toHaveLength(createdDuringRecovery);
      const results = [await first, ...await Promise.all([run(1), run(2)])];
      snapshots.push(pool.observation);
      expect(pool.observation).toMatchObject({ state: 'active', ready: true });
      for (const result of results) { expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain('app.ts:1'); }
      expect(pool.healthy).toBe(true);
      await pool.destroy();
      expect(workers.every(worker => worker.threadId === -1)).toBe(true);
      expect(pool.activity).toEqual({ queued: 0, executing: 0, starting: 0, retiring: 0 });
      if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'unlimited-recovery.jsonl'), JSON.stringify({
        mode, scenario: 'recovered', results, snapshots, observations, finalActivity: pool.activity,
      }) + '\n');
    } catch (error) { primaryFailure = error; throw error; }
    finally {
      let cleanupFailure: unknown;
      try { await pool.destroy(); if (!workers.every(worker => worker.threadId === -1)) throw new Error('recovery fixture still owns live native workers');
        await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); }
      catch (error) { cleanupFailure = error; }
      finally {
        try {
          if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'unlimited-recovery-cleanup.jsonl'), JSON.stringify({
            mode, scenario: 'recovered', observations, finalActivity: pool.activity, actualExited: workers.every(worker => worker.threadId === -1),
            primaryFailure: primaryFailure ? String(primaryFailure) : null, cleanupFailure: cleanupFailure ? String(cleanupFailure) : null,
          }) + '\n');
        } catch (ledgerError) { if (!primaryFailure && !cleanupFailure) throw ledgerError; process.stderr.write(`Recovery ledger also failed: ${String(ledgerError)}\n`); }
      }
      if (cleanupFailure) { if (!primaryFailure) throw cleanupFailure; process.stderr.write(`Recovery cleanup also failed: ${String(cleanupFailure)}\n`); }
    }
  }, 120000);
  it('exhausts actual failed read-only SQLite starts and settles all timer-free demand without resurrection or repair', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-unlimited-broken-')));
    fs.mkdirSync(path.join(root, '.codegraph'));
    const contents = 'foreign data that must not be repaired'; fs.writeFileSync(getDatabasePath(root), contents);
    const execution = createQueryExecutionContext(resolveQueryPolicy({ poolSize: 'unlimited', busyTimeoutMs: 'unlimited' }, 'read-only-fallback', 8),
      { readOnly: true, writer: false, synchronization: false });
    const workers: Worker[] = [], observations: any[] = [];
    const pool = new QueryPool({ root, execution, createWorker: () => {
      const worker = new Worker(path.resolve(__dirname, '../dist/mcp/query-worker.js'), { workerData: { root, execution } });
      const threadId = worker.threadId; workers.push(worker);
      worker.on('message', message => observations.push({ threadId, ...message }));
      worker.on('exit', code => observations.push({ type: 'exit', threadId, code }));
      return worker;
    } });
    let outcomes = 0;
    let primaryFailure: unknown;
    try {
      const results = await Promise.all(Array.from({ length: 6 }, () => pool.run('codegraph_search', {}).then(result => { outcomes++; return result; })));
      expect(results.every(result => result.isError && /capacity.*unavailable/.test(result.content[0].text))).toBe(true);
      expect(outcomes).toBe(6); expect(pool.state).toBe('failed');
      const created = workers.length;
      expect(created).toBeLessThanOrEqual(13);
      const unavailable = pool.observation;
      expect(unavailable).toMatchObject({ state: 'unavailable', ready: false });
      expect(unavailable.reason).toMatch(/failed|crash|capacity|worker/i);
      for (let index = 0; index < 20; index++) expect(pool.observation.state).toBe('unavailable');
      expect(workers).toHaveLength(created);
      expect((await pool.run('codegraph_search', {})).isError).toBe(true); expect(workers).toHaveLength(created);
      await pool.destroy();
      expect(workers.every(worker => worker.threadId === -1)).toBe(true);
      expect(fs.readFileSync(getDatabasePath(root), 'utf8')).toBe(contents);
      expect(fs.readdirSync(path.join(root, '.codegraph'))).toEqual(['codegraph.db']);
      expect(pool.activity).toEqual({ queued: 0, executing: 0, starting: 0, retiring: 0 });
      if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'unlimited-recovery.jsonl'), JSON.stringify({
        mode: 'read-only-fallback', scenario: 'exhausted', outcomes, created, results, unavailable, observations, finalActivity: pool.activity,
      }) + '\n');
    } catch (error) { primaryFailure = error; throw error; }
    finally {
      let cleanupFailure: unknown;
      try { await pool.destroy(); if (!workers.every(worker => worker.threadId === -1)) throw new Error('exhaustion fixture still owns live native workers');
        await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); }
      catch (error) { cleanupFailure = error; }
      finally {
        try {
          if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'unlimited-recovery-cleanup.jsonl'), JSON.stringify({
            mode: 'read-only-fallback', scenario: 'exhausted', outcomes, observations, finalActivity: pool.activity,
            actualExited: workers.every(worker => worker.threadId === -1), primaryFailure: primaryFailure ? String(primaryFailure) : null,
            cleanupFailure: cleanupFailure ? String(cleanupFailure) : null,
          }) + '\n');
        } catch (ledgerError) { if (!primaryFailure && !cleanupFailure) throw ledgerError; process.stderr.write(`Exhaustion ledger also failed: ${String(ledgerError)}\n`); }
      }
      if (cleanupFailure) { if (!primaryFailure) throw cleanupFailure; process.stderr.write(`Exhaustion cleanup also failed: ${String(cleanupFailure)}\n`); }
    }
  }, 120000);
});
