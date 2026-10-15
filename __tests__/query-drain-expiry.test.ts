import { describe, expect, it } from 'vitest';
import { Worker } from 'worker_threads';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CodeGraph } from '../src';
import { QueryPool } from '../src/mcp/query-pool';
import { createQueryExecutionContext } from '../src/mcp/query-context';
import { resolveQueryPolicy } from '../src/mcp/query-policy';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean, timeout = 5000): Promise<void> {
  const end = Date.now() + timeout;
  while (!check()) {
    if (Date.now() > end) throw new Error('Natural drain did not reach the declared state');
    await sleep(10);
  }
}

describe('finite admission expiry during natural drain', () => {
  it.each(['queued', 'executing'] as const)('preserves actual %s work accounting through owned worker exit', async phase => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-drain-expiry-')));
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function drainExpirySymbol() { return 1; }\n');
    const graph = await CodeGraph.init(root); await graph.indexAll(); graph.close();
    const execution = createQueryExecutionContext(resolveQueryPolicy({ poolSize: '1', busyTimeoutMs: '1000' }, 'shared-daemon', 8),
      { readOnly: false, writer: true, synchronization: true });
    const gate = new SharedArrayBuffer(5 * Int32Array.BYTES_PER_ELEMENT), state = new Int32Array(gate);
    let exits = 0, outcomes = 0, drained = false;
    const pool = new QueryPool({ root, execution, createWorker: () => {
      const worker = new Worker(path.join(__dirname, 'fixtures/query-abandonment-worker.cjs'), {
        workerData: { root, execution, gate, readyDelay: phase === 'queued' ? 2000 : 0 },
      });
      worker.on('exit', () => { exits++; }); return worker;
    } });
    try {
      if (phase === 'executing') await until(() => pool.activity.starting === 0);
      const call = pool.run('codegraph_explore', { query: 'drainExpirySymbol' }).then(result => { outcomes++; return result; });
      if (phase === 'executing') await until(() => state[2] === 1);
      const natural = pool.drainNaturally().then(() => { drained = true; });
      const busy = await call;
      expect(busy.isError).toBeFalsy(); expect(busy.content[0].text).toContain('after admission');
      expect(outcomes).toBe(1);
      if (phase === 'executing') {
        await sleep(100);
        expect(drained).toBe(false); expect(exits).toBe(0);
        expect(pool.activity).toMatchObject({ queued: 0, executing: 1 });
        expect(pool.liveWorkers).toBe(1);
        Atomics.store(state, 3, 1); Atomics.notify(state, 3);
      } else {
        expect(state[2]).toBe(0);
        expect(pool.activity).toMatchObject({ queued: 0, executing: 0 });
      }
      await until(() => drained, 4000); await natural;
      expect(outcomes).toBe(1); expect(exits).toBe(1);
      expect(state[2]).toBe(phase === 'queued' ? 0 : 1);
      expect(state[4]).toBe(phase === 'queued' ? 0 : 1);
      expect(pool.liveWorkers).toBe(0);
      expect(pool.activity).toEqual({ queued: 0, executing: 0, starting: 0, retiring: 0 });
      if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'drain-expiry-facts.jsonl'),
        JSON.stringify({ phase, outcomes, exits, starts: state[2], completions: state[4], drained, finalActivity: pool.activity }) + '\n');
    } finally {
      Atomics.store(state, 3, 1); Atomics.notify(state, 3);
      await pool.destroy(); fs.rmSync(root, { recursive: true, force: true });
    }
  }, 15000);
});
