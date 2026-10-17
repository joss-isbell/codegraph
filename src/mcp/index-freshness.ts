import { existsSync } from 'fs';
import * as path from 'path';
import { Worker } from 'worker_threads';
import { terminateOnceStarted, workerStarted } from '../worker-teardown';
import type { McpIndexRoute } from '../directory';

export interface PendingChangeCounts {
  added: number;
  modified: number;
  removed: number;
}

/** `getChangedFiles()` may fall back to a huge filesystem scan (#1959). */
const MEASURE_TIMEOUT_MS = 8_000;
let liveMeasurements = 0;
const active = new Map<string, Promise<PendingChangeCounts | null>>();
interface Measurement {
  readonly loaded: Promise<void>;
  readonly exited: Promise<void>;
  termination: Promise<void> | null;
}
/** Every actual worker whose native exit has not been observed. */
const live = new Map<Worker, Measurement>();

export function measurePendingChanges(root: string, timeoutMs = MEASURE_TIMEOUT_MS, projectRoute?: McpIndexRoute): Promise<PendingChangeCounts | null> {
  const selectedRoot = path.resolve(root);
  const held = projectRoute ? Object.freeze({ root: projectRoute.root, index: projectRoute.index }) : undefined;
  const key = JSON.stringify([selectedRoot, held?.root ?? null, held?.index ?? null]);
  const existing = active.get(key);
  if (existing) return existing;
  // Status probes for many projects must not exhaust the shared daemon.
  if (liveMeasurements >= 2) return Promise.resolve(null);

  const pending = runMeasurement(selectedRoot, timeoutMs, held).finally(() => active.delete(key));
  active.set(key, pending);
  return pending;
}

/**
 * Terminate every measurement still running, each once it has loaded its
 * modules (worker-teardown.ts) — for a server that is about to exit, since
 * exiting tears the workers down the same way terminating them does.
 */
export async function endFreshnessMeasurements(): Promise<void> {
  await Promise.all([...live].map(([worker, measurement]) => endMeasurement(worker, measurement)));
}

function endMeasurement(worker: Worker, measurement: Measurement): Promise<void> {
  return measurement.termination ??= (async () => {
    await terminateOnceStarted(worker, measurement.loaded);
    // The legacy safe-start helper deliberately swallows terminate failures.
    // Only the native exit event proves resource/slot release.
    await measurement.exited;
  })();
}

function runMeasurement(root: string, timeoutMs: number, projectRoute?: McpIndexRoute): Promise<PendingChangeCounts | null> {
  // The compiled sibling is beside us in production; Vitest loads src/ but
  // builds dist/ before the tests, so use that copy for the worker there.
  const sibling = path.join(__dirname, 'index-freshness-worker.js');
  const workerFile = existsSync(sibling)
    ? sibling
    : path.resolve(__dirname, '../../dist/mcp/index-freshness-worker.js');
  if (!existsSync(workerFile)) return Promise.resolve(null);

  let worker: Worker;
  try {
    worker = new Worker(workerFile, { workerData: { root, projectRoute } });
  } catch {
    return Promise.resolve(null);
  }

  // Its first message is 'loaded', posted once its modules are in. Terminating
  // it before then can crash the whole process on Windows (worker-teardown.ts).
  const exited = new Promise<void>(resolve => worker.once('exit', () => {
    live.delete(worker); liveMeasurements--; resolve();
  }));
  const measurement: Measurement = { loaded: workerStarted(worker), exited, termination: null };
  live.set(worker, measurement);
  liveMeasurements++;
  return new Promise(resolve => {
    let settled = false;
    const finish = (counts: PendingChangeCounts | null) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      // A worker inside a synchronous Git call may take time to terminate, and
      // one still loading its modules must not be terminated yet. Return
      // unknown on deadline, but keep its concurrency slot until it is gone.
      void endMeasurement(worker, measurement);
      resolve(counts);
    };
    const timer = setTimeout(() => finish(null), timeoutMs);
    worker.on('message', (msg: { type?: string; counts?: PendingChangeCounts | null }) => {
      if (msg?.type !== 'counts') return;
      const value = msg.counts;
      const valid = value && typeof value === 'object' && ['added', 'modified', 'removed'].every(
        key => Number.isSafeInteger(value[key as keyof PendingChangeCounts]) && value[key as keyof PendingChangeCounts] >= 0,
      );
      finish(valid ? value : null);
    });
    worker.once('error', () => finish(null));
    worker.once('exit', () => finish(null));
  });
}
