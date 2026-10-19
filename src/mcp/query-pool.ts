/**
 * Query pool — runs CPU-heavy read-tool calls on a pool of worker threads so
 * the shared daemon's main event loop stays free for the MCP transport.
 *
 * Why this exists: see {@link ./query-worker}. One daemon, one event loop, one
 * synchronous SQLite connection serializes every concurrent `codegraph_explore`
 * AND starves the transport (a 10-way wave delivered 0 transport heartbeats in
 * 25s — responses can't flush until the whole batch drains, so clients time
 * out). Spreading the dispatch across worker threads (each its own WAL read
 * connection) restores true multi-core parallelism and an idle main loop.
 *
 * Properties:
 *   - lazy growth: one warm worker on construct, then demand grows toward the
 *     resolved finite maximum or without a configured maximum in unlimited
 *     selection. Readiness paces cold starts without capping active demand.
 *   - crash recovery: a dead worker is respawned and its in-flight call retried
 *     once; a poison call that keeps crashing fails gracefully (never wedges the
 *     pool). A crash budget trips a circuit breaker (`healthy` → false) so the
 *     pool settles affected calls instead of silently leaving worker execution.
 *   - optional admission deadline: a call that can't be served within `softTimeoutMs`
 *     resolves with SUCCESS-shaped "busy, retry" guidance — never `isError`, so
 *     a momentary overload can't teach the agent to abandon codegraph — instead
 *     of continuing to wait. Calling applications have independent deadlines.
 */

import { Worker } from 'worker_threads';
import * as path from 'path';
import * as os from 'os';
import type { McpIndexRoute } from '../directory';
import type { ToolResult } from './tools';
import { createQueryExecutionContext, type QueryExecutionContext, type QueryRequestContext } from './query-context';
import { requestedQueryPolicy, resolveLegacyQueryPolicy, resolveLegacyPoolSize } from './query-policy';

/** Compiled sibling — `query-worker.js` lives next to this file in `dist/mcp/`. */
const WORKER_FILE = path.join(__dirname, 'query-worker.js');

/**
 * Minimal worker surface the pool drives — satisfied by a real `worker_threads`
 * Worker. Abstracted so tests can inject a fake worker and exercise the pool's
 * queue / growth / crash-recovery / backstop logic without spawning threads or
 * needing a built `dist/`.
 */
export interface PoolWorker {
  postMessage(msg: unknown): void;
  terminate(): Promise<number> | void;
  on(event: 'message', cb: (m: unknown) => void): void;
  on(event: 'error', cb: (e: Error) => void): void;
  on(event: 'exit', cb: (code: number) => void): void;
}

/**
 * Total worker deaths before the pool declares permanent capacity loss.
 * High enough to ride out a few transient
 * crashes, low enough that a systematically-broken worker (e.g. a platform that
 * can't spawn threads) fails truthfully instead of respawning forever.
 */
const CRASH_BUDGET = 12;

/**
 * Max workers cold-starting at once. A worker's cold start is heavy — full
 * module load (tree-sitter etc.) + opening a large WAL DB — and starting the
 * whole pool simultaneously thrashes CPU/I-O so badly it can stall the daemon's
 * main loop for tens of seconds. Warming a couple at a time keeps each start
 * fast; as one reports ready the next begins, so the pool still reaches full
 * size within a few calls of a burst, just without the thundering herd.
 */
const MAX_CONCURRENT_SPAWN = 2;

/** Internal idle grace v1. Query demand, rather than chat/heartbeat activity, owns it. */
export const QUERY_IDLE_RETIREMENT_MS = 30_000;

/**
 * How long `destroy()` waits for a worker that is still starting up before
 * terminating it anyway. Terminating a worker while it is still loading its
 * modules can take the whole process down with an access violation
 * (0xC0000005, seen on Windows), and the daemon shuts its pool down whenever
 * it stops — a session that ends within a few seconds of starting has a
 * worker in exactly that state. A start takes about a second normally and a
 * few under heavy load; the cap only bounds a start that is wedged.
 */
const WORKER_START_SETTLE_MS = 15_000;

/** Provisional engineering bounds v1; never applied to healthy routine handover. */
export const QUERY_CLEANUP_BOUNDS = Object.freeze({ version: 1, drainMs: 15_000, startupMs: WORKER_START_SETTLE_MS, cleanupMs: 5_000 });

/** Shape of a message a worker posts back (ready handshake or a tool result). */
interface WorkerMessage {
  type?: string;
  ok?: boolean;
  id?: number;
  result?: ToolResult;
}

interface Job {
  id: number;
  toolName: string;
  args: Record<string, unknown>;
  context?: QueryRequestContext;
  resolve: (r: ToolResult) => void;
  retries: number;
  settled: boolean;
  enqueuedAt: number;
  softTimer?: NodeJS.Timeout;
  detachAbort?: () => void;
  abandonmentTimer?: NodeJS.Timeout;
}

export interface QueryPoolOptions {
  /** Internal engine adapter: diagnostics and open chats must not create workers. */
  warmWorker?: boolean;
  /** Engine-resolved immutable policy/authority; bypasses every legacy env parser. */
  execution?: QueryExecutionContext;
  /** Default project root each worker opens at spawn, or null for per-call projects. */
  root: string | null;
  /** Exact acknowledged DB-file route from the engine, never re-resolved here. */
  projectRoute?: McpIndexRoute;
  /** Max worker threads. Defaults to `clamp(cores-1, 1, 16)`. */
  size?: number;
  /** Admission-to-answer backstop (queue plus execution). Default 45s. */
  softTimeoutMs?: number;
  /** Retries for an in-flight call whose worker crashed. Default 1. */
  maxRetries?: number;
  /** Worker factory (tests inject a fake). Defaults to a real `worker_threads` Worker. */
  createWorker?: () => PoolWorker;
  /** How long destroy() waits on a worker still starting up (tests shorten it). Default 15s. */
  startSettleMs?: number;
}

/**
 * Resolve the pool size from the `CODEGRAPH_QUERY_POOL_SIZE` override and the
 * machine's core count. `0` (or a negative) explicitly disables the pool (the
 * caller serves in-process — today's behavior). Unset → `clamp(cores-1, 1, 16)`:
 * leave a core for the main loop + OS, but never zero, since even one worker
 * frees the transport and lets responses flush incrementally.
 */
export function resolvePoolSize(envVal: string | undefined, cpuCount: number): number {
  return resolveLegacyPoolSize(envVal, cpuCount);
}

/** Success-shaped overload guidance (NEVER isError — see the abandonment rule). */
function busyGuidance(waitedMs: number): ToolResult {
  const secs = (Math.max(0, waitedMs) / 1000).toFixed(3);
  return {
    content: [{
      type: 'text',
      text:
        `CodeGraph could not return this pooled call within ${secs}s after admission (including queueing and execution). ` +
        `This is busy guidance, not an error. Wait a few seconds and retry this exact call. ` +
        `If you can't wait, use your built-in tools for just this one step.`,
    }],
  };
}

export class QueryPool {
  private idle: PoolWorker[] = [];
  private queue: Job[] = [];
  private inflight = new Map<PoolWorker, Job>();
  private workers = new Set<PoolWorker>();
  // Workers spawned but not yet 'ready'. Growth must count these so a single
  // first call (with the eager worker still starting) doesn't spawn the WHOLE
  // pool at once — N simultaneous cold worker starts (each a full module load +
  // a large DB open) saturate the box and starve the main loop. Grow only when
  // the queue outstrips idle + pending.
  private pendingWorkers = new Set<PoolWorker>();
  // Each worker's start, settled when it posts 'ready' (either way) or goes
  // away — what destroy() waits on before terminating it (see
  // WORKER_START_SETTLE_MS).
  private starts = new Map<PoolWorker, { settled: Promise<void>; settle: () => void; timer: NodeJS.Timeout }>();
  private terminations = new Set<Promise<void>>();
  private retirements = new Map<PoolWorker, Promise<void>>();
  private failedRetirements = new Set<PoolWorker>();
  private destroyPromise: Promise<void> | null = null;
  private nextId = 1;
  private totalCrashes = 0;
  private everReady = false;
  private recovering = false;
  private destroyed = false;
  private accepting = true;
  private quietWaiters: Array<{ resolve: () => void; reject: (error: Error) => void }> = [];
  private cleanupFailure: Error | null = null;
  private failure: string | null = null;
  private readonly root: string | null;
  private readonly projectRoute?: McpIndexRoute;
  private readonly maxSize: number;
  private readonly softTimeoutMs: number | null;
  readonly execution: QueryExecutionContext;
  private readonly maxRetries: number;
  private readonly createWorker: () => PoolWorker;
  private readonly startSettleMs: number;
  private idleRetirementTimers = new Map<PoolWorker, NodeJS.Timeout>();

  constructor(opts: QueryPoolOptions) {
    this.root = opts.root;
    if (opts.projectRoute && opts.projectRoute.root !== opts.root) throw new Error('Query worker route must match its default root.');
    this.projectRoute = opts.projectRoute ? Object.freeze({ root: opts.projectRoute.root, index: opts.projectRoute.index }) : undefined;
    // Standalone legacy callers retain permissive parsing and ordinary defaults.
    // Engine callers always supply one already-resolved context; no env reread.
    this.execution = opts.execution ?? createQueryExecutionContext(resolveLegacyQueryPolicy({ ...requestedQueryPolicy(process.env),
      poolSize: String(opts.size ?? Math.max(1, Math.min(os.cpus().length - 1, 16))),
      ...(opts.softTimeoutMs !== undefined ? { busyTimeoutMs: String(opts.softTimeoutMs) } : {}),
    }, 'shared-daemon', os.cpus().length), { readOnly: false, writer: false, synchronization: false });
    const { concurrency, deadline } = this.execution.policy;
    if (concurrency.kind === 'disabled') throw new Error('Disabled query policy must execute on the main thread, without a QueryPool.');
    this.maxSize = concurrency.kind === 'finite' ? concurrency.maximum : Infinity;
    this.softTimeoutMs = opts.execution ? (deadline.kind === 'finite' ? deadline.milliseconds : null) : opts.softTimeoutMs ?? (deadline.kind === 'finite' ? deadline.milliseconds : null);
    this.maxRetries = opts.maxRetries ?? 1;
    this.createWorker = opts.createWorker ?? (() => new Worker(WORKER_FILE, { workerData: { root: this.root, projectRoute: this.projectRoute, execution: this.execution } }));
    this.startSettleMs = opts.startSettleMs ?? WORKER_START_SETTLE_MS;
    if (opts.warmWorker !== false) this.spawnOne(); // preserve standalone callers
  }

  /** Pool size cap (for logging/status). */
  get size(): number { return this.maxSize; }

  /** Live worker count (for tests/status). */
  get liveWorkers(): number { return this.workers.size; }

  /**
   * False after permanent capacity loss or destroy. Pool presence remains the
   * dispatch boundary; this flag must never select a main-thread fallback.
   */
  get healthy(): boolean {
    return !this.destroyed && this.failure === null;
  }

  get state(): 'accepting' | 'draining' | 'failed' | 'destroyed' {
    return this.destroyed ? 'destroyed' : this.failure ? 'failed' : this.accepting ? 'accepting' : 'draining';
  }

  /** Immutable ownership facts, including execution whose caller already expired. */
  get activity(): Readonly<{ queued: number; executing: number; starting: number; retiring: number }> {
    return Object.freeze({ queued: this.queue.filter(job => !job.settled).length, executing: this.inflight.size,
      starting: this.pendingWorkers.size, retiring: this.terminations.size });
  }

  private unavailable(): ToolResult {
    return { isError: true, content: [{ type: 'text', text: this.destroyed
      ? 'codegraph is shutting down; retry shortly.'
      : this.failure ? `CodeGraph query worker capacity is unavailable: ${this.failure}. Retry after correcting the worker failure.`
      : 'CodeGraph query pool is draining for handover; retry with its replacement.' }] };
  }

  private fail(reason: string): void {
    if (!this.healthy) return;
    this.failure = reason;
    for (const job of [...this.inflight.values(), ...this.queue]) this.settle(job, this.unavailable());
    const workers = [...this.workers];
    this.idle = [];
    this.queue = [];
    for (const worker of workers) this.retire(worker);
    this.notifyQuiet();
  }

  /**
   * Whether actual ready capacity exists, including occupied workers. Cold or
   * recovering demand still belongs to this pool and waits in its queue.
   */
  get ready(): boolean {
    return this.healthy && (this.idle.some(worker => !this.retirements.has(worker))
      || [...this.inflight.keys()].some(worker => !this.retirements.has(worker)));
  }

  /** Pure observation: never dispatches, starts workers or resets idle grace. */
  get observation() {
    const activity = this.activity;
    const ready = this.ready;
    const state = this.destroyed ? 'stopped' as const : this.failure ? 'unavailable' as const
      : !this.accepting ? 'draining' as const : ready ? 'active' as const
      : activity.starting ? this.recovering ? 'recovering' as const : 'starting' as const
      : this.workers.size ? 'retiring' as const : this.everReady ? 'retired' as const : 'cold' as const;
    return Object.freeze({ state, ready, workers: this.liveWorkers, ...activity, reason: this.failure ?? this.cleanupFailure?.message ?? null });
  }

  private spawnOne(): void {
    if (!this.healthy || this.workers.size >= this.maxSize) return;
    let w: PoolWorker;
    try {
      w = this.createWorker();
    } catch {
      this.recovering = true;
      this.totalCrashes++; // counts toward the circuit breaker
      if (this.totalCrashes >= CRASH_BUDGET) this.fail('worker creation failed repeatedly');
      return;
    }
    this.workers.add(w);
    this.pendingWorkers.add(w);
    let settle!: () => void;
    const settled = new Promise<void>((resolve) => { settle = resolve; });
    // This bounds only startup, independently of a job's optional busy timer.
    // At expiry the worker has had the full Windows startup-handshake grace.
    const timer = setTimeout(() => {
      this.startSettled(w);
      this.onWorkerGone(w, 'worker did not become ready within its startup allowance');
    }, this.startSettleMs);
    timer.unref?.();
    this.starts.set(w, { settled, settle, timer });
    w.on('message', (m) => this.onMessage(w, (m ?? {}) as WorkerMessage));
    w.on('error', () => { this.startSettled(w); this.onWorkerGone(w, 'worker error'); });
    w.on('exit', (code) => { this.startSettled(w); this.onWorkerGone(w, `worker exited (${code})`); });
  }

  private onMessage(w: PoolWorker, m: WorkerMessage): void {
    // Before the retired-worker check: destroy() waits on this for a worker
    // it has already let go of.
    if (m?.type === 'ready') this.startSettled(w);
    if (!m || !this.workers.has(w)) return; // ignore late messages from retired workers
    if (m.type === 'ready') {
      if (!this.pendingWorkers.delete(w)) return; // already handled this handshake
      if (this.retirements.has(w) || !this.healthy) return;
      if (m.ok !== true) {
        this.onWorkerGone(w); // failed opens consume the same budget as crashes
        return;
      }
      this.everReady = true;
      this.recovering = false;
      this.idle.push(w);
      this.drain();
      return;
    }
    if (m.type === 'result') {
      const job = this.inflight.get(w);
      // A late/duplicate/wrong-id result cannot release a different job's slot.
      if (!job || m.id !== job.id || !m.result) return;
      this.inflight.delete(w);
      if (job.abandonmentTimer) clearTimeout(job.abandonmentTimer);
      if (!this.retirements.has(w) && this.healthy) this.idle.push(w);
      this.settle(job, m.result);
      this.drain();
      this.notifyQuiet();
    }
  }

  // A worker died (crash hook, OOM, segfault, exit≠0). Respawn a replacement and
  // retry its in-flight job once; a job that keeps crashing workers fails
  // gracefully so it can't loop the pool forever.
  private onWorkerGone(w: PoolWorker, reason = 'worker failed to open its project'): void {
    if (this.retirements.has(w)) return; // intentional retirement is not a capacity crash
    if (!this.workers.has(w)) return; // already handled (error+exit both fire)
    this.workers.delete(w);
    this.pendingWorkers.delete(w);
    this.idle = this.idle.filter((x) => x !== w);
    this.totalCrashes++;
    this.recovering = true;
    const job = this.inflight.get(w);
    this.inflight.delete(w);
    if (job?.abandonmentTimer) clearTimeout(job.abandonmentTimer);
    this.retire(w);
    if (this.totalCrashes >= CRASH_BUDGET) {
      if (job) this.settle(job, { isError: true, content: [{ type: 'text', text: `CodeGraph query worker capacity is unavailable: ${reason}; recovery exhausted.` }] });
      this.fail(`${reason}; recovery exhausted`);
      return;
    }
    if (this.healthy) this.spawnOne(); // keep capacity
    if (job && !job.settled) {
      if (job.retries < this.maxRetries && this.healthy) {
        job.retries++;
        this.queue.unshift(job); // head of line — retry promptly
      } else {
        this.settle(job, { isError: true, content: [{ type: 'text', text: 'codegraph worker crashed; please retry the call.' }] });
      }
    }
    this.drain();
    this.notifyQuiet();
  }

  private drain(): void {
    if (!this.healthy) return;
    // Expired queued callers are no longer demand. Prune before growth so late
    // readiness cannot create workers for calls the admission timer answered.
    this.queue = this.queue.filter(job => !job.settled);
    // Grow toward maxSize while queued work outstrips workers that are idle OR
    // already on their way up (pending) — so we never spawn the whole pool for a
    // single call whose eager worker just hasn't reported ready yet.
    while (
      this.queue.length > this.idle.length + this.pendingWorkers.size &&
      this.workers.size < this.maxSize &&
      this.pendingWorkers.size < MAX_CONCURRENT_SPAWN &&
      this.healthy
    ) {
      this.spawnOne();
    }
    while (this.idle.length && this.queue.length) {
      // Skip jobs the backstop already answered.
      let job: Job | undefined;
      while (this.queue.length && (job = this.queue.shift()) && job.settled) job = undefined;
      if (!job || job.settled) break;
      const w = this.idle.pop()!;
      this.clearIdleRetirement(w);
      this.inflight.set(w, job);
      try {
        w.postMessage({ type: 'call', id: job.id, toolName: job.toolName, args: job.args, context: job.context });
      } catch {
        this.onWorkerGone(w, 'worker dispatch failed');
      }
    }
    this.updateIdleRetirement();
  }

  private clearIdleRetirement(worker?: PoolWorker): void {
    for (const [owned, timer] of this.idleRetirementTimers) {
      if (worker && worker !== owned) continue;
      clearTimeout(timer);
      this.idleRetirementTimers.delete(owned);
    }
  }

  /** An unused ready peer may retire while another worker remains occupied. */
  private canRetireIdle(worker: PoolWorker): boolean {
    return this.execution.policy.retention === 'retire-idle' && this.healthy && this.accepting
      && this.workers.has(worker) && this.idle.includes(worker)
      && !this.queue.some(job => !job.settled) && !this.inflight.has(worker)
      && !this.pendingWorkers.has(worker) && !this.retirements.has(worker);
  }

  private updateIdleRetirement(): void {
    for (const worker of this.idleRetirementTimers.keys()) {
      if (!this.canRetireIdle(worker)) this.clearIdleRetirement(worker);
    }
    for (const worker of this.idle) {
      if (!this.canRetireIdle(worker) || this.idleRetirementTimers.has(worker)) continue;
      const timer = setTimeout(() => {
        this.idleRetirementTimers.delete(worker);
        if (!this.canRetireIdle(worker)) return;
        // Remove only this dispatchable slot before asynchronous teardown. An
        // occupied peer keeps executing. Retiring workers remain owned/count
        // toward finite capacity until actual exit; new demand waits or uses
        // other free capacity, without reviving this retired worker.
        this.idle = this.idle.filter(candidate => candidate !== worker);
        // Background release has the same peer-isolated ownership contract as
        // targeted abandoned work: a rejected teardown reserves only this slot.
        this.retire(worker, true);
      }, QUERY_IDLE_RETIREMENT_MS);
      timer.unref?.();
      this.idleRetirementTimers.set(worker, timer);
    }
  }

  private settle(job: Job, result: ToolResult): void {
    if (job.settled) return; // already answered (by backstop or worker)
    job.settled = true;
    if (job.softTimer) clearTimeout(job.softTimer);
    job.detachAbort?.();
    job.resolve(result);
  }

  private abandon(job: Job): void {
    this.settle(job, { content: [{ type: 'text', text: 'CodeGraph request cancelled or abandoned.' }] });
    this.queue = this.queue.filter(candidate => candidate !== job);
    const worker = [...this.inflight].find(([, current]) => current === job)?.[0];
    if (worker && !job.abandonmentTimer && !this.destroyed) {
      // Cancellation settles delivery, not execution. Only this abandoned
      // worker is retired after its drain allowance; shared peers keep running.
      job.abandonmentTimer = setTimeout(() => {
        if (this.inflight.get(worker) === job) this.retire(worker, true);
      }, QUERY_CLEANUP_BOUNDS.drainMs);
      job.abandonmentTimer.unref?.();
    }
    this.notifyQuiet();
  }

  private notifyQuiet(): void {
    this.updateIdleRetirement();
    if (this.queue.some(job => !job.settled) || this.inflight.size) return;
    for (const waiter of this.quietWaiters.splice(0)) waiter.resolve();
  }

  /** Routine handover has no execution deadline and never interrupts admitted work. */
  async drainNaturally(): Promise<void> {
    this.accepting = false;
    this.clearIdleRetirement();
    if (this.cleanupFailure) throw this.cleanupFailure;
    if (this.queue.some(job => !job.settled) || this.inflight.size) {
      await new Promise<void>((resolve, reject) => this.quietWaiters.push({ resolve, reject }));
    }
    await this.destroy();
  }

  /** Abandon only this engine generation's session; occupied slots stay owned. */
  cancelSession(sessionId: string): void {
    for (const job of [...this.queue, ...this.inflight.values()]) {
      if (job.context?.sessionId === sessionId && job.context.execution.generation === this.execution.generation) {
        this.abandon(job);
      }
    }
    this.queue = this.queue.filter(job => !job.settled);
  }

  /** Run a read tool on the pool. Always resolves (never rejects). */
  run(toolName: string, args: Record<string, unknown>, context?: QueryRequestContext, signal?: AbortSignal): Promise<ToolResult> {
    if (!this.healthy || !this.accepting) return Promise.resolve(this.unavailable());
    return new Promise<ToolResult>((resolve) => {
      const job: Job = {
        id: this.nextId++, toolName, args, context, resolve,
        retries: 0, settled: false, enqueuedAt: Date.now(),
      };
      const aborted = () => this.abandon(job);
      signal?.addEventListener('abort', aborted, { once: true });
      job.detachAbort = () => signal?.removeEventListener('abort', aborted);
      if (signal?.aborted) { this.abandon(job); return; }
      // This optional timer starts once on admission, spanning queue and actual
      // execution. Retry keeps the same timer. Expiry answers only the caller;
      // an executing worker stays occupied until completion or safe teardown.
      if (this.softTimeoutMs !== null) {
        job.softTimer = setTimeout(() => {
          if (!job.settled) this.settle(job, busyGuidance(Date.now() - job.enqueuedAt));
          // The last queued caller can expire while natural handover waits.
          // Inflight execution still prevents quiet, even after its caller settles.
          this.notifyQuiet();
        }, this.softTimeoutMs);
        job.softTimer.unref?.();
      }
      this.queue.push(job);
      this.drain();
    });
  }

  private startSettled(w: PoolWorker): void {
    const start = this.starts.get(w);
    if (!start) return;
    this.starts.delete(w);
    clearTimeout(start.timer);
    start.settle();
  }

  private retire(worker: PoolWorker, preservePeers = false): Promise<void> {
    const prior = this.retirements.get(worker);
    if (prior) return prior;
    const termination = this.terminateStarted(worker);
    this.retirements.set(worker, termination);
    this.terminations.add(termination);
    void termination.then(() => {
      this.terminations.delete(termination);
      this.retirements.delete(worker);
      this.failedRetirements.delete(worker);
      const job = this.inflight.get(worker);
      if (job?.abandonmentTimer) clearTimeout(job.abandonmentTimer);
      this.inflight.delete(worker);
      this.workers.delete(worker);
      this.pendingWorkers.delete(worker);
      this.idle = this.idle.filter(candidate => candidate !== worker);
      this.notifyQuiet();
      this.drain();
    }, error => {
      // Keep the ownership record and expose incomplete cleanup. Never pretend
      // a still-owned worker exited, and never let a lost slot escape to main.
      this.cleanupFailure = new Error(`owned worker cleanup incomplete: ${error instanceof Error ? error.message : String(error)}`);
      this.failedRetirements.add(worker);
      for (const waiter of this.quietWaiters.splice(0)) waiter.reject(this.cleanupFailure);
      if (preservePeers) {
        // One abandoned or idle peer cannot take healthy shared workers down.
        // Retain its slot and rejected retirement for truthful final shutdown.
        process.stderr.write(`[CodeGraph MCP] Targeted ${this.cleanupFailure.message}; its slot remains owned, without stopping unrelated workers.\n`);
        // Failed owned slots cannot be oversubscribed. If every finite slot is
        // irrecoverably reserved, timer-free queued/new demand must fail rather
        // than wait forever. Pending successful retirements remain recoverable.
        if (this.healthy && this.workers.size >= this.maxSize && [...this.workers].every(owned => this.failedRetirements.has(owned))) {
          this.failure = 'all configured worker slots remain reserved after owned cleanup failed';
          for (const job of this.queue) this.settle(job, this.unavailable());
          this.queue = [];
          this.notifyQuiet();
        }
      } else this.fail(this.cleanupFailure.message);
    });
    return termination;
  }

  /**
   * Terminate a worker, but not while it is still starting up: wait for it to
   * post 'ready' (or go away), up to `startSettleMs` (WORKER_START_SETTLE_MS —
   * see that constant for the crash this avoids).
   */
  private async terminateStarted(w: PoolWorker): Promise<void> {
    const start = this.starts.get(w);
    if (start) {
      let cap: NodeJS.Timeout | undefined;
      await Promise.race([
        start.settled,
        new Promise<void>((resolve) => { cap = setTimeout(resolve, this.startSettleMs); cap.unref?.(); }),
      ]);
      clearTimeout(cap);
    }
    this.startSettled(w);
    let cleanup: NodeJS.Timeout | undefined;
    try {
      await Promise.race([Promise.resolve(w.terminate()), new Promise<never>((_, reject) => {
        cleanup = setTimeout(() => reject(new Error('owned worker did not exit within provisional 5s cleanup allowance')), QUERY_CLEANUP_BOUNDS.cleanupMs);
        cleanup.unref?.();
      })]);
    } finally { clearTimeout(cleanup); }
  }

  /**
   * Intentional shutdown: settle delivery, preserve actual occupancy for the
   * provisional drain, then terminate owned workers after the distinct startup
   * handshake allowance and await bounded final cleanup. Routine handover must
   * use drainNaturally(), which has no healthy-execution deadline.
   */
  destroy(): Promise<void> {
    if (this.destroyPromise) return this.destroyPromise;
    this.destroyed = true;
    this.accepting = false;
    this.clearIdleRetirement();
    const ws = [...this.workers];
    this.idle = [];
    for (const job of [...this.inflight.values(), ...this.queue]) {
      this.settle(job, { isError: true, content: [{ type: 'text', text: 'codegraph is shutting down; retry shortly.' }] });
    }
    this.queue = [];
    this.destroyPromise = (async () => {
      if (this.inflight.size && !this.cleanupFailure) {
        let drain: NodeJS.Timeout | undefined;
        await Promise.race([new Promise<void>((resolve, reject) => this.quietWaiters.push({ resolve, reject })), new Promise<void>(resolve => {
          drain = setTimeout(resolve, QUERY_CLEANUP_BOUNDS.drainMs); drain.unref?.();
        })]).catch(() => { /* known incomplete retirement; still stop the other owned workers */ });
        clearTimeout(drain);
      }
      for (const worker of ws) this.retire(worker);
      const outcomes = await Promise.allSettled([...this.terminations]);
      const incomplete = outcomes.find((outcome): outcome is PromiseRejectedResult => outcome.status === 'rejected');
      if (incomplete) throw incomplete.reason;
      this.notifyQuiet();
    })();
    return this.destroyPromise;
  }
}
