/**
 * MCP shared engine — the heavyweight, *shared* state for an MCP server:
 * the project's {@link CodeGraph} instance, file watcher, and the
 * {@link ToolHandler} cache for cross-project queries.
 *
 * One engine, many sessions:
 * - direct mode (single stdio session) instantiates one engine + one session;
 * - daemon mode instantiates one engine and a new session per socket
 *   connection. Every session reads from the same SQLite WAL and the same
 *   inotify watch set — that's the entire point of issue #411.
 */

import * as os from 'os';
import * as path from 'path';
import { readFileSync, realpathSync } from 'fs';
import type CodeGraph from '../index';
import { admitMcpRoute, assertMcpIndexRoute, captureMcpIndexRoute, getCodeGraphDir, McpIndexRouteUnavailableError, resolveMcpRoot, type McpIndexRoute } from '../directory';
import { ToolHandler } from './tools';
import { WslSharedIndexError } from '../db/wsl-shared-index';
import { assertNoRebuild, decodeWriterLockInfo, getWriterPidPath, tryAcquireWriterLock, writerLockHeldMessage, type WriterLockInfo } from './writer-lock';
import { QueryPool } from './query-pool';
import { immutable, requestedQueryPolicy, resolveLegacyQueryPolicy, type QueryExecutionMode, type ResolvedQueryPolicy } from './query-policy';
import { createQueryExecutionContext, type QueryExecutionContext } from './query-context';
import { endFreshnessMeasurements } from './index-freshness';
import { acquireProject, admitQueryProject, assertProjectWriterReservation, retainStartupProjectWriter, rollbackStartupProjectWriter, type IndexProgressLease, type ProjectAcquisition, type ProjectLease, type ProjectResourceObserver, type ProjectWriterReservation } from './project-lifecycle';
import { CodeGraphPackageVersion } from './version';
import type { QueryDiagnostics } from './query-diagnostics';
import { isServicePolicyRefusal, servicePolicyRefusal, type ServicePolicyRefusal } from './query-service-compatibility';

/** How often the per-tool-call retry may re-run the sub-project down-scan. */
const RETRY_SUBSCAN_TTL_MS = 5_000;

export interface MCPEngineOptions {
  /** Internal selected-mode boundary; absent injection preserves legacy callers. */
  queryPolicy?: ResolvedQueryPolicy;
  /** Prospective shared-service policy from the same captured startup request. */
  queryAdmissionPolicy?: ResolvedQueryPolicy;
  /** Acknowledged canonical own route; identity only, never cached permission. */
  projectRoute?: McpIndexRoute;
  /** Actual outer native resource observation; never a permission verdict. */
  resourceObserver?: ProjectResourceObserver;
  /** Explicit caller-owned startup authority, released by that caller. */
  projectWriter?: ProjectWriterReservation;
  /** Primary direct MCP must own its default writer, including late resolution. */
  requireDefaultWriter?: boolean;
  onInitialDefaultOutcome?: (outcome: InitialDefaultOutcome) => void;
  executionMode?: QueryExecutionMode;
  /** Serve existing index contents without syncing, watching, or claiming a writer slot. */
  readOnly?: boolean;
  /**
   * Whether to start the file watcher when initializing. Daemon and direct
   * modes both want this true; tests may set it false to keep the engine
   * cheap. Honors {@link watchDisabledReason} regardless.
   */
  watch?: boolean;
  /**
   * Whether to off-load read-tool dispatch to a worker-thread pool. Both daemon
   * and direct sessions can issue concurrent calls on one event loop.
   * `CODEGRAPH_QUERY_POOL_SIZE=0` disables it in either mode.
   */
  queryPool?: boolean;
  /**
   * Worker cap when `CODEGRAPH_QUERY_POOL_SIZE` is unset. A direct (single-client)
   * session sets a small cap so every session doesn't hold one worker per core;
   * the shared daemon leaves it unset and scales with the machine.
   */
  queryPoolDefaultMax?: number;
  /**
   * Project root whose writer slot must be claimed synchronously before this
   * engine can open the graph. Used by proxy fallback to fence catch-up sync,
   * not just the later file watcher.
   */
  writerLockRoot?: string;
}

export type InitialDefaultOutcome = { readonly kind: 'ready'; readonly route: McpIndexRoute }
  | { readonly kind: 'no-route' } | { readonly kind: 'refused'; readonly refusal: ServicePolicyRefusal };

/**
 * Shared MCP engine. Thread-safe in the sense that multiple sessions can
 * call its methods concurrently — internally it serializes initialization
 * through a single promise so multiple sessions racing each other on first
 * connect never double-open the SQLite file.
 */
export class MCPEngine {
  private cg: CodeGraph | null = null;
  private toolHandler: ToolHandler;
  // Chosen project root. A supplied held route seeds identity before opening;
  // otherwise it remains null until `ensureInitialized` succeeds
  // (or null forever if no .codegraph/ ever turned up — that's a valid
  // state for the engine, since cross-project queries still work).
  private projectPath: string | null = null;
  // Set on first `ensureInitialized` so subsequent sessions don't redo work.
  private initPromise: Promise<void> | null = null;
  private readonly initializationLifetime = new AbortController();
  private readonly resourceLifetime = new AbortController();
  private defaultRoute: McpIndexRoute | null = null;
  // Provenance only: the existing defaultRoute remains the sole route identity.
  private readonly projectRouteSupplied: boolean;
  // Throttle for the retry path's sub-project down-scan (#1606) — the scan is
  // bounded but shouldn't run on every tool call in the no-default state.
  private lastRetrySubScanAt = 0;
  private initializationStarted = false;
  private initialDefaultPending = true;
  private readonly resourceObserver?: ProjectResourceObserver;
  private readonly onInitialDefaultOutcome?: MCPEngineOptions['onInitialDefaultOutcome'];
  private readonly requireDefaultWriter: boolean;
  private projectWriter: ProjectWriterReservation | null = null;
  private ownedStartupReservation: ProjectWriterReservation | null = null;
  // Sole cleanup accounting: exact holders enter before acquisition can await.
  // Published default/explicit graph references below are lookup state only.
  private readonly projectAcquisitions = new Set<ProjectAcquisition>();
  private explicitProjects = new Map<CodeGraph, Set<ProjectLease>>();
  private defaultLease: ProjectLease | null = null;
  private opts: Required<Pick<MCPEngineOptions, 'readOnly' | 'watch' | 'queryPool'>>;
  private readonly execution: QueryExecutionContext;
  private readonly queryAdmissionPolicy: ResolvedQueryPolicy;
  private closed = false;
  private stopPromise: Promise<void> | null = null;
  // Off-loop read-tool pool. Workers each hold their own WAL read connections;
  // sessions without a default index open projects lazily via projectPath.
  private queryPool: QueryPool | null = null;

  constructor(opts: MCPEngineOptions = {}) {
    this.projectRouteSupplied = opts.projectRoute !== undefined;
    this.resourceObserver = opts.resourceObserver;
    this.onInitialDefaultOutcome = opts.onInitialDefaultOutcome;
    const mode = opts.executionMode ?? opts.queryPolicy?.executionMode ?? (opts.readOnly ? 'read-only-fallback' : 'shared-daemon');
    this.opts = { readOnly: opts.readOnly ?? mode === 'read-only-fallback', watch: opts.watch ?? true, queryPool: opts.queryPool ?? false };
    this.requireDefaultWriter = opts.requireDefaultWriter ?? false;
    if (this.requireDefaultWriter && (mode !== 'direct' || this.opts.readOnly)) {
      throw new Error('Primary default writer ownership requires a read-write direct engine.');
    }
    if ((opts.queryPolicy && opts.queryPolicy.executionMode !== mode) || (mode === 'read-only-fallback') !== this.opts.readOnly) {
      throw new Error('Query policy execution mode and engine capability must agree.');
    }
    let policy = opts.queryPolicy, admission = opts.queryAdmissionPolicy;
    if (!policy || !admission) {
      // Legacy callers share one request/CPU snapshot; fully injected startup
      // must not recapture either input after its public boundary selected it.
      const requested = requestedQueryPolicy(process.env), cpuCount = os.cpus().length;
      policy ??= resolveLegacyQueryPolicy(requested, mode, cpuCount,
        { poolEnabled: this.opts.queryPool, defaultMaximum: opts.queryPoolDefaultMax });
      admission ??= resolveLegacyQueryPolicy(requested, 'shared-daemon', cpuCount);
    }
    if (admission.executionMode !== 'shared-daemon') throw new Error('Query admission requires a prospective shared-service policy.');
    this.queryAdmissionPolicy = immutable({ ...admission, requested: { ...admission.requested }, defaults: { ...admission.defaults } });
    this.execution = createQueryExecutionContext(policy, { readOnly: this.opts.readOnly, writer: !this.opts.readOnly, synchronization: !this.opts.readOnly });
    this.toolHandler = new ToolHandler(null, this.execution);
    if (opts.projectRoute) {
      const { root, index } = opts.projectRoute;
      this.defaultRoute = Object.freeze({ root, index });
      this.projectPath = root;
      this.toolHandler.setDefaultProjectHint(root);
    }
    this.toolHandler.setQueryDiagnostics(project => this.getQueryDiagnostics(project));
    this.toolHandler.setProjectLifecycle({
      resourceObserver: this.resourceObserver,
      admit: (route, signal) => admitQueryProject(route.root, this.queryAdmissionPolicy, signal),
      open: async (route, signal) => {
        this.initializationStarted = true;
        const acquisition = signal ? AbortSignal.any([signal, this.initializationLifetime.signal]) : this.initializationLifetime.signal;
        let retained: ProjectAcquisition | null = null, lease: ProjectLease;
        try {
          lease = await acquireProject({ route, signal: acquisition, lifetime: this.resourceLifetime.signal,
            intent: this.opts.readOnly || !this.opts.watch ? 'read' : 'watch', watchOptions: this.watchOptions(),
            resourceObserver: this.resourceObserver,
            retain: holder => { retained = holder; this.projectAcquisitions.add(holder); },
            admit: (selected, lifetime) => admitQueryProject(selected.root, this.queryAdmissionPolicy, lifetime) });
        } catch (primary) {
          // acquireProject already disposed this same holder. Join its cached
          // disposition to remove only proved cleanup; retain any rejection.
          if (retained) {
            try { await this.releaseProjectAcquisition(retained); }
            catch (secondary) { this.reportProjectCleanup(primary, secondary); }
          }
          throw primary;
        }
        if (this.closed || signal?.aborted) {
          const primary = new Error('Project acquisition cancelled'); primary.name = 'AbortError';
          try { await this.releaseProjectAcquisition(lease.acquisition); }
          catch (secondary) { this.reportProjectCleanup(primary, secondary); }
          throw primary;
        }
        const leases = this.explicitProjects.get(lease.cg) ?? new Set<ProjectLease>();
        leases.add(lease); this.explicitProjects.set(lease.cg, leases);
        this.maybeStartPool(null);
        return lease.cg;
      },
      activate: (cg) => this.leaseFor(cg)?.ready() ?? Promise.resolve(),
      // ToolHandler returns cache ownership during shutdown; the engine keeps
      // these same leases until every native worker/tool drain has joined.
      release: (cg) => this.closed ? Promise.resolve() : this.releaseExplicitProject(cg),
      watchState: (cg) => this.leaseFor(cg)?.watchState(),
      rearmWatcher: (cg) => this.leaseFor(cg)?.rearmWatcher() ?? false,
    });
    // A tool call found the default project's database replaced on disk (a
    // `codegraph index` rebuild) and reopened it (#1902). Reconcile the new
    // file with the usual catch-up — `sync()` serializes on the index mutex,
    // so it never overlaps an in-flight watcher sync. The actual project lease
    // decides ownership: default watch:false still owns one catch-up, while a
    // read-only engine or explicit watch:false lease cannot start writing.
    this.toolHandler.setOnDatabaseReopened((cg) => {
      const lease = this.leaseFor(cg);
      if (lease && !this.opts.readOnly) this.toolHandler.setCatchUpGate(lease.ready(true));
    });
    if (opts.projectWriter) this.setProjectWriter(opts.projectWriter);
    if (opts.writerLockRoot && !this.opts.readOnly) {
      if (this.projectWriter) throw new Error('Startup writer must have one explicit owner.');
      const route = this.defaultRoute ?? captureMcpIndexRoute(opts.writerLockRoot);
      if (!route || route.root !== realpathSync(opts.writerLockRoot)) throw new Error('Startup writer requires its exact own index route.');
      this.ownedStartupReservation = this.acquireOwnedStartupWriter(route, 'fallback');
      this.setProjectWriter(this.ownedStartupReservation);
    }
  }

  /** Exact synchronous acquisition shared by legacy fallback and late direct startup. */
  private acquireOwnedStartupWriter(route: McpIndexRoute, mode: 'direct' | 'fallback'): ProjectWriterReservation {
    assertNoRebuild(route.root);
    const pidPath = getWriterPidPath(route.root);
    const locator = path.join(realpathSync(path.dirname(pidPath)), path.basename(pidPath));
    let previous: WriterLockInfo | null = null;
    try {
      previous = decodeWriterLockInfo(readFileSync(locator, 'utf8'));
      if (!previous) throw new Error('The previous native writer record is malformed.');
    } catch (error) { if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error; }
    const writer = tryAcquireWriterLock(route.root, mode);
    if (writer.kind === 'taken') throw new Error(writerLockHeldMessage(writer.existing, writer.pidPath));
    try { return retainStartupProjectWriter(route, writer.info); }
    catch (primary) {
      try { rollbackStartupProjectWriter(route, writer, { pidPath, locator, previous }); }
      catch (secondary) { this.reportProjectCleanup(primary, secondary); }
      throw primary;
    }
  }

  /**
   * Start the worker-thread query pool after resolving the default project
   * (which may be absent). A configured pool remains authoritative through
   * startup and recovery; worker failures settle through its capacity contract.
   */
  private maybeStartPool(root: string | null): void {
    if (!this.opts.queryPool || this.queryPool || this.closed) return;
    const concurrency = this.execution.policy.concurrency;
    if (concurrency.kind === 'disabled') {
      process.stderr.write(this.opts.readOnly
        ? '[CodeGraph MCP] Local read-only fallback uses main-thread reads unless pooling is explicitly selected.\n'
        : '[CodeGraph MCP] Query pool disabled (CODEGRAPH_QUERY_POOL_SIZE=0); serving reads in-process.\n');
      return;
    }
    this.initializationStarted = true;
    this.queryPool = new QueryPool({ root, projectRoute: root ? this.defaultRoute ?? undefined : undefined, execution: this.execution, warmWorker: false });
    this.toolHandler.setQueryPool(this.queryPool);
    const capacity = concurrency.kind === 'finite' ? `up to ${concurrency.maximum} worker thread(s)` : 'on-demand worker threads with no configured maximum';
    process.stderr.write(`[CodeGraph MCP] Query pool: ${capacity} for concurrent reads${this.opts.readOnly ? ' (local read-only fallback; no writer or auto-sync ownership)' : ''}.\n`);
  }

  /**
   * Convenience for {@link MCPServer} compatibility: pre-seed an explicit
   * project path (from the `--path` CLI flag) without yet opening it. This
   * keeps the synchronous constructor cheap; the actual open happens on the
   * first `ensureInitialized` call.
   */
  setProjectPathHint(projectPath: string): void {
    if ((this.projectRouteSupplied || this.projectWriter) && projectPath !== this.defaultRoute?.root) {
      throw new Error('Cannot replace a held project route with a path hint.');
    }
    this.projectPath = projectPath;
    this.toolHandler.setDefaultProjectHint(projectPath);
  }

  /** Attach an actual startup owner's lifetime once, before any engine work. */
  setProjectWriter(reservation: ProjectWriterReservation): void {
    if (this.initializationStarted || this.closed || this.projectWriter) {
      throw new Error('Startup writer must be attached exactly once before engine activity.');
    }
    assertProjectWriterReservation(reservation);
    const route = reservation.route;
    if (this.defaultRoute && (this.defaultRoute.root !== route.root || this.defaultRoute.index !== route.index)) {
      throw new Error('Startup writer route differs from the held engine route.');
    }
    if (!this.defaultRoute) {
      if (this.projectPath && this.projectPath !== route.root) throw new Error('Startup writer cannot override the selected project hint.');
      // Legacy Daemon.start captures its route after construction. Binding this
      // opaque owner's identity performs no discovery or permission work.
      this.defaultRoute = Object.freeze({ root: route.root, index: route.index });
      this.projectPath = route.root;
      this.toolHandler.setDefaultProjectHint(route.root);
    }
    this.projectWriter = reservation;
  }

  /** Whether this engine only reads: no watcher, no sync, no writer slot. */
  isReadOnly(): boolean {
    return this.opts.readOnly;
  }

  /** Internal resolved request, capability and ownership identity; no public diagnostic claim. */
  getQueryExecutionContext(): QueryExecutionContext { return this.execution; }

  /** Owner observation only: no initialization, graph opening or pool demand. */
  getQueryDiagnostics(project?: QueryDiagnostics['project']): QueryDiagnostics {
    const { requested, ...effective } = this.execution.policy;
    const enabled = effective.concurrency.kind !== 'disabled' && this.opts.queryPool;
    const pool = this.queryPool?.observation;
    const stopped = this.closed;
    let resolvedProject = project ?? null;
    const defaultRoot = this.cg?.getProjectRoot() ?? this.projectPath;
    if (project === undefined && this.defaultRoute) {
      resolvedProject = { route: 'default', ...this.defaultRoute };
    } else if (project === undefined && defaultRoot) {
      // A daemon can send its hello before asynchronous graph initialization.
      // Resolve only its own existing index; absence remains unknown, with no
      // graph open, ancestor substitution or initialization for diagnostics.
      try { resolvedProject = { route: 'default', root: realpathSync(defaultRoot),
        index: realpathSync(path.join(getCodeGraphDir(defaultRoot), 'codegraph.db')) }; } catch { /* unknown identity */ }
    }
    return immutable({ schemaVersion: 1,
      implementation: { packageVersion: CodeGraphPackageVersion, policySchemaVersion: 1,
        support: { independentControls: true,
          combinedPreset: effective.source === 'strict',
          idleRetirement: true } },
      requested, effective,
      ownership: { writerAuthority: this.execution.ownership.writer, synchronizationAuthority: this.execution.ownership.synchronization,
        capability: this.execution.capability,
        resourceScope: effective.executionMode === 'shared-daemon' ? 'shared-project' : 'local-engine', pid: process.pid, generation: this.execution.generation },
      lifecycle: pool ? { ...pool, dispatch: stopped || pool.state === 'unavailable' || pool.state === 'draining' ? 'unavailable' : 'worker' }
        : { state: stopped ? 'stopped' : enabled ? 'cold' : 'disabled', dispatch: stopped ? 'unavailable' : enabled ? 'worker' : 'main-thread',
          ready: !stopped && !enabled, workers: 0, queued: 0, executing: 0, starting: 0, retiring: 0, reason: null },
      project: resolvedProject });
  }

  /** Session disappearance never stops shared peers, only that session's jobs. */
  cancelQuerySession(sessionId: string): void { this.queryPool?.cancelSession(sessionId); }

  /** Routine fallback handover waits indefinitely for admitted healthy execution. */
  async drain(): Promise<void> {
    this.closed = true; // no initialization/pool resurrection during handover
    this.beginResourceDrain();
    if (this.initPromise) await this.initPromise;
    await this.queryPool?.drainNaturally();
    await this.stop();
  }

  /** Internal construction facts for later diagnostics; resolution is not uptake. */
  getQueryExecutionState() {
    const pool = this.queryPool;
    const pooled = !!pool?.healthy;
    return immutable({ execution: this.execution, actual: {
      executionMode: this.execution.policy.executionMode,
      resourceScope: this.execution.policy.executionMode === 'shared-daemon' ? 'shared-project' as const : 'local-engine' as const,
      dispatch: pool ? pooled ? 'worker' as const : 'unavailable' as const : 'main-thread' as const,
      lifecycle: pool?.state ?? 'disabled', ready: pool?.ready ?? false,
      concurrency: pooled ? this.execution.policy.concurrency : { kind: 'disabled' as const },
      deadline: pooled ? this.execution.policy.deadline : { kind: 'inapplicable' as const },
      retention: pool ? this.execution.policy.retention : 'ordinary' as const, workers: pool?.liveWorkers ?? 0,
    } });
  }

  /** Chosen root, possibly held before initialization; no open/permission claim. */
  getProjectPath(): string | null {
    return this.projectPath;
  }

  /** Shared ToolHandler — sessions delegate tool dispatch through this. */
  getToolHandler(): ToolHandler {
    return this.toolHandler;
  }

  /** Whether the default project's CodeGraph is open. */
  hasDefaultCodeGraph(): boolean {
    return this.toolHandler.hasDefaultCodeGraph();
  }

  /** Truthful fast handshake hint; filesystem checks cannot open SQLite. */
  hasVerifiedDefaultRoute(): boolean {
    if (!this.defaultRoute || !this.toolHandler.hasDefaultCodeGraph()) return false;
    try { assertMcpIndexRoute(this.defaultRoute); return true; } catch { return false; }
  }

  /**
   * Walk up from `searchFrom` to find the nearest `.codegraph/` and open it.
   * Idempotent: concurrent callers share one in-flight init; subsequent
   * callers after success are no-ops.
   *
   * The original `MCPServer.tryInitializeDefault` carried the same retry-on-
   * subsequent-tool-call semantics; we preserve them by NOT throwing when the
   * search misses (just leaves `cg` null so the next call can retry).
   */
  async ensureInitialized(searchFrom: string): Promise<void> {
    await this.initialize(searchFrom, true);
  }

  /** Late default recovery still performs mandatory asynchronous admission. */
  async retryInitialize(searchFrom: string): Promise<void> {
    const scanDue = Date.now() - this.lastRetrySubScanAt >= RETRY_SUBSCAN_TTL_MS;
    if (scanDue) this.lastRetrySubScanAt = Date.now();
    await this.initialize(searchFrom, scanDue);
  }

  private async initialize(searchFrom: string, subprojectScan: boolean): Promise<void> {
    if (this.closed || this.toolHandler.hasDefaultCodeGraph()) return;
    if (this.initPromise) return this.initPromise;
    this.initializationStarted = true;
    const pending = this.doInitialize(searchFrom, subprojectScan);
    this.initPromise = pending;
    try { await pending; }
    finally { if (this.initPromise === pending) this.initPromise = null; }
  }

  /**
   * Close everything. Used on graceful daemon shutdown (SIGTERM/idle timeout)
   * and on direct-mode stop. Idempotent.
   */
  stop(): Promise<void> {
    if (this.stopPromise) return this.stopPromise;
    this.closed = true;
    this.beginResourceDrain();

    // Detach + terminate the worker pool first so no tool call routes to a
    // worker mid-teardown; outstanding pool calls resolve with graceful guidance.
    // Stopping waits for the workers to end — the pool's, and any
    // `codegraph_status` change count still measuring: the daemon exits right
    // after, and exiting while a worker is still starting up can crash the
    // process.
    this.toolHandler.setQueryPool(null);
    const stoppingPool = this.queryPool;
    const poolDown = stoppingPool ? stoppingPool.destroy().then(() => {
      if (this.queryPool === stoppingPool) this.queryPool = null;
    }) : Promise.resolve();
    const measurementsDown = endFreshnessMeasurements();
    let drained: Promise<void>;
    try { drained = this.toolHandler.closeAll(); } catch (error) { drained = Promise.reject(error); }
    this.stopPromise = Promise.allSettled([drained, poolDown, measurementsDown]).then(async results => {
      const failures = results.flatMap(result => result.status === 'rejected' ? [result.reason] : []);
      if (this.initPromise) {
        try { await this.initPromise; } catch { /* refusal/cancellation already belongs to its caller */ }
      }
      // An already-running status call may have created a worker after the
      // first snapshot. Once tool calls joined, the final snapshot is stable.
      let measurementIncomplete = results[2].status === 'rejected';
      try { await endFreshnessMeasurements(); }
      catch (error) { failures.push(error); measurementIncomplete = true; }
      if (results[1].status === 'rejected' || measurementIncomplete || this.toolHandler.hasUnjoinedProjectWork()) {
        // A logical stop is not proof that query/measurement workers or tool
        // work ended. Their real teardown promises own that evidence.
        // Retain draining project leases, observation, and startup authority;
        // the outer owner's existing incomplete-shutdown boundary remains.
        if (this.toolHandler.hasUnjoinedProjectWork()) failures.push(new Error('MCP tool work remains unjoined'));
        throw new AggregateError(failures, 'MCP engine native drain is incomplete; project resources retained');
      }
      let projectCleanupIncomplete = false;
      // This includes failed acquisitions and unpublished graphs. Neither a
      // request rejection nor an absent cache entry proves their native drain.
      for (const holder of [...this.projectAcquisitions]) {
        try { await this.releaseProjectAcquisition(holder); }
        catch (error) { failures.push(error); projectCleanupIncomplete = true; }
      }
      // Legacy fallback and late direct startup own this reservation. Explicit startup callers
      // release their own reservation after the engine's real native drain.
      if (!projectCleanupIncomplete) {
        try { await this.ownedStartupReservation?.release(); this.ownedStartupReservation = null; }
        catch (error) { failures.push(error); projectCleanupIncomplete = true; }
      }
      if (!projectCleanupIncomplete) {
        this.defaultLease = null; this.explicitProjects.clear();
        this.cg = null; this.resourceLifetime.abort();
      }
      if (failures.length) throw new AggregateError(failures, 'MCP engine resource cleanup failed');
    });
    return this.stopPromise;
  }

  private async releaseExplicitProject(cg: CodeGraph): Promise<void> {
    const leases = this.explicitProjects.get(cg);
    if (!leases) return;
    const snapshot = [...leases];
    const results = await Promise.allSettled(snapshot.map(lease => this.releaseProjectAcquisition(lease.acquisition)));
    // A new cache owner can join this same reader while old cleanup awaits.
    // Return only successfully released snapshot holders, never its new lease.
    snapshot.forEach((lease, index) => { if (results[index]?.status === 'fulfilled') leases.delete(lease); });
    if (this.explicitProjects.get(cg) === leases && leases.size === 0) this.explicitProjects.delete(cg);
    const failures = results.flatMap(result => result.status === 'rejected' ? [result.reason] : []);
    if (failures.length) throw new AggregateError(failures, 'Explicit project cleanup failed');
  }

  private async releaseProjectAcquisition(holder: ProjectAcquisition): Promise<void> {
    await holder.release();
    this.projectAcquisitions.delete(holder);
  }

  private reportProjectCleanup(primary: unknown, secondary: unknown): void {
    process.stderr.write(`[CodeGraph MCP] Project cleanup after ${String(primary).slice(0, 2048)}: ${String(secondary).slice(0, 2048)}\n`);
  }

  private leaseFor(cg: CodeGraph): ProjectLease | undefined {
    return this.defaultLease?.cg === cg ? this.defaultLease : this.explicitProjects.get(cg)?.values().next().value;
  }

  private beginResourceDrain(): void {
    this.initializationLifetime.abort();
    for (const holder of this.projectAcquisitions) holder.beginDrain();
  }

  private notifyInitialDefault(outcome: InitialDefaultOutcome): void {
    if (!this.initialDefaultPending || this.closed || this.initializationLifetime.signal.aborted) return;
    this.initialDefaultPending = false;
    this.onInitialDefaultOutcome?.(outcome);
  }

  /** Watch options shared by the default project and explicit projects. */
  private watchOptions(): Parameters<CodeGraph['watch']>[0] {
    // Optional override for the debounce window via env var (issue #403).
    // Useful for workspaces with bursty writes (formatter-on-save chains,
    // large generated outputs) where the 2s default fires too often. Clamped
    // to [100ms, 60s]; out-of-range / non-numeric values fall back to the
    // FileWatcher default. We log the active value so it's discoverable.
    const debounceMs = parseDebounceEnv(process.env.CODEGRAPH_WATCH_DEBOUNCE_MS);
    if (debounceMs !== undefined) {
      process.stderr.write(`[CodeGraph MCP] File watcher debounce: ${debounceMs}ms (CODEGRAPH_WATCH_DEBOUNCE_MS)\n`);
    }
    return {
      debounceMs,
      onSyncComplete: (result) => {
        if (result.filesChanged > 0) {
          process.stderr.write(
            `[CodeGraph MCP] Auto-synced ${result.filesChanged} file(s) in ${result.durationMs}ms\n`
          );
        }
      },
      onSyncError: (err) => {
        process.stderr.write(`[CodeGraph MCP] Auto-sync error: ${err.message}\n`);
      },
      onDegraded: (reason) => {
        // Live watching gave up permanently (watch-resource exhaustion or a
        // write lock held past the retry budget). Say so loudly and ONCE — the
        // graph will no longer auto-update, so a long-running MCP session must
        // not keep assuming it's fresh. The reason already names the remedy
        // (`codegraph sync` / git sync hooks).
        process.stderr.write(`[CodeGraph MCP] File watcher degraded — ${reason}\n`);
      },
    };
  }

  private async doInitialize(searchFrom: string, subprojectScan: boolean): Promise<void> {
    this.toolHandler.setDefaultProjectHint(this.defaultRoute?.root ?? searchFrom);
    const signal = this.initializationLifetime.signal;
    const progress = new Map<string, IndexProgressLease>();
    const options = { signal, subprojectScan,
      admit: async (route: McpIndexRoute, lifetime?: AbortSignal): Promise<void> => {
        if (this.resourceObserver && !progress.has(route.index)) {
          progress.set(route.index, await this.resourceObserver.acquireIndexProgress(route, lifetime));
        }
        // The observer may replace native supervision asynchronously. Permission
        // is freshly obtained afterwards, before the resolver's schema probe.
        await admitQueryProject(route.root, this.queryAdmissionPolicy, lifetime);
      } };
    let resolvedRoot: string | undefined;
    let acquired: ProjectLease | null = null;
    let retained: ProjectAcquisition | null = null;
    try {
      const res = this.defaultRoute
        ? { route: await admitMcpRoute(this.defaultRoute, options), viaSubScan: false, candidates: [] }
        : await resolveMcpRoot(searchFrom, options);
      resolvedRoot = res.route?.root;
      if (!resolvedRoot) {
        if (signal.aborted || this.closed) return;
        this.projectPath = searchFrom;
        this.toolHandler.setKnownSubprojects([...res.candidates], searchFrom);
        process.stderr.write(
          `[CodeGraph MCP] No .codegraph/ at or above ${searchFrom}: no default project, live sync disabled until an indexed project is accessed via projectPath.\n`
        );
        if (res.candidates.length > 0) {
          const rels = res.candidates.map(c => path.relative(searchFrom, c) || '.');
          process.stderr.write(`[CodeGraph MCP] Indexed sub-projects found: ${rels.join(', ')}. Pass projectPath per call, or launch with --path.\n`);
        }
        this.maybeStartPool(null);
        this.notifyInitialDefault({ kind: 'no-route' });
        return;
      }
      if (res.viaSubScan) this.logSubprojectAdoption(searchFrom, resolvedRoot);
      this.defaultRoute = res.route;
      this.projectPath = resolvedRoot;
      // Finish async grammar preparation, then refresh permission and hold the
      // same own route through the synchronous facade's schema/open checks.
      await (require('../extraction') as typeof import('../extraction')).initGrammars();
      if (signal.aborted || this.closed) return;
      await admitMcpRoute(this.defaultRoute!, options);
      if (signal.aborted || this.closed) return;
      assertMcpIndexRoute(this.defaultRoute!);
      assertNoRebuild(resolvedRoot);
      if (this.requireDefaultWriter && !this.projectWriter) {
        try {
          const reservation = this.acquireOwnedStartupWriter(this.defaultRoute!, 'direct');
          // Initialization owns this exact route already; the public pre-start
          // setter fence stays strict. Retain authority across every later wait.
          this.ownedStartupReservation = reservation;
          this.projectWriter = reservation;
        } catch (error) {
          if (isServicePolicyRefusal(error)) throw error;
          throw servicePolicyRefusal('unknown', resolvedRoot,
            error instanceof Error ? error.message : String(error));
        }
      }
      acquired = await acquireProject({ route: this.defaultRoute!, signal, lifetime: this.resourceLifetime.signal,
        intent: this.opts.readOnly ? 'read' : this.opts.watch ? 'watch' : 'catch-up',
        watchOptions: this.watchOptions(), resourceObserver: this.resourceObserver,
        startupOwner: this.projectWriter ?? undefined,
        retain: holder => { retained = holder; this.projectAcquisitions.add(holder); },
        admit: (route, lifetime) => admitQueryProject(route.root, this.queryAdmissionPolicy, lifetime) });
      if (signal.aborted || this.closed) return;
      // Lease acquisition includes owner/freshness waits. Neither the original
      // route nor a previous permit authorizes publication after those waits.
      await admitMcpRoute(this.defaultRoute!, options);
      if (signal.aborted || this.closed) return;
      assertMcpIndexRoute(this.defaultRoute!); assertNoRebuild(resolvedRoot);
      this.defaultLease = acquired; acquired = null;
      retained = null; // published lookup; the same holder remains in engine ownership
      this.cg = this.defaultLease.cg;
      this.toolHandler.setDefaultCodeGraph(this.cg, this.defaultRoute!);
      this.toolHandler.setCatchUpGate(this.defaultLease.ready());
      this.maybeStartPool(resolvedRoot);
      this.notifyInitialDefault({ kind: 'ready', route: this.defaultRoute! });
    } catch (err) {
      if (signal.aborted || this.closed) return;
      if (isServicePolicyRefusal(err)) {
        // One actual default attempt owns this outcome; explicit beta routing
        // and concurrent waiters do not acquire a second primary latch.
        try { this.notifyInitialDefault({ kind: 'refused', refusal: err }); }
        catch (secondary) { process.stderr.write(`[CodeGraph MCP] Default refusal notification failed: ${String(secondary).slice(0, 2048)}\n`); }
        throw err;
      }
      const msg = err instanceof Error ? err.message : String(err);
      process.stderr.write(`[CodeGraph MCP] Failed to open project at ${resolvedRoot ?? searchFrom}: ${msg}\n`);
      this.toolHandler.setDefaultOpenFailure(err instanceof WslSharedIndexError || err instanceof McpIndexRouteUnavailableError ? err : null);
      if (err instanceof McpIndexRouteUnavailableError) return;
      this.maybeStartPool(null);
    } finally {
      // Temporary resolver observation overlaps the lasting project lease, so
      // there is no gap in native progress tracking before publication/drain.
      if (retained) {
        try { await this.releaseProjectAcquisition(retained); }
        catch (secondary) { process.stderr.write(`[CodeGraph MCP] Unpublished project cleanup failed: ${String(secondary).slice(0, 2048)}\n`); }
      }
      for (const lease of progress.values()) {
        try { await lease.release(); }
        catch (secondary) { process.stderr.write(`[CodeGraph MCP] Resolver progress cleanup failed: ${String(secondary).slice(0, 2048)}\n`); }
      }
    }
  }

  /** One stderr line when the default project came from the down-scan (#1606). */
  private logSubprojectAdoption(searchFrom: string, root: string): void {
    const rel = path.relative(searchFrom, root) || root;
    process.stderr.write(
      `[CodeGraph MCP] No .codegraph/ at ${searchFrom}; adopted the single indexed sub-project ${rel} as the default project.\n`
    );
  }

}

/**
 * Parse and clamp the CODEGRAPH_WATCH_DEBOUNCE_MS env override.
 *
 * Issue #403: workspaces with bursty writes (formatter-on-save, multi-file
 * refactors) sometimes want a longer quiet window before sync. Returns
 * `undefined` for unset / empty / non-numeric / out-of-range values so the
 * FileWatcher default (2000ms) takes over — never throws.
 *
 * Clamp range: 100ms (faster would mean a sync per keystroke) to 60s (longer
 * and the watcher feels broken). Out-of-range values are treated as "ignore
 * this misconfiguration" rather than capped, since silently capping a 0 or
 * a typoed value would mask a real config bug.
 */
export function parseDebounceEnv(raw: string | undefined): number | undefined {
  if (!raw || !raw.trim()) return undefined;
  const n = Number(raw);
  if (!Number.isFinite(n) || !Number.isInteger(n)) return undefined;
  if (n < 100 || n > 60000) return undefined;
  return n;
}
