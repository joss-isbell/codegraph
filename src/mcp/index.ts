/**
 * CodeGraph MCP Server
 *
 * Model Context Protocol server that exposes CodeGraph functionality
 * as tools for AI assistants like Claude.
 *
 * @module mcp
 *
 * @example
 * ```typescript
 * import { MCPServer } from 'codegraph';
 *
 * const server = new MCPServer('/path/to/project');
 * await server.start();
 * ```
 *
 * Runtime modes (decided in {@link MCPServer.start}):
 *
 * - **Direct** — one process serves one MCP client over stdio. The pre-#411
 *   behavior; used when the user opts out (`CODEGRAPH_NO_DAEMON=1`), no
 *   `.codegraph/` is reachable. Primary policy refusal remains terminal.
 * - **Proxy** — what an MCP host actually talks to when sharing is on: a thin
 *   stdio↔socket pipe to the shared daemon. The proxy carries the #277 PPID
 *   watchdog, so a SIGKILL'd host reaps its proxy promptly. See {@link ./proxy.ts}.
 * - **Daemon** — a *detached* background process (its own session/process
 *   group) that serves N proxies over a Unix-domain socket / named pipe,
 *   sharing one CodeGraph + watcher + SQLite handle. Spawned on demand; never a
 *   child of any host, so it survives individual sessions and is reaped by
 *   client-refcount + idle timeout. See {@link ./daemon.ts} and issue #411.
 *
 * The detached-daemon + always-proxy split is the fix for the review finding
 * that the original in-process daemon (a) was the first host's child, so closing
 * that terminal severed every other client, and (b) disabled the PPID watchdog,
 * regressing #277 (orphaned daemons on host SIGKILL).
 */

import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { performance } from 'perf_hooks';
import { spawn, StdioOptions } from 'child_process';
import { getCodeGraphDir, codeGraphDirNameFor, resolveMcpRoot, admitMcpRoute,
  captureMcpIndexRoute, assertMcpIndexRoute, type McpIndexRoute } from '../directory';
import type { JsonRpcTransport } from './transport';
import { MCPEngine } from './engine';
import { MCPSession } from './session';
import { immutable, resolveQueryPolicy, type QueryStartupRequest,
  type QueryExecutionMode, type ResolvedQueryPolicy } from './query-policy';
import { captureMcpQueryStartup, daemonQueryEnvironment } from './query-startup-transport';
import { QueryPolicyRefusalError, isQueryPolicyRefusal } from './query-policy-refusal';
import { awaitDaemonBootstrap, confirmDaemonBootstrap, reportDaemonBootstrapRefusal,
  type DaemonBootstrapResult } from './query-bootstrap';
import { admitQueryProject, retainStartupProjectWriter, rollbackStartupProjectWriter, type ProjectWriterReservation,
  type IndexProgressLease } from './project-lifecycle';
import { assertNoLiveQueryDaemon, readQueryServiceOwners, processPresence } from './query-service-owner';
import {
  Daemon,
  clearStaleDaemonLock,
  installChangedReason,
  isProcessAlive,
  tryAcquireDaemonLock,
} from './daemon';
import { clearStaleDaemonArtifacts } from './daemon-registry';
import { connectWithHello, closeOwnedDaemonSocket, runLocalHandshakeProxy, type LocalHandshakeDeps,
  type PreparedLocalHandshake, type LocalHandshakePreparationControls } from './proxy';
import { CodeGraphPackageJsonPath, CodeGraphPackageVersion } from './version';
import {
  readWriterLock,
  getWriterPidPath,
  decodeWriterLockInfo,
  assertNoRebuild,
  tryAcquireWriterLock,
  writerLockHeldMessage,
  type WriterLockInfo,
} from './writer-lock';
import {
  canProbeDaemonIdentity,
  decodeLockInfo,
  getDaemonPidPath,
  getDaemonSocketCandidates,
  probeDaemonIdentity,
} from './daemon-paths';
import { getTelemetry } from '../telemetry';
import { checkForUpdateInBackground } from '../upgrade/update-check';
import { installMainThreadWatchdog, WatchdogHandle } from './liveness-watchdog';
import { HOST_PPID_ENV } from '../extraction/wasm-runtime-flags';

/**
 * Env var that marks a process as the *detached daemon* itself (set by
 * {@link spawnDetachedDaemon} when it re-invokes the CLI). Without it a
 * `serve --mcp` invocation is a launcher that connects-or-spawns; with it, the
 * process IS the daemon and must never try to spawn another (infinite spawn).
 */
const DAEMON_INTERNAL_ENV = 'CODEGRAPH_DAEMON_INTERNAL';

/**
 * Env var naming the launcher that holds the project's writer slot for the
 * daemon it spawns, after stopping an older one (#2335). That daemon takes the
 * slot over instead of finding it held.
 */
const DAEMON_HANDOVER_ENV = 'CODEGRAPH_DAEMON_HANDOVER';

/**
 * Retries for the detached daemon arbitrating the O_EXCL lock against a racing
 * sibling. Tiny — the lock resolves on the first round in practice; the retries
 * only cover clearing a genuinely stale (dead-pid) lockfile.
 */
const TAKEOVER_MAX_RETRIES = 5;
const TAKEOVER_RETRY_DELAY_MS = 100;

/**
 * How long a daemon the writer slot is held for (#2335) waits out a racing
 * candidate that took the daemon lock first. That candidate cannot get the
 * slot, so it leaves; this bounds a wait that never ends.
 */
const HANDOVER_CANDIDATE_WAIT_MS = 5_000;

/**
 * A fallback that serves reads without a watcher or a writer lock (#1963).
 * Say so on stderr: otherwise a session that quietly stopped syncing looks
 * the same as a healthy one in the logs.
 */
function selectedQueryPolicy(startup: QueryStartupRequest, mode: QueryExecutionMode): ResolvedQueryPolicy {
  return resolveQueryPolicy(startup.requested, mode, startup.cpuCount);
}

function cancelled(signal?: AbortSignal): void {
  if (signal?.aborted) throw signal.reason ?? new Error('Query startup was cancelled.');
}

function unknownStartup(root: string, detail: string): QueryPolicyRefusalError {
  return new QueryPolicyRefusalError('unknown', root, 'Inspect project ownership and startup before retrying.', detail);
}

function assertStartupRoute(route: McpIndexRoute): void {
  try { assertMcpIndexRoute(route); }
  catch (error) { throw unknownStartup(route.root,
    `The primary held index route is unavailable: ${error instanceof Error ? error.message : String(error)}`); }
}

interface StartupWriterObservation {
  readonly pidPath: string;
  readonly locator: string;
  readonly previous: WriterLockInfo | null;
}

/** The own DB already exists: observe exact native authority before acquisition. */
function observeStartupWriter(route: McpIndexRoute): StartupWriterObservation {
  const pidPath = getWriterPidPath(route.root);
  const locator = path.join(fs.realpathSync(path.dirname(pidPath)), path.basename(pidPath));
  let previous: WriterLockInfo | null = null;
  try {
    previous = decodeWriterLockInfo(fs.readFileSync(locator, 'utf8'));
    if (!previous) throw unknownStartup(route.root, 'The previous native writer record is malformed.');
  } catch (error) { if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') throw error; }
  return { pidPath, locator, previous };
}

function retainObservedStartupWriter(route: McpIndexRoute, observed: StartupWriterObservation,
  acquired: Extract<ReturnType<typeof tryAcquireWriterLock>, { kind: 'acquired' }>): ProjectWriterReservation {
  try { return retainStartupProjectWriter(route, acquired.info); }
  catch (primary) {
    // The lifecycle coordinator retains unresolved native authority in its
    // existing entry; there is no raw-unlink fallback after its rejection.
    try { rollbackStartupProjectWriter(route, acquired, observed); }
    catch (cleanup) { reportStartupCleanup('unretained startup writer rollback', cleanup); }
    throw primary;
  }
}

function readOnlyFallback(holder: string, startup: QueryStartupRequest, prospective: ResolvedQueryPolicy,
  route: McpIndexRoute, controls: LocalHandshakePreparationControls): MCPEngine {
  const queryPolicy = selectedQueryPolicy(startup, 'read-only-fallback');
  process.stderr.write(`[CodeGraph MCP] Serving reads in-process without auto-sync: ${holder}; local read-only fallback resources are not shared across chats.\n`);
  return new MCPEngine({ readOnly: true, queryPool: true, queryPolicy, queryAdmissionPolicy: prospective,
    projectRoute: route, executionMode: 'read-only-fallback', resourceObserver: controls.resourceObserver,
    onInitialDefaultOutcome: controls.onInitialDefaultOutcome });
}

/**
 * Create an in-process fallback only when it cannot conflict with a live
 * legacy daemon. Plain-PID locks cannot prove daemon identity, but they still
 * prove that a process owns the legacy writer slot.
 */
function makeFallbackEngine(route: McpIndexRoute, startup: QueryStartupRequest, prospective: ResolvedQueryPolicy,
  controls: LocalHandshakePreparationControls, retain: (reservation: ProjectWriterReservation) => void): MCPEngine {
  const root = route.root;
  assertStartupRoute(route);
  assertNoLiveQueryDaemon(root);
  assertNoRebuild(root);
  let existing: ReturnType<typeof decodeLockInfo> = null;
  try {
    existing = decodeLockInfo(fs.readFileSync(getDaemonPidPath(root), 'utf8'));
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code !== 'ENOENT') {
      throw unknownStartup(root, `The daemon lock could not be read (${code ?? 'unknown error'}); refusing an in-process fallback.`);
    }
  }
  if (
    existing &&
    isProcessAlive(existing.pid) &&
    !canProbeDaemonIdentity(existing)
  ) {
    throw unknownStartup(root, `Cannot start an in-process fallback while live legacy daemon pid ${existing.pid} holds the project lock.`);
  }
  const writer = readWriterLock(root);
  if (writer && writer.pid > 0 && isProcessAlive(writer.pid)) {
    // Another process owns updates. A fallback may still serve read-only WAL
    // queries without claiming a second writer or starting a watcher (#1963).
    return readOnlyFallback(`writer lock held by PID ${writer.pid} (${writer.mode} mode)`, startup, prospective, route, controls);
  }
  if (existing && isProcessAlive(existing.pid)) {
    throw unknownStartup(root, `Live daemon PID ${existing.pid} has no proved compatible attachment.`);
  }
  // This session's own install was upgraded or removed underneath it (#2335):
  // its code no longer loads whole, so as the project's writer it would only
  // keep a daemon from the current install from starting. Its daemon exits
  // for the same reason, which is how it got here.
  if (CodeGraphPackageVersion !== '0.0.0-unknown' &&
      installChangedReason(CodeGraphPackageJsonPath, CodeGraphPackageVersion) !== null) {
    return readOnlyFallback('this session\'s CodeGraph install was upgraded or removed; restart the session to use the current one', startup, prospective, route, controls);
  }
  const queryPolicy = resolveQueryPolicy(startup.requested, 'writable-fallback', startup.cpuCount);
  assertNoLiveQueryDaemon(root);
  const observed = observeStartupWriter(route);
  const acquired = tryAcquireWriterLock(root, 'fallback');
  if (acquired.kind === 'taken') throw unknownStartup(root, writerLockHeldMessage(acquired.existing, acquired.pidPath));
  const reservation = retainObservedStartupWriter(route, observed, acquired);
  retain(reservation);
  try {
    return new MCPEngine({ projectWriter: reservation, queryPool: true, queryPolicy, queryAdmissionPolicy: prospective,
      projectRoute: route, executionMode: 'writable-fallback', resourceObserver: controls.resourceObserver,
      onInitialDefaultOutcome: controls.onInitialDefaultOutcome });
  } catch (error) {
    if (isQueryPolicyRefusal(error)) throw error;
    throw unknownStartup(root, `Writable fallback construction failed after its owner check: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/**
 * How long a launcher waits for a freshly-spawned daemon to bind its socket
 * before giving up and running in-process. The daemon binds the socket *before*
 * the (backgrounded) engine/grammar warm-up, so this only needs to cover node
 * process startup. 60 × 100ms = 6s of headroom for a cold/slow box; on the
 * common path the socket appears within a few rounds.
 */
// Poll finely (25ms) so the proxy attaches the instant the freshly-spawned
// daemon binds, instead of waiting up to a coarse 100ms after — shaves the
// cold-start handshake (the window the headless agent races). Same ~6s total
// give-up budget (240 × 25ms), just finer granularity; socket-connect probes
// are cheap. Paired with deferring the CodeGraph load (engine.ts) off the bind
// path, this narrows the "No such tool available" race window.
const DAEMON_CONNECT_MAX_RETRIES = 240;
const DAEMON_CONNECT_RETRY_DELAY_MS = 25;

/** Whether `CODEGRAPH_NO_DAEMON` was set to a truthy value. */
function daemonOptOutSet(): boolean {
  const raw = process.env.CODEGRAPH_NO_DAEMON;
  if (!raw) return false;
  return raw !== '0' && raw.toLowerCase() !== 'false';
}

/** Whether this process was spawned to BE the detached daemon. */
function daemonInternalSet(): boolean {
  const raw = process.env[DAEMON_INTERNAL_ENV];
  return !!raw && raw !== '0' && raw.toLowerCase() !== 'false';
}

/**
 * Prefix every `process.stderr.write` chunk with an ISO-8601 timestamp. Called
 * once, only when this process becomes the detached daemon — whose stderr is
 * appended to `.codegraph/daemon.log`. Before #1431 no log line carried a
 * timestamp, so watchdog kills and restarts could be counted but never placed
 * in time. (The watchdog child writes its kill notice through its own
 * inherited fd 2, bypassing this wrapper — it stamps that line itself.)
 */
export function timestampStderrLines(): void {
  const orig = process.stderr.write.bind(process.stderr);
  process.stderr.write = ((chunk: string | Uint8Array, ...rest: unknown[]) => {
    return (orig as (...args: unknown[]) => boolean)(stampLogChunk(chunk), ...rest);
  }) as typeof process.stderr.write;
}

/** Prepend `[<ISO-8601>] ` to a log chunk; unknown chunk types pass through. */
export function stampLogChunk(chunk: string | Uint8Array): string | Uint8Array {
  try {
    const stamp = `[${new Date().toISOString()}] `;
    if (typeof chunk === 'string') return stamp + chunk;
    if (Buffer.isBuffer(chunk)) return Buffer.concat([Buffer.from(stamp), chunk]);
  } catch { /* stamping is best-effort; never block the write */ }
  return chunk;
}

/**
 * Watchdog `progressPaths` for a server keyed on `root`'s index: the SQLite DB
 * + its WAL. With these, the #850 liveness watchdog only kills on heartbeat
 * silence when the DB files are NOT advancing — the same slow-disk deferral
 * the CLI `index`/`init` path got in #1231. Without it, one >timeout
 * synchronous statement on a big DB (multi-GB index behind Windows Defender)
 * SIGKILLs a perfectly healthy daemon — and a daemon SIGKILL'd at the end of
 * nearly every session is what ratcheted the WAL leak in #1431. A true wedge
 * still dies: a wedged loop writes nothing, so the files stay still.
 */
export function watchdogProgressPaths(root: string | null): { progressPaths?: string[] } {
  if (!root) return {};
  const dbPath = path.join(getCodeGraphDir(root), 'codegraph.db');
  return { progressPaths: [dbPath, `${dbPath}-wal`] };
}

/** One exact primary bootstrap outcome; never cached route permission. */
interface BootstrapSeed {
  readonly route: McpIndexRoute;
  readonly result: Exclude<DaemonBootstrapResult, { kind: 'refused' }>;
}

async function connectHeldService(route: McpIndexRoute, prospective: ResolvedQueryPolicy, signal: AbortSignal) {
  cancelled(signal);
  assertStartupRoute(route);
  const owners = readQueryServiceOwners(route.root);
  const candidates = getDaemonSocketCandidates(route.root);
  if (owners.daemon.kind === 'live' && canProbeDaemonIdentity(owners.daemon.value)) {
    candidates.unshift(owners.daemon.value.socketPath);
  }
  for (const candidate of new Set(candidates)) {
    const socket = await connectWithHello(candidate, undefined, prospective, route.root);
    try { cancelled(signal); assertStartupRoute(route); }
    catch (primary) {
      if (socket) {
        try { await closeOwnedDaemonSocket(socket); }
        catch (cleanup) { reportStartupCleanup('refused socket close', cleanup); }
      }
      throw primary;
    }
    if (socket) return socket;
  }
  return null;
}

function reportStartupCleanup(phase: string, error: unknown): void {
  try { process.stderr.write(`[CodeGraph MCP] ${phase}: ${String(error).slice(0, 2048)}\n`); } catch { /* preserve primary */ }
}

async function waitAcknowledgedCandidate(route: McpIndexRoute, candidatePid: number,
  prospective: ResolvedQueryPolicy, signal: AbortSignal): Promise<void> {
  for (let attempt = 0; attempt < DAEMON_CONNECT_MAX_RETRIES; attempt++) {
    cancelled(signal);
    assertStartupRoute(route);
    const socket = await connectHeldService(route, prospective, signal);
    if (socket) { await closeOwnedDaemonSocket(socket); return; }
    if (processPresence(candidatePid) === 'absent') break;
    await sleep(DAEMON_CONNECT_RETRY_DELAY_MS);
  }
  if (processPresence(candidatePid) !== 'absent') {
    throw unknownStartup(route.root, `Acknowledged candidate PID ${candidatePid} has no proved attachment or native absence.`);
  }
  // The candidate's absence alone never grants a new writer.
  await admitQueryProject(route.root, prospective, signal);
  cancelled(signal);
  assertStartupRoute(route);
  assertNoLiveQueryDaemon(route.root);
}

/**
 * Spawn the shared daemon as a fully detached background process: its own
 * session/process group (so a SIGHUP/SIGINT to the launcher's terminal can't
 * reach it) with stdio decoupled from the launcher (logs to
 * `.codegraph/daemon.log`). Re-invokes the *same* CLI faithfully across dev and
 * bundled launches by reusing `process.argv[0]` (the right node), the current
 * `process.execArgv` (carries `--liftoff-only`, so the daemon never re-execs)
 * and `process.argv[1]` (this script). The spawned process self-arbitrates the
 * O_EXCL lock, so racing launchers may each spawn one — losers exit and every
 * launcher proxies through the single winner. `handover` tells the daemon this
 * launcher holds the writer slot for it (see {@link DAEMON_HANDOVER_ENV}).
 */
async function spawnDetachedDaemon(route: McpIndexRoute, startup: QueryStartupRequest, handover = false,
  signal?: AbortSignal, acknowledgePermission = true): Promise<DaemonBootstrapResult> {
  cancelled(signal);
  assertStartupRoute(route);
  const root = route.root;
  const selector = codeGraphDirNameFor(root);
  if (fs.realpathSync.native(path.join(root, selector, 'codegraph.db')) !== route.index) {
    throw unknownStartup(root, 'The captured child index-directory binding changed.');
  }
  const scriptPath = process.argv[1];
  if (!scriptPath) {
    throw unknownStartup(root, 'Cannot resolve the CLI script path to spawn the daemon.');
  }

  let logFd: number | null = null;
  let stdio: StdioOptions = 'ignore';
  try {
    logFd = fs.openSync(path.join(getCodeGraphDir(root), 'daemon.log'), 'a');
    stdio = ['ignore', logFd, logFd, 'ipc'];
  } catch {
    stdio = ['ignore', 'ignore', 'ignore', 'ipc'];
  }
  try {
    // The daemon has no host: scrub the threaded host pid so it can't leak
    // into the daemon's env (and from there into anything the daemon spawns),
    // where a long-dead session's host pid would trigger spurious shutdowns.
    const env = daemonQueryEnvironment({ ...process.env, CODEGRAPH_DIR: selector, [DAEMON_INTERNAL_ENV]: '1' }, startup);
    delete env[HOST_PPID_ENV];
    if (handover) env[DAEMON_HANDOVER_ENV] = String(process.pid);
    else delete env[DAEMON_HANDOVER_ENV];
    const bootstrapDeadline = performance.now() + 15_000;
    const child = spawn(
      process.execPath,
      [...process.execArgv, scriptPath, 'serve', '--mcp', '--path', root],
      {
        detached: true,
        stdio,
        windowsHide: true,
        env,
      },
    );
    type NativeClose = { exitCode: number | null; signal: NodeJS.Signals | null; observedAt: number };
    let closeFacts: NativeClose | undefined;
    let resolveClose: (facts: NativeClose) => void = () => {};
    const nativeClose = new Promise<NativeClose>(resolve => { resolveClose = resolve; });
    const onClose = (exitCode: number | null, signal: NodeJS.Signals | null) => {
      closeFacts = { exitCode, signal, observedAt: performance.now() };
      resolveClose(closeFacts);
    };
    // Native close may immediately follow exit/error; observe before H starts.
    child.once('close', onClose);
    let closeTimer: NodeJS.Timeout | undefined;
    // An upgrade can delete the install this launcher runs from — its daemon
    // then exits (#2335) and a respawn finds no executable. spawn reports that
    // as an asynchronous 'error' event, which would crash this process if
    // nobody listened. No daemon binds either way; the caller's poll gives up
    // and the session is served in-process.
    const abort = () => { if (child.connected) child.disconnect(); };
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    try {
      // An unsupported discovery preflight must remain inert even if a child
      // unexpectedly advertises permission. H's explicit option denies ACK.
      const result = acknowledgePermission
        ? await awaitDaemonBootstrap(child, root)
        : await awaitDaemonBootstrap(child, root, { acknowledgePermission: false });
      cancelled(signal);
      if (result.kind === 'unavailable') {
        const remaining = bootstrapDeadline - performance.now();
        if (!closeFacts && remaining <= 0) throw unknownStartup(root, 'Candidate native close exceeded the original 15000ms bootstrap deadline.');
        const closed = closeFacts ?? await Promise.race([nativeClose, new Promise<never>((_resolve, reject) => {
          closeTimer = setTimeout(() => reject(unknownStartup(root,
            'Candidate native close did not complete within the original 15000ms bootstrap deadline.')), remaining);
        })]);
        cancelled(signal);
        if (closed.observedAt > bootstrapDeadline) throw unknownStartup(root, 'Candidate native close exceeded the original bootstrap deadline.');
        if (result.cause === 'pre-service-exit') {
          if (child.pid === undefined || !Number.isSafeInteger(child.pid) || child.pid <= 0
            || closed.exitCode !== result.exitCode || closed.signal !== result.signal) {
            throw unknownStartup(root, 'Candidate native exit/close identity or outcome is inconsistent.');
          }
        } else if (child.pid !== undefined) {
          // Spawn failure has no native candidate exit. Preserve its distinct
          // error/close facts rather than treating it as a spawned process.
          throw unknownStartup(root, 'Candidate spawn failure unexpectedly has a native process identity.');
        }
      }
      return result;
    } finally {
      signal?.removeEventListener('abort', abort);
      child.removeListener('close', onClose);
      if (closeTimer) clearTimeout(closeTimer);
    }
  } finally {
    // The child holds its own dup of the log fd now; the launcher doesn't need it.
    if (logFd !== null) {
      try { fs.closeSync(logFd); } catch { /* ignore */ }
    }
  }
}

/**
 * Wait out the daemon candidate `pid` holding the daemon lock while launcher
 * `handoverFrom` holds the writer slot for this daemon (#2335): the candidate
 * cannot get the slot, so it releases the lock and exits. Resolves true once
 * `pid` no longer holds the lock; false at once when no slot is held for this
 * daemon, and false when that stops being so or the wait runs out.
 */
export async function outwaitCandidate(root: string, handoverFrom: number | null, pid: number): Promise<boolean> {
  const heldForUs = (): boolean => {
    const writer = readWriterLock(root);
    return handoverFrom !== null && writer?.pid === handoverFrom && writer.mode === 'handover';
  };
  if (!heldForUs()) return false;
  process.stderr.write(`[CodeGraph daemon] Waiting for daemon candidate pid ${pid} to give up the lock; the writer lock is held for this daemon.\n`);
  const deadline = Date.now() + HANDOVER_CANDIDATE_WAIT_MS;
  while (Date.now() < deadline) {
    if (!heldForUs()) return false;
    let holder: number | undefined;
    try { holder = decodeLockInfo(fs.readFileSync(getDaemonPidPath(root), 'utf8'))?.pid; } catch { holder = undefined; }
    if (holder !== pid || !isProcessAlive(pid)) return true;
    await sleep(25);
  }
  return false;
}

/**
 * MCP Server for CodeGraph
 *
 * Implements the Model Context Protocol to expose CodeGraph
 * functionality as tools that can be called by AI assistants.
 *
 * Backwards-compatible constructor and `start()` signature with the
 * pre-issue-#411 implementation: callers continue to do
 * `new MCPServer(path).start()`. Internally we now pick from direct / proxy /
 * daemon at start time.
 */
export class MCPServer {
  private projectPath: string | null;
  private readonly queryStartup: QueryStartupRequest;
  private readonly queryAdmissionPolicy: ResolvedQueryPolicy;
  private readonly directQueryPolicy: ResolvedQueryPolicy;
  private directStopPromise: Promise<void> | null = null;
  // Direct-mode-only state. In daemon mode the per-connection sessions live
  // inside the Daemon class; in proxy mode there is no session at all.
  private session: MCPSession | null = null;
  private engine: MCPEngine | null = null;
  private daemon: Daemon | null = null;
  // Worker-thread liveness watchdog (#850). Long-lived modes only; SIGKILLs the
  // process if the main thread wedges in a non-yielding sync loop.
  private livenessWatchdog: WatchdogHandle | null = null;
  // Idempotency guard for stop().
  private stopped = false;
  /** Caller-owned startup authority; fallback entries bind their exact engine. */
  private startupWriters = new Map<ProjectWriterReservation, MCPEngine | null>();

  constructor(projectPath?: string, startup = captureMcpQueryStartup(process.env, () => os.cpus().length,
    process.connected === true && typeof process.send === 'function', path.resolve(projectPath ?? process.cwd()),
    typeof process.send === 'function')) {
    // Validate before start() can bootstrap, attach or acquire any authority.
    // Keep the original immutable inputs for the execution mode selected later.
    if (!Number.isSafeInteger(startup.cpuCount) || startup.cpuCount < 0) throw new Error('Invalid captured query CPU count.');
    this.queryStartup = immutable({ requested: { ...startup.requested }, cpuCount: startup.cpuCount });
    this.directQueryPolicy = resolveQueryPolicy(this.queryStartup.requested, 'direct', this.queryStartup.cpuCount);
    this.queryAdmissionPolicy = resolveQueryPolicy(this.queryStartup.requested, 'shared-daemon', this.queryStartup.cpuCount);
    this.projectPath = projectPath || null;
  }

  /**
   * Start the MCP server.
   *
   * Decision order:
   *   1. `CODEGRAPH_NO_DAEMON=1` → direct mode (unchanged pre-#411 behavior).
   *   2. `CODEGRAPH_DAEMON_INTERNAL=1` → we ARE the detached daemon; listen.
   *   3. No `.codegraph/` reachable → direct mode (the daemon's lockfile and
   *      socket both live under `.codegraph/`).
   *   4. Otherwise connect to (or spawn) the shared daemon and proxy to it.
   *
   * The outer prepared shell exposes static discovery immediately. Primary
   * admission remains terminal; genuinely absent service may select fallback.
   */
  async start(): Promise<void> {
    // Long-lived process (direct / proxy / daemon alike): flush buffered
    // telemetry opportunistically. Fire-and-forget + unref'd — adds nothing
    // to the handshake path and never keeps the process alive.
    getTelemetry().startInterval();

    // #1243: the MCP config launches the local binary, so a server left
    // running drifts behind releases with no signal. Refresh the shared
    // update-check cache in the background and log ONE stderr notice when a
    // newer version exists (stderr only — stdout is the protocol channel).
    // The notice also reaches the agent via the initialize instructions and
    // codegraph_status. Fire-and-forget: adds nothing to the handshake path.
    checkForUpdateInBackground();

    // The detached daemon process itself. Checked before the opt-out so the
    // daemon honors the same env it was spawned with (it never sets NO_DAEMON).
    if (daemonInternalSet()) {
      return this.startDaemonProcess();
    }

    // P2 owns one input parser and its supervision lifetime, even when initial
    // verification ultimately selects direct/no-root. Refusal is never downgraded.
    await runLocalHandshakeProxy({ hint: this.projectPath ?? process.cwd(),
      prepare: (signal, controls) => this.prepareHandshake(signal, controls) });
  }

  /**
   * Stop the server. In daemon mode this triggers graceful shutdown of every
   * connected session. Prepared direct mode only releases resources; its
   * outer shell owns process exit and the shutdown deadline.
   */
  async stop(): Promise<void> {
    return this.directStopPromise ??= this.stopResources();
  }

  private async stopResources(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    if (this.livenessWatchdog) {
      this.livenessWatchdog.stop();
      this.livenessWatchdog = null;
    }
    if (this.daemon) {
      await this.daemon.stop('stop()');
      // Daemon.stop owns final exit, but its rejected drain remains observable.
      return;
    }
    let failure: unknown, failed = false;
    const recordFailure = (phase: string, error: unknown) => {
      if (!failed) { failed = true; failure = error; } else reportStartupCleanup(phase, error);
    };
    try { this.session?.stop(); } catch (error) { recordFailure('session stop', error); }
    this.session = null;
    let engineStopped = !this.engine;
    try { await this.engine?.drain(); engineStopped = true; }
    catch (error) {
      recordFailure('engine drain', error);
    }
    if (engineStopped) this.engine = null;
    if (engineStopped) for (const reservation of this.startupWriters.keys()) {
      try { await reservation.release(); this.startupWriters.delete(reservation); }
      catch (error) { recordFailure('startup writer reservation release', error); }
    }
    // Resource-only: the prepared shell owns final exit and its bounded backstop.
    if (failed) throw failure;
  }

  private async prepareHandshake(signal: AbortSignal, controls: LocalHandshakePreparationControls): Promise<PreparedLocalHandshake> {
    // Selection is deferred to the actual session, including client roots.
    // Null here means inert, not a proved no-index/cwd permission result.
    if (daemonOptOutSet()) return { kind: 'direct', route: null,
      makeSession: (transport, factorySignal) => this.makeDirectSession(null, transport, factorySignal, controls),
      stop: () => this.stop() };
    const seeds = new Map<string, BootstrapSeed>();
    const routeKey = (route: McpIndexRoute) => JSON.stringify([route.root, route.index]);
    const progress = new Set<IndexProgressLease>();
    let primaryFailed = false;
    try {
    const hint = path.resolve(this.projectPath ?? process.cwd());
    let primary: McpIndexRoute | null;
    try { primary = captureMcpIndexRoute(hint); }
    catch (error) { throw unknownStartup(hint,
      `The primary own-index route could not be captured: ${error instanceof Error ? error.message : String(error)}`); }
    const resolution = await resolveMcpRoot(this.projectPath ?? process.cwd(), { signal, admit: async route => {
      progress.add(await controls.resourceObserver.acquireIndexProgress(route, signal));
      cancelled(signal);
      await admitQueryProject(route.root, this.queryAdmissionPolicy, signal);
      cancelled(signal);
      // Preserve the exact primary preset's pre-permission schema fence. Do
      // not start services for speculative ancestor/subproject candidates.
      if (this.queryAdmissionPolicy.preset !== 'unlimited'
        || route.root !== primary?.root || route.index !== primary.index) return;
      const existing = await connectHeldService(route, this.queryAdmissionPolicy, signal);
      if (existing) { await closeOwnedDaemonSocket(existing); return; }
      assertNoLiveQueryDaemon(route.root);
      const result = await spawnDetachedDaemon(route, this.queryStartup, false, signal);
      if (result.kind === 'refused') throw result.refusal;
      cancelled(signal);
      assertStartupRoute(route);
      await admitQueryProject(route.root, this.queryAdmissionPolicy, signal);
      cancelled(signal);
      assertStartupRoute(route);
      seeds.set(routeKey(route), Object.freeze({ route, result }));
    } }).catch(error => {
      cancelled(signal);
      if (isQueryPolicyRefusal(error)) throw error;
      throw unknownStartup(this.projectPath ?? process.cwd(),
        `Primary project selection failed: ${error instanceof Error ? error.message : String(error)}`);
    });
    const route = resolution.route;
    const seed = primary ? seeds.get(routeKey(primary)) : undefined;
    seeds.clear();
    cancelled(signal);
    if (seed && (!route || route.root !== seed.route.root || route.index !== seed.route.index)) {
      throw unknownStartup(seed.route.root, 'The bootstrapped primary own index did not remain the selected verified route.');
    }
    if (route) {
      return { kind: 'proxy', deps: this.proxyDependencies(route, this.queryStartup, seed, signal, controls) };
    }
    return { kind: 'direct', route,
      makeSession: (transport, factorySignal) => this.makeDirectSession(route, transport, factorySignal, controls),
      stop: () => this.stop() };
    } catch (error) { primaryFailed = true; throw error; }
    finally {
      let cleanupFailed = false, cleanupFailure: unknown;
      for (const lease of progress) try { await lease.release(); }
      catch (error) {
        if (!cleanupFailed) { cleanupFailed = true; cleanupFailure = error; }
        reportStartupCleanup('schema progress release', error);
      }
      if (cleanupFailed && !primaryFailed) throw cleanupFailure;
    }
  }

  private async makeDirectSession(route: McpIndexRoute | null, transport: JsonRpcTransport,
    signal: AbortSignal, controls: LocalHandshakePreparationControls): Promise<MCPSession> {
    cancelled(signal);
    if (this.stopped) throw new Error('MCP startup is closed.');
    let projectWriter: ProjectWriterReservation | undefined;
    if (route) {
      await admitMcpRoute(route, { signal, admit: async held => {
        await admitQueryProject(held.root, this.queryAdmissionPolicy, signal);
        cancelled(signal);
      } }).catch(error => {
        cancelled(signal);
        if (isQueryPolicyRefusal(error)) throw error;
        throw unknownStartup(route.root, `Primary direct admission failed: ${error instanceof Error ? error.message : String(error)}`);
      });
      cancelled(signal);
      if (this.stopped) throw new Error('MCP startup is closed.');
      assertStartupRoute(route);
      assertNoRebuild(route.root);
      const observed = observeStartupWriter(route);
      const writer = tryAcquireWriterLock(route.root, 'direct');
      if (writer.kind === 'taken') throw unknownStartup(route.root, writerLockHeldMessage(writer.existing, writer.pidPath));
      projectWriter = retainObservedStartupWriter(route, observed, writer);
      this.startupWriters.set(projectWriter, null);
    }
    // No await separates the final closing/ownership fence and publication.
    cancelled(signal);
    if (this.stopped) throw new Error('MCP startup is closed.');
    this.engine = new MCPEngine({ queryPool: true, queryPolicy: this.directQueryPolicy,
      queryAdmissionPolicy: this.queryAdmissionPolicy, requireDefaultWriter: true, ...(route ? { projectRoute: route, projectWriter } : {}),
      resourceObserver: controls.resourceObserver, onInitialDefaultOutcome: controls.onInitialDefaultOutcome, executionMode: 'direct' });
    this.session = new MCPSession(transport, this.engine, { explicitProjectPath: this.projectPath });
    return this.session;
  }

  /**
   * Run as the detached shared daemon (process spawned with
   * `CODEGRAPH_DAEMON_INTERNAL=1`). Arbitrate the O_EXCL lock, then either
   * become the daemon (bind the socket, serve forever) or — if a live daemon
   * already holds the lock — exit so we don't leak a redundant process.
   *
   * No PPID watchdog and no stdin handlers: the daemon is detached on purpose
   * and reaps itself via client-refcount + idle timeout (see {@link Daemon}).
   */
  private async startDaemonProcess(): Promise<void> {
    // In daemon mode stderr IS `.codegraph/daemon.log`; stamp every line so
    // kills/restarts can be placed in time (#1431 — the log was undatable).
    timestampStderrLines();
    const hint = this.projectPath ?? process.cwd();
    const qualified = process.env[DAEMON_INTERNAL_ENV] === '1' && typeof process.send === 'function';
    let route: McpIndexRoute;
    let queryPolicy: ResolvedQueryPolicy;
    try {
      if (qualified && !process.connected) throw unknownStartup(hint,
        'Native candidate bootstrap IPC disconnected before daemon startup.');
      // Support is selected before any schema/authority; parent --path binds an
      // exact own index, never a nearest-root search inside the child.
      queryPolicy = selectedQueryPolicy(this.queryStartup, 'shared-daemon');
      const captured = captureMcpIndexRoute(hint);
      if (!captured) throw unknownStartup(hint, 'The detached candidate has no held own index.');
      route = await admitMcpRoute(captured, { admit: async held => {
        await admitQueryProject(held.root, this.queryAdmissionPolicy);
        if (qualified) await confirmDaemonBootstrap(held.root);
      } });
    } catch (error) {
      const refusal = isQueryPolicyRefusal(error) ? error : unknownStartup(hint,
        `The detached candidate held route could not be proved: ${error instanceof Error ? error.message : String(error)}`);
      if (qualified && process.connected) await reportDaemonBootstrapRefusal(refusal);
      throw refusal;
    }
    const root = route.root;
    // Read once and dropped, so nothing this daemon spawns inherits it.
    const handoverPid = Number(process.env[DAEMON_HANDOVER_ENV]);
    const handoverFrom = Number.isInteger(handoverPid) && handoverPid > 0 ? handoverPid : null;
    delete process.env[DAEMON_HANDOVER_ENV];
    for (let attempt = 0; attempt < TAKEOVER_MAX_RETRIES; attempt++) {
      assertStartupRoute(route);
      // A changed owner after the ACK/schema wait cannot authorize takeover.
      await admitQueryProject(root, this.queryAdmissionPolicy);
      assertStartupRoute(route);
      const lock = tryAcquireDaemonLock(root);

      if (lock.kind === 'acquired') {
        const daemon = new Daemon(root, { handoverFrom, queryPolicy, queryAdmissionPolicy: this.queryAdmissionPolicy, projectRoute: route });
        await daemon.start();
        this.daemon = daemon;
        // The detached daemon has no PPID watchdog or stdin lifeline, so a
        // wedged main thread would pin a core forever (#850). The liveness
        // watchdog is its only recovery path.
        this.livenessWatchdog = installMainThreadWatchdog({ progressPaths: [route.index, `${route.index}-wal`] });
        return; // the net.Server keeps the process alive
      }

      // Taken. If the holder is alive, another daemon already serves (or is
      // binding) — we're redundant; exit cleanly so the launcher proxies to it.
      const existing = lock.existing;
      let disprovedLiveIdentity = false;
      if (existing && existing.pid > 0 && isProcessAlive(existing.pid)) {
        // Give a newly-elected daemon time to bind, then require its socket hello
        // to match the lock PID/version. PID existence alone accepts an unrelated
        // process after OS PID reuse and permanently wedges startup (#1553).
        const age = Date.now() - existing.startedAt;
        const startupGraceMs = 10_000;
        const stillStarting = existing.startedAt > 0 && age >= 0 && age < startupGraceMs;
        // Legacy plain-PID locks have no socket identity to test. Preserve those
        // live holders: an inconclusive probe is not permission to create a
        // second writer.
        if (
          !canProbeDaemonIdentity(existing) ||
          stillStarting ||
          await probeDaemonIdentity(existing)
        ) {
          // A daemon the writer slot is held for outwaits a candidate still
          // starting in its way, which cannot get the slot and leaves (#2335).
          if (stillStarting && await outwaitCandidate(root, handoverFrom, existing.pid)) continue;
          process.stderr.write(
            `[CodeGraph daemon] Another daemon (pid ${existing.pid}) already holds the lock; exiting.\n`
          );
          process.exit(0);
        }
        disprovedLiveIdentity = true;
      }

      // The holder is dead, the record is unreadable, or a completed socket
      // hello disproved a live PID's identity. Revalidate the exact record and
      // retry the acquire only after cleanup succeeds safely.
      if (disprovedLiveIdentity) {
        // Re-probe and claim writer.pid before cleanup. A daemon that is merely
        // delayed already owns that writer lock, and a paired live-PID record is
        // ambiguous under the legacy lock format, so both cases fail closed.
        await clearStaleDaemonArtifacts(root);
      } else if (lock.lockContents !== null) {
        clearStaleDaemonLock(lock.pidPath, existing?.pid, {
          expectedLockContents: lock.lockContents,
        });
      }
      await sleep(TAKEOVER_RETRY_DELAY_MS);
    }

    process.stderr.write('[CodeGraph daemon] Could not acquire the daemon lock; exiting.\n');
    process.exit(0);
  }

  /**
   * Proxy mode (the common case). Serve the MCP handshake LOCALLY for instant
   * tool registration, forwarding tool calls to the shared daemon — which is
   * connected in the background (probed, then spawned + polled if absent; one of
   * an older release is stopped first, #2335) so the
   * handshake never waits ~600ms on it. Runs until the host disconnects; the
   * proxy falls back to an in-process engine if the daemon never binds, so this
   * never wedges a session.
   */
  private proxyDependencies(route: McpIndexRoute, startup: QueryStartupRequest, initialSeed: BootstrapSeed | undefined,
    signal: AbortSignal, controls: LocalHandshakePreparationControls): LocalHandshakeDeps {
    const root = route.root;
    const prospective = this.queryAdmissionPolicy;
    let seed = initialSeed;
    const connectAnyCandidate = () => connectHeldService(route, prospective, signal);
    const getDaemonSocket = async () => {
      // Every attempt performs current permission before attachment or spawn.
      cancelled(signal);
      const capturedSeed = seed;
      seed = undefined;
      if (capturedSeed && (capturedSeed.route.root !== route.root || capturedSeed.route.index !== route.index)) {
        throw unknownStartup(root, 'The bootstrap outcome does not belong to the held primary route.');
      }
      await admitQueryProject(root, prospective, signal);
      cancelled(signal);
      assertStartupRoute(route);
      const probe = await connectAnyCandidate();
      if (probe) return probe;
      assertNoLiveQueryDaemon(root);
      const bootstrapResult = capturedSeed?.result ?? await spawnDetachedDaemon(route, startup, false, signal);
      if (bootstrapResult.kind === 'refused') throw bootstrapResult.refusal;
      cancelled(signal);
      assertStartupRoute(route);
      if (bootstrapResult.kind === 'permitted') {
        await waitAcknowledgedCandidate(route, bootstrapResult.candidatePid, prospective, signal);
        const attached = await connectAnyCandidate();
        if (attached) return attached;
        if (processPresence(bootstrapResult.candidatePid) !== 'absent') throw unknownStartup(root,
          `Acknowledged candidate PID ${bootstrapResult.candidatePid} lost its proved attachment while still alive or ambiguous.`);
      }
      // Genuine pre-service unavailability, or v5's exact absent-candidate
      // exception, still requires the same valid route and fresh owner proof.
      await admitQueryProject(root, prospective, signal);
      cancelled(signal);
      assertStartupRoute(route);
      const appeared = await connectAnyCandidate();
      if (appeared) return appeared;
      assertNoLiveQueryDaemon(root);
      const writer = readQueryServiceOwners(root).writer;
      selectedQueryPolicy(startup, writer.kind === 'live' ? 'read-only-fallback' : 'writable-fallback');
      return null;
    };
    return { getDaemonSocket, makeEngine: () => {
      cancelled(signal);
      if (this.stopped) throw new Error('MCP startup is closed.');
      assertStartupRoute(route);
      let owner: ProjectWriterReservation | undefined;
      const engine = makeFallbackEngine(route, startup, prospective, controls, reservation => {
        this.startupWriters.set(reservation, null);
        owner = reservation;
      });
      if (owner) this.startupWriters.set(owner, engine);
      return engine;
    }, releaseEngine: async retired => {
      for (const [reservation, owner] of this.startupWriters) {
        if (owner !== retired) continue;
        await reservation.release();
        this.startupWriters.delete(reservation);
      }
    }, root, stop: () => this.stop() };
  }


}

function sleep(ms: number): Promise<void> {
  // Deliberately NOT unref'd. During the daemon connect/takeover retry loop we
  // may be between processes — no socket bound yet, no transport, no listener
  // pinning the event loop. An unref'd timer would let Node drain the loop and
  // exit silently before we get a chance to try again.
  return new Promise((resolve) => { setTimeout(resolve, ms); });
}

// Export for use in CLI
export { StdioTransport } from './transport';
export { tools, ToolHandler } from './tools';
// Surface a few daemon-mode bits for tests + diagnostics.
export { Daemon } from './daemon';
export { CodeGraphPackageVersion } from './version';
