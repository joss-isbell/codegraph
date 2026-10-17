/**
 * MCP proxy mode — issue #411.
 *
 * The proxy is a near-transparent stdio↔socket pipe. Once it has verified
 * the daemon's hello line (same major.minor.patch as ours), it does no
 * protocol parsing of its own: every byte the MCP host writes to the proxy's
 * stdin goes straight to the daemon socket, and every byte the daemon emits
 * goes straight to the host's stdout. Server-initiated JSON-RPC requests
 * (e.g. `roots/list`) flow through the same pipe transparently.
 *
 * Lifecycle expectations:
 *   - The proxy exits when *either* stream closes (host stdin closed →
 *     daemon socket end, or daemon-side socket close → host stdout end).
 *   - Closing the socket on the proxy side is what tells the daemon to
 *     decrement its connected-clients refcount.
 *   - On a parent-process death we can't detect via stdin close (e.g. SIGKILL
 *     of the MCP host), the proxy's PPID watchdog catches it — same logic
 *     the direct-mode server uses; see issue #277.
 */

import * as fs from 'fs';
import * as net from 'net';
import * as os from 'os';
import { randomUUID } from 'crypto';
import { createQueryRequestContext } from './query-context';
import { requestedQueryPolicy, resolveLegacyQueryPolicy, type ResolvedQueryPolicy } from './query-policy';
import { HOST_PPID_ENV } from '../extraction/wasm-runtime-flags';
import { DaemonClientHello, DaemonHello, MAX_HELLO_LINE_BYTES } from './daemon';
import { EARLY_PPID } from './early-ppid';
import { supervisionLostReason } from './ppid-watchdog';
import { armStartupHandshakeTimeout } from './startup-handshake';
import { treatStdinFailureAsShutdown } from './stdin-teardown';
import { CodeGraphPackageVersion } from './version';
import { assertCompatibleQueryService, isServicePolicyRefusal, readSharedQueryService, servicePolicyRefusal, type ServicePolicyRefusal } from './query-service-compatibility';
import { assertQueryServiceOwner } from './query-service-owner';
import { SERVER_INFO, PROTOCOL_VERSION, initializeInstructions, type MCPSession } from './session';
import { SERVER_INSTRUCTIONS, SERVER_INSTRUCTIONS_PENDING } from './server-instructions';
import { getStaticTools } from './tools';
import { ErrorCodes, ParsedMessageTransport, type JsonRpcResponse } from './transport';
import type { McpIndexRoute } from '../directory';
import { ExploreSessionState } from './explore-session-state';
import { getTelemetry, ClientInfo } from '../telemetry';
import { installMainThreadWatchdog, WatchdogHandle } from './liveness-watchdog';
import type { MCPEngine, InitialDefaultOutcome } from './engine';
import type { ProjectResourceObserver, IndexProgressLease } from './project-lifecycle';
import { QUERY_CLEANUP_BOUNDS } from './query-pool';

/** Default poll cadence for the PPID watchdog (same as the direct server). */
const DEFAULT_PPID_POLL_MS = 5000;

/**
 * How long a proxy serving in-process waits before trying the shared daemon
 * again, doubling after each miss up to the cap (#2277). Tunable through
 * `CODEGRAPH_DAEMON_RETRY_MS` (`0` turns retrying off) and
 * `CODEGRAPH_DAEMON_RETRY_MAX_MS`.
 */
const DEFAULT_DAEMON_RETRY_MS = 5_000;
const DEFAULT_DAEMON_RETRY_MAX_MS = 300_000;

/**
 * Longest the proxy waits, once it is shutting down, for its owned sockets to
 * close and its in-process engines to stop before exiting anyway. An engine's stop waits up to
 * the distinct startup allowance for a query worker still starting up (exiting
 * mid-start can crash the process on Windows), after the abandoned drain and
 * before final cleanup. These provisional v1 bounds never apply to handover.
 */
const SHUTDOWN_BACKSTOP_MS = QUERY_CLEANUP_BOUNDS.drainMs + QUERY_CLEANUP_BOUNDS.startupMs + QUERY_CLEANUP_BOUNDS.cleanupMs;

/** Native close is an event, not the stream's earlier closed/destroyed flags.
 * Weak identity retains prior facts; the strong map holds only open resources. */
interface OwnedSocketObservation {
  socket: net.Socket;
  actualCloseObserved: boolean;
  hadError: boolean;
  firstError?: unknown;
  furtherError?: unknown;
  unavailableError?: unknown;
  destroyError?: unknown;
  completion: Promise<OwnedSocketObservation>;
  startupClose?: Promise<void>;
}

const ownedSocketObservations = new WeakMap<net.Socket, OwnedSocketObservation>();
const activeOwnedSockets = new Map<net.Socket, OwnedSocketObservation>();
let includeInSocketDrain: ((observation: OwnedSocketObservation) => void) | null = null;

function observeOwnedSocket(socket: net.Socket): OwnedSocketObservation {
  const existing = ownedSocketObservations.get(socket);
  if (existing) return existing;
  let complete!: (observation: OwnedSocketObservation) => void;
  const observation: OwnedSocketObservation = {
    socket, actualCloseObserved: false, hadError: false,
    // Always resolves, even on failure: no rejected orphan cleanup promise.
    completion: new Promise(resolve => { complete = resolve; }),
  };
  const onError = (error: Error): void => {
    if (observation.firstError === undefined) observation.firstError = error;
    else observation.furtherError ??= error;
  };
  socket.on('error', onError);
  socket.once('close', (hadError: boolean) => {
    observation.actualCloseObserved = true;
    observation.hadError = hadError;
    socket.removeListener('error', onError);
    activeOwnedSockets.delete(socket);
    complete(observation);
  });
  ownedSocketObservations.set(socket, observation);
  activeOwnedSockets.set(socket, observation);
  // A connection created by an already-running acquisition during shutdown
  // belongs to the same drain, with native observers installed before destroy.
  includeInSocketDrain?.(observation);
  return observation;
}

function ownedSocketFailure(observation: OwnedSocketObservation): unknown {
  // An ordinary pre-connect availability cause is not a cleanup failure.
  const error = observation.firstError === observation.unavailableError
    ? observation.furtherError ?? observation.destroyError
    : observation.firstError ?? observation.furtherError ?? observation.destroyError;
  if (error !== undefined) return error;
  if (observation.hadError && observation.unavailableError === undefined) {
    return new Error('owned daemon socket closed with an unobserved error');
  }
  return undefined;
}

/** Join the creation-time native close observation; never infer it from flags
 * or install a late observer after shutdown may already have closed the peer. */
export function closeOwnedDaemonSocket(socket: net.Socket): Promise<void> {
  const observation = ownedSocketObservations.get(socket);
  if (!observation) return Promise.reject(new Error('Daemon socket has no original ownership observation'));
  if (observation.startupClose) return observation.startupClose;
  observation.startupClose = (async () => {
    const deadline = performance.now() + 1500;
    let timer: NodeJS.Timeout | undefined;
    try {
      if (!observation.actualCloseObserved) {
        const timeout = new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new Error('Owned startup daemon socket native close incomplete')), 1500);
        });
        try { socket.destroy(); } catch (error) { observation.destroyError ??= error; }
        try {
          await Promise.race([observation.completion, timeout]);
          if (performance.now() >= deadline) throw new Error('Owned startup daemon socket native close incomplete');
        } catch (primary) {
          const retained = ownedSocketFailure(observation);
          if (retained !== undefined) throw new AggregateError([retained, primary], 'Owned startup daemon socket cleanup incomplete');
          throw primary;
        }
      } else await observation.completion;
      const error = ownedSocketFailure(observation);
      if (error !== undefined) throw error;
    } finally { if (timer) clearTimeout(timer); }
  })();
  return observation.startupClose;
}

function beginOwnedSocketDrain(selected: Iterable<net.Socket>): { complete(): Promise<unknown> } {
  // This collection exists only during process shutdown, including connections
  // that close before acquisition returns. Normal retirement keeps no history.
  const included = new Set<OwnedSocketObservation>();
  const include = (observation: OwnedSocketObservation): void => {
    if (included.has(observation)) return;
    included.add(observation);
    if (!observation.actualCloseObserved) {
      try { observation.socket.destroy(); }
      catch (error) { observation.destroyError ??= error; }
    }
  };
  includeInSocketDrain = include;
  for (const socket of selected) include(observeOwnedSocket(socket));
  for (const observation of activeOwnedSockets.values()) include(observation);
  return {
    async complete(): Promise<unknown> {
      // Acquisition and engine stops settle before this final drain. Repeat
      // for any late observed socket; only its actual close resolves completion.
      while ([...included].some(observation => !observation.actualCloseObserved)) {
        await Promise.all([...included].map(observation => observation.completion));
      }
      for (const observation of included) {
        // Retain the original ordinary availability cause without turning a
        // healthy ENOENT/ECONNREFUSED fallback into cleanup failure. Any other
        // earlier error, subsequent error or destroy failure stays significant.
        const error = ownedSocketFailure(observation);
        if (error !== undefined) return error;
      }
      return undefined;
    },
  };
}

/**
 * Env var that opts INTO the "attached to shared daemon" log line. Off by
 * default: the line is benign INFO, but MCP hosts render any server stderr at
 * error level (and append an `undefined` data field), so on every session start
 * a healthy attach showed up as `[error] … undefined`. Set to `1` to surface it
 * when debugging daemon attach. (#618; approach from #640 by @mturac)
 */
const LOG_ATTACH_ENV = 'CODEGRAPH_MCP_LOG_ATTACH';

/**
 * Log a successful daemon attach — gated behind {@link LOG_ATTACH_ENV} so it is
 * silent by default (see #618). Exported for tests.
 */
export function logAttachedDaemon(socketPath: string, hello: DaemonHello): void {
  if (process.env[LOG_ATTACH_ENV] !== '1') return;
  process.stderr.write(
    `[CodeGraph MCP] Attached to shared daemon on ${socketPath} (pid ${hello.pid}, v${hello.codegraph}).\n`
  );
}

export interface ProxyResult {
  /**
   * `proxied` — successfully attached to a same-version daemon and piped
   * stdio. The proxy stays alive until either end closes.
   * `fallback-needed` — the daemon rejected us (version mismatch / unreachable
   * socket) and the caller should run the server in direct mode.
   */
  outcome: 'proxied' | 'fallback-needed';
  reason?: string;
}

/**
 * Attempt to connect to the daemon at `socketPath` and pipe stdio through it.
 *
 * Returns a promise that resolves when either:
 *   - the connection succeeded and one of stdin/socket has now closed
 *     (after which the process should exit), or
 *   - the connection failed early enough that the caller can still fall
 *     back to direct mode.
 *
 * The `expectedVersion` param defaults to the package's own version — daemon
 * and proxy MUST match exactly. Mismatch resolves with
 * `outcome: 'fallback-needed'` so the caller can transparently start its own
 * server. (We accept the cost of two concurrent servers in this case as the
 * price of never silently running a stale daemon against newer client code.)
 */
export async function runProxy(
  socketPath: string,
  expectedVersion: string = CodeGraphPackageVersion,
): Promise<ProxyResult> {
  // POSIX: refuse to connect to a stale socket file that points at no
  // listening process. `fs.existsSync` is a cheap pre-check; a real
  // ECONNREFUSED below catches the rare "exists but unbound" race.
  if (process.platform !== 'win32' && !fs.existsSync(socketPath)) {
    return { outcome: 'fallback-needed', reason: 'socket file missing' };
  }

  const socket = net.createConnection(socketPath);
  observeOwnedSocket(socket);

  const hello = await readHelloLine(socket).catch((err) => {
    socket.destroy();
    return new Error(String(err));
  });
  if (hello instanceof Error) {
    return { outcome: 'fallback-needed', reason: hello.message };
  }

  if (hello.codegraph !== expectedVersion) {
    process.stderr.write(
      `[CodeGraph MCP] Found a daemon on ${socketPath} but version (${hello.codegraph}) ` +
      `differs from ours (${expectedVersion}); falling back to direct mode.\n`
    );
    socket.destroy();
    return { outcome: 'fallback-needed', reason: 'version mismatch' };
  }

  logAttachedDaemon(socketPath, hello);

  socket.setEncoding('utf8');
  sendClientHello(socket);
  startPpidWatchdog(socket);
  await pipeUntilClose(socket);
  // Host disconnected (or the daemon went away). The proxy's only job is the
  // pipe; exit now so we don't linger — process.stdin's 'data' listener would
  // otherwise keep the event loop alive and leave a zombie launcher behind.
  process.exit(0);
}

/**
 * Verify actual shared behavior and exact wire implementation before returning
 * a socket. Null means native absence before any connection; a connected peer
 * without a usable proof is a typed unknown refusal.
 * An explicit project root also requires matching live owner records. Unlike
 * {@link runProxy} this does not pipe; the caller owns the verified socket.
 * It remains paused until the caller installs its consumer and resumes it.
 */
export async function connectWithHello(
  socketPath: string,
  expectedVersion: string = CodeGraphPackageVersion,
  prospectivePolicy: ResolvedQueryPolicy = resolveLegacyQueryPolicy(requestedQueryPolicy(process.env), 'shared-daemon', os.cpus().length),
  projectRoot?: string,
): Promise<net.Socket | null> {
  const observed = await connectObservedService(socketPath, projectRoot ?? socketPath);
  if (!observed) return null;
  const { socket, hello } = observed;
  try {
    assertCompatibleQueryService(hello, expectedVersion, prospectivePolicy, projectRoot ?? socketPath);
    if (projectRoot) assertQueryServiceOwner(projectRoot, hello);
  }
  catch (error) { socket.destroy(); throw error; }
  logAttachedDaemon(socketPath, hello);
  sendClientHello(socket, prospectivePolicy);
  return socket;
}

/** Internal resource channel for status/catch-up only. This validates live
 * implementation/ownership but never supplies a caller's policy permission.
 * The caller installs its status consumer before resuming the paused socket. */
export async function connectForProjectFreshness(socketPath: string, root: string): Promise<net.Socket | null> {
  const observed = await connectObservedService(socketPath, root);
  if (!observed) return null;
  const { socket, hello } = observed;
  try {
    readSharedQueryService(hello, CodeGraphPackageVersion, root);
    assertQueryServiceOwner(root, hello);
  } catch (error) { socket.destroy(); throw error; }
  sendClientHello(socket, null);
  return socket;
}

/** One bounded hello transport for admission and resource observation alike. */
async function connectObservedService(socketPath: string, refusalRoot: string): Promise<{ socket: net.Socket; hello: DaemonHello } | null> {
  let socket: net.Socket;
  try { socket = net.createConnection(socketPath); }
  catch { throw servicePolicyRefusal('unknown', refusalRoot, 'shared-service connection could not be observed'); }
  const ownership = observeOwnedSocket(socket);
  let connected = false;
  socket.once('connect', () => { connected = true; });
  // Keep an 'error' listener attached for the socket's ENTIRE life. readHelloLine
  // attaches its own and then REMOVES it on success (its cleanup()), which left a
  // window — from here until the caller attaches its onDaemonLost handler — where
  // a socket 'error' had NO listener. In Node an unhandled socket 'error' is
  // re-thrown as an uncaughtException, which the global fatal handler turns into
  // process.exit(1); to an MCP client that surfaces as a bare "Transport closed"
  // (#974). The window is rarely hit on a healthy FS but is common on flaky
  // AF_UNIX-over-DrvFs (WSL2 /mnt drives). A no-op guard makes the error
  // recoverable by the caller's classification, without an uncaught exception.
  socket.on('error', () => { /* observed by hello/acquisition or attached-session listeners */ });
  const hello = await readHelloLine(socket).catch((error: unknown) => {
    socket.destroy();
    const code = (error as NodeJS.ErrnoException | null)?.code;
    if (!connected && (code === 'ENOENT' || code === 'ECONNREFUSED')) {
      ownership.unavailableError = error;
      return null;
    }
    throw servicePolicyRefusal('unknown', refusalRoot, connected
      ? 'a connected shared-service peer did not prove a usable hello'
      : 'shared-service transport availability could not be proved');
  });
  if (!hello) {
    socket.destroy();
    return null; // no connection was established; owner classification still follows
  }
  if (socketUnusableForAttachment(socket)) {
    socket.destroy();
    throw servicePolicyRefusal('unknown', refusalRoot, 'the observed shared-service socket closed before attachment');
  }
  // Hello framing consumes raw bytes. Decode subsequent RPCs only after the
  // complete hello, while paused so its exact buffered tail cannot be lost.
  socket.setEncoding('utf8');
  return { socket, hello };
}

function socketUnusableForAttachment(socket: net.Socket): boolean {
  return socket.destroyed || socket.closed || socket.readableEnded || socket.writableEnded || !socket.readable || !socket.writable;
}

/**
 * Tell the daemon our pids right after we verify its hello, so its liveness
 * sweep can reap this client if our process dies without the socket ever
 * signalling close (the Windows named-pipe hazard behind #692). Best-effort:
 * sent before any piped bytes so it's always the daemon's first line from us,
 * and a write failure here is harmless (the daemon just falls back to the
 * socket-close lifecycle). `hostPid` mirrors the PPID watchdog: the threaded
 * host pid if set, else our own parent (the host, on a no-relaunch bundle).
 */
function sendClientHello(socket: net.Socket, queryPolicy: ResolvedQueryPolicy | null = resolveLegacyQueryPolicy(requestedQueryPolicy(process.env), 'shared-daemon', os.cpus().length)): void {
  const clientHello: DaemonClientHello = {
    codegraph_client: 1,
    ...(queryPolicy ? { queryPolicy } : {}),
    pid: process.pid,
    hostPid: parseHostPpid(process.env[HOST_PPID_ENV]) ?? EARLY_PPID,
  };
  try { socket.write(JSON.stringify(clientHello) + '\n'); } catch { /* best-effort */ }
}

type JsonRpc = Record<string, unknown>;

/** Dependencies the local-handshake proxy needs, injected by MCPServer (which
 *  owns the daemon-spawn machinery and the engine factory). */
export interface LocalHandshakeDeps {
  /** Probe ownership/compatibility → optional spawn → retry → hello-verify;
   *  resolves a connected daemon socket, or null when the daemon
   *  path is genuinely unavailable (→ in-process fallback). */
  getDaemonSocket(): Promise<net.Socket | null>;
  /** Lazily create an in-process engine — used only while the daemon is
   *  unreachable, preserving the "a broken daemon never wedges a session"
   *  guarantee. Called again whenever the previous engine was retired (#2277). */
  makeEngine(): MCPEngine;
  /** Release only this engine's caller-owned authority after its successful native drain. */
  releaseEngine?: (engine: MCPEngine) => Promise<void>;
  /** Project root for the fallback engine's lazy init. */
  root: string;
  /** Release caller-owned fallback reservations after engines/borrowers drain.
   * Resource-only and joined once by the outer cached shutdown; never exits. */
  stop?: () => Promise<void>;
}

export interface LocalHandshakePreparationControls {
  readonly resourceObserver: ProjectResourceObserver;
  readonly onInitialDefaultOutcome: (outcome: InitialDefaultOutcome) => void;
}

/** Prepared permission/route decisions belong to the caller, never this shell. */
export type PreparedLocalHandshake =
  | { readonly kind: 'proxy'; readonly deps: LocalHandshakeDeps }
  | {
      readonly kind: 'direct';
      readonly route: McpIndexRoute | null;
      readonly makeSession: (transport: ParsedMessageTransport, signal: AbortSignal) => Promise<MCPSession>;
      /** Resource-only, cached and safe before factory invocation; never exits. */
      readonly stop: () => Promise<void>;
    };

export type LocalHandshakeInput = LocalHandshakeDeps | {
  readonly hint: string;
  readonly prepare: (signal: AbortSignal, controls: LocalHandshakePreparationControls) => Promise<PreparedLocalHandshake>;
};

/**
 * Local-handshake proxy (the cold-start fix).
 *
 * Answers `initialize` + `tools/list` from STATIC constants the instant the
 * client asks — tools register in ~process-startup time instead of waiting
 * ~600ms for the daemon to spawn+bind, which is what produced the "No such tool
 * available" race that made headless agents flail into grep/Read. Tool CALLS are
 * forwarded to the shared daemon (connected in the background); the daemon's
 * response to the forwarded `initialize` is suppressed (the client already got
 * the local one). If the daemon never comes up (version mismatch / spawn fail),
 * a lazily-created in-process engine serves the calls — so the handshake speedup
 * never costs the old fall-back-to-direct robustness.
 *
 * Serving in-process is not for the rest of the session (#2277): that engine
 * may own the project's writer lock, and no daemon can start while it does, so
 * every other session on the project would be stuck without auto-sync. The
 * proxy retries the daemon with backoff, handing the writer lock back first,
 * and proxies again once one answers.
 */
export async function runLocalHandshakeProxy(input: LocalHandshakeInput): Promise<void> {
  // The proxy is long-lived and can serve fallback tool calls in-process. Match
  // direct/daemon mode by killing this launcher if its main thread wedges, so an
  // MCP host retry cannot accumulate abandoned `serve --mcp` wrapper processes.
  let livenessWatchdog: WatchdogHandle | null = installMainThreadWatchdog();
  const preparationLifetime = new AbortController();
  let selectedKind: 'preparing' | 'proxy' | 'direct' = 'prepare' in input ? 'preparing' : 'proxy';
  let deps: LocalHandshakeDeps | null = 'prepare' in input ? null : input;
  const proxyDeps = (): LocalHandshakeDeps => {
    if (selectedKind !== 'proxy' || !deps) throw new Error('No prepared proxy route');
    return deps;
  };
  const rootHint = 'prepare' in input ? input.hint : input.root;
  type ProvisionalRequest = { line: string; controller: AbortController; commit?: () => void };
  const readinessQueue: Array<{ line: string; parsed: unknown; request?: ProvisionalRequest }> = [];
  let originalInitialize: JsonRpc | undefined;
  let direct: Extract<PreparedLocalHandshake, { kind: 'direct' }> | null = null;
  let directTransport: ParsedMessageTransport | null = null;
  let directSession: MCPSession | null = null;
  let directReady = false;
  let activation: Promise<void> | null = null;
  let selection: Promise<void> | null = null;
  let directResourceStop: Promise<void> | null = null;
  let directStopPromise: Promise<void> | null = null;
  const directCalls = new Set<Promise<void>>();
  let daemonStatus: 'connecting' | 'ready' | 'failed' | 'refused' = 'connecting';
  let policyRefusal: ServicePolicyRefusal | null = null;
  let daemonSocket: net.Socket | null = null;
  // A verified result is not attached authority yet. Retain loss observation
  // across the await/attachment handoff, including an already-emitted close.
  const attachmentGuards = new Map<net.Socket, () => void>();
  const acquisitions = new Set<Promise<net.Socket | null>>();
  let clientInitLine: string | undefined;  // replayed to a daemon that comes up mid-session (#2277)
  const locallyAnsweredInitializes = new Map<string, unknown>();
  let primeDaemonInitialize: ((line: string) => void) | null = null;
  // Telemetry attribution for the in-process fallback only — calls routed to
  // the daemon are counted by the daemon's own session (which receives the
  // forwarded initialize, clientInfo included), never double-counted here.
  let telemetryClient: ClientInfo | undefined;
  const pending: string[] = [];            // client lines buffered until the daemon resolves
  let engine: MCPEngine | null = null;
  let engineReady: Promise<void> | null = null;
  // Calls being served in-process; the engine is only ever stopped after they finish.
  const localCalls = new Set<Promise<void>>();
  // Engines retired but not yet stopped; shutdown() waits for them too.
  const retiring = new Set<MCPEngine>();
  // Reuse the exact retirement attempt on shutdown. A rejected native drain
  // cannot become a successful second stop/no-op or release caller authority.
  const retirementDrains = new WeakMap<MCPEngine, Promise<void>>();
  let handoverFailure: { readonly reason: unknown } | null = null;
  let shuttingDown = false;
  const progressReferences = new Map<string, number>();
  let watchedPaths: string[] = [];
  let watchdogQueue: Promise<void> = Promise.resolve();
  let watchdogStop: Promise<void> | null = null;
  let watchdogFailed = false;
  let watchdogFailure: unknown;
  // Cache the bounded actual-close wait too: a failed close cannot acquire an
  // additional allowance on shutdown or authorize a replacement child.
  const watchdogCloses = new WeakMap<WatchdogHandle, Promise<void>>();
  const closeWatchdog = (handle: WatchdogHandle): Promise<void> => {
    const existing = watchdogCloses.get(handle);
    if (existing) return existing;
    const closing = (async () => {
      let timer: NodeJS.Timeout | undefined;
      // Start before stop(): native teardown and its observers consume the
      // same allowance. Neither synchronous observation nor a later shutdown
      // join can restart this absolute deadline.
      const deadline = performance.now() + QUERY_CLEANUP_BOUNDS.cleanupMs;
      const timeout = new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error('Native watchdog close incomplete')), QUERY_CLEANUP_BOUNDS.cleanupMs);
      });
      try {
        handle.stop();
        await Promise.race([handle.closed, timeout]);
        if (performance.now() >= deadline) throw new Error('Native watchdog close incomplete');
      } finally { if (timer) clearTimeout(timer); }
    })();
    watchdogCloses.set(handle, closing);
    return closing;
  };
  const replaceWatchdog = (): Promise<void> => {
    const replacement = watchdogQueue.then(async () => {
      if (watchdogFailed) throw watchdogFailure;
      if (shuttingDown || watchdogStop) return;
      const currentPaths = (): string[] => [...new Set([...progressReferences.keys()].flatMap(index => [index, `${index}-wal`]))].sort();
      const paths = currentPaths();
      if (paths.length === watchedPaths.length && paths.every((value, i) => value === watchedPaths[i])) return;
      if (livenessWatchdog) await closeWatchdog(livenessWatchdog);
      livenessWatchdog = null;
      if (shuttingDown || watchdogStop) return;
      // Cancellation/release can change references during actual native close.
      // A stale pre-wait set must never arm progress for a cancelled borrower.
      const livePaths = currentPaths();
      // Disabled/degraded installation remains null as before. Permission is
      // independently established by the actual caller after this async wait.
      livenessWatchdog = installMainThreadWatchdog({ progressPaths: livePaths });
      watchedPaths = livePaths;
    });
    watchdogQueue = replacement.catch(error => { watchdogFailed = true; watchdogFailure = error; });
    return replacement;
  };
  const stopWatchdog = (): Promise<void> => {
    if (watchdogStop) return watchdogStop;
    // Set the stop promise before any queued continuation can rearm.
    watchdogStop = watchdogQueue.then(async () => {
      progressReferences.clear();
      if (livenessWatchdog) await closeWatchdog(livenessWatchdog);
      livenessWatchdog = null;
      if (watchdogFailed) throw watchdogFailure;
    });
    return watchdogStop;
  };
  const resourceObserver: ProjectResourceObserver = {
    async acquireIndexProgress(route, signal): Promise<IndexProgressLease> {
      if (shuttingDown || watchdogStop || signal?.aborted) throw new Error('Index progress registration cancelled');
      // route.index is the already captured canonical DATABASE FILE. No root
      // discovery, selector recapture, schema read or permission verdict here.
      const index = route.index;
      progressReferences.set(index, (progressReferences.get(index) ?? 0) + 1);
      let releasePromise: Promise<void> | null = null;
      const release = (): Promise<void> => {
        if (releasePromise) return releasePromise;
        const count = progressReferences.get(index) ?? 0;
        if (count <= 1) progressReferences.delete(index);
        else progressReferences.set(index, count - 1);
        releasePromise = replaceWatchdog();
        return releasePromise;
      };
      const onAbort = (): void => {
        // Drop the reference synchronously before a pending native close can
        // finish. Its queued replacement remains independently joined below.
        void release().catch(error => {
          try { process.stderr.write(`[CodeGraph MCP] Cancelled index progress cleanup incomplete: ${String(error).slice(0, 512)}.\n`); }
          catch { /* preserve original registration failure and sticky watchdog loss */ }
        });
      };
      signal?.addEventListener('abort', onAbort, { once: true });
      try {
        await replaceWatchdog();
        if (shuttingDown || watchdogStop || signal?.aborted) throw new Error('Index progress registration cancelled');
        return { release };
      } catch (primary) {
        // Release the failed registration without masking its original cause.
        try { await release(); }
        catch (cleanup) {
          try { process.stderr.write(`[CodeGraph MCP] Index progress cleanup incomplete: ${String(cleanup).slice(0, 512)}.\n`); }
          catch { /* preserve primary; failed native close remains sticky */ }
        }
        throw primary;
      } finally { signal?.removeEventListener('abort', onAbort); }
    },
  };
  const retryBaseMs = parseDelayMs(process.env.CODEGRAPH_DAEMON_RETRY_MS, DEFAULT_DAEMON_RETRY_MS);
  const retryMaxMs = Math.max(retryBaseMs, parseDelayMs(process.env.CODEGRAPH_DAEMON_RETRY_MAX_MS, DEFAULT_DAEMON_RETRY_MAX_MS));
  let retryDelayMs = retryBaseMs;
  let retryTimer: NodeJS.Timeout | null = null;
  let attachedAt = 0;
  // Requests forwarded to the daemon and not yet answered, keyed by JSON-RPC id.
  // If the daemon dies mid-session (#662 — e.g. an MCP host SIGTERM's it when a
  // new session starts), these would otherwise hang forever; we re-serve them
  // in-process so the host always gets a reply.
  const inflight = new Map<unknown, string>();
  const requests = new Map<unknown, ProvisionalRequest>();
  const liveReadiness = (item: typeof readinessQueue[number]): boolean => {
    const message = item.parsed && typeof item.parsed === 'object' ? item.parsed as JsonRpc : {};
    return message.id === undefined || !message.method || message.method === 'initialize'
      || requests.get(message.id) === item.request;
  };
  // Explore call history for the ONE host connection this proxy serves (CG-17).
  // Only the daemon-unavailable fallback below uses it; when the daemon is up,
  // the tracking happens on the daemon's own MCPSession.
  const exploreSession = new ExploreSessionState();
  // Stable across local-engine retirement/handover; engine generations stay
  // per engine and request admission stays per invocation, never worker globals.
  const sessionId = randomUUID();
  const liveLine = (line: string): boolean => {
    try {
      const message = JSON.parse(line) as JsonRpc;
      return message.id === undefined || !message.method || message.method === 'initialize' || requests.get(message.id)?.line === line;
    } catch { return true; }
  };
  const trackInflight = (line: string): void => {
    try {
      const m = JSON.parse(line) as JsonRpc;
      if (m && m.id !== undefined && typeof m.method === 'string' && m.method !== 'initialize') {
        inflight.set(m.id, line);
      }
    } catch { /* unparseable — nothing we could re-serve anyway */ }
  };

  const writeClient = (obj: JsonRpc | string): boolean => {
    if (shuttingDown || process.stdout.destroyed || process.stdout.writableEnded) return false;
    try {
      process.stdout.write((typeof obj === 'string' ? obj : JSON.stringify(obj)) + '\n');
      return true; // stream backpressure still means the write was accepted
    } catch { return false; }
  };
  const refuseLine = (line: string, parsed?: unknown): void => {
    if (!policyRefusal) return;
    let message: JsonRpc;
    try { message = (parsed === undefined ? JSON.parse(line) : parsed) as JsonRpc; } catch { return; }
    if (!message || typeof message !== 'object') return;
    if (parsed === undefined ? !liveLine(line) :
      message.id !== undefined && message.method && message.method !== 'initialize' && requests.get(message.id)?.line !== line) return;
    if (message.id === undefined || !message.method || message.method === 'initialize') return;
    const { code, reason, projectRoot, guidance } = policyRefusal;
    writeClient({ jsonrpc: '2.0', id: message.id, error: { code: -32603,
      message: policyRefusal.message, data: { code, reason, projectRoot, guidance } } });
    requests.delete(message.id); inflight.delete(message.id);
  };
  const refuseAcquisition = (error: ServicePolicyRefusal): void => {
    if (shuttingDown || policyRefusal) return;
    policyRefusal = error;
    daemonStatus = 'refused';
    if (retryTimer) { clearTimeout(retryTimer); retryTimer = null; }
    process.stderr.write(`[CodeGraph MCP] ${error.message}\n`);
    if (selectedKind === 'proxy') for (const line of pending.splice(0)) refuseLine(line);
    else {
      for (const item of readinessQueue) if (liveReadiness(item)) refuseLine(item.line, item.parsed);
      pending.length = 0;
    }
    readinessQueue.length = 0;
    // The actual session owns delegated pending calls and their history
    // delivery. Stopping it here would silently abandon their typed answers.
    directSession?.refuseInitialDefault(error);
  };
  const dispatchDirect = (parsed: unknown): void => {
    if (shuttingDown || !directTransport || !directReady) return;
    // Never await here: a pending tool can need a roots response or cancellation
    // from the very next stdin message. The real session owns request lifetime.
    const call = directTransport.accept(parsed);
    directCalls.add(call);
    const done = (): void => { directCalls.delete(call); };
    void call.then(done, error => {
      done();
      process.stderr.write(`[CodeGraph MCP] Direct dispatch failed: ${String(error)}.\n`);
    });
  };
  const stopDirectResources = (): Promise<void> | null => {
    // No descriptor yet is not a completed stop. Selection may return one
    // after EOF; that exact descriptor must still receive its one resource stop.
    if (!direct) return null;
    if (!directResourceStop) {
      const descriptor = direct;
      directResourceStop = Promise.resolve().then(() => descriptor.stop());
      void directResourceStop.catch(() => { /* joined by stopDirect below */ });
    }
    return directResourceStop;
  };
  const stopDirect = (): Promise<void> => {
    if (directStopPromise) return directStopPromise;
    directReady = false;
    const failures: unknown[] = [];
    try { directTransport?.stop(); } catch (error) { failures.push(error); }
    try { directSession?.stop(); } catch (error) { failures.push(error); }
    // Initiate cancellation BEFORE joining a factory that may need this very
    // resource stop to finish. The same promise is retained for the final join.
    stopDirectResources();
    directStopPromise = (async () => {
      // Join raw selection, not its refusal handler (which can itself call us).
      // Closing selection starts late descriptor cleanup before returning.
      await Promise.allSettled([selection]);
      try { directSession?.stop(); } catch (error) { failures.push(error); }
      const completed = await Promise.allSettled([stopDirectResources(), ...directCalls]);
      for (const result of completed) if (result.status === 'rejected') failures.push(result.reason);
      if (failures.length) throw new AggregateError(failures, 'Direct resource cleanup incomplete');
    })();
    return directStopPromise;
  };
  const shutdown = (): void => {
    if (shuttingDown) return; shuttingDown = true;
    preparationLifetime.abort();
    directReady = false;
    const stopPrepared = stopDirect();
    for (const request of requests.values()) request.controller.abort();
    requests.clear(); inflight.clear(); pending.length = 0;
    if (retryTimer) clearTimeout(retryTimer);
    const sockets = new Set(attachmentGuards.keys());
    if (daemonSocket) sockets.add(daemonSocket);
    for (const release of attachmentGuards.values()) {
      release();
    }
    const socketDrain = beginOwnedSocketDrain(sockets);
    // Exit once every owned socket has closed and every engine has stopped,
    // a retiring one included: exiting while
    // a query worker is still starting up can crash the process on Windows (see
    // QueryPool.destroy), and stopping releases the writer lock. The backstop
    // bounds a stop that never settles; it stays ref'd so the process lives
    // until one of the two exits it.
    setTimeout(() => {
      process.stderr.write(`[CodeGraph MCP] Owned sockets or in-process engines did not stop within ${SHUTDOWN_BACKSTOP_MS}ms; exiting anyway.\n`);
      process.exit(1);
    }, SHUTDOWN_BACKSTOP_MS);
    const stopping = [engine, ...retiring].map((e) => Promise.resolve().then(() => {
      const retiringDrain = e && retirementDrains.get(e);
      if (retiringDrain) return retiringDrain;
      e?.cancelQuerySession?.(sessionId);
      return e?.stop();
    }));
    // An in-flight acquisition can create/return sockets after stdin closes.
    // Keep the process-owned drain armed until those producers and engines stop.
    // Selection can reveal proxy deps after EOF. Join it and every borrowed
    // engine/call before releasing that exact descriptor's reservation hook.
    const stopProxyResources = (async () => {
      await Promise.allSettled([selection, ...acquisitions, ...localCalls]);
      const drains = await Promise.allSettled(stopping);
      const failed = drains.filter((result): result is PromiseRejectedResult => result.status === 'rejected');
      // A failed native drain retains the exact caller reservation/resources.
      // Repeating engine.stop (possibly a no-op) cannot establish completion.
      if (failed.length) throw new AggregateError(failed.map(result => result.reason), 'Owned engine drain incomplete; caller resources retained');
      await deps?.stop?.();
    })();
    // Fence replacement immediately via shuttingDown, but retain the current
    // native protection until actual SQLite/resource work has drained.
    let resourceDrainIncomplete = false;
    const stopSupervision = Promise.allSettled([...stopping, stopPrepared, stopProxyResources])
      .then(results => {
        const failed = results.filter((result): result is PromiseRejectedResult => result.status === 'rejected');
        if (failed.length) {
          resourceDrainIncomplete = true;
          throw new AggregateError(failed.map(result => result.reason), 'Resource drain incomplete; native protection retained');
        }
        return stopWatchdog();
      });
    void Promise.allSettled([...acquisitions, ...stopping, stopPrepared, stopProxyResources, stopSupervision]).then(async results => {
      const socketError = await socketDrain.complete();
      const incomplete = results.filter(result => result.status === 'rejected');
      if (socketError !== undefined) incomplete.push({ status: 'rejected', reason: socketError });
      for (const result of incomplete) process.stderr.write(`[CodeGraph MCP] Intentional cleanup incomplete: ${String(result.reason)}.\n`);
      // Failed native/resource drain retains its leases/progress until the
      // already armed original backstop. Do not claim a clean early exit or
      // discard protection while a poisoned owned handle remains live.
      if (resourceDrainIncomplete) return;
      process.exit(incomplete.length ? 1 : 0);
    });
  };
  // Resolves the engine a call was started on, so a call in flight while the
  // engine is being retired still finishes on it.
  const ensureEngine = async (): Promise<MCPEngine> => {
    // shutdown() stops only the engines it can see; never start one after it.
    if (handoverFailure) throw handoverFailure.reason;
    if (!engine && shuttingDown) throw new Error('codegraph is shutting down');
    if (!engine) {
      engine = proxyDeps().makeEngine();
      engineReady = engine.ensureInitialized(proxyDeps().root).catch(error => {
        // This is initial engine acquisition, regardless of which canonical
        // own root a workspace/subfolder hint resolves to. Later explicit-target
        // refusals from an initialized handler remain scoped to that request.
        if (isServicePolicyRefusal(error)) { refuseAcquisition(error); throw error; }
        /* ordinary no-index/open failures retain degraded guidance */
      });
    }
    const current = engine;
    await engineReady;
    return current;
  };
  // Stop the in-process engine once the calls it is serving have answered.
  // Stopping it releases the writer lock if it held one.
  const retireEngine = async (): Promise<boolean> => {
    const retired = engine;
    engine = null;
    engineReady = null;
    if (!retired) return !handoverFailure;
    retiring.add(retired);
    const draining = (async () => {
      await Promise.allSettled([...localCalls]);
      if (retired.drain) await retired.drain();
      else await retired.stop();
      await proxyDeps().releaseEngine?.(retired);
    })();
    retirementDrains.set(retired, draining);
    try {
      await draining;
      retiring.delete(retired);
      return true;
    } catch (error) {
      // Latch before any new factory/dispatch. Keep this engine and its exact
      // rejected drain in the existing retiring lifetime through the backstop.
      if (!handoverFailure) handoverFailure = { reason: error };
      let message: string;
      try { message = (error instanceof Error ? error.message : String(error)).slice(0, 512); }
      catch { message = 'Owned engine cleanup incomplete (unprintable failure)'; }
      try { process.stderr.write(`[CodeGraph MCP] Fallback handover incomplete; owned engine retained: ${message}.\n`); }
      catch { /* the original drain remains latched and joined below */ }
      // These requests still belong to the proxy, never to a replacement
      // session. Settle once before shutdown fences output, without committing
      // explore history or inventing a query-policy refusal.
      for (const [id, request] of [...requests]) {
        if (requests.get(id) !== request) continue;
        requests.delete(id); inflight.delete(id);
        if (!request.controller.signal.aborted) writeClient({ jsonrpc: '2.0', id,
          error: { code: -32603, message, data: { code: 'CODEGRAPH_CLEANUP_INCOMPLETE' } } });
        request.controller.abort();
      }
      pending.length = 0;
      shutdown();
      return false;
    }
  };
  // Daemon-unavailable fallback: serve a client message in-process.
  const handleLocally = (line: string): Promise<void> => {
    if (daemonStatus === 'refused') { refuseLine(line); return Promise.resolve(); }
    const call = serveLocally(line);
    localCalls.add(call);
    const done = (): void => { localCalls.delete(call); };
    call.then(done, done);
    return call;
  };
  const serveLocally = async (line: string): Promise<void> => {
    let msg: JsonRpc; try { msg = JSON.parse(line) as JsonRpc; } catch { return; }
    const id = msg.id;
    const request = requests.get(id);
    if (shuttingDown || !liveLine(line) || request?.controller.signal.aborted) return;
    if (msg.method === 'tools/call' && id !== undefined) {
      try {
        const local = await ensureEngine();
        if (shuttingDown || !liveLine(line) || request?.controller.signal.aborted) return;
        const params = (msg.params || {}) as { name: string; arguments?: Record<string, unknown> };
        const context = createQueryRequestContext(local.getQueryExecutionContext(), sessionId, id as string | number);
        const result = await local.getToolHandler().execute(params.name, params.arguments || {}, exploreSession, context, request ? {
          signal: request.controller.signal, deferDelivery: commit => { request.commit = commit; },
        } : undefined);
        if (shuttingDown || !liveLine(line) || request?.controller.signal.aborted) return;
        const delivered = writeClient({ jsonrpc: '2.0', id, result });
        requests.delete(id);
        if (!delivered) return;
        request?.commit?.();
        getTelemetry().recordUsage('mcp_tool', params.name, !result.isError, telemetryClient);
      } catch (err) {
        if (shuttingDown || !liveLine(line) || request?.controller.signal.aborted) return;
        if (isServicePolicyRefusal(err)) {
          // Factory/primary refusal is terminal; a running engine's explicit
          // target refusal belongs only to this request and leaves alpha alive.
          let primary = true;
          try { primary = fs.realpathSync(proxyDeps().root) === err.projectRoot; } catch { /* unproved primary stays terminal */ }
          if (policyRefusal || !engine || primary) { if (!policyRefusal) refuseAcquisition(err); refuseLine(line); return; }
          writeClient({ jsonrpc: '2.0', id, error: { code: -32603, message: err.message,
            data: { code: err.code, reason: err.reason, projectRoot: err.projectRoot, guidance: err.guidance } } });
          requests.delete(id); inflight.delete(id);
          return;
        }
        writeClient({ jsonrpc: '2.0', id, error: { code: -32603, message: err instanceof Error ? err.message : String(err) } });
        requests.delete(id);
      }
    } else if (msg.method === 'ping' && id !== undefined) {
      writeClient({ jsonrpc: '2.0', id, result: {} });
      requests.delete(id);
    } else if (id !== undefined && msg.method !== 'initialize') {
      // A request we can't serve in-process (and the daemon is gone) — answer
      // with an error rather than let the host hang on a reply that won't come.
      writeClient({ jsonrpc: '2.0', id, error: { code: -32603, message: 'CodeGraph daemon unavailable' } });
      requests.delete(id);
    }
    // initialize already answered locally; notifications (initialized) need no reply.
  };
  const routeToDaemon = (line: string, parsed?: unknown): void => {
    if (shuttingDown) return;
    let request: ProvisionalRequest | undefined;
    try {
      const value = parsed === undefined ? JSON.parse(line) : parsed;
      const message = value && typeof value === 'object' ? value as JsonRpc : {};
      if (message.method === 'notifications/cancelled') {
        const id = (message.params as { requestId?: unknown } | undefined)?.requestId;
        requests.get(id)?.controller.abort();
        requests.delete(id); inflight.delete(id);
        for (let i = pending.length - 1; i >= 0; i--) if (!liveLine(pending[i]!)) pending.splice(i, 1);
      } else if (message.id !== undefined && message.method && message.method !== 'initialize') {
        if (requests.has(message.id)) return;
        request = { line, controller: new AbortController() };
        requests.set(message.id, request);
      }
    } catch { /* preserve ordinary malformed-message handling */ }
    if (selectedKind !== 'proxy') {
      if (daemonStatus === 'refused') { refuseLine(line, parsed); return; }
      readinessQueue.push({ line, parsed, request });
      pending.push(line);
      return;
    }
    if (daemonStatus === 'ready' && daemonSocket) {
      trackInflight(line);
      primeDaemonInitialize?.(line);
      if (process.env.CODEGRAPH_MCP_DEBUG) process.stderr.write(`[mcp-debug] proxy->daemon ${line.slice(0, 80)}\n`);
      try { daemonSocket.write(line.endsWith('\n') ? line : line + '\n'); } catch { /* close path */ }
    } else if (daemonStatus === 'refused') {
      refuseLine(line);
    } else if (daemonStatus === 'failed') {
      void handleLocally(line);
    } else {
      if (process.env.CODEGRAPH_MCP_DEBUG) process.stderr.write(`[mcp-debug] proxy-buffer(${daemonStatus}) ${line.slice(0, 80)}\n`);
      pending.push(line);
    }
  };

  // ---- client (stdin) ----
  let stdinBuf = '';
  process.stdin.setEncoding('utf8');
  process.stdin.on('data', (chunk: string) => {
    // Nothing is served once shutdown starts: a call now could start an engine
    // shutdown() never sees, and the process exits as soon as the others stop.
    if (shuttingDown) return;
    stdinBuf += chunk;
    let idx: number;
    while ((idx = stdinBuf.indexOf('\n')) !== -1) {
      const line = stdinBuf.slice(0, idx).trim();
      stdinBuf = stdinBuf.slice(idx + 1);
      if (!line) continue;
      let parsed: unknown;
      try { parsed = JSON.parse(line); }
      catch {
        if (selectedKind === 'proxy') routeToDaemon(line);
        else writeClient({ jsonrpc: '2.0', id: null, error: { code: ErrorCodes.ParseError, message: 'Parse error' } });
        continue;
      }
      if (directReady) { dispatchDirect(parsed); continue; }
      const msg = parsed && typeof parsed === 'object' ? parsed as JsonRpc : {};
      if (msg.method === 'initialize') {
        if (selectedKind !== 'proxy' && originalInitialize) { routeToDaemon(line, parsed); continue; }
        if (!originalInitialize) originalInitialize = msg;
        clientInitLine = line;
        locallyAnsweredInitializes.set(line, msg.id);
        const initParams = (msg.params ?? {}) as { clientInfo?: { name?: unknown; version?: unknown } };
        if (initParams.clientInfo) {
          telemetryClient = {
            name: typeof initParams.clientInfo.name === 'string' ? initParams.clientInfo.name : undefined,
            version: typeof initParams.clientInfo.version === 'string' ? initParams.clientInfo.version : undefined,
          };
        }
        const instructions = selectedKind === 'proxy' ? SERVER_INSTRUCTIONS : SERVER_INSTRUCTIONS_PENDING;
        writeClient({ jsonrpc: '2.0', id: msg.id, result: { protocolVersion: PROTOCOL_VERSION, capabilities: { tools: {} }, serverInfo: SERVER_INFO, instructions: initializeInstructions(instructions) } });
        routeToDaemon(line, parsed); // replay after selection; suppress only the one early reply
      } else if (msg.method === 'tools/list') {
        writeClient({ jsonrpc: '2.0', id: msg.id, result: { tools: getStaticTools() } });
      } else if (msg.method === 'resources/list') {
        // No resources exposed — answer the probe locally so it never reaches
        // the daemon as an unhandled method and logs `-32601`. (#621)
        writeClient({ jsonrpc: '2.0', id: msg.id, result: { resources: [] } });
      } else if (msg.method === 'resources/templates/list') {
        writeClient({ jsonrpc: '2.0', id: msg.id, result: { resourceTemplates: [] } });
      } else if (msg.method === 'prompts/list') {
        writeClient({ jsonrpc: '2.0', id: msg.id, result: { prompts: [] } });
      } else if (msg.method === 'server/discover' && msg.id !== undefined) {
        // Newer clients (Antigravity 2.5) probe this before `initialize` and wait
        // for the answer. We don't implement it; Method-not-found is what sends
        // them on to `initialize`. Answer locally — forwarded, it waited on the
        // daemon connection and was lost if the client closed stdin first. (#2084)
        writeClient({ jsonrpc: '2.0', id: msg.id, error: { code: ErrorCodes.MethodNotFound, message: 'Method not found: server/discover' } });
      } else {
        routeToDaemon(line, parsed);
      }
    }
  });
  // Shut down when stdin ends/closes — and also on a stdin `'error'`, which a
  // socket-backed stdin (the VS Code stdio shape) can emit on client death
  // instead of a clean close; destroying the stream stops a hung fd from
  // busy-spinning the event loop (#799).
  treatStdinFailureAsShutdown(shutdown);
  if ('prepare' in input) {
    // The prepared shell is the sole process supervisor. Its caller must not
    // retain MCPServer's old direct signal handlers or a second stdin loop.
    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);
  }
  startPpidWatchdogNoSocket(shutdown);
  // Backstop for a launch abandoned before any of the above can see it: killed
  // launcher + held-open pipes + reparent that beat the EARLY_PPID capture
  // (#1185). A server that never receives a single byte isn't serving anyone.
  // Armed after the stdin 'data' consumer above so no bytes are emitted while
  // only the backstop's listener exists.
  armStartupHandshakeTimeout(() => {
    process.stderr.write(
      '[CodeGraph MCP] No MCP traffic since startup; assuming an abandoned launch and shutting down (#1185). ' +
      'Tune with CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS (0 disables).\n'
    );
    shutdown();
  });

  // ---- daemon connection (background) ----
  const connectDaemon = (): Promise<net.Socket | null> => {
    if (shuttingDown || policyRefusal || selectedKind !== 'proxy') return Promise.resolve(null);
    const acquisition = (async (): Promise<net.Socket | null> => {
      let socket: net.Socket | null = null;
      try { socket = await proxyDeps().getDaemonSocket(); }
      catch (error) {
        if (isServicePolicyRefusal(error)) refuseAcquisition(error);
        socket = null;
      }
      if (!socket) return null;
      observeOwnedSocket(socket);
      if (shuttingDown) { try { socket.destroy(); } catch { /* host shutdown */ } return null; }
      const observed = socket;
      const onUnprovedLoss = (): void => refuseBeforeAttachment(observed);
      const release = (): void => {
        observed.removeListener('error', onUnprovedLoss);
        observed.removeListener('close', onUnprovedLoss);
        attachmentGuards.delete(observed);
      };
      attachmentGuards.set(observed, release);
      observed.once('error', onUnprovedLoss);
      observed.once('close', onUnprovedLoss);
      // Listeners precede this recheck: close may already have happened before
      // the dependency returned, or may arrive before attachDaemon takes over.
      if (socketUnusableForAttachment(observed)) {
        refuseBeforeAttachment(observed);
        return null;
      }
      return observed;
    })();
    acquisitions.add(acquisition);
    void acquisition.then(() => { acquisitions.delete(acquisition); }, () => { acquisitions.delete(acquisition); });
    return acquisition;
  };
  const refuseBeforeAttachment = (socket: net.Socket): void => {
    attachmentGuards.get(socket)?.();
    if (daemonSocket === socket) daemonSocket = null;
    try { socket.destroy(); } catch { /* refusal still settles pending work */ }
    if (!shuttingDown && !policyRefusal) refuseAcquisition(servicePolicyRefusal('unknown', proxyDeps().root,
      'the verified shared-service socket closed before attachment'));
  };
  // Every buffered call binds to the engine now, so a later retirement waits for all of them.
  const serveBufferedLocally = (): void => {
    for (const line of pending.splice(0)) if (liveLine(line)) void handleLocally(line);
  };
  const scheduleDaemonRetry = (): void => {
    if (shuttingDown || selectedKind !== 'proxy' || daemonStatus === 'refused' || retryTimer || retryBaseMs <= 0) return;
    const delay = retryDelayMs;
    retryDelayMs = Math.min(retryDelayMs * 2, retryMaxMs);
    retryTimer = setTimeout(() => { retryTimer = null; void retryDaemon(); }, delay);
    retryTimer.unref?.();
  };
  const retryDaemon = async (): Promise<void> => {
    if (shuttingDown || selectedKind !== 'proxy' || daemonStatus !== 'failed') return;
    // While a writing engine owns writer.pid no daemon can start (#2277). Hand
    // the lock back first, once the calls it is serving have answered, and
    // buffer new calls until a daemon answers or we resume in-process. One
    // writer at a time: the engine is fully stopped before a daemon is spawned.
    const handover = engine !== null && !engine.isReadOnly();
    if (handover) {
      daemonStatus = 'connecting';
      if (!await retireEngine()) return; // failed drain already owns terminal shutdown
    }
    const socket = await connectDaemon();
    if (shuttingDown || policyRefusal) return;
    if (socket) {
      process.stderr.write('[CodeGraph MCP] Shared daemon reachable; proxying this session to it instead of serving in-process.\n');
      attachDaemon(socket, true);
      // A read-only engine holds no lock: let its calls finish, then close it.
      if (!handover) void retireEngine();
      return;
    }
    daemonStatus = 'failed';
    serveBufferedLocally();
    scheduleDaemonRetry();
  };

  const attachDaemon = (socket: net.Socket, replayInitialize: boolean): void => {
    if (shuttingDown || policyRefusal || socketUnusableForAttachment(socket)) {
      refuseBeforeAttachment(socket);
      return;
    }
    daemonSocket = socket;
    daemonStatus = 'ready';
    attachedAt = Date.now();
    let sockBuf = '';
    // Count only initializes actually written to THIS attachment. Consuming
    // one reply cannot suppress a later ping/tool that reuses its wire ID.
    const initializeReplies = new Map<unknown, number>();
    primeDaemonInitialize = (line: string): void => {
      if (daemonSocket !== socket || !locallyAnsweredInitializes.has(line)) return;
      const id = locallyAnsweredInitializes.get(line);
      initializeReplies.set(id, (initializeReplies.get(id) ?? 0) + 1);
    };
    // Preserve any pending raw UTF8 prefix decoded after hello by the transport.
    if (socket.readableEncoding !== 'utf8') socket.setEncoding('utf8');
    socket.on('data', (chunk: string) => {
      if (shuttingDown || daemonSocket !== socket) return; // a retired attachment cannot answer a replacement
      sockBuf += chunk;
      let idx: number;
      while ((idx = sockBuf.indexOf('\n')) !== -1) {
        const line = sockBuf.slice(0, idx);
        sockBuf = sockBuf.slice(idx + 1);
        if (!line.trim()) continue;
        let resp: JsonRpc | null = null;
        try { resp = JSON.parse(line) as JsonRpc; } catch { /* not JSON — relay verbatim */ }
        if (process.env.CODEGRAPH_MCP_DEBUG) process.stderr.write(`[mcp-debug] daemon->proxy ${line.slice(0, 80)}\n`);
        if (resp && resp.id !== undefined && ('result' in resp || 'error' in resp)) {
          // Suppress the daemon's reply to the initialize we forwarded to prime it
          // (the client already got the local handshake response).
          const remaining = initializeReplies.get(resp.id) ?? 0;
          if (remaining) {
            if (remaining === 1) initializeReplies.delete(resp.id);
            else initializeReplies.set(resp.id, remaining - 1);
            continue;
          }
          const forwarded = inflight.get(resp.id);
          if (!forwarded || !liveLine(forwarded)) continue; // cancelled/duplicate/old-generation response
          inflight.delete(resp.id);
          requests.delete(resp.id);
        }
        writeClient(line);
      }
    });
    // Losing a transport does not establish service unavailability: the same
    // live owner may still be reachable, with an unknown/incompatible policy.
    // Buffer orphaned and new work until acquisition classifies it, even when
    // periodic fallback retries are disabled. Only genuine unavailability may
    // replay work through an exceptional local engine.
    const onDaemonLost = (): void => {
      if (shuttingDown || daemonSocket !== socket) return; // host teardown, or already handled
      daemonStatus = 'connecting';
      daemonSocket = null;
      primeDaemonInitialize = null;
      try { socket.destroy(); } catch { /* ignore */ }
      process.stderr.write(
        `[CodeGraph MCP] Shared daemon connection lost; rechecking service compatibility before replaying ${inflight.size} in-flight request(s).\n`
      );
      const orphaned = [...inflight.values()];
      inflight.clear();
      pending.unshift(...orphaned.filter(liveLine));
      // A link that stayed up past the longest wait starts the backoff over; one
      // that keeps dropping right after it attaches keeps backing off.
      if (Date.now() - attachedAt >= retryMaxMs) retryDelayMs = retryBaseMs;
      void (async () => {
        const replacement = await connectDaemon();
        if (shuttingDown || policyRefusal) return;
        if (replacement) {
          attachDaemon(replacement, true);
          return;
        }
        daemonStatus = 'failed';
        process.stderr.write('[CodeGraph MCP] Shared daemon unavailable; serving this session in-process (degraded).\n');
        serveBufferedLocally();
        scheduleDaemonRetry();
      })();
    };
    socket.on('close', onDaemonLost);
    socket.on('error', onDaemonLost);
    if (socketUnusableForAttachment(socket)) {
      refuseBeforeAttachment(socket);
      return;
    }
    // The attached-session loss handlers now own this exact usable socket.
    attachmentGuards.get(socket)?.();
    // A daemon that came up mid-session has never seen this client: prime its
    // session with the client's own initialize, whose reply is suppressed
    // above like the first one.
    if (replayInitialize && clientInitLine !== undefined) {
      primeDaemonInitialize(clientInitLine);
      try { socket.write(clientInitLine + '\n'); } catch { /* close path */ }
    }
    for (const line of pending) {
      if (!liveLine(line)) continue;
      trackInflight(line);
      primeDaemonInitialize(line);
      if (process.env.CODEGRAPH_MCP_DEBUG) process.stderr.write(`[mcp-debug] proxy-flush ${line.slice(0, 80)}\n`);
      try { socket.write(line + '\n'); } catch { /* ignore */ }
    }
    pending.length = 0;
    // The hello reader paused before unshifting any glued RPC tail. Every
    // relay/loss handler and buffered request identity is now installed.
    socket.resume();
  };

  const selectPrepared = async (): Promise<void> => {
    if ('prepare' in input) {
      const prepared = await input.prepare(preparationLifetime.signal, {
        resourceObserver,
        onInitialDefaultOutcome: outcome => {
          if (outcome.kind === 'refused') refuseAcquisition(outcome.refusal);
        },
      });
      if (prepared.kind === 'direct') {
        selectedKind = 'direct'; direct = prepared;
        if (shuttingDown || preparationLifetime.signal.aborted) { stopDirectResources(); return; }
        if (policyRefusal) { await stopDirectResources(); return; }
        activation = (async () => {
          let suppressInitialize = false;
          directTransport = new class extends ParsedMessageTransport {
            override send(response: JsonRpcResponse): boolean {
              if (suppressInitialize && response.id === originalInitialize?.id) {
                suppressInitialize = false;
                return true;
              }
              return super.send(response);
            }
          }(writeClient);
          let factoryProgress: IndexProgressLease | null = null;
          let factoryFailed = false;
          try {
            if (prepared.route) factoryProgress = await resourceObserver.acquireIndexProgress(prepared.route, preparationLifetime.signal);
            if (shuttingDown || preparationLifetime.signal.aborted || policyRefusal) return;
            // Actual factory/engine admission must refresh after the observer
            // await. This temporary ref covers known-route factory work; its
            // real project leases cover subsequent work and late selections.
            directSession = await prepared.makeSession(directTransport, preparationLifetime.signal);
          } catch (primary) { factoryFailed = true; throw primary; }
          finally {
            try { await factoryProgress?.release(); }
            catch (cleanup) {
              if (!factoryFailed) throw cleanup;
              try { process.stderr.write(`[CodeGraph MCP] Factory progress cleanup incomplete: ${String(cleanup).slice(0, 512)}.\n`); }
              catch { /* preserve actual factory failure; native loss stays sticky */ }
            }
          }
          if (shuttingDown || preparationLifetime.signal.aborted) return; // cached stop joins/disposes this late session
          directSession.start();
          if (policyRefusal) directSession.refuseInitialDefault(policyRefusal);
          if (originalInitialize) {
            suppressInitialize = true;
            try { await directTransport.accept(originalInitialize); }
            finally { suppressInitialize = false; }
          }
          if (shuttingDown || preparationLifetime.signal.aborted) return;
          // Extract survivors before relinquishing provisional bookkeeping.
          // The real session alone owns controllers and explore history now.
          const ready = readinessQueue.splice(0).filter(liveReadiness);
          requests.clear(); inflight.clear(); pending.length = 0;
          directReady = true;
          for (const item of ready) if (item.parsed !== originalInitialize) dispatchDirect(item.parsed);
        })();
        await activation;
        return;
      }
      selectedKind = 'proxy'; deps = prepared.deps;
      if (shuttingDown || preparationLifetime.signal.aborted) return; // cached outer shutdown joins this late hook
      if (policyRefusal) return;
      readinessQueue.length = 0;
    }
    if (shuttingDown || preparationLifetime.signal.aborted) return;
    const socket = await connectDaemon();
    if (socket) attachDaemon(socket, false);
    else if (!shuttingDown && !policyRefusal) {
      daemonStatus = 'failed';
      process.stderr.write('[CodeGraph MCP] Shared daemon unavailable; serving this session in-process (degraded).\n');
      serveBufferedLocally(); scheduleDaemonRetry();
    }
  };
  // Parser and ONE outer supervision lifetime precede this handled promise.
  // A failed direct factory is terminal; it never enters daemon/fallback code.
  selection = Promise.resolve().then(selectPrepared);
  void selection.catch(async error => {
    if (shuttingDown) return;
    refuseAcquisition(isServicePolicyRefusal(error) ? error :
      servicePolicyRefusal('unknown', rootHint, `prepared MCP route failed: ${error instanceof Error ? error.message : String(error)}`));
    if (direct) {
      try { await stopDirect(); }
      catch (cleanupError) { process.stderr.write(`[CodeGraph MCP] Direct resource cleanup incomplete: ${String(cleanupError)}.\n`); shutdown(); }
    }
  });

  await new Promise<void>(() => { /* stdin keeps the loop alive; exit via shutdown() */ });
}

/** PPID watchdog for the local-handshake proxy — same #277 logic as
 *  {@link startPpidWatchdog} but with no socket to close (the caller's shutdown
 *  handles teardown). */
function startPpidWatchdogNoSocket(onDeath: () => void): void {
  const pollMs = parsePollMs(process.env.CODEGRAPH_PPID_POLL_MS);
  if (pollMs <= 0) return;
  // Baseline from the CLI entry's earliest capture, not process.ppid here —
  // a launcher killed during our first ~100ms would otherwise leave the
  // baseline at 1 and blind the divergence check forever (#1185).
  const originalPpid = EARLY_PPID;
  const hostPpid = parseHostPpid(process.env[HOST_PPID_ENV]);
  const timer = setInterval(() => {
    const reason = supervisionLostReason({
      originalPpid,
      currentPpid: process.ppid,
      hostPpid,
      isAlive: isProcessAliveLocal,
    });
    if (reason) {
      process.stderr.write(`[CodeGraph MCP] Parent process exited (${reason}); shutting down.\n`);
      onDeath();
    }
  }, pollMs);
  timer.unref?.();
}

/**
 * Read one CRLF/LF-terminated JSON line from the socket, parse it as the
 * daemon hello, and return it. Bounded to {@link MAX_HELLO_LINE_BYTES} so a
 * malicious or broken peer can't OOM us. Times out at 3s — a healthy daemon
 * sends hello immediately on accept.
 */
function readHelloLine(socket: net.Socket): Promise<DaemonHello> {
  return new Promise((resolve, reject) => {
    let buffer = Buffer.alloc(0);
    const cleanup = () => {
      socket.removeListener('data', onData);
      socket.removeListener('error', onError);
      socket.removeListener('close', onClose);
      clearTimeout(timer);
    };
    const onData = (chunk: Buffer) => {
      if (!Buffer.isBuffer(chunk)) {
        cleanup();
        reject(new Error('daemon hello transport did not provide raw bytes'));
        return;
      }
      const idx = chunk.indexOf(10);
      const prefix = idx === -1 ? chunk : chunk.subarray(0, idx);
      // Count RAW bytes before LF, including CR and incomplete UTF8 prefixes.
      // The check also applies when LF is in this same received chunk; bytes
      // after LF belong to the next consumer and do not consume this budget.
      if (buffer.length + prefix.length > MAX_HELLO_LINE_BYTES) {
        cleanup();
        reject(new Error('daemon hello line exceeded size limit'));
        return;
      }
      buffer = Buffer.concat([buffer, prefix]);
      if (idx === -1) return;
      const line = buffer.toString('utf8');
      const tail = chunk.subarray(idx + 1);
      // Removing data listeners does not pause a flowing stream. Preserve the
      // exact raw tail until the verified caller installs all its handlers.
      socket.pause();
      cleanup();
      if (tail.length > 0) {
        // Push back via unshift — Node's net.Socket supports it on readable streams.
        socket.unshift(tail);
      }
      try {
        const parsed = JSON.parse(line) as DaemonHello;
        if (typeof parsed.codegraph !== 'string' || typeof parsed.pid !== 'number') {
          reject(new Error('daemon hello missing required fields'));
          return;
        }
        resolve(parsed);
      } catch (err) {
        reject(new Error(`daemon hello not JSON: ${err instanceof Error ? err.message : String(err)}`));
      }
    };
    const onError = (err: Error) => { cleanup(); reject(err); };
    const onClose = () => { cleanup(); reject(new Error('daemon closed connection before hello')); };
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error('timed out waiting for daemon hello'));
    }, 3000);
    timer.unref?.();
    socket.on('data', onData);
    socket.on('error', onError);
    socket.on('close', onClose);
  });
}

/**
 * Pipe stdin → socket and socket → stdout. Resolves once either end closes
 * so the process can exit. Note: we deliberately do NOT use
 * `process.stdin.pipe(socket)` because pipe propagates 'end' onto the
 * downstream, which would close the socket prematurely if stdin happens to
 * end early — the MCP spec allows it to stay open across reconnects.
 */
function pipeUntilClose(socket: net.Socket): Promise<void> {
  return new Promise((resolve) => {
    let resolved = false;
    const done = () => { if (!resolved) { resolved = true; resolve(); } };

    process.stdin.on('data', (chunk) => {
      try { socket.write(chunk); } catch { /* socket may have errored — close path catches it */ }
    });
    process.stdin.on('end', () => {
      try { socket.end(); } catch { /* ignore */ }
      done();
    });
    // 'close' and 'error' both tear down: a socket-backed stdin can fail with
    // an 'error' (ECONNRESET/hangup) rather than a clean close; destroying it
    // stops a hung fd from busy-spinning the event loop (#799).
    const teardown = () => {
      try { process.stdin.destroy(); } catch { /* ignore */ }
      try { socket.destroy(); } catch { /* ignore */ }
      done();
    };
    process.stdin.on('close', teardown);
    process.stdin.on('error', teardown);

    socket.on('data', (chunk) => {
      try { process.stdout.write(chunk); } catch { /* ignore */ }
    });
    socket.on('end', () => done());
    socket.on('close', () => done());
    socket.on('error', (err) => {
      process.stderr.write(`[CodeGraph MCP] daemon socket error: ${err.message}\n`);
      done();
    });
    socket.resume();
  });
}

/**
 * PPID watchdog mirroring the one in `MCPServer.start` — kills the proxy if
 * the MCP host (or its proxy of a host, see HOST_PPID_ENV) goes away without
 * closing stdin. Issue #277 documents why we can't rely on stdin EOF on
 * Linux: the parent may be SIGKILL'd and reparenting doesn't close pipes.
 *
 * The proxy's "kill" is just a socket close + process.exit — no SQLite or
 * watchers to clean up, so this is cheap.
 */
function startPpidWatchdog(socket: net.Socket): void {
  const pollMs = parsePollMs(process.env.CODEGRAPH_PPID_POLL_MS);
  if (pollMs <= 0) return;
  // Baseline from the CLI entry's earliest capture, not process.ppid here —
  // a launcher killed during our first ~100ms would otherwise leave the
  // baseline at 1 and blind the divergence check forever (#1185).
  const originalPpid = EARLY_PPID;
  const hostPpid = parseHostPpid(process.env[HOST_PPID_ENV]);
  const timer = setInterval(() => {
    const reason = supervisionLostReason({
      originalPpid,
      currentPpid: process.ppid,
      hostPpid,
      isAlive: isProcessAliveLocal,
    });
    if (reason) {
      process.stderr.write(`[CodeGraph MCP] Parent process exited (${reason}); shutting down.\n`);
      try { socket.destroy(); } catch { /* ignore */ }
      process.exit(0);
    }
  }, pollMs);
  timer.unref?.();
}

function parsePollMs(raw: string | undefined): number {
  return parseDelayMs(raw, DEFAULT_PPID_POLL_MS);
}

/** A non-negative millisecond env value; unset or malformed gives `fallback`. */
function parseDelayMs(raw: string | undefined, fallback: number): number {
  if (raw === undefined || raw === '') return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed)) return fallback;
  if (parsed < 0) return fallback;
  return Math.floor(parsed);
}

function parseHostPpid(raw: string | undefined): number | null {
  if (raw === undefined || raw === '') return null;
  const parsed = Number(raw);
  if (!Number.isInteger(parsed) || parsed <= 1) return null;
  return parsed;
}

function isProcessAliveLocal(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (err: unknown) {
    const e = err as NodeJS.ErrnoException;
    if (e.code === 'EPERM') return true;
    return false;
  }
}
