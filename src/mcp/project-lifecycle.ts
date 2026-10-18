/** Shared native project resources and writer ownership for MCP engines. */
import type { Socket } from 'net';
import { setTimeout as waitForStartup } from 'node:timers/promises';
import { readFileSync, realpathSync } from 'fs';
import * as path from 'path';
import type CodeGraph from '../index';
import { admitMcpRoute, assertMcpIndexRoute, canonicalProjectRoot, type McpIndexRoute } from '../directory';
import { LockUnavailableError, watchDisabledReason, type PendingFile } from '../sync';
import { canProbeDaemonIdentity, getDaemonSocketCandidates, type DaemonLockInfo } from './daemon-paths';
import { connectForProjectFreshness, connectWithHello } from './proxy';
import type { ResolvedQueryPolicy } from './query-policy';
import { isServicePolicyRefusal, servicePolicyRefusal } from './query-service-compatibility';
import { assertNoLiveQueryDaemon, assertReadableQueryOwners, readQueryServiceOwners, type QueryServiceOwners } from './query-service-owner';
import { CodeGraphPackageVersion } from './version';
import { assertNoRebuild, decodeWriterLockInfo, getWriterPidPath, markWriterReady, readWriterLock, releaseWriterLock, tryAcquireWriterLock, type WriterLockInfo } from './writer-lock';

export interface IndexProgressLease { release(): Promise<void> }
/** Observes actual work on [route.index, `${route.index}-wal`], never permission. */
export interface ProjectResourceObserver {
  acquireIndexProgress(route: McpIndexRoute, signal?: AbortSignal): Promise<IndexProgressLease>;
}
export interface ProjectWriterReservation { readonly route: McpIndexRoute; release(): Promise<void> }
/** The actual holder exists before acquisition can await or open native work. */
export interface ProjectAcquisition {
  readonly route: McpIndexRoute;
  beginDrain(): void;
  release(): Promise<void>;
}
export interface ProjectWatchState {
  readonly active: boolean; readonly degraded: boolean; readonly recovering: boolean;
  readonly reason: string | null; readonly pending: PendingFile[];
}
export interface ProjectRequest {
  readonly route: McpIndexRoute;
  readonly intent: 'read' | 'catch-up' | 'watch';
  readonly lifetime: AbortSignal;
  /** Acquisition cancellation only; a published cache lease uses lifetime. */
  readonly signal?: AbortSignal;
  readonly watchOptions: Parameters<CodeGraph['watch']>[0];
  readonly admit: (route: McpIndexRoute, signal?: AbortSignal) => Promise<void>;
  readonly resourceObserver?: ProjectResourceObserver;
  readonly startupOwner?: ProjectWriterReservation;
  /** Synchronous ownership handoff only; does not publish readiness or a graph. */
  readonly retain?: (acquisition: ProjectAcquisition) => void;
}
export interface ProjectLease {
  readonly route: McpIndexRoute;
  /** Exact same module-owned holder registered before acquisition's first wait. */
  readonly acquisition: ProjectAcquisition;
  /** Stable actual read-only graph, including while ownership changes. */
  readonly cg: CodeGraph;
  ready(reconcile?: boolean): Promise<void>;
  watchState(): ProjectWatchState;
  rearmWatcher(): boolean;
  /** Fence this holder's new activation/watch work; retain admitted drain. */
  beginDrain(): void;
  release(): Promise<void>;
}
interface LeaseState { readonly request: ProjectRequest; progress: IndexProgressLease | null; released: boolean; published: boolean; draining: boolean }
interface Project {
  readonly key: string; readonly route: McpIndexRoute;
  reader: CodeGraph | null; writer: CodeGraph | null; owner: WriterLockInfo | null;
  ownerPath: string | null; ownerLocator: string | null;
  readonly leases: Set<LeaseState>; reservations: number;
  caughtUp: boolean; watchAttempted: boolean;
  retirement: Promise<void> | null; gate: Promise<void> | null;
  opening: Promise<void> | null;
  failure: Error | null;
  socket: Socket | null; timer: NodeJS.Timeout | null;
}
interface ReservationState { readonly project: Project; released: boolean }
const projects = new Map<string, Project>();
const reservations = new WeakMap<ProjectWriterReservation, ReservationState>();
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
function loadCodeGraph(): typeof import('../index').default {
  const module: typeof import('../index') = require('../index'); return module.default;
}
function held(route: McpIndexRoute): McpIndexRoute { return Object.freeze({ root: route.root, index: route.index }); }
function sameOwner(left: WriterLockInfo | null, right: WriterLockInfo | null): boolean {
  return !!left && !!right && left.pid === right.pid && left.mode === right.mode && left.startedAt === right.startedAt;
}
function writerLocator(pidPath: string): string { return path.join(realpathSync(path.dirname(pidPath)), path.basename(pidPath)); }
function cancelled(): Error { const error = new Error('CodeGraph project acquisition cancelled or abandoned.'); error.name = 'AbortError'; return error; }
function alive(state: LeaseState): boolean {
  return !state.released && !state.request.lifetime.aborted && (state.published || !state.request.signal?.aborted);
}
function live(state: LeaseState): void { if (!alive(state)) throw cancelled(); }
function writeDemand(project: Project): LeaseState | undefined {
  return [...project.leases].find(state => alive(state) && !state.draining && state.request.intent !== 'read');
}
function retainedWriterDemand(project: Project): LeaseState | undefined {
  return [...project.leases].find(state => alive(state) && state.request.intent !== 'read');
}
function watchDemand(project: Project): LeaseState | undefined {
  return [...project.leases].find(state => alive(state) && !state.draining && state.request.intent === 'watch');
}
function entryFor(route: McpIndexRoute): Project {
  assertMcpIndexRoute(route);
  return identityEntry(route);
}
/** Identity storage only; failed startup cleanup may need it after FS drift. */
function identityEntry(route: McpIndexRoute): Project {
  const key = canonicalProjectRoot(route.root), existing = projects.get(key);
  if (existing) {
    if (existing.failure) throw existing.failure;
    if (existing.route.root !== route.root || existing.route.index !== route.index) {
      throw servicePolicyRefusal('unknown', route.root, 'the live project resource holds a different own index');
    }
    return existing;
  }
  const project: Project = { key, route: held(route), reader: null, writer: null, owner: null, ownerPath: null, ownerLocator: null,
    leases: new Set(), reservations: 0, caughtUp: false, watchAttempted: false,
    retirement: null, gate: null, opening: null, failure: null, socket: null, timer: null };
  projects.set(key, project); return project;
}
function assertOwner(project: Project): void {
  assertMcpIndexRoute(project.route); assertNoRebuild(project.route.root);
  const owners = readQueryServiceOwners(project.route.root);
  assertReadableQueryOwners(project.route.root, owners);
  if (owners.daemon.kind === 'live' && owners.daemon.value.pid !== process.pid) {
    throw servicePolicyRefusal('unknown', project.route.root, 'a foreign daemon cannot authorize this retained local writer');
  }
  if (!project.owner || project.owner.pid !== process.pid || !project.ownerPath || !project.ownerLocator
    || project.ownerPath !== getWriterPidPath(project.route.root)
    || project.ownerLocator !== writerLocator(project.ownerPath)
    || !sameOwner(project.owner, readWriterLock(project.route.root))) {
    throw servicePolicyRefusal('unknown', project.route.root, 'the retained native writer identity is no longer owned');
  }
}

/** Explicit provenance for authority acquired by the actual startup caller. */
export function retainStartupProjectWriter(route: McpIndexRoute, acquired: WriterLockInfo): ProjectWriterReservation {
  const project = entryFor(route); assertNoRebuild(route.root);
  const pidPath = getWriterPidPath(route.root), locator = writerLocator(pidPath);
  if (acquired.pid !== process.pid || !['direct', 'fallback', 'daemon'].includes(acquired.mode)
    || !sameOwner(acquired, readWriterLock(route.root)) || (project.owner && !sameOwner(project.owner, acquired))
    || (project.ownerPath && project.ownerPath !== pidPath) || (project.ownerLocator && project.ownerLocator !== locator)) {
    throw servicePolicyRefusal('unknown', route.root, 'startup did not retain the exact acquired native writer');
  }
  project.owner = Object.freeze({ ...acquired }); project.ownerPath = pidPath; project.ownerLocator = locator; project.reservations++;
  const state: ReservationState = { project, released: false };
  let released: Promise<void> | null = null;
  const reservation: ProjectWriterReservation = Object.freeze({ route: project.route, release: () => released ??= (async () => {
    if (state.released) return;
    if (project.failure) throw project.failure;
    state.released = true; project.reservations--;
    try { if (!retainedWriterDemand(project) && !project.reservations) await retire(project); }
    catch (error) { state.released = false; project.reservations++; throw error; }
  })() });
  reservations.set(reservation, state); return reservation;
}

/** Internal provenance check; identity only, with no I/O or permission capture. */
export function assertProjectWriterReservation(reservation: ProjectWriterReservation): void {
  const state = reservations.get(reservation);
  if (!state || state.released || state.project.failure || reservation.route !== state.project.route) {
    throw new Error('Startup writer reservation is not a live module-owned resource.');
  }
}

/** Fallible rollback of only this synchronous startup's native acquisition. */
export function rollbackStartupProjectWriter(route: McpIndexRoute,
  acquired: Extract<ReturnType<typeof tryAcquireWriterLock>, { kind: 'acquired' }>,
  observed: { readonly pidPath: string; readonly locator: string; readonly previous: WriterLockInfo | null }): 'borrowed' | 'released' {
  // A failed retain may have left no entry, or its held index may have changed.
  // Retain cleanup identity in the SAME map; this grants no open/admission.
  const project = projects.get(canonicalProjectRoot(route.root)) ?? identityEntry(route);
  if (project.failure) throw project.failure;
  try {
    if (project.route.root !== route.root || project.route.index !== route.index
      || acquired.info.pid !== process.pid || !Number.isInteger(acquired.info.pid)
      || !['direct', 'fallback', 'daemon'].includes(acquired.info.mode)
      || !Number.isFinite(acquired.info.startedAt) || acquired.info.startedAt < 0) {
      throw new Error('Startup rollback cannot prove the acquired route and native identity.');
    }
    const borrower = observed.previous?.pid === process.pid || project.owner !== null;
    if (borrower) {
      if ((observed.previous?.pid === process.pid && !sameOwner(observed.previous, acquired.info))
        || (project.owner && (!sameOwner(project.owner, acquired.info)
          || project.ownerPath !== acquired.pidPath || project.ownerLocator !== observed.locator))) {
        throw new Error('Startup rollback cannot replace a different retained module borrower.');
      }
    } else {
      // Save the real acquisition receipt before fallible locator validation.
      project.owner = Object.freeze({ ...acquired.info }); project.ownerPath = acquired.pidPath; project.ownerLocator = observed.locator;
    }
    if (acquired.pidPath !== observed.pidPath || getWriterPidPath(route.root) !== observed.pidPath
      || writerLocator(observed.pidPath) !== observed.locator) {
      throw new Error('Startup rollback retained authority: exact native locator changed.');
    }
    if (borrower) return 'borrowed';
    releaseOwnedWriter(project, 'rollback');
    project.owner = null; project.ownerPath = null; project.ownerLocator = null;
    if (!project.leases.size && !project.reservations && !project.reader && !project.writer
      && !project.gate && projects.get(project.key) === project) projects.delete(project.key);
    return 'released';
  } catch (secondary) {
    project.failure ??= new AggregateError([secondary, Object.freeze({ route: held(route),
      acquired: Object.freeze({ ...acquired, info: Object.freeze({ ...acquired.info }) }),
      observed: Object.freeze({ ...observed, previous: observed.previous ? Object.freeze({ ...observed.previous }) : null }) })],
    'CodeGraph startup writer rollback failed; native authority retained');
    throw project.failure;
  }
}

/** Mandatory caller-specific permission. Shared resources cache no verdict. */
export async function admitQueryProject(root: string, policy: ResolvedQueryPolicy, signal?: AbortSignal): Promise<void> {
  // Match index.ts's existing 240 x 25ms bind allowance and 10s election grace.
  // This is readiness observation only: no policy permission until the ordinary
  // hello, implementation, project and actual-owner checks all succeed.
  const bindDeadline = performance.now() + 240 * 25;
  let starting: { daemon: DaemonLockInfo; writer: WriterLockInfo | null } | undefined;
  const observeOwners = (): QueryServiceOwners => {
    const owners = readQueryServiceOwners(root); assertReadableQueryOwners(root, owners);
    if (starting) {
      const { daemon, writer } = owners;
      // Bind refreshes daemon.startedAt, but may not switch service identity.
      // Once a writer appears, its reservation identity must stay fixed too.
      if (daemon.kind !== 'live' || daemon.value.pid !== starting.daemon.pid
        || daemon.value.version !== starting.daemon.version || daemon.value.socketPath !== starting.daemon.socketPath
        || (writer.kind !== 'absent' && (writer.kind !== 'live' || writer.value.pid !== starting.daemon.pid
          || writer.value.mode !== 'daemon' || !Number.isFinite(writer.value.startedAt) || writer.value.startedAt <= 0))
        || (starting.writer && (writer.kind !== 'live' || !sameOwner(starting.writer, writer.value)))) {
        throw servicePolicyRefusal('unknown', root, 'the observed starting service ownership changed before compatible attachment');
      }
      if (writer.kind === 'live') starting.writer ??= writer.value;
    }
    return owners;
  };
  for (;;) {
    if (signal?.aborted) throw cancelled();
    const owners = observeOwners();
    const candidates = getDaemonSocketCandidates(root);
    if (owners.daemon.kind === 'live' && canProbeDaemonIdentity(owners.daemon.value)) candidates.unshift(owners.daemon.value.socketPath);
    for (const candidate of new Set(candidates)) {
      if (signal?.aborted) throw cancelled();
      const socket = await connectWithHello(candidate, undefined, policy, root);
      if (!socket) continue;
      socket.destroy(); if (signal?.aborted) throw cancelled(); observeOwners(); return;
    }
    if (signal?.aborted) throw cancelled();
    const current = observeOwners(), daemon = current.daemon, writer = current.writer;
    if (!starting && daemon.kind === 'live' && daemon.value.pid !== process.pid
      && daemon.value.version === CodeGraphPackageVersion && canProbeDaemonIdentity(daemon.value)
      && Number.isFinite(daemon.value.startedAt) && daemon.value.startedAt > 0
      && Date.now() >= daemon.value.startedAt && Date.now() - daemon.value.startedAt < 10_000
      && (writer.kind === 'absent' || (writer.kind === 'live' && writer.value.pid === daemon.value.pid
        && writer.value.mode === 'daemon' && Number.isFinite(writer.value.startedAt) && writer.value.startedAt > 0))) {
      starting = { daemon: daemon.value, writer: writer.kind === 'live' ? writer.value : null };
    }
    if (!starting || performance.now() >= bindDeadline) {
      assertNoLiveQueryDaemon(root, current); return;
    }
    await waitForStartup(Math.min(25, bindDeadline - performance.now()), undefined, { signal });
  }
}

/** One existing coordinator; actual query readers never become writers. */
export async function acquireProject(request: ProjectRequest): Promise<ProjectLease> {
  const route = held(request.route), project = entryFor(route);
  const state: LeaseState = { request: { ...request, route }, progress: null, released: false, published: false, draining: false };
  project.leases.add(state);
  let released: Promise<void> | null = null;
  const acquisition: ProjectAcquisition = Object.freeze({ route: project.route,
    beginDrain: () => beginDrain(project, state), release: () => released ??= releaseState(project, state) });
  try {
    // The same holder, including its cached failed release, is visible to the
    // engine before observer/admission/native work can lose its disposition.
    request.retain?.(acquisition);
    live(state);
    if (request.startupOwner) {
      const startup = reservations.get(request.startupOwner);
      if (!startup || startup.released || startup.project !== project) {
        throw servicePolicyRefusal('unknown', route.root, 'project writer reservation is not a live exact-route startup owner');
      }
    }
    state.progress = await request.resourceObserver?.acquireIndexProgress(route, request.signal ?? request.lifetime) ?? null;
    live(state); if (project.retirement) await project.retirement;
    await admitCaller(state);
    let initialGate: Promise<void> | null = null;
    if (request.intent !== 'read') {
      initialGate = ready(project);
      // Publish after the fresh-admitted native open, while actual catch-up
      // remains owned by the existing bounded ToolHandler freshness gate.
      if (project.opening) await project.opening;
    }
    // Observer, owner and freshness waits invalidate the earlier caller permit.
    await admitCaller(state); assertNoRebuild(route.root); live(state);
    if (!project.reader) project.reader = loadCodeGraph().openSync(route.root, { readOnly: true });
    const reader = project.reader; live(state); state.published = true; armTimer(project);
    return Object.freeze({ route: project.route, acquisition, cg: reader,
      ready: async (reconcile = false) => {
        live(state); await admitCaller(state);
        if (request.intent !== 'read') {
          const gate = !reconcile && initialGate ? initialGate : ready(project, reconcile);
          initialGate = null;
          await gate;
        }
        await admitCaller(state);
      },
      watchState: () => observeWatch(project.writer ?? reader),
      rearmWatcher: () => {
        if (!writeDemand(project) || !project.writer) return false;
        assertOwner(project); return project.writer.rearmWatcherAfterLockContention();
      },
      beginDrain: acquisition.beginDrain,
      release: acquisition.release,
    });
  } catch (primary) {
    try { await acquisition.release(); } catch (secondary) { cleanupFailure(route.root, primary, secondary); }
    throw primary;
  }
}
async function admitCaller(state: LeaseState): Promise<void> {
  live(state);
  const signal = state.published ? state.request.lifetime : state.request.signal ?? state.request.lifetime;
  await admitMcpRoute(state.request.route, { signal, admit: state.request.admit }); live(state);
}
function observeWatch(graph: CodeGraph): ProjectWatchState {
  return { active: graph.isWatching(), degraded: graph.isWatcherDegraded(), recovering: graph.isWatcherRecovering(),
    reason: graph.getWatcherDegradedReason(), pending: graph.getPendingFiles() };
}
function cleanupFailure(root: string, primary: unknown, secondary: unknown): void {
  process.stderr.write(`[CodeGraph MCP] Project ${root} cleanup after ${String(primary).slice(0, 2048)}: ${String(secondary).slice(0, 2048)}\n`);
}
async function releaseState(project: Project, state: LeaseState): Promise<void> {
  if (project.failure) throw project.failure;
  if (state.released) return;
  beginDrain(project, state);
  // Keep the published holder alive through its already-running freshness
  // operation. Its acquisition signal no longer controls admitted native work.
  if (project.gate) { try { await project.gate; } catch { /* query caller retains the freshness refusal */ } }
  if (project.failure) throw project.failure;
  if (state.request.intent !== 'read' && ![...project.leases].some(other =>
    other !== state && alive(other) && other.request.intent !== 'read')) {
    // Watcher-triggered sync also uses the real index mutex, without necessarily
    // appearing in project.gate. Keep the final holder alive through that work.
    project.writer?.unwatch();
    while (project.writer?.isIndexing()) await sleep(25);
  }
  // Fence demand, but retain this actual holder/progress in the coordinator
  // until native retirement AND observation release have both succeeded.
  state.released = true;
  if (!writeDemand(project)) {
    if (project.timer) clearInterval(project.timer);
    project.timer = null; project.writer?.unwatch(); project.watchAttempted = false;
  }
  // A rejected retirement means native drain is unproved. Keep observation
  // with the poisoned handle/owner; releasing it would claim completed work.
  if (!retainedWriterDemand(project)) await retire(project);
  await state.progress?.release();
  project.leases.delete(state);
}
function beginDrain(project: Project, state: LeaseState): void {
  if (state.released || state.draining) return;
  state.draining = true;
  if (!watchDemand(project)) { project.writer?.unwatch(); project.watchAttempted = false; }
  if (!writeDemand(project)) { if (project.timer) clearInterval(project.timer); project.timer = null; }
}
function armTimer(project: Project): void {
  if (project.timer || !writeDemand(project)) return;
  project.timer = setInterval(() => { void ready(project).catch(() => { /* next caller admits independently */ }); }, 1000);
  project.timer.unref();
}
async function retire(project: Project): Promise<void> {
  if (project.failure) throw project.failure;
  if (project.retirement) return project.retirement;
  const retirement = (async () => {
    if (project.timer) clearInterval(project.timer);
    project.timer = null; project.socket?.destroy(); project.socket = null;
    if (project.gate) { try { await project.gate; } catch { /* acquisition caller retains refusal */ } }
    if (project.failure) throw project.failure;
    project.writer?.unwatch(); while (project.writer?.isIndexing()) await sleep(25);
    if (retainedWriterDemand(project)) { armTimer(project); return; }
    const failures: unknown[] = [];
    try { project.writer?.close(); project.writer = null; } catch (error) { failures.push(error); }
    if (![...project.leases].some(state => !state.released)) {
      try { project.reader?.close(); project.reader = null; } catch (error) { failures.push(error); }
    }
    // A failed native close never grants early release or resurrection. Keep
    // its handle/owner evidence and refuse further acquisition in this process.
    if (!failures.length && !project.reservations) {
      try {
        releaseOwnedWriter(project, 'release');
      }
      catch (error) { failures.push(error); }
      if (!failures.length) { project.owner = null; project.ownerPath = null; project.ownerLocator = null; }
    }
    if (failures.length) {
      project.failure ??= new AggregateError(failures, 'CodeGraph project resource cleanup failed');
      throw project.failure;
    }
    project.caughtUp = false;
    if (![...project.leases].some(state => !state.released) && !project.reservations && projects.get(project.key) === project) projects.delete(project.key);
  })();
  const disposition = retirement.catch(error => {
    project.failure ??= new AggregateError([error], 'CodeGraph project retirement failed');
    throw project.failure;
  });
  project.retirement = disposition;
  // A rejected retirement remains the same sticky native disposition. Later
  // reader/reservation releases cannot retry an idempotent close into success.
  await disposition;
  if (project.retirement === disposition) project.retirement = null;
}

/** Exact acquired locator disposition, without changing best-effort callers. */
function writerDisposition(project: Project): 'absent' | 'replaced' | 'owned' {
  let raw: string;
  try { raw = readFileSync(project.ownerLocator!, 'utf8'); }
  catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return 'absent';
    throw new Error(`Retained writer record is unreadable: ${String(error).slice(0, 2048)}`);
  }
  const current = decodeWriterLockInfo(raw);
  let parsed: unknown;
  try { parsed = JSON.parse(raw); } catch { /* unknown identity below */ }
  if (!current || !Number.isInteger(current.pid) || current.pid <= 0 || !['direct', 'fallback', 'daemon', 'handover'].includes(current.mode)
    || typeof parsed !== 'object' || parsed === null || !('startedAt' in parsed) || typeof parsed.startedAt !== 'number'
    || !Number.isFinite(current.startedAt) || current.startedAt < 0) {
    throw new Error('Retained writer record has an unknown identity; release is unproved.');
  }
  return sameOwner(project.owner, current) ? 'owned' : 'replaced';
}
function releaseOwnedWriter(project: Project, phase: 'release' | 'rollback'): void {
  if (!project.owner) return;
  if (!project.ownerPath || !project.ownerLocator || project.ownerPath !== getWriterPidPath(project.route.root)
    || project.ownerLocator !== writerLocator(project.ownerPath)) {
    throw servicePolicyRefusal('unknown', project.route.root, `the retained native writer locator changed before ${phase}`);
  }
  // Never unlink an already replaced record, including another same-PID
  // identity. The legacy helper still owns its unrelated best-effort contract.
  if (writerDisposition(project) !== 'owned') return;
  releaseWriterLock(project.route.root);
  if (writerDisposition(project) === 'owned') {
    throw new Error(`Exact acquired writer record remains owned after ${phase}; native release is unproved.`);
  }
}

function ready(project: Project, reconcile = false): Promise<void> {
  if (project.failure) return Promise.reject(project.failure);
  if (reconcile) project.caughtUp = false;
  if (project.gate) return project.gate;
  if (!writeDemand(project)) return Promise.resolve();
  if (project.owner && project.writer && project.caughtUp && (!watchDemand(project) || project.watchAttempted)) return Promise.resolve();
  if (project.socket && !project.socket.destroyed) return Promise.resolve();
  let opened!: () => void, openingFailed!: (error: unknown) => void;
  const opening = new Promise<void>((resolve, reject) => { opened = resolve; openingFailed = reject; });
  project.opening = opening;
  // A timer/ready-only consumer may have no acquisition awaiting the open.
  // Retain the rejection for acquisition while preventing an unowned promise.
  void opening.catch(() => {});
  const gate = activate(project, opened).catch(error => {
    openingFailed(error);
    if (error instanceof LockUnavailableError || (error instanceof Error && error.name === 'AbortError')) return;
    if (isServicePolicyRefusal(error)) throw error;
    process.stderr.write(`[CodeGraph MCP] Catch-up sync failed for ${project.route.root}: ${String(error).slice(0, 2048)}\n`);
  }).finally(() => {
    opened();
    if (project.gate === gate) project.gate = null;
    if (project.opening === opening) project.opening = null;
  });
  void gate.catch(() => {}); // the lease/ToolHandler retains the real result
  project.gate = gate; return gate;
}
async function activate(project: Project, opened: () => void): Promise<void> {
  const root = project.route.root;
  let attached = false;
  await admitMcpRoute(project.route, { admit: async () => { attached = await attachFreshnessPeer(project, opened); } });
  if (attached || !writeDemand(project)) return;
  assertNoRebuild(root);
  let acquiredHere = false;
  if (!project.owner) {
    // An unregistered same-process external record grants no borrowing authority.
    if (readWriterLock(root)?.pid === process.pid) return;
    const locator = writerLocator(getWriterPidPath(root));
    const acquired = tryAcquireWriterLock(root, 'fallback');
    if (acquired.kind === 'taken') {
      if (acquired.existing?.mode === 'daemon' && await attachFreshnessPeer(project, opened)) return;
      opened(); // ordinary foreign owner permits a genuine native RO reader
      const end = Date.now() + 30_000;
      while (writeDemand(project) && acquired.existing?.ready === false && Date.now() < end) {
        await sleep(25);
        const current = readWriterLock(root);
        if (!sameOwner(current, acquired.existing)) return activate(project, opened);
        if (current?.ready !== false) break;
        try { process.kill(current.pid, 0); } catch (error) {
          if (error instanceof Error && 'code' in error && error.code === 'ESRCH') return activate(project, opened);
        }
      }
      return;
    }
    project.owner = Object.freeze({ ...acquired.info }); project.ownerPath = acquired.pidPath; project.ownerLocator = locator; acquiredHere = true;
  }
  try {
    const assertFresh = await beforeSync(project);
    assertFresh();
    if (!project.writer && !writeDemand(project)) throw cancelled();
    if (!project.writer) project.writer = loadCodeGraph().openSync(root, { readOnly: false, beforeSync: () => beforeSync(project) });
    assertOwner(project);
    opened();
    const watcher = watchDemand(project);
    if (watcher && !project.watchAttempted) {
      project.watchAttempted = true;
      const disabled = watchDisabledReason(root);
      if (disabled) process.stderr.write(`[CodeGraph MCP] File watcher disabled for ${root} — ${disabled}.\n`);
      else if (project.writer.watch(watcher.request.watchOptions)) process.stderr.write(`[CodeGraph MCP] File watcher active for ${root} — graph will auto-sync on changes\n`);
    }
    await project.writer.sync();
    if (!writeDemand(project)) return;
    assertOwner(project); project.caughtUp = true; markWriterReady(root);
  } catch (primary) {
    // A failed native open published no writer. Startup authority has its own
    // explicit lifetime; only this attempt's newly acquired record is released.
    if (acquiredHere && !project.writer && !project.reservations) {
      try {
        releaseOwnedWriter(project, 'rollback');
        project.owner = null; project.ownerPath = null; project.ownerLocator = null;
      } catch (secondary) {
        project.failure ??= new AggregateError([secondary], 'CodeGraph writer rollback failed');
        cleanupFailure(root, primary, secondary);
      }
    }
    throw primary;
  }
}
async function beforeSync(project: Project): Promise<() => void> {
  if (!retainedWriterDemand(project)) throw cancelled();
  // The shared resource retains native ownership, never a caller's policy
  // verdict. Re-admit each live writer demand inside the actual write mutex.
  for (const state of project.leases) {
    if (alive(state) && state.request.intent !== 'read') await admitCaller(state);
  }
  await admitMcpRoute(project.route, { admit: async () => {
    if (await attachFreshnessPeer(project)) throw new LockUnavailableError('A foreign freshness peer grants no local writer authority');
  } });
  // Run this final native/route/lifetime assertion after the awaiting caller
  // resumes, immediately before its synchronous open/reopen/write boundary.
  return () => { if (!retainedWriterDemand(project)) throw cancelled(); assertOwner(project); };
}

/** Policy-neutral resource observation; caller policy remains per request. */
async function attachFreshnessPeer(project: Project, opened?: () => void): Promise<boolean> {
  const root = project.route.root, owners = readQueryServiceOwners(root);
  assertReadableQueryOwners(root, owners);
  const candidates = getDaemonSocketCandidates(root);
  if (owners.daemon.kind === 'live' && canProbeDaemonIdentity(owners.daemon.value)) candidates.unshift(owners.daemon.value.socketPath);
  for (const candidate of new Set(candidates)) {
    const socket = await connectForProjectFreshness(candidate, root);
    if (!socket) continue;
    if (!retainedWriterDemand(project)) { socket.destroy(); return true; }
    if (owners.daemon.kind === 'live' && owners.daemon.value.pid === process.pid) {
      socket.destroy(); return false; // verified self; do not recurse through our catch-up gate
    }
    project.socket?.destroy(); project.socket = socket;
    socket.once('close', () => { if (project.socket === socket) project.socket = null; });
    opened?.();
    await daemonCatchUp(socket); return true;
  }
  assertNoLiveQueryDaemon(root); return false;
}
function daemonCatchUp(socket: Socket): Promise<void> {
  return new Promise((resolve, reject) => {
    let buffer = '', settled = false;
    const finish = (error?: unknown): void => {
      if (settled) return;
      settled = true; clearTimeout(timer);
      socket.removeListener('data', onData); socket.removeListener('close', finish);
      if (isServicePolicyRefusal(error)) reject(error); else resolve();
    };
    const onData = (chunk: string | Buffer): void => {
      buffer += String(chunk); let newline: number;
      while ((newline = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, newline); buffer = buffer.slice(newline + 1);
        try {
          const message = JSON.parse(line);
          if (message.id === 'project-catchup') {
            const refusal = message.error?.data && Object.assign(new Error(message.error.message), message.error.data);
            finish(isServicePolicyRefusal(refusal) ? refusal : undefined);
          }
        } catch { /* unrelated or incomplete response */ }
      }
    };
    const timer = setTimeout(() => { socket.destroy(); finish(); }, 30_000); timer.unref();
    socket.on('data', onData); socket.once('close', finish);
    socket.write(JSON.stringify({ jsonrpc: '2.0', id: 'project-catchup', method: 'tools/call',
      params: { name: 'codegraph_status', arguments: {} } }) + '\n'); socket.resume();
  });
}
