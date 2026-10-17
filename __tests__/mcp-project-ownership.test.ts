import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn } from 'child_process';
import { createHash } from 'crypto';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import type { MCPEngineOptions, InitialDefaultOutcome } from '../src/mcp/engine';
import type { ProjectRequest, ProjectResourceObserver, ProjectWriterReservation } from '../src/mcp/project-lifecycle';
import type { McpIndexRoute } from '../src/directory';

interface NativeFact { file: string; readOnly: boolean; pid: number; threadId: number; writer: { pid: number; mode: string; startedAt: number } | null }
const native: { begin(roots: string[]): void; facts(): { total: number; overflow: boolean; records: NativeFact[] }; restore(): void } =
  require('./fixtures/mcp-project-ownership-observer.cjs');
const { CodeGraph }: typeof import('../src') = require('../dist');
const { MCPEngine }: typeof import('../src/mcp/engine') = require('../dist/mcp/engine');
const { captureMcpIndexRoute, admitMcpRoute }: typeof import('../src/directory') = require('../dist/directory');
const { acquireProject, admitQueryProject, retainStartupProjectWriter }: typeof import('../src/mcp/project-lifecycle') = require('../dist/mcp/project-lifecycle');
const { tryAcquireWriterLock, releaseWriterLock, readWriterLock, getWriterPidPath }: typeof import('../src/mcp/writer-lock') = require('../dist/mcp/writer-lock');
const { getDaemonPidPath }: typeof import('../src/mcp/daemon-paths') = require('../dist/mcp/daemon-paths');
const { resolveQueryPolicy }: typeof import('../src/mcp/query-policy') = require('../dist/mcp/query-policy');
const roots: string[] = [], engines: Array<InstanceType<typeof MCPEngine>> = [];
const cleanup: Array<() => Promise<void>> = [], facts: unknown[] = [];
let body: Promise<void> | undefined, settled = true, closing = false, poison: string | undefined;
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
function active(): void { if (closing || poison) throw new Error('Ownership fixture cannot acquire after cleanup'); }
function owned(work: () => Promise<void>): () => Promise<void> {
  return () => { body = (async () => { try { await work(); } finally { settled = true; } })(); void body.catch(() => {}); return body; };
}
async function bounded<T>(work: Promise<T>, label: string, ms = 5000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try { return await Promise.race([work, new Promise<never>((_, reject) => {
    timer = setTimeout(() => { poison ??= `${label} deadline`; reject(new Error(poison)); }, ms);
  })]); } finally { if (timer) clearTimeout(timer); }
}
async function until(test: () => boolean, label: string): Promise<void> {
  const end = Date.now() + 10_000;
  while (!test()) { if (Date.now() >= end) throw new Error(`${label} deadline`); await sleep(20); }
}
function temp(): string { active(); const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-ownership-'))); roots.push(root); return root; }
async function indexed(root: string, symbol = 'alphaOriginal'): Promise<void> {
  active(); fs.writeFileSync(path.join(root, 'app.ts'), `export function ${symbol}() { return 7; }\n`);
  const graph = await CodeGraph.init(root);
  let primary: { error: unknown } | undefined, secondary: { error: unknown } | undefined;
  try { active(); await graph.indexAll(); } catch (error) { primary = { error }; }
  try { graph.close(); } catch (error) { secondary = { error }; }
  if (primary || secondary) facts.push({ kind: 'fixture-failure', root, primary: primary ? String(primary.error).slice(0, 2048) : null,
    secondary: secondary ? String(secondary.error).slice(0, 2048) : null });
  if (primary) throw primary.error; if (secondary) throw secondary.error;
}
function route(root: string): McpIndexRoute { const held = captureMcpIndexRoute(root); if (!held) throw new Error('Missing fixture own index'); return held; }
function make(options: MCPEngineOptions = {}): InstanceType<typeof MCPEngine> {
  active(); const actual = resolveQueryPolicy({ poolSize: '0' }, 'direct', 3);
  const engine = new MCPEngine({ executionMode: 'direct', queryPolicy: actual,
    queryAdmissionPolicy: resolveQueryPolicy(actual.requested, 'shared-daemon', 3), queryPool: false, ...options });
  engines.push(engine); if (engines.length > 2) throw new Error('Ownership fixture engine budget'); return engine;
}
function hash(file: string): string { return createHash('sha256').update(fs.readFileSync(file)).digest('hex'); }
function ownedNativeOpens(): void {
  const writes = native.facts().records.filter(record => !record.readOnly);
  expect(writes.length).toBeGreaterThan(0);
  for (const record of writes) expect(record.writer).toMatchObject({ pid: record.pid });
  expect(native.facts().overflow).toBe(false);
}
async function search(engine: InstanceType<typeof MCPEngine>, symbol: string, projectPath?: string) {
  return engine.getToolHandler().execute('codegraph_search', { query: symbol, ...(projectPath ? { projectPath } : {}) });
}
function host(role: string, root: string, beta?: string, log?: string) {
  active();
  const child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, '--require', path.join(__dirname, 'fixtures/mcp-project-ownership-observer.cjs'),
    path.join(__dirname, 'fixtures/mcp-project-ownership-host.cjs'), role, root, ...(beta ? [beta] : [])],
  { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, ...(log ? { CODEGRAPH_OWNERSHIP_NATIVE_LOG: log } : {}) } });
  const chunks: Buffer[] = [], errors: Buffer[] = [];
  let bytes = 0, exit: { code: number | null; signal: NodeJS.Signals | null } | null = null;
  let stdoutEnd = false, stderrEnd = false, didClose = false, firstFailure: string | undefined;
  child.on('error', error => { firstFailure ??= String(error); });
  child.stdout.on('data', chunk => { bytes += chunk.length; if (bytes <= 1024 * 1024) chunks.push(Buffer.from(chunk)); else firstFailure ??= 'Native stdout overflow'; });
  child.stderr.on('data', chunk => { bytes += chunk.length; if (bytes <= 1024 * 1024) errors.push(Buffer.from(chunk)); else firstFailure ??= 'Native stderr overflow'; });
  child.stdout.once('end', () => { stdoutEnd = true; }); child.stderr.once('end', () => { stderrEnd = true; });
  child.once('exit', (code, signal) => { exit = { code, signal }; });
  const close = new Promise<void>(resolve => child.once('close', () => { didClose = true; resolve(); }));
  const snapshot = () => ({ pid: child.pid, exit, didClose, stdoutEnd, stderrEnd, firstFailure,
    stdout: Buffer.concat(chunks).toString('utf8'), stderr: Buffer.concat(errors).toString('utf8') });
  cleanup.push(async () => {
    child.stdin.end();
    try { await bounded(close, 'native graceful close'); }
    catch (error) { child.kill('SIGKILL'); try { await bounded(close, 'native killed close'); } finally { facts.push({ kind: 'native-cleanup-primary', error: String(error), ...snapshot() }); } throw error; }
    facts.push({ kind: 'native-close', ...snapshot() });
    expect(snapshot()).toMatchObject({ didClose: true, stdoutEnd: true, stderrEnd: true, firstFailure: undefined });
  });
  if (cleanup.length > 3) throw new Error('Ownership native cleanup budget');
  return { child, close, snapshot, waitReady: () => until(() => snapshot().stdout.includes('"event":"ready"'), 'native owner ready') };
}

beforeEach(() => {
  if (poison || !settled) throw new Error(`Ownership fixture cannot reuse incomplete body: ${poison}`);
  body = undefined; settled = false; closing = false;
  vi.stubEnv('CODEGRAPH_DIR', '.codegraph'); vi.stubEnv('CODEGRAPH_NO_WATCH', '1');
  vi.stubEnv('CODEGRAPH_QUERY_MODE', 'default'); vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '0');
  native.begin([]);
});
afterEach(async context => {
  closing = true; const failures: string[] = [], pending = !settled ? body : undefined;
  if (pending) poison ??= 'Framework stopped awaiting owned ownership body';
  for (const engine of engines.splice(0)) try { await bounded(engine.stop(), 'engine drain'); } catch (error) { failures.push(String(error).slice(0, 2048)); }
  for (const release of cleanup.splice(0).reverse()) try { await release(); } catch (error) { failures.push(String(error).slice(0, 2048)); }
  if (pending) try { await bounded(pending, 'body join'); } catch (error) { failures.push(String(error).slice(0, 2048)); }
  facts.push({ test: context.task.name, primary: context.task.result?.errors?.slice(0, 4).map(error => String(error.message).slice(0, 2048)),
    roots: roots.splice(0), sqlite: native.facts(), failures, poison, settled });
  try {
    if (process.env.VALIDATION_OUT) {
      const encoded = JSON.stringify(facts); if (Buffer.byteLength(encoded) > 2 * 1024 * 1024) throw new Error('Ownership evidence overflow');
      fs.writeFileSync(path.join(process.env.VALIDATION_OUT, 'mcp-project-ownership-facts.json'), encoded);
    }
  } catch (error) { failures.push(String(error).slice(0, 2048)); process.stderr.write(`Ownership evidence failure: ${String(error)}\n`); }
  finally { vi.unstubAllEnvs(); }
  // Retain disposable indexes and native logs, including every failed fixture.
  expect(failures).toEqual([]); expect(native.facts().overflow).toBe(false); expect(poison).toBeUndefined();
}, 60_000);
afterAll(() => { if (settled) native.restore(); });

describe('real MCP project ownership precedes native writes', () => {
  it('an inert rootless direct engine acquires its late default owner before native writable open and catches up', owned(async () => {
    const root = temp(), outcomes: InitialDefaultOutcome[] = [], engine = make({ watch: false, onInitialDefaultOutcome: value => outcomes.push(value) });
    native.begin([root]); expect(native.facts().total).toBe(0); expect(readWriterLock(root)).toBeNull();
    await indexed(root); fs.writeFileSync(path.join(root, 'app.ts'), 'export function lateDefault() { return 8; }\n'); native.begin([root]);
    await engine.ensureInitialized(root); const answer = await search(engine, 'lateDefault');
    expect(answer.content[0]?.text).toContain('lateDefault'); expect(outcomes).toEqual([{ kind: 'ready', route: route(root) }]);
    expect(engine.getQueryExecutionContext().policy.executionMode).toBe('direct'); ownedNativeOpens();
  }), 30_000);

  it.each([false, true])('default watch=%s keeps one owned catch-up while no watcher is installed', async watch => {
    await owned(async () => {
      const root = temp(); await indexed(root); fs.writeFileSync(path.join(root, 'app.ts'), 'export function caughtUp() { return 8; }\n');
      const engine = make({ projectRoute: route(root), watch }); native.begin([root]); await engine.ensureInitialized(root);
      expect((await search(engine, 'caughtUp')).content[0]?.text).toContain('caughtUp');
      expect(readWriterLock(root)).toMatchObject({ pid: process.pid, ready: true }); ownedNativeOpens();
      fs.writeFileSync(path.join(root, 'app.ts'), 'export function notWatched() { return 9; }\n'); await sleep(1250);
      expect((await search(engine, 'notWatched')).content[0]?.text).not.toContain('function notWatched');
    })();
  }, 30_000);

  it('ordinary native foreign owner permits only actual read-only opens and no directory repair', owned(async () => {
    const alpha = temp(), beta = temp(); await indexed(alpha); await indexed(beta, 'betaOriginal');
    const peer = host('writer', beta); await peer.waitReady(); fs.unlinkSync(path.join(beta, '.codegraph', '.gitignore'));
    const dbBefore = hash(route(beta).index), ownerBefore = fs.readFileSync(getWriterPidPath(beta), 'utf8');
    const engine = make({ projectRoute: route(alpha) }); native.begin([alpha, beta]); await engine.ensureInitialized(alpha);
    expect((await search(engine, 'betaOriginal', beta)).content[0]?.text).toContain('betaOriginal');
    const betaOpens = native.facts().records.filter(record => record.file === route(beta).index);
    expect(betaOpens.length).toBeGreaterThan(0); expect(betaOpens.every(record => record.readOnly)).toBe(true);
    expect(hash(route(beta).index)).toBe(dbBefore); expect(fs.existsSync(path.join(beta, '.codegraph', '.gitignore'))).toBe(false);
    expect(fs.readFileSync(getWriterPidPath(beta), 'utf8')).toBe(ownerBefore);
  }), 30_000);

  it('explicit watch:false is actual read-only with no writer or catch-up', owned(async () => {
    const root = temp(); await indexed(root); fs.writeFileSync(path.join(root, 'app.ts'), 'export function laterSource() {}\n');
    const engine = make({ watch: false }); native.begin([root]);
    expect((await search(engine, 'alphaOriginal', root)).content[0]?.text).toContain('alphaOriginal');
    expect(readWriterLock(root)).toBeNull(); expect(native.facts().records.every(record => record.readOnly)).toBe(true);
  }), 30_000);

  it('foreign native exit triggers automatic owned takeover without an engine query', owned(async () => {
    const root = temp(); await indexed(root); const peer = host('writer', root); await peer.waitReady();
    const engine = make({ projectRoute: route(root) }); native.begin([root]); await engine.ensureInitialized(root);
    expect(native.facts().records.every(record => record.readOnly)).toBe(true);
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function takeoverSource() {}\n');
    peer.child.stdin.end(); await bounded(peer.close, 'foreign native exit and close');
    expect(peer.snapshot().exit).toEqual({ code: 0, signal: null });
    await until(() => readWriterLock(root)?.pid === process.pid && readWriterLock(root)?.ready === true, 'automatic owned catch-up');
    const reader = CodeGraph.openSync(root, { readOnly: true });
    try { expect(reader.searchNodes('takeoverSource').length).toBeGreaterThan(0); } finally { reader.close(); }
    ownedNativeOpens();
  }), 30_000);

  it('two engines share stable read resources and a startup reservation cannot release the survivor writer', owned(async () => {
    const root = temp(); await indexed(root); const held = route(root), acquired = tryAcquireWriterLock(root, 'direct');
    if (acquired.kind !== 'acquired') throw new Error('Fixture startup owner failed');
    const reservation: ProjectWriterReservation = retainStartupProjectWriter(held, acquired.info); cleanup.push(() => reservation.release());
    const first = make({ projectRoute: held, projectWriter: reservation }), second = make({ projectRoute: held });
    native.begin([root]); await Promise.all([first.ensureInitialized(root), second.ensureInitialized(root)]);
    // Native publication precedes catch-up readiness. Snapshot the complete
    // authority record only after the real query joins that existing gate.
    expect((await search(second, 'alphaOriginal')).content[0]?.text).toContain('alphaOriginal');
    await until(() => readWriterLock(root)?.pid === process.pid && readWriterLock(root)?.ready === true, 'shared writer catch-up readiness');
    const ownerBefore = readWriterLock(root), bytesBefore = fs.readFileSync(getWriterPidPath(root), 'utf8');
    expect(ownerBefore).toMatchObject({ pid: process.pid, ready: true });
    expect(() => retainStartupProjectWriter(held, { ...acquired.info, startedAt: acquired.info.startedAt + 1 })).toThrow('exact acquired native writer');
    expect(fs.readFileSync(getWriterPidPath(root), 'utf8')).toBe(bytesBefore);
    await first.stop(); await reservation.release();
    expect(readWriterLock(root)).toEqual(ownerBefore); expect(fs.readFileSync(getWriterPidPath(root), 'utf8')).toBe(bytesBefore);
    expect((await search(second, 'alphaOriginal')).content[0]?.text).toContain('alphaOriginal');
    await second.stop(); expect(readWriterLock(root)).toBeNull(); ownedNativeOpens();
  }), 30_000);

  it('read leases expose the same real read-only graph and one release does not close its survivor', owned(async () => {
    const root = temp(); await indexed(root); const held = route(root), lifetime = new AbortController();
    const policy = resolveQueryPolicy({ poolSize: '0' }, 'shared-daemon', 3);
    const request: ProjectRequest = { route: held, intent: 'read', lifetime: lifetime.signal, watchOptions: {},
      admit: (selected: McpIndexRoute, signal?: AbortSignal) => admitQueryProject(selected.root, policy, signal) };
    native.begin([root]); const first = await acquireProject(request); cleanup.push(() => first.release());
    const second = await acquireProject(request); cleanup.push(() => second.release());
    expect(first.cg).toBe(second.cg); await first.release(); expect(second.cg.searchNodes('alphaOriginal').length).toBeGreaterThan(0);
    expect(readWriterLock(root)).toBeNull(); expect(native.facts().records.every(record => record.readOnly)).toBe(true);
  }), 30_000);

  it('legacy startup attachment binds only a live exact reservation before any native engine work', owned(async () => {
    const root = temp(), beta = temp(); await indexed(root); await indexed(beta, 'betaOriginal');
    const held = route(root), acquired = tryAcquireWriterLock(root, 'direct');
    if (acquired.kind !== 'acquired') throw new Error('Legacy fixture startup owner failed');
    const reservation = retainStartupProjectWriter(held, acquired.info); cleanup.push(() => reservation.release());
    const engine = make({ watch: false }); native.begin([root, beta]);
    const forged: ProjectWriterReservation = { route: held, release: async () => {} };
    expect(() => engine.setProjectWriter(forged)).toThrow('module-owned'); expect(engine.getProjectPath()).toBeNull();
    engine.setProjectWriter(reservation); expect(engine.getProjectPath()).toBe(held.root);
    engine.setProjectPathHint(held.root); expect(() => engine.setProjectPathHint(beta)).toThrow();
    expect(() => engine.setProjectWriter(reservation)).toThrow(); expect(native.facts().total).toBe(0);
    await engine.ensureInitialized(beta); expect((await search(engine, 'alphaOriginal')).content[0]?.text).toContain('alphaOriginal');
    expect(engine.getProjectPath()).toBe(held.root);
    const conflicting = make({ projectRoute: route(beta), watch: false }); native.begin([root, beta]);
    expect(() => conflicting.setProjectWriter(reservation)).toThrow('differs');
    expect(conflicting.getProjectPath()).toBe(beta); expect(native.facts().total).toBe(0);
    await engine.stop();
    expect(readWriterLock(root)).toMatchObject({ pid: process.pid, mode: 'direct' });
    await reservation.release(); expect(readWriterLock(root)).toBeNull();
    expect(() => conflicting.setProjectWriter(reservation)).toThrow('module-owned');
  }), 30_000);

  it('a draining foreign-reader lease keeps useful reads but cannot activate after native owner exit', owned(async () => {
    const root = temp(); await indexed(root); const peer = host('writer', root); await peer.waitReady();
    const policy = resolveQueryPolicy({ poolSize: '0' }, 'shared-daemon', 3), lifetime = new AbortController();
    native.begin([root]); const lease = await acquireProject({ route: route(root), intent: 'watch', lifetime: lifetime.signal,
      watchOptions: {}, admit: (selected, signal) => admitQueryProject(selected.root, policy, signal) });
    cleanup.push(() => lease.release());
    // The real foreign fixture intentionally owns ready:false. Remove this
    // lease's write demand before joining its gate, rather than waiting for a
    // readiness transition that only the later foreign EOF could enable.
    lease.beginDrain(); await lease.ready();
    expect(lease.cg.searchNodes('alphaOriginal').length).toBeGreaterThan(0);
    peer.child.stdin.end(); await bounded(peer.close, 'draining foreign native exit');
    expect(peer.snapshot()).toMatchObject({ exit: { code: 0, signal: null }, didClose: true, stdoutEnd: true, stderrEnd: true });
    await sleep(1250);
    expect(readWriterLock(root)).toBeNull(); expect(native.facts().records.every(record => record.readOnly)).toBe(true);
    expect(lease.cg.searchNodes('alphaOriginal').length).toBeGreaterThan(0);
    await lease.release(); expect(() => lease.cg.getStats()).toThrow();
  }), 30_000);

  it('resource observation retains exact alpha and beta database-file routes until actual engine drain', owned(async () => {
    const alpha = temp(), beta = temp(); await indexed(alpha); await indexed(beta, 'betaOriginal');
    const heldAlpha = route(alpha), heldBeta = route(beta), retained = new Map<string, number>();
    const observer: ProjectResourceObserver = { acquireIndexProgress: async selected => {
      retained.set(selected.index, (retained.get(selected.index) ?? 0) + 1); let released = false;
      return { release: async () => { if (released) return; released = true;
        const refs = (retained.get(selected.index) ?? 0) - 1; if (refs) retained.set(selected.index, refs); else retained.delete(selected.index); } };
    } };
    const engine = make({ projectRoute: heldAlpha, resourceObserver: observer }); await engine.ensureInitialized(alpha);
    await search(engine, 'betaOriginal', beta); expect([...retained.keys()].sort()).toEqual([heldAlpha.index, heldBeta.index].sort());
    await engine.stop(); expect(retained.size).toBe(0);
  }), 30_000);

  it('an explicit project reacquired during prior eviction keeps its new lease through the next idle release', owned(async () => {
    const root = temp(), missing = temp(); await indexed(root); const held = route(root);
    const policy = resolveQueryPolicy({ poolSize: '0' }, 'shared-daemon', 3);
    // Another real reader keeps the coordinator's shared graph alive while the
    // first engine lease returns its writer and then waits for outer observation.
    const keeper = await acquireProject({ route: held, intent: 'read', lifetime: new AbortController().signal,
      watchOptions: {}, admit: (selected, signal) => admitQueryProject(selected.root, policy, signal) });
    cleanup.push(() => keeper.release());
    let enterOldRelease!: () => void, resumeOldRelease!: () => void;
    const oldReleaseEntered = new Promise<void>(resolve => { enterOldRelease = resolve; });
    const oldReleaseGate = new Promise<void>(resolve => { resumeOldRelease = resolve; });
    const retained = new Set<number>(), engineLeases: number[] = [], released: number[] = [];
    let nextProgress = 0;
    const observer: ProjectResourceObserver = { acquireIndexProgress: async (selected, signal) => {
      expect(selected).toEqual(held);
      const id = ++nextProgress; retained.add(id);
      // Public per-request observation has no request signal in this fixture;
      // the engine-owned acquisition always carries its actual lifetime signal.
      if (signal) engineLeases.push(id);
      let release: Promise<void> | undefined;
      return { release: () => release ??= (async () => {
        if (id === engineLeases[0]) { enterOldRelease(); await oldReleaseGate; }
        retained.delete(id); released.push(id);
      })() };
    } };
    const engine = make({ watch: true, resourceObserver: observer });
    vi.stubEnv('CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS', '0'); native.begin([root]);
    const expireCache = async () => {
      vi.stubEnv('CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS', '1'); await sleep(5);
      const answer = await search(engine, 'unused', missing);
      expect(answer.isError).not.toBe(true); expect(answer.content[0]?.text).toContain("isn't indexed");
      vi.stubEnv('CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS', '0');
    };
    try {
      expect((await search(engine, 'alphaOriginal', root)).content[0]?.text).toContain('alphaOriginal');
      expect(engineLeases).toHaveLength(1); expect(readWriterLock(root)).toMatchObject({ pid: process.pid, ready: true });
      await expireCache(); await bounded(oldReleaseEntered, 'old explicit observation release');
      expect(readWriterLock(root)).toBeNull(); expect(keeper.cg.searchNodes('alphaOriginal').length).toBeGreaterThan(0);

      // Re-enter through the normal public tool boundary while the old cache
      // close is still pending, rather than reading or mutating engine lookups.
      expect((await search(engine, 'alphaOriginal', root)).content[0]?.text).toContain('alphaOriginal');
      expect(engineLeases).toHaveLength(2); expect(retained.has(engineLeases[1])).toBe(true);
      expect(readWriterLock(root)).toMatchObject({ pid: process.pid });
      // The shared reader can answer while this new writer's actual catch-up
      // is still running. Join its readiness before releasing the old observer.
      await until(() => readWriterLock(root)?.pid === process.pid && readWriterLock(root)?.ready === true,
        'reacquired writer catch-up readiness');
      resumeOldRelease(); await until(() => released.includes(engineLeases[0]), 'old observation released');
      // Join the old release's continuation before triggering another eviction.
      await new Promise<void>(resolve => setImmediate(resolve));
      expect((await search(engine, 'alphaOriginal', root)).content[0]?.text).toContain('alphaOriginal');
      expect(retained.has(engineLeases[1])).toBe(true);

      await expireCache();
      await until(() => released.includes(engineLeases[1]), 'reacquired explicit observation released before engine stop');
      expect(readWriterLock(root)).toBeNull(); expect(retained.size).toBe(0);
      expect(keeper.cg.searchNodes('alphaOriginal').length).toBeGreaterThan(0);
      await keeper.release(); expect(() => keeper.cg.getStats()).toThrow();
      await engine.stop(); ownedNativeOpens();
    } finally {
      resumeOldRelease();
      facts.push({ kind: 'explicit-eviction-reacquire', root, engineLeases: [...engineLeases],
        released: [...released], retained: [...retained], owner: readWriterLock(root) });
    }
  }), 30_000);

  it.each(['unknown', 'missing', 'abort'])('%s after observer registration is rechecked before schema and primary outcome', async action => {
    await owned(async () => {
      const root = temp(); await indexed(root); const held = route(root), outcomes: InitialDefaultOutcome[] = [];
      let entered!: () => void, resume!: () => void, released = 0;
      const enteredGate = new Promise<void>(resolve => { entered = resolve; }), gate = new Promise<void>(resolve => { resume = resolve; });
      const observer: ProjectResourceObserver = { acquireIndexProgress: async observed => {
        expect(observed).toEqual(held); entered(); await gate; return { release: async () => { released++; } };
      } };
      const engine = make({ projectRoute: held, resourceObserver: observer, onInitialDefaultOutcome: outcome => outcomes.push(outcome) });
      native.begin([root]); const pending = engine.ensureInitialized(root); void pending.catch(() => {}); await enteredGate;
      if (action === 'unknown') fs.writeFileSync(getDaemonPidPath(root), '{');
      if (action === 'missing') fs.renameSync(held.index, held.index + '-retained');
      const stopping = action === 'abort' ? engine.stop() : undefined; resume();
      if (action === 'unknown') await expect(pending).rejects.toMatchObject({ reason: 'unknown' }); else await pending;
      await stopping; expect(native.facts().total).toBe(0); expect(released).toBe(1); expect(engine.hasDefaultCodeGraph()).toBe(false);
      expect(outcomes.map(outcome => outcome.kind)).toEqual(action === 'unknown' ? ['refused'] : []);
    })();
  }, 30_000);

  it('genuine no-route ends primary once, while an ordinary observer failure does not', owned(async () => {
    const missing = temp(), root = temp(); await indexed(root); const outcomes: InitialDefaultOutcome[] = [];
    const first = make({ watch: false, onInitialDefaultOutcome: value => outcomes.push(value) }); await first.ensureInitialized(missing);
    await first.ensureInitialized(missing); expect(outcomes).toEqual([{ kind: 'no-route' }]);
    const failed: InitialDefaultOutcome[] = [], second = make({ projectRoute: route(root),
      resourceObserver: { acquireIndexProgress: async () => { throw new Error('Real outer observer unavailable'); } },
      onInitialDefaultOutcome: value => failed.push(value) });
    native.begin([root]); await second.ensureInitialized(root); expect(failed).toEqual([]); expect(native.facts().total).toBe(0);
  }), 30_000);

  it('post-mutex sync admission refuses a new unknown owner before reopening or writing', owned(async () => {
    const root = temp(); await indexed(root); const held = route(root);
    const policy = resolveQueryPolicy({ poolSize: '0' }, 'shared-daemon', 3);
    const owner = tryAcquireWriterLock(root, 'direct'); if (owner.kind !== 'acquired') throw new Error('Fixture sync owner failed');
    let entered!: () => void, resume!: () => void, guards = 0;
    const enteredGate = new Promise<void>(resolve => { entered = resolve; }), gate = new Promise<void>(resolve => { resume = resolve; });
    const graph = CodeGraph.openSync(root, { beforeSync: async () => {
      if (++guards === 1) { entered(); await gate; }
      await admitMcpRoute(held, { admit: (selected, signal) => admitQueryProject(selected.root, policy, signal) });
    } });
    cleanup.push(async () => { graph.close(); releaseWriterLock(root); });
    const first = graph.sync(); void first.catch(() => {}); await enteredGate;
    const second = graph.sync(); void second.catch(() => {}); fs.writeFileSync(getDaemonPidPath(root), '{');
    const before = hash(held.index); native.begin([root]); resume();
    await expect(first).rejects.toMatchObject({ reason: 'unknown' }); await expect(second).rejects.toMatchObject({ reason: 'unknown' });
    expect(guards).toBe(2);
    expect(hash(held.index)).toBe(before); expect(native.facts().total).toBe(0);
  }), 30_000);

  it('real default and cached explicit query-worker handles stay read-only under a read-write policy', owned(async () => {
    const alpha = temp(), beta = temp(); await indexed(alpha); await indexed(beta, 'betaOriginal'); const log = path.join(alpha, 'worker-native.jsonl');
    const peer = host('workers', alpha, beta, log); await bounded(peer.close, 'worker host close', 20_000);
    const observed = peer.snapshot(); expect(observed.exit).toEqual({ code: 0, signal: null }); expect(observed.stdout).toContain('betaOriginal');
    const rows: NativeFact[] = fs.readFileSync(log, 'utf8').trim().split(/\r?\n/).map(line => JSON.parse(line));
    const workers = rows.filter(record => record.threadId > 0); expect(workers.length).toBeGreaterThan(0);
    expect(workers.every(record => record.readOnly)).toBe(true);
    expect(workers.some(record => record.file === route(alpha).index)).toBe(true);
    expect(workers.some(record => record.file === route(beta).index)).toBe(true);
    expect(observed.stdout).toContain('"capability":"read-write"');
  }), 30_000);

  it('a real measurement drains only after its actual Worker exit and opens the held index read-only', owned(async () => {
    const root = temp(); await indexed(root); fs.writeFileSync(path.join(root, 'app.ts'), 'export function measuredChange() {}\n');
    const log = path.join(root, 'measurement-native.jsonl'), peer = host('measurements', root, undefined, log);
    await bounded(peer.close, 'measurement host native close', 20_000);
    const observed = peer.snapshot(); expect(observed.exit).toEqual({ code: 0, signal: null });
    const messages: Array<{ event: string; counts?: { modified: number }; workers?: { overflow: boolean;
      records: Array<{ file: string; workerId: number; exited: boolean; code: number | null }> } }> =
      observed.stdout.trim().split(/\r?\n/).map(line => JSON.parse(line));
    const measurement = messages.find(message => message.event === 'measurement-drained');
    expect(measurement?.counts?.modified).toBe(1); expect(measurement?.workers?.overflow).toBe(false);
    const workers = measurement?.workers?.records.filter(record => record.file.endsWith('index-freshness-worker.js')) ?? [];
    expect(workers).toHaveLength(1); expect(workers[0].workerId).toBeGreaterThan(0); expect(workers[0].exited).toBe(true);
    expect(workers[0].code).not.toBeNull();
    const rows: NativeFact[] = fs.readFileSync(log, 'utf8').trim().split(/\r?\n/).map(line => JSON.parse(line));
    const reads = rows.filter(record => record.threadId > 0 && record.file === route(root).index);
    expect(reads.length).toBeGreaterThan(0); expect(reads.every(record => record.readOnly)).toBe(true);
    expect(readWriterLock(root)).toBeNull();
  }), 30_000);

  it.each(['selector', 'missing'])('a real measurement refuses held route %s drift before any native SQLite open', async drift => {
    await owned(async () => {
      const root = temp(); await indexed(root); const held = route(root), log = path.join(root, 'measurement-drift-native.jsonl');
      const peer = host('measurement-drift', root, drift, log); await bounded(peer.close, 'measurement drift native close', 20_000);
      const observed = peer.snapshot(); expect(observed.exit).toEqual({ code: 0, signal: null });
      const messages: Array<{ event: string; counts?: unknown; held?: McpIndexRoute; workers?: { overflow: boolean;
        records: Array<{ file: string; workerId: number; exited: boolean; code: number | null }> } }> =
        observed.stdout.trim().split(/\r?\n/).map(line => JSON.parse(line));
      const measurement = messages.find(message => message.event === 'measurement-drained');
      expect(measurement?.held).toEqual(held); expect(measurement?.counts).toBeNull(); expect(measurement?.workers?.overflow).toBe(false);
      const workers = measurement?.workers?.records.filter(record => record.file.endsWith('index-freshness-worker.js')) ?? [];
      expect(workers).toHaveLength(1); expect(workers[0].workerId).toBeGreaterThan(0); expect(workers[0].exited).toBe(true);
      expect(workers[0].code).not.toBeNull(); expect(fs.existsSync(log) ? fs.readFileSync(log, 'utf8') : '').toBe('');
      expect(readWriterLock(root)).toBeNull();
    })();
  }, 30_000);
  // Native child isolation keeps intentionally poisoned real resources out of
  // the next row. Only close/unlink errors are injected; the graph, route,
  // admission and writer authority remain the actual production resources.
  it.each(['default-unpublished', 'explicit-unpublished', 'default-acquire', 'explicit-acquire',
    'registration-undefined', 'reader-reservation-poison', 'unlink-retire', 'unlink-rollback', 'replacement-record', 'unreadable-record', 'constructor-unlink'])
  ('retains real cleanup evidence across %s (narrow fault injection)', async scenario => {
    await owned(async () => {
      const root = temp(); await indexed(root); const held = route(root), log = path.join(root, 'cleanup-fault-native.jsonl');
      const peer = host('cleanup-fault', root, scenario, log); await bounded(peer.close, 'cleanup fault native host close', 20_000);
      const observed = peer.snapshot(); expect(observed.exit).toEqual({ code: 0, signal: null });
      const messages: Array<{ event: string; scenario?: string; checks?: Record<string, boolean>; retained?: boolean;
        progressRetained?: boolean; owner?: { pid: number }; primary?: string; nativeCloseAttempts?: number;
        unlinkAttempts?: number; facts?: { records: NativeFact[]; overflow: boolean } }> =
        observed.stdout.trim().split(/\r?\n/).map(line => JSON.parse(line));
      const result = messages.find(message => message.event === 'cleanup-fault-observed');
      expect(result?.scenario).toBe(scenario); expect(result?.checks).toBeDefined();
      expect(Object.values(result?.checks ?? {})).not.toHaveLength(0);
      expect(Object.values(result?.checks ?? {}).every(Boolean)).toBe(true);
      expect(result?.facts?.overflow).toBe(false);
      if (scenario === 'replacement-record') {
        expect(result?.retained).toBe(false); expect(result?.unlinkAttempts).toBe(0);
      } else {
        expect(result?.retained).toBe(true); expect(result?.progressRetained).toBe(scenario !== 'constructor-unlink');
        expect(result?.owner?.pid).toBe(peer.child.pid);
      }
      if (scenario === 'registration-undefined') expect(result?.primary).toBe('undefined');
      if (scenario.includes('unpublished') || scenario.endsWith('-acquire') || scenario === 'reader-reservation-poison') {
        expect(result?.nativeCloseAttempts).toBe(1);
        expect(result?.facts?.records.some(record => !record.readOnly && record.writer?.pid === record.pid)).toBe(true);
      }
      facts.push({ kind: 'intentional-native-cleanup-fault', scenario, result, ...observed });
    })();
  }, 30_000);

});
