/**
 * Explicit-`projectPath` project lifecycle (#1835).
 *
 * A server whose root has no index of its own (a workspace whose indexed
 * children are gitignored) serves each child through `projectPath`. Before
 * this fix those projects were opened read-only: no catch-up sync on open and
 * no file watcher, so their answers went stale until someone ran
 * `codegraph sync` by hand. Now the engine gives an explicit project the same
 * lifecycle the default project gets — a catch-up sync the first call waits
 * for, a watcher while it stays cached — bounded (LRU) and released on stop().
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest';
import * as fs from 'fs';
import { spawn, ChildProcess } from 'child_process';
import * as path from 'path';
import * as os from 'os';
import { createHash } from 'crypto';
import type { MCPEngine, MCPEngineOptions } from '../src/mcp/engine';
import { MAX_CACHED_PROJECTS } from '../src/mcp/tools';
import { requestedQueryPolicy, resolveLegacyQueryPolicy, type RequestedQueryPolicy, type ResolvedQueryPolicy } from '../src/mcp/query-policy';
const compatibility: typeof import('../src/mcp/query-service-compatibility') = require('../dist/mcp/query-service-compatibility');
import type { DaemonHello } from '../src/mcp/daemon';
import { getDaemonSocketPath } from '../src/mcp/daemon-paths';
import { CodeGraphPackageVersion } from '../src/mcp/version';

// Default and read-only opens use the engine's lazy CommonJS loader.
const { MCPEngine: BuiltMCPEngine } = require('../dist/mcp/engine') as typeof import('../src/mcp/engine');
const { CodeGraph }: typeof import('../src') = require('../dist');
type Graph = InstanceType<typeof CodeGraph>;
const nativeOpen = CodeGraph.openSync;

const opened: Graph[] = [];
const openModes = new WeakMap<Graph, boolean>();
let onOpen: ((cg: Graph, readOnly: boolean) => void) | undefined;
/** Delegates every actual open, including both the stable reader and writer. */
const RecordingCodeGraph = {
  openSync(projectRoot: string, options: Parameters<typeof CodeGraph.openSync>[1]): Graph {
    const cg = nativeOpen(projectRoot, options);
    const readOnly = options?.readOnly === true;
    opened.push(cg);
    openModes.set(cg, readOnly);
    onOpen?.(cg, readOnly);
    return cg;
  },
};
function projectGraphs(root: string): Graph[] { return opened.filter(cg => cg.getProjectRoot() === root); }
function readers(root: string): Graph[] { return projectGraphs(root).filter(cg => openModes.get(cg) === true); }
function writers(root: string): Graph[] { return projectGraphs(root).filter(cg => openModes.get(cg) === false); }
function reader(root: string, index = 0): Graph {
  const cg = readers(root)[index]; if (!cg) throw new Error(`Missing observed actual reader for ${root} #${index}`); return cg;
}
function writer(root: string, index = 0): Graph {
  const cg = writers(root)[index]; if (!cg) throw new Error(`Missing observed actual writer for ${root} #${index}`); return cg;
}
function expectHandles(root: string, readCount: number, writeCount: number): void {
  expect(projectGraphs(root)).toHaveLength(readCount + writeCount);
  expect(readers(root)).toHaveLength(readCount); expect(writers(root)).toHaveLength(writeCount);
  expect(new Set(projectGraphs(root)).size).toBe(readCount + writeCount);
  for (const cg of readers(root)) expect(cg.isWatching()).toBe(false);
}
function expectClosed(root: string): void {
  for (const cg of projectGraphs(root)) { expect(cg.isWatching()).toBe(false); expect(() => cg.getStats()).toThrow(); }
}
function expectOpen(root: string): void {
  for (const cg of projectGraphs(root)) expect(() => cg.getStats()).not.toThrow();
}

async function makeProject(dir: string, symbol: string): Promise<void> {
  fs.mkdirSync(path.join(dir, 'src'), { recursive: true });
  fs.writeFileSync(path.join(dir, 'src', 'sample.ts'), `export function ${symbol}() { return 1; }\n`);
  const cg = await CodeGraph.init(dir, { config: { include: ['**/*.ts'], exclude: [] } });
  await cg.indexAll();
  cg.close();
}

async function waitFor(check: () => Promise<boolean>, timeoutMs: number): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return true;
    await new Promise((r) => setTimeout(r, 100));
  }
  return check();
}

const CAPTURE_BYTES = 65_536;
const FIXTURE_WAIT_MS = 10_000;
const errorText = (error: unknown): string => Buffer.from(error instanceof Error ? error.stack ?? error.message : String(error)).subarray(0, 4096).toString('utf8');
const sha256 = (bytes: Buffer): string => createHash('sha256').update(bytes).digest('hex');
async function bounded<T>(work: Promise<T>, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`${label} exceeded ${FIXTURE_WAIT_MS} ms`)), FIXTURE_WAIT_MS);
    })]);
  } finally { if (timer) clearTimeout(timer); }
}

class PrefixCapture {
  readonly prefix = Buffer.alloc(CAPTURE_BYTES);
  seen = 0;
  retained = 0;
  ended = false;
  closed = false;
  error: string | null = null;
  constructor(stream: NodeJS.ReadableStream, fail: (message: string) => void) {
    stream.on('data', (value: Buffer | string) => {
      const bytes = Buffer.isBuffer(value) ? value : Buffer.from(value);
      this.seen += bytes.length;
      const keep = Math.min(bytes.length, CAPTURE_BYTES - this.retained);
      bytes.copy(this.prefix, this.retained, 0, keep);
      this.retained += keep;
      if (this.seen > CAPTURE_BYTES) fail('Owned child stream exceeded its cumulative byte bound');
    });
    stream.on('end', () => { this.ended = true; });
    stream.on('close', () => { this.closed = true; });
    stream.on('error', error => { this.error ??= errorText(error); fail(this.error); });
  }
  bytes(): Buffer { return this.prefix.subarray(0, this.retained); }
  facts() { return { seen: this.seen, retained: this.retained, truncated: this.seen > this.retained, ended: this.ended, closed: this.closed, error: this.error, sha256: sha256(this.bytes()) }; }
}

type OwnerPhase = 'ready' | 'catchup-entered' | 'catchup-released' | 'catchup-complete' | 'catchup-return-held' | 'catchup-return-released' | 'idle-armed' | 'stop-entered';
interface OwnerMessage { protocol: 1; phase: OwnerPhase; pid: number; root: string; mode: 'direct' | 'daemon' }
class OwnedChild {
  readonly stdout: PrefixCapture | null;
  readonly stderr: PrefixCapture | null;
  readonly messages: OwnerMessage[] = [];
  private messageBytes = 0;
  firstError: string | null = null;
  gracefulStopAttempted = false;
  gracefulFailure: string | null = null;
  forceAttempted = false;
  nativeExit: { code: number | null; signal: NodeJS.Signals | null } | null = null;
  nativeClose: { code: number | null; signal: NodeJS.Signals | null } | null = null;
  private readonly closed: Promise<void>;
  constructor(readonly child: ChildProcess, readonly root: string, readonly mode: 'direct' | 'daemon' | 'inert') {
    const fail = (message: string): void => { this.firstError ??= errorText(message); };
    this.closed = new Promise(resolve => child.once('close', (code, signal) => {
      this.nativeClose = { code, signal }; resolve();
    }));
    child.on('error', error => fail(errorText(error)));
    child.once('exit', (code, signal) => { this.nativeExit = { code, signal }; });
    this.stdout = child.stdout ? new PrefixCapture(child.stdout, fail) : null;
    this.stderr = child.stderr ? new PrefixCapture(child.stderr, fail) : null;
    child.on('message', value => {
      try { this.messageBytes += Buffer.byteLength(JSON.stringify(value) ?? 'null'); }
      catch (error) { fail(errorText(error)); return; }
      if (this.messageBytes > CAPTURE_BYTES) { fail('Owned-child IPC cumulative byte bound exceeded'); return; }
      const message = value as Partial<OwnerMessage> | null;
      if (!message || message.protocol !== 1 || typeof message.pid !== 'number' || !Number.isSafeInteger(message.pid) || message.pid <= 0
        || message.pid !== child.pid || typeof message.root !== 'string' || message.root !== root || message.mode !== mode
        || (message.mode !== 'direct' && message.mode !== 'daemon')
        || !['ready', 'catchup-entered', 'catchup-released', 'catchup-complete', 'catchup-return-held', 'catchup-return-released', 'idle-armed', 'stop-entered'].includes(String(message.phase))) {
        fail('Invalid owned-child IPC identity/phase'); return;
      }
      if (this.messages.length >= 64) { fail('Owned-child IPC record bound exceeded'); return; }
      this.messages.push({ protocol: 1, phase: message.phase as OwnerPhase, pid: message.pid, root: message.root, mode: message.mode });
    });
  }
  async phase(phase: OwnerPhase): Promise<OwnerMessage> {
    const found = (): OwnerMessage | undefined => this.messages.find(message => message.phase === phase);
    const ready = await waitFor(async () => {
      if (this.firstError) throw new Error(this.firstError);
      if (found()) return true;
      if (this.nativeClose) throw new Error(`Owner closed before ${phase}`);
      return false;
    }, FIXTURE_WAIT_MS);
    if (!ready) throw new Error(`Owner did not report ${phase} within ${FIXTURE_WAIT_MS} ms`);
    return found()!;
  }
  async send(phase: 'arm-idle' | 'release-catchup' | 'release-catchup-return' | 'stop'): Promise<void> {
    try {
      await bounded(new Promise<void>((resolve, reject) => {
        this.child.send({ protocol: 1, phase, root: this.root, pid: this.child.pid }, error => {
          if (error) reject(error); else resolve();
        });
      }), `Owner ${phase} send`);
    } catch (error) { this.firstError ??= errorText(error); throw error; }
  }
  async stopGracefully(): Promise<void> {
    this.gracefulStopAttempted = true;
    try {
      await this.send('stop');
      await this.phase('stop-entered');
      await this.waitClosed();
      this.assertCleanClose();
      // A surviving reader may already own the slot. The exited fixture
      // process must have released every record, rather than leaving stale
      // identity bytes that liveness cleanup happens to ignore later.
      for (const value of Object.values(ownerBytes(this.root))) {
        if (value !== null) expect(JSON.parse(value).pid).not.toBe(this.child.pid);
      }
    } catch (error) {
      this.gracefulFailure ??= errorText(error);
      this.firstError ??= this.gracefulFailure;
      throw error;
    }
  }
  forceAfterGracefulFailure(): void {
    if (!this.gracefulFailure) throw new Error('Refuse force without a retained graceful failure');
    this.forceAttempted = true;
    this.child.kill('SIGKILL');
  }
  async waitClosed(): Promise<void> { await bounded(this.closed, 'Owned native close'); }
  assertCleanClose(): void {
    expect(this.nativeExit).not.toBeNull();
    expect(this.nativeClose).not.toBeNull();
    if (this.mode !== 'inert') {
      expect(this.nativeExit).toEqual({ code: 0, signal: null });
      expect(this.nativeClose).toEqual({ code: 0, signal: null });
    }
    expect(this.firstError).toBeNull();
    for (const capture of [this.stdout, this.stderr]) if (capture) {
      expect(capture.closed).toBe(true);
      expect(capture.error).toBeNull();
      expect(capture.seen).toBe(capture.retained);
    }
  }
  facts() { return { pid: this.child.pid, root: this.root, mode: this.mode, messageBytes: this.messageBytes, messages: this.messages, firstError: this.firstError,
    gracefulStopAttempted: this.gracefulStopAttempted, gracefulFailure: this.gracefulFailure, forceAttempted: this.forceAttempted,
    nativeExit: this.nativeExit, nativeClose: this.nativeClose, stdout: this.stdout?.facts() ?? null, stderr: this.stderr?.facts() ?? null }; }
}

function ownerBytes(root: string): Record<string, string | null> {
  return Object.fromEntries(['writer.pid', 'daemon.pid'].map(name => {
    const file = path.join(root, '.codegraph', name);
    try {
      if (fs.statSync(file).size > 4096) throw new Error('Owned fixture lock record exceeds bound');
      return [name, fs.readFileSync(file, 'utf8')];
    } catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return [name, null]; throw error; }
  }));
}

function ownIndexBytes(root: string): Array<{ path: string; bytes: number; sha256: string }> {
  const directory = path.join(root, '.codegraph');
  const result: Array<{ path: string; bytes: number; sha256: string }> = [];
  let total = 0;
  const visit = (relative: string): void => {
    for (const entry of fs.readdirSync(path.join(directory, relative), { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      const name = path.join(relative, entry.name);
      if (entry.isSymbolicLink()) throw new Error('Unexpected link in owned fixture index');
      if (entry.isDirectory()) { visit(name); continue; }
      const file = path.join(directory, name), size = fs.statSync(file).size;
      total += size;
      if (result.length >= 128 || total > 16 * 1024 * 1024) throw new Error('Owned fixture index inventory exceeded bound');
      result.push({ path: name, bytes: size, sha256: sha256(fs.readFileSync(file)) });
    }
  };
  visit('');
  return result;
}

// Every case drives real catch-up syncs and waits for them (see the gate note
// below), so each gets room for a loaded Windows VM; cases that do more set
// their own (#1773). These bounds only catch a hang.
describe('MCP explicit projectPath lifecycle (#1835)', { timeout: 30_000 }, () => {
  let workspace: string;
  let serviceA: string;
  let serviceB: string;
  let engine: MCPEngine;
  const engines: MCPEngine[] = [];
  const children: OwnedChild[] = [];
  let requested: RequestedQueryPolicy;
  let prospective: ResolvedQueryPolicy;
  let cpuCount: number;
  let details: Record<string, unknown>;
  let hellos: Array<{ root: string; hello: DaemonHello; expectedVersion: string; prospective: ResolvedQueryPolicy }>;
  let callerPolicies: ResolvedQueryPolicy[];
  const lifecycleFacts: unknown[] = [];
  const prevDebounce = process.env.CODEGRAPH_WATCH_DEBOUNCE_MS;
  const prevGate = process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS;

  // Both hooks build indexes or wait out a sync; a loaded Windows VM exceeds
  // the default 10s hook timeout, and has taken over 30s (#1773).
  function makeCaller(options: MCPEngineOptions = {}): MCPEngine {
    callerPolicies.push(prospective);
    const caller = new BuiltMCPEngine({ ...options, queryAdmissionPolicy: prospective });
    engines.push(caller);
    return caller;
  }
  function ownChild(child: ChildProcess, root: string, mode: 'direct' | 'daemon' | 'inert'): OwnedChild {
    const owned = new OwnedChild(child, root, mode);
    children.push(owned);
    return owned;
  }

  beforeEach(async () => {
    workspace = '';
    details = {};
    hellos = [];
    callerPolicies = [];
    requested = requestedQueryPolicy(process.env);
    cpuCount = os.cpus().length;
    prospective = resolveLegacyQueryPolicy(requested, 'shared-daemon', cpuCount);
    workspace = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-1835-')));
    serviceA = path.join(workspace, 'service-a');
    serviceB = path.join(workspace, 'service-b');
    await makeProject(serviceA, 'alphaOriginal');
    await makeProject(serviceB, 'betaOriginal');
    process.env.CODEGRAPH_WATCH_DEBOUNCE_MS = '100';
    // These cases assert what a call left behind, so wait for its catch-up
    // rather than the gate's 3s serve-anyway deadline, which a loaded machine
    // misses (#1773). That deadline has its own coverage in
    // mcp-catchup-gate.test.ts; the eviction case sets its own values.
    process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS = '0';
    opened.length = 0;
    onOpen = undefined;
    vi.spyOn(CodeGraph, 'openSync').mockImplementation((root, options) => RecordingCodeGraph.openSync(root, options));
    const compare = compatibility.assertCompatibleQueryService;
    vi.spyOn(compatibility, 'assertCompatibleQueryService').mockImplementation((hello, version, policy, root) => {
      compare(hello, version, policy, root);
      if (hellos.length >= 64 || Buffer.byteLength(JSON.stringify(hello)) > 4096) throw new Error('Actual admission hello evidence exceeded bound');
      hellos.push({ root, hello: JSON.parse(JSON.stringify(hello)) as DaemonHello, expectedVersion: version, prospective: policy });
    });
    engine = makeCaller({ watch: true });
    // Two indexed children, none at the root: no default project (#1607).
    await engine.ensureInitialized(workspace);
  }, 60_000);

  afterEach(async (context) => {
    const primaryErrors = context.task.result?.errors ?? [];
    const primary = primaryErrors.slice(0, 8).map(error => errorText(error.stack ?? error.message));
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    let omittedCleanup = 0;
    const phase = async (name: string, work: () => void | Promise<void>): Promise<void> => {
      const record = { phase: name, error: null as string | null };
      if (cleanup.length < 32) cleanup.push(record); else omittedCleanup++;
      try { await work(); } catch (failure) { record.error = errorText(failure); }
    };
    const ownedEngines = engines.splice(0), ownedChildren = children.splice(0);
    const states: unknown[] = [];
    await phase('engine-observations', () => { for (const owned of ownedEngines) states.push(owned.getQueryExecutionState()); });
    await Promise.all(ownedEngines.map((owned, index) => phase(`engine-stop-${index}`, () => bounded(owned.stop(), 'Engine stop'))));
    await Promise.all(ownedChildren.map(async (owned, index) => {
      await phase(`child-stop-${index}`, async () => {
        if (!owned.nativeExit && !owned.nativeClose) {
          if (!owned.child.pid) throw new Error('Owned child has no spawned PID');
          // Inert holders own no engine/daemon and have no graceful IPC
          // protocol. Their actual forced-exit facts remain separately typed.
          if (owned.mode === 'inert') owned.child.kill('SIGTERM');
          else await owned.stopGracefully();
        }
      });
      await phase(`child-failed-stop-force-${index}`, () => {
        if (!owned.nativeExit && !owned.nativeClose && owned.gracefulFailure) owned.forceAfterGracefulFailure();
      });
      await phase(`child-close-${index}`, async () => { await owned.waitClosed(); owned.assertCleanClose(); });
    }));
    await phase('both-native-handles-drained', () => {
      for (const cg of opened) { expect(cg.isWatching()).toBe(false); expect(() => cg.getStats()).toThrow(); }
    });
    await phase('restore-mocks', () => { vi.restoreAllMocks(); });
    await phase('restore-environment', () => {
      if (prevDebounce === undefined) delete process.env.CODEGRAPH_WATCH_DEBOUNCE_MS;
      else process.env.CODEGRAPH_WATCH_DEBOUNCE_MS = prevDebounce;
      if (prevGate === undefined) delete process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS;
      else process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS = prevGate;
    });
    const captures: Array<{ child: number; stream: string; path: string }> = [];
    for (const [index, owned] of ownedChildren.entries()) for (const [name, capture] of [['stdout', owned.stdout], ['stderr', owned.stderr]] as const) {
      if (!capture) continue;
      const file = path.join(workspace, `owner-${index}-${name}.raw`);
      await phase(`retain-${index}-${name}`, () => { fs.writeFileSync(file, capture.bytes()); captures.push({ child: index, stream: name, path: file }); });
    }
    const facts = { name: context.task.name, workspace, retention: 'all-outcomes; inert workspace/index/captures retained; native resources must close',
      primary, omittedPrimary: Math.max(0, primaryErrors.length - primary.length), details, requested, cpuCount, prospective,
      callersUseSameProspective: callerPolicies.every(policy => policy === prospective),
      actualGraphOpens: opened.map(cg => ({ root: cg.getProjectRoot(), readOnly: openModes.get(cg) })),
      actualHellos: hellos, engineStatesBeforeCleanup: states, children: ownedChildren.map(owned => owned.facts()), captures, cleanup, omittedCleanup };
    if (lifecycleFacts.length >= 32) {
      await phase('row-bound', () => { throw new Error('Lifecycle row evidence bound exceeded'); });
    } else lifecycleFacts.push(facts);
    const file = process.env.VALIDATION_OUT ? path.join(process.env.VALIDATION_OUT, 'lifecycle-fixture-facts.json') : path.join(workspace, 'lifecycle-fixture-facts.json');
    await phase('retain-facts', () => { fs.writeFileSync(file, JSON.stringify(lifecycleFacts, null, 2) + '\n'); });
    const failures = cleanup.filter(item => item.error !== null);
    if (failures.length || omittedCleanup) {
      // A failed evidence write still leaves a bounded diagnostic in the outer
      // runner's stderr; never replace an already recorded primary failure.
      try { process.stderr.write(errorText(JSON.stringify({ fixture: workspace, primary, cleanup: failures, omittedCleanup })) + '\n'); } catch { /* preserve primary */ }
      if (!primary.length) throw new Error(`Lifecycle fixture cleanup/evidence failed: ${errorText(JSON.stringify(failures))}`);
    }
  }, 60_000);

  function names(root: string): string[] {
    const reader = nativeOpen(root, { readOnly: true });
    try { return reader.getNodesByKind('function').map((n) => n.name); }
    finally { reader.close(); }
  }

  /**
   * A daemon owner starts with idle exit OFF. Armed at startup, its 500ms
   * timer raced this process's first connection and lost on a loaded machine,
   * so the daemon exited before any engine reached it (#1773). A test that
   * needs the idle exit enables it with {@link armIdleExit} once it holds a
   * session; the daemon reads the value each time its last client leaves.
   */
  async function startOwner(mode: 'direct' | 'daemon', slowCatchUp = false, root = serviceA): Promise<OwnedChild> {
    const modulePath = path.resolve(__dirname, '../dist/mcp');
    const ownerScript = mode === 'daemon' ? `
      const { Daemon, tryAcquireDaemonLock } = require(process.argv[1] + '/daemon');
      if (tryAcquireDaemonLock(root).kind !== 'acquired') throw new Error('Owned daemon lock unavailable');
      const daemon = new Daemon(root, { idleTimeoutMs: 0 });
      installStop(() => daemon.stop('fixture IPC stop'));
      process.on('message', (message) => {
        if (!isCommand(message, 'arm-idle')) return;
        daemon.idleTimeoutMs = 500;
        report('idle-armed');
      });
      daemon.start().then(result => {
        if (typeof result.socketPath !== 'string' || result.lock.pid !== process.pid) throw new Error('Owned daemon did not start');
        if (!process.env.CG_TEST_HOLD_CATCHUP) report('ready');
      }).catch(fail);
    ` : `
      const { MCPEngine } = require(process.argv[1] + '/engine');
      const engine = new MCPEngine();
      installStop(() => engine.stop());
      engine.ensureInitialized(root).then(async () => {
        await engine.getToolHandler().execute('codegraph_status', {});
        if (!process.env.CG_TEST_HOLD_CATCHUP) report('ready');
      }).catch(fail);
    `;
    const script = `
      const root = process.argv[2], mode = process.argv[3];
      const fail = error => { process.stderr.write(String(error.stack || error) + '\\n'); process.exitCode = 1; };
      const report = phase => new Promise(resolve => {
        try {
          process.send({ protocol: 1, phase, pid: process.pid, root, mode }, error => {
            if (error) { fail(error); resolve(false); } else resolve(true);
          });
        } catch (error) { fail(error); resolve(false); }
      });
      const isCommand = (message, phase) => message && message.protocol === 1 && message.pid === process.pid && message.root === root && message.phase === phase;
      const installStop = stop => {
        let stopping = false;
        process.on('message', message => {
          if (!isCommand(message, 'stop') || stopping) return;
          stopping = true;
          (async () => {
            if (!await report('stop-entered')) throw new Error('Owned stop ACK failed');
            await stop();
            process.exitCode ??= 0;
            // Drain the real watcher/socket close callbacks naturally. The
            // fixture IPC handle must not keep a stopped direct owner alive.
            if (process.connected) process.disconnect();
          })().catch(fail);
        });
      };
      if (process.env.CG_TEST_HOLD_CATCHUP) {
        const CodeGraph = require(process.argv[1] + '/../index').default;
        const sync = CodeGraph.prototype.sync;
        let first = true;
        CodeGraph.prototype.sync = async function (...args) {
          if (!first) return sync.apply(this, args);
          first = false;
          await new Promise((resolve, reject) => {
            const timer = setTimeout(() => { process.off('message', release); reject(new Error('Catch-up release deadline')); }, 10000);
            const release = message => {
              if (!isCommand(message, 'release-catchup')) return;
              clearTimeout(timer); process.off('message', release); report('catchup-released'); resolve();
            };
            process.on('message', release);
            report('catchup-entered');
          });
          await new Promise((resolve) => setTimeout(resolve, 300));
          const result = await sync.apply(this, args);
          report('catchup-complete');
          // Hold only the return from the real completed sync. This makes
          // first-query gating observable without using IPC delivery order
          // as evidence that the first query waited for actual completion.
          await new Promise((resolve, reject) => {
            const timer = setTimeout(() => { process.off('message', release); reject(new Error('Catch-up return release deadline')); }, 10000);
            const release = message => {
              if (!isCommand(message, 'release-catchup-return')) return;
              clearTimeout(timer); process.off('message', release); report('catchup-return-released'); resolve();
            };
            process.on('message', release);
            report('catchup-return-held');
          });
          return result;
        };
      }
    ` + ownerScript;
    const environment: NodeJS.ProcessEnv = { ...process.env, CODEGRAPH_NO_DAEMON: mode === 'daemon' ? '0' : '1', CG_TEST_HOLD_CATCHUP: slowCatchUp ? '1' : '' };
    for (const name of ['CODEGRAPH_QUERY_MODE', 'CODEGRAPH_QUERY_POOL_SIZE', 'CODEGRAPH_QUERY_BUSY_TIMEOUT_MS']) delete environment[name];
    if (requested.mode !== undefined) environment.CODEGRAPH_QUERY_MODE = requested.mode;
    if (requested.poolSize !== undefined) environment.CODEGRAPH_QUERY_POOL_SIZE = requested.poolSize;
    if (requested.busyTimeoutMs !== undefined) environment.CODEGRAPH_QUERY_BUSY_TIMEOUT_MS = requested.busyTimeoutMs;
    // Ordinary direct writers do not advertise or grant shared-service policy.
    if (mode === 'direct') environment.CODEGRAPH_QUERY_POOL_SIZE = '0';
    const child = ownChild(spawn(process.execPath, ['-e', script, modulePath, root, mode], {
      stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
      env: environment,
    }), root, mode);
    await child.phase(slowCatchUp ? 'catchup-entered' : 'ready');
    return child;
  }

  async function armIdleExit(owner: OwnedChild): Promise<void> {
    await owner.send('arm-idle');
    await owner.phase('idle-armed');
  }

  function assertActualDaemonHello(owner: OwnedChild, minimum = 1): void {
    const observed = hellos.filter(item => item.root === owner.root && item.hello.pid === owner.child.pid);
    expect(observed.length).toBeGreaterThanOrEqual(minimum);
    const generations = new Set<string>();
    for (const item of observed) {
      expect(item.expectedVersion).toBe(CodeGraphPackageVersion);
      expect(item.hello.codegraph).toBe(CodeGraphPackageVersion);
      expect(item.hello.socketPath).toBe(getDaemonSocketPath(owner.root));
      expect(item.prospective).toEqual(prospective);
      const actual = compatibility.readSharedQueryService(item.hello, CodeGraphPackageVersion, owner.root);
      if (actual.effective === 'unknown' || actual.ownership === 'unknown') throw new Error('Unknown actual daemon hello');
      expect(actual.effective.concurrency).toEqual(prospective.concurrency);
      expect(actual.effective.deadline).toEqual(prospective.deadline);
      expect(actual.effective.retention).toBe(prospective.retention);
      expect(actual.ownership.pid).toBe(owner.child.pid);
      expect(actual.ownership.generation).not.toBe('');
      generations.add(actual.ownership.generation);
    }
    expect(generations.size).toBe(1);
    const records = ownerBytes(owner.root);
    expect(JSON.parse(records['writer.pid']!).pid).toBe(owner.child.pid);
    expect(JSON.parse(records['daemon.pid']!).pid).toBe(owner.child.pid);
    details.actualDaemon = { pid: owner.child.pid, root: owner.root, generation: [...generations][0], observedHellos: observed.length, records };
  }

  async function searchDuringCatchUp(owner: OwnedChild, symbol: string): Promise<string> {
    expect(owner.messages.some(message => message.phase === 'catchup-entered')).toBe(true);
    expect(owner.messages.some(message => message.phase === 'catchup-released' || message.phase === 'catchup-complete')).toBe(false);
    let settled = false;
    let returnReleaseIssued = false;
    let settledBeforeReturnRelease = false;
    const pending = search(owner.root, symbol).then(result => {
      settled = true; settledBeforeReturnRelease = !returnReleaseIssued; return result;
    }, error => {
      settled = true; settledBeforeReturnRelease = !returnReleaseIssued; throw error;
    });
    // Retain a rejection handler even if IPC release fails before awaiting the
    // already-started real request. This barrier orders fixture setup only.
    void pending.catch(() => undefined);
    details.firstSearch = { root: owner.root, symbol, invokedWhileSyncHeld: true };
    await owner.send('release-catchup');
    await owner.phase('catchup-complete');
    await owner.phase('catchup-return-held');
    details.firstSearch = { ...details.firstSearch as object, actualSyncCompleted: true, trailingReturnHeld: true, settledWhileReturnHeld: settled };
    expect(settled).toBe(false);
    returnReleaseIssued = true;
    await owner.send('release-catchup-return');
    const result = await pending;
    await owner.phase('catchup-return-released');
    details.firstSearch = { ...details.firstSearch as object, result, returnReleaseIssued, settledBeforeReturnRelease };
    expect(settledBeforeReturnRelease).toBe(false);
    return result;
  }

  async function search(projectPath: string, symbol: string): Promise<string> {
    const res = await engine.getToolHandler().execute('codegraph_search', { query: symbol, projectPath });
    expect(res.isError).toBeFalsy();
    return res.content.map((c) => (c.type === 'text' ? c.text : '')).join('\n');
  }

  for (const options of [{ watch: true }, { watch: false }, { readOnly: true }]) {
    it(`defers explicit project opens during a rebuild (${JSON.stringify(options)})`, async () => {
      await engine.stop();
      engine = makeCaller(options);
      const holder = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
      ownChild(holder, serviceA, 'inert');
      if (!holder.pid) throw new Error('Failed to spawn rebuild holder');
      const fence = path.join(serviceA, '.codegraph/rebuild.pid');
      fs.writeFileSync(fence, JSON.stringify({ pid: holder.pid, mode: 'rebuild', startedAt: Date.now() }));
      const response = await engine.getToolHandler().execute('codegraph_search', {
        projectPath: serviceA, query: 'alphaOriginal',
      });
      expect(JSON.stringify(response)).toContain('rebuild is in progress');
      // Expected and temporary: guidance, never a tool error that teaches abandonment.
      expect(JSON.stringify(response)).not.toContain('"isError":true');
      expect(opened).toHaveLength(0);
      expect(fs.existsSync(path.join(serviceA, '.codegraph/writer.pid'))).toBe(false);
      fs.unlinkSync(fence);
      expect(await search(serviceA, 'alphaOriginal')).toContain('alphaOriginal');
    });

    it(`defers default project opens and synchronous retries during a rebuild (${JSON.stringify(options)})`, async () => {
      const reader = makeCaller(options);
      const holder = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
      ownChild(holder, serviceA, 'inert');
      if (!holder.pid) throw new Error('Failed to spawn rebuild holder');
      const fence = path.join(serviceA, '.codegraph/rebuild.pid');
      fs.writeFileSync(fence, JSON.stringify({ pid: holder.pid, mode: 'rebuild', startedAt: Date.now() }));
      await reader.ensureInitialized(serviceA);
      expect(reader.hasDefaultCodeGraph()).toBe(false);
      await reader.retryInitialize(serviceA);
      expect(reader.hasDefaultCodeGraph()).toBe(false);
      expect(fs.existsSync(path.join(serviceA, '.codegraph/writer.pid'))).toBe(false);
      fs.unlinkSync(fence);
      await reader.retryInitialize(serviceA);
      expect(reader.hasDefaultCodeGraph()).toBe(true);
    });
  }

  it('catches up an edit made before the first call and watches later edits', async () => {
    // Edited while no server owned the index — the catch-up path.
    fs.writeFileSync(path.join(serviceA, 'src', 'sample.ts'), 'export function alphaRenamed() { return 1; }\n');
    const first = await search(serviceA, 'alphaRenamed');
    expect(first).toContain('alphaRenamed');
    expect(names(serviceA)).toContain('alphaRenamed');
    expect(names(serviceA)).not.toContain('alphaOriginal');
    expect(first).not.toContain('alphaOriginal');
    expect(opened).toHaveLength(2); expectHandles(serviceA, 1, 1);
    expect(writer(serviceA).isWatching()).toBe(true);
    await writer(serviceA).waitUntilWatcherReady(5000);

    // Edited while the project stays cached — the watcher path.
    fs.writeFileSync(path.join(serviceA, 'src', 'sample.ts'), 'export function alphaWatched() { return 1; }\n');
    const seen = await waitFor(async () => names(serviceA).includes('alphaWatched'), 10000);
    expect(seen).toBe(true);
  });

  it.runIf(process.platform !== 'win32')('keeps one watched instance per canonical root and closes it on stop()', async () => {
    const link = path.join(workspace, 'link-to-b');
    fs.symlinkSync(serviceB, link, 'dir');
    expect(await search(serviceB, 'betaOriginal')).toContain('betaOriginal');
    expect(await search(link, 'betaOriginal')).toContain('betaOriginal');
    expect(await search(path.join(serviceB, 'src'), 'betaOriginal')).toContain('betaOriginal');
    expect(opened).toHaveLength(2); expectHandles(serviceB, 1, 1);
    expect(writer(serviceB).isWatching()).toBe(true);
    expect(fs.existsSync(path.join(serviceB, '.codegraph', 'writer.pid'))).toBe(true);

    await engine.stop();
    expect(writer(serviceB).isWatching()).toBe(false);
    expect(fs.existsSync(path.join(serviceB, '.codegraph', 'writer.pid'))).toBe(false);
    expectClosed(serviceB);
  });

  it('does not take over a project another live process is already syncing', async () => {
    const foreign = await startOwner('direct', false, serviceB);
    const before = ownerBytes(serviceB);
    const writer = JSON.parse(before['writer.pid']!);
    expect(writer.pid).toBe(foreign.child.pid);
    expect(writer.mode).toBe('fallback');
    expect(before['daemon.pid']).toBeNull();
    const result = await search(serviceB, 'betaOriginal');
    details.foreignWriter = { pid: foreign.child.pid, root: serviceB, actualWriterMode: writer.mode, ordinaryBareEngineOwner: true, before, result };
    expect(result).toContain('betaOriginal');
    expect(opened).toHaveLength(1); expectHandles(serviceB, 1, 0);
    expect(reader(serviceB).isWatching()).toBe(false);
    expect(ownerBytes(serviceB)).toEqual(before);
    await engine.stop();
    const after = ownerBytes(serviceB);
    details.foreignWriter = { ...details.foreignWriter as object, after, readerWatchingAfterStop: reader(serviceB).isWatching() };
    expectClosed(serviceB);
    expect(after).toEqual(before);
    expect(foreign.nativeExit).toBeNull();
    expect(foreign.nativeClose).toBeNull();
    expect(() => process.kill(foreign.child.pid!, 0)).not.toThrow();
  });

  it('refuses an unproved live daemon claim without opening or changing the project', async () => {
    const foreign = ownChild(spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' }), serviceB, 'inert');
    if (!foreign.child.pid) throw new Error('Failed to spawn owned daemon-claim holder');
    expect(() => process.kill(foreign.child.pid!, 0)).not.toThrow();
    fs.writeFileSync(path.join(serviceB, '.codegraph', 'writer.pid'), JSON.stringify({ pid: foreign.child.pid, mode: 'daemon', startedAt: Date.now() }));
    const before = ownIndexBytes(serviceB), recordsBefore = ownerBytes(serviceB);
    const dispatch = vi.spyOn(engine.getToolHandler(), 'executeReadTool');
    const sync = vi.spyOn(CodeGraph.prototype, 'sync');
    const watch = vi.spyOn(CodeGraph.prototype, 'watch');
    const workersBefore = engine.getQueryExecutionState().actual.workers;
    const refusals: unknown[] = [];
    details.unknownDaemonClaim = { pid: foreign.child.pid, root: serviceB, before, recordsBefore, workersBefore, refusals };
    for (let attempt = 0; attempt < 2; attempt++) {
      let refusal: unknown;
      try { await search(serviceB, 'betaOriginal'); } catch (error) { refusal = error; }
      if (!compatibility.isServicePolicyRefusal(refusal)) throw new Error(`Expected structural unknown refusal, received ${errorText(refusal)}`);
      refusals.push({ code: refusal.code, reason: refusal.reason, projectRoot: refusal.projectRoot, guidance: refusal.guidance });
      expect(refusal.code).toBe('CODEGRAPH_QUERY_POLICY_REFUSED');
      expect(refusal.reason).toBe('unknown');
      expect(refusal.projectRoot).toBe(serviceB);
      expect(refusal.guidance).toContain(serviceB);
    }
    await engine.stop();
    const after = ownIndexBytes(serviceB), recordsAfter = ownerBytes(serviceB);
    const workersAfter = engine.getQueryExecutionState().actual.workers;
    details.unknownDaemonClaim = { ...details.unknownDaemonClaim as object, after, recordsAfter, workersAfter,
      graphOpens: opened.length, dispatches: dispatch.mock.calls.length, syncs: sync.mock.calls.length, watches: watch.mock.calls.length };
    expect(after).toEqual(before);
    expect(recordsAfter).toEqual(recordsBefore);
    expect(opened).toHaveLength(0);
    expect(dispatch).not.toHaveBeenCalled();
    expect(sync).not.toHaveBeenCalled();
    expect(watch).not.toHaveBeenCalled();
    expect(workersBefore).toBe(0);
    expect(workersAfter).toBe(0);
    expect(foreign.nativeExit).toBeNull();
    expect(foreign.nativeClose).toBeNull();
    expect(() => process.kill(foreign.child.pid!, 0)).not.toThrow();
  });

  it('catches up both children without selecting a default', async () => {
    for (const [root, symbol] of [[serviceA, 'alphaNew'], [serviceB, 'betaNew']]) {
      fs.writeFileSync(path.join(root!, 'src/sample.ts'), `export function ${symbol}() {}\n`);
      await search(root!, symbol!);
      expect(names(root!)).toEqual([symbol]);
    }
    expect(engine.hasDefaultCodeGraph()).toBe(false);
  });

  it('shares the catch-up gate across concurrent calls and engines', async () => {
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    let enterSync!: () => void, publishReader!: () => void;
    const syncEntered = new Promise<void>(resolve => { enterSync = resolve; });
    const readerPublished = new Promise<void>(resolve => { publishReader = resolve; });
    onOpen = (cg, readOnly) => {
      if (cg.getProjectRoot() !== serviceA) return;
      if (readOnly) { publishReader(); return; }
      const sync = cg.sync.bind(cg);
      vi.spyOn(cg, 'sync').mockImplementation(async (...args) => { enterSync(); await held; return sync(...args); });
    };
    fs.writeFileSync(path.join(serviceA, 'src/sample.ts'), 'export function concurrentNew() {}\n');
    const second = makeCaller();
    let completed = 0;
    const calls = [
      search(serviceA, 'concurrentNew'),
      search(path.join(serviceA, 'src'), 'concurrentNew'),
      second.getToolHandler().execute('codegraph_search', { projectPath: serviceA, query: 'concurrentNew' }),
    ].map((p) => p.then((r) => { completed++; return r; }));
    try {
      // Publication follows actual asynchronous admission; elapsed time is
      // not evidence that both shared native handles reached their barriers.
      await bounded(Promise.all([syncEntered, readerPublished]), 'Shared writer sync and reader publication');
      expect(completed).toBe(0);
      expect(opened).toHaveLength(2); expectHandles(serviceA, 1, 1);
    } finally { release(); }
    await Promise.all(calls);
    expect(names(serviceA)).toEqual(['concurrentNew']);
    expect(writer(serviceA).sync).toHaveBeenCalledTimes(1);
  });

  it('keeps watching after one of two engines releases its lease', async () => {
    const second = makeCaller();
    await search(serviceA, 'alphaOriginal');
    await second.getToolHandler().execute('codegraph_search', { projectPath: serviceA, query: 'alphaOriginal' });
    expect(opened).toHaveLength(2); expectHandles(serviceA, 1, 1);
    await writer(serviceA).waitUntilWatcherReady(5000);
    await engine.stop();
    expect(writer(serviceA).isWatching()).toBe(true); expectOpen(serviceA);
    expect(fs.existsSync(path.join(serviceA, '.codegraph/writer.pid'))).toBe(true);
    fs.writeFileSync(path.join(serviceA, 'src/sample.ts'), 'export function survivingSession() {}\n');
    expect(await waitFor(async () => names(serviceA).includes('survivingSession'), 10000)).toBe(true);
    await second.stop();
    expect(fs.existsSync(path.join(serviceA, '.codegraph/writer.pid'))).toBe(false);
    expectClosed(serviceA);
  });

  it('retries a foreign writer and catches up after its ownership ends', async () => {
    const lock = path.join(serviceA, '.codegraph/writer.pid');
    fs.writeFileSync(lock, JSON.stringify({ pid: process.ppid, mode: 'direct', startedAt: Date.now() }));
    await search(serviceA, 'alphaOriginal');
    expectHandles(serviceA, 1, 0); expect(reader(serviceA).isWatching()).toBe(false);
    fs.writeFileSync(path.join(serviceA, 'src/sample.ts'), 'export function afterOwnerExit() {}\n');
    fs.unlinkSync(lock);
    // No further query should be necessary to activate the surviving session.
    expect(await waitFor(async () => names(serviceA).includes('afterOwnerExit'), 10000)).toBe(true);
    expectHandles(serviceA, 1, 1); expect(writer(serviceA).isWatching()).toBe(true);
  });

  it('quietly retries catch-up after an indexing lock is released (#1361)', async () => {
    const lock = path.join(serviceA, '.codegraph/codegraph.lock');
    const writerPath = path.join(serviceA, '.codegraph/writer.pid');
    const prev = process.env.CODEGRAPH_NO_WATCH;
    // Ensure the lifecycle retry, not a watcher event, repairs the index.
    process.env.CODEGRAPH_NO_WATCH = '1';
    const stderr = vi.spyOn(process.stderr, 'write');
    try {
      fs.writeFileSync(path.join(serviceA, 'src/sample.ts'), 'export function afterIndexLock() {}\n');
      fs.writeFileSync(lock, String(process.pid));
      await search(serviceA, 'afterIndexLock');
      expect(names(serviceA)).toEqual(['alphaOriginal']);
      expect(fs.readFileSync(lock, 'utf8')).toBe(String(process.pid));
      expect(JSON.parse(fs.readFileSync(writerPath, 'utf8')).ready).toBe(false);
      expect(stderr.mock.calls.map(([text]) => String(text)).join('')).not.toContain('Catch-up sync failed');

      fs.unlinkSync(lock);
      // No second query: the lifecycle's periodic retry must finish catch-up.
      // SQLite rows can be visible before sync's final maintenance finishes.
      expect(await waitFor(async () => JSON.parse(fs.readFileSync(writerPath, 'utf8')).ready === true, 10000)).toBe(true);
      expect(names(serviceA)).toEqual(['afterIndexLock']);
      expectHandles(serviceA, 1, 1); expect(writer(serviceA).isWatching()).toBe(false);
    } finally {
      fs.rmSync(lock, { force: true });
      stderr.mockRestore();
      if (prev === undefined) delete process.env.CODEGRAPH_NO_WATCH;
      else process.env.CODEGRAPH_NO_WATCH = prev;
    }
  });

  it('honors no-watch for explicit projects', async () => {
    await engine.stop();
    engine = makeCaller({ watch: false });
    fs.writeFileSync(path.join(serviceA, 'src/sample.ts'), 'export function noWatchEdit() {}\n');
    await search(serviceA, 'noWatchEdit');
    expect(names(serviceA)).toEqual(['alphaOriginal']);
    expect(opened).toHaveLength(1); expectHandles(serviceA, 1, 0); expect(reader(serviceA).isWatching()).toBe(false);
    expect(fs.existsSync(path.join(serviceA, '.codegraph/writer.pid'))).toBe(false);
  });

  it('honors the CLI no-watch policy while still catching up on access', async () => {
    const prev = process.env.CODEGRAPH_NO_WATCH;
    process.env.CODEGRAPH_NO_WATCH = '1';
    try {
      fs.writeFileSync(path.join(serviceA, 'src/sample.ts'), 'export function policyCatchUp() {}\n');
      await search(serviceA, 'policyCatchUp');
      expect(names(serviceA)).toEqual(['policyCatchUp']);
      expect(opened).toHaveLength(2); expectHandles(serviceA, 1, 1); expect(writer(serviceA).isWatching()).toBe(false);
      fs.writeFileSync(path.join(serviceA, 'src/sample.ts'), 'export function policyUnwatched() {}\n');
      await search(serviceA, 'policyUnwatched');
      expect(names(serviceA)).toEqual(['policyCatchUp']);
    } finally {
      if (prev === undefined) delete process.env.CODEGRAPH_NO_WATCH;
      else process.env.CODEGRAPH_NO_WATCH = prev;
    }
  });

  // Builds and reconciles nine real indexes; a Windows VM exceeds the default
  // 5s, and a loaded one exceeded 15s (#1773).
  it('defers eviction and shutdown until an active catch-up finishes', async () => {
    const roots = [serviceA, serviceB];
    for (let i = roots.length; i <= MAX_CACHED_PROJECTS; i++) {
      const root = path.join(workspace, `extra-${i}`);
      await makeProject(root, `symbol${i}`);
      roots.push(root);
    }
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    onOpen = (cg, readOnly) => {
      if (readOnly || cg.getProjectRoot() !== serviceA) return;
      const sync = cg.sync.bind(cg);
      vi.spyOn(cg, 'sync').mockImplementation(async (...args) => { await held; return sync(...args); });
    };
    const prev = process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS;
    process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS = '10';
    try {
      fs.writeFileSync(path.join(serviceA, 'src/sample.ts'), 'export function evictionCatchUp() {}\n');
      await search(serviceA, 'evictionCatchUp');
      // Only A should time out. Finish every other catch-up before expecting
      // B to be the oldest evictable entry; 10ms can expire on those too.
      process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS = '0';
      for (const root of roots.slice(1)) await search(root, 'symbol');
      expectHandles(serviceA, 1, 1); expectHandles(serviceB, 1, 1);
      expectOpen(serviceA); expectClosed(serviceB);
      let stopped = false;
      const stop = engine.stop().then(() => { stopped = true; });
      await new Promise((r) => setTimeout(r, 50));
      expect(stopped).toBe(false);
      expectOpen(serviceA);
      release();
      await stop;
      expect(names(serviceA)).toEqual(['evictionCatchUp']);
      expectClosed(serviceA);
      expect(fs.existsSync(path.join(serviceA, '.codegraph/writer.pid'))).toBe(false);
    } finally {
      release();
      if (prev === undefined) delete process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS;
      else process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS = prev;
    }
  }, 60_000);


  // A daemon that answered one projectPath query for another project must not
  // keep that project's writer lock until it exits: the project's own daemon
  // and `codegraph index` there would stay locked out (#2087).
  it('releases an idle explicit project and its writer lock, and retakes it on the next call', async () => {
    const prev = process.env.CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS;
    process.env.CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS = '200';
    const lock = path.join(serviceB, '.codegraph/writer.pid');
    try {
      expect(await search(serviceB, 'betaOriginal')).toContain('betaOriginal');
      expect(JSON.parse(fs.readFileSync(lock, 'utf8')).pid).toBe(process.pid);
      expect(opened).toHaveLength(2); expectHandles(serviceB, 1, 1); expect(writer(serviceB).isWatching()).toBe(true);

      expect(await waitFor(async () => !fs.existsSync(lock), 10000)).toBe(true);
      expectClosed(serviceB);

      // Still synchronized while in use: the next call reopens and catches up.
      fs.writeFileSync(path.join(serviceB, 'src/sample.ts'), 'export function afterIdleRelease() {}\n');
      expect(await search(serviceB, 'afterIdleRelease')).toContain('afterIdleRelease');
      expect(opened).toHaveLength(4); expectHandles(serviceB, 2, 2);
      expect(writer(serviceB, 1).isWatching()).toBe(true);
      expect(() => reader(serviceB, 0).getStats()).toThrow(); expect(() => writer(serviceB, 0).getStats()).toThrow();
      expect(() => reader(serviceB, 1).getStats()).not.toThrow();
      expect(JSON.parse(fs.readFileSync(lock, 'utf8')).pid).toBe(process.pid);
    } finally {
      if (prev === undefined) delete process.env.CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS;
      else process.env.CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS = prev;
    }
  });

  it('defers an idle release until an active catch-up finishes (#2087)', async () => {
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    onOpen = (cg, readOnly) => {
      if (readOnly) return;
      const sync = cg.sync.bind(cg);
      vi.spyOn(cg, 'sync').mockImplementation(async (...args) => { await held; return sync(...args); });
    };
    const prevIdle = process.env.CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS;
    process.env.CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS = '50';
    process.env.CODEGRAPH_CATCHUP_GATE_TIMEOUT_MS = '10';
    const lock = path.join(serviceB, '.codegraph/writer.pid');
    try {
      fs.writeFileSync(path.join(serviceB, 'src/sample.ts'), 'export function idleCatchUp() {}\n');
      await search(serviceB, 'betaOriginal');
      await new Promise((r) => setTimeout(r, 300));
      expectHandles(serviceB, 1, 1); expectOpen(serviceB);
      expect(fs.existsSync(lock)).toBe(true);
      release();
      expect(await waitFor(async () => !fs.existsSync(lock), 10000)).toBe(true);
      expect(names(serviceB)).toEqual(['idleCatchUp']);
      expectClosed(serviceB);
    } finally {
      release();
      if (prevIdle === undefined) delete process.env.CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS;
      else process.env.CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS = prevIdle;
    }
  });

  it('drains a tool operation before closing its cached graph', async () => {
    await search(serviceA, 'alphaOriginal');
    const handler = engine.getToolHandler();
    const dispatch = handler.executeReadTool.bind(handler);
    let release!: () => void;
    const held = new Promise<void>((resolve) => { release = resolve; });
    vi.spyOn(handler, 'executeReadTool').mockImplementation(async (...args) => {
      await held;
      return dispatch(...args);
    });
    const call = search(serviceA, 'alphaOriginal');
    let stopped = false;
    const stop = engine.stop().then(() => { stopped = true; });
    try {
      await new Promise((resolve) => setTimeout(resolve, 50));
      expect(stopped).toBe(false);
      expectHandles(serviceA, 1, 1); expectOpen(serviceA); expect(reader(serviceA).getStats().fileCount).toBe(1);
    } finally { release(); }
    await call;
    await stop;
    expectClosed(serviceA);
  });

  it.each(['direct', 'daemon'] as const)('takes over when a real %s owner exits', async (mode) => {
    fs.writeFileSync(path.join(serviceA, 'src/sample.ts'), 'export function ownerCatchUp() {}\n');
    const owner = await startOwner(mode, true);
    const before = ownerBytes(serviceA);
    expect(await searchDuringCatchUp(owner, 'ownerCatchUp')).toContain('ownerCatchUp');
    expect(names(serviceA)).toEqual(['ownerCatchUp']);
    expect(opened).toHaveLength(1); expectHandles(serviceA, 1, 0); expect(reader(serviceA).isWatching()).toBe(false);
    const afterCatchUp = ownerBytes(serviceA);
    const lock = JSON.parse(afterCatchUp['writer.pid']!);
    expect(lock.pid).toBe(owner.child.pid);
    // The owner's legitimate startup transition changes only ready, never its
    // identity or ownership. Record that transition rather than hiding it.
    expect(lock).toEqual({ ...JSON.parse(before['writer.pid']!), ready: true });
    expect(afterCatchUp['daemon.pid']).toEqual(before['daemon.pid']);
    if (mode === 'daemon') assertActualDaemonHello(owner);
    details.takeover = { mode, before, afterCatchUp, readerWatching: reader(serviceA).isWatching() };
    await owner.stopGracefully();
    expect(owner.nativeExit).toEqual({ code: 0, signal: null });
    const afterOwnerClose = ownerBytes(serviceA);
    expect(afterOwnerClose['daemon.pid']).toBeNull();
    // The surviving lease may already have reacquired the slot on its timer.
    // It must never retain the exited owner, and no query is used to trigger it.
    if (afterOwnerClose['writer.pid'] !== null) expect(JSON.parse(afterOwnerClose['writer.pid']).pid).toBe(process.pid);
    fs.writeFileSync(path.join(serviceA, 'src/sample.ts'), 'export function realOwnerExit() {}\n');
    expect(await waitFor(async () => names(serviceA).includes('realOwnerExit'), 10000)).toBe(true);
    expect(opened).toHaveLength(2); expectHandles(serviceA, 1, 1); expect(writer(serviceA).isWatching()).toBe(true);
    const takeover = ownerBytes(serviceA);
    expect(JSON.parse(takeover['writer.pid']!).pid).toBe(process.pid);
    details.takeover = { ...details.takeover as object, ownerClosed: owner.facts(), afterOwnerClose, afterExitEdit: 'realOwnerExit', queryFree: true,
      takeover, readerWatchingAfterTakeover: reader(serviceA).isWatching(), writerWatchingAfterTakeover: writer(serviceA).isWatching() };
  }, 60_000);

  it('retains a daemon while either accessing engine remains connected', async () => {
    const owner = await startOwner('daemon');
    const first = await search(serviceA, 'alphaOriginal');
    expect(first).toContain('alphaOriginal');
    const second = makeCaller();
    const secondResult = await second.getToolHandler().execute('codegraph_search', { projectPath: serviceA, query: 'alphaOriginal' });
    expect(secondResult.isError).toBeFalsy();
    expect(JSON.stringify(secondResult)).toContain('alphaOriginal');
    expect(opened).toHaveLength(1); expectHandles(serviceA, 1, 0);
    assertActualDaemonHello(owner, 2);
    const before = ownerBytes(serviceA);
    details.retention = { first, secondResult, before, sharedGraphs: opened.length };
    // Both engines now share one daemon session; from here on the daemon may
    // idle out, and only losing that session should make it do so.
    await armIdleExit(owner);
    await engine.stop();
    await new Promise((resolve) => setTimeout(resolve, 1000));
    expect(owner.nativeExit).toBeNull();
    expect(owner.nativeClose).toBeNull();
    expect(() => process.kill(owner.child.pid!, 0)).not.toThrow();
    expect(ownerBytes(serviceA)).toEqual(before);
    fs.writeFileSync(path.join(serviceA, 'src/sample.ts'), 'export function retainedDaemon() {}\n');
    expect(await waitFor(async () => names(serviceA).includes('retainedDaemon'), 10000)).toBe(true);
    expect(ownerBytes(serviceA)).toEqual(before);
    expectOpen(serviceA);
    details.retention = { ...details.retention as object, afterFirstStop: owner.facts(), afterEdit: names(serviceA), ownerRecordsAfterEdit: ownerBytes(serviceA) };
    await second.stop();
    await owner.waitClosed();
    owner.assertCleanClose();
    expect(owner.nativeExit).toEqual({ code: 0, signal: null });
    expect(owner.nativeClose).toEqual({ code: 0, signal: null });
    expect(ownerBytes(serviceA)).toEqual({ 'writer.pid': null, 'daemon.pid': null });
    expectClosed(serviceA);
    details.retention = { ...details.retention as object, naturalLastExit: owner.facts(), finalRecords: ownerBytes(serviceA), sharedGraphClosed: true };
  }, 60_000);

});
