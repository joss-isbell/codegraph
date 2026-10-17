import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { pathToFileURL } from 'url';
import { TextDecoder } from 'util';
import { createHash } from 'crypto';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { CodeGraph } from '../src';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { recordSpawns, removeSpawnLog, settleLosingCandidates, spawnLogFor } from './daemon-candidates';
import { PublicNativeEvidence } from './public-native-evidence';

const cli = path.resolve(__dirname, '../dist/bin/codegraph.js');
const keys = ['CODEGRAPH_QUERY_MODE', 'CODEGRAPH_QUERY_POOL_SIZE', 'CODEGRAPH_QUERY_BUSY_TIMEOUT_MS'];
function environment(settings: Record<string, string> = {}) {
  const env = { ...process.env };
  for (const key of keys) delete env[key];
  return { ...env, CODEGRAPH_NO_UPDATE_CHECK: '1', CODEGRAPH_TELEMETRY: '0', CODEGRAPH_NO_WATCH: '1',
    CODEGRAPH_NO_WATCHDOG: '1', CODEGRAPH_PPID_POLL_MS: '0', CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS: '0',
    CODEGRAPH_NO_RELAUNCH: '1', CODEGRAPH_WASM_RELAUNCHED: '1', ...settings };
}

function inventory(directory: string): unknown[] {
  if (!fs.existsSync(directory)) return [];
  return fs.readdirSync(directory, { recursive: true, withFileTypes: true }).map(entry => {
    const absolute = path.join(entry.parentPath, entry.name);
    const relative = path.relative(directory, absolute).replace(/\\/g, '/');
    if (!entry.isFile()) return { relative, kind: entry.isDirectory() ? 'directory' : 'other' };
    const bytes = fs.readFileSync(absolute);
    return { relative, kind: 'file', size: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') };
  }).sort((left, right) => left.relative.localeCompare(right.relative));
}

function owners(index: string) {
  return Object.fromEntries(['daemon.pid', 'writer.pid'].map(name => {
    const file = path.join(index, name);
    return [name, fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : null];
  }));
}

class ProcessClient {
  readonly child: ChildProcessWithoutNullStreams;
  readonly replies: any[] = [];
  private stdoutPrefix = Buffer.alloc(0);
  private stderrPrefix = Buffer.alloc(0);
  private stdoutSeen = 0;
  private stderrSeen = 0;
  private stdoutChunks = 0;
  private stdoutChunkEnds: number[] = [];
  private captureFailure: Error | null = null;
  private stdoutEof = false;
  private stderrEof = false;
  private closePromise: Promise<{ code: number | null; signal: NodeJS.Signals | null }> | null = null;
  private nativeClose: { code: number | null; signal: NodeJS.Signals | null } | null = null;
  readonly closed: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  private nextId = 0;
  private pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void; timer: NodeJS.Timeout }>();
  constructor(args: string[], root: string, env: NodeJS.ProcessEnv) {
    this.child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, ...args], { cwd: root, env, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
    let buffer = '';
    const stdoutDecoder = new TextDecoder('utf-8', { fatal: true });
    const stderrDecoder = new TextDecoder('utf-8', { fatal: true });
    const decode = (stream: 'stdout' | 'stderr', chunk?: Buffer, final = false) => {
      try {
        const text = (stream === 'stdout' ? stdoutDecoder : stderrDecoder).decode(chunk, { stream: !final });
        if (stream === 'stdout' && !this.captureFailure) buffer += text;
      } catch (error) { this.captureFailed(new Error(`invalid or incomplete owned ${stream} UTF-8: ${String(error).slice(0, 1024)}`)); }
    };
    this.child.stdout.on('data', data => {
      const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
      this.stdoutSeen += chunk.length;
      this.stdoutChunks++;
      if (this.stdoutChunkEnds.length < 64) this.stdoutChunkEnds.push(this.stdoutSeen);
      this.stdoutPrefix = Buffer.concat([this.stdoutPrefix, chunk.subarray(0, Math.max(0, 65536 - this.stdoutPrefix.length))]);
      if (this.stdoutSeen > 65536) this.captureFailed(new Error('owned stdout overflow'));
      if (this.captureFailure) { buffer = ''; return; }
      decode('stdout', chunk);
      if (this.captureFailure) { buffer = ''; return; }
      let end: number;
      while ((end = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
        let reply: any;
        try { reply = JSON.parse(line); }
        catch (error) { this.captureFailed(new Error(`malformed owned stdout: ${String(error).slice(0, 1024)}`)); buffer = ''; return; }
        if (!reply || typeof reply !== 'object' || Array.isArray(reply)) {
          this.captureFailed(new Error('malformed owned stdout JSON-RPC object')); buffer = ''; return;
        }
        this.replies.push(reply);
        const pending = this.pending.get(reply.id);
        if (pending) { this.pending.delete(reply.id); clearTimeout(pending.timer); reply.error ? pending.reject(new Error(JSON.stringify(reply.error))) : pending.resolve(reply.result); }
      }
    });
    this.child.stderr.on('data', data => {
      const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
      this.stderrSeen += chunk.length;
      this.stderrPrefix = Buffer.concat([this.stderrPrefix, chunk.subarray(0, Math.max(0, 65536 - this.stderrPrefix.length))]);
      if (this.stderrSeen > 65536) this.captureFailed(new Error('owned stderr overflow'));
      decode('stderr', chunk);
    });
    this.child.stdout.once('end', () => {
      this.stdoutEof = true; decode('stdout', undefined, true);
      if (buffer.length && !this.captureFailure) this.captureFailed(new Error('unterminated owned stdout at native EOF'));
      buffer = '';
    });
    this.child.stderr.once('end', () => { this.stderrEof = true; decode('stderr', undefined, true); });
    this.child.stdin.on('error', error => this.fail(error));
    this.child.on('error', error => this.fail(error));
    this.closed = new Promise(resolve => this.child.once('close', (code, signal) => {
      if (!this.stdoutEof || !this.stderrEof) this.captureFailed(new Error('missing owned pipe EOF before native close'));
      this.nativeClose = { code, signal };
      this.fail(new Error(`owned server closed: ${code}/${signal}: ${this.stderr}`)); resolve(this.nativeClose);
    }));
  }
  get stdout() { return this.stdoutPrefix.toString(); }
  get stderr() { return this.stderrPrefix.toString(); }
  capture() { return { pid: this.child.pid, nativeClose: this.nativeClose,
    stdout: { text: this.stdout, base64: this.stdoutPrefix.toString('base64'), sha256: createHash('sha256').update(this.stdoutPrefix).digest('hex'),
      seenBytes: this.stdoutSeen, retainedBytes: this.stdoutPrefix.length, truncated: this.stdoutSeen > this.stdoutPrefix.length, eof: this.stdoutEof },
    stderr: { text: this.stderr, base64: this.stderrPrefix.toString('base64'), sha256: createHash('sha256').update(this.stderrPrefix).digest('hex'),
      seenBytes: this.stderrSeen, retainedBytes: this.stderrPrefix.length, truncated: this.stderrSeen > this.stderrPrefix.length, eof: this.stderrEof },
    stdoutChunkEnds: this.stdoutChunkEnds, stdoutChunks: this.stdoutChunks,
    stdoutChunkMetadataTruncated: this.stdoutChunks > this.stdoutChunkEnds.length,
    failure: this.captureFailure?.message ?? null, replies: this.replies }; }
  private captureFailed(error: Error) { this.captureFailure ??= error; this.fail(this.captureFailure); }
  private fail(error: Error) { for (const value of this.pending.values()) { clearTimeout(value.timer); value.reject(error); } this.pending.clear(); }
  request(method: string, params: unknown = {}): Promise<any> {
    if (this.captureFailure) return Promise.reject(this.captureFailure);
    const id = ++this.nextId;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`independent 15000ms client deadline: ${this.stderr}`)); }, 15000);
      this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
    });
  }
  waitForClose(): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.captureFailed(new Error('independent 15000ms native close deadline'));
        reject(this.captureFailure);
      }, 15000);
      this.closed.then(result => {
        clearTimeout(timer);
        this.captureFailure ? reject(this.captureFailure) : resolve(result);
      });
    });
  }
  close(): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
    return this.closePromise ??= (async () => {
      if (!this.child.stdin.destroyed) this.child.stdin.end();
      return this.waitForClose();
    })();
  }
}

describe('strict policy through actual MCP startup', () => {
  it('rejects the complete invalid syntax table before direct, proxy or daemon startup touches authority', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-strict-invalid-')));
    const clients: ProcessClient[] = [];
    const facts: any[] = [];
    const index = path.join(root, '.codegraph');
    const registry = path.join(os.homedir(), '.codegraph', 'daemons');
    let before: unknown[] = [];
    let registryBefore: unknown[] = [];
    let primaryFailed = false, primaryFailure: unknown;
    const failures: Array<{ phase: string; failure: string }> = [];
    let finalIndex: unknown[] | undefined, finalOwners: unknown, finalRegistry: unknown[] | undefined;
    let cleanup: PromiseSettledResult<{ code: number | null; signal: NodeJS.Signals | null }>[] = [];
    const attempt = (phase: string, collect: () => void) => {
      try { collect(); } catch (error) { failures.push({ phase, failure: boundedFailure(error) }); }
    };
    const persist = () => attempt('persist', () => {
      if (process.env.VALIDATION_OUT) publishStartupReceipt(path.join(process.env.VALIDATION_OUT, 'strict-invalid-entry.json'),
        { before, after: finalIndex, owners: finalOwners, registryBefore, registryAfter: finalRegistry,
          primaryFailed, primaryFailure: primaryFailed ? boundedFailure(primaryFailure) : null, facts,
          captures: clients.map(client => client.capture()), failures, fixtureRemoved: !fs.existsSync(root),
          cleanup: cleanup.map(result => result.status === 'fulfilled' ? result : { status: result.status, reason: boundedFailure(result.reason) }) });
    });
    try {
      const graph = await CodeGraph.init(root); graph.close();
      before = inventory(index); registryBefore = inventory(registry);
      const invalid = [
        ...['', ' ', 'Default', 'Unlimited', 'DEFAULT', 'unlimited x', '0'].map(raw => ['CODEGRAPH_QUERY_MODE', raw, 'default or unlimited']),
        ...['', ' ', '-1', '+1', '1.0', '.5', '1e3', '0x10', 'NaN', 'Infinity', '-Infinity', 'abc', 'UNLIMITED', '1_000', '9007199254740992', '99999999999999999999999999999999999999']
          .map(raw => ['CODEGRAPH_QUERY_POOL_SIZE', raw, 'decimal whole numbers 0–9,007,199,254,740,991 or unlimited']),
        ...['', ' ', '0', '999', '-1000', '+1000', '1000.0', '1e3', '0x1000', 'NaN', 'Infinity', 'abc', 'UNLIMITED', '2147483648', '9007199254740991']
          .map(raw => ['CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', raw, 'decimal whole numbers 1,000–2,147,483,647 or unlimited']),
      ];
      for (const route of ['direct', 'automatic', 'daemon-internal']) for (const [key, raw, alternatives] of invalid) {
        const client = new ProcessClient([cli, 'serve', '--mcp', '--path', root], root, environment({
          CODEGRAPH_NO_DAEMON: route === 'direct' ? '1' : '0',
          CODEGRAPH_DAEMON_INTERNAL: route === 'daemon-internal' ? '1' : '',
          ...(key === 'CODEGRAPH_QUERY_BUSY_TIMEOUT_MS' ? { CODEGRAPH_QUERY_POOL_SIZE: '0' } : {}), [key]: raw,
        })); clients.push(client);
        const exit = await client.waitForClose();
        const after = inventory(index);
        const registryAfter = inventory(registry);
        facts.push({ route, key, raw, exit, expectedError: `Invalid ${key} value ${JSON.stringify(raw)}; expected ${alternatives}.`,
          before, after, owners: owners(index), registryBefore, registryAfter, capture: client.capture() });
        expect(exit, client.stderr).toEqual({ code: 1, signal: null });
        expect(client.stderr).toContain(`Invalid ${key} value ${JSON.stringify(raw)}; expected ${alternatives}.`);
        expect(client.stdout).toBe('');
        expect(after).toEqual(before);
        expect(registryAfter).toEqual(registryBefore);
      }
    } catch (error) { primaryFailed = true; primaryFailure = error; }
    finally {
      cleanup = await Promise.allSettled(clients.map(client => client.close()));
      for (const result of cleanup) if (result.status === 'rejected') failures.push({ phase: 'host-close', failure: boundedFailure(result.reason) });
      attempt('index-inventory', () => { finalIndex = inventory(index); });
      attempt('ownership', () => { finalOwners = owners(index); });
      attempt('registry-inventory', () => { finalRegistry = inventory(registry); });
      persist();
      if (!primaryFailed && failures.length === 0 && facts.length === 114
        && clients.every(client => client.capture().failure === null && client.capture().nativeClose?.code === 1 && client.capture().nativeClose?.signal === null)) {
        attempt('fixture-remove', () => fs.rmSync(root, { recursive: true, force: true }));
      }
      persist();
    }
    if (primaryFailed) throw primaryFailure;
    expect(facts).toHaveLength(114); expect(failures).toEqual([]);
  }, 120000);

  it.each(['direct', 'writable-fallback'] as const)('keeps the constructor request through a later environment change and default/explicit/default real queries in %s', async mode => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-strict-captured-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    let client: ProcessClient | undefined;
    let diagnostics: unknown;
    const observations = path.join(fixture, 'workers.jsonl');
    const events = (): any[] => fs.existsSync(observations) ? fs.readFileSync(observations, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)) : [];
    let before: unknown[] = [];
    const registry = path.join(os.homedir(), '.codegraph', 'daemons');
    let registryBefore: unknown[] = [];
    let primaryFailed = false, primaryFailure: unknown;
    const setupFailures: any[] = [], finalFailures: any[] = [];
    try {
      for (const [i, root] of roots.entries()) {
        fs.mkdirSync(root);
        fs.writeFileSync(path.join(root, 'app.ts'), `export function ${i ? 'beta' : 'alpha'}CapturedSymbol() { return ${i}; }\n`);
        await indexFixture(root, setupFailures);
      }
      before = roots.map(root => ({ root, index: inventory(path.join(root, '.codegraph')), owners: owners(path.join(root, '.codegraph')) }));
      registryBefore = inventory(registry);
      client = new ProcessClient(['--require', path.join(__dirname, 'fixtures/query-unlimited-preload.cjs'), path.join(__dirname, 'fixtures/query-captured-startup.cjs'), roots[0]], roots[0], environment({
        CODEGRAPH_NO_DAEMON: mode === 'direct' ? '1' : '0', CODEGRAPH_QUERY_MODE: 'default', CODEGRAPH_QUERY_POOL_SIZE: '3', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
        CODEGRAPH_DAEMON_RETRY_MS: '0', QUERY_UNLIMITED_FAIL_DAEMON: mode === 'writable-fallback' ? '1' : '0',
        QUERY_UNLIMITED_OBSERVATIONS: observations, QUERY_UNLIMITED_RELEASE: path.join(fixture, 'unused-release'),
      }));
      await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'captured-startup', version: '1' } });
      const status = await client.request('tools/call', { name: 'codegraph_status', arguments: {} });
      const parsed = parseQueryDiagnostics(status); diagnostics = parsed;
      expect(parsed.requested).toEqual({ mode: 'default', poolSize: '3', busyTimeoutMs: 'unlimited' });
      expect(parsed.effective).toMatchObject({ source: 'strict', executionMode: mode, concurrency: { kind: 'finite', maximum: 3 }, deadline: { kind: 'absent' }, retention: 'ordinary' });
      expect(parsed.implementation.support.combinedPreset).toBe(true);
      for (const [i, projectPath] of [[0, undefined], [1, roots[1]], [0, undefined]] as const) {
        const symbol = `${i ? 'beta' : 'alpha'}CapturedSymbol`;
        const result = await client.request('tools/call', { name: 'codegraph_search', arguments: { query: symbol, ...(projectPath ? { projectPath } : {}) } });
        expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain(symbol);
        const explored = await client.request('tools/call', { name: 'codegraph_explore', arguments: { query: symbol, ...(projectPath ? { projectPath } : {}) } });
        expect(explored.isError).toBeFalsy(); expect(explored.content[0].text).toContain(symbol);
      }
      const history = events().filter(event => event.event === 'session-history');
      expect(history.map(event => [event.projectRoot, event.query, event.index])).toEqual([
        [roots[0], 'alphaCapturedSymbol', 1], [roots[1], 'betaCapturedSymbol', 1], [roots[0], 'alphaCapturedSymbol', 2],
      ]);
      expect(new Set(history.map(event => event.historyId)).size).toBe(1);
      if (mode === 'writable-fallback') {
        expect(events().filter(event => event.event === 'unavailable-candidate-exit').map(event => event.code)).toEqual([78]);
        expect(client.stderr).toContain('Shared daemon unavailable');
        expect(client.stderr).not.toMatch(/version.*differs|Attached to shared daemon/);
      }
      const exit = await client.close(); expect(exit).toEqual({ code: 0, signal: null });
    } catch (error) { primaryFailed = true; primaryFailure = error; }
    finally {
      const cleanup = await Promise.allSettled(client ? [client.close()] : []);
      try { if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, `strict-captured-startup-${mode}.json`), JSON.stringify({ mode, roots, before,
        after: roots.map(root => ({ root, index: inventory(path.join(root, '.codegraph')), owners: owners(path.join(root, '.codegraph')) })),
        registryBefore, registryAfter: inventory(registry), diagnostics, events: events(), capture: client?.capture(),
        primaryFailed, primaryFailure: primaryFailed ? boundedFailure(primaryFailure) : null, setupFailures,
        cleanup: cleanup.map(result => result.status === 'fulfilled' ? result : { status: result.status, reason: boundedFailure(result.reason) }) }, null, 2)); }
      catch (error) { finalFailures.push({ phase: 'persist', failure: boundedFailure(error) }); }
      for (const result of cleanup) if (result.status === 'rejected') finalFailures.push({ phase: 'host-close', failure: boundedFailure(result.reason) });
      if (!primaryFailed && setupFailures.length === 0 && finalFailures.length === 0) {
        try { fs.rmSync(fixture, { recursive: true, force: true }); }
        catch (error) { finalFailures.push({ phase: 'fixture-remove', failure: boundedFailure(error) }); }
      }
    }
    if (primaryFailed) throw primaryFailure;
    expect(setupFailures).toEqual([]); expect(finalFailures).toEqual([]);
  }, 120000);
});

const wait = (milliseconds: number) => new Promise(resolve => setTimeout(resolve, milliseconds));
async function untilNative(predicate: () => boolean, milliseconds = 15000): Promise<void> {
  const deadline = Date.now() + milliseconds;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('independent native startup observation deadline');
    await wait(25);
  }
}
function nativePresence(pid: number): 'alive' | 'absent' | 'unknown' {
  try { process.kill(pid, 0); return 'alive'; }
  catch (error) { return (error as NodeJS.ErrnoException).code === 'ESRCH' ? 'absent' : 'unknown'; }
}
function boundedSpawnLog(file: string) {
  const capture = boundedNativeLog(file, false, 'spawn log');
  return !capture.exists && !capture.failure ? null : capture;
}

function boundedFailure(error: unknown): string {
  try { return String(error).slice(0, 4096); } catch { return 'unprintable thrown value'; }
}

let receiptSequence = 0;

/** Never open the last complete receipt for truncation, including after removal. */
function publishStartupReceipt(file: string, value: object): void {
  const temporary = `${file}.pending-${process.pid}-${++receiptSequence}`;
  const reportFailure = (phase: string, error: unknown): void => {
    try {
      const removed = Reflect.get(value, 'fixtureRemoved');
      process.stderr.write(`Startup receipt ${phase} (${file}; fixtureRemoved=${typeof removed === 'boolean' ? removed : 'unknown'}): ${boundedFailure(error)}\n`);
    } catch { /* never replace the original failure */ }
  };
  let descriptor: number | undefined, failed = false, primary: unknown;
  try {
    descriptor = fs.openSync(temporary, 'wx', 0o600);
    fs.writeFileSync(descriptor, JSON.stringify(value, null, 2));
    fs.fsyncSync(descriptor);
  } catch (error) { failed = true; primary = error; }
  finally {
    if (descriptor !== undefined) try { fs.closeSync(descriptor); }
    catch (error) {
      if (!failed) { failed = true; primary = error; }
      else reportFailure('secondary close failure', error);
    }
  }
  // Failed or partial temporaries remain separate and recoverable. A native
  // same-directory replacement is attempted only after the complete fd closes.
  if (failed) { reportFailure('temporary publication failure', primary); throw primary; }
  try { fs.renameSync(temporary, file); }
  catch (error) { reportFailure('replacement failure', error); throw error; }
}

/** Keep the unowned collector's scratch writes away from the published capture. */
class StartupNativeEvidence extends PublicNativeEvidence {
  private sequence = 0;
  private receiptFailure: string | null = null;
  private readonly publicationFailures: string[] = [];
  private omittedPublicationFailures = 0;
  constructor(roots: string[], observations: string, private readonly publishedName: string) {
    super(roots, observations, `${publishedName}.staging`);
  }
  override get failed(): boolean { return super.failed || this.receiptFailure !== null; }
  override failures() {
    const failures = super.failures();
    return { ...failures, persistence: failures.persistence ?? this.receiptFailure };
  }
  override persist(stage: string, facts: () => object): void {
    const sequence = ++this.sequence;
    let capturedFacts: object | undefined;
    super.persist(stage, () => {
      capturedFacts = facts();
      return { ...capturedFacts, receiptSequence: sequence };
    });
    if (!process.env.VALIDATION_OUT) return;
    try {
      const file = path.join(process.env.VALIDATION_OUT, this.publishedName);
      const capture = JSON.parse(fs.readFileSync(`${file}.staging`, 'utf8'));
      if (capture.receiptSequence !== sequence || capture.stage !== stage) throw new Error('Startup staging receipt is incomplete or stale.');
      publishStartupReceipt(file, { ...capture, evidenceFailures: this.failures(), receiptPublicationFailure: this.receiptFailure,
        publicationFailures: this.publicationFailures, omittedPublicationFailures: this.omittedPublicationFailures });
    } catch (error) {
      const failure = `startup receipt (${stage.slice(0, 128)}): ${boundedFailure(error)}`;
      this.receiptFailure ??= failure;
      if (this.publicationFailures.length < 16) this.publicationFailures.push(failure);
      else this.omittedPublicationFailures++;
      try {
        const removed = capturedFacts && Reflect.get(capturedFacts, 'fixtureRemoved');
        process.stderr.write(`Startup evidence publication failure (fixtureRemoved=${typeof removed === 'boolean' ? removed : 'unknown'}): ${failure}\n`);
      } catch { /* keep cleanup running */ }
    }
  }
}

async function indexFixture(root: string, secondary: any[]): Promise<void> {
  const graph = await CodeGraph.init(root);
  let primary: unknown, failed = false;
  try { await graph.indexAll(); } catch (error) { failed = true; primary = error; }
  try { graph.close(); } catch (error) {
    secondary.push({ phase: 'fixture-graph-close', root, failure: boundedFailure(error) });
    if (!failed) { failed = true; primary = error; }
  }
  if (failed) throw primary;
}

/** One closed native merged fd, retained losslessly without stdout attribution. */
function boundedNativeLog(file: string, required: boolean, label = 'merged daemon.log') {
  let descriptor: number | undefined, prefix = Buffer.alloc(0), seenBytes = 0;
  let failure: string | null = null, closeFailure: string | null = null, exists = false, stable = false;
  try {
    exists = fs.existsSync(file);
    if (!exists) { if (required) failure = `required ${label} missing`; }
    else {
      descriptor = fs.openSync(file, 'r'); const before = fs.fstatSync(descriptor); seenBytes = before.size;
      const bytes = Buffer.alloc(Math.min(seenBytes, 65536));
      const retained = fs.readSync(descriptor, bytes, 0, bytes.length, 0); prefix = bytes.subarray(0, retained);
      const after = fs.fstatSync(descriptor); stable = before.size === after.size && before.mtimeMs === after.mtimeMs;
      if (seenBytes > 65536 || retained !== seenBytes || !stable) failure = `${label} overflow, short read or unstable tail`;
    }
  } catch (error) { failure ??= boundedFailure(error); }
  finally { if (descriptor !== undefined) try { fs.closeSync(descriptor); } catch (error) { closeFailure = boundedFailure(error); failure ??= closeFailure; } }
  let text = '';
  try { text = new TextDecoder('utf-8', { fatal: true }).decode(prefix); } catch (error) { failure ??= `${label} UTF-8: ${boundedFailure(error)}`; }
  return { file, required, exists, text, base64: prefix.toString('base64'), sha256: createHash('sha256').update(prefix).digest('hex'),
    seenBytes, retainedBytes: prefix.length, truncated: seenBytes > prefix.length, stable, failure, closeFailure };
}

describe('one captured startup through actual CLI and native candidate', () => {
  it.each(['direct', 'writable-fallback', 'shared-daemon', 'disconnected-native'] as const)
  ('preserves CPU3 and original absence after later mutation in %s', async mode => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-native-startup-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    const observations = path.join(fixture, 'observations.jsonl');
    const evidence = new StartupNativeEvidence(roots, observations, `native-startup-${mode}.json`);
    let client: ProcessClient | undefined, primaryFailure: unknown, primaryFailed = false;
    const diagnostics: any[] = [], routes: any[] = [], cleanup: any[] = [], setupFailures: any[] = [];
    let spawnLog: ReturnType<typeof boundedSpawnLog> = null;
    let mergedLog: ReturnType<typeof boundedNativeLog> | null = null;
    let candidateAbsences: Array<{ pid: number; presence: string; ownExit: unknown }> = [];
    const events = () => evidence.events();
    const persist = (stage: string) => evidence.persist(stage, () => ({ mode, roots,
      primaryFailed, primaryFailure: primaryFailed ? boundedFailure(primaryFailure) : null, diagnostics, routes, setupFailures,
      capture: client?.capture(), cleanup, spawnLog, mergedLog, candidateAbsences, fixtureRemoved: !fs.existsSync(fixture) }));
    try {
      for (const [i, root] of roots.entries()) {
        fs.mkdirSync(root);
        fs.writeFileSync(path.join(root, 'app.ts'), `export function ${i ? 'beta' : 'alpha'}SnapshotSymbol() { return ${i}; }\n`);
        await indexFixture(root, setupFailures);
      }
      const recorder = recordSpawns(roots[0]);
      client = new ProcessClient([...recorder.args, '--require', path.join(__dirname, 'fixtures/query-startup-transport-observer.cjs'),
        cli, 'serve', '--mcp', '--path', roots[0]], roots[0], environment({ ...recorder.env,
        CODEGRAPH_NO_DAEMON: mode === 'direct' ? '1' : '0', CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '1000',
        CODEGRAPH_DAEMON_INSTALL_CHECK_MS: '0', CODEGRAPH_DAEMON_RETRY_MS: '0',
        CODEGRAPH_INTERNAL_QUERY_STARTUP_CPU_V1: 'stale-unqualified-metadata',
        QUERY_STARTUP_DESCENDANT: '1', QUERY_UNLIMITED_OBSERVATIONS: observations,
        QUERY_UNLIMITED_FAIL_DAEMON: mode === 'writable-fallback' ? '1' : '0',
        QUERY_STARTUP_DISCONNECT: mode === 'disconnected-native' ? 'before-capture' : '0',
      }));
      expect((await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {},
        clientInfo: { name: 'native-startup-snapshot', version: '1' } })).serverInfo.name).toBe('codegraph');
      expect((await client.request('tools/list')).tools.length).toBeGreaterThan(0);
      const state = parseQueryDiagnostics(await client.request('tools/call', { name: 'codegraph_status', arguments: {} }));
      diagnostics.push(state);
      expect(state.requested).toEqual({});
      expect(state.effective).toMatchObject({ executionMode: mode === 'disconnected-native' ? 'writable-fallback' : mode,
        source: mode === 'shared-daemon' ? 'legacy' : 'strict',
        preset: 'default', concurrency: { kind: 'finite', maximum: 2 }, deadline: { kind: 'finite', milliseconds: 45000 } });
      expect(state.implementation.support.combinedPreset).toBe(mode !== 'shared-daemon');
      for (const i of [0, 1, 0]) {
        const query = `${i ? 'beta' : 'alpha'}SnapshotSymbol`;
        for (const name of ['codegraph_search', 'codegraph_explore']) {
          const result = await client.request('tools/call', { name, arguments: { query, ...(i ? { projectPath: roots[i] } : {}) } });
          routes.push({ name, root: roots[i], result });
          expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain(query);
          expect(result.content[0].text).toContain('app.ts');
        }
      }
      // Observe delivered history only after the same session has processed a
      // subsequent request; a tool's wire response alone is not that barrier.
      expect(await client.request('ping')).toEqual({});
      const captured = events().filter(event => event.event === 'startup-capture');
      expect(captured.filter(event => !event.qualified).map(event => [event.startup, event.cpuObservations]))
        .toEqual([[{ requested: {}, cpuCount: 3 }, 1]]);
      const actualEngine = events().filter(event => event.event === 'startup-engine');
      expect(actualEngine).toHaveLength(1);
      expect(events().filter(event => event.event === 'startup-engine-cpu-recapture')).toEqual([]);
      expect(actualEngine[0].sameProspective).toBe(true);
      if (mode === 'direct') {
        // Explicit direct selection is inert until the real session chooses
        // its strongest client hint; it has not proved cwd/--path rootless.
        expect(actualEngine[0].options.projectRoute).toBeUndefined();
        expect(actualEngine[0].hasResourceObserver).toBe(true);
        expect(actualEngine[0].hasDefaultOutcomeCallback).toBe(true);
      } else expect(actualEngine[0].options.projectRoute).toEqual({ root: roots[0], index: fs.realpathSync(path.join(roots[0], '.codegraph/codegraph.db')) });
      expect(actualEngine[0].options.queryAdmissionPolicy).toMatchObject({ requested: {}, executionMode: 'shared-daemon',
        source: 'strict', concurrency: { kind: 'finite', maximum: 2 } });
      if (mode === 'direct') expect(actualEngine[0].sameDirect).toBe(true);
      const admitted = events().filter(event => event.event === 'startup-admitted');
      // Engines defensively copy the captured prospective policy. Its value
      // and immutability, not private reference identity, are the contract.
      expect(admitted.length).toBeGreaterThan(0);
      for (const event of admitted) {
        expect(event.policy).toEqual(actualEngine[0].options.queryAdmissionPolicy);
        expect(event.frozen).toBe(true);
      }
      const sqliteOpens = events().filter(event => event.event === 'startup-sqlite-open');
      expect(sqliteOpens.length).toBeGreaterThan(0);
      expect(sqliteOpens.every(open => admitted.some(permission => permission.pid === open.pid
        && BigInt(permission.eventTimeNs) < BigInt(open.eventTimeNs)))).toBe(true);
      const history = events().filter(event => event.event === 'session-history');
      expect(history.map(event => [event.projectRoot, event.query, event.index])).toEqual([
        [roots[0], 'alphaSnapshotSymbol', 1], [roots[1], 'betaSnapshotSymbol', 1], [roots[0], 'alphaSnapshotSymbol', 2],
      ]);
      expect(new Set(history.map(event => event.historyId)).size).toBe(1);
      const workers = events().filter(event => event.event === 'startup-worker-env');
      expect(workers.length).toBeGreaterThan(0);
      expect(workers.every(event => event.execution.policy.requested && Object.keys(event.execution.policy.requested).length === 0)).toBe(true);
      if (mode === 'shared-daemon') {
        expect(captured.filter(event => event.qualified).map(event => [event.startup, event.cpuObservations, event.rawAfter]))
          .toEqual([[{ requested: {}, cpuCount: 3 }, 0, {}]]);
        const produced = events().filter(event => event.event === 'startup-candidate-spawn');
        expect(produced).toHaveLength(1);
        expect(produced[0].raw).toEqual({ CODEGRAPH_INTERNAL_QUERY_STARTUP_CPU_V1: '{"protocol":1,"cpuCount":3}' });
        expect(produced[0].selector).toBe('.codegraph');
        const acknowledgment = events().find(event => event.event === 'startup-acknowledged');
        expect(acknowledgment).toBeDefined();
        expect(acknowledgment.pid).toBe(produced[0].candidatePid);
        // Supported discovery may inspect an admitted schema before selecting
        // one route. It must not promote a service for every scanned candidate.
        const parentPid = captured.find(event => !event.qualified)!.pid;
        expect(sqliteOpens.some(open => open.pid === parentPid
          && BigInt(open.eventTimeNs) < BigInt(produced[0].eventTimeNs))).toBe(true);
        const childOpens = sqliteOpens.filter(open => open.pid === produced[0].candidatePid);
        expect(childOpens.length).toBeGreaterThan(0);
        expect(childOpens.every(open => BigInt(open.eventTimeNs) > BigInt(acknowledgment.eventTimeNs))).toBe(true);
        expect(BigInt(actualEngine[0].eventTimeNs)).toBeGreaterThan(BigInt(acknowledgment.eventTimeNs));
        expect(events().filter(event => event.event === 'startup-bootstrap-status').map(event => event.status))
          .toEqual([{ protocol: 1, kind: 'permitted' }]);
        expect(events().filter(event => event.event === 'startup-bootstrap-ack-sent').map(event => event.status))
          .toEqual([{ protocol: 1, kind: 'acknowledged' }]);
        expect(workers.every(event => Object.keys(event.raw).length === 0)).toBe(true);
        await untilNative(() => events().some(event => event.event === 'startup-descendant-close'));
        const descendant = events().filter(event => event.event === 'startup-descendant-close');
        expect(descendant).toHaveLength(1);
        expect(descendant[0]).toMatchObject({ code: 0, signal: null, failure: null, stderr: '' });
        expect(JSON.parse(descendant[0].stdout)).toEqual({});
      } else if (mode === 'writable-fallback') {
        expect(events().filter(event => event.event === 'unavailable-candidate-exit').map(event => event.code)).toEqual([78]);
        expect(events().filter(event => event.event === 'startup-bootstrap-result').map(event => event.result))
          .toEqual([{ kind: 'unavailable', cause: 'pre-service-exit', exitCode: 78, signal: null }]);
      } else if (mode === 'disconnected-native') {
        const candidates = events().filter(event => event.event === 'startup-candidate-spawn');
        expect(candidates).toHaveLength(1);
        const childPid = candidates[0].candidatePid;
        expect(events().filter(event => event.event === 'startup-native-disconnect').map(event => event.pid)).toEqual([childPid]);
        expect(events().filter(event => event.event === 'startup-capture-refused')).toEqual([
          expect.objectContaining({ pid: childPid, qualified: true, nativeIpc: true, liveIpc: false,
            cpuObservations: 0, rawAfter: {}, refusal: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: roots[0] } }),
        ]);
        expect(events().filter(event => event.event === 'startup-bootstrap-result').map(event => event.result))
          .toEqual([{ kind: 'unavailable', cause: 'pre-service-exit', exitCode: 1, signal: null }]);
        expect(events().filter(event => event.event === 'startup-bootstrap-status' || event.event === 'startup-bootstrap-ack-sent')).toEqual([]);
        expect(events().filter(event => event.pid === childPid
          && ['startup-sqlite-open', 'startup-engine', 'spawn', 'call-start', 'result', 'session-history'].includes(event.event))).toEqual([]);
        const close = events().filter(event => event.event === 'startup-candidate-close');
        expect(close).toEqual([expect.objectContaining({ candidatePid: childPid, code: 1, signal: null })]);
        expect(BigInt(actualEngine[0].eventTimeNs)).toBeGreaterThan(BigInt(close[0].eventTimeNs));
      } else expect(events().filter(event => event.event === 'startup-candidate-spawn')).toEqual([]);
      persist('ready-and-queried');
    } catch (error) { primaryFailed = true; primaryFailure = error; }
    finally {
      persist('before-cleanup');
      try {
        if (client) {
          const result = await client.close();
          expect(result).toEqual({ code: 0, signal: null });
          cleanup.push({ phase: 'host-close', result });
        }
      }
      catch (error) { cleanup.push({ phase: 'host-close', failure: boundedFailure(error) }); }
      try {
        await settleLosingCandidates(roots[0], () => null, 15000);
        await untilNative(() => {
          const all = events();
          return all.filter(event => event.event === 'startup-candidate-spawn').every(event => nativePresence(event.candidatePid) === 'absent')
            && all.filter(event => event.event === 'startup-descendant-spawn').length === all.filter(event => event.event === 'startup-descendant-close').length
            && all.filter(event => event.event === 'spawn').length === all.filter(event => event.event === 'exit').length;
        });
        candidateAbsences = events().filter(event => event.event === 'startup-candidate-spawn').map(event => ({
          pid: event.candidatePid, presence: nativePresence(event.candidatePid),
          ownExit: events().find(exit => exit.event === 'startup-candidate-own-exit' && exit.pid === event.candidatePid) ?? null,
        }));
        const expectedExit = mode === 'shared-daemon' ? 0 : mode === 'disconnected-native' ? 1 : 78;
        for (const candidate of candidateAbsences) {
          expect(candidate.presence).toBe('absent');
          if (mode === 'shared-daemon' || mode === 'disconnected-native') expect(candidate.ownExit).toMatchObject({ code: expectedExit });
          else expect(candidate.ownExit).toBeNull(); // inert -e exit78 has no preload/self-report
        }
        if (mode !== 'shared-daemon') expect(events().filter(event => event.event === 'startup-candidate-close')
          .map(event => [event.code, event.signal])).toEqual(mode === 'direct' ? [] : [[expectedExit, null]]);
        expect(evidence.owners().every(owner => owner.writer === null && owner.daemon === null)).toBe(true);
        cleanup.push({ phase: 'native-descendants-and-workers', outcome: 'closed' });
      } catch (error) { cleanup.push({ phase: 'native-descendants-and-workers', failure: boundedFailure(error) }); }
      try { spawnLog = boundedSpawnLog(spawnLogFor(roots[0])); if (spawnLog?.failure) throw new Error(spawnLog.failure); }
      catch (error) { cleanup.push({ phase: 'spawn-log', failure: boundedFailure(error) }); }
      try { mergedLog = boundedNativeLog(path.join(roots[0], '.codegraph/daemon.log'), mode !== 'direct'); if (mergedLog.failure) throw new Error(mergedLog.failure); }
      catch (error) { cleanup.push({ phase: 'merged-daemon-log', failure: boundedFailure(error) }); }
      persist('native-cleanup');
      if (!primaryFailed && !evidence.failed && setupFailures.length === 0 && cleanup.every(item => !item.failure)) {
        evidence.allowObservationRemoval();
        try { removeSpawnLog(roots[0]); fs.rmSync(fixture, { recursive: true, force: true }); }
        catch (error) { cleanup.push({ phase: 'fixture-remove', failure: boundedFailure(error) }); }
      }
      persist('final');
    }
    if (primaryFailed) throw primaryFailure;
    expect(setupFailures).toEqual([]);
    expect(cleanup.filter(item => item.failure)).toEqual([]); expect(evidence.failures()).toEqual({ observation: null, ownership: null, persistence: null, timeline: null });
    expect(client?.capture().nativeClose).toEqual({ code: 0, signal: null });
  }, 120000);

  it.each(['missing', 'extra-key', 'control'] as const)
  ('keeps qualified %s transport failure terminal before SQLite or fallback', async corruption => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-native-startup-refusal-')));
    const observations = path.join(root, 'observations.jsonl');
    const evidence = new StartupNativeEvidence([root], observations, `native-startup-refusal-${corruption}.json`);
    const index = path.join(root, '.codegraph'), registry = path.join(os.homedir(), '.codegraph', 'daemons');
    let client: ProcessClient | undefined, primaryFailure: unknown, primaryFailed = false;
    let before: unknown[] = [], after: unknown[] = [], registryBefore: unknown[] = [], registryAfter: unknown[] = [];
    const refusals: any[] = [], cleanup: any[] = [];
    let spawnLog: ReturnType<typeof boundedSpawnLog> = null;
    let mergedLog: ReturnType<typeof boundedNativeLog> | null = null;
    const candidateAbsences: Array<{ stage: string; pid: number; presence: ReturnType<typeof nativePresence> }> = [];
    let candidateLogIdentity: { dev: string; ino: string; birthtimeNs: string } | null = null;
    const events = () => evidence.events();
    const persist = (stage: string) => evidence.persist(stage, () => ({ corruption, root, before, after,
      registryBefore, registryAfter, refusals, primaryFailed, primaryFailure: primaryFailed ? boundedFailure(primaryFailure) : null,
      capture: client?.capture(), cleanup, spawnLog, mergedLog, candidateAbsences, candidateLogIdentity, fixtureRemoved: !fs.existsSync(root) }));
    try {
      const graph = await CodeGraph.init(root); graph.close();
      before = inventory(index); registryBefore = inventory(registry);
      const recorder = recordSpawns(root);
      client = new ProcessClient([...recorder.args, '--require', path.join(__dirname, 'fixtures/query-startup-transport-observer.cjs'),
        cli, 'serve', '--mcp', '--path', root], root, environment({ ...recorder.env,
        CODEGRAPH_NO_DAEMON: '0', CODEGRAPH_DAEMON_RETRY_MS: '0',
        // The unsupported-shared discovery branch requires a real inert
        // preflight before parent schema work; corruption must remain terminal.
        CODEGRAPH_QUERY_MODE: 'unlimited',
        QUERY_STARTUP_CORRUPT: corruption, QUERY_UNLIMITED_OBSERVATIONS: observations,
      }));
      await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'native-qualified-refusal', version: '1' } });
      expect((await client.request('tools/list')).tools.length).toBeGreaterThan(0);
      const outcomes = await Promise.allSettled(['codegraph_status', 'codegraph_search'].map(name => client!.request('tools/call',
        { name, arguments: name === 'codegraph_search' ? { query: 'neverExecuted' } : {} })));
      for (const outcome of outcomes) {
        expect(outcome.status).toBe('rejected');
        if (outcome.status !== 'rejected') throw new Error('Qualified refusal unexpectedly returned a result');
        const response = JSON.parse((outcome.reason as Error).message); refusals.push(response);
        expect(response).toMatchObject({ code: -32603, data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root } });
      }
      const captured = events().filter(event => event.event === 'startup-capture-refused');
      expect(captured).toHaveLength(1);
      expect(captured[0]).toMatchObject({ qualified: true, cpuObservations: 0, rawAfter: {}, refusal: { reason: 'unknown' } });
      expect(events().filter(event => event.event === 'startup-bootstrap-result').map(event => event.result.kind)).toEqual(['refused']);
      expect(events().filter(event => event.event === 'startup-bootstrap-result').map(event => event.options))
        .toEqual([{ acknowledgePermission: false }]);
      expect(events().filter(event => event.event === 'startup-bootstrap-status').map(event => event.status))
        .toEqual([expect.objectContaining({ protocol: 1, kind: 'refused', refusal: expect.objectContaining({ reason: 'unknown', projectRoot: root }) })]);
      expect(events().filter(event => event.event === 'startup-bootstrap-ack-sent')).toEqual([]);
      expect(events().filter(event => ['startup-sqlite-open', 'startup-engine', 'spawn', 'call-start', 'result', 'session-history'].includes(event.event))).toEqual([]);
      after = inventory(index); registryAfter = inventory(registry);
      // daemon.log is the declared bootstrap diagnostic output. Retain the
      // full inventory; every original file and every SQLite sidecar is exact.
      expect(after.filter((entry: any) => entry.relative !== 'daemon.log')).toEqual(before);
      expect(after.filter((entry: any) => entry.relative === 'daemon.log')).toHaveLength(1);
      expect(owners(index)).toEqual({ 'daemon.pid': null, 'writer.pid': null });
      expect(registryAfter).toEqual(registryBefore);
      persist('refused');
    } catch (error) { primaryFailed = true; primaryFailure = error; }
    finally {
      persist('before-cleanup');
      // Keep the actual observer host alive through its child's native exit,
      // IPC disconnection and real inherited-file-descriptor close. Node can
      // omit ChildProcess.close after explicit disconnect; never synthesize it.
      try {
        const produced = events().filter(event => event.event === 'startup-candidate-spawn');
        expect(produced).toHaveLength(1);
        const candidate = produced[0];
        expect(candidate.stdio).toEqual(['ignore', expect.any(Number), candidate.stdio[1], 'ipc']);
        expect(candidate.parentStdioNull).toEqual([true, true, true, true]);
        expect(candidate.inheritedDescriptors).toEqual([candidate.stdio[1]]);
        const logStat = fs.statSync(path.join(index, 'daemon.log'), { bigint: true });
        candidateLogIdentity = { dev: String(logStat.dev), ino: String(logStat.ino), birthtimeNs: String(logStat.birthtimeNs) };
        expect(candidate.descriptorIdentities).toEqual([{ descriptor: candidate.stdio[1], identity: candidateLogIdentity }]);
        await untilNative(() => {
          const all = events();
          const failed = all.find(event => event.event.startsWith('startup-candidate-parent-fd-') && event.failure);
          if (failed) throw new Error(failed.failure);
          return all.some(event => event.event === 'startup-candidate-exit' && event.candidatePid === candidate.candidatePid)
            && all.some(event => event.event === 'startup-candidate-disconnect' && event.candidatePid === candidate.candidatePid)
            && all.some(event => event.event === 'startup-candidate-parent-fd-close' && event.candidatePid === candidate.candidatePid)
            && nativePresence(candidate.candidatePid) === 'absent';
        });
        const all = events();
        expect(all.filter(event => event.event === 'startup-candidate-own-exit').map(event => [event.pid, event.code]))
          .toEqual([[candidate.candidatePid, 1]]);
        expect(all.filter(event => event.event === 'startup-candidate-exit').map(event => [event.candidatePid, event.code, event.signal]))
          .toEqual([[candidate.candidatePid, 1, null]]);
        expect(all.filter(event => event.event === 'startup-candidate-disconnect').map(event => [event.candidatePid, event.connected]))
          .toEqual([[candidate.candidatePid, false]]);
        const descriptors = all.filter(event => event.event === 'startup-candidate-parent-fd-close');
        expect(descriptors).toHaveLength(1);
        expect(descriptors[0]).toMatchObject({ candidatePid: candidate.candidatePid, descriptor: candidate.stdio[1],
          creationIdentity: candidate.descriptorIdentities[0].identity, observedIdentity: candidate.descriptorIdentities[0].identity,
          closeCount: 1, afterClose: 'EBADF', failure: null });
        const genuineClose = all.filter(event => event.event === 'startup-candidate-close');
        expect(genuineClose.length).toBeLessThanOrEqual(1);
        for (const event of genuineClose) expect(event).toMatchObject({ candidatePid: candidate.candidatePid, code: 1, signal: null });
        candidateAbsences.push({ stage: 'before-host-close', pid: candidate.candidatePid, presence: nativePresence(candidate.candidatePid) });
        expect(candidateAbsences).toEqual([{ stage: 'before-host-close', pid: candidate.candidatePid, presence: 'absent' }]);
        cleanup.push({ phase: 'native-candidate-before-host-close', outcome: 'closed', genuineCloseObserved: genuineClose.length === 1 });
      }
      catch (error) { cleanup.push({ phase: 'native-candidate-before-host-close', failure: boundedFailure(error) }); }
      try {
        if (client) {
          const result = await client.close();
          expect(result).toEqual({ code: 0, signal: null });
          cleanup.push({ phase: 'host-close', result });
        }
      }
      catch (error) { cleanup.push({ phase: 'host-close', failure: boundedFailure(error) }); }
      try {
        await settleLosingCandidates(root, () => null, 15000);
        const all = events();
        const absent = all.filter(event => event.event === 'startup-candidate-spawn')
          .map(event => ({ stage: 'after-host-close', pid: event.candidatePid, presence: nativePresence(event.candidatePid) }));
        candidateAbsences.push(...absent);
        expect(absent).toHaveLength(1);
        expect(absent.every(candidate => candidate.presence === 'absent')).toBe(true);
        expect(all.filter(event => event.event.startsWith('startup-candidate-parent-fd-') && event.failure)).toEqual([]);
        expect(all.filter(event => event.event === 'startup-candidate-parent-fd-close')).toHaveLength(1);
        const genuineClose = all.filter(event => event.event === 'startup-candidate-close');
        expect(genuineClose.length).toBeLessThanOrEqual(1);
        for (const event of genuineClose) expect(event).toMatchObject({ candidatePid: absent[0].pid, code: 1, signal: null });
        expect(evidence.owners().every(owner => owner.writer === null && owner.daemon === null)).toBe(true);
        cleanup.push({ phase: 'native-candidate-after-host-close', outcome: 'absent' });
      }
      catch (error) { cleanup.push({ phase: 'native-candidate', failure: boundedFailure(error) }); }
      try {
        after = inventory(index);
        expect(after.filter((entry: any) => entry.relative !== 'daemon.log')).toEqual(before);
        expect(after.filter((entry: any) => entry.relative === 'daemon.log')).toHaveLength(1);
      } catch (error) { cleanup.push({ phase: 'index-inventory', failure: boundedFailure(error) }); }
      try { registryAfter = inventory(registry); expect(registryAfter).toEqual(registryBefore); }
      catch (error) { cleanup.push({ phase: 'registry-inventory', failure: boundedFailure(error) }); }
      try { spawnLog = boundedSpawnLog(spawnLogFor(root)); if (spawnLog?.failure) throw new Error(spawnLog.failure); }
      catch (error) { cleanup.push({ phase: 'spawn-log', failure: boundedFailure(error) }); }
      try { mergedLog = boundedNativeLog(path.join(index, 'daemon.log'), true); if (mergedLog.failure) throw new Error(mergedLog.failure); }
      catch (error) { cleanup.push({ phase: 'merged-daemon-log', failure: boundedFailure(error) }); }
      persist('native-cleanup');
      if (!primaryFailed && !evidence.failed && cleanup.every(item => !item.failure)) {
        evidence.allowObservationRemoval();
        try { removeSpawnLog(root); fs.rmSync(root, { recursive: true, force: true }); }
        catch (error) { cleanup.push({ phase: 'fixture-remove', failure: boundedFailure(error) }); }
      }
      persist('final');
    }
    if (primaryFailed) throw primaryFailure;
    expect(cleanup.filter(item => item.failure)).toEqual([]); expect(evidence.failed).toBe(false);
    expect(client?.capture().nativeClose).toEqual({ code: 0, signal: null });
  }, 90000);
});

describe('actual direct client selection and lossless native capture', () => {
  it('closes the exact verified startup socket when native stdin EOF cancels preparation', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-startup-socket-cancel-')));
    const observations = path.join(root, 'observations.jsonl'), release = path.join(root, 'release-verified-socket');
    const evidence = new StartupNativeEvidence([root], observations, 'startup-verified-socket-cancel.json');
    const cleanup: any[] = [], setupFailures: any[] = [];
    let client: ProcessClient | undefined, primaryFailed = false, primaryFailure: unknown;
    let mergedLog: ReturnType<typeof boundedNativeLog> | null = null, spawnLog: ReturnType<typeof boundedSpawnLog> = null;
    const events = () => evidence.events();
    const persist = (stage: string) => evidence.persist(stage, () => ({ root, primaryFailed,
      primaryFailure: primaryFailed ? boundedFailure(primaryFailure) : null, setupFailures, cleanup, mergedLog, spawnLog,
      capture: client?.capture(), fixtureRemoved: !fs.existsSync(root) }));
    try {
      fs.writeFileSync(path.join(root, 'app.ts'), 'export function unopenedStartupSymbol() { return 1; }\n');
      await indexFixture(root, setupFailures);
      const recorder = recordSpawns(root);
      client = new ProcessClient([...recorder.args, '--require', path.join(__dirname, 'fixtures/query-startup-transport-observer.cjs'),
        cli, 'serve', '--mcp', '--path', root], root, environment({ ...recorder.env, CODEGRAPH_NO_DAEMON: '0',
        CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '1000', CODEGRAPH_DAEMON_INSTALL_CHECK_MS: '0', CODEGRAPH_DAEMON_RETRY_MS: '0',
        QUERY_STARTUP_VERIFIED_SOCKET_RELEASE: release, QUERY_UNLIMITED_OBSERVATIONS: observations }));
      expect((await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {},
        clientInfo: { name: 'startup-socket-cancellation', version: '1' } })).serverInfo.name).toBe('codegraph');
      expect((await client.request('tools/list')).tools.length).toBeGreaterThan(0);
      const action = client.request('tools/call', { name: 'codegraph_status', arguments: {} })
        .then(value => ({ ok: true as const, value }), error => ({ ok: false as const, error }));
      await untilNative(() => events().some(event => event.event === 'startup-held-verified-socket'));
      client.child.stdin.end();
      await untilNative(() => events().some(event => event.event === 'startup-stdin-end'));
      // P2 may close this owned socket while acquisition is still pending.
      // Require that ordering, then exercise the caller's original-close join.
      await untilNative(() => events().some(event => event.event === 'startup-held-socket-close'));
      fs.writeFileSync(release, 'release actual verified socket after actual EOF');
      expect((await action).ok).toBe(false);
      expect(await client.close()).toEqual({ code: 0, signal: null });
      expect(nativePresence(client.child.pid!)).toBe('absent');
      const held = events().filter(event => event.event === 'startup-held-verified-socket');
      const closed = events().filter(event => event.event === 'startup-held-socket-close');
      expect(held).toHaveLength(1); expect(closed).toHaveLength(1);
      expect(closed[0].socketId).toBe(held[0].socketId);
      expect(BigInt(closed[0].eventTimeNs)).toBeGreaterThan(BigInt(events().find(event => event.event === 'startup-stdin-end').eventTimeNs));
      const returned = events().filter(event => event.event === 'startup-held-socket-return');
      expect(returned).toHaveLength(1); expect(returned[0].socketId).toBe(held[0].socketId);
      expect(BigInt(closed[0].eventTimeNs)).toBeLessThan(BigInt(returned[0].eventTimeNs));
      expect(client.stderr).not.toMatch(/refused socket close|startup.*socket.*(?:incomplete|exceeded|timeout)/i);
      expect(events().filter(event => event.event === 'startup-engine' && event.pid === client!.child.pid)).toEqual([]);
      expect(events().filter(event => event.event === 'session-history')).toEqual([]);
      persist('cancelled-and-closed');
    } catch (error) { primaryFailed = true; primaryFailure = error; }
    finally {
      persist('before-cleanup');
      // Release a controlled wait even after an assertion failure; preserve
      // the fixture unless every independent native/evidence predicate passes.
      try { fs.writeFileSync(release, 'cleanup release'); } catch (error) { cleanup.push({ phase: 'release', failure: boundedFailure(error) }); }
      try { if (client) expect(await client.close()).toEqual({ code: 0, signal: null }); }
      catch (error) { cleanup.push({ phase: 'host-close', failure: boundedFailure(error) }); }
      try {
        await settleLosingCandidates(root, () => null, 15000);
        await untilNative(() => events().filter(event => event.event === 'startup-candidate-spawn').every(event => nativePresence(event.candidatePid) === 'absent')
          && events().filter(event => event.event === 'spawn').length === events().filter(event => event.event === 'exit').length);
        const candidates = events().filter(event => event.event === 'startup-candidate-spawn'); expect(candidates).toHaveLength(1);
        expect(events().filter(event => event.event === 'startup-candidate-own-exit').map(event => event.code)).toEqual([0]);
        expect(evidence.owners().every(owner => owner.writer === null && owner.daemon === null)).toBe(true);
      } catch (error) { cleanup.push({ phase: 'native-candidate-and-owners', failure: boundedFailure(error) }); }
      try { mergedLog = boundedNativeLog(path.join(root, '.codegraph/daemon.log'), true); if (mergedLog.failure) throw new Error(mergedLog.failure); }
      catch (error) { cleanup.push({ phase: 'merged-daemon-log', failure: boundedFailure(error) }); }
      try { spawnLog = boundedSpawnLog(spawnLogFor(root)); if (spawnLog?.failure) throw new Error(spawnLog.failure); }
      catch (error) { cleanup.push({ phase: 'spawn-log', failure: boundedFailure(error) }); }
      persist('native-cleanup');
      if (!primaryFailed && !evidence.failed && setupFailures.length === 0 && cleanup.every(item => !item.failure)) {
        evidence.allowObservationRemoval();
        try { removeSpawnLog(root); fs.rmSync(root, { recursive: true, force: true }); }
        catch (error) { cleanup.push({ phase: 'fixture-remove', failure: boundedFailure(error) }); }
      }
      persist('final');
    }
    if (primaryFailed) throw primaryFailure;
    expect(setupFailures).toEqual([]); expect(cleanup.filter(item => item.failure)).toEqual([]); expect(evidence.failed).toBe(false);
  }, 90000);

  it.each(['rootUri', 'workspaceFolders', 'roots', 'explicit-path'] as const)
  ('honors original client/--path precedence through %s', async selection => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-direct-client-hint-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    const observations = path.join(fixture, 'observations.jsonl');
    const evidence = new StartupNativeEvidence(roots, observations, `direct-client-hint-${selection}.json`);
    const cleanup: any[] = [], setupFailures: any[] = [];
    let client: ProcessClient | undefined, primaryFailure: unknown, primaryFailed = false;
    const events = () => evidence.events();
    const persist = (stage: string) => evidence.persist(stage, () => ({ selection, roots, setupFailures,
      primaryFailed, primaryFailure: primaryFailed ? boundedFailure(primaryFailure) : null,
      capture: client?.capture(), cleanup, fixtureRemoved: !fs.existsSync(fixture) }));
    try {
      for (const [i, root] of roots.entries()) {
        fs.mkdirSync(root); fs.writeFileSync(path.join(root, 'app.ts'), `export function ${i ? 'beta' : 'alpha'}ClientSymbol() { return ${i}; }\n`);
        await indexFixture(root, setupFailures);
      }
      client = new ProcessClient(['--require', path.join(__dirname, 'fixtures/query-startup-transport-observer.cjs'),
        cli, 'serve', '--mcp', ...(selection === 'roots' ? [] : ['--path', roots[0]])], roots[0], environment({ CODEGRAPH_NO_DAEMON: '1',
        QUERY_UNLIMITED_OBSERVATIONS: observations }));
      const uri = pathToFileURL(roots[1]).href;
      await client.request('initialize', { protocolVersion: '2024-11-05', clientInfo: { name: 'direct-client-precedence', version: '1' },
        capabilities: selection === 'roots' || selection === 'explicit-path' ? { roots: { listChanged: true } } : {},
        ...(selection === 'rootUri' ? { rootUri: uri } : selection === 'workspaceFolders' ? { workspaceFolders: [{ uri, name: 'beta' }] } : {}) });
      expect((await client.request('tools/list')).tools.length).toBeGreaterThan(0);
      const selectedRoot = selection === 'explicit-path' ? roots[0] : roots[1];
      const symbol = selection === 'explicit-path' ? 'alphaClientSymbol' : 'betaClientSymbol';
      const query = client.request('tools/call', { name: 'codegraph_explore', arguments: { query: symbol } });
      // Own an early rejection while roots discovery is being answered.
      const queryOutcome = query.then(value => ({ ok: true as const, value }), error => ({ ok: false as const, error }));
      if (selection === 'roots') {
        await untilNative(() => client!.replies.some(reply => reply.method === 'roots/list'));
        const request = client.replies.find(reply => reply.method === 'roots/list');
        client.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id: request.id, result: { roots: [{ uri, name: 'beta' }] } }) + '\n');
      }
      const outcome = await queryOutcome; if (!outcome.ok) throw outcome.error; const result = outcome.value;
      expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain(symbol);
      expect(result.content[0].text).not.toContain(selection === 'explicit-path' ? 'betaClientSymbol' : 'alphaClientSymbol');
      if (selection === 'explicit-path') expect(client.replies.filter(reply => reply.method === 'roots/list')).toEqual([]);
      // Explore history commits after successful delivery, before a later
      // request can be processed by this same host session.
      expect(await client.request('ping')).toEqual({});
      const engines = events().filter(event => event.event === 'startup-engine');
      expect(engines).toHaveLength(1); expect(engines[0].options.projectRoute).toBeUndefined();
      expect(engines[0].hasResourceObserver).toBe(true); expect(engines[0].hasDefaultOutcomeCallback).toBe(true);
      const admitted = events().filter(event => event.event === 'startup-admitted');
      expect(admitted.length).toBeGreaterThan(0); expect(admitted.every(event => event.root === selectedRoot)).toBe(true);
      expect(events().filter(event => event.event === 'startup-sqlite-open'
        && event.file === fs.realpathSync(path.join(roots[selection === 'explicit-path' ? 1 : 0], '.codegraph/codegraph.db')))).toEqual([]);
      expect(events().filter(event => event.event === 'session-history').map(event => event.projectRoot)).toEqual([selectedRoot]);
      expect(events().filter(event => event.event === 'startup-candidate-spawn')).toEqual([]);
      persist('queried');
    } catch (error) { primaryFailed = true; primaryFailure = error; }
    finally {
      persist('before-cleanup');
      try { if (client) expect(await client.close()).toEqual({ code: 0, signal: null }); }
      catch (error) { cleanup.push({ phase: 'host-close', failure: boundedFailure(error) }); }
      try { expect(evidence.owners().every(owner => owner.writer === null && owner.daemon === null)).toBe(true); }
      catch (error) { cleanup.push({ phase: 'owners', failure: boundedFailure(error) }); }
      persist('native-cleanup');
      if (!primaryFailed && !evidence.failed && setupFailures.length === 0 && cleanup.every(item => !item.failure)) {
        evidence.allowObservationRemoval(); try { fs.rmSync(fixture, { recursive: true, force: true }); }
        catch (error) { cleanup.push({ phase: 'fixture-remove', failure: boundedFailure(error) }); }
      }
      persist('final');
    }
    if (primaryFailed) throw primaryFailure;
    expect(setupFailures).toEqual([]);
    expect(cleanup.filter(item => item.failure)).toEqual([]); expect(evidence.failed).toBe(false);
  }, 90000);

  it.each(['split', 'invalid', 'incomplete', 'stderr-invalid', 'stderr-incomplete'] as const)
  ('retains actual CLI bytes and validates %s UTF-8 through EOF', async wire => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-native-wire-')));
    const observations = path.join(root, 'observations.jsonl');
    const evidence = new StartupNativeEvidence([root], observations, `startup-wire-${wire}.json`);
    const cleanup: any[] = []; let client: ProcessClient | undefined, primaryFailure: unknown, primaryFailed = false;
    const events = () => evidence.events();
    const persist = (stage: string) => evidence.persist(stage, () => ({ wire, root, primaryFailed,
      primaryFailure: primaryFailed ? boundedFailure(primaryFailure) : null, capture: client?.capture(), cleanup,
      fixtureRemoved: !fs.existsSync(root) }));
    try {
      client = new ProcessClient(['--require', path.join(__dirname, 'fixtures/query-startup-transport-observer.cjs'),
        cli, 'serve', '--mcp'], root, environment({ CODEGRAPH_NO_DAEMON: '1',
        QUERY_STARTUP_WIRE: wire, QUERY_UNLIMITED_OBSERVATIONS: observations }));
      const initialize = client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {},
        clientInfo: { name: 'actual-cli-utf8', version: '1' } });
      // Attach rejection ownership before native EOF can settle the request.
      const outcome = initialize.then(value => ({ ok: true as const, value }), error => ({ ok: false as const, error }));
      if (wire === 'incomplete') { await untilNative(() => events().some(event => event.event === 'startup-wire')); client.child.stdin.end(); }
      const reply = await outcome;
      if (wire === 'split' || wire.startsWith('stderr-')) {
        expect(reply.ok).toBe(true); if (reply.ok) expect(reply.value.serverInfo.name).toBe('codegraph');
      } else { expect(reply.ok).toBe(false); if (!reply.ok) expect(String(reply.error)).toContain('UTF-8'); }
      const closed = await Promise.allSettled([client.close()]);
      expect(closed[0].status).toBe(wire === 'split' ? 'fulfilled' : 'rejected');
      expect(client.capture().nativeClose).toEqual({ code: 0, signal: null });
      expect(client.capture().failure).toEqual(wire === 'split' ? null : expect.stringContaining('UTF-8'));
      for (const stream of [client.capture().stdout, client.capture().stderr]) {
        const bytes = Buffer.from(stream.base64, 'base64');
        expect(bytes.length).toBe(stream.retainedBytes); expect(stream.truncated).toBe(false);
        expect(stream.seenBytes).toBe(stream.retainedBytes); expect(stream.eof).toBe(true);
        expect(createHash('sha256').update(bytes).digest('hex')).toBe(stream.sha256);
      }
      const wireEvents = events().filter(event => event.event === 'startup-wire');
      expect(wireEvents).toHaveLength(1);
      if (wire === 'split') {
        // Require an actual parent data event ending inside the multibyte
        // sequence; a producer split coalesced by the OS is insufficient.
        expect(client.capture().stdoutChunkEnds).toContain(wireEvents[0].absoluteBoundary);
        expect(Buffer.from(client.capture().stdout.base64, 'base64').subarray(wireEvents[0].bytesBefore,
          wireEvents[0].bytesBefore + Buffer.from(wireEvents[0].originalBase64, 'base64').length))
          .toEqual(Buffer.from(wireEvents[0].originalBase64, 'base64'));
      }
      persist('native-closed');
    } catch (error) { primaryFailed = true; primaryFailure = error; }
    finally {
      persist('before-cleanup');
      if (client) await Promise.allSettled([client.close()]);
      if (!client || client.capture().nativeClose?.code !== 0 || client.capture().nativeClose?.signal !== null) cleanup.push({ phase: 'native-close', failure: 'missing exact close0/null' });
      persist('native-cleanup');
      if (!primaryFailed && !evidence.failed && cleanup.every(item => !item.failure)) {
        evidence.allowObservationRemoval(); try { fs.rmSync(root, { recursive: true, force: true }); }
        catch (error) { cleanup.push({ phase: 'fixture-remove', failure: boundedFailure(error) }); }
      }
      persist('final');
    }
    if (primaryFailed) throw primaryFailure;
    expect(cleanup.filter(item => item.failure)).toEqual([]); expect(evidence.failed).toBe(false);
  }, 90000);
});
