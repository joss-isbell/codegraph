import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { createHash } from 'crypto';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { CodeGraph } from '../src';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { parseQueryDiagnostics } from './query-diagnostics-helper';

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
  private captureFailure: Error | null = null;
  private closePromise: Promise<{ code: number | null; signal: NodeJS.Signals | null }> | null = null;
  private nativeClose: { code: number | null; signal: NodeJS.Signals | null } | null = null;
  readonly closed: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  private nextId = 0;
  private pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void; timer: NodeJS.Timeout }>();
  constructor(args: string[], root: string, env: NodeJS.ProcessEnv) {
    this.child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, ...args], { cwd: root, env, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
    let buffer = '';
    this.child.stdout.on('data', data => {
      const chunk = Buffer.isBuffer(data) ? data : Buffer.from(data);
      this.stdoutSeen += chunk.length;
      this.stdoutPrefix = Buffer.concat([this.stdoutPrefix, chunk.subarray(0, Math.max(0, 65536 - this.stdoutPrefix.length))]);
      if (this.stdoutSeen > 65536) this.captureFailed(new Error('owned stdout overflow'));
      if (this.captureFailure) { buffer = ''; return; }
      buffer += data;
      let end: number;
      while ((end = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
        let reply: any; try { reply = JSON.parse(line); } catch { continue; }
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
    });
    this.child.stdin.on('error', error => this.fail(error));
    this.child.on('error', error => this.fail(error));
    this.closed = new Promise(resolve => this.child.once('close', (code, signal) => {
      this.nativeClose = { code, signal };
      this.fail(new Error(`owned server closed: ${code}/${signal}: ${this.stderr}`)); resolve(this.nativeClose);
    }));
  }
  get stdout() { return this.stdoutPrefix.toString(); }
  get stderr() { return this.stderrPrefix.toString(); }
  capture() { return { pid: this.child.pid, nativeClose: this.nativeClose,
    stdout: { text: this.stdout, seenBytes: this.stdoutSeen, retainedBytes: this.stdoutPrefix.length, truncated: this.stdoutSeen > this.stdoutPrefix.length },
    stderr: { text: this.stderr, seenBytes: this.stderrSeen, retainedBytes: this.stderrPrefix.length, truncated: this.stderrSeen > this.stderrPrefix.length },
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
    } finally {
      const cleanup = await Promise.allSettled(clients.map(client => client.close()));
      if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, 'strict-invalid-entry.json'), JSON.stringify({ before, after: inventory(index), owners: owners(index), registryBefore, registryAfter: inventory(registry), facts, captures: clients.map(client => client.capture()), cleanup: cleanup.map(result => result.status === 'fulfilled' ? result : { status: result.status, reason: String(result.reason) }) }, null, 2));
      fs.rmSync(root, { recursive: true, force: true });
      expect(cleanup.filter(result => result.status === 'rejected')).toEqual([]);
    }
  }, 120000);

  it('keeps the constructor request through a later environment change and default/explicit/default real queries', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-strict-captured-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    let client: ProcessClient | undefined;
    let diagnostics: unknown;
    let before: unknown[] = [];
    const registry = path.join(os.homedir(), '.codegraph', 'daemons');
    let registryBefore: unknown[] = [];
    try {
      for (const [i, root] of roots.entries()) {
        fs.mkdirSync(root);
        fs.writeFileSync(path.join(root, 'app.ts'), `export function ${i ? 'beta' : 'alpha'}CapturedSymbol() { return ${i}; }\n`);
        const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      }
      before = roots.map(root => ({ root, index: inventory(path.join(root, '.codegraph')), owners: owners(path.join(root, '.codegraph')) }));
      registryBefore = inventory(registry);
      client = new ProcessClient([path.join(__dirname, 'fixtures/query-captured-startup.cjs'), roots[0]], roots[0], environment({
        CODEGRAPH_NO_DAEMON: '1', CODEGRAPH_QUERY_MODE: 'default', CODEGRAPH_QUERY_POOL_SIZE: '3', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
      }));
      await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'captured-startup', version: '1' } });
      const status = await client.request('tools/call', { name: 'codegraph_status', arguments: {} });
      const parsed = parseQueryDiagnostics(status); diagnostics = parsed;
      expect(parsed.requested).toEqual({ mode: 'default', poolSize: '3', busyTimeoutMs: 'unlimited' });
      expect(parsed.effective).toMatchObject({ source: 'strict', executionMode: 'direct', concurrency: { kind: 'finite', maximum: 3 }, deadline: { kind: 'absent' }, retention: 'ordinary' });
      expect(parsed.implementation.support.combinedPreset).toBe(true);
      for (const [i, projectPath] of [[0, undefined], [1, roots[1]], [0, undefined]] as const) {
        const symbol = `${i ? 'beta' : 'alpha'}CapturedSymbol`;
        const result = await client.request('tools/call', { name: 'codegraph_search', arguments: { query: symbol, ...(projectPath ? { projectPath } : {}) } });
        expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain(symbol);
      }
      const exit = await client.close(); expect(exit).toEqual({ code: 0, signal: null });
    } finally {
      const cleanup = await Promise.allSettled(client ? [client.close()] : []);
      if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, 'strict-captured-startup.json'), JSON.stringify({ roots, before,
        after: roots.map(root => ({ root, index: inventory(path.join(root, '.codegraph')), owners: owners(path.join(root, '.codegraph')) })),
        registryBefore, registryAfter: inventory(registry), diagnostics, capture: client?.capture(),
        cleanup: cleanup.map(result => result.status === 'fulfilled' ? result : { status: result.status, reason: String(result.reason) }) }, null, 2));
      fs.rmSync(fixture, { recursive: true, force: true });
      expect(cleanup.filter(result => result.status === 'rejected')).toEqual([]);
    }
  }, 120000);
});
