import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { CodeGraph } from '../src';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { parseQueryDiagnostics } from './query-diagnostics-helper';

type Settings = { mode?: string; poolSize?: string; busyTimeoutMs?: string };
type Expected = { concurrency: 'default' | 'disabled' | 'unlimited' | number; deadline: 'absent' | 'inapplicable' | number;
  retention: 'ordinary' | 'retire-idle'; classification: 'default' | 'custom' | 'unlimited' };
type Row = { name: string; requested: Settings; expected: Expected };

// Literal outcomes encode the agreed public table independently of the resolver.
// A default maximum is host dependent; its contract is min(cpuCount - 1, 2), at least 1.
const defaultRows: Array<[string | undefined, string | undefined, Expected]> = [
  [undefined, undefined, { concurrency: 'default', deadline: 45000, retention: 'ordinary', classification: 'default' }],
  [undefined, '45000', { concurrency: 'default', deadline: 45000, retention: 'ordinary', classification: 'custom' }],
  [undefined, 'unlimited', { concurrency: 'default', deadline: 'absent', retention: 'ordinary', classification: 'custom' }],
  ['0', undefined, { concurrency: 'disabled', deadline: 'inapplicable', retention: 'ordinary', classification: 'custom' }],
  ['0', '45000', { concurrency: 'disabled', deadline: 'inapplicable', retention: 'ordinary', classification: 'custom' }],
  ['0', 'unlimited', { concurrency: 'disabled', deadline: 'inapplicable', retention: 'ordinary', classification: 'custom' }],
  ['2', undefined, { concurrency: 2, deadline: 45000, retention: 'ordinary', classification: 'custom' }],
  ['2', '45000', { concurrency: 2, deadline: 45000, retention: 'ordinary', classification: 'custom' }],
  ['2', 'unlimited', { concurrency: 2, deadline: 'absent', retention: 'ordinary', classification: 'custom' }],
  ['unlimited', undefined, { concurrency: 'unlimited', deadline: 45000, retention: 'retire-idle', classification: 'custom' }],
  ['unlimited', '45000', { concurrency: 'unlimited', deadline: 45000, retention: 'retire-idle', classification: 'custom' }],
  ['unlimited', 'unlimited', { concurrency: 'unlimited', deadline: 'absent', retention: 'retire-idle', classification: 'unlimited' }],
];
const unlimitedRows: Array<[string | undefined, string | undefined, Expected]> = [
  [undefined, undefined, { concurrency: 'unlimited', deadline: 'absent', retention: 'retire-idle', classification: 'unlimited' }],
  [undefined, '45000', { concurrency: 'unlimited', deadline: 45000, retention: 'retire-idle', classification: 'custom' }],
  [undefined, 'unlimited', { concurrency: 'unlimited', deadline: 'absent', retention: 'retire-idle', classification: 'unlimited' }],
  ['0', undefined, { concurrency: 'disabled', deadline: 'inapplicable', retention: 'retire-idle', classification: 'custom' }],
  ['0', '45000', { concurrency: 'disabled', deadline: 'inapplicable', retention: 'retire-idle', classification: 'custom' }],
  ['0', 'unlimited', { concurrency: 'disabled', deadline: 'inapplicable', retention: 'retire-idle', classification: 'custom' }],
  ['2', undefined, { concurrency: 2, deadline: 'absent', retention: 'retire-idle', classification: 'custom' }],
  ['2', '45000', { concurrency: 2, deadline: 45000, retention: 'retire-idle', classification: 'custom' }],
  ['2', 'unlimited', { concurrency: 2, deadline: 'absent', retention: 'retire-idle', classification: 'custom' }],
  ['unlimited', undefined, { concurrency: 'unlimited', deadline: 'absent', retention: 'retire-idle', classification: 'unlimited' }],
  ['unlimited', '45000', { concurrency: 'unlimited', deadline: 45000, retention: 'retire-idle', classification: 'custom' }],
  ['unlimited', 'unlimited', { concurrency: 'unlimited', deadline: 'absent', retention: 'retire-idle', classification: 'unlimited' }],
];
const rows: Row[] = [
  ...[undefined, 'default'].flatMap(mode => defaultRows.map(([poolSize, busyTimeoutMs, expected], i) => ({
    name: `${mode ?? 'unset'}-${i + 1}`, requested: {
      ...(mode === undefined ? {} : { mode }), ...(poolSize === undefined ? {} : { poolSize }),
      ...(busyTimeoutMs === undefined ? {} : { busyTimeoutMs }),
    }, expected,
  }))),
  ...unlimitedRows.map(([poolSize, busyTimeoutMs, expected], i) => ({
    name: `unlimited-${i + 1}`, requested: { mode: 'unlimited',
      ...(poolSize === undefined ? {} : { poolSize }), ...(busyTimeoutMs === undefined ? {} : { busyTimeoutMs }) }, expected,
  })),
  { name: 'trimmed-default-leading-zero-equal-controls', requested: { mode: ' default ', poolSize: ' 0002 ', busyTimeoutMs: ' 00045000 ' },
    expected: { concurrency: 2, deadline: 45000, retention: 'ordinary', classification: 'custom' } },
  { name: 'trimmed-zero-with-valid-minimum-timeout', requested: { mode: ' unlimited ', poolSize: ' 000 ', busyTimeoutMs: '001000' },
    expected: { concurrency: 'disabled', deadline: 'inapplicable', retention: 'retire-idle', classification: 'custom' } },
  { name: 'exact-upper-boundaries-leading-zeroes', requested: { poolSize: ' 0009007199254740991 ', busyTimeoutMs: '02147483647' },
    expected: { concurrency: 9007199254740991, deadline: 2147483647, retention: 'ordinary', classification: 'custom' } },
  { name: 'exact-positive-minima', requested: { poolSize: '1', busyTimeoutMs: ' 001000 ' },
    expected: { concurrency: 1, deadline: 1000, retention: 'ordinary', classification: 'custom' } },
  { name: 'trimmed-preset-and-independent-sentinels', requested: { mode: ' unlimited ', poolSize: ' unlimited ', busyTimeoutMs: ' unlimited ' },
    expected: { concurrency: 'unlimited', deadline: 'absent', retention: 'retire-idle', classification: 'unlimited' } },
  { name: 'pool-sentinel-retirement-with-minimum-finite-timeout', requested: { poolSize: ' unlimited ', busyTimeoutMs: '1000' },
    expected: { concurrency: 'unlimited', deadline: 1000, retention: 'retire-idle', classification: 'custom' } },
];

function environment(requested: Settings, mode: string, observations: string): NodeJS.ProcessEnv {
  const env = { ...process.env };
  for (const key of ['CODEGRAPH_QUERY_MODE', 'CODEGRAPH_QUERY_POOL_SIZE', 'CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', 'CODEGRAPH_DAEMON_INTERNAL']) delete env[key];
  return { ...env, CODEGRAPH_NO_DAEMON: mode === 'direct' ? '1' : '0',
    ...(requested.mode === undefined ? {} : { CODEGRAPH_QUERY_MODE: requested.mode }),
    ...(requested.poolSize === undefined ? {} : { CODEGRAPH_QUERY_POOL_SIZE: requested.poolSize }),
    ...(requested.busyTimeoutMs === undefined ? {} : { CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: requested.busyTimeoutMs }),
    CODEGRAPH_NO_UPDATE_CHECK: '1', CODEGRAPH_TELEMETRY: '0', CODEGRAPH_NO_WATCH: '1', CODEGRAPH_NO_WATCHDOG: '1',
    CODEGRAPH_PPID_POLL_MS: '0', CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS: '0', CODEGRAPH_NO_RELAUNCH: '1', CODEGRAPH_WASM_RELAUNCHED: '1',
    CODEGRAPH_DAEMON_RETRY_MS: '0', CODEGRAPH_MCP_LOG_ATTACH: '1',
    QUERY_UNLIMITED_OBSERVATIONS: observations, QUERY_UNLIMITED_RELEASE: `${observations}.unused-release`,
    QUERY_UNLIMITED_FAIL_DAEMON: mode === 'writable-fallback' ? '1' : '0' };
}

class Client {
  readonly child: ChildProcessWithoutNullStreams;
  private readonly pending = new Map<number, { resolve: (result: any) => void; reject: (error: Error) => void; timer: NodeJS.Timeout }>();
  private nextId = 0;
  private firstFailure: Error | null = null;
  private stdoutPrefix = Buffer.alloc(0);
  private stderrPrefix = Buffer.alloc(0);
  private stdoutSeen = 0;
  private stderrSeen = 0;
  private nativeClose: { code: number | null; signal: NodeJS.Signals | null } | null = null;
  private readonly closed: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  private closePromise: Promise<{ code: number | null; signal: NodeJS.Signals | null }> | null = null;
  readonly replies: any[] = [];
  constructor(root: string, env: NodeJS.ProcessEnv) {
    this.child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, '--require', path.join(__dirname, 'fixtures/query-unlimited-preload.cjs'),
      path.resolve(__dirname, '../dist/bin/codegraph.js'), 'serve', '--mcp', '--path', root, '--no-watch'],
    { cwd: root, env, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'] });
    let buffer = '';
    this.child.stdout.on('data', data => {
      const bytes = Buffer.isBuffer(data) ? data : Buffer.from(data);
      this.stdoutSeen += bytes.length;
      this.stdoutPrefix = Buffer.concat([this.stdoutPrefix, bytes.subarray(0, Math.max(0, 65536 - this.stdoutPrefix.length))]);
      if (this.stdoutSeen > 65536) this.fail(new Error('owned stdout capture overflow'));
      if (this.firstFailure) { buffer = ''; return; }
      buffer += bytes.toString();
      let end: number;
      while ((end = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
        let reply: any; try { reply = JSON.parse(line); } catch { continue; }
        this.replies.push(reply);
        const request = this.pending.get(reply.id);
        if (request) { this.pending.delete(reply.id); clearTimeout(request.timer);
          reply.error ? request.reject(new Error(JSON.stringify(reply.error))) : request.resolve(reply.result); }
      }
    });
    this.child.stderr.on('data', data => {
      const bytes = Buffer.isBuffer(data) ? data : Buffer.from(data);
      this.stderrSeen += bytes.length;
      this.stderrPrefix = Buffer.concat([this.stderrPrefix, bytes.subarray(0, Math.max(0, 65536 - this.stderrPrefix.length))]);
      if (this.stderrSeen > 65536) this.fail(new Error('owned stderr capture overflow'));
    });
    this.child.on('error', error => this.fail(error));
    this.child.stdin.on('error', error => this.fail(error));
    this.closed = new Promise(resolve => this.child.once('close', (code, signal) => {
      this.nativeClose = { code, signal }; this.rejectPending(new Error(`owned host closed ${code}/${signal}`)); resolve(this.nativeClose);
    }));
  }
  private rejectPending(error: Error) { for (const request of this.pending.values()) { clearTimeout(request.timer); request.reject(error); } this.pending.clear(); }
  private fail(error: Error) { this.firstFailure ??= error; this.rejectPending(this.firstFailure); }
  get stderr() { return this.stderrPrefix.toString(); }
  capture() { return { pid: this.child.pid, nativeClose: this.nativeClose, failure: this.firstFailure?.message ?? null,
    stdout: { text: this.stdoutPrefix.toString(), seenBytes: this.stdoutSeen, retainedBytes: this.stdoutPrefix.length, truncated: this.stdoutSeen > this.stdoutPrefix.length },
    stderr: { text: this.stderr, seenBytes: this.stderrSeen, retainedBytes: this.stderrPrefix.length, truncated: this.stderrSeen > this.stderrPrefix.length }, replies: this.replies }; }
  request(method: string, params: unknown = {}): Promise<any> {
    if (this.firstFailure) return Promise.reject(this.firstFailure);
    const id = ++this.nextId;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`independent 20000ms request deadline: ${this.stderr}`)); }, 20000);
      this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
    });
  }
  close(): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
    return this.closePromise ??= new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.fail(new Error('independent 15000ms native close deadline')); reject(this.firstFailure); }, 15000);
      this.closed.then(result => { clearTimeout(timer); this.firstFailure ? reject(this.firstFailure) : resolve(result); });
      if (!this.child.stdin.destroyed) this.child.stdin.end();
    });
  }
}

describe('strict preset and overrides through real direct and writable MCP hosts', () => {
  it.each((['direct', 'writable-fallback'] as const).flatMap(mode => rows.map(row => [mode, row.name, row] as const)))
  ('reports the complete policy and routes real queries in %s / %s', async (mode, _name, row) => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-mcp-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    const observations = path.join(fixture, 'workers.jsonl');
    const events = (): any[] => {
      if (!fs.existsSync(observations)) return [];
      if (fs.statSync(observations).size > 1048576) throw new Error('owned observation capture overflow');
      return fs.readFileSync(observations, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
    };
    let client: Client | undefined;
    let diagnostics: unknown;
    let primaryFailure: unknown;
    let close: PromiseSettledResult<unknown> | undefined;
    let cleanupEvents: any[] = [];
    let fixtureRemoved = false;
    const results: any[] = [];
    try {
      for (const [i, root] of roots.entries()) {
        fs.mkdirSync(root); fs.writeFileSync(path.join(root, 'app.ts'), `export function ${i ? 'beta' : 'alpha'}PolicySymbol() { return ${i}; }\n`);
        const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      }
      client = new Client(roots[0], environment(row.requested, mode, observations));
      const initialized = await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'strict-policy-table', version: '1' } });
      expect(initialized.serverInfo.name).toBe('codegraph');
      const actual = parseQueryDiagnostics(await client.request('tools/call', { name: 'codegraph_status', arguments: {} })); diagnostics = actual;
      expect(actual.requested).toEqual(row.requested);
      expect(actual.effective).toMatchObject({ source: 'strict', executionMode: mode, preset: row.requested.mode?.trim() ?? 'default',
        concurrency: typeof row.expected.concurrency === 'number' ? { kind: 'finite', maximum: row.expected.concurrency }
          : row.expected.concurrency === 'default' ? { kind: 'finite', maximum: Math.max(1, Math.min(os.cpus().length - 1, 2)) } : { kind: row.expected.concurrency },
        deadline: typeof row.expected.deadline === 'number' ? { kind: 'finite', milliseconds: row.expected.deadline } : { kind: row.expected.deadline },
        retention: row.expected.retention, classification: row.expected.classification });
      expect(actual.effective.defaults).toEqual({ concurrency: { kind: 'finite', maximum: Math.max(1, Math.min(os.cpus().length - 1, 2)) },
        deadline: { kind: 'finite', milliseconds: 45000 }, retention: 'ordinary' });
      expect(actual.implementation.support.combinedPreset).toBe(true);
      expect(actual.lifecycle).toMatchObject({ workers: 0, queued: 0, executing: 0,
        dispatch: row.expected.concurrency === 'disabled' ? 'main-thread' : 'worker' });
      expect(actual.ownership).toMatchObject({ writerAuthority: true, synchronizationAuthority: true, capability: 'read-write', resourceScope: 'local-engine' });
      for (const [i, projectPath] of [[0, undefined], [1, roots[1]], [0, undefined]] as const) {
        const symbol = `${i ? 'beta' : 'alpha'}PolicySymbol`;
        for (const name of ['codegraph_search', 'codegraph_explore']) {
          const result = await client.request('tools/call', { name, arguments: { query: symbol, ...(projectPath ? { projectPath } : {}) } });
          results.push({ name, symbol, projectPath, result });
          expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain(symbol); expect(result.content[0].text).toContain('app.ts');
        }
      }
      const facts = events();
      const history = facts.filter(event => event.event === 'session-history');
      expect(history.map(event => [event.projectRoot, event.query, event.index])).toEqual([
        [roots[0], 'alphaPolicySymbol', 1], [roots[1], 'betaPolicySymbol', 1], [roots[0], 'alphaPolicySymbol', 2],
      ]);
      expect(new Set(history.map(event => event.historyId)).size).toBe(1);
      const spawned = facts.filter(event => event.event === 'spawn');
      if (row.expected.concurrency === 'disabled') expect(spawned).toHaveLength(0);
      else {
        expect(spawned.length).toBeGreaterThan(0);
        for (const worker of spawned) expect(worker.execution.policy).toMatchObject({ requested: row.requested, source: 'strict', executionMode: mode,
          concurrency: actual.effective.concurrency, deadline: actual.effective.deadline, retention: row.expected.retention, classification: row.expected.classification });
      }
      const candidates = facts.filter(event => event.event === 'unavailable-candidate');
      const exitedCandidates = facts.filter(event => event.event === 'unavailable-candidate-exit');
      if (mode === 'writable-fallback') {
        expect(candidates).toHaveLength(1); expect(candidates[0].candidatePid).toBeGreaterThan(0);
        expect(exitedCandidates.map(event => [event.candidatePid, event.code])).toEqual([[candidates[0].candidatePid, 78]]);
        expect(client.stderr).toContain('Shared daemon unavailable');
        expect(client.stderr).not.toMatch(/version.*differs|Attached to shared daemon/);
      } else { expect(candidates).toHaveLength(0); expect(exitedCandidates).toHaveLength(0); }
      expect(await client.close()).toEqual({ code: 0, signal: null });
    } catch (error) { primaryFailure = error; throw error; }
    finally {
      close = (await Promise.allSettled(client ? [client.close()] : []))[0];
      let eventFailure: unknown;
      try { cleanupEvents = events(); } catch (error) { eventFailure = error; }
      const nativeClosed = !client || client.capture().nativeClose !== null;
      const workersClosed = cleanupEvents.filter(event => event.event === 'spawn').length === cleanupEvents.filter(event => event.event === 'exit').length;
      try {
        if (nativeClosed && workersClosed && close?.status !== 'rejected' && !eventFailure) {
          await fs.promises.rm(fixture, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 }); fixtureRemoved = true;
        }
      } finally {
        if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, `strict-policy-${mode}-${row.name}.json`), JSON.stringify({
          mode, row, roots, diagnostics, results, events: cleanupEvents, capture: client?.capture(), fixtureRemoved,
          primaryFailure: primaryFailure ? String(primaryFailure) : null, eventFailure: eventFailure ? String(eventFailure) : null,
          nativeClosed, workersClosed, close: close?.status === 'rejected' ? { status: 'rejected', reason: String(close.reason) } : close,
        }, null, 2));
      }
      // Complete cleanup assertions remain part of the selected test's tail.
      if (!primaryFailure) { expect(eventFailure).toBeUndefined(); expect(close?.status).not.toBe('rejected'); expect(nativeClosed).toBe(true); expect(workersClosed).toBe(true); expect(fixtureRemoved).toBe(true); }
    }
  }, 45000);
});
