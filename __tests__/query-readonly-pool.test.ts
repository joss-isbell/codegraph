import { afterEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { Worker } from 'worker_threads';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { CodeGraph, getDatabasePath } from '../src';
import { getWriterPidPath, tryAcquireWriterLock, releaseWriterLock } from '../src/mcp/writer-lock';
import type { MCPEngine } from '../src/mcp/engine';
import { createDatabase } from '../src/db/sqlite-adapter';
import { createQueryExecutionContext, createQueryRequestContext, routeQueryRequest } from '../src/mcp/query-context';
import { resolveQueryPolicy } from '../src/mcp/query-policy';
import { ExploreSessionState } from '../src/mcp/explore-session-state';
import { recordSpawns, removeSpawnLog, settleLosingCandidates } from './daemon-candidates';
const BuiltPool: typeof import('../src/mcp/query-pool').QueryPool = require('../dist/mcp/query-pool').QueryPool;

const BuiltEngine: typeof import('../src/mcp/engine').MCPEngine = require('../dist/mcp/engine').MCPEngine;
const BuiltConnection: typeof import('../src/db').DatabaseConnection = require('../dist/db').DatabaseConnection;
const roots: string[] = [];
afterEach(async () => {
  vi.unstubAllEnvs();
  // Native watcher/SQLite handle rundown can lag close briefly on Windows.
  for (const root of roots.splice(0)) {
    await fs.promises.rm(root, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    expect(fs.existsSync(root)).toBe(false);
  }
}, 30000);
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function fixture(symbol: string): Promise<string> {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-readonly-pool-')));
  roots.push(root);
  fs.writeFileSync(path.join(root, 'app.ts'), `export function ${symbol}() { return 1; }\n`);
  const cg = await CodeGraph.init(root);
  try { await cg.indexAll(); } finally { cg.close(); }
  return root;
}
function observeWorkers() {
  const threads = require('worker_threads') as typeof import('worker_threads');
  const original = threads.Worker;
  const events: any[] = [];
  threads.Worker = class extends original {
    constructor(file: string | URL, options?: import('worker_threads').WorkerOptions) {
      super(file, options);
      if (!String(file).replaceAll('\\', '/').endsWith('/mcp/query-worker.js')) return;
      const threadId = this.threadId;
      events.push({ type: 'spawn', threadId, execution: options?.workerData?.execution });
      this.on('message', value => events.push({ threadId, ...value }));
      this.on('exit', code => events.push({ type: 'exit', threadId, code }));
    }
  } as typeof Worker;
  return { events, restore: () => { threads.Worker = original; } };
}
function record(name: string, value: unknown) {
  if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'readonly-pool-facts.jsonl'), JSON.stringify({ name, ...value as object }) + '\n');
}

describe('explicit finite read-only query pooling', () => {
  it.each([undefined, '0'])('keeps read-only selection %s unpooled with an inapplicable pool deadline', async poolSize => {
    const root = await fixture('ordinaryReadOnlySymbol');
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', poolSize);
    const observed = observeWorkers();
    const reader = new BuiltEngine({ readOnly: true, queryPool: true, watch: true });
    try {
      await reader.ensureInitialized(root);
      const response = await reader.getToolHandler().execute('codegraph_search', { query: 'ordinaryReadOnlySymbol' });
      expect(response.content[0].text).toContain('app.ts:1');
      expect(reader.getQueryExecutionState().actual).toMatchObject({ dispatch: 'main-thread', concurrency: { kind: 'disabled' }, deadline: { kind: 'inapplicable' }, workers: 0, resourceScope: 'local-engine' });
      expect(observed.events).toEqual([]);
      expect(fs.existsSync(getWriterPidPath(root))).toBe(false);
    } finally { await reader.stop(); observed.restore(); }
  });
  it('closes the real read connection when unsuitable SQLite data rejects opening', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-readonly-open-failure-')));
    roots.push(root);
    const file = path.join(root, 'broken.db');
    fs.writeFileSync(file, 'This is not a SQLite database and must remain unchanged.');
    const sqlite = require('node:sqlite');
    const RealDatabase = sqlite.DatabaseSync;
    const opened: any[] = [];
    sqlite.DatabaseSync = class extends RealDatabase {
      constructor(...args: any[]) { super(...args); opened.push(this); }
    };
    try {
      expect(() => BuiltConnection.open(file, { readOnly: true })).toThrow();
      expect(opened).toHaveLength(1);
      expect(opened[0].isOpen).toBe(false);
      expect(fs.readFileSync(file, 'utf8')).toBe('This is not a SQLite database and must remain unchanged.');
      record('failed-open-closed', { file, count: opened.length, open: opened[0].isOpen });
    } finally {
      sqlite.DatabaseSync = RealDatabase;
      for (const db of opened) if (db.isOpen) db.close();
    }
  });
  it('keeps cold default/explicit reads on real workers while the live owner watches and commits', async () => {
    const root = await fixture('originalSymbol');
    const owner = await CodeGraph.open(root);
    expect(tryAcquireWriterLock(root, 'direct').kind).toBe('acquired');
    owner.watch({ debounceMs: 50 });
    const lockPath = getWriterPidPath(root);
    const lock = fs.readFileSync(lockPath, 'utf8');
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '2');
    const observed = observeWorkers();
    const reader: MCPEngine = new BuiltEngine({ readOnly: true, queryPool: true, watch: true, writerLockRoot: root });
    try {
      await reader.ensureInitialized(root);
      for (const projectPath of [undefined, root]) {
        const result = await reader.getToolHandler().execute('codegraph_search', { query: 'originalSymbol', ...(projectPath ? { projectPath } : {}) });
        expect(result.isError).toBeFalsy();
        expect(result.content[0].text).toContain('app.ts:1');
      }
      expect(reader.getQueryExecutionState().actual).toMatchObject({ dispatch: 'worker', concurrency: { kind: 'finite', maximum: 2 } });
      expect(observed.events.filter(event => event.type === 'result')).toHaveLength(2);
      expect(observed.events.filter(event => event.type === 'spawn').every(event => event.execution.capability === 'read-only'
        && !event.execution.ownership.writer && !event.execution.ownership.synchronization)).toBe(true);
      fs.writeFileSync(path.join(root, 'added.ts'), 'export function ownerAddedSymbol() { return 2; }\n');
      const deadline = Date.now() + 10000;
      while (!owner.searchNodes('ownerAddedSymbol').length && Date.now() < deadline) await sleep(50);
      expect(owner.searchNodes('ownerAddedSymbol')).toHaveLength(1);
      const changed = await reader.getToolHandler().execute('codegraph_search', { query: 'ownerAddedSymbol' });
      expect(changed.content[0].text).toContain('added.ts:1');
      expect(owner.isWatching()).toBe(true);
      expect(fs.readFileSync(lockPath, 'utf8')).toBe(lock);
      await reader.stop();
      expect(owner.isWatching()).toBe(true);
      expect(fs.readFileSync(lockPath, 'utf8')).toBe(lock);
      fs.writeFileSync(path.join(root, 'after.ts'), 'export function ownerAfterCleanup() {}\n');
      const finalDeadline = Date.now() + 10000;
      while (!owner.searchNodes('ownerAfterCleanup').length && Date.now() < finalDeadline) await sleep(50);
      expect(owner.searchNodes('ownerAfterCleanup')).toHaveLength(1);
      expect(observed.events.filter(event => event.type === 'spawn')).toHaveLength(observed.events.filter(event => event.type === 'exit').length);
      record('live-owner', { root, index: getDatabasePath(root), lock, events: observed.events, state: reader.getQueryExecutionState() });
    } finally { await reader.stop(); observed.restore(); owner.close(); releaseWriterLock(root); }
  }, 30000);
  it('opens isolated explicit projects from an unindexed default and preserves session emission history', async () => {
    const alpha = await fixture('alphaSymbol');
    const beta = await fixture('betaSymbol');
    const rootless = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-readonly-rootless-')));
    roots.push(rootless);
    const alias = path.join(rootless, 'alias');
    fs.symlinkSync(alpha, alias, process.platform === 'win32' ? 'junction' : 'dir');
    const owners = await Promise.all([CodeGraph.open(alpha), CodeGraph.open(beta)]);
    for (let i = 0; i < 2; i++) { expect(tryAcquireWriterLock([alpha, beta][i], 'direct').kind).toBe('acquired'); owners[i].watch({ debounceMs: 50 }); }
    const lockBytes = [alpha, beta].map(root => fs.readFileSync(getWriterPidPath(root), 'utf8'));
    const observed = observeWorkers();
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '1');
    const reader = new BuiltEngine({ readOnly: true, queryPool: true });
    try {
      await reader.ensureInitialized(rootless);
      expect(reader.hasDefaultCodeGraph()).toBe(false);
      const execution = reader.getQueryExecutionContext();
      const sessions = [new ExploreSessionState(), new ExploreSessionState()];
      for (const [requestId, projectPath, query] of [[1, alpha, 'alphaSymbol'], [2, beta, 'betaSymbol'], [3, alias, 'alphaSymbol']] as const) {
        const response = await reader.getToolHandler().execute('codegraph_search', { projectPath, query }, sessions[0], createQueryRequestContext(execution, 'reader-a', requestId));
        expect(response.isError).toBeFalsy();
        expect(response.content[0].text).toContain('app.ts:1');
        expect(response.content[0].text).not.toContain(query === 'betaSymbol' ? 'alphaSymbol' : 'betaSymbol');
      }
      for (const [index, query] of [[0, 'alphaSymbol'], [0, 'alphaSymbol'], [1, 'alphaSymbol']] as const) {
        const response = await reader.getToolHandler().execute('codegraph_explore', { projectPath: alpha, query }, sessions[index], createQueryRequestContext(execution, `reader-${index}`, `explore-${index}-${sessions[index].callCount(alpha)}`));
        expect(response.isError).toBeFalsy();
        expect(Object.keys(response)).not.toContain('_cgExploreEmission');
        expect(JSON.stringify(response)).not.toContain('admissionId');
      }
      expect(sessions[0].callCount(alpha)).toBe(2);
      expect(sessions[1].callCount(alpha)).toBe(1);
      const routes = observed.events.filter(event => event.type === 'result').map(event => event.context.project);
      expect(routes.slice(0, 3)).toEqual([
        { route: 'explicit', root: alpha, index: getDatabasePath(alpha) },
        { route: 'explicit', root: beta, index: getDatabasePath(beta) },
        { route: 'explicit', root: alpha, index: getDatabasePath(alpha) },
      ]);
      expect(reader.getQueryExecutionState().actual).toMatchObject({ dispatch: 'worker', resourceScope: 'local-engine' });
      await reader.stop();
      for (let i = 0; i < 2; i++) {
        expect(owners[i].isWatching()).toBe(true);
        expect(fs.readFileSync(getWriterPidPath([alpha, beta][i]), 'utf8')).toBe(lockBytes[i]);
      }
      expect(observed.events.filter(event => event.type === 'spawn')).toHaveLength(observed.events.filter(event => event.type === 'exit').length);
      record('rootless-routing', { alpha, beta, rootless, routes, events: observed.events });
    } finally { await reader.stop(); observed.restore(); for (let i = 0; i < 2; i++) { owners[i].close(); releaseWriterLock([alpha, beta][i]); } }
  }, 30000);
  it('uses real CLI read-only fallback when no shared service can own the live writer project', async () => {
    const alpha = await fixture('alphaSymbol');
    const beta = await fixture('betaSymbol');
    const owners = await Promise.all([CodeGraph.open(alpha), CodeGraph.open(beta)]);
    for (let i = 0; i < 2; i++) { expect(tryAcquireWriterLock([alpha, beta][i], 'direct').kind).toBe('acquired'); owners[i].watch({ debounceMs: 50 }); }
    const locks = [alpha, beta].map(root => fs.readFileSync(getWriterPidPath(root), 'utf8'));
    const observer = createDatabase(getDatabasePath(alpha), { readOnly: true }).db;
    const beforeVersion = observer.pragma('data_version', { simple: true });
    const logRoot = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-readonly-host-')));
    roots.push(logRoot);
    const log = path.join(logRoot, 'workers.jsonl');
    const clients: Array<{ child: ChildProcessWithoutNullStreams; request: (method: string, params?: unknown) => Promise<any>; stderr: () => string; close: () => Promise<void> }> = [];
    const recorder = recordSpawns(alpha);
    const openClient = () => {
      const child = spawn(process.execPath, ['--liftoff-only', ...recorder.args, '--require', path.join(__dirname, 'fixtures/readonly-worker-observer.cjs'), path.resolve(__dirname, '../dist/bin/codegraph.js'), 'serve', '--mcp', '--path', alpha], {
        cwd: alpha, stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, ...recorder.env,
          CODEGRAPH_QUERY_POOL_SIZE: '2', CODEGRAPH_NO_DAEMON: '0', CODEGRAPH_NO_WATCHDOG: '1', CODEGRAPH_NO_UPDATE_CHECK: '1',
          CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS: '0', CODEGRAPH_NO_RELAUNCH: '1', CODEGRAPH_WASM_RELAUNCHED: '1',
          CODEGRAPH_PPID_POLL_MS: '0', CG_READONLY_WORKER_LOG: log },
      }) as ChildProcessWithoutNullStreams;
      let nextId = 0;
      let buffer = '';
      let stderr = '';
      const pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void; timer: NodeJS.Timeout }>();
      child.stderr.on('data', data => { stderr += data.toString(); });
      child.stdout.on('data', data => {
        buffer += data.toString();
        for (;;) {
          const newline = buffer.indexOf('\n');
          if (newline < 0) break;
          const line = buffer.slice(0, newline); buffer = buffer.slice(newline + 1);
          let response: any; try { response = JSON.parse(line); } catch { continue; }
          const request = pending.get(response.id);
          if (request) { clearTimeout(request.timer); pending.delete(response.id); request.resolve(response); }
        }
      });
      child.on('error', error => { for (const request of pending.values()) { clearTimeout(request.timer); request.reject(error); } pending.clear(); });
      child.stdin.on('error', () => {});
      const close = async () => {
        if (child.exitCode !== null || child.signalCode !== null) return;
        child.stdin.end();
        const deadline = Date.now() + 15000;
        while (child.exitCode === null && child.signalCode === null && Date.now() < deadline) await sleep(25);
        if (child.exitCode === null && child.signalCode === null) { child.kill('SIGTERM'); throw new Error('Read-only client did not finish owned cleanup'); }
        expect(child.exitCode).toBe(0);
      };
      const client = { child, stderr: () => stderr, close, request: (method: string, params?: unknown) => new Promise<any>((resolve, reject) => {
        const id = ++nextId;
        const timer = setTimeout(() => { pending.delete(id); reject(new Error(`CLI response ${id} timed out: ${stderr}`)); }, 30000);
        pending.set(id, { resolve, reject, timer });
        child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
      }) };
      clients.push(client);
      return client;
    };
    try {
      const first = openClient();
      const second = openClient();
      await Promise.all(clients.map(client => client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'readonly-proof', version: '1' } })));
      const results = [];
      for (const client of [first, second]) {
        for (const [projectPath, query] of [[undefined, 'alphaSymbol'], [alpha, 'alphaSymbol'], [beta, 'betaSymbol']] as const) {
          const response = await client.request('tools/call', { name: 'codegraph_search', arguments: { query, ...(projectPath ? { projectPath } : {}) } });
          expect(response.error).toBeUndefined();
          expect(response.result.isError).toBeFalsy();
          expect(response.result.content[0].text).toContain('app.ts:1');
          expect(response.result.content[0].text).toContain(query);
          expect(JSON.stringify(response.result)).not.toContain('_cgExploreEmission');
          results.push(response);
        }
        expect(client.stderr()).toContain('Shared daemon unavailable');
        expect(client.stderr()).toContain('local read-only fallback');
        expect(client.stderr()).not.toMatch(/version.*differs|Attached to shared daemon/);
      }
      expect(observer.pragma('data_version', { simple: true })).toBe(beforeVersion);
      fs.writeFileSync(path.join(alpha, 'live.ts'), 'export function fromLiveOwner() {}\n');
      const deadline = Date.now() + 10000;
      while (!owners[0].searchNodes('fromLiveOwner').length && Date.now() < deadline) await sleep(50);
      expect(owners[0].searchNodes('fromLiveOwner')).toHaveLength(1);
      expect(observer.pragma('data_version', { simple: true })).not.toBe(beforeVersion);
      for (const client of clients) {
        const response = await client.request('tools/call', { name: 'codegraph_search', arguments: { query: 'fromLiveOwner' } });
        expect(response.result.content[0].text).toContain('live.ts:1');
      }
      await Promise.all(clients.map(client => client.close()));
      await settleLosingCandidates(alpha, () => process.pid);
      const events = fs.readFileSync(log, 'utf8').trim().split('\n').map(line => JSON.parse(line));
      const spawns = events.filter(event => event.type === 'spawn');
      expect(new Set(spawns.map(event => event.pid)).size).toBe(2);
      expect(new Set(spawns.map(event => event.execution.generation)).size).toBe(2);
      expect(spawns.every(event => event.execution.capability === 'read-only' && !event.execution.ownership.writer && !event.execution.ownership.synchronization)).toBe(true);
      expect(events.filter(event => event.type === 'result')).toHaveLength(8);
      expect(spawns).toHaveLength(events.filter(event => event.type === 'exit').length);
      for (let i = 0; i < 2; i++) {
        expect(owners[i].isWatching()).toBe(true);
        expect(fs.readFileSync(getWriterPidPath([alpha, beta][i]), 'utf8')).toBe(locks[i]);
      }
      fs.writeFileSync(path.join(beta, 'after.ts'), 'export function ownerAfterReaders() {}\n');
      const finalDeadline = Date.now() + 10000;
      while (!owners[1].searchNodes('ownerAfterReaders').length && Date.now() < finalDeadline) await sleep(50);
      expect(owners[1].searchNodes('ownerAfterReaders')).toHaveLength(1);
      record('actual-cli-fallback', { alpha, beta, events, results, stderr: clients.map(client => client.stderr()) });
    } finally {
      await Promise.all(clients.map(client => client.close()));
      await settleLosingCandidates(alpha, () => process.pid);
      removeSpawnLog(alpha); observer.close();
      for (let i = 0; i < 2; i++) { owners[i].close(); releaseWriterLock([alpha, beta][i]); }
    }
  }, 90000);
  it('settles incompatible real SQLite worker starts without a busy timer or a writable retry', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-readonly-broken-')));
    roots.push(root);
    fs.mkdirSync(path.join(root, '.codegraph'));
    const foreign = 'This is not a SQLite database. It must not be repaired.';
    fs.writeFileSync(getDatabasePath(root), foreign);
    const execution = createQueryExecutionContext(resolveQueryPolicy({ poolSize: '1', busyTimeoutMs: 'unlimited' }, 'read-only-fallback', 8), { readOnly: true, writer: false, synchronization: false });
    const observed = observeWorkers();
    const pool = new BuiltPool({ root, execution });
    try {
      const result = await pool.run('codegraph_search', { query: 'missing' }, routeQueryRequest(createQueryRequestContext(execution, 'broken-client', 1), 'default', root, getDatabasePath(root)));
      expect(result.isError).toBe(true);
      expect(result.content[0].text).toContain('capacity is unavailable');
      expect(pool.state).toBe('failed');
      expect(pool.execution.policy.deadline).toEqual({ kind: 'absent' });
      expect(fs.readFileSync(getDatabasePath(root), 'utf8')).toBe(foreign);
      expect(fs.readdirSync(path.join(root, '.codegraph'))).toEqual(['codegraph.db']);
      await pool.destroy();
      expect(pool.activity).toEqual({ queued: 0, executing: 0, starting: 0, retiring: 0 });
      expect(observed.events.filter(event => event.type === 'ready').every(event => event.ok === false)).toBe(true);
      expect(observed.events.filter(event => event.type === 'spawn')).toHaveLength(observed.events.filter(event => event.type === 'exit').length);
      record('incompatible-start', { root, events: observed.events });
    } finally { await pool.destroy(); observed.restore(); }
  }, 30000);
  it('recreates actual failed read-only workers without repairing the live owner database', async () => {
    const root = await fixture('survivingReaderSymbol');
    const owner = await CodeGraph.open(root);
    expect(tryAcquireWriterLock(root, 'direct').kind).toBe('acquired');
    owner.watch({ debounceMs: 50 });
    const control = createDatabase(getDatabasePath(root)).db;
    control.exec('DROP TRIGGER nodes_ai; DROP INDEX idx_nodes_kind; DELETE FROM schema_versions;');
    const observer = createDatabase(getDatabasePath(root), { readOnly: true }).db;
    const version = observer.pragma('data_version', { simple: true });
    const schema = observer.prepare('SELECT name,sql FROM sqlite_master ORDER BY name').all();
    const walBytes = fs.statSync(getDatabasePath(root) + '-wal').size;
    const lock = fs.readFileSync(getWriterPidPath(root), 'utf8');
    vi.stubEnv('CODEGRAPH_QUERY_WORKER_ALLOW_TEST_CRASH', '1');
    const execution = createQueryExecutionContext(resolveQueryPolicy({ poolSize: '1', busyTimeoutMs: 'unlimited' }, 'read-only-fallback', 8), { readOnly: true, writer: false, synchronization: false });
    const observed = observeWorkers();
    const pool = new BuiltPool({ root, execution });
    try {
      const failed = await pool.run('__test_crash__', {}, routeQueryRequest(createQueryRequestContext(execution, 'crash-reader', 1), 'default', root, getDatabasePath(root)));
      expect(failed.isError).toBe(true);
      const result = await pool.run('codegraph_search', { query: 'survivingReaderSymbol' }, routeQueryRequest(createQueryRequestContext(execution, 'crash-reader', 2), 'default', root, getDatabasePath(root)));
      expect(result.isError).toBeFalsy();
      expect(result.content[0].text).toContain('app.ts:1');
      expect(pool.state).toBe('accepting');
      await pool.destroy();
      expect(observer.pragma('data_version', { simple: true })).toBe(version);
      expect(observer.prepare('SELECT name,sql FROM sqlite_master ORDER BY name').all()).toEqual(schema);
      expect(fs.statSync(getDatabasePath(root) + '-wal').size).toBe(walBytes);
      expect(owner.isWatching()).toBe(true);
      expect(fs.readFileSync(getWriterPidPath(root), 'utf8')).toBe(lock);
      expect(observed.events.filter(event => event.type === 'spawn')).toHaveLength(3);
      expect(observed.events.filter(event => event.type === 'exit')).toHaveLength(3);
      expect(observed.events.filter(event => event.type === 'exit' && event.code === 13)).toHaveLength(2);
      record('read-only-recovery', { root, events: observed.events, version, walBytes, schema });
    } finally { await pool.destroy(); observed.restore(); observer.close(); control.close(); owner.close(); releaseWriterLock(root); }
  }, 30000);
  it.each(['missing', 'outdated'])('leaves %s index metadata, schema and WAL untouched in parent and lazy worker opens', async metadata => {
    const alpha = await fixture('alphaSymbol');
    const beta = await fixture('betaSymbol');
    const owners = await Promise.all([CodeGraph.open(alpha), CodeGraph.open(beta)]);
    const observers = [alpha, beta].map(root => createDatabase(getDatabasePath(root), { readOnly: true }).db);
    const controls = [alpha, beta].map(root => createDatabase(getDatabasePath(root)).db);
    const snapshot = (index: number) => ({
      version: observers[index].pragma('data_version', { simple: true }),
      schema: observers[index].prepare('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name').all(),
      versions: observers[index].prepare('SELECT * FROM schema_versions').all(),
      nodes: observers[index].prepare('SELECT id,name,file_path FROM nodes ORDER BY id').all(),
      walBytes: fs.statSync(getDatabasePath([alpha, beta][index]) + '-wal').size,
    });
    for (let i = 0; i < 2; i++) {
      expect(tryAcquireWriterLock([alpha, beta][i], 'direct').kind).toBe('acquired');
      owners[i].watch({ debounceMs: 50 });
      controls[i].exec('DROP TRIGGER nodes_ai; DROP INDEX idx_nodes_kind; DELETE FROM schema_versions;');
      const ignore = path.join([alpha, beta][i], '.codegraph', '.gitignore');
      if (metadata === 'missing') fs.unlinkSync(ignore);
      else fs.writeFileSync(ignore, '# CodeGraph data files\ncodegraph.db\ncodegraph.db-wal\ncodegraph.db-shm\n');
    }
    const before = [snapshot(0), snapshot(1)];
    const lockBytes = [alpha, beta].map(root => fs.readFileSync(getWriterPidPath(root), 'utf8'));
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '2');
    vi.stubEnv('CODEGRAPH_WAL_HEAL_MB', '0.000001');
    const observed = observeWorkers();
    const reader = new BuiltEngine({ readOnly: true, queryPool: true });
    try {
      await reader.ensureInitialized(alpha);
      for (const [projectPath, query] of [[undefined, 'alphaSymbol'], [alpha, 'alphaSymbol'], [beta, 'betaSymbol']] as const) {
        const result = await reader.getToolHandler().execute('codegraph_search', { query, ...(projectPath ? { projectPath } : {}) });
        expect(result.isError).toBeFalsy();
        expect(result.content[0].text).toContain('app.ts:1');
      }
      await reader.stop();
      for (let i = 0; i < 2; i++) {
        expect(snapshot(i)).toEqual(before[i]);
        const ignore = path.join([alpha, beta][i], '.codegraph', '.gitignore');
        if (metadata === 'missing') expect(fs.existsSync(ignore)).toBe(false);
        else expect(fs.readFileSync(ignore, 'utf8')).toBe('# CodeGraph data files\ncodegraph.db\ncodegraph.db-wal\ncodegraph.db-shm\n');
        expect(fs.readFileSync(getWriterPidPath([alpha, beta][i]), 'utf8')).toBe(lockBytes[i]);
        expect(owners[i].isWatching()).toBe(true);
      }
      expect(observed.events.filter(event => event.type === 'result')).toHaveLength(3);
      record('read-only-invariants', { metadata, alpha, beta, before, after: [snapshot(0), snapshot(1)], events: observed.events });
    } finally {
      await reader.stop(); observed.restore();
      for (const db of [...observers, ...controls]) db.close();
      for (let i = 0; i < 2; i++) { owners[i].close(); releaseWriterLock([alpha, beta][i]); }
    }
  }, 30000);
  it.runIf(process.platform !== 'win32')('reopens replaced default and lazy indexes read-only without schema or metadata repair', async () => {
    const alpha = await fixture('oldAlpha');
    const beta = await fixture('oldBeta');
    const replacementAlpha = await fixture('newAlpha');
    const replacementBeta = await fixture('newBeta');
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '1');
    const observed = observeWorkers();
    const reader = new BuiltEngine({ readOnly: true, queryPool: true });
    const controls: ReturnType<typeof createDatabase>['db'][] = [];
    const observers: ReturnType<typeof createDatabase>['db'][] = [];
    try {
      await reader.ensureInitialized(alpha);
      for (const [projectPath, query] of [[undefined, 'oldAlpha'], [beta, 'oldBeta']] as const) {
        expect((await reader.getToolHandler().execute('codegraph_search', { query, ...(projectPath ? { projectPath } : {}) })).content[0].text).toContain('app.ts:1');
      }
      const before: unknown[] = [];
      for (const [index, root, replacement] of [[0, alpha, replacementAlpha], [1, beta, replacementBeta]] as const) {
        // POSIX permits replacing an open inode; Windows deliberately cannot.
        fs.renameSync(path.join(root, '.codegraph'), path.join(root, 'retained-old-index'));
        fs.cpSync(path.join(replacement, '.codegraph'), path.join(root, '.codegraph'), { recursive: true });
        fs.unlinkSync(path.join(root, '.codegraph', '.gitignore'));
        controls[index] = createDatabase(getDatabasePath(root)).db;
        controls[index].exec('DROP TRIGGER nodes_ai; DROP INDEX idx_nodes_kind; DELETE FROM schema_versions;');
        observers[index] = createDatabase(getDatabasePath(root), { readOnly: true }).db;
        before[index] = { version: observers[index].pragma('data_version', { simple: true }), schema: observers[index].prepare('SELECT name,sql FROM sqlite_master ORDER BY name').all(), walBytes: fs.statSync(getDatabasePath(root) + '-wal').size };
      }
      for (const [index, projectPath, query] of [[0, undefined, 'newAlpha'], [1, beta, 'newBeta']] as const) {
        const response = await reader.getToolHandler().execute('codegraph_search', { query, ...(projectPath ? { projectPath } : {}) });
        expect(response.isError).toBeFalsy();
        expect(response.content[0].text).toContain(query);
        expect(response.content[0].text).toContain('app.ts:1');
        expect(fs.existsSync(path.join([alpha, beta][index], '.codegraph', '.gitignore'))).toBe(false);
      }
      await reader.stop();
      const after = [alpha, beta].map((root, index) => ({ version: observers[index].pragma('data_version', { simple: true }), schema: observers[index].prepare('SELECT name,sql FROM sqlite_master ORDER BY name').all(), walBytes: fs.statSync(getDatabasePath(root) + '-wal').size }));
      expect(after).toEqual(before);
      expect(observed.events.filter(event => event.type === 'spawn')).toHaveLength(observed.events.filter(event => event.type === 'exit').length);
      record('replaced-indexes', { alpha, beta, before, after, events: observed.events });
    } finally { await reader.stop(); observed.restore(); for (const db of [...observers, ...controls]) db.close(); }
  }, 30000);
});
