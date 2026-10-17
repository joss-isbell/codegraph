import { afterAll, afterEach, beforeEach, describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as net from 'net';
import { createHash } from 'crypto';
import { spawn } from 'child_process';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { resolveMcpRoot, admitMcpRoute, captureMcpIndexRoute, assertMcpIndexRoute, McpIndexRouteUnavailableError } from '../src/directory';

// The native module is patched before the built facade/engine captures it.
const observer = require('./fixtures/query-admission-sqlite-observer.cjs') as {
  begin(roots: string[]): void; facts(): { total: number; overflow: boolean; records: unknown[] }; restore(): void;
};
const { CodeGraph } = require('../dist') as typeof import('../src');
const { MCPEngine } = require('../dist/mcp/engine') as typeof import('../src/mcp/engine');
const { acquireProject, admitQueryProject } = require('../dist/mcp/project-lifecycle') as typeof import('../src/mcp/project-lifecycle');
const { resolveQueryPolicy }: typeof import('../src/mcp/query-policy') = require('../dist/mcp/query-policy');
const { getWriterPidPath } = require('../dist/mcp/writer-lock') as typeof import('../src/mcp/writer-lock');
const { MCPSession } = require('../dist/mcp/session') as typeof import('../src/mcp/session');
const { SocketTransport } = require('../dist/mcp/transport') as typeof import('../src/mcp/transport');
const { getDaemonSocketPath } = require('../dist/mcp/daemon-paths') as typeof import('../src/mcp/daemon-paths');

const roots: string[] = [];
const engines: Array<InstanceType<typeof MCPEngine>> = [];
const facts: unknown[] = [];
const nativeCleanup: Array<() => Promise<void>> = [];
// Declared maximum per row: two native phases at 16s each, one engine at 5s,
// plus 5s joining an incomplete body and 18s for retained evidence. Cases remain 30s.
const CLEANUP_HOOK_MS = 60_000;
let fixturePoison: string | undefined;
type CaseToken = { name: string; open: boolean; cleanupStarted: boolean; bodySettled: boolean; body?: Promise<void> };
let caseToken: CaseToken = { name: 'setup', open: true, cleanupStarted: false, bodySettled: true };
function ownedBody<Args extends unknown[]>(work: (...args: Args) => Promise<void>): (...args: Args) => Promise<void> {
  return (...args) => {
    const token = caseToken;
    const pending = (async () => { try { await work(...args); } finally { token.bodySettled = true; } })();
    token.body = pending;
    void pending.catch(() => {}); // retain ownership if the framework stops awaiting a timed-out body
    return pending;
  };
}
function assertBodyActive(): void {
  if (caseToken.cleanupStarted || fixturePoison) throw new Error('Owned admission body cannot acquire resources after cleanup began');
}
const lateFacts: unknown[] = [];
let omittedLateFacts = 0;
function persistLateFacts(): void {
  try {
    if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, 'admission-routing-late-cleanup.json'),
      JSON.stringify({ poison: fixturePoison, records: lateFacts, omitted: omittedLateFacts }));
  } catch (error) { fixturePoison ??= 'Late cleanup evidence persistence failed'; process.stderr.write(`Admission late evidence failed: ${String(error).slice(0, 2048)}\n`); }
}
function retainLate(token: typeof caseToken, value: unknown): void {
  fixturePoison ??= `Late owned fixture event after ${token.name}`;
  const encoded = JSON.stringify(value);
  if (lateFacts.length < 32) lateFacts.push({ test: token.name, event: Buffer.byteLength(encoded) <= 8192 ? value : { detail: encoded.slice(0, 1024), truncated: true } });
  else omittedLateFacts++;
  persistLateFacts();
}
function recordForCase(): (value: unknown) => void {
  const token = caseToken;
  return value => {
    if (!token.open) { retainLate(token, value); return; }
    // Snapshot error arrays and native observations, so later callbacks cannot
    // alter an earlier row when the next complete facts artifact is written.
    facts.push(JSON.parse(JSON.stringify(value)));
  };
}
function registerNative(close: () => Promise<void>): void {
  nativeCleanup.push(close); // own the resource before any awaited setup
  if (nativeCleanup.length > 2) { fixturePoison = 'Native cleanup phase budget exceeded'; throw new Error(fixturePoison); }
}
beforeEach(context => {
  if (fixturePoison) throw new Error(`Admission fixture stopped after incomplete owned work: ${fixturePoison}`);
  if (caseToken.body && !caseToken.bodySettled) throw new Error('Previous owned admission body is still pending');
  caseToken = { name: context.task.name, open: true, cleanupStarted: false, bodySettled: false };
});
const sleep = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean): Promise<void> {
  const deadline = Date.now() + 5000;
  while (!check()) { if (Date.now() >= deadline) { fixturePoison = 'Owned fixture state deadline'; throw new Error(fixturePoison); } await sleep(5); }
}
function ownedSocket(socket: net.Socket, errors: string[]): Promise<void> {
  const record = recordForCase();
  socket.on('error', error => { retainError(errors, error); record({ kind: 'socket-error', detail: String(error).slice(0, 2048) }); });
  return new Promise(resolve => socket.once('close', hadError => {
    record({ kind: 'socket-close', hadError, destroyed: socket.destroyed });
    resolve();
  }));
}
function retainError(errors: string[], error: unknown): void {
  if (errors.length < 16) errors.push(String(error).slice(0, 2048));
}
async function bounded<T>(work: Promise<T>, label: string, limit = 5000): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  let expired = false;
  const token = caseToken;
  // A raced timeout does not cancel the owned operation. Fence future rows,
  // retain its eventual outcome separately, and never reuse its observer/facts.
  void work.then(() => { if (expired) retainLate(token, { kind: 'late-phase-settlement', label, outcome: 'fulfilled' }); },
    error => { if (expired) retainLate(token, { kind: 'late-phase-settlement', label, outcome: 'rejected', error: String(error).slice(0, 2048) }); });
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => { expired = true; fixturePoison = `Owned ${label} deadline`; reject(new Error(fixturePoison)); }, limit);
    })]);
  } finally { if (timer) clearTimeout(timer); }
}
async function listen(server: net.Server, address: string | number): Promise<void> {
  await bounded(new Promise<void>((resolve, reject) => {
    const failed = (error: Error) => { server.removeListener('listening', ready); reject(error); };
    const ready = () => { server.removeListener('error', failed); resolve(); };
    server.once('error', failed); server.once('listening', ready);
    if (typeof address === 'number') server.listen(address, '127.0.0.1'); else server.listen(address);
  }), 'listen');
}
async function closeResources(server: net.Server, sockets: net.Socket[], closes: Promise<void>[], errors: string[]): Promise<void> {
  const serverClosed = server.listening
    ? new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()))
    : Promise.resolve();
  for (const socket of sockets) { try { socket.destroy(); } catch (error) { retainError(errors, error); } }
  const results = await Promise.allSettled([bounded(Promise.all(closes), 'socket close'), bounded(serverClosed, 'server close')]);
  for (const result of results) if (result.status === 'rejected') retainError(errors, result.reason);
}
async function blackhole(root: string): Promise<{ accepted: () => number; close: () => Promise<void> }> {
  assertBodyActive();
  const record = recordForCase();
  const sockets: net.Socket[] = [], closes: Promise<void>[] = [], errors: string[] = [];
  const server = net.createServer(socket => { sockets.push(socket); closes.push(ownedSocket(socket, errors)); socket.on('data', () => {}); });
  server.on('error', error => retainError(errors, error));
  let closePromise: Promise<void> | undefined;
  const close = () => closePromise ??= (async () => {
    await closeResources(server, sockets, closes, errors);
    record({ kind: 'negative-no-hello-peer', root, accepted: sockets.length, ownedSocketCount: closes.length, errors });
    expect(errors).toEqual([]);
  })();
  registerNative(close);
  await listen(server, getDaemonSocketPath(root));
  return { accepted: () => sockets.length, close };
}
async function endpoint(value: InstanceType<typeof MCPEngine>, root: string) {
  assertBodyActive();
  const record = recordForCase();
  const sockets: net.Socket[] = [], closes: Promise<void>[] = [], sessions: Array<InstanceType<typeof MCPSession>> = [];
  const errors: string[] = [], replies: Array<{ id: number; result?: any; error?: any }> = [];
  const server = net.createServer(socket => {
    sockets.push(socket); closes.push(ownedSocket(socket, errors));
    try {
      const session = new MCPSession(new SocketTransport(socket), value, { explicitProjectPath: root });
      sessions.push(session); session.start();
    } catch (error) { retainError(errors, error); socket.destroy(); }
  });
  server.on('error', error => retainError(errors, error));
  let stopped = false;
  registerNative(async () => {
    if (stopped) return; stopped = true;
    const secondary: string[] = [];
    for (const session of sessions) { try { session.stop(); } catch (error) { retainError(secondary, error); } }
    await closeResources(server, sockets, closes, errors);
    if (buffer.length) retainError(errors, 'Owned RPC capture ended with an incomplete frame');
    record({ kind: 'real-session', ownedSocketCount: closes.length, errors, secondary, replies: replies.length, rpcSeenBytes: seen,
      histories: sessions.map(session => session.getExploreSessionState().snapshot()) });
    expect(errors).toEqual([]); expect(secondary).toEqual([]);
  });
  let buffer = Buffer.alloc(0), seen = 0;
  await listen(server, 0);
  assertBodyActive();
  const client = net.createConnection((server.address() as net.AddressInfo).port, '127.0.0.1');
  sockets.push(client); closes.push(ownedSocket(client, errors));
  client.on('data', chunk => {
    seen += chunk.length;
    if (seen > 1024 * 1024 || buffer.length + chunk.length > 65536) { retainError(errors, 'Owned RPC capture overflow'); client.destroy(); return; }
    buffer = Buffer.concat([buffer, chunk]); let end: number;
    while ((end = buffer.indexOf(10)) !== -1) {
      const line = buffer.subarray(0, end); buffer = buffer.subarray(end + 1);
      try { if (replies.length >= 64) throw new Error('Owned RPC reply bound'); replies.push(JSON.parse(line.toString('utf8'))); }
      catch (error) { retainError(errors, error); }
    }
  });
  await until(() => sessions.length === 1);
  return { sessions, async request(id: number, method: string, params?: unknown) {
    client.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
    await until(() => errors.length > 0 || replies.some(reply => reply.id === id));
    expect(errors).toEqual([]); return replies.find(reply => reply.id === id)!;
  } };
}
function proxyChild(root: string, hint = root) {
  assertBodyActive();
  const record = recordForCase();
  const child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, path.join(__dirname, 'fixtures/query-admission-sqlite-observer.cjs'), root, hint],
    { stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, CODEGRAPH_NO_UPDATE_CHECK: '1', CODEGRAPH_DAEMON_RETRY_MS: '20', CODEGRAPH_DAEMON_RETRY_MAX_MS: '20' } });
  const errors: string[] = [], secondary: string[] = [], replies: any[] = [];
  const output = { stdout: Buffer.alloc(0), stderr: Buffer.alloc(0) }, seen = { stdout: 0, stderr: 0 };
  let line = Buffer.alloc(0), force = false;
  const persistRaw = () => {
    for (const stream of ['stdout', 'stderr'] as const) {
      try { fs.writeFileSync(path.join(root, `proxy-${stream}.raw`), output[stream]); }
      catch (error) { retainError(secondary, error); record({ kind: 'proxy-raw-persistence-error', root, stream, error: String(error).slice(0, 2048) }); }
    }
  };
  child.on('error', error => { retainError(errors, error); record({ kind: 'proxy-error', detail: String(error).slice(0, 2048) }); });
  child.stdin.on('error', error => { retainError(errors, error); record({ kind: 'proxy-stdin-error', detail: String(error).slice(0, 2048) }); });
  const closed = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>(resolve => child.once('close', (code, signal) => {
    persistRaw(); // actual late native close still retains the final bounded bytes in this unique root
    record({ kind: 'proxy-native-close', root, pid: child.pid, code, signal, seen, retained: { stdout: output.stdout.length, stderr: output.stderr.length }, errors, secondary });
    resolve({ code, signal });
  }));
  for (const stream of ['stdout', 'stderr'] as const) child[stream].on('data', (chunk: Buffer) => {
    seen[stream] += chunk.length;
    output[stream] = Buffer.concat([output[stream], chunk.subarray(0, Math.max(0, 65536 - output[stream].length))]);
    if (seen[stream] > 65536) retainError(errors, `Owned proxy ${stream} overflow`);
    if (stream !== 'stdout' || errors.length) return;
    line = Buffer.concat([line, chunk]); let end: number;
    while ((end = line.indexOf(10)) !== -1) {
      const raw = line.subarray(0, end); line = line.subarray(end + 1);
      try {
        if (replies.length >= 16) throw new Error('Owned proxy reply bound');
        replies.push(JSON.parse(raw.toString('utf8')));
      } catch (error) { retainError(errors, error); }
    }
  });
  const close = async () => {
    let native: { code: number | null; signal: NodeJS.Signals | null } | undefined;
    try { child.stdin.end(); native = await bounded(closed, 'proxy close'); }
    catch (error) {
      retainError(secondary, error); force = true;
      try { child.kill(); } catch (forceError) { retainError(secondary, forceError); }
      try { native = await bounded(closed, 'forced proxy close'); } catch (closeError) { retainError(secondary, closeError); }
    }
    if (line.length) retainError(errors, 'Owned proxy incomplete stdout frame');
    persistRaw(); // retain current bounded bytes even if native close remains unproved
    record({ kind: 'proxy-child', root, hint, pid: child.pid, native, force, seen,
      retained: { stdout: output.stdout.length, stderr: output.stderr.length }, errors, secondary });
    expect(errors).toEqual([]); expect(secondary).toEqual([]); expect(force).toBe(false);
    expect(native).toEqual({ code: 0, signal: null });
  };
  let closePromise: Promise<void> | undefined;
  const stop = () => closePromise ??= close();
  registerNative(stop);
  return { replies, stop, send(message: unknown) { child.stdin.write(JSON.stringify(message) + '\n'); },
    async response(id: number) {
      await until(() => errors.length > 0 || replies.some(reply => reply.id === id));
      expect(errors).toEqual([]); return replies.find(reply => reply.id === id)!;
    } };
}
function temp(): string {
  assertBodyActive();
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-admission-routing-')));
  roots.push(root); return root;
}
async function indexed(root: string): Promise<void> {
  assertBodyActive();
  fs.mkdirSync(root, { recursive: true });
  fs.writeFileSync(path.join(root, 'app.ts'), 'export function admittedSymbol() { return 7; }\n');
  const cg = await CodeGraph.init(root);
  try { assertBodyActive(); await cg.indexAll(); } finally { cg.close(); }
}
function membership(root: string): Record<string, string> {
  const dir = path.join(root, '.codegraph');
  return Object.fromEntries(fs.readdirSync(dir).sort().filter(name => fs.statSync(path.join(dir, name)).isFile())
    .map(name => [name, createHash('sha256').update(fs.readFileSync(path.join(dir, name))).digest('hex')]));
}
function unknownOwner(root: string): void {
  fs.writeFileSync(getWriterPidPath(root), JSON.stringify({ pid: process.pid, mode: 'daemon', startedAt: Date.now() }));
}
function engine(): InstanceType<typeof MCPEngine> {
  assertBodyActive();
  const value = new MCPEngine({ readOnly: true, watch: false, queryPool: false });
  engines.push(value);
  if (engines.length > 1) { fixturePoison = 'Engine cleanup phase budget exceeded'; throw new Error(fixturePoison); }
  return value;
}
function assertNoOpen(root: string, before: Record<string, string>): void {
  expect(observer.facts()).toMatchObject({ total: 0, overflow: false });
  expect(membership(root)).toEqual(before); // WAL/SHM and owner records included.
}
afterEach(async context => {
  if (!caseToken.open) return; // a poisoned later row never began fixture work
  const cleanup: string[] = [];
  const incompleteBody = caseToken.body && !caseToken.bodySettled ? caseToken.body : undefined;
  // A framework timeout can begin this hook before any local phase bound fires.
  // Poison synchronously before cleanup awaits, so no next observer lifetime can start.
  if (incompleteBody) fixturePoison ??= `Framework ended ${caseToken.name} with its owned body still pending`;
  caseToken.cleanupStarted = true;
  for (const close of nativeCleanup.splice(0)) { try { await bounded(close(), 'native cleanup phase', 16_000); } catch (error) { retainError(cleanup, error); } }
  for (const value of engines.splice(0)) { try { await bounded(value.stop(), 'engine stop'); } catch (error) { retainError(cleanup, error); } }
  if (incompleteBody) { try { await bounded(incompleteBody, 'incomplete test body settlement'); } catch (error) { retainError(cleanup, error); } }
  const retained = roots.splice(0).map(root => {
    try { return { root, index: fs.existsSync(path.join(root, '.codegraph')) ? membership(root) : null }; }
    catch (error) { retainError(cleanup, error); return { root, error: String(error).slice(0, 2048) }; }
  });
  facts.push({ test: context.task.name, primary: context.task.result?.errors?.slice(0, 4).map(error => String(error.message).slice(0, 2048)),
    roots: retained, sqlite: observer.facts(), cleanup, poison: fixturePoison, incompleteBodyAtCleanup: !!incompleteBody, bodySettled: caseToken.bodySettled, cleanupHookMs: CLEANUP_HOOK_MS });
  caseToken.open = false;
  // Retain every disposable index and exact failure membership for inspection.
  try {
    if (process.env.VALIDATION_OUT) {
      const encoded = JSON.stringify(facts);
      if (Buffer.byteLength(encoded) > 1024 * 1024) throw new Error('Admission fixture facts exceeded 1 MiB');
      fs.writeFileSync(path.join(process.env.VALIDATION_OUT, 'admission-routing-facts.json'), encoded);
    }
  } catch (error) {
    retainError(cleanup, error);
    // A bounded fallback cannot replace the primary Vitest failure or skip cleanup.
    process.stderr.write(`Admission fixture evidence persistence failed: ${String(error).slice(0, 2048)}\n`);
  }
  persistLateFacts();
  expect(cleanup).toEqual([]);
  expect(observer.facts().overflow).toBe(false);
  expect(fixturePoison).toBeUndefined();
}, CLEANUP_HOOK_MS);
afterAll(() => {
  if (caseToken.body && !caseToken.bodySettled) retainLate(caseToken, { kind: 'body-unsettled-at-suite-end', sqlite: observer.facts() });
  observer.restore();
});

async function proxyRefusalProof(root: string, hint = root): Promise<void> {
  const before = membership(root), child = proxyChild(root, hint);
  child.send({ jsonrpc: '2.0', id: 1, method: 'initialize', params: { capabilities: {} } });
  child.send({ jsonrpc: '2.0', id: 100, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query: 'admittedSymbol' } } });
  child.send({ jsonrpc: '2.0', id: 101, method: 'tools/call', params: { name: 'codegraph_search', arguments: { query: 'admittedSymbol' } } });
  expect((await child.response(1)).result).toBeDefined();
  for (const id of [100, 101]) expect((await child.response(id)).error.data).toMatchObject({ reason: 'unknown', projectRoot: root });
  // Observe six configured 20ms retry opportunities after terminal acquisition;
  // this is a no-retry fixture window, never a healthy query latency contract.
  const quietStart = Date.now(); await sleep(120);
  child.send({ jsonrpc: '2.0', id: 102, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query: 'admittedSymbol' } } });
  expect((await child.response(102)).error.data).toMatchObject({ reason: 'unknown', projectRoot: root });
  const quietElapsedMs = Date.now() - quietStart;
  await child.stop();
  for (const id of [100, 101, 102]) expect(child.replies.filter(reply => reply.id === id)).toHaveLength(1);
  const observedPath = path.join(root, 'proxy-admission-facts.json');
  expect(fs.statSync(observedPath).size).toBeLessThanOrEqual(65536);
  const observed = JSON.parse(fs.readFileSync(observedPath, 'utf8'));
  recordForCase()({ kind: 'proxy-native-admission', root, hint, quietElapsedMs, configuredRetryMs: 20, observed });
  expect(observed).toMatchObject({ code: 0, root, hint, factories: 1, calls: 0, historyRecords: 0, captureError: null,
    sqlite: { total: 0, overflow: false } });
  expect(observed.firstRefusal).toMatchObject({ factories: 1, calls: 0, historyRecords: 0 });
  expect(observed.firstRefusal.acquisitions).toBeGreaterThanOrEqual(1);
  expect(observed.acquisitions).toBe(observed.firstRefusal.acquisitions);
  expect(membership(root)).toEqual(before);
}

describe('MCP admission precedes native SQLite and holds its own route', () => {
  it.each(['explicit', 'default', 'retry'] as const)('%s unknown owner refuses without any native constructor or sidecar', ownedBody(async (mode: 'explicit' | 'default' | 'retry') => {
    const root = temp(); await indexed(root); unknownOwner(root);
    const before = membership(root), value = engine(); observer.begin([root]);
    const work = mode === 'explicit'
      ? value.getToolHandler().execute('codegraph_search', { query: 'admittedSymbol', projectPath: root })
      : mode === 'default' ? value.ensureInitialized(root) : value.retryInitialize(root);
    await expect(work).rejects.toMatchObject({ code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root });
    assertNoOpen(root, before);
  }), 30_000);

  it('a speculative default refusal leaves an independent permitted explicit target usable', ownedBody(async () => {
    const base = temp(), alpha = path.join(base, 'alpha'), beta = path.join(base, 'beta');
    await indexed(alpha); await indexed(beta); unknownOwner(alpha);
    const before = membership(alpha), value = engine(); observer.begin([base]);
    await expect(value.ensureInitialized(alpha)).rejects.toMatchObject({ reason: 'unknown' });
    expect(observer.facts().total).toBe(0);
    const result = await value.getToolHandler().execute('codegraph_search', { query: 'admittedSymbol', projectPath: beta });
    expect(result.isError).not.toBe(true); expect(result.content[0].text).toContain('admittedSymbol');
    expect(membership(alpha)).toEqual(before);
    expect(observer.facts().records.every((entry: any) => entry.file.startsWith(beta + path.sep))).toBe(true);
  }), 30_000);

  it('real session initialize/list answers while native default permission is pending; explicit beta stays independent', ownedBody(async () => {
    const base = temp(), alpha = path.join(base, 'alpha'), beta = path.join(base, 'beta');
    await indexed(alpha); await indexed(beta); unknownOwner(alpha);
    const peer = await blackhole(alpha), before = membership(alpha), value = engine();
    observer.begin([base]);
    const wire = await endpoint(value, alpha);
    const initialized = await wire.request(1, 'initialize', { capabilities: {} });
    expect(initialized.result.instructions).toMatch(/verification is pending/);
    await until(() => peer.accepted() > 0);
    expect((await wire.request(2, 'tools/list')).result.tools.length).toBeGreaterThan(0);
    assertNoOpen(alpha, before); // native permission has not completed
    await peer.close(); // genuine native EOF now makes the default unknown
    const betaResult = await wire.request(3, 'tools/call', { name: 'codegraph_explore', arguments: { projectPath: beta, query: 'admittedSymbol' } });
    expect(betaResult.error).toBeUndefined(); expect(betaResult.result.isError).not.toBe(true);
    expect(betaResult.result.content[0].text).toContain('admittedSymbol');
    const refused = await wire.request(4, 'tools/call', { name: 'codegraph_search', arguments: { query: 'admittedSymbol' } });
    expect(refused.error.data).toMatchObject({ reason: 'unknown', projectRoot: alpha });
    expect(membership(alpha)).toEqual(before);
    expect(wire.sessions[0]!.getExploreSessionState().callCount(beta)).toBe(1);
    expect(wire.sessions[0]!.getExploreSessionState().callCount(alpha)).toBe(0);
  }), 30_000);

  it('real stdio proxy retains factory initialization refusal and settles initial/new requests once without SQLite', ownedBody(async () => {
    const root = temp(); await indexed(root); unknownOwner(root);
    await proxyRefusalProof(root);
  }), 30_000);

  it.each(['workspace-child', 'subfolder-ancestor'] as const)('real stdio %s hint latches its resolved initial refusal, without later factory or retry work', ownedBody(async (mode: 'workspace-child' | 'subfolder-ancestor') => {
    const base = temp(), root = path.join(base, 'indexed'); await indexed(root); unknownOwner(root);
    const hint = mode === 'workspace-child' ? base : path.join(root, 'subfolder');
    if (mode === 'workspace-child') fs.mkdirSync(path.join(base, '.git')); // filesystem-only eligible workspace marker
    if (mode === 'subfolder-ancestor') fs.mkdirSync(hint);
    expect(hint).not.toBe(root);
    await proxyRefusalProof(fs.realpathSync(root), fs.realpathSync(hint));
  }), 30_000);

  it('an initialized default rejects an index selector change before any new-target SQLite or ancestor route', ownedBody(async () => {
    const base = temp(), root = path.join(base, 'child'), replacement = temp();
    await indexed(base); await indexed(root); await indexed(replacement);
    const value = engine(); await value.ensureInitialized(root);
    const first = await value.getToolHandler().execute('codegraph_search', { query: 'admittedSymbol' });
    expect(first.isError).not.toBe(true); expect(first.content[0].text).toContain('admittedSymbol');
    const alternative = '.codegraph-selector-change';
    for (const target of [base, root]) {
      fs.mkdirSync(path.join(target, alternative));
      fs.copyFileSync(path.join(replacement, '.codegraph/codegraph.db'), path.join(target, alternative, 'codegraph.db'));
    }
    const alternativeMembership = (target: string) => Object.fromEntries(fs.readdirSync(path.join(target, alternative)).sort()
      .map(name => [name, createHash('sha256').update(fs.readFileSync(path.join(target, alternative, name))).digest('hex')]));
    const before = { original: membership(root), child: alternativeMembership(root), ancestor: alternativeMembership(base) };
    const previous = process.env.CODEGRAPH_DIR;
    try {
      process.env.CODEGRAPH_DIR = alternative; observer.begin([base]);
      const answer = await value.getToolHandler().execute('codegraph_search', { query: 'admittedSymbol' });
      expect(answer.isError).not.toBe(true);
      expect(answer.content[0].text).toMatch(/verified own codegraph index \(changed\)/);
      expect(answer.content[0].text).toContain('cannot use an ancestor index');
      expect(observer.facts()).toMatchObject({ total: 0, overflow: false });
      expect({ original: membership(root), child: alternativeMembership(root), ancestor: alternativeMembership(base) }).toEqual(before);
      recordForCase()({ kind: 'initialized-default-selector-change', root, ancestor: base, alternative,
        guidance: answer.content[0].text, sqlite: observer.facts(), exactMembership: before });
    } finally { if (previous === undefined) delete process.env.CODEGRAPH_DIR; else process.env.CODEGRAPH_DIR = previous; }
  }), 30_000);

  it('stop during a real pending native permission attempt opens no graph after closure', ownedBody(async () => {
    const root = temp(); await indexed(root); unknownOwner(root);
    const peer = await blackhole(root), before = membership(root), value = engine(); observer.begin([root]);
    const pending = value.ensureInitialized(root);
    void pending.catch(() => {}); // own failure before stopping the engine
    await until(() => peer.accepted() > 0);
    const stopped = value.stop();
    await peer.close();
    await stopped;
    await pending.catch(() => {});
    expect(value.hasDefaultCodeGraph()).toBe(false); assertNoOpen(root, before);
  }), 30_000);

  it('cancellation during actual permission prevents schema, graph work and history', ownedBody(async () => {
    const root = temp(); await indexed(root); unknownOwner(root);
    const peer = await blackhole(root), before = membership(root), value = engine();
    const controller = new AbortController(); observer.begin([root]);
    const pending = value.getToolHandler().execute('codegraph_explore', { projectPath: root, query: 'admittedSymbol' },
      undefined, undefined, { signal: controller.signal });
    await until(() => peer.accepted() > 0);
    controller.abort(); await peer.close();
    const result = await pending;
    expect(result.isError).not.toBe(true); expect(result.content[0].text).toMatch(/cancelled or abandoned/);
    assertNoOpen(root, before);
  }), 30_000);

  it('concurrent retry and initial callers share one real pending permission attempt', ownedBody(async () => {
    const root = temp(); await indexed(root); unknownOwner(root);
    const peer = await blackhole(root), before = membership(root), value = engine(); observer.begin([root]);
    const initial = value.ensureInitialized(root), retry = value.retryInitialize(root);
    const outcomes = Promise.allSettled([initial, retry]);
    await until(() => peer.accepted() > 0); await peer.close();
    const settled = await outcomes;
    expect(peer.accepted()).toBe(1);
    expect(settled.every(result => result.status === 'rejected' && (result.reason as any).reason === 'unknown')).toBe(true);
    assertNoOpen(root, before);
  }), 30_000);

  it('refreshes permission after actual grammar preparation before the captured synchronous open', ownedBody(async () => {
    const root = temp(); await indexed(root); const value = engine();
    const extraction = require('../dist/extraction/grammars') as typeof import('../src/extraction/grammars');
    const original = extraction.initGrammars;
    let entered = false, before: Record<string, string> = {};
    extraction.initGrammars = async () => {
      await original(); // actual preparation/result unchanged
      unknownOwner(root); before = membership(root); observer.begin([root]); entered = true;
    };
    try {
      await expect(value.ensureInitialized(root)).rejects.toMatchObject({ reason: 'unknown', projectRoot: root });
      expect(entered).toBe(true); assertNoOpen(root, before);
      recordForCase()({ kind: 'post-grammar-owner-change', root, entered, sqlite: observer.facts() });
    } finally { extraction.initGrammars = original; }
  }), 30_000);

  it('filesystem capture/assert and admitted valid replacement retain exactly the chosen own route', ownedBody(async () => {
    const root = temp(), replacement = temp(); await indexed(root); await indexed(replacement);
    const route = captureMcpIndexRoute(root)!;
    observer.begin([root]); assertMcpIndexRoute(route);
    expect(Object.isFrozen(route)).toBe(true); expect(observer.facts().total).toBe(0);
    fs.copyFileSync(path.join(replacement, '.codegraph/codegraph.db'), route.index);
    const held = await admitMcpRoute(route, { admit: async candidate => {
      expect(Object.isFrozen(candidate)).toBe(true); expect(candidate).toEqual(route);
    } });
    expect(held).toEqual(route);
    const value = engine();
    const answer = await value.getToolHandler().execute('codegraph_search', { projectPath: root, query: 'admittedSymbol' });
    expect(answer.isError).not.toBe(true); expect(answer.content[0].text).toContain('admittedSymbol');
  }), 30_000);

  it('unknown nearer permission stops before inspecting it or a valid ancestor', ownedBody(async () => {
    const root = temp(); await indexed(root);
    const child = path.join(root, 'child'); fs.mkdirSync(path.join(child, '.codegraph'), { recursive: true });
    fs.writeFileSync(path.join(child, '.codegraph/codegraph.db'), Buffer.alloc(200));
    const called: string[] = []; observer.begin([root]);
    const refusal = Object.assign(new Error('unknown owner'), { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown' });
    await expect(resolveMcpRoot(child, { subprojectScan: false, admit: async route => { called.push(route.root); throw refusal; } })).rejects.toBe(refusal);
    expect(called).toEqual([fs.realpathSync(child)]); expect(observer.facts().total).toBe(0);
  }));

  it('only an admitted conclusively invalid nearer database permits nearest ancestor selection', ownedBody(async () => {
    const root = temp(); await indexed(root);
    const child = path.join(root, 'child'); fs.mkdirSync(path.join(child, '.codegraph'), { recursive: true });
    fs.writeFileSync(path.join(child, '.codegraph/codegraph.db'), Buffer.alloc(200));
    const called: string[] = []; observer.begin([root]);
    const result = await resolveMcpRoot(child, { subprojectScan: false, admit: async route => { called.push(route.root); } });
    expect(result.route?.root).toBe(root); expect(called).toEqual([fs.realpathSync(child), root]);
    expect(observer.facts().total).toBeGreaterThan(0);
  }));

  it.each(['cancel', 'changed', 'missing'] as const)('%s during pending permission never opens or selects an ancestor', ownedBody(async (action: 'cancel' | 'changed' | 'missing') => {
    const root = temp(); await indexed(root);
    const child = path.join(root, 'child'); await indexed(child);
    const route = { root: fs.realpathSync(child), index: fs.realpathSync(path.join(child, '.codegraph/codegraph.db')) };
    const controller = new AbortController(); let release!: () => void;
    const gate = new Promise<void>(done => { release = done; }); observer.begin([root]);
    const pending = admitMcpRoute(route, { signal: controller.signal, admit: async () => { await gate; } });
    if (action === 'cancel') controller.abort();
    else if (action === 'missing') fs.unlinkSync(route.index);
    else { const oldDir = path.join(child, '.codegraph'); fs.renameSync(oldDir, oldDir + '-retained'); fs.mkdirSync(oldDir); }
    release();
    if (action === 'cancel') await expect(pending).rejects.toMatchObject({ name: 'AbortError' });
    else await expect(pending).rejects.toBeInstanceOf(McpIndexRouteUnavailableError);
    expect(observer.facts().total).toBe(0);
  }));

  it('policy-neutral freshness rejects a newly unknown owner before any schema constructor', ownedBody(async () => {
    const root = temp(); await indexed(root);
    const held = captureMcpIndexRoute(root); if (!held) throw new Error('Freshness fixture missing own route');
    const lifetime = new AbortController(), policy = resolveQueryPolicy({ poolSize: '0' }, 'shared-daemon', 3);
    const lease = await acquireProject({ route: held, intent: 'watch', lifetime: lifetime.signal, watchOptions: {},
      admit: (selected, signal) => admitQueryProject(selected.root, policy, signal) });
    try {
      unknownOwner(root); const before = membership(root); observer.begin([root]);
      await expect(lease.ready()).rejects.toMatchObject({ reason: 'unknown' });
      assertNoOpen(root, before);
    } finally { await lease.release(); }
  }), 30_000);
});
