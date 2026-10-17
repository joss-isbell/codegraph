/**
 * Shared MCP daemon — issue #411.
 *
 * Validates the daemon architecture in `src/mcp/{daemon,proxy,session,index}.ts`
 * AFTER the review fixes:
 *
 *   - The daemon is a *detached* background process; every `serve --mcp`
 *     invocation is a thin proxy to it. Two invocations against one project
 *     share ONE daemon.
 *   - Concurrent launchers converge on a single daemon (the must-fix-1
 *     lockfile-race: an empty-pidfile window used to let a racing candidate
 *     delete the winner's lock → two daemons).
 *   - Killing the launcher that spawned the daemon does NOT take the daemon
 *     down — other attached clients keep working (the must-fix-2 detach: the
 *     in-process daemon used to die with its launcher's process group and
 *     orphan on host SIGKILL, regressing #277).
 *   - A stale lockfile (dead pid) is cleared; `CODEGRAPH_NO_DAEMON=1` opts out;
 *     the proxy refuses to attach across a version mismatch; the daemon
 *     idle-times-out after the last client leaves (so a single session can't
 *     leak a daemon forever).
 *
 * These tests intentionally spawn real `node dist/bin/codegraph.js` processes
 * over real sockets/pipes — the same surface a Claude Code / Cursor / Codex
 * install exercises. The daemon logs to `.codegraph/daemon.log` (it has no
 * client stderr of its own), so daemon-side assertions read that file.
 *
 * `realRoot` vs `tempDir`: processes are spawned with the (possibly symlinked)
 * `tempDir` as cwd/rootUri — on macOS `os.tmpdir()` lives under `/var`, a
 * symlink to `/private/var`, and a spawned child's `process.cwd()` is already
 * realpath'd. The daemon canonicalizes the root with `realpathSync`, so all
 * path assertions use `realRoot` (the canonical form). That this matches end to
 * end is itself the proof the canonicalization works.
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ChildProcessWithoutNullStreams, spawn } from 'child_process';
import * as fs from 'fs';
import * as net from 'net';
import * as os from 'os';
import * as path from 'path';
import { CodeGraph } from '../src';
import { getDaemonSocketPath } from '../src/mcp/daemon-paths';
import { CodeGraphPackageVersion } from '../src/mcp/version';
import { once } from 'events';
import { performance } from 'perf_hooks';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { createHash } from 'crypto';
import { TextDecoder } from 'util';

const BIN = path.resolve(__dirname, '../dist/bin/codegraph.js');

// PRIVATE UNRUN proposal against the forthcoming accepted startup join.
// Original 26 rows + 8 additive rows; combined shared/RO positive histories
// stay required under #31→15, and their current exact controls refuse early.
// Native observations are delegated and bounded; roots
// and lossless raw captures are always retained, independently of native cleanup.
const OBSERVER = path.resolve(__dirname, 'fixtures/query-daemon-reconciliation.cjs');
const MAX_STREAM = 8 * 1024 * 1024, MAX_TRACE_TOTAL = 64 * 1024 * 1024;
type NativeClose = { code: number | null; signal: NodeJS.Signals | null };
interface SpawnedServer {
  child: ChildProcessWithoutNullStreams; stdout: string[]; stderr: string[];
  closed: Promise<NativeClose>; close?: NativeClose; stop: () => Promise<void>;
  assertCapture: () => void; persistCapture: (phase: string) => void; expectedSignal?: NodeJS.Signals;
}
interface CaseEvidence {
  root: string; directory: string; trace: string; control: string; roots: string[];
  observations: any[]; secondary: string[]; cancelTickers: Set<() => void>; row?: string; primary?: unknown; closing?: boolean;
}
let current: CaseEvidence, fixturePoison: string | undefined, activeCase = false;
const tracked = new Map<ChildProcessWithoutNullStreams, SpawnedServer>();
const ordinaryOwners: Array<{ child: ChildProcessWithoutNullStreams; server: SpawnedServer; root: string; stop?: Promise<void> }> = [];
const peerSockets = new Set<net.Socket>(), peerServers = new Set<net.Server>();
const ownedSockets = new Map<net.Socket, Promise<void>>();
function secondary(message: string, evidence = current): void {
  if (evidence.secondary.length < 64) evidence.secondary.push(message.slice(0, 2048));
  else { evidence.secondary[63] = 'Secondary diagnostic overflow'; fixturePoison ||= 'Secondary diagnostic overflow'; }
}
const sha = (value: Buffer | string) => createHash('sha256').update(value).digest('hex');
function bounded(file: string, maximum = MAX_STREAM): Buffer {
  let fd: number | undefined, primary: unknown, value: Buffer | undefined;
  try {
    fd = fs.openSync(file, 'r'); const size = fs.fstatSync(fd).size;
    if (size > maximum) throw new Error(`Bounded record overflow: ${file}`);
    value = Buffer.alloc(size);
    if (fs.readSync(fd, value, 0, size, 0) !== size) throw new Error(`Short read: ${file}`);
  } catch (error) { primary = error; }
  if (fd !== undefined) try { fs.closeSync(fd); } catch (error) {
    secondary(`record-close: ${String(error)}`); primary ||= error;
  }
  if (primary) throw primary;
  return value!;
}
function trace(): any[] {
  let total = 0; const result: any[] = [];
  const files = fs.readdirSync(current.trace).filter(file => /\.jsonl$/.test(file));
  if (files.length > 128) throw new Error('Native trace file-count overflow');
  for (const file of files) {
    const raw = bounded(path.join(current.trace, file)); total += raw.length;
    if (total > MAX_TRACE_TOTAL) throw new Error('Native trace total overflow');
    for (const line of raw.toString('utf8').split(/\r?\n/).filter(Boolean)) result.push(JSON.parse(line));
  }
  const failures = fs.readdirSync(current.trace).filter(file => /\.failure$/.test(file));
  if (failures.length) throw new Error(failures.map(file => bounded(path.join(current.trace, file), 4096).toString()).join('\n'));
  return result;
}
function observeChild(child: ChildProcessWithoutNullStreams, label: string): SpawnedServer {
  const stdout: string[] = [], stderr: string[] = [], errors: string[] = [];
  const streams = [child.stdout, child.stderr] as const;
  const raw = [Buffer.alloc(0), Buffer.alloc(0)], seen = [0, 0], eof = [false, false], invalid = [false, false];
  // Fatal streaming validation preserves split code points and rejects dangling
  // or malformed bytes on BOTH pipes. Raw prefixes never depend on decoding.
  const decoders = [new TextDecoder('utf-8', { fatal: true }), new TextDecoder('utf-8', { fatal: true })];
  const pending = ['', ''];
  const evidence = current, root = evidence.directory;
  const captureError = (message: string) => { if (errors.length < 64) errors.push(message.slice(0, 2048)); else { errors[63] = 'Capture diagnostic overflow'; fixturePoison ||= errors[63]; } };
  let close: NativeClose | undefined, closePromise: Promise<void> | undefined, retentionStarted = false, persistenceReported = false;
  const persistCapture = (phase: string): void => {
    retentionStarted = true;
    const rawFallbacks: Array<{ stream: string; file: string; encoding: 'base64'; bytes: number; sha256: string; data: string }> = [];
    for (let index = 0; index < 2; index++) {
      const stream = index ? 'stderr' : 'stdout', file = `${label}-${child.pid}-${stream}.bin`;
      try { fs.writeFileSync(path.join(root, file), raw[index]); }
      catch (error) {
        captureError(`raw persistence: ${String(error)}`); // Sticky failure even if the independent fallback succeeds.
        // A different durable file carries the ACTUAL bounded bytes, not just a
        // hash or a retry of the failed path. Recover with Buffer.from(data, 'base64').
        // Each 8MiB prefix expands to at most 11184812 ASCII bytes; two stay
        // below 24MiB with bounded metadata, within the existing 64MiB file cap.
        rawFallbacks.push({ stream, file, encoding: 'base64', bytes: raw[index].length, sha256: sha(raw[index]), data: raw[index].toString('base64') });
      }
    }
    try {
      fs.writeFileSync(path.join(root, `${label}-${child.pid}-capture.json`), JSON.stringify({ phase, label, pid: child.pid,
        close: close ?? null, eof: eof.slice(), invalid: invalid.slice(), seen: seen.slice(),
        retained: raw.map(bytes => bytes.length), sha256: raw.map(sha), rawFallbacks, errors: errors.slice() }, null, 2));
    } catch (error) {
      captureError(`capture facts persistence${rawFallbacks.length ? ' / raw prefix fallback INCOMPLETE' : ''}: ${String(error)}`);
      if (!persistenceReported) { persistenceReported = true; process.stderr.write(`Owned capture persistence failed: ${String(error).slice(0, 2048)}\n`); }
    }
  };
  const decode = (index: number, chunk?: Buffer): void => {
    if (invalid[index] || seen[index] > MAX_STREAM) return;
    try { pending[index] += chunk === undefined ? decoders[index].decode() : decoders[index].decode(chunk, { stream: true }); }
    catch (error) { invalid[index] = true; captureError(`${index ? 'stderr' : 'stdout'} invalid/incomplete UTF-8: ${String(error)}`); return; }
    let lf; while ((lf = pending[index].indexOf('\n')) >= 0) {
      (index === 0 ? stdout : stderr).push(pending[index].slice(0, lf));
      pending[index] = pending[index].slice(lf + 1);
    }
  };
  streams.forEach((stream, index) => {
    stream.on('data', (chunk: Buffer) => {
      seen[index] += chunk.length;
      const room = Math.max(0, MAX_STREAM - raw[index].length);
      raw[index] = Buffer.concat([raw[index], chunk.subarray(0, room)]);
      if (seen[index] > MAX_STREAM && !errors.includes('capture overflow')) captureError('capture overflow');
      decode(index, chunk); // Overflow/malformed streams still drain every native byte.
      if (retentionStarted) persistCapture('late native data');
    });
    stream.once('end', () => {
      eof[index] = true; decode(index);
      if (index === 0 && pending[index]) captureError('stdout incomplete final RPC frame');
      if (pending[index]) (index === 0 ? stdout : stderr).push(pending[index]);
      pending[index] = '';
      if (retentionStarted) persistCapture('actual native EOF');
    });
    stream.on('error', error => { captureError(`${index === 0 ? 'stdout' : 'stderr'}: ${String(error)}`); if (retentionStarted) persistCapture('late native stream error'); });
  });
  child.on('error', error => { captureError(`spawn: ${String(error)}`); if (retentionStarted) persistCapture('late spawn error'); });
  child.stdin.on('error', error => { captureError(`stdin: ${String(error)}`); if (retentionStarted) persistCapture('late stdin error'); });
  const closed = new Promise<NativeClose>(resolve => child.once('close', (code, signal) => {
    close = { code, signal };
    for (let index = 0; index < 2; index++) if (!eof[index]) captureError(`${index ? 'stderr' : 'stdout'} missing actual EOF`);
    persistCapture('actual ChildProcess.close');
    evidence.observations.push({ event: 'child-native-close', label, pid: child.pid, ...close, seen: seen.slice(), eof: eof.slice(), retained: raw.map(bytes => bytes.length), errors: errors.slice() });
    resolve(close);
  }));
  const server: SpawnedServer = { child, stdout, stderr, closed, get close() { return close; }, persistCapture,
    assertCapture() {
      expect(errors).toEqual([]); expect(close).toBeDefined(); expect(eof).toEqual([true, true]);
      expect(invalid).toEqual([false, false]); expect(pending[0]).toBe('');
      for (const line of stdout.filter(line => line.trim())) expect(JSON.parse(line).jsonrpc).toBe('2.0');
    },
    stop() {
      return closePromise ||= (async () => {
        try {
          if (!close) child.stdin.end();
          try { await deadline(closed, 5000, 'owned child graceful close'); }
          catch (error) {
            secondary(`graceful child close: ${String(error)}`, evidence);
            child.kill('SIGKILL'); await deadline(closed, 5000, 'owned child forced close');
            throw error; // Forced teardown cannot make the row pass.
          }
        } finally {
          if (!close) for (let index = 0; index < 2; index++) if (!eof[index]) captureError(`${index ? 'stderr' : 'stdout'} missing actual EOF after bounded cleanup`);
          persistCapture('owned stop settled or failed');
        }
      })();
    } };
  tracked.set(child, server); return server;
}
async function deadline<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try { return await Promise.race([promise, new Promise<never>((_, reject) => { timer = setTimeout(() => {
    fixturePoison ||= `Unsettled native phase: ${label}`; reject(new Error(fixturePoison));
  }, ms); })]); } finally { if (timer) clearTimeout(timer); }
}
function spawnServer(cwd: string, env: NodeJS.ProcessEnv = {}, args: string[] = []): SpawnedServer {
  if (current.closing || fixturePoison) throw new Error(fixturePoison || 'No new child after case cleanup begins');
  const child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, '--require', OBSERVER, BIN, 'serve', '--mcp', ...args], {
    cwd, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'],
    env: { CODEGRAPH_MCP_LOG_ATTACH: '1', ...process.env, ...env,
      CG_DAEMON_RECON_TRACE: current.trace, CG_DAEMON_RECON_CONTROL: current.control,
      CG_DAEMON_RECON_ROOTS: JSON.stringify(current.roots) },
  }) as ChildProcessWithoutNullStreams;
  return observeChild(child, 'proxy');
}
function registerRoot(root: string): void { if (!current.roots.includes(root)) current.roots.push(root); }
function arm(fault: 'stop-before-status' | 'exit78-before-status' | 'hold-before-status' | 'observe', attempt = 'initial'): void {
  fs.writeFileSync(current.control, JSON.stringify({ fault, attempt }));
}
function proveUnavailable(server: SpawnedServer, attempt = 'initial'): void {
  const records = trace().filter(record => record.pid === server.child.pid);
  const candidates = records.filter(record => record.event === 'candidate-spawn' && record.attempt === attempt);
  expect(candidates).toHaveLength(1);
  const candidatePid = candidates[0].candidatePid;
  const firstFactory = records.find(record => record.event === 'engine-constructor' && record.sequence > candidates[0].sequence);
  expect(records.find(record => record.event === 'candidate-controlled-stop' && record.candidatePid === candidatePid))
    .toMatchObject({ statuses: 0, acknowledgements: 0, signal: 'SIGKILL' });
  expect(records.find(record => record.event === 'candidate-exit' && record.candidatePid === candidatePid)).toMatchObject({ code: null, signal: 'SIGKILL' });
  expect(records.find(record => record.event === 'candidate-close' && record.candidatePid === candidatePid)).toMatchObject({ code: null, signal: 'SIGKILL', statuses: 0, acknowledgements: 0 });
  const close = records.find(record => record.event === 'candidate-close' && record.candidatePid === candidatePid);
  if (firstFactory) expect(close.sequence).toBeLessThan(firstFactory.sequence);
  expect(isAlive(candidatePid)).toBe(false);
  expect(fs.existsSync(path.join(current.root, '.codegraph/daemon.pid'))).toBe(false);
  const entry = JSON.parse(bounded(path.join(current.trace, `candidate-${candidatePid}-entered.json`)).toString('utf8'));
  expect(entry.argv[1]).toBe(BIN); expect(entry.argv.slice(2)).toEqual(candidates[0].args.slice(candidates[0].args.indexOf(BIN) + 1));
  expect(entry.controls).toEqual(candidates[0].controls); expect(entry.startupCpu).toBe(candidates[0].startupCpu);
  expect(trace().filter(record => record.pid === candidatePid && ['engine-before-constructor','engine-constructor','sqlite-constructor','graph-open','graph-openSync','graph-sync','graph-watch','tool-start','history'].includes(record.event))).toEqual([]);
  const factory = records.find(record => record.event === 'engine-before-constructor' && record.sequence > candidates[0].sequence);
  if (firstFactory) expect(factory?.held.find((route: any) => route.root === current.root)?.owners['daemon.pid']).toEqual({ present: false });
  const held = current.roots.map(root => ({ root, index: fs.realpathSync(path.join(root, '.codegraph/codegraph.db')) }));
  current.observations.push({ event: 'controlled-pre-service-unavailable', candidatePid, attempt, held,
    daemonAbsent: true, writer: readWriterInfo(current.root), evidence: 'native fault before status/ACK; exit+close; fresh owner/held-route records' });
}
function proveQualified78(server: SpawnedServer): void {
  const records = trace().filter(record => record.pid === server.child.pid);
  const firstFactory = records.find(record => record.event === 'engine-constructor');
  const candidates = records.filter(record => record.event === 'candidate-spawn' && (!firstFactory || record.sequence < firstFactory.sequence));
  expect(candidates).toHaveLength(1);
  const pid = candidates[0].candidatePid;
  expect(records.find(record => record.event === 'candidate-exit' && record.candidatePid === pid)).toMatchObject({ code: 78, signal: null, statuses: 0, acknowledgements: 0 });
  const closed = records.find(record => record.event === 'candidate-close' && record.candidatePid === pid);
  expect(closed).toMatchObject({ code: 78, signal: null, statuses: 0, acknowledgements: 0 });
  expect(trace().find(record => record.pid === pid && record.event === 'candidate-controlled-exit')).toMatchObject({ code: 78 });
  expect(records.filter(record => ['candidate-status','candidate-parent-send'].includes(record.event) && record.candidatePid === pid)).toEqual([]);
  expect(firstFactory).toBeDefined(); expect(closed.sequence).toBeLessThan(firstFactory!.sequence);
  expect(isAlive(pid)).toBe(false); expect(fs.existsSync(path.join(current.root, '.codegraph/daemon.pid'))).toBe(false);
  const entry = JSON.parse(bounded(path.join(current.trace, `candidate-${pid}-entered.json`)).toString('utf8'));
  expect(entry.argv[1]).toBe(BIN);
  expect(entry.controls).toEqual(candidates[0].controls); expect(entry.startupCpu).toBe(candidates[0].startupCpu);
  expect(trace().filter(record => record.pid === pid && ['engine-before-constructor','engine-constructor','sqlite-constructor','graph-open','graph-openSync','graph-sync','graph-watch','tool-start','history'].includes(record.event))).toEqual([]);
  const factory = records.find(record => record.event === 'engine-before-constructor');
  expect(factory?.held.find((route: any) => route.root === current.root)?.owners['daemon.pid']).toEqual({ present: false });
  expect(entry.argv.slice(2)).toEqual(candidates[0].args.slice(candidates[0].args.indexOf(BIN) + 1));
  expect(candidates[0].controls).toEqual({ CODEGRAPH_QUERY_MODE: 'unlimited', CODEGRAPH_QUERY_POOL_SIZE: '2', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited' });
  current.observations.push({ event: 'controlled-native-pre-status-78', pid, spawned: candidates[0], entry,
    daemonAbsent: true, writer: readWriterInfo(current.root), held: fs.realpathSync(path.join(current.root, '.codegraph/codegraph.db')) });
}
async function proveTypedUnsupported(server: SpawnedServer, root: string): Promise<void> {
  const records = trace().filter(record => record.pid === server.child.pid);
  const candidates = records.filter(record => record.event === 'candidate-spawn'); expect(candidates).toHaveLength(1);
  const pid = candidates[0].candidatePid;
  const status = records.find(record => record.event === 'candidate-status' && record.candidatePid === pid);
  expect(status?.message).toMatchObject({ protocol: 1, kind: 'refused', refusal: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unsupported-mode', projectRoot: root, guidance: expect.any(String) } });
  expect(records.filter(record => record.event === 'candidate-parent-send' && record.candidatePid === pid)).toEqual([]);
  await waitFor(() => trace().find(record => record.pid === server.child.pid && record.event === 'candidate-close' && record.candidatePid === pid), 5000);
  const closed = trace().find(record => record.pid === server.child.pid && record.event === 'candidate-close' && record.candidatePid === pid);
  expect(closed).toMatchObject({ signal: null, acknowledgements: 0 }); expect(isAlive(pid)).toBe(false);
  expect(trace().filter(record => record.pid === pid && record.event === 'candidate-controlled-exit')).toEqual([]);
  current.observations.push({ event: 'actual-typed-unsupported-terminal', candidatePid: pid, status, closed });
}
function snapshot(root: string) {
  const dir = path.join(root, '.codegraph');
  return fs.readdirSync(dir).filter(name => /^(codegraph\.db(?:-wal|-shm|-journal)?|daemon\.pid|writer\.pid)$/.test(name)).sort().map(name => {
    const file = path.join(dir, name), stat = fs.lstatSync(file);
    return stat.isDirectory() ? { name, kind: 'directory', entries: fs.readdirSync(file).sort() } : { name, kind: 'file', bytes: stat.size, sha256: sha(bounded(file)) };
  });
}
function ownSocket(socket: net.Socket): net.Socket {
  if (!ownedSockets.has(socket)) ownedSockets.set(socket, new Promise<void>(resolve => {
    socket.once('close', hadError => { current.observations.push({ event: 'parent-socket-native-close', hadError }); resolve(); });
    socket.on('error', error => secondary(`owned socket: ${String(error)}`));
  }));
  return socket;
}
async function ordinaryWriter(root: string) {
  if (current.closing || fixturePoison) throw new Error(fixturePoison || 'No new owner after case cleanup begins');
  registerRoot(root);
  const child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, OBSERVER, '--ordinary-writer', root], {
    cwd: root, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
    env: { ...process.env, CG_DAEMON_RECON_TRACE: current.trace, CG_DAEMON_RECON_ROOTS: JSON.stringify(current.roots) },
  }) as ChildProcessWithoutNullStreams;
  const server = observeChild(child, 'ordinary-writer');
  const owner = { child, server, root }; ordinaryOwners.push(owner);
  const ready = await deadline(new Promise<any>(resolve => child.on('message', message => {
    if ((message as any)?.kind === 'ordinary-writer-ready') resolve(message);
  })), 10000, 'ordinary writer readiness');
  expect(ready).toMatchObject({ root, pid: child.pid, writer: { pid: child.pid, mode: 'fallback' } });
  expect(readLockPid(root)).toBeNull(); return owner;
}
async function stopOrdinary(owner: typeof ordinaryOwners[number]) {
  return owner.stop ||= (async () => {
    try {
      if (!owner.server.close) owner.child.send({ kind: 'stop-ordinary-writer', pid: owner.child.pid, root: owner.root });
      const closed = await deadline(owner.server.closed, 5000, 'ordinary writer typed stop/native close');
      expect(closed).toEqual({ code: 0, signal: null }); owner.server.assertCapture();
      expect(fs.existsSync(path.join(owner.root, '.codegraph/writer.pid'))).toBe(false);
    } catch (error) {
      secondary(`Ordinary writer graceful failure: ${String(error)}`);
      if (!owner.server.close) { owner.child.kill('SIGKILL'); await deadline(owner.server.closed, 5000, 'ordinary writer forced native close'); }
      throw error; // A forced close remains an acceptance failure.
    }
  })();
}
async function oldPeer(root: string) {
  const socketPath = getDaemonSocketPath(root);
  const peer = net.createServer(socket => {
    peerSockets.add(socket); socket.once('close', () => peerSockets.delete(socket)); socket.on('error', error => secondary(String(error)));
    socket.end(JSON.stringify({ codegraph: '0.0.0-mismatch', pid: process.pid, socketPath, protocol: 1 }) + '\n');
  });
  peerServers.add(peer);
  await deadline(new Promise<void>((resolve, reject) => { peer.once('error', reject); peer.listen(socketPath, resolve); }), 3000, 'negative old peer listen');
  return async () => { for (const socket of peerSockets) socket.destroy(); await deadline(new Promise<void>(resolve => peer.close(() => resolve())), 3000, 'negative old peer close'); peerServers.delete(peer); };
}
async function terminal(server: SpawnedServer, reason: 'unknown' | 'unsupported-mode', root: string, first = 2) {
  for (const id of [first, first + 1]) {
    sendMessage(server.child, { jsonrpc: '2.0', id, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query: 'originalSymbol' } } });
    const result = await waitFor(() => findResponse(server.stdout, id), 5000);
    expect(result).toMatchObject({ error: { code: -32603, data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason, projectRoot: root, guidance: expect.any(String) } } });
    expect(result.result).toBeUndefined(); expect(result.error.data.guidance.trim().length).toBeGreaterThan(0);
    expect(server.stdout.filter(line => { try { return JSON.parse(line).id === id; } catch { return false; } })).toHaveLength(1);
  }
}
function zeroRequesterWork(server: SpawnedServer, allowedCandidate = false): void {
  const records = trace();
  const candidatePids = records.filter(record => record.pid === server.child.pid && record.event === 'candidate-spawn').map(record => record.candidatePid);
  if (!allowedCandidate) expect(candidatePids).toEqual([]); else expect(candidatePids).toHaveLength(1);
  const owned = records.filter(record => record.pid === server.child.pid || candidatePids.includes(record.pid));
  expect(owned.filter(record => ['engine-before-constructor', 'engine-constructor', 'sqlite-constructor', 'graph-open', 'graph-openSync', 'graph-sync', 'graph-watch', 'worker-spawn', 'tool-start', 'history'].includes(record.event))).toEqual([]);
  expect(server.stderr.some(line => /serving this session in-process|Serving reads in-process|Attached to shared daemon/.test(line))).toBe(false);
}
async function closedClient(server: SpawnedServer) { await server.stop(); expect(server.close).toEqual({ code: 0, signal: null }); server.assertCapture(); }
async function indexLifecycleSymbol(root: string): Promise<void> {
  fs.writeFileSync(path.join(root, 'lifecycle.ts'), 'export function lifecycleSymbol() { return 73; }\n');
  const graph = CodeGraph.openSync(root); try { await graph.indexAll(); } finally { graph.close(); }
}
async function realExplore(server: SpawnedServer, id: number): Promise<void> {
  sendMessage(server.child, { jsonrpc: '2.0', id, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query: 'lifecycleSymbol' } } });
  const response = await waitFor(() => findResponse(server.stdout, id), 15000);
  expect(response.error).toBeUndefined(); expect(response.result.isError).toBeFalsy();
  expect(response.result.content[0].text).toContain('function lifecycleSymbol');
  expect(server.stdout.filter(line => { try { return JSON.parse(line).id === id; } catch { return false; } })).toHaveLength(1);
}
function actualHistory(): any[] {
  const records = trace().filter(record => record.event === 'history');
  expect(records.every(record => typeof record.historyId === 'string' && typeof record.root === 'string' && Number.isInteger(record.index))).toBe(true);
  const counts = new Map<string, number>();
  for (const record of records) { const key = `${record.historyId}:${record.root}`; const expected = (counts.get(key) || 0) + 1; expect(record.index).toBe(expected); counts.set(key, expected); }
  current.observations.push({ event: 'actual-branch-session-history', records, continuousHostHistoryClaim: false });
  return records;
}


function ownedTicker(operation: () => void, intervalMs: number): () => void {
  const evidence = current; let canceled = false;
  const cancel = () => { if (canceled) return; canceled = true; clearInterval(timer); evidence.cancelTickers.delete(cancel); };
  const timer = setInterval(() => {
    if (canceled || evidence.closing || fixturePoison) return;
    try { operation(); } catch (error) {
      secondary(`owned handover ticker: ${String(error)}`, evidence);
      fixturePoison ||= 'Owned handover ticker failed'; cancel();
    }
  }, intervalMs);
  evidence.cancelTickers.add(cancel); return cancel;
}

function sendMessage(child: ChildProcessWithoutNullStreams, msg: unknown): void {
  if (current.closing || fixturePoison) throw new Error(fixturePoison || 'No new body request after cleanup starts');
  child.stdin.write(JSON.stringify(msg) + '\n');
}

function sendInitialize(child: ChildProcessWithoutNullStreams, rootUri: string, id: number): void {
  sendMessage(child, {
    jsonrpc: '2.0',
    id,
    method: 'initialize',
    params: {
      protocolVersion: '2024-11-05',
      capabilities: {},
      clientInfo: { name: 'test', version: '0.0.0' },
      rootUri,
    },
  });
}

/** Find a JSON-RPC response with the given id (result OR error) on stdout. */
function findResponse(stdout: string[], id: number): any | null {
  for (const line of stdout) {
    if (!line.trim()) continue;
    try {
      const parsed = JSON.parse(line);
      if (parsed && parsed.id === id && (parsed.result !== undefined || parsed.error !== undefined)) {
        return parsed;
      }
    } catch { /* not JSON */ }
  }
  return null;
}

function waitFor<T>(
  predicate: () => T | undefined | null | false,
  timeoutMs: number,
  pollMs = 25,
  label: string | (() => string) = '',
): Promise<T> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const tick = () => {
      let v: T | undefined | null | false;
      try { v = predicate(); } catch (e) { return reject(e); }
      if (v) return resolve(v as T);
      if (Date.now() - started > timeoutMs) {
        // Name the wait: an async stack loses the await site, so an unlabeled
        // timeout can't tell WHICH step flaked (the #662 test's recurring
        // timeout was undiagnosable for exactly this reason).
        const detail = typeof label === 'function' ? label() : label;
        return reject(new Error(`Timed out after ${timeoutMs}ms${detail ? ` waiting for: ${detail}` : ''}`));
      }
      setTimeout(tick, pollMs);
    };
    tick();
  });
}

function isAlive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'ESRCH') return false;
    throw error; // EPERM/ambiguous is never native absence.
  }
}

function readLockPid(root: string): number | null {
  try {
    const raw = bounded(path.join(root, '.codegraph', 'daemon.pid'), 65536).toString('utf8');
    const info = JSON.parse(raw);
    return typeof info.pid === 'number' ? info.pid : null;
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') secondary(`daemon owner read: ${String(error)}`); return null; }
}

function readDaemonLog(root: string): string {
  const file = path.join(root, '.codegraph', 'daemon.log');
  return fs.existsSync(file) ? bounded(file).toString('utf8') : '';
}

/** Bounded diagnostic prefixes; do not read a whole growing log into memory. */
function shutdownFile(file: string, limitBytes: number, allowMissing: boolean) {
  const result: { present: boolean | null; text: string; seenBytes: number; retainedBytes: number;
    limitBytes: number; truncated: boolean; code?: string; error?: string; expectedMissing?: boolean;
    identity?: { dev: number; ino: number; mtimeMs: number; size: number }; captureErrors: string[] } = {
    present: null, text: '', seenBytes: 0, retainedBytes: 0, limitBytes, truncated: false, captureErrors: [],
  };
  let fd: number | undefined;
  try {
    fd = fs.openSync(file, 'r');
    result.present = true;
    // procfs reports size zero even when its virtual file has content.
    const stat = fs.fstatSync(fd), prefix = Buffer.alloc(limitBytes);
    const retainedBytes = fs.readSync(fd, prefix, 0, prefix.length, 0);
    result.text = prefix.subarray(0, retainedBytes).toString('utf8');
    result.retainedBytes = retainedBytes;
    result.seenBytes = Math.max(stat.size, retainedBytes);
    result.identity = { dev: stat.dev, ino: stat.ino, mtimeMs: stat.mtimeMs, size: stat.size };
    const extra = retainedBytes === limitBytes ? fs.readSync(fd, Buffer.alloc(1), 0, 1, retainedBytes) : 0;
    result.seenBytes = Math.max(stat.size, retainedBytes + extra);
    result.truncated = stat.size > retainedBytes || extra > 0;
  } catch (error) {
    result.code = (error as NodeJS.ErrnoException).code;
    result.error = String(error).slice(0, 1024);
    result.expectedMissing = result.code === 'ENOENT' && allowMissing;
    if (result.code === 'ENOENT') result.present = false;
    if (!result.expectedMissing) result.captureErrors.push(result.error);
  } finally {
    if (fd !== undefined) {
      try { fs.closeSync(fd); }
      catch (error) { result.captureErrors.push(`close: ${String(error)}`.slice(0, 1024)); }
    }
  }
  return result;
}

function shutdownProcess(pid: number, allowMissing: boolean, captureFailure: (error: string) => void) {
  if (process.platform !== 'linux') return { pid, alive: isAlive(pid), proc: 'unavailable' };
  const stat = shutdownFile(`/proc/${pid}/stat`, 4096, allowMissing);
  stat.captureErrors.forEach(captureFailure);
  if (stat.truncated) captureFailure('bounded shutdown proc prefix overflow');
  if (stat.present !== true) return { pid, alive: isAlive(pid), proc: stat.code === 'ENOENT' ? 'absent' : 'unavailable', detail: stat.error };
  const fields = stat.text.slice(stat.text.lastIndexOf(')') + 2).trim().split(/\s+/);
  const state = fields[0];
  if (!/^[A-Za-z]$/.test(state) || !/^\d+$/.test(fields[1]) || !/^\d+$/.test(fields[19])) {
    captureFailure('invalid bounded shutdown proc stat fields');
  }
  return { pid, alive: isAlive(pid), state, ppid: Number(fields[1]), startTimeTicks: fields[19], truncated: stat.truncated,
    proc: state === 'Z' ? 'zombie' : state === 'T' || state === 't' ? 'stopped' : 'live' };
}

/** The exit must be observed within the original signal-relative budget, including diagnostic I/O. */
function waitProcessExitBefore(pid: number, deadline: number): Promise<{ exited: boolean; observedAt: number }> {
  return new Promise(resolve => {
    const tick = () => {
      if (performance.now() > deadline) return resolve({ exited: false, observedAt: performance.now() });
      const alive = isAlive(pid), observedAt = performance.now();
      if (observedAt > deadline) return resolve({ exited: false, observedAt });
      if (!alive) return resolve({ exited: true, observedAt });
      setTimeout(tick, Math.min(25, Math.max(0, deadline - observedAt)));
    };
    tick();
  });
}

function countListeningLines(root: string): number {
  return readDaemonLog(root).split('\n').filter((l) => l.includes('[CodeGraph daemon] Listening on')).length;
}

function killTree(...procs: ChildProcessWithoutNullStreams[]): void {
  for (const p of procs) {
    if (!p.killed) { const owner = tracked.get(p); if (owner) owner.expectedSignal = 'SIGKILL'; p.kill('SIGKILL'); }
  }
}

async function waitProcessExit(pid: number, timeoutMs: number): Promise<boolean> {
  return waitFor(() => !isAlive(pid), timeoutMs).then(() => true).catch(() => false);
}

async function staleIndex(root: string): Promise<Buffer> {
  fs.writeFileSync(path.join(root, 'app.ts'), 'export function originalSymbol() {}\n');
  const cg = CodeGraph.openSync(root);
  try { await cg.indexAll(); } finally { cg.close(); }
  const before = bounded(path.join(root, '.codegraph', 'codegraph.db'));
  fs.writeFileSync(path.join(root, 'app.ts'), 'export function changedSymbol() {}\n');
  return before;
}

describe('Shared MCP daemon (issue #411)', () => {
  let tempDir: string;   // the (possibly symlinked) path processes are spawned with
  let realRoot: string;  // its canonical form — what the daemon keys paths on
  const servers: SpawnedServer[] = [];
  let shutdownDiagnostic: ((phase: string) => void) | undefined;
  let shutdownDiagnosticFailure: unknown;

  beforeEach(async () => {
    if (fixturePoison) throw new Error(fixturePoison);
    shutdownDiagnostic = undefined; shutdownDiagnosticFailure = undefined;
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-mcp-daemon-'));
    realRoot = fs.realpathSync(tempDir);
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-daemon-evidence-'));
    current = { root: realRoot, directory, trace: path.join(directory, 'trace'), control: path.join(directory, 'control.json'), roots: [realRoot], observations: [], secondary: [], cancelTickers: new Set() };
    activeCase = true; fs.mkdirSync(current.trace); arm('observe');
    const cg = await CodeGraph.init(tempDir); cg.close();
  });

  afterEach(async context => {
    if (!activeCase) return; activeCase = false;
    // Each cleanup/persistence phase is independent. Never erase failed roots
    // or pretend a forced close is a healthy native tail.
    const primary = context.task.result?.errors?.map(error => String(error.message).slice(0, 2048)) ?? [];
    for (const cancel of [...current.cancelTickers]) cancel(); // Fence queued ticks BEFORE sendMessage's cleanup fence.
    current.closing = true;
    if (primary.some(error => /timed out|timeout/i.test(error))) fixturePoison ||= 'Framework case timeout: no later observer lifetime allowed';
    const errors: unknown[] = [];
    const attempt = async (label: string, operation: () => unknown | Promise<unknown>) => {
      try { await operation(); } catch (error) { errors.push(error); secondary(`${label}: ${String(error)}`); }
    };
    await attempt('shutdown diagnostics before cleanup', () => shutdownDiagnostic?.('afterEach before owned cleanup'));
    await Promise.all(servers.map(server => attempt('proxy cleanup', async () => {
      await server.stop(); server.assertCapture();
      expect(server.close).toEqual({ code: server.expectedSignal ? null : 0, signal: server.expectedSignal ?? null });
    })));
    await Promise.all(ordinaryOwners.map(owner => attempt('ordinary writer cleanup', () => stopOrdinary(owner))));
    // Independent of stop/close success, every owned child retains both bounded
    // raw prefixes now; late native events update that SAME unique evidence root.
    for (const server of tracked.values()) await attempt('independent raw capture persistence', () => { server.persistCapture('after owned child cleanup'); server.assertCapture(); });
    for (const socket of ownedSockets.keys()) socket.destroy();
    await Promise.all([...ownedSockets.values()].map(closed => attempt('parent socket actual close', () => deadline(closed, 3000, 'parent socket actual close'))));
    for (const socket of peerSockets) socket.destroy();
    await Promise.all([...peerServers].map(peer => attempt('negative peer close', () => deadline(new Promise<void>(resolve => peer.close(() => resolve())), 3000, 'negative peer close'))));
    let records: any[] = [];
    await attempt('native trace read', () => { records = trace(); });
    // Cleanup identities remain readable independently of a malformed/growing
    // event trace. Never silently turn an unreadable registry into absence.
    const registered: Array<{ candidatePid: number; root: string }> = [];
    await attempt('candidate registry inventory', async () => {
      const files = fs.readdirSync(current.trace).filter(file => /^candidate-spawn-\d+\.json$/.test(file));
      if (files.length > 128) throw new Error('Candidate registry overflow');
      await Promise.all(files.map(file => attempt(`candidate registry ${file}`, () => {
        const value = JSON.parse(bounded(path.join(current.trace, file), 65536).toString('utf8'));
        if (value.identity !== 'actual-delegated-child-spawn' || !Number.isInteger(value.candidatePid) || value.candidatePid <= 0 || file !== `candidate-spawn-${value.candidatePid}.json` || !current.roots.includes(value.root)) throw new Error('Malformed/unowned candidate registry identity');
        registered.push(value);
      })));
    });
    const candidates = [...new Set<number>([...registered.map(value => value.candidatePid), ...records.filter(record => record.event === 'candidate-spawn').map(record => record.candidatePid)])];
    await Promise.all(candidates.map(pid => attempt(`candidate ${pid} owned cleanup`, async () => {
      if (!isAlive(pid)) return;
      const root = registered.find(value => value.candidatePid === pid)?.root;
      if (!root) throw new Error(`Cannot safely stop candidate ${pid} without exact owned registry root`);
      fs.writeFileSync(path.join(current.trace, `stop-daemon-${pid}.json`), JSON.stringify({ kind: 'stop-owned-daemon', root, pid }));
      if (!(await waitProcessExit(pid, 5000))) {
        secondary(`Graceful owned daemon stop failed for ${pid}`);
        // No PID-only force of a detached/reused identity. The qualified
        // supervisor still owns its native creation identity and final cleanup;
        // a failed graceful tail cannot pass this row or the whole gate.
        throw new Error(`Owned daemon cleanup incomplete ${pid}; native supervisor cleanup required`);
      }
    })));
    await attempt('final native traces', () => { records = trace(); });
    await attempt('actual bootstrap status/ACK authority ordering', () => {
      for (const pid of candidates) {
        const childRecords = records.filter(record => record.pid === pid);
        const permission = childRecords.find(record => record.event === 'native-ipc-message' && record.message?.protocol === 1 && record.message.kind === 'acknowledged');
        const authority = childRecords.filter(record => ['engine-before-constructor','engine-constructor','sqlite-constructor','graph-open','graph-openSync','graph-sync','graph-watch','worker-spawn','tool-start','history'].includes(record.event));
        if (authority.length) { expect(permission).toBeDefined(); for (const record of authority) expect(BigInt(record.monotonicNs) > BigInt(permission.monotonicNs)).toBe(true); }
        const status = records.find(record => record.event === 'candidate-status' && record.candidatePid === pid);
        const acknowledgements = records.filter(record => record.event === 'candidate-parent-send' && record.candidatePid === pid);
        if (acknowledgements.length) { expect(status?.message).toMatchObject({ protocol: 1, kind: 'permitted' }); expect(acknowledgements).toHaveLength(1); expect(acknowledgements[0].message).toEqual({ protocol: 1, kind: 'acknowledged' }); expect(acknowledgements[0].sequence).toBeGreaterThan(status.sequence); }
        if (status?.message.kind === 'refused') expect(acknowledgements).toEqual([]);
      }
    });
    for (const root of current.roots) await attempt('final merged-fd daemon stdout/stderr', () => {
      const file = path.join(root, '.codegraph/daemon.log');
      if (!fs.existsSync(file)) { current.observations.push({ event: 'daemon-native-output', root, identity: 'merged-fd', present: false }); return; }
      const bytes = bounded(file); const copy = path.join(current.directory, `daemon-merged-${sha(root).slice(0,16)}.bin`);
      fs.writeFileSync(copy, bytes); current.observations.push({ event: 'daemon-native-output', root, identity: 'merged-fd', present: true, bytes: bytes.length, sha256: sha(bytes), copy, allOwnedCandidatesAbsent: candidates.every(pid => !isAlive(pid)) });
      expect(candidates.every(pid => !isAlive(pid))).toBe(true);
    });
    await attempt('shutdown diagnostic final', () => shutdownDiagnostic?.('afterEach after owned cleanup; inert roots retained'));
    await attempt('retained evidence', () => {
      const roots = [...current.roots, current.directory];
      const membership: any[] = [];
      const visit = (root: string, relative = '') => {
        for (const name of fs.readdirSync(path.join(root, relative)).sort()) {
          const item = path.join(relative, name), file = path.join(root, item), stat = fs.lstatSync(file);
          if (stat.isSymbolicLink()) throw new Error(`Unexpected fixture reparse entry: ${file}`);
          if (stat.isDirectory()) visit(root, item);
          else if (stat.isFile()) { if (membership.length >= 4096) throw new Error('Fixture membership overflow'); membership.push({ root, relative: item, bytes: stat.size, sha256: sha(bounded(file, MAX_TRACE_TOTAL)) }); }
        }
      };
      for (const root of roots) visit(root);
      const facts = { row: context.task.name, primary, secondary: current.secondary, roots, disposition: 'all inert roots and raw evidence retained; native cleanup separate',
        nativeClients: servers.map(server => ({ pid: server.child.pid, close: server.close })), candidatePids: candidates, candidateAbsence: candidates.map(pid => ({ pid, alive: isAlive(pid) })),
        observations: current.observations, records, membership };
      fs.writeFileSync(path.join(current.directory, 'final-facts.json'), JSON.stringify(facts, null, 2));
      if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'daemon-reconciliation-facts.jsonl'), JSON.stringify({ row: context.task.name, directory: current.directory, root: realRoot, factsSHA256: sha(bounded(path.join(current.directory, 'final-facts.json'), 128 * 1024 * 1024)) }) + '\n');
    });
    if (current.secondary.length || errors.length || fixturePoison) {
      try { fs.writeFileSync(path.join(current.directory, 'secondary-failures.json'), JSON.stringify({ primary, secondary: current.secondary, cleanup: errors.map(error => String(error).slice(0, 2048)), fixturePoison })); }
      catch (error) { process.stderr.write(`Retained fixture diagnostic persistence failed: ${String(error).slice(0, 2048)}\n`); errors.push(error); }
    }
    servers.length = 0; ordinaryOwners.length = 0; peerSockets.clear(); peerServers.clear(); ownedSockets.clear(); tracked.clear();
    if (shutdownDiagnosticFailure) errors.push(shutdownDiagnosticFailure);
    if (current.secondary.length || errors.length) throw new AggregateError(errors, `Daemon fixture secondary failures: ${current.secondary.join('; ')}`);
  }, 45000);

  it.runIf(process.platform !== 'win32')('stops despite a socket still waiting for its client hello (#1963)', async () => {
    const server = spawnServer(tempDir);
    servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(server.stdout, 1), 10000);
    // The lock is written before the socket is bound; an attached proxy proves
    // the daemon is listening (#1773).
    await waitFor(() => server.stderr.some((l) => l.includes('Attached to shared daemon')), 10000);
    const pid = await waitFor(() => readLockPid(realRoot), 10000);
    const raw = ownSocket(net.connect(getDaemonSocketPath(realRoot)));
    let started: number | undefined, samples: any[] = [], snapshots: any[] = [], primaryFailure: string | null = null;
    let exitedWithinDeadline: boolean | undefined;
    let exitObservedElapsedMs: number | undefined;
    let ownedCleanupFinished = false;
    const captureFailure = (error: string) => { shutdownDiagnosticFailure ??= new Error(error); };
    const processSnapshot = () => shutdownProcess(pid, started !== undefined || ownedCleanupFinished, captureFailure);
    const sample = () => {
      if (samples.length >= 128) { shutdownDiagnosticFailure ??= new Error('bounded shutdown sample overflow'); return; }
      const process = processSnapshot();
      samples.push({ elapsedMs: started === undefined ? null : performance.now() - started, process });
    };
    shutdownDiagnostic = phase => {
      try {
        if (snapshots.length >= 16) throw new Error('bounded shutdown snapshot overflow');
        const deleted = phase === 'afterEach after fixture deletion';
        if (phase === 'afterEach after owned cleanup before deletion' || deleted) ownedCleanupFinished = true;
        const files = { daemonLog: shutdownFile(path.join(realRoot, '.codegraph/daemon.log'), 65536, deleted),
          daemonLock: shutdownFile(path.join(realRoot, '.codegraph/daemon.pid'), 4096, started !== undefined || ownedCleanupFinished),
          writerLock: shutdownFile(path.join(realRoot, '.codegraph/writer.pid'), 4096, started !== undefined || ownedCleanupFinished) };
        Object.values(files).flatMap(file => file.captureErrors).forEach(captureFailure);
        if (Object.values(files).some(file => file.truncated)) shutdownDiagnosticFailure ??= new Error('bounded shutdown file prefix overflow');
        sample(); snapshots.push({ phase, elapsedMs: started === undefined ? null : performance.now() - started, files,
          raw: { destroyed: raw.destroyed, readable: raw.readable, writable: raw.writable }, process: processSnapshot() });
        if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, 'daemon-shutdown-diagnostic.json'),
          JSON.stringify({ root: realRoot, pid, deadlineMs: 1500, exitedWithinDeadline, exitObservedElapsedMs, primaryFailure,
            captureFailure: shutdownDiagnosticFailure ? String(shutdownDiagnosticFailure) : null, samples, snapshots }, null, 2));
      } catch (error) { shutdownDiagnosticFailure ??= error; }
    };
    let sampling: NodeJS.Timeout | undefined;
    try {
      await new Promise<void>((resolve, reject) => {
        raw.once('data', () => resolve());
        raw.once('error', reject);
      });
      shutdownDiagnostic('before SIGTERM');
      started = performance.now(); sampling = setInterval(sample, 25);
      process.kill(pid, 'SIGTERM');
      const exitObservation = waitProcessExitBefore(pid, started + 1500);
      shutdownDiagnostic('after SIGTERM');
      const observed = await exitObservation;
      exitedWithinDeadline = observed.exited; exitObservedElapsedMs = observed.observedAt - started;
      clearInterval(sampling); sampling = undefined;
      shutdownDiagnostic('before original exit assertion');
      expect(exitedWithinDeadline).toBe(true);
      expect(fs.existsSync(path.join(realRoot, '.codegraph', 'writer.pid'))).toBe(false);
      shutdownDiagnostic('after original assertions');
    } catch (error) { primaryFailure = String(error); throw error;
    } finally {
      if (sampling) clearInterval(sampling);
      shutdownDiagnostic('finally before raw close');
      raw.destroy();
      shutdownDiagnostic('finally after raw close');
    }
  }, 20000);

  it('a disconnect before client hello leaves no phantom session and the idle reaper exits (#1356)', async () => {
    const server = spawnServer(tempDir, {
      CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '800',
      CODEGRAPH_DAEMON_MAX_IDLE_MS: '0',
      CODEGRAPH_DAEMON_CLIENT_SWEEP_MS: '0',
    });
    servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(server.stdout, 1), 10000);
    await waitFor(() => server.stderr.some((l) => l.includes('Attached to shared daemon')), 10000);
    const pid = await waitFor(() => readLockPid(realRoot), 10000);
    const raw = ownSocket(net.connect(getDaemonSocketPath(realRoot)));
    try {
      await new Promise<void>((resolve, reject) => {
        raw.once('data', () => resolve());
        raw.once('error', reject);
      });
      raw.destroy();
      server.child.stdin.end();
      expect(await waitProcessExit(pid, 8000)).toBe(true);
      expect(readDaemonLog(realRoot)).toContain('Shutting down (idle timeout; clients=0)');
      expect(fs.existsSync(path.join(realRoot, '.codegraph', 'writer.pid'))).toBe(false);
    } finally { raw.destroy(); }
  }, 20000);

  it('two invocations share ONE detached daemon; both attach as proxies', async () => {
    const env = { CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '15000' };

    const first = spawnServer(tempDir, env);
    servers.push(first);
    sendInitialize(first.child, `file://${tempDir}`, 1);
    const firstResp = await waitFor(() => findResponse(first.stdout, 1), 10000, 25,
      () => 'first initialize; ' + JSON.stringify({ root: realRoot, pid: first.child.pid,
        exitCode: first.child.exitCode, signal: first.child.signalCode, stdout: first.stdout,
        stderr: first.stderr, daemonPid: readLockPid(realRoot), daemonLog: readDaemonLog(realRoot) }));
    expect(firstResp.result.serverInfo.name).toBe('codegraph');

    // The launcher is a PROXY (not the daemon itself) — that's the detach fix.
    await waitFor(() => first.stderr.some((l) => l.includes('Attached to shared daemon')), 8000);

    // A detached daemon came up and recorded itself.
    await waitFor(() => fs.existsSync(path.join(realRoot, '.codegraph', 'daemon.pid')), 8000);
    await waitFor(() => countListeningLines(realRoot) >= 1, 8000);
    const daemonPid = readLockPid(realRoot);
    expect(daemonPid).toBeTruthy();
    expect(isAlive(daemonPid!)).toBe(true);
    // The socket exists at the path the code computes from the canonical root.
    // On Windows the daemon listens on a named pipe (\\.\pipe\...), which isn't
    // a filesystem entry — existsSync doesn't apply there, and the "Attached to
    // shared daemon" proof above already confirms the proxy reached it.
    if (process.platform !== 'win32') {
      expect(fs.existsSync(getDaemonSocketPath(realRoot))).toBe(true);
    }

    // Second invocation attaches as a proxy to the SAME daemon.
    const second = spawnServer(tempDir, env);
    servers.push(second);
    sendInitialize(second.child, `file://${tempDir}`, 2);
    const secondResp = await waitFor(() => findResponse(second.stdout, 2), 10000, 25,
      () => 'second initialize; ' + JSON.stringify({ root: realRoot, pid: second.child.pid,
        exitCode: second.child.exitCode, signal: second.child.signalCode, stdout: second.stdout,
        stderr: second.stderr, daemonPid: readLockPid(realRoot), daemonLog: readDaemonLog(realRoot) }));
    expect(secondResp.result.serverInfo.name).toBe('codegraph');
    await waitFor(() => second.stderr.some((l) => l.includes('Attached to shared daemon')), 8000);

    // Exactly one daemon ever bound, and it's the same pid both attached to.
    expect(countListeningLines(realRoot)).toBe(1);
    expect(readLockPid(realRoot)).toBe(daemonPid);
  }, 40000);

  it('concurrent launchers converge on a single daemon (lockfile race — must-fix 1)', async () => {
    const env = { CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '15000' };

    // Fire three launchers as close to simultaneously as possible — this is the
    // race window where the old code could end up with two daemons.
    const procs = [spawnServer(tempDir, env), spawnServer(tempDir, env), spawnServer(tempDir, env)];
    procs.forEach((p, i) => { servers.push(p); sendInitialize(p.child, `file://${tempDir}`, i + 1); });

    // All three get a valid initialize response...
    for (let i = 0; i < procs.length; i++) {
      const resp = await waitFor(() => findResponse(procs[i].stdout, i + 1), 12000);
      expect(resp.result.serverInfo.name).toBe('codegraph');
    }
    // ...and all three attached as proxies (none fell back / wedged).
    for (const p of procs) {
      await waitFor(() => p.stderr.some((l) => l.includes('Attached to shared daemon')), 10000);
    }

    // The decisive assertion: exactly ONE daemon bound the socket. Losing
    // candidates log "already holds the lock; exiting" and never listen.
    expect(countListeningLines(realRoot)).toBe(1);
    const daemonPid = readLockPid(realRoot);
    expect(daemonPid).toBeTruthy();
    expect(isAlive(daemonPid!)).toBe(true);
  }, 45000);

  it('daemon survives the first client dying; a second client keeps working (must-fix 2 / #277)', async () => {
    // Idle high so the daemon doesn't reap mid-test; poll fast so proxy 1
    // notices its dead parent quickly.
    const env = { CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '30000', CODEGRAPH_PPID_POLL_MS: '200' };

    const first = spawnServer(tempDir, env);
    servers.push(first);
    sendInitialize(first.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(first.stdout, 1), 10000);
    await waitFor(() => (readLockPid(realRoot) ?? 0) > 0, 8000);
    const daemonPid = readLockPid(realRoot)!;
    expect(isAlive(daemonPid)).toBe(true);

    const second = spawnServer(tempDir, env);
    servers.push(second);
    sendInitialize(second.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(second.stdout, 1), 10000);
    await waitFor(() => second.stderr.some((l) => l.includes('Attached to shared daemon')), 8000);

    // Kill the launcher that spawned the daemon. With the old in-process design
    // this would take the daemon (and thus the second client) down.
    killTree(first.child);

    // The daemon is detached — it must still be alive a beat later.
    await new Promise((r) => setTimeout(r, 1500));
    expect(isAlive(daemonPid)).toBe(true);

    // And the second client can still drive a real tool call through it.
    sendMessage(second.child, { jsonrpc: '2.0', id: 2, method: 'tools/list' });
    const toolsResp = await waitFor(() => findResponse(second.stdout, 2), 10000);
    expect(Array.isArray(toolsResp.result.tools)).toBe(true);
    expect(toolsResp.result.tools.length).toBeGreaterThan(0);
  }, 45000);

  it('CODEGRAPH_NO_DAEMON=1 keeps each process independent (no socket/pidfile)', async () => {
    const env = { CODEGRAPH_NO_DAEMON: '1' };
    const first = spawnServer(tempDir, env);
    servers.push(first);
    sendInitialize(first.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(first.stdout, 1), 10000);
    // Direct mode — no daemon machinery touched.
    expect(first.stderr.some((l) => l.includes('Attached to shared daemon'))).toBe(false);
    expect(fs.existsSync(path.join(realRoot, '.codegraph', 'daemon.pid'))).toBe(false);
    expect(fs.existsSync(path.join(realRoot, '.codegraph', 'daemon.log'))).toBe(false);
  }, 20000);

  it.each(['2', '0'])('direct stdio honors query pool size=%s and exits after concurrent reads', async (size) => {
    fs.writeFileSync(path.join(tempDir, 'app.ts'), 'export function directSymbol() { return 42; }\n');
    const cg = await CodeGraph.open(tempDir);
    try { await cg.indexAll(); } finally { cg.close(); }
    const server = spawnServer(tempDir, {
      CODEGRAPH_NO_DAEMON: '1', CODEGRAPH_QUERY_POOL_SIZE: size,
    }, ['--no-watch', '--path', tempDir]);
    servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(server.stdout, 1), 10000);
    await waitFor(() => server.stderr.some((l) => l.includes(size === '0'
      ? 'Query pool disabled' : 'Query pool: up to 2 worker thread(s)')), 10000, 25, 'direct query pool configuration');
    const ids = [2, 3, 4, 5, 6, 7];
    for (const id of ids) {
      sendMessage(server.child, {
        jsonrpc: '2.0', id, method: 'tools/call',
        params: { name: 'codegraph_explore', arguments: { query: 'directSymbol' } },
      });
    }
    const replies = await Promise.all(ids.map((id) => waitFor(() => findResponse(server.stdout, id), 15000)));
    for (const reply of replies) {
      expect(reply.error).toBeUndefined();
      expect(reply.result.isError).toBeFalsy();
      expect(reply.result.content[0].text).toContain('directSymbol');
      expect(reply.result).not.toHaveProperty('_cgExploreEmission');
    }
    const exited = new Promise<number | null>((resolve) => server.child.once('exit', resolve));
    server.child.stdin.end();
    expect(await exited).toBe(0);
  }, 30000);

  it('direct stdio queries multiple projects without a default index', async () => {
    fs.rmSync(path.join(tempDir, '.codegraph'), { recursive: true, force: true });
    for (const name of ['alpha', 'beta']) {
      const root = path.join(tempDir, name);
      fs.mkdirSync(root);
      fs.writeFileSync(path.join(root, 'app.ts'), `export function ${name}Symbol() { return 42; }\n`);
      const cg = await CodeGraph.init(root);
      try { await cg.indexAll(); } finally { cg.close(); }
    }
    const server = spawnServer(tempDir, {
      CODEGRAPH_NO_DAEMON: '1', CODEGRAPH_QUERY_POOL_SIZE: '2',
    }, ['--no-watch', '--path', tempDir]);
    servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(server.stdout, 1), 10000);
    await waitFor(() => server.stderr.some((l) => l.includes('Query pool: up to 2')), 10000);
    for (const [i, name] of ['alpha', 'beta'].entries()) {
      sendMessage(server.child, {
        jsonrpc: '2.0', id: i + 2, method: 'tools/call',
        params: { name: 'codegraph_explore', arguments: { query: `${name}Symbol`, projectPath: path.join(tempDir, name) } },
      });
    }
    for (const [i, name] of ['alpha', 'beta'].entries()) {
      const reply = await waitFor(() => findResponse(server.stdout, i + 2), 15000);
      expect(reply.result.isError).toBeFalsy();
      expect(reply.result.content[0].text).toContain(`${name}Symbol`);
    }
    sendMessage(server.child, {
      jsonrpc: '2.0', id: 4, method: 'tools/call',
      params: { name: 'codegraph_explore', arguments: { query: 'alphaSymbol' } },
    });
    const missing = await waitFor(() => findResponse(server.stdout, 4), 10000);
    expect(missing.result.isError).toBeFalsy();
    expect(missing.result.content[0].text).toContain('projectPath');
    expect(fs.existsSync(path.join(tempDir, '.codegraph'))).toBe(false);
  }, 30000);

  it('proxy fallback enables the query pool when no live writer holds the project', async () => {
    // Actual delegated CLI candidate, controlled native stop BEFORE status/ACK.
    // This is not an old peer, null acquisition injection or fake child argv.
    arm('stop-before-status');
    const server = spawnServer(tempDir, { CODEGRAPH_QUERY_POOL_SIZE: '2' }, ['--no-watch']); servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(server.stdout, 1), 10000);
    sendMessage(server.child, { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'codegraph_files', arguments: {} } });
    const reply = await waitFor(() => findResponse(server.stdout, 2), 10000);
    expect(reply.error).toBeUndefined(); expect(reply.result.isError).toBeFalsy();
    proveUnavailable(server);
    expect(readWriterInfo(realRoot)).toMatchObject({ pid: server.child.pid, mode: 'fallback' });
    const actual = trace().find(record => record.pid === server.child.pid && record.event === 'engine-diagnostics')?.diagnostics;
    expect(actual.effective).toMatchObject({ executionMode: 'writable-fallback', concurrency: { kind: 'finite', maximum: 2 } });
    expect(actual.ownership).toMatchObject({ pid: server.child.pid, capability: 'read-write', writerAuthority: true, synchronizationAuthority: true });
    expect(server.stderr.some(line => line.includes('serving this session in-process'))).toBe(true);
    expect(server.stderr.some(line => line.includes('Query pool: up to 2'))).toBe(true);
    arm('observe', 'after-first-fallback');
  }, 30000);

  async function historyProfile(mode: 'direct' | 'shared-daemon' | 'writable-fallback' | 'read-only-fallback' | 'legacy-shared-daemon' | 'legacy-read-only-fallback') {
    fs.writeFileSync(path.join(tempDir, 'app.ts'), 'export function alphaPolicySymbol() { return 42; }\n');
    const alpha = await CodeGraph.open(tempDir); try { await alpha.indexAll(); } finally { alpha.close(); }
    const beta = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-policy-beta-'))); registerRoot(beta);
    fs.writeFileSync(path.join(beta, 'app.ts'), 'export function betaPolicySymbol() { return 24; }\n');
    const betaGraph = await CodeGraph.init(beta); try { await betaGraph.indexAll(); } finally { betaGraph.close(); }
    const readOnly = mode === 'read-only-fallback' || mode === 'legacy-read-only-fallback';
    const legacy = mode.startsWith('legacy-');
    const owner = readOnly ? await ordinaryWriter(realRoot) : undefined;
    const beforeAlpha = snapshot(realRoot), beforeBeta = snapshot(beta);
    const alphaDB = bounded(path.join(realRoot, '.codegraph/codegraph.db')), betaDB = bounded(path.join(beta, '.codegraph/codegraph.db'));
    if (readOnly) arm('stop-before-status');
    if (mode === 'writable-fallback') arm('exit78-before-status');
    const diagnostic = path.join(tempDir, '.policy-session-history.jsonl');
    const server = spawnServer(tempDir, {
      CODEGRAPH_NO_DAEMON: mode === 'direct' ? '1' : '0',
      CODEGRAPH_QUERY_MODE: legacy ? undefined : 'unlimited',
      CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited', CODEGRAPH_QUERY_POOL_SIZE: '2',
      CODEGRAPH_MCP_TOOLS: 'explore,node,search,callers,callees,impact,files,status',
      CODEGRAPH_EXPLORE_DEBUG: diagnostic, CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '1000',
    }, ['--path', tempDir]); servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    expect((await waitFor(() => findResponse(server.stdout, 1), 10000)).result.serverInfo.name).toBe('codegraph');
    sendMessage(server.child, { jsonrpc: '2.0', id: 2, method: 'tools/list' });
    const pending = await waitFor(() => findResponse(server.stdout, 2), 10000);
    expect(pending.result.tools.map((tool: any) => tool.name).sort()).toEqual([
      'codegraph_explore','codegraph_node','codegraph_search','codegraph_callers','codegraph_callees','codegraph_impact','codegraph_files','codegraph_status',
    ].sort());
    if (mode === 'read-only-fallback' || mode === 'shared-daemon') {
      // Original exact combined controls are retained, but combined RO is not
      // activated until #31. BOTH original exact-profile positive [1,1,2]
      // obligations remain pending #31→15, never replaced by the legacy rows.
      await terminal(server, 'unsupported-mode', realRoot, 3);
      if (mode === 'read-only-fallback') proveUnavailable(server); else await proveTypedUnsupported(server, realRoot);
      zeroRequesterWork(server, true);
      expect(snapshot(realRoot)).toEqual(beforeAlpha); expect(snapshot(beta)).toEqual(beforeBeta);
      if (owner) { expect(isAlive(owner.child.pid!)).toBe(true); expect(readWriterInfo(realRoot)).toMatchObject({ pid: owner.child.pid, mode: 'fallback' }); }
      // Candidate absence does not clear a primary terminal latch.
      await terminal(server, 'unsupported-mode', realRoot, 7);
      expect(snapshot(realRoot)).toEqual(beforeAlpha); expect(snapshot(beta)).toEqual(beforeBeta);
      await closedClient(server);
      zeroRequesterWork(server, true);
      expect(snapshot(realRoot)).toEqual(beforeAlpha); expect(snapshot(beta)).toEqual(beforeBeta);
      if (owner) { expect(isAlive(owner.child.pid!)).toBe(true); expect(readWriterInfo(realRoot)).toMatchObject({ pid: owner.child.pid, mode: 'fallback' }); }
      return;
    }
    for (const [id, projectPath, symbol] of [[3, undefined, 'alphaPolicySymbol'], [4, beta, 'betaPolicySymbol'], [5, undefined, 'alphaPolicySymbol']] as const) {
      sendMessage(server.child, { jsonrpc: '2.0', id, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query: symbol, ...(projectPath ? { projectPath } : {}) } } });
      const reply = await waitFor(() => findResponse(server.stdout, id), 15000);
      expect(reply.error).toBeUndefined(); expect(reply.result.isError).toBeFalsy();
      expect(reply.result.content[0].text).toContain(`function ${symbol}`);
      expect(reply.result).not.toHaveProperty('_cgExploreEmission'); expect(reply.result).not.toHaveProperty('context');
      if (id === 3) {
        // Move the original tiny-three assertion AFTER its first original query;
        // no extra explore warm-up changes session history.
        sendMessage(server.child, { jsonrpc: '2.0', id: 6, method: 'tools/list' });
        const listed = await waitFor(() => findResponse(server.stdout, 6), 10000);
        const expected = mode === 'direct' ? ['codegraph_explore','codegraph_node','codegraph_search'] : [
          'codegraph_explore','codegraph_node','codegraph_search','codegraph_callers','codegraph_callees','codegraph_impact','codegraph_files','codegraph_status',
        ];
        expect(listed.result.tools.map((tool: any) => tool.name).sort()).toEqual(expected.sort());
      }
    }
    const reports = bounded(diagnostic).toString('utf8').trim().split(/\r?\n/).map(line => JSON.parse(line));
    expect(reports.map(report => report.session.callIndex)).toEqual([1,1,2]);
    expect(reports[2].session.priorCalls).toBe(1); expect(reports[2].session.priorFiles.length).toBeGreaterThan(0);
    const actual = trace().filter(record => record.event === 'engine-diagnostics').map(record => record.diagnostics)
      .find(value => value.ownership?.pid === (mode === 'legacy-shared-daemon' ? readLockPid(realRoot) : server.child.pid));
    expect(actual.effective).toMatchObject({ executionMode: mode === 'direct' ? 'direct' : mode === 'legacy-shared-daemon' ? 'shared-daemon' : readOnly ? 'read-only-fallback' : 'writable-fallback',
      concurrency: { kind: 'finite', maximum: 2 }, deadline: { kind: 'absent' }, retention: legacy ? 'ordinary' : 'retire-idle' });
    if (mode === 'writable-fallback') {
      // Controlled actual CLI pre-status native78 allows supported strict W.
      // A real typed unsupported refusal is always terminal, even followed78.
      proveQualified78(server);
      expect(readWriterInfo(realRoot)).toMatchObject({ pid: server.child.pid, mode: 'fallback' });
    }
    if (readOnly) {
      proveUnavailable(server); arm('observe', 'after-read-only-history');
      expect(server.stderr.some(line => line.includes('Serving reads in-process without auto-sync:'))).toBe(true);
      expect(server.stderr.some(line => line.includes('Query pool: up to 2 worker thread(s) for concurrent reads (local read-only fallback; no writer or auto-sync ownership)'))).toBe(true);
      expect(bounded(path.join(realRoot, '.codegraph/codegraph.db'))).toEqual(alphaDB);
      expect(bounded(path.join(beta, '.codegraph/codegraph.db'))).toEqual(betaDB);
      expect(readLockPid(realRoot)).toBeNull();
      expect(readWriterInfo(realRoot)).toMatchObject({ pid: owner!.child.pid, mode: 'fallback' });
    }
    const daemonPid = mode === 'legacy-shared-daemon' ? readLockPid(realRoot) : null;
    await closedClient(server);
    if (daemonPid) expect(await waitProcessExit(daemonPid, 10000)).toBe(true);
    expect(server.stderr.some(line => line.includes('up to unlimited'))).toBe(false);
  }
  it.each(['direct','shared-daemon','writable-fallback','read-only-fallback'] as const)
  ('preserves default and explicit project results and session history in %s (#9)', historyProfile, 45000);
  it.each(['legacy-shared-daemon','legacy-read-only-fallback'] as const)
  ('preserves the distinct actual legacy results and session history in %s (#9)', historyProfile, 45000);

  it('clears a stale (dead-pid) lockfile and a fresh daemon takes over', async () => {
    // Plant a lockfile pointing at a definitely-dead pid + the real socket path.
    fs.writeFileSync(
      path.join(realRoot, '.codegraph', 'daemon.pid'),
      JSON.stringify({
        pid: 999_999,
        version: '0.0.0-fake',
        socketPath: getDaemonSocketPath(realRoot),
        startedAt: Date.now() - 1000,
      }),
    );

    const env = { CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '15000' };
    const server = spawnServer(tempDir, env);
    servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    const resp = await waitFor(() => findResponse(server.stdout, 1), 10000).catch((e) => {
      throw new Error(`${(e as Error).message}\nstderr:\n${server.stderr.join('\n')}\ndaemon.log:\n${readDaemonLog(realRoot)}`);
    });
    expect(resp.result.serverInfo.name).toBe('codegraph');
    await waitFor(() => countListeningLines(realRoot) >= 1, 10000);
    // The pidfile now names a live daemon, not the planted-dead 999999.
    const livePid = readLockPid(realRoot);
    expect(livePid).not.toBe(999_999);
    expect(isAlive(livePid!)).toBe(true);
  }, 40000);

  it('preserves paired daemon/writer locks when their live PID may have been reused', async () => {
    const env = { CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '30000' };
    const first = spawnServer(tempDir, env);
    servers.push(first);
    sendInitialize(first.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(first.stdout, 1), 10000);
    await waitFor(() => countListeningLines(realRoot) >= 1, 10000);
    // Listening precedes engine initialization. Do not kill SQLite while its
    // initial connection is still being configured for this fixture.
    sendMessage(first.child, {
      jsonrpc: '2.0', id: 10, method: 'tools/call',
      params: { name: 'codegraph_status', arguments: {} },
    });
    const ready = await waitFor(() => findResponse(first.stdout, 10), 10000);
    expect(ready.result?.isError).not.toBe(true);
    expect(JSON.stringify(ready.result)).toContain('CodeGraph Status');
    const killedPid = readLockPid(realRoot)!;

    // End the first proxy before simulating PID reuse. Otherwise it can switch
    // to a fallback writer while this test prepares the replacement locks/DB.
    first.child.stdin.end();
    await waitFor(() => first.child.exitCode !== null, 5000);
    process.kill(killedPid, 'SIGKILL');
    expect(await waitProcessExit(killedPid, 8000)).toBe(true);

    // Model OS PID reuse without risking another process: the stale lock now
    // names this live vitest worker, but no daemon answers the leftover socket.
    const daemonPath = path.join(realRoot, '.codegraph', 'daemon.pid');
    const writerPath = path.join(realRoot, '.codegraph', 'writer.pid');
    const staleDaemonLock = JSON.stringify({
      pid: process.pid,
      version: CodeGraphPackageVersion,
      socketPath: getDaemonSocketPath(realRoot),
      startedAt: Date.now() - 60_000,
    });
    const staleWriterLock = JSON.stringify({
      pid: process.pid,
      mode: 'daemon',
      startedAt: Date.now() - 60_000,
    }) + '\n';
    fs.writeFileSync(daemonPath, staleDaemonLock);
    fs.writeFileSync(writerPath, staleWriterLock);

    // Make the index stale by changing the source, without opening a new
    // SQLite writer against the database of the daemon we just killed.
    const before = bounded(path.join(realRoot, '.codegraph', 'codegraph.db'));
    fs.writeFileSync(path.join(realRoot, 'app.ts'), 'export function changedSymbol() {}\n');
    const beforeState = snapshot(realRoot);
    const second = spawnServer(tempDir, env);
    servers.push(second);
    sendInitialize(second.child, `file://${tempDir}`, 2);
    const response = await waitFor(() => findResponse(second.stdout, 2), 12000);
    expect(response.result.serverInfo.name).toBe('codegraph');
    await terminal(second, 'unknown', realRoot, 3);
    expect(second.stderr.some(line => line.includes('Attached to shared daemon'))).toBe(false);
    expect(countListeningLines(realRoot)).toBe(1);
    expect(bounded(daemonPath, 65536).toString('utf8')).toBe(staleDaemonLock);
    expect(bounded(writerPath, 65536).toString('utf8')).toBe(staleWriterLock);
    expect(isAlive(process.pid)).toBe(true);
    await closedClient(second); zeroRequesterWork(second);
    expect(snapshot(realRoot)).toEqual(beforeState);
    expect(bounded(path.join(realRoot, '.codegraph/codegraph.db'))).toEqual(before);
  }, 50000);

  it('does not replace a live legacy lock with a second daemon', async () => {
    const pidPath = path.join(realRoot, '.codegraph/daemon.pid');
    fs.writeFileSync(pidPath, `${process.pid}\n`);
    const before = snapshot(realRoot);
    const server = spawnServer(tempDir, { CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '15000' }); servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    expect((await waitFor(() => findResponse(server.stdout, 1), 12000)).result.serverInfo.name).toBe('codegraph');
    await terminal(server, 'unknown', realRoot);
    expect(bounded(pidPath, 65536).toString('utf8')).toBe(`${process.pid}\n`);
    expect(countListeningLines(realRoot)).toBe(0); expect(isAlive(process.pid)).toBe(true);
    await closedClient(server); zeroRequesterWork(server); expect(snapshot(realRoot)).toEqual(before);
  }, 40000);

  it('does not start a fallback writer when the daemon lock is unreadable', async () => {
    fs.mkdirSync(path.join(realRoot, '.codegraph/daemon.pid'));
    const before = snapshot(realRoot);
    const server = spawnServer(tempDir); servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    expect((await waitFor(() => findResponse(server.stdout, 1), 12000)).result.serverInfo.name).toBe('codegraph');
    await terminal(server, 'unknown', realRoot);
    await closedClient(server); zeroRequesterWork(server); expect(snapshot(realRoot)).toEqual(before);
  }, 40000);

  it.each([null, 'daemon', 'fallback'])('proxy refuses unknown service without read-only fallback on a daemon version mismatch (writer: %s)', async mode => {
    const beforeDB = await staleIndex(realRoot);
    const writerPath = path.join(realRoot, '.codegraph/writer.pid');
    const writer = JSON.stringify({ pid: process.pid, mode, startedAt: Date.now() });
    if (mode) fs.writeFileSync(writerPath, writer);
    fs.writeFileSync(path.join(realRoot, '.codegraph/daemon.pid'), JSON.stringify({ pid: process.pid, version: '0.0.0-mismatch', socketPath: getDaemonSocketPath(realRoot), startedAt: Date.now() }));
    const disappear = await oldPeer(realRoot), before = snapshot(realRoot);
    const server = spawnServer(tempDir); servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    expect((await waitFor(() => findResponse(server.stdout, 1), 10000)).result.serverInfo.name).toBe('codegraph');
    sendMessage(server.child, { jsonrpc: '2.0', id: 10, method: 'tools/list' });
    expect((await waitFor(() => findResponse(server.stdout, 10), 5000)).result.tools.length).toBeGreaterThan(0);
    await terminal(server, 'unknown', realRoot);
    expect(snapshot(realRoot)).toEqual(before);
    await disappear(); // Real negative peer native sockets/server close.
    await terminal(server, 'unknown', realRoot, 4); // Latch survives disappearance.
    await closedClient(server); zeroRequesterWork(server);
    expect(snapshot(realRoot)).toEqual(before);
    expect(bounded(path.join(realRoot, '.codegraph/codegraph.db'))).toEqual(beforeDB);
    expect(readLockPid(realRoot)).toBe(process.pid);
    if (mode) expect(bounded(writerPath, 65536).toString('utf8')).toBe(writer);
  }, 30000);

  it.each(['pool-without-writer','writable-history','read-only-history','read-only-handover'] as const)
  ('older connected peer is terminal for obsolete %s fallback and remains so after peer disappearance', async profile => {
    await staleIndex(realRoot);
    const combined = profile === 'writable-history' || profile === 'read-only-history';
    if (profile === 'read-only-history' || profile === 'read-only-handover') {
      fs.writeFileSync(path.join(realRoot, '.codegraph/daemon.pid'), JSON.stringify({ pid: process.pid, version: '0.0.0-mismatch', socketPath: getDaemonSocketPath(realRoot), startedAt: Date.now() }));
      if (profile === 'read-only-history') fs.writeFileSync(path.join(realRoot, '.codegraph/writer.pid'), JSON.stringify({ pid: process.pid, mode: 'daemon', startedAt: Date.now() }));
    }
    const disappear = await oldPeer(realRoot), before = snapshot(realRoot);
    const server = spawnServer(tempDir, { ...(profile !== 'read-only-handover' ? { CODEGRAPH_QUERY_POOL_SIZE: '2' } : {}),
      ...(combined ? { CODEGRAPH_QUERY_MODE: 'unlimited', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited' } : {}),
      ...(profile === 'read-only-handover' ? { CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '30000', CODEGRAPH_DAEMON_RETRY_MS: '300', CODEGRAPH_DAEMON_RETRY_MAX_MS: '1000' } : {}),
    }, profile === 'pool-without-writer' ? ['--no-watch'] : []); servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    expect((await waitFor(() => findResponse(server.stdout, 1), 10000)).result.serverInfo.name).toBe('codegraph');
    await terminal(server, 'unknown', realRoot);
    expect(snapshot(realRoot)).toEqual(before); await disappear();
    await terminal(server, 'unknown', realRoot, 4);
    await closedClient(server); zeroRequesterWork(server); expect(snapshot(realRoot)).toEqual(before);
  }, 30000);

  // The over-the-wire client-hello → record → sweep path, and the inactivity
  // backstop's liveness gate, are covered by the deterministic unit tests in
  // daemon-client-liveness (`reapDeadClients`, `backstopShouldExit`) — a
  // raw-socket variant here was flaky under heavy parallel load. What stays
  // here is the lifecycle behavior that needs real procs: a live-but-quiet
  // client must SURVIVE the inactivity backstop. Reaping it used to silently
  // degrade the session (and any others sharing the daemon) to an in-process
  // engine; on a real machine the backstop fired on live sessions far more
  // often than on the phantoms it exists for. The phantom case it still covers
  // (an unknown-pid connection) is the `backstopShouldExit` unit test.
  it('does NOT reap a live-but-quiet client on the inactivity backstop (#692)', async () => {
    // Backstop short, idle timeout long: with a client connected the idle timer
    // never arms, so the inactivity backstop is the only thing that could take
    // the daemon down — and it must not, because the client's peer is alive.
    const env = { CODEGRAPH_DAEMON_MAX_IDLE_MS: '1200', CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '60000' };
    const server = spawnServer(tempDir, env);
    servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(server.stdout, 1), 10000);
    // initialize is answered locally, and a PID file can belong to a daemon
    // still starting. Establish a real live session before measuring silence.
    const attached = await waitFor(
      () => server.stderr.find((line) => line.includes('Attached to shared daemon')),
      10000,
    );
    sendMessage(server.child, {
      jsonrpc: '2.0', id: 2, method: 'tools/call',
      params: { name: 'codegraph_status', arguments: {} },
    });
    const status = await waitFor(() => findResponse(server.stdout, 2), 10000);
    expect(status.error).toBeUndefined();
    expect(status.result?.isError).not.toBe(true);
    expect(JSON.stringify(status.result)).toContain('CodeGraph Status');
    const daemonPid = readLockPid(realRoot)!;
    expect(attached).toContain(`(pid ${daemonPid},`);
    expect(isAlive(daemonPid)).toBe(true);

    // Stay silent well past several backstop windows. The live session's peer is
    // provably alive, so the daemon must keep running (and never log a backstop
    // shutdown), with its lockfile intact.
    await new Promise((r) => setTimeout(r, 4000)); // > 3× maxIdle
    expect(isAlive(daemonPid), readDaemonLog(realRoot) + '\n' + server.stderr.join('\n')).toBe(true);
    expect(readDaemonLog(realRoot)).not.toContain('inactivity backstop');
    expect(readLockPid(realRoot)).toBe(daemonPid);
  }, 30000);

  it('daemon idle-times-out after the last client disconnects', async () => {
    const env = { CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '800', CODEGRAPH_PPID_POLL_MS: '200' };
    const server = spawnServer(tempDir, env);
    servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(server.stdout, 1), 10000);
    await waitFor(() => (readLockPid(realRoot) ?? 0) > 0, 8000);
    const daemonPid = readLockPid(realRoot)!;

    // Close the only client's stdin → proxy exits → daemon refcount hits 0 →
    // idle timer fires → daemon exits and cleans up its lockfile.
    server.child.stdin.end();

    expect(await waitProcessExit(daemonPid, 10000)).toBe(true);
    expect(fs.existsSync(path.join(realRoot, '.codegraph', 'daemon.pid'))).toBe(false);
  }, 30000);

  it('proxy survives the daemon dying mid-session and keeps serving (#662)', async () => {
    await indexLifecycleSymbol(realRoot);
    // The #662 scenario: an MCP host SIGTERM's the shared daemon while a session
    // is live. The proxy must NOT exit (losing CodeGraph for that session) — it
    // falls back to an in-process engine and keeps answering.
    const env = { CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '30000', CODEGRAPH_PPID_POLL_MS: '5000' };
    const server = spawnServer(tempDir, env);
    servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(server.stdout, 1), 20000, 25, 'initialize response');
    await waitFor(() => server.stderr.some((l) => l.includes('Attached to shared daemon')), 8000, 25, 'daemon attach log');
    await waitFor(() => (readLockPid(realRoot) ?? 0) > 0, 8000, 25, 'daemon pidfile');
    const daemonPid = readLockPid(realRoot)!;

    // A warm call goes through the daemon.
    sendMessage(server.child, { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    try {
      await waitFor(() => findResponse(server.stdout, 2), 30000, 25, 'warm tools/call via daemon');
    } catch (e) {
      // This is the wait that historically flaked — surface WHERE the request
      // died: proxy side (stderr) or daemon side (daemon.log).
      let daemonLog = '<no daemon.log>';
      try { daemonLog = readDaemonLog(realRoot).split('\n').slice(-25).join('\n'); } catch (error) { secondary(`diagnostic daemon log: ${String(error)}`); }
      throw new Error(
        `${(e as Error).message}\ndaemonAlive=${isAlive(daemonPid)} proxyAlive=${isAlive(server.child.pid!)}\n` +
        `--- proxy stderr tail ---\n${server.stderr.slice(-15).join('')}\n--- daemon.log tail ---\n${daemonLog}`
      );
    }

    await realExplore(server, 1000);
    const warmHistory = actualHistory(); expect(warmHistory.filter(record => record.root === realRoot)).toHaveLength(1);
    arm('stop-before-status', 'post-loss');
    // Kill the daemon out from under the live proxy; the actual NEXT candidate
    // is stopped natively before bootstrap status/ACK, not relabelled unknown.
    process.kill(daemonPid, 'SIGTERM');
    expect(await waitProcessExit(daemonPid, 8000)).toBe(true);

    // The proxy must still be alive and still answer — served in-process now.
    expect(isAlive(server.child.pid!)).toBe(true);
    await waitFor(() => server.stderr.some((l) => l.includes('serving this session in-process')), 8000, 25, 'in-process failover log');
    sendMessage(server.child, { jsonrpc: '2.0', id: 3, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    const resp = await waitFor(() => findResponse(server.stdout, 3), 15000);
    expect(resp.error).toBeUndefined();
    expect(JSON.stringify(resp.result)).toContain('CodeGraph Status');
    proveUnavailable(server, 'post-loss');
    expect(readWriterInfo(realRoot)).toMatchObject({ pid: server.child.pid, mode: 'fallback' });
    expect(trace().some(record => record.pid === server.child.pid && record.event === 'engine-diagnostics' && record.diagnostics.effective.executionMode === 'writable-fallback')).toBe(true);
    await realExplore(server, 1001);
    const history = actualHistory().filter(record => record.root === realRoot); expect(history).toHaveLength(2);
    expect(new Set(history.map(record => record.historyId)).size).toBe(2); // Real daemon session then real local session, no cross-session contamination.
    arm('observe', 'after-post-loss-fallback');
    expect(server.stdout.filter(line => { try { return JSON.parse(line).id === 3; } catch { return false; } })).toHaveLength(1);
    expect(isAlive(server.child.pid!)).toBe(true);
  }, 45000);

  it('a proxy serving in-process hands the writer lock back to a fresh daemon (#2277)', async () => {
    await indexLifecycleSymbol(realRoot);
    // After its daemon dies, the proxy's in-process engine takes writer.pid in
    // fallback mode. Every later daemon start used to fail on that lock for the
    // rest of the session, so every other session on the project ran read-only
    // without auto-sync. The degraded proxy now retries the daemon: it stops
    // its engine (releasing the lock), lets a daemon start, and proxies again.
    const env = {
      CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '30000',
      CODEGRAPH_PPID_POLL_MS: '5000',
      // The default, spelled out: long enough to observe the lockout first.
      CODEGRAPH_DAEMON_RETRY_MS: '5000',
    };
    const a = spawnServer(tempDir, env);
    servers.push(a);
    const proxyPid = a.child.pid!;
    sendInitialize(a.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(a.stdout, 1), 20000, 25, 'initialize response');
    await waitFor(() => a.stderr.some((l) => l.includes('Attached to shared daemon')), 8000, 25, 'first daemon attach');
    sendMessage(a.child, { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    await waitFor(() => findResponse(a.stdout, 2), 30000, 25, 'warm tools/call via daemon');
    const firstDaemon = readLockPid(realRoot)!;
    await realExplore(a, 1000);

    arm('stop-before-status', 'post-loss');
    process.kill(firstDaemon, 'SIGTERM');
    expect(await waitProcessExit(firstDaemon, 8000)).toBe(true);
    await waitFor(() => a.stderr.some((l) => l.includes('serving this session in-process')), 8000, 25, 'in-process failover');
    sendMessage(a.child, { jsonrpc: '2.0', id: 3, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    await waitFor(() => findResponse(a.stdout, 3), 15000, 25, 'in-process tools/call');

    proveUnavailable(a, 'post-loss');
    // Arm the NEXT actual candidate before the first local query: this is a
    // second one-use native pre-status failure, not synthetic null acquisition.
    arm('stop-before-status', 'local-recreation'); await realExplore(a, 1001);
    const firstLocal = trace().filter(record => record.pid === proxyPid && record.event === 'tool-start' && record.requestId === 1001);
    expect(firstLocal).toHaveLength(1);
    expect(firstLocal[0].diagnostics.effective.executionMode).toBe('writable-fallback');
    const firstGeneration = firstLocal[0].generation;
    expect(firstGeneration).toBe(firstLocal[0].diagnostics.ownership.generation);
    const firstLocalWriter = JSON.parse(fs.readFileSync(path.join(realRoot, '.codegraph/writer.pid'), 'utf8'));
    expect(firstLocalWriter).toMatchObject({ pid: proxyPid, mode: 'fallback' });
    let nextId = 4;
    const send = (): void => {
      sendMessage(a.child, { jsonrpc: '2.0', id: nextId++, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    };
    const stopTicker = ownedTicker(send, 250);
    await waitFor(() => trace().find(record => record.pid === proxyPid && record.event === 'candidate-close' &&
      trace().some(spawn => spawn.pid === proxyPid && spawn.event === 'candidate-spawn' && spawn.attempt === 'local-recreation' && spawn.candidatePid === record.candidatePid)), 15000, 25, 'second genuine unavailable native close');
    await waitFor(() => trace().find(record => record.pid === proxyPid && record.event === 'engine-constructor' &&
      record.diagnostics.effective.executionMode === 'writable-fallback' && record.diagnostics.ownership.generation !== firstGeneration), 15000, 25, 'actual replacement local engine');
    proveUnavailable(a, 'local-recreation'); arm('observe', 'handover');
    await realExplore(a, 1003);
    const localCalls = trace().filter(record => record.pid === proxyPid && record.event === 'tool-start' && [1001,1003].includes(record.requestId));
    expect(localCalls).toHaveLength(2);
    expect(localCalls.map(record => record.diagnostics.effective.executionMode)).toEqual(['writable-fallback','writable-fallback']);
    expect(new Set(localCalls.map(record => record.generation)).size).toBe(2);
    expect(new Set(localCalls.map(record => record.sessionId)).size).toBe(1);
    const retirement = trace().find(record => record.pid === proxyPid && record.event === 'engine-drain-complete' && record.generation === firstGeneration);
    expect(retirement).toBeDefined();
    const replacement = trace().find(record => record.pid === proxyPid && record.event === 'engine-constructor' && record.diagnostics.ownership.generation === localCalls[1].generation);
    expect(retirement.sequence).toBeLessThan(replacement.sequence);
    const secondLocalWriter = JSON.parse(fs.readFileSync(path.join(realRoot, '.codegraph/writer.pid'), 'utf8'));
    expect(secondLocalWriter).toMatchObject({ pid: proxyPid, mode: 'fallback' });
    // The same proxy must relinquish the first native reservation after drain,
    // then acquire a NEW writer for the actual replacement engine. Keeping the
    // old live-PID record would silently downgrade this session to read-only.
    expect(secondLocalWriter.startedAt).toBeGreaterThan(firstLocalWriter.startedAt);
    const localHistory = actualHistory().filter(record => record.pid === proxyPid && record.root === realRoot);
    expect(localHistory.map(record => record.index)).toEqual([1,2]);
    expect(new Set(localHistory.map(record => record.historyId)).size).toBe(1);
    current.observations.push({ event: 'same-local-history-over-actual-writable-retirement',
      generations: localCalls.map(record => record.generation), sessionId: localCalls[0].sessionId,
      retiredGeneration: retirement.generation, localHistoryId: localHistory[0].historyId, indexes: localHistory.map(record => record.index) });
    // The lockout: the in-process engine owns the project's writer slot.
    expect(readWriterInfo(realRoot)).toMatchObject({ pid: proxyPid, mode: 'fallback' });

    // Another session starting now cannot get a daemon while that lock is held.
    const b = spawnServer(tempDir, env);
    servers.push(b);
    sendInitialize(b.child, `file://${tempDir}`, 1);
    await waitFor(() => findResponse(b.stdout, 1), 20000, 25, 'second session initialize');

    // Keep calling through the handover: every request gets exactly one reply.
    // The same owned ticker spans local recreation and the original handover.
    let daemonWriter: { pid: number; mode: string } | null = null;
    try {
      daemonWriter = await waitFor(() => {
        const w = readWriterInfo(realRoot);
        return w && w.mode === 'daemon' && w.pid !== proxyPid && isAlive(w.pid) ? w : null;
      }, 30000, 25, 'a daemon to own the writer lock');
      await waitFor(
        () => a.stderr.some((l) => l.includes(`Attached to shared daemon`) && l.includes(`(pid ${daemonWriter!.pid},`)),
        15000, 25, 'the proxy to reattach to the new daemon',
      );
    } finally {
      stopTicker();
    }
    send();
    const lastId = nextId - 1;
    await waitFor(() => findResponse(a.stdout, lastId), 15000, 25, 'a tools/call through the new daemon');
    for (let id = 2; id <= lastId; id++) {
      const replies = a.stdout.filter((line) => {
        try { const m = JSON.parse(line); return m.id === id && ('result' in m || 'error' in m); } catch { return false; }
      });
      expect(replies, `replies to request ${id}`).toHaveLength(1);
      expect(JSON.parse(replies[0]).error, `request ${id}`).toBeUndefined();
    }
    expect(readLockPid(realRoot)).toBe(daemonWriter!.pid);
    expect(daemonWriter!.pid).not.toBe(firstDaemon);
    const branch = trace().filter(record => record.event === 'engine-diagnostics').map(record => record.diagnostics);
    expect(branch.some(value => value.ownership.pid === daemonWriter!.pid && value.effective.executionMode === 'shared-daemon')).toBe(true);
    const localEngines = trace().filter(record => record.pid === proxyPid && record.event === 'engine-constructor');
    expect(localEngines.map(record => record.diagnostics.effective.executionMode)).toEqual(['writable-fallback', 'writable-fallback']);
    const finalDrain = trace().find(record => record.pid === proxyPid && record.event === 'engine-drain-complete' && record.generation === localCalls[1].generation);
    const finalCandidate = trace().find(record => record.pid === proxyPid && record.event === 'candidate-spawn' && record.attempt === 'handover');
    expect(finalDrain).toBeDefined();
    const localGenerations = new Set(localCalls.map(record => record.generation));
    const localWorkers = trace().filter(record => record.pid === proxyPid && record.event === 'worker-spawn' && localGenerations.has(record.execution.generation));
    expect(new Set(localWorkers.map(record => record.execution.generation))).toEqual(localGenerations);
    const workerKey = (record: any) => record.pid + ':' + record.workerThreadId;
    const localWorkerKeys = localWorkers.map(workerKey).sort();
    expect(new Set(localWorkerKeys).size).toBe(localWorkerKeys.length);
    const localExits = trace().filter(record => record.pid === proxyPid && record.event === 'worker-exit' && localWorkerKeys.includes(workerKey(record)));
    expect(localExits.map(workerKey).sort()).toEqual(localWorkerKeys);
    expect(localExits.every(record => record.code === 0 || record.code === 1)).toBe(true);
    // Another real session may win daemon election; when this proxy spawns
    // the candidate itself, its native drain must precede that exact spawn.
    if (finalCandidate) expect(finalDrain.sequence).toBeLessThan(finalCandidate.sequence);

    // The session that started during the lockout ends up on the shared daemon too.
    await waitFor(
      () => b.stderr.some((l) => l.includes('Attached to shared daemon') && l.includes(`(pid ${daemonWriter!.pid},`)),
      30000, 25, 'the second session to attach',
    );
    sendMessage(b.child, { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    const bReply = await waitFor(() => findResponse(b.stdout, 2), 15000, 25, 'second session tools/call');
    expect(bReply.error).toBeUndefined();
    await realExplore(a, 1002); await realExplore(b, 1000);
    const history = actualHistory().filter(record => record.root === realRoot); expect(history).toHaveLength(5);
    expect(new Set(history.map(record => record.historyId)).size).toBe(4); // Old daemon, SAME local state across two engines, then two new daemon sessions.
  }, 90000);

  it('a read-only in-process session moves to the shared daemon once one can start (#2277)', async () => {
    fs.writeFileSync(path.join(realRoot, 'app.ts'), 'export function handoverSymbol() { return 42; }\n');
    const graph = CodeGraph.openSync(realRoot); try { await graph.indexAll(); } finally { graph.close(); }
    const owner = await ordinaryWriter(realRoot), before = snapshot(realRoot);
    arm('stop-before-status');
    const diagnostic = path.join(tempDir, '.handover-history.jsonl');
    const server = spawnServer(tempDir, {
      CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '30000', CODEGRAPH_DAEMON_RETRY_MS: '300', CODEGRAPH_DAEMON_RETRY_MAX_MS: '1000',
      CODEGRAPH_EXPLORE_DEBUG: diagnostic,
    }); servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    expect((await waitFor(() => findResponse(server.stdout, 1), 10000)).result.serverInfo.name).toBe('codegraph');
    await waitFor(() => server.stderr.some(line => line.includes('serving this session in-process')), 10000, 25, 'genuine unavailable in-process fallback');
    sendMessage(server.child, { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    const local = await waitFor(() => findResponse(server.stdout, 2), 10000, 25, 'read-only tools/call');
    expect(local.error).toBeUndefined();
    expect(server.stderr.some(line => line.includes('Serving reads in-process without auto-sync'))).toBe(true);
    proveUnavailable(server); arm('observe', 'ordinary-writer-retries');
    expect(readLockPid(realRoot)).toBeNull();
    expect(readWriterInfo(realRoot)).toMatchObject({ pid: owner.child.pid, mode: 'fallback' });
    const localPolicy = trace().find(record => record.pid === server.child.pid && record.event === 'engine-diagnostics')?.diagnostics;
    expect(localPolicy.effective).toMatchObject({ executionMode: 'read-only-fallback', concurrency: { kind: 'disabled' } });
    expect(localPolicy.ownership).toMatchObject({ pid: server.child.pid, writerAuthority: false, synchronizationAuthority: false });
    sendMessage(server.child, { jsonrpc: '2.0', id: 20, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query: 'handoverSymbol' } } });
    const first = await waitFor(() => findResponse(server.stdout, 20), 10000); expect(first.error).toBeUndefined(); expect(first.result.content[0].text).toContain('function handoverSymbol');
    // Repeated actual permitted candidates cannot get the ordinary writer's
    // slot. No old/incompatible peer is treated as a recoverable blocker.
    await waitFor(() => trace().filter(record => record.pid === server.child.pid && record.event === 'candidate-spawn').length >= 3, 10000, 25, 'fresh retries beside ordinary writer');
    expect(countListeningLines(realRoot)).toBe(0); expect(snapshot(realRoot)).toEqual(before);
    // RO retry retains this same engine until successful attachment; this is
    // local history over retries, NOT an engine-retirement assertion.
    sendMessage(server.child, { jsonrpc: '2.0', id: 22, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query: 'handoverSymbol' } } });
    const againLocal = await waitFor(() => findResponse(server.stdout, 22), 10000); expect(againLocal.error).toBeUndefined(); expect(againLocal.result.isError).toBeFalsy();
    await stopOrdinary(owner); // Typed actual engine.stop → native0/null + lease release.
    const attached = await waitFor(() => server.stderr.find(line => line.includes('Attached to shared daemon')), 30000, 25, 'fresh qualified service after actual ordinary-owner close');
    const daemonPid = readLockPid(realRoot)!;
    expect(attached).toContain(`(pid ${daemonPid},`); expect(daemonPid).not.toBe(owner.child.pid);
    expect(readWriterInfo(realRoot)).toMatchObject({ pid: daemonPid, mode: 'daemon' });
    sendMessage(server.child, { jsonrpc: '2.0', id: 3, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    const through = await waitFor(() => findResponse(server.stdout, 3), 15000, 25, 'tools/call through daemon');
    expect(through.error).toBeUndefined(); expect(JSON.stringify(through.result)).toContain('CodeGraph Status');
    sendMessage(server.child, { jsonrpc: '2.0', id: 21, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query: 'handoverSymbol' } } });
    const second = await waitFor(() => findResponse(server.stdout, 21), 15000); expect(second.error).toBeUndefined(); expect(second.result.content[0].text).toContain('function handoverSymbol');
    const reports = bounded(diagnostic).toString('utf8').trim().split(/\r?\n/).map(line => JSON.parse(line));
    // Actual old local and fresh daemon MCPSession histories are separate
    // under the accepted contract; never reset an existing live local state.
    expect(reports.map(value => value.session.callIndex)).toEqual([1,2,1]); expect(reports[1].session.priorCalls).toBe(1);
    const history = actualHistory().filter(record => record.root === realRoot); expect(history).toHaveLength(3);
    expect(new Set(history.map(record => record.historyId)).size).toBe(2);
    for (const id of [2,3,20,21,22]) expect(server.stdout.filter(line => { try { return JSON.parse(line).id === id; } catch { return false; } })).toHaveLength(1);
    expect(trace().some(record => record.event === 'engine-diagnostics' && record.diagnostics.ownership.pid === daemonPid && record.diagnostics.effective.executionMode === 'shared-daemon')).toBe(true);
  }, 60000);

  it('post-loss compatible reattachment stays distinct from genuine unavailable fallback', async () => {
    await indexLifecycleSymbol(realRoot);
    const server = spawnServer(tempDir, { CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '30000' }); servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1); await waitFor(() => findResponse(server.stdout, 1), 10000);
    await waitFor(() => server.stderr.some(line => line.includes('Attached to shared daemon')), 8000);
    const first = readLockPid(realRoot)!;
    sendMessage(server.child, { jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    expect((await waitFor(() => findResponse(server.stdout, 2), 15000)).error).toBeUndefined();
    await realExplore(server, 1000);
    process.kill(first, 'SIGTERM'); expect(await waitProcessExit(first, 8000)).toBe(true);
    sendMessage(server.child, { jsonrpc: '2.0', id: 3, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    const reply = await waitFor(() => findResponse(server.stdout, 3), 15000); expect(reply.error).toBeUndefined(); expect(JSON.stringify(reply.result)).toContain('CodeGraph Status');
    const replacement = readLockPid(realRoot)!; expect(replacement).not.toBe(first); expect(isAlive(replacement)).toBe(true);
    expect(readWriterInfo(realRoot)).toMatchObject({ pid: replacement, mode: 'daemon' });
    expect(server.stderr.some(line => line.includes('serving this session in-process'))).toBe(false);
    expect(trace().some(record => record.pid === server.child.pid && record.event === 'engine-constructor')).toBe(false);
    expect(trace().some(record => record.event === 'engine-diagnostics' && record.diagnostics.ownership.pid === replacement && record.diagnostics.effective.executionMode === 'shared-daemon')).toBe(true);
    expect(server.stdout.filter(line => { try { return JSON.parse(line).id === 3; } catch { return false; } })).toHaveLength(1);
    await realExplore(server, 1001);
    const history = actualHistory().filter(record => record.root === realRoot); expect(history).toHaveLength(2);
    expect(new Set(history.map(record => record.historyId)).size).toBe(2);
  }, 45000);

  it('native held pre-service preparation exposes only static discovery before permission', async () => {
    const before = snapshot(realRoot); arm('hold-before-status');
    const server = spawnServer(tempDir, { CODEGRAPH_QUERY_MODE: 'unlimited', CODEGRAPH_QUERY_POOL_SIZE: '2', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited' }); servers.push(server);
    sendInitialize(server.child, `file://${tempDir}`, 1);
    const initialized = await waitFor(() => findResponse(server.stdout, 1), 3000);
    expect(initialized.result.instructions).toContain('verification is pending');
    sendMessage(server.child, { jsonrpc: '2.0', id: 2, method: 'tools/list' });
    expect((await waitFor(() => findResponse(server.stdout, 2), 3000)).result.tools.length).toBeGreaterThan(0);
    await waitFor(() => trace().find(record => record.pid === server.child.pid && record.event === 'candidate-pre-service-barrier'), 3000);
    zeroRequesterWork(server, true); expect(snapshot(realRoot)).toEqual(before);
    fs.writeFileSync(`${current.control}.initial.release`, 'release actual CLI startup');
    await terminal(server, 'unsupported-mode', realRoot, 3);
    await proveTypedUnsupported(server, realRoot);
    zeroRequesterWork(server, true); expect(snapshot(realRoot)).toEqual(before);
    await closedClient(server);
    zeroRequesterWork(server, true); expect(snapshot(realRoot)).toEqual(before);
  }, 30000);

});

function readWriterInfo(root: string): { pid: number; mode: string } | null {
  try {
    const info = JSON.parse(bounded(path.join(root, '.codegraph', 'writer.pid'), 65536).toString('utf8'));
    if (typeof info.pid !== 'number' || typeof info.mode !== 'string') throw new Error('Malformed writer owner record');
    return info;
  } catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') secondary(`writer owner read: ${String(error)}`); return null; }
}
