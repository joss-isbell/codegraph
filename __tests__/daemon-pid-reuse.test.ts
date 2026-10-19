// Preserve successful PID-reuse recovery alongside the live-lock guards in #1850.
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
import * as os from 'os';
import * as path from 'path';
import { CodeGraph } from '../src';
import { getDaemonSocketPath } from '../src/mcp/daemon-paths';
import { CodeGraphPackageVersion } from '../src/mcp/version';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { StringDecoder } from 'string_decoder';

const BIN = path.resolve(__dirname, '../dist/bin/codegraph.js');
const OBSERVER = path.resolve(__dirname, 'fixtures/query-daemon-reconciliation.cjs');
const MAX_STREAM = 8 * 1024 * 1024;
type NativeExit = { code: number | null; signal: NodeJS.Signals | null; at: number };

interface SpawnedServer {
  name: string;
  child: ChildProcessWithoutNullStreams;
  stdout: string[];
  stderr: string[];
  raw: Buffer[];
  seen: number[];
  eof: boolean[];
  errors: string[];
  pending: string[];
  exit?: NativeExit;
  close?: NativeExit;
}

interface CaseEvidence {
  root: string;
  directory: string;
  trace: string;
  control: string;
  servers: SpawnedServer[];
  stages: object[];
  served: Set<number>;
  secondary: string[];
  primary?: string;
  closing: boolean;
}
let evidence: CaseEvidence;
const detail = (error: unknown) => error instanceof Error ? error.stack ?? error.message : String(error);

/** Bounded exact bytes, including missing/read-error distinctions. Originals stay in a failed fixture. */
function captureFile(file: string, maximum = 65536) {
  let fd: number | undefined, bytes = Buffer.alloc(0), seenBytes = 0, error: string | null = null;
  let present = false;
  try {
    fd = fs.openSync(file, 'r'); present = true;
    const stat = fs.fstatSync(fd); seenBytes = stat.size;
    if (!stat.isFile()) throw new Error(`Not a regular evidence file: ${file}`);
    const buffer = Buffer.alloc(Math.min(seenBytes, maximum));
    let retained = 0;
    while (retained < buffer.length) {
      const count = fs.readSync(fd, buffer, retained, buffer.length - retained, retained);
      if (!count) break;
      retained += count;
    }
    bytes = buffer.subarray(0, retained);
    if (retained !== seenBytes) throw new Error(`Incomplete/bounded evidence capture: ${file} (${retained}/${seenBytes})`);
  } catch (value) { if ((value as NodeJS.ErrnoException).code !== 'ENOENT' || present) error = detail(value); }
  finally { if (fd !== undefined) try { fs.closeSync(fd); } catch (value) { error ??= detail(value); } }
  return { file, present, seenBytes, retainedBytes: bytes.length, base64: bytes.toString('base64'), error };
}

/** Same exact-root delegated-spawn namespace used by mcp-daemon.test.ts. No lock PID is cleanup authority. */
function ownedCandidates(): Array<{ candidatePid: number; launcherPid: number; root: string }> {
  const files = fs.readdirSync(evidence.trace).filter(file => /^candidate-spawn-\d+\.json$/.test(file));
  if (files.length > 128) throw new Error('PID-reuse candidate registry overflow');
  return files.map(file => {
    const capture = captureFile(path.join(evidence.trace, file));
    if (capture.error || !capture.present) throw new Error(capture.error ?? `Missing registry ${file}`);
    const value = JSON.parse(Buffer.from(capture.base64, 'base64').toString('utf8'));
    if (value.identity !== 'actual-delegated-child-spawn' || !Number.isSafeInteger(value.candidatePid)
      || value.candidatePid <= 0 || value.candidatePid === process.pid || value.root !== evidence.root
      || value.command !== process.execPath || file !== `candidate-spawn-${value.candidatePid}.json`
      || !evidence.servers.some(server => server.child.pid === value.launcherPid)) {
      throw new Error(`Unowned PID-reuse candidate registry: ${file}`);
    }
    return value;
  });
}

function nativeFacts(): object[] {
  const files = fs.readdirSync(evidence.trace).filter(file => /\.jsonl$|\.failure$/.test(file));
  if (files.length > 128) throw new Error('PID-reuse native trace inventory overflow');
  let total = 0;
  return files.flatMap(file => {
    const capture = captureFile(path.join(evidence.trace, file), MAX_STREAM);
    total += capture.seenBytes;
    if (capture.error || total > 64 * 1024 * 1024) throw new Error(capture.error ?? 'PID-reuse native trace total overflow');
    const raw = Buffer.from(capture.base64, 'base64').toString('utf8');
    if (file.endsWith('.failure')) throw new Error(`Native observer failure ${file}: ${raw}`);
    // Raw trace files remain alongside the receipt; summarize actual lifecycle facts, never synthesize missing events.
    return raw.split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line))
      .filter(record => /^(candidate-|process-exit|owned-daemon-stop)/.test(record.event));
  });
}

function observe(stage: string, facts: object = {}): void {
  const owners = ['daemon.pid', 'writer.pid'].map(name => captureFile(path.join(evidence.root, '.codegraph', name)));
  for (const capture of owners) if (capture.error) evidence.secondary.push(capture.error);
  try {
    const owner = owners[0].present && !owners[0].error
      ? JSON.parse(Buffer.from(owners[0].base64, 'base64').toString('utf8')) : null;
    if (owner && ownedCandidates().some(value => value.candidatePid === owner.pid)) evidence.served.add(owner.pid);
  } catch (error) { evidence.secondary.push(`owner observation: ${detail(error)}`); }
  if (evidence.stages.length < 64) evidence.stages.push({ stage, at: Date.now(), monotonicNs: process.hrtime.bigint().toString(), owners, ...facts });
  else evidence.secondary.push('PID-reuse stage overflow');
  persistEvidence(stage);
}

/** Persistence never replaces the primary assertion/timeout; each failure is sticky and prevents disposal. */
function persistEvidence(stage: string): void {
  const servers = evidence.servers.map(server => ({ name: server.name, pid: server.child.pid, exit: server.exit ?? null,
    close: server.close ?? null, eof: server.eof, seenBytes: server.seen, retainedBytes: server.raw.map(value => value.length),
    errors: server.errors, pending: server.pending, stdout: server.stdout, stderr: server.stderr,
    replies: server.stdout.flatMap(line => {
      try { const value = JSON.parse(line); return value?.id !== undefined && (value.result !== undefined || value.error !== undefined) ? [{ raw: line, value }] : []; }
      catch { return []; } // Non-RPC output remains in the exact raw capture and stdout ledger.
    }),
    rawFiles: ['stdout', 'stderr'].map(stream => path.join(evidence.directory, `${server.name}-${stream}.bin`)) }));
  let native: object[] = [];
  try { native = nativeFacts(); } catch (error) { evidence.secondary.push(`native evidence: ${detail(error)}`); }
  const daemonLog = captureFile(path.join(evidence.root, '.codegraph', 'daemon.log'), MAX_STREAM);
  if (daemonLog.error) evidence.secondary.push(daemonLog.error);
  const receipt = { stage, fixture: evidence.root, directory: evidence.directory, trace: evidence.trace,
    primary: evidence.primary ?? null, secondary: evidence.secondary, stages: evidence.stages,
    knownServedPids: [...evidence.served], servers, native, daemonLog };
  try {
    for (const server of evidence.servers) for (const [index, stream] of ['stdout', 'stderr'].entries()) {
      fs.writeFileSync(path.join(evidence.directory, `${server.name}-${stream}.bin`), server.raw[index]);
    }
    fs.writeFileSync(path.join(evidence.directory, 'receipt.json'), JSON.stringify(receipt, null, 2));
  } catch (error) {
    evidence.secondary.push(`evidence persistence: ${detail(error)}`);
    // Independent fallback retains the actual stream bytes if the selected output becomes unwritable.
    try { fs.writeFileSync(path.join(evidence.root, 'pid-reuse-evidence.json'), JSON.stringify({ ...receipt,
      raw: evidence.servers.map(server => ({ name: server.name, base64: server.raw.map(bytes => bytes.toString('base64')) })) }, null, 2)); }
    catch (fallback) { evidence.secondary.push(`evidence fallback: ${detail(fallback)}`); }
    process.stderr.write(`PID-reuse evidence persistence failed; fixture retained: ${evidence.root}\n`);
  }
}

function spawnServer(cwd: string, env: NodeJS.ProcessEnv, name: string): SpawnedServer {
  if (evidence.closing) throw new Error('No PID-reuse child after cleanup begins');
  // Passing the runtime flags avoids an untracked CLI re-exec grandchild on Windows.
  const child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, '--require', OBSERVER, BIN, 'serve', '--mcp'], {
    cwd,
    windowsHide: true,
    stdio: ['pipe', 'pipe', 'pipe'],
    // #618: the daemon-attach log line is now off by default; opt the test
    // harness into it (CODEGRAPH_MCP_LOG_ATTACH=1) so the attach assertions
    // below can still observe a successful attach. A per-test env still wins.
    env: { CODEGRAPH_MCP_LOG_ATTACH: '1', ...process.env, ...env,
      CG_DAEMON_RECON_TRACE: evidence.trace, CG_DAEMON_RECON_CONTROL: evidence.control,
      CG_DAEMON_RECON_ROOTS: JSON.stringify([evidence.root]) },
  }) as ChildProcessWithoutNullStreams;
  const stdout: string[] = [];
  const stderr: string[] = [];
  const decoders = [new StringDecoder('utf8'), new StringDecoder('utf8')];
  const server: SpawnedServer = { name, child, stdout, stderr, raw: [Buffer.alloc(0), Buffer.alloc(0)],
    seen: [0, 0], eof: [false, false], errors: [], pending: ['', ''] };
  child.on('error', error => { server.errors.push(`child: ${detail(error)}`); });
  child.stdin.on('error', error => { server.errors.push(`stdin: ${detail(error)}`); });
  [child.stdout, child.stderr].forEach((stream, index) => {
    const lines = index === 0 ? stdout : stderr;
    const decode = (text: string) => {
      server.pending[index] += text;
      let lf: number;
      while ((lf = server.pending[index].indexOf('\n')) !== -1) {
        lines.push(server.pending[index].slice(0, lf)); server.pending[index] = server.pending[index].slice(lf + 1);
      }
    };
    stream.on('data', (chunk: Buffer) => {
      server.seen[index] += chunk.length;
      const kept = chunk.subarray(0, Math.max(0, MAX_STREAM - server.raw[index].length));
      server.raw[index] = Buffer.concat([server.raw[index], kept]);
      decode(decoders[index].write(kept));
      if (server.seen[index] > MAX_STREAM && !server.errors.includes('stream capture overflow')) server.errors.push('stream capture overflow');
      if (evidence.closing) persistEvidence(`${name} late native data`);
    });
    stream.once('end', () => {
      server.eof[index] = true; decode(decoders[index].end());
      if (server.pending[index]) lines.push(server.pending[index]);
      if (evidence.closing) persistEvidence(`${name} native EOF`);
    });
    stream.on('error', error => { server.errors.push(`${index ? 'stderr' : 'stdout'}: ${detail(error)}`); });
  });
  child.once('exit', (code, signal) => { server.exit = { code, signal, at: Date.now() }; });
  child.once('close', (code, signal) => {
    server.close = { code, signal, at: Date.now() }; persistEvidence(`${name} actual ChildProcess.close`);
  });
  return server;
}

function sendMessage(child: ChildProcessWithoutNullStreams, msg: unknown): void {
  if (evidence.closing) throw new Error('No PID-reuse request after cleanup begins');
  try { child.stdin.write(JSON.stringify(msg) + '\n'); } catch { /* child may be gone */ }
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
  label = '',
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
        return reject(new Error(`Timed out after ${timeoutMs}ms${label ? ` waiting for: ${label}` : ''}`));
      }
      setTimeout(tick, pollMs);
    };
    tick();
  });
}

function isAlive(pid: number): boolean {
  try { process.kill(pid, 0); return true; } catch (error) {
    if ((error as NodeJS.ErrnoException).code === 'EPERM') return true;
    if ((error as NodeJS.ErrnoException).code === 'ESRCH') return false;
    throw error; // An ambiguous probe is not evidence of native exit.
  }
}

function readLockPid(root: string): number | null {
  try {
    const raw = fs.readFileSync(path.join(root, '.codegraph', 'daemon.pid'), 'utf8');
    const info = JSON.parse(raw);
    return typeof info.pid === 'number' ? info.pid : null;
  } catch { return null; }
}

function readDaemonLog(root: string): string {
  try { return fs.readFileSync(path.join(root, '.codegraph', 'daemon.log'), 'utf8'); }
  catch { return ''; }
}

function countListeningLines(root: string): number {
  return readDaemonLog(root).split('\n').filter((l) => l.includes('[CodeGraph daemon] Listening on')).length;
}

async function observedWait<T>(label: string, predicate: () => T | undefined | null | false, timeoutMs: number): Promise<T> {
  const started = Date.now(); observe(`${label}: start`, { timeoutMs });
  try {
    const result = await waitFor(() => {
      if (evidence.closing) throw new Error(`Case stopped awaiting ${label}; cleanup has begun`);
      return predicate();
    }, timeoutMs, 25, label);
    observe(`${label}: complete`, { elapsedMs: Date.now() - started }); return result;
  } catch (error) {
    observe(`${label}: failed`, { elapsedMs: Date.now() - started, error: detail(error) }); throw error;
  }
}

async function waitProcessExit(pid: number, timeoutMs: number): Promise<boolean> {
  return observedWait('first daemon native absence after SIGKILL', () => !isAlive(pid), timeoutMs)
    .then(() => true).catch(() => false);
}

describe('Shared MCP daemon (issue #411)', () => {
  let tempDir: string;   // the (possibly symlinked) path processes are spawned with
  let realRoot: string;  // its canonical form — what the daemon keys paths on
  const servers: SpawnedServer[] = [];

  beforeEach(async () => {
    servers.length = 0;
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-mcp-daemon-'));
    realRoot = fs.realpathSync(tempDir);
    // Ordinary CI gets its own retained sibling, not a shared filename or a directory the fixture cleanup removes.
    const directory = fs.mkdtempSync(path.join(process.env.VALIDATION_OUT ?? path.dirname(realRoot), 'daemon-pid-reuse-evidence-'));
    evidence = { root: realRoot, directory, trace: path.join(directory, 'trace'), control: path.join(directory, 'control.json'),
      servers, stages: [], served: new Set(), secondary: [], closing: false };
    fs.mkdirSync(evidence.trace); fs.writeFileSync(evidence.control, JSON.stringify({ fault: 'observe' }));
    observe('fixture created');
    const cg = await CodeGraph.init(tempDir);
    cg.close(); observe('fixture initialized');
  });

  afterEach(async context => {
    if (!evidence) return;
    evidence.closing = true;
    evidence.primary ??= context.task.result?.errors?.map(error => detail(error)).join('\n') || undefined;
    observe('afterEach before owned cleanup');
    const cleanup: string[] = [];
    const attempt = async (label: string, operation: () => Promise<unknown> | void) => {
      try { await operation(); }
      catch (error) { cleanup.push(`${label}: ${detail(error)}`); }
    };
    // Stop launchers first so cleanup cannot create another candidate. Await actual close+EOF, not a 50ms sleep.
    await Promise.all(servers.map(server => attempt(`${server.name} native close`, async () => {
      try {
        if (!server.close) server.child.stdin.end();
        await waitFor(() => server.close, 5000, 25, `${server.name} graceful ChildProcess.close`);
      }
      catch (error) {
        cleanup.push(`${server.name} graceful close: ${detail(error)}`);
        if (!server.close) server.child.kill('SIGKILL'); // The actual tracked ChildProcess, never a PID from a lock.
        await waitFor(() => server.close, 5000, 25, `${server.name} forced ChildProcess.close`);
        // The first graceful failure stays in cleanup even if forced recovery succeeds or fails separately.
      }
      if (!server.exit || !server.eof.every(Boolean)) throw new Error(`${server.name} missing native exit or stream EOF`);
      if (!server.close || server.close.code !== 0 || server.close.signal !== null) throw new Error(`${server.name} unsuccessful native close: ${JSON.stringify(server.close)}`);
    })));
    observe('afterEach proxies closed');
    let candidates: ReturnType<typeof ownedCandidates> = [];
    await attempt('exact delegated candidate inventory', () => { candidates = ownedCandidates(); });
    await Promise.all(candidates.filter(value => evidence.served.has(value.candidatePid)).map(value =>
      attempt(`owned served daemon ${value.candidatePid} stop`, async () => {
        if (!isAlive(value.candidatePid)) return;
        // The observer's daemon checks its OWN root/PID before delegating Daemon.stop.
        // Neither the planted live worker nor an external/reused PID receives a signal.
        fs.writeFileSync(path.join(evidence.trace, `stop-daemon-${value.candidatePid}.json`), JSON.stringify({
          kind: 'stop-owned-daemon', root: value.root, pid: value.candidatePid,
        }));
        await waitFor(() => !isAlive(value.candidatePid), 5000, 25, `owned served daemon ${value.candidatePid} native absence`);
      })));
    await attempt('all own candidates absent', async () => {
      await waitFor(() => candidates.every(value => !isAlive(value.candidatePid)), 5000, 25, 'all exact delegated candidates natively absent');
    });
    evidence.secondary.push(...cleanup, ...servers.flatMap(server => server.errors.map(error => `${server.name} capture: ${error}`)));
    observe('afterEach owned cleanup settled', { cleanup });
    const failed = evidence.primary || context.task.result?.state === 'fail' || evidence.secondary.length;
    if (!failed) {
      const cleanupCount = cleanup.length;
      await attempt('successful fixture disposal', () => {
        const absolute = path.resolve(realRoot), stat = fs.lstatSync(absolute);
        if (path.dirname(absolute) !== fs.realpathSync(os.tmpdir()) || !path.basename(absolute).startsWith('codegraph-mcp-daemon-')
          || !stat.isDirectory() || stat.isSymbolicLink() || fs.realpathSync(absolute) !== absolute) {
          throw new Error(`Refusing unsafe PID-reuse fixture disposal: ${realRoot}`);
        }
        fs.rmSync(absolute, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
      });
      evidence.secondary.push(...cleanup.slice(cleanupCount));
    }
    // A failed root is never disposed. Do not overwrite the final pre-disposal log capture with a missing file on success.
    if (fs.existsSync(realRoot)) observe('afterEach failed fixture retained', { fixtureRetained: true });
    process.stderr.write(`PID-reuse evidence: ${evidence.directory}; fixture ${fs.existsSync(realRoot) ? 'retained' : 'removed'}: ${realRoot}\n`);
    // Keep the single case's capture owners reachable: late native events after a failed cleanup update the same receipt.
    expect(evidence.secondary).toEqual([]);
  }, 30000);

  it.each([CodeGraphPackageVersion, '0.0.1'])('takes over after SIGKILL even when the stale PID has been reused (#1553) [stale record version %s]', async version => {
    try {
      const env = { CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '30000' };
      const first = spawnServer(tempDir, env, 'first');
      servers.push(first);
      sendInitialize(first.child, `file://${tempDir}`, 1);
      await observedWait('first initialize response (id 1)', () => findResponse(first.stdout, 1), 10000);
      await observedWait('first daemon Listening line', () => countListeningLines(realRoot) >= 1, 10000);
      const killedPid = readLockPid(realRoot)!;

      if (!ownedCandidates().some(value => value.candidatePid === killedPid)) throw new Error('First daemon is not an exact owned delegated candidate');
      observe('before original first-daemon SIGKILL', { killedPid });
      process.kill(killedPid, 'SIGKILL');
      expect(await waitProcessExit(killedPid, 8000)).toBe(true);

      // Model OS PID reuse without risking another process: the stale lock now
      // names this live vitest worker, but no daemon answers the leftover socket.
      fs.writeFileSync(
        path.join(realRoot, '.codegraph', 'daemon.pid'),
        JSON.stringify({
          pid: process.pid,
          version,
          socketPath: getDaemonSocketPath(realRoot),
          startedAt: Date.now() - 60_000,
        }),
      );
      observe('planted live Vitest PID in stale daemon record', { killedPid, plantedPid: process.pid });

      const second = spawnServer(tempDir, env, 'second');
      servers.push(second);
      sendInitialize(second.child, `file://${tempDir}`, 2);
      const response = await observedWait('second initialize response (id 2)', () => findResponse(second.stdout, 2), 12000);
      expect(response.result.serverInfo.name).toBe('codegraph');
      // initialize can succeed even when default-project admission is refused.
      // Observe the actual public answer BEFORE the original replacement wait; do not assert a new policy expectation.
      sendMessage(second.child, { jsonrpc: '2.0', id: 3, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
      const status = await observedWait('second codegraph_status tools/call response (id 3)', () => findResponse(second.stdout, 3), 10000);
      observe('second public status reply before original Listening wait', { response: status });
      await observedWait('second daemon Listening line (original replacement expectation)', () => countListeningLines(realRoot) >= 2, 10000);

      const replacementPid = readLockPid(realRoot)!;
      expect(replacementPid).not.toBe(killedPid);
      expect(replacementPid).not.toBe(process.pid);
      expect(isAlive(replacementPid)).toBe(true);
      expect(isAlive(process.pid)).toBe(true);
      observe('original replacement and live-worker assertions passed', { killedPid, replacementPid, plantedPid: process.pid });
    } catch (error) {
      evidence.primary ??= detail(error); observe('body failed'); throw error;
    }
  }, 50000);

});
