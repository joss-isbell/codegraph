/**
 * Explicit daemon management and ordinary acquisition of another version.
 *
 * A daemon keeps running the code it started with. The 1.6.1 daemon in #2335
 * outlived an upgrade to 1.6.2, went on loading grammars from the install the
 * upgrade removed, and wrote every file its watcher re-indexed as empty. A
 * launcher must not solve this by signaling that daemon or serving around its
 * unproved policy. Ordinary acquisition now refuses an older, newer or unknown
 * service without changing the owner, its index or locks. A failed usual socket
 * probe is not permission to replace a live owner recorded in the project lock.
 * The explicit stopOlderDaemon utility and writer handover are still tested
 * independently; only a separately authorized management action may use them.
 *
 * The end-to-end cases run real daemons of other versions: this build's dist/
 * linked into a temp "install" whose package.json carries another version. A
 * daemon resolves its version, and every grammar it loads, from its own files,
 * so that install's daemon advertises that version in its lock and its hello.
 */
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { ChildProcessWithoutNullStreams, execFileSync, spawn } from 'child_process';
import { once } from 'events';
import * as fs from 'fs';
import * as net from 'net';
import * as os from 'os';
import * as path from 'path';
import { createHash } from 'crypto';
import { isUtf8 } from 'buffer';
import { StringDecoder } from 'string_decoder';
import { CodeGraph, getDatabasePath } from '../src';
import { encodeLockInfo, getDaemonPidPath, getDaemonSocketCandidates, getDaemonSocketPath } from '../src/mcp/daemon-paths';
import { isProcessAlive, stopOlderDaemon } from '../src/mcp/daemon-registry';
import { connectWithHello } from '../src/mcp/proxy';
import { CodeGraphPackageVersion, isOlderRelease } from '../src/mcp/version';
import { getWriterPidPath, readWriterLock, swapWriterLock } from '../src/mcp/writer-lock';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { recordSpawns, removeSpawnLog, settleLosingCandidates, spawnLogFor } from './daemon-candidates';

const BIN = path.resolve(__dirname, '../dist/bin/codegraph.js');
const DIST = path.resolve(__dirname, '../dist');
const NODE_MODULES = path.resolve(__dirname, '../node_modules');
const OLDER_DAEMON = path.resolve(__dirname, 'fixtures/older-daemon.cjs');

/** Plain releases always below / above any version this repo will carry. */
const OLDER = '0.0.1';
const NEWER = '999.0.0';

describe('isOlderRelease', () => {
  it('orders plain releases numerically', () => {
    expect(isOlderRelease('1.6.1', '1.6.2')).toBe(true);
    expect(isOlderRelease('1.6.9', '1.6.10')).toBe(true);
    expect(isOlderRelease('1.9.0', '1.10.0')).toBe(true);
    expect(isOlderRelease('0.99.99', '1.0.0')).toBe(true);
    expect(isOlderRelease('1.6.2', '1.6.2')).toBe(false);
    expect(isOlderRelease('1.6.3', '1.6.2')).toBe(false);
    expect(isOlderRelease('2.0.0', '1.99.99')).toBe(false);
  });

  it('never calls a prerelease, a build or an unknown version older, in either position', () => {
    for (const odd of ['0.0.0-unknown', '0.0.0-mismatch', '1.6.1-beta.1', '1.6.1+local', 'v1.6.1', 'unknown', 'test', '', '1.6']) {
      expect(isOlderRelease(odd, '1.6.2'), odd).toBe(false);
      expect(isOlderRelease('1.6.1', odd), odd).toBe(false);
    }
  });

  it('lets at most one of two versions replace the other', () => {
    const versions = ['0.9.0', '1.6.1', '1.6.2', '1.6.10', '1.7.0', '2.0.0', '1.6.2-rc.1', '0.0.0-unknown'];
    for (const a of versions) {
      for (const b of versions) {
        expect(isOlderRelease(a, b) && isOlderRelease(b, a), `${a} / ${b}`).toBe(false);
      }
    }
  });
});

/** A path a test server can listen on: a named pipe on Windows, a socket file elsewhere. */
function listenPath(dir: string, name: string): string {
  return process.platform === 'win32'
    ? `\\\\.\\pipe\\${path.basename(dir)}-${name}`
    : path.join(dir, `${name}.sock`);
}

async function listen(server: net.Server, socketPath: string): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(socketPath, resolve);
  });
}

describe('connectWithHello', () => {
  let dir: string;
  let server: net.Server | null = null;

  beforeEach(() => { dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cg-hello-')); });
  afterEach(async () => {
    if (server) await new Promise<void>((resolve) => server!.close(() => resolve()));
    server = null;
    fs.rmSync(dir, { recursive: true, force: true });
  });

  it.each([OLDER, NEWER, '0.0.0-unknown'])('refuses a %s daemon with explicit unknown-policy guidance', async (version) => {
    const socketPath = listenPath(dir, 'daemon');
    server = net.createServer((socket) => {
      socket.write(JSON.stringify({ codegraph: version, pid: process.pid, socketPath, protocol: 1 }) + '\n');
    });
    await listen(server, socketPath);
    const stderr = vi.spyOn(process.stderr, 'write').mockImplementation(() => true);
    try {
      await expect(connectWithHello(socketPath, '1.6.2')).rejects.toMatchObject({
        code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: socketPath,
        guidance: expect.stringContaining(socketPath),
      });
      expect(stderr.mock.calls.some(([line]) => String(line).includes('Attached to shared daemon'))).toBe(false);
    } finally {
      stderr.mockRestore();
    }
  });
});

describe('swapWriterLock', () => {
  let root: string;
  beforeEach(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'cg-writer-swap-'));
    fs.mkdirSync(path.join(root, '.codegraph'));
  });
  afterEach(() => { fs.rmSync(root, { recursive: true, force: true }); });

  const record = (pid: number, mode: string) => ({ pid, mode, startedAt: 1, ready: false });

  it('replaces the record of the given pid, and only that record', () => {
    const file = getWriterPidPath(root);
    fs.writeFileSync(file, JSON.stringify(record(111, 'daemon')) + '\n');
    expect(swapWriterLock(root, 222, record(333, 'handover'))).toBe(false);
    expect(readWriterLock(root)).toMatchObject({ pid: 111, mode: 'daemon' });
    expect(swapWriterLock(root, 111, record(333, 'handover'))).toBe(true);
    expect(readWriterLock(root)).toMatchObject({ pid: 333, mode: 'handover' });
    // A slot nobody holds is not handed over: it is acquired the usual way.
    fs.rmSync(file);
    expect(swapWriterLock(root, 333, record(444, 'daemon'))).toBe(false);
    expect(fs.existsSync(file)).toBe(false);
    expect(fs.readdirSync(path.join(root, '.codegraph'))).toEqual([]);
  });
});

/** A live process to stand in for a daemon; signals reach it, never the test. */
function startDetachedProcess(): number {
  const source = [
    "const { spawn } = require('child_process');",
    "const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { detached: true, stdio: 'ignore' });",
    'child.unref();',
    'console.log(child.pid);',
  ].join(' ');
  const pid = Number(execFileSync(process.execPath, ['-e', source], { encoding: 'utf8' }).trim());
  if (!Number.isInteger(pid) || pid <= 0) throw new Error('could not start daemon fixture');
  return pid;
}

async function deadPid(): Promise<number> {
  const child = spawn(process.execPath, ['-e', 'process.exit(0)']);
  await once(child, 'exit');
  await new Promise((r) => setTimeout(r, 50));
  return child.pid!;
}

describe('stopOlderDaemon', () => {
  let root: string;
  let pidPath: string;
  let socketPath: string;
  let server: net.Server | null;
  let pids: number[];

  beforeEach(() => {
    root = fs.mkdtempSync(path.join(os.tmpdir(), 'cg-older-daemon-'));
    fs.mkdirSync(path.join(root, '.codegraph'));
    pidPath = getDaemonPidPath(root);
    socketPath = getDaemonSocketPath(root);
    server = null;
    pids = [];
  });

  afterEach(async () => {
    vi.restoreAllMocks();
    for (const pid of pids) if (isProcessAlive(pid)) process.kill(pid, 'SIGKILL');
    if (server) await new Promise<void>((resolve) => server!.close(() => resolve()));
    fs.rmSync(root, { recursive: true, force: true });
  });

  /** A daemon of `version`: its lock, and a socket answering the hello as `helloPid`. */
  async function fakeDaemon(version: string, helloPid?: number): Promise<{ pid: number; lock: string }> {
    const pid = startDetachedProcess();
    pids.push(pid);
    server = net.createServer((socket) => {
      socket.end(JSON.stringify({ protocol: 1, codegraph: version, pid: helloPid ?? pid, socketPath }) + '\n');
    });
    await listen(server, socketPath);
    const lock = encodeLockInfo({ pid, version, socketPath, startedAt: Date.now() });
    fs.writeFileSync(pidPath, lock);
    return { pid, lock };
  }

  /** Signals sent to `pid`, other than the signal-0 liveness probe. */
  function signalsTo(kill: ReturnType<typeof vi.spyOn>, pid: number): unknown[] {
    return kill.mock.calls.filter(([target, signal]) => target === pid && signal !== 0).map(([, signal]) => signal);
  }

  /** The writer record, decoded. */
  const writerRecord = (): any => {
    try { return JSON.parse(fs.readFileSync(getWriterPidPath(root), 'utf8')); } catch { return null; }
  };

  it('stops a verified daemon of an older release and clears its lock', async () => {
    const { pid } = await fakeDaemon('1.6.1');
    const result = await stopOlderDaemon(root, '1.6.2');
    expect(result).toMatchObject({ pid, outcome: 'term', version: '1.6.1' });
    expect(isProcessAlive(pid)).toBe(false);
    expect(fs.existsSync(pidPath)).toBe(false);
    // Kept for the daemon the caller starts next.
    expect(writerRecord()).toMatchObject({ pid: process.pid, mode: 'handover' });
  }, 15000);

  it('holds the writer slot from before the signal, so it is never free or stale', async () => {
    const { pid } = await fakeDaemon('1.6.1');
    fs.writeFileSync(getWriterPidPath(root), JSON.stringify({ pid, mode: 'daemon', startedAt: 1, ready: true }) + '\n');
    let atSignal: unknown = null;
    const originalKill = process.kill.bind(process);
    vi.spyOn(process, 'kill').mockImplementation((target, signal) => {
      if (target === pid && signal === 'SIGTERM') atSignal = writerRecord();
      return originalKill(target, signal);
    });
    expect(await stopOlderDaemon(root, '1.6.2')).toMatchObject({ pid, outcome: 'term' });
    // A session of the old daemon serving itself the moment it went found the
    // slot held, and so served reads only.
    expect(atSignal).toMatchObject({ pid: process.pid, mode: 'handover' });
    expect(writerRecord()).toMatchObject({ pid: process.pid, mode: 'handover' });
    expect(fs.existsSync(pidPath)).toBe(false);
  }, 15000);

  it('gives the writer slot back to an older daemon that does not exit', async () => {
    const { pid, lock } = await fakeDaemon('1.6.1');
    const record = { pid, mode: 'daemon', startedAt: 1, ready: true };
    fs.writeFileSync(getWriterPidPath(root), JSON.stringify(record) + '\n');
    // The signal reaches nothing: the daemon closes its socket, as its own
    // shutdown does first, and then never exits.
    const originalKill = process.kill.bind(process);
    vi.spyOn(process, 'kill').mockImplementation((target, signal) => {
      if (target === pid && signal === 'SIGTERM') { server?.close(); return true; }
      return originalKill(target, signal);
    });
    expect(await stopOlderDaemon(root, '1.6.2', { shutdownGraceMs: 300 })).toMatchObject({ pid, outcome: 'still-running' });
    expect(writerRecord()).toEqual(record);
    expect(fs.readFileSync(pidPath, 'utf8')).toBe(lock);
    expect(isProcessAlive(pid)).toBe(true);
  }, 15000);

  it('leaves the daemon to another launcher already holding the slot to replace it', async () => {
    const { pid, lock } = await fakeDaemon('1.6.1');
    const other = startDetachedProcess();
    pids.push(other);
    const claim = { pid: other, mode: 'handover', startedAt: 1, ready: false };
    fs.writeFileSync(getWriterPidPath(root), JSON.stringify(claim) + '\n');
    const kill = vi.spyOn(process, 'kill');
    expect(await stopOlderDaemon(root, '1.6.2')).toBeNull();
    expect(signalsTo(kill, pid)).toEqual([]);
    expect(writerRecord()).toEqual(claim);
    expect(fs.readFileSync(pidPath, 'utf8')).toBe(lock);
  });

  it.each(['1.6.2', '1.6.3', '2.0.0', '1.6.1-beta.1', '0.0.0-unknown', 'test'])(
    'never signals a daemon of version %s',
    async (version) => {
      const { pid, lock } = await fakeDaemon(version);
      const kill = vi.spyOn(process, 'kill');
      expect(await stopOlderDaemon(root, '1.6.2')).toBeNull();
      expect(signalsTo(kill, pid)).toEqual([]);
      expect(isProcessAlive(pid)).toBe(true);
      expect(fs.readFileSync(pidPath, 'utf8')).toBe(lock);
    },
  );

  it('leaves an older lock alone when its socket answers for another process (#1553)', async () => {
    const { pid, lock } = await fakeDaemon('1.6.1', process.pid);
    const kill = vi.spyOn(process, 'kill');
    expect(await stopOlderDaemon(root, '1.6.2')).toMatchObject({ pid, outcome: 'unverified' });
    expect(signalsTo(kill, pid)).toEqual([]);
    expect(signalsTo(kill, process.pid)).toEqual([]);
    expect(isProcessAlive(pid)).toBe(true);
    expect(fs.readFileSync(pidPath, 'utf8')).toBe(lock);
  });

  it('reports an older daemon that already exited, leaving its lock to its successor', async () => {
    const lock = encodeLockInfo({ pid: await deadPid(), version: '1.6.1', socketPath, startedAt: Date.now() });
    fs.writeFileSync(pidPath, lock);
    expect(await stopOlderDaemon(root, '1.6.2')).toMatchObject({ outcome: 'not-running', version: '1.6.1' });
    expect(fs.readFileSync(pidPath, 'utf8')).toBe(lock);
  });

  it('has nothing to stop without a lock, and leaves a listening socket alone', async () => {
    server = net.createServer((socket) => {
      socket.end(JSON.stringify({ protocol: 1, codegraph: '1.6.1', pid: process.pid, socketPath }) + '\n');
    });
    await listen(server, socketPath);
    expect(await stopOlderDaemon(root, '1.6.2')).toBeNull();
    await expect(connectWithHello(socketPath, '1.6.2')).rejects.toMatchObject({
      code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: socketPath,
    });
  });

  it('does not act on an unreadable lock', async () => {
    fs.mkdirSync(pidPath);
    expect(await stopOlderDaemon(root, '1.6.2')).toEqual({ root, pid: null, outcome: 'unverified' });
  });
});

/**
 * A CodeGraph install of `version`: an independent copy of this build's dist/,
 * next to a package.json of that version and a link to this checkout's modules.
 * Hard links emit metadata writes on the source files on Windows; copies keep
 * the tested build immutable while the temporary installs come and go.
 */
function makeInstall(version: string): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), `cg-install-${version}-`));
  const copyTree = (from: string, to: string): void => {
    fs.mkdirSync(to, { recursive: true });
    for (const entry of fs.readdirSync(from, { withFileTypes: true })) {
      const source = path.join(from, entry.name);
      const target = path.join(to, entry.name);
      if (entry.isDirectory()) copyTree(source, target);
      else fs.copyFileSync(source, target);
    }
  };
  copyTree(DIST, path.join(dir, 'dist'));
  fs.symlinkSync(NODE_MODULES, path.join(dir, 'node_modules'), 'junction');
  fs.writeFileSync(path.join(dir, 'package.json'), JSON.stringify({ name: '@colbymchenry/codegraph', version }) + '\n');
  return dir;
}

interface Session {
  child: ChildProcessWithoutNullStreams;
  stdout: string[];
  stderr: string[];
  root: string;
  kind: 'launcher' | 'daemon';
  closed: boolean;
  errors: string[];
  capture: { stdout: LineCapture; stderr: LineCapture };
  refused: Array<{ id: number; root: string; label: string }>;
}

interface LineCapture {
  seenBytes: number;
  retainedBytes: number;
  eof: boolean;
  tail: string;
  raw: Buffer;
  errors: string[];
}

function lines(stream: NodeJS.ReadableStream, into: string[], protocol: boolean, maximum: number): LineCapture {
  const capture: LineCapture = { seenBytes: 0, retainedBytes: 0, eof: false, tail: '', raw: Buffer.alloc(maximum), errors: [] };
  const fail = (detail: string) => { if (capture.errors.length < 16) capture.errors.push(detail.slice(0, 2048)); };
  const decoder = new StringDecoder('utf8');
  let buffer = '';
  const consume = (text: string) => {
    buffer += text;
    let idx: number;
    while ((idx = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, idx);
      buffer = buffer.slice(idx + 1);
      if (into.length >= 4096) { fail('bounded line capture overflow'); continue; }
      into.push(line);
      if (protocol) {
        try {
          const value = JSON.parse(line);
          if (!value || typeof value !== 'object' || Array.isArray(value) || value.jsonrpc !== '2.0') fail('invalid MCP stdout frame');
          else if (value.method !== undefined ? typeof value.method !== 'string' || value.result !== undefined || value.error !== undefined
            : value.id === undefined || (value.result !== undefined) === (value.error !== undefined)) fail('invalid MCP stdout envelope');
        } catch { fail('malformed MCP stdout JSON frame'); }
      }
    }
  };
  stream.on('data', (chunk: Buffer) => {
    capture.seenBytes += chunk.length;
    const kept = chunk.subarray(0, Math.max(0, maximum - capture.retainedBytes));
    kept.copy(capture.raw, capture.retainedBytes); capture.retainedBytes += kept.length;
    if (kept.length !== chunk.length) fail('bounded raw capture overflow');
    consume(decoder.write(kept)); capture.tail = buffer;
  });
  stream.on('end', () => {
    consume(decoder.end()); capture.eof = true; capture.tail = buffer;
    if (protocol && !isUtf8(capture.raw.subarray(0, capture.retainedBytes))) fail('invalid UTF-8 MCP stdout');
    if (protocol && buffer.length) fail('unterminated MCP stdout frame at EOF');
  });
  stream.on('error', error => fail('stream error: ' + String(error)));
  stream.on('close', () => {
    if (!capture.eof) fail('stream closed without EOF');
  });
  return capture;
}

function observeSession(child: ChildProcessWithoutNullStreams, root: string, kind: Session['kind']): Session {
  const stdout: string[] = [], stderr: string[] = [];
  const session: Session = { child, root, kind, stdout, stderr, closed: false, errors: [], refused: [],
    capture: { stdout: lines(child.stdout, stdout, kind === 'launcher', 8 * 1024 * 1024),
      stderr: lines(child.stderr, stderr, false, 1024 * 1024) } };
  const fail = (detail: string) => { if (session.errors.length < 16) session.errors.push(detail.slice(0, 2048)); };
  child.on('error', error => fail('child error: ' + String(error)));
  child.stdin.on('error', error => fail('stdin error: ' + String(error)));
  child.on('close', () => { session.closed = true; });
  return session;
}

/** Close/EOF, not merely exit: all terminal protocol bytes must be observable. */
async function closeSession(session: Session): Promise<void> {
  const { child } = session;
  if (child.exitCode === null && child.signalCode === null) {
    if (session.kind === 'daemon') child.kill('SIGKILL'); // owned fixture stop, after assertions
    else child.stdin.end();
  }
  try {
    await waitFor(() => session.closed && session.capture.stdout.eof && session.capture.stderr.eof, 5000, 'owned child close and both stream EOFs');
  } catch (primary) {
    if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL');
    // Bound the repair too; an incomplete tail still fails even if SIGKILL works.
    await waitFor(() => session.closed && session.capture.stdout.eof && session.capture.stderr.eof, 5000, 'forced owned child close and EOFs')
      .catch(error => { session.errors.push(String(error).slice(0, 2048)); });
    throw primary;
  }
  if (session.kind === 'launcher' && (child.exitCode !== 0 || child.signalCode !== null)) {
    throw new Error(`Owned launcher cleanup exit ${child.exitCode}/${child.signalCode}`);
  }
}

function findResponse(stdout: string[], id: number): any | null {
  for (const line of stdout) {
    try {
      const parsed = JSON.parse(line);
      if (parsed && parsed.id === id && (parsed.result !== undefined || parsed.error !== undefined)) return parsed;
    } catch { /* not JSON */ }
  }
  return null;
}

function waitFor<T>(predicate: () => T | undefined | null | false, timeoutMs: number, label: string): Promise<T> {
  return new Promise((resolve, reject) => {
    const started = Date.now();
    const tick = (): void => {
      let value: T | undefined | null | false;
      try { value = predicate(); } catch (e) { reject(e); return; }
      if (value) { resolve(value as T); return; }
      if (Date.now() - started > timeoutMs) { reject(new Error(`Timed out after ${timeoutMs}ms waiting for: ${label}`)); return; }
      setTimeout(tick, 25);
    };
    tick();
  });
}

function isAlive(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) { if ((error as NodeJS.ErrnoException).code === 'ESRCH') return false; return true; }
}

async function waitProcessExit(pid: number, timeoutMs: number): Promise<boolean> {
  return waitFor(() => !isAlive(pid), timeoutMs, `pid ${pid} to exit`).then(() => true, () => false);
}

function readJson(file: string): any | null {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); } catch { return null; }
}

describe('a launcher meeting a daemon of another version (#2335)', () => {
  let olderInstall: string;
  let newerInstall: string;
  let tempDir: string;
  let realRoot: string;
  const sessions: Session[] = [];
  const daemonPids = new Set<number>();
  const extraInstalls: string[] = [];
  let preserveInstalls = false;

  beforeAll(() => {
    olderInstall = makeInstall(OLDER);
    newerInstall = makeInstall(NEWER);
  });

  afterAll(async () => {
    // An incomplete fixture tail keeps its installs/links and ownership evidence.
    if (preserveInstalls) return;
    for (const dir of [olderInstall, newerInstall, ...extraInstalls]) {
      if (!dir) continue;
      // The link first: removing an install must never reach this checkout's modules.
      try { fs.unlinkSync(path.join(dir, 'node_modules')); } catch { /* not created */ }
      await fs.promises.rm(dir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
    }
  });

  beforeEach(async () => {
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cg-older-version-'));
    fs.writeFileSync(path.join(tempDir, 'app.ts'), 'export function appMain() { return 1; }\n');
    const cg = await CodeGraph.init(tempDir);
    try { await cg.indexAll(); } finally { cg.close(); }
    realRoot = fs.realpathSync(tempDir);
  });

  afterEach(async () => {
    // Every launcher first (with the runtime flags there is no relaunch child),
    // then the losing daemon candidates, then the daemons themselves — and only
    // then the fixture: on Windows a live process inside it blocks the removal.
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    let terminalAuditComplete = false, fixtureRemoved = false, spawnLogRemoved = false;
    const attempt = async (phase: string, action: () => unknown | Promise<unknown>) => {
      try { await action(); cleanup.push({ phase, error: null }); }
      catch (error) { cleanup.push({ phase, error: error instanceof Error ? error.stack ?? error.message : String(error) }); }
    };
    await Promise.all(sessions.map((session, index) => attempt(`session ${index} close and EOF`, () => closeSession(session))));
    const lockPid = (): number | null => readJson(path.join(realRoot, '.codegraph', 'daemon.pid'))?.pid ?? null;
    await attempt('losing daemon candidates exit', () => settleLosingCandidates(tempDir, lockPid, 10000));
    const holder = lockPid();
    if (holder) daemonPids.add(holder);
    await Promise.all([...daemonPids].map(pid => attempt(`owned daemon ${pid} exit`, async () => {
      if (pid === process.pid) throw new Error('fixture owner must never be the test process');
      if (isAlive(pid)) { try { process.kill(pid, 'SIGKILL'); } catch { /* checked below, never assumed gone */ } }
      expect(await waitProcessExit(pid, 5000), `owned daemon ${pid} actually exited`).toBe(true);
    })));
    await attempt('terminal protocol and refused-response audit', () => {
      for (const session of sessions) {
        expect(session.closed, 'actual child close').toBe(true);
        for (const capture of Object.values(session.capture)) {
          expect(capture.eof, 'actual stream EOF').toBe(true);
          expect(capture.errors).toEqual([]);
          expect(capture.retainedBytes).toBe(capture.seenBytes);
        }
        expect(session.errors).toEqual([]);
        if (session.kind === 'launcher') expect(session.capture.stdout.tail, 'complete terminal MCP framing').toBe('');
        for (const issued of session.refused) {
          const replies = session.stdout.map(line => JSON.parse(line)).filter(value => value?.id === issued.id
            && (value.result !== undefined || value.error !== undefined));
          expect(replies, `${issued.label}: terminal exactly-once response`).toHaveLength(1);
          expectRefusal(replies[0], issued.root, issued.label);
        }
      }
      terminalAuditComplete = true;
    });
    const evidence = () => ({ fixture: tempDir, cleanup, terminalAuditComplete, fixtureRemoved, spawnLogRemoved,
      outcome: cleanup.some(row => row.error) ? 'fail' : 'pass', daemonPids: [...daemonPids],
      remainingDaemonPids: [...daemonPids].filter(isAlive), sessions: sessions.map(session => ({
        pid: session.child.pid, root: session.root, kind: session.kind, exitCode: session.child.exitCode,
        signalCode: session.child.signalCode, closed: session.closed, errors: session.errors, refused: session.refused,
        stdout: session.stdout, stderr: session.stderr, capture: Object.fromEntries(Object.entries(session.capture).map(([name, value]) =>
          [name, { ...value, raw: undefined, rawBase64: value.raw.subarray(0, value.retainedBytes).toString('base64') }])),
      })) });
    const evidencePath = process.env.VALIDATION_OUT
      ? path.join(process.env.VALIDATION_OUT, `${path.basename(tempDir)}-cleanup.json`)
      : `${tempDir}.cleanup.json`;
    await attempt('persist complete cleanup facts', () => fs.writeFileSync(evidencePath, JSON.stringify(evidence(), null, 2) + '\n'));
    if (cleanup.every(row => row.error === null)) {
      await attempt('remove verified fixture', async () => {
        await fs.promises.rm(tempDir, { recursive: true, force: true, maxRetries: 5, retryDelay: 100 });
        fixtureRemoved = true;
        removeSpawnLog(tempDir);
        spawnLogRemoved = true;
      });
    }
    await attempt('persist final cleanup disposition', () => fs.writeFileSync(evidencePath, JSON.stringify(evidence(), null, 2) + '\n'));
    const failures = cleanup.filter(row => row.error !== null);
    if (failures.length) {
      preserveInstalls = true;
      throw new Error(`Owned daemon fixture cleanup incomplete; retained ${tempDir}, ${spawnLogFor(tempDir)}, ${evidencePath}: ${JSON.stringify(failures)}`);
    }
    sessions.length = 0;
    daemonPids.clear();
  }, 45_000);

  /** An agent session: `install`'s launcher, `serve --mcp` in the project. */
  function startSession(install: string | null, env: NodeJS.ProcessEnv = {}): Session {
    const bin = install ? path.join(install, 'dist', 'bin', 'codegraph.js') : BIN;
    const recorder = recordSpawns(tempDir);
    const child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, ...recorder.args, bin, 'serve', '--mcp'], {
      cwd: tempDir,
      stdio: ['pipe', 'pipe', 'pipe'],
      env: {
        ...process.env,
        CODEGRAPH_MCP_LOG_ATTACH: '1',
        CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '30000',
        ...recorder.env,
        ...env,
      },
    }) as ChildProcessWithoutNullStreams;
    const session = observeSession(child, tempDir, 'launcher');
    sessions.push(session);
    send(session, {
      jsonrpc: '2.0', id: 1, method: 'initialize',
      params: { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'test', version: '0.0.0' }, rootUri: `file://${tempDir}` },
    });
    return session;
  }

  function send(session: Session, msg: unknown): void {
    try { session.child.stdin.write(JSON.stringify(msg) + '\n'); } catch { /* gone */ }
  }

  async function status(session: Session, id: number, label: string): Promise<any> {
    send(session, { jsonrpc: '2.0', id, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    const reply = await waitFor(() => findResponse(session.stdout, id), 20000, label).catch((err: Error) => {
      throw new Error(`${err.message}\n--- stderr ---\n${session.stderr.join('\n')}\n--- daemon.log ---\n${daemonLog()}`);
    });
    expect(reply.error, label).toBeUndefined();
    expect(reply.result?.isError, label).not.toBe(true);
    expect(JSON.stringify(reply.result), label).toContain('CodeGraph Status');
    return reply;
  }

  async function refusedTool(session: Session, id: number, label: string, name = 'codegraph_status'): Promise<any> {
    if (session.refused.some(issued => issued.id === id)) throw new Error('refused fixture request ID reused');
    session.refused.push({ id, root: realRoot, label });
    send(session, { jsonrpc: '2.0', id, method: 'tools/call', params: { name,
      arguments: name === 'codegraph_search' ? { query: 'appMain' } : {} } });
    const reply = await waitFor(() => findResponse(session.stdout, id), 20000, label).catch((err: Error) => {
      throw new Error(`${err.message}\n--- stderr ---\n${session.stderr.join('\n')}\n--- daemon.log ---\n${daemonLog()}`);
    });
    expectRefusal(reply, realRoot, label);
    expect(session.stdout.map(line => { try { return JSON.parse(line); } catch { return null; } })
      .filter(value => value?.id === id && (value.result !== undefined || value.error !== undefined)), label).toHaveLength(1);
    return reply;
  }

  function expectRefusal(reply: any, root: string, label: string): void {
    expect(reply.jsonrpc, label).toBe('2.0');
    expect(reply.method, label).toBeUndefined();
    expect(reply.error, label).toMatchObject({ code: -32603, data: {
      code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root,
    } });
    expect(reply.error.data.guidance, label).toContain(root);
    expect(reply.result, label).toBeUndefined();
  }

  const attachedLine = (session: Session): string | undefined =>
    session.stderr.find((l) => l.includes('Attached to shared daemon'));
  const daemonLock = (): any => readJson(path.join(realRoot, '.codegraph', 'daemon.pid'));
  const writerLock = (): any => readJson(path.join(realRoot, '.codegraph', 'writer.pid'));
  const daemonLog = (): string => {
    try { return fs.readFileSync(path.join(realRoot, '.codegraph', 'daemon.log'), 'utf8'); } catch { return ''; }
  };

  /** Durable index/source/ownership bytes and write times must survive a refused
   * acquisition. SQLite SHM and daemon logs are coordination/observation files,
   * not the durable index; the original daemon remains legitimately usable. */
  function preservedArtifacts(): Record<string, unknown> {
    return Object.fromEntries([
      path.join(realRoot, 'app.ts'), getDatabasePath(realRoot), `${getDatabasePath(realRoot)}-wal`,
      path.join(realRoot, '.codegraph', '.gitignore'), getDaemonPidPath(realRoot), getWriterPidPath(realRoot),
      path.join(realRoot, '.codegraph', 'launcher-preservation-sentinel'), spawnLogFor(tempDir),
    ].map(file => {
      if (!fs.existsSync(file)) return [file, null];
      const stat = fs.statSync(file);
      return [file, { sha256: createHash('sha256').update(fs.readFileSync(file)).digest('hex'), size: stat.size,
        mtimeMs: stat.mtimeMs, ctimeMs: stat.ctimeMs, birthtimeMs: stat.birthtimeMs, ino: stat.ino }];
    }));
  }

  function protectOwner(): Record<string, unknown> {
    fs.writeFileSync(path.join(realRoot, '.codegraph', 'launcher-preservation-sentinel'), 'existing owner must remain untouched\n');
    return preservedArtifacts();
  }

  function expectPreservedOwner(session: Session, pid: number, lock: unknown, artifacts: Record<string, unknown>): void {
    expect(isAlive(pid), 'existing owner remains alive').toBe(true);
    expect(daemonLock()).toEqual(lock);
    expect(writerLock()).toMatchObject({ pid, mode: 'daemon' });
    expect(preservedArtifacts()).toEqual(artifacts);
    expect(attachedLine(session), 'refused session never attaches to a replacement').toBeUndefined();
    expect(session.stderr.join('\n')).not.toMatch(/\[CodeGraph MCP\] Stopped|serving this session in-process|Serving reads in-process|local read-only fallback|File watcher active/);
  }

  /** A session of `install` attached to a daemon of its own, which owns the project. */
  async function sessionWithDaemon(install: string, version: string, env: NodeJS.ProcessEnv = {}): Promise<{ session: Session; pid: number }> {
    const session = startSession(install, env);
    await waitFor(() => attachedLine(session), 20000, `the ${version} session to attach`);
    // Attached means listening, not initialized: let the engine open the
    // database before anything stops this daemon.
    await status(session, 2, `a ${version} tool call`);
    const lock = daemonLock();
    expect(lock).toMatchObject({ version });
    expect(writerLock()).toMatchObject({ pid: lock.pid, mode: 'daemon' });
    daemonPids.add(lock.pid);
    return { session, pid: lock.pid };
  }

  /** A daemon process for the project, started the way a launcher starts one. */
  function startDaemon(env: NodeJS.ProcessEnv): { child: ChildProcessWithoutNullStreams; log: string[] } {
    const child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, BIN, 'serve', '--mcp', '--path', realRoot], {
      cwd: tempDir,
      stdio: ['pipe', 'pipe', 'pipe'],
      env: { ...process.env, CODEGRAPH_DAEMON_INTERNAL: '1', CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '30000', ...env },
    }) as ChildProcessWithoutNullStreams;
    const session = observeSession(child, tempDir, 'daemon');
    sessions.push(session);
    daemonPids.add(child.pid!);
    return { child, log: session.stderr };
  }

  /** The slot as a launcher that just stopped an older daemon holds it: this test process stands in. */
  function holdWriterSlotForSuccessor(): string {
    const claim = JSON.stringify({ pid: process.pid, mode: 'handover', startedAt: Date.now(), ready: false }) + '\n';
    fs.writeFileSync(path.join(realRoot, '.codegraph', 'writer.pid'), claim);
    return claim;
  }

  it('a daemon takes over the writer slot held for it, and only that one', async () => {
    const claim = holdWriterSlotForSuccessor();

    // A daemon the slot is not held for finds it held, as by any live writer.
    const stranger = startDaemon({});
    const [strangerCode] = await once(stranger.child, 'exit');
    expect(strangerCode).toBe(1);
    expect(stranger.log.join('\n')).toContain(`writer lock held by PID ${process.pid} (handover mode)`);
    expect(fs.readFileSync(path.join(realRoot, '.codegraph', 'writer.pid'), 'utf8')).toBe(claim);

    const successor = startDaemon({ CODEGRAPH_DAEMON_HANDOVER: String(process.pid) });
    await waitFor(
      () => daemonLock()?.pid === successor.child.pid && successor.log.some((l) => l.includes('Listening on')),
      20000, 'the successor to listen',
    );
    expect(writerLock()).toMatchObject({ pid: successor.child.pid, mode: 'daemon' });
    expect(successor.log.join('\n')).toContain(`Took over the writer lock from launcher pid ${process.pid}`);
  }, 60000);

  it('a daemon the slot is held for outwaits a candidate still starting in its way', async () => {
    holdWriterSlotForSuccessor();
    // A racing launcher's candidate holds the daemon lock, still starting; it
    // cannot get the writer slot, so it will give up.
    const candidate = startDetachedProcess();
    daemonPids.add(candidate);
    const pidPath = path.join(realRoot, '.codegraph', 'daemon.pid');
    const candidateLock = encodeLockInfo({
      pid: candidate, version: CodeGraphPackageVersion, socketPath: getDaemonSocketPath(realRoot), startedAt: Date.now(),
    });
    fs.writeFileSync(pidPath, candidateLock);

    // Any other daemon yields to it at once.
    const yielding = startDaemon({});
    const [yieldingCode] = await once(yielding.child, 'exit');
    expect(yieldingCode).toBe(0);
    expect(yielding.log.join('\n')).toContain(`Another daemon (pid ${candidate}) already holds the lock`);

    const successor = startDaemon({ CODEGRAPH_DAEMON_HANDOVER: String(process.pid) });
    await waitFor(
      () => successor.log.some((l) => l.includes(`Waiting for daemon candidate pid ${candidate}`)),
      20000, 'the successor to wait for the candidate',
    );
    expect(successor.child.exitCode).toBeNull();
    expect(fs.readFileSync(pidPath, 'utf8')).toBe(candidateLock);
    // The candidate gives up: its lock goes, then so does it.
    fs.rmSync(pidPath);
    process.kill(candidate, 'SIGKILL');
    await waitFor(
      () => daemonLock()?.pid === successor.child.pid && successor.log.some((l) => l.includes('Listening on')),
      20000, 'the successor to listen',
    );
    expect(writerLock()).toMatchObject({ pid: successor.child.pid, mode: 'daemon' });
  }, 60000);

  it('preserves an older daemon and its recurring healthy session calls when another install is refused', async () => {
    const before = await sessionWithDaemon(olderInstall, OLDER);
    const lock = daemonLock();
    const artifacts = protectOwner();
    // The older session keeps using its actual daemon while the new install
    // attempts acquisition. Refusal must not abandon any healthy owner call.
    let next = 100;
    const calling = setInterval(() => {
      send(before.session, { jsonrpc: '2.0', id: next++, method: 'tools/call', params: { name: 'codegraph_status', arguments: {} } });
    }, 20);
    let after: Session;
    try {
      after = startSession(null);
      await refusedTool(after, 2, 'the new install to refuse the older service');
      await refusedTool(after, 3, 'the refused session to refuse a real query', 'codegraph_search');
    } finally {
      clearInterval(calling);
    }
    const last = next - 1;
    expect(last).toBeGreaterThanOrEqual(100);
    await waitFor(() => findResponse(before.session.stdout, last), 20000, 'the older session to answer every recurring call during refused acquisition');
    for (let id = 100; id <= last; id++) {
      const reply = findResponse(before.session.stdout, id);
      expect(reply, `healthy owner call ${id}`).toBeTruthy();
      expect(reply.error, `healthy owner call ${id}`).toBeUndefined();
      expect(reply.result?.isError, `healthy owner call ${id}`).not.toBe(true);
      expect(JSON.stringify(reply.result), `healthy owner call ${id}`).toContain('CodeGraph Status');
    }
    const stderr = before.session.stderr.join('\n');
    expect(stderr).not.toMatch(/Shared daemon connection lost|Serving reads in-process without auto-sync/);
    expectPreservedOwner(after!, before.pid, lock, artifacts);
    await status(before.session, 3, 'the original service remains usable after all recurring calls');
  }, 120000);

  it('refuses an older install without replacing its daemon or changing its index and locks', async () => {
    const before = await sessionWithDaemon(olderInstall, OLDER);
    const lock = daemonLock();
    const artifacts = protectOwner();
    const after = startSession(null);
    await refusedTool(after, 2, 'an older service produces an explicit policy refusal');
    await refusedTool(after, 3, 'refused query cannot create a bypass engine', 'codegraph_search');
    expectPreservedOwner(after, before.pid, lock, artifacts);
    expect(daemonLog()).not.toContain(`Took over the writer lock from launcher pid ${after.child.pid}`);
    expect(daemonLog()).not.toContain('Shutting down (SIGTERM');
    await status(before.session, 3, 'the original older service still serves its own session');
    expect(before.session.stderr.join('\n')).not.toMatch(/Shared daemon connection lost|Serving reads in-process without auto-sync/);
    expectPreservedOwner(after, before.pid, lock, artifacts);
  }, 120000);

  it('refuses an older live lock owner when every usual probe fails, preserving its index and locks', async () => {
    // On Windows a 1.6.1 daemon named its pipe after the root as typed (#2278),
    // so the usual probes find no service. A live project lock still forbids
    // treating that absence as permission to signal, spawn or serve around it.
    const socketPath = listenPath(tempDir, 'older');
    const older = spawn(process.execPath, [OLDER_DAEMON, realRoot, OLDER, socketPath], { detached: true, stdio: 'ignore', windowsHide: true });
    older.unref();
    daemonPids.add(older.pid!);
    await waitFor(() => daemonLock()?.pid === older.pid, 10000, 'the older daemon to hold the project');
    expect(writerLock()).toMatchObject({ pid: older.pid, mode: 'daemon' });
    expect(getDaemonSocketCandidates(realRoot)).not.toContain(socketPath);
    for (const candidate of getDaemonSocketCandidates(realRoot)) {
      expect(await connectWithHello(candidate), `usual probe ${candidate} is genuinely unavailable`).toBeNull();
    }
    const lock = daemonLock();
    const artifacts = protectOwner();
    const session = startSession(null);
    await refusedTool(session, 2, 'failed usual probes and a live older owner produce an explicit refusal');
    await refusedTool(session, 3, 'a repeated query cannot replace or bypass the hidden owner', 'codegraph_search');
    expectPreservedOwner(session, older.pid!, lock, artifacts);
    expect(older.exitCode).toBeNull();
    expect(older.signalCode).toBeNull();
    expect(daemonLog()).not.toContain(`Took over the writer lock from launcher pid ${session.child.pid}`);
  }, 90000);

  it('never stops a daemon from a newer install', async () => {
    const retry = { CODEGRAPH_DAEMON_RETRY_MS: '300', CODEGRAPH_DAEMON_RETRY_MAX_MS: '600' };
    const newer = await sessionWithDaemon(newerInstall, NEWER);
    const lock = daemonLock();
    const artifacts = protectOwner();
    const current = startSession(null, retry);
    // A version mismatch is terminal policy refusal, not unavailable service
    // eligible for retrying with an in-process fallback or replacement daemon.
    await refusedTool(current, 2, 'a newer service produces an explicit policy refusal');
    await refusedTool(current, 3, 'the same refused client cannot bypass the newer service', 'codegraph_search');
    expectPreservedOwner(current, newer.pid, lock, artifacts);
    await status(newer.session, 3, 'a tool call through the newer daemon');
    expectPreservedOwner(current, newer.pid, lock, artifacts);
  }, 90000);

  it('a session whose own install was upgraded serves reads only once its daemon exits', async () => {
    // An upgrade in place: the daemon notices its package.json changed and
    // exits (its install check), and its session falls back in-process. As
    // the project's writer that session — running code the upgrade replaced —
    // would only keep a daemon from the new install from starting.
    const install = makeInstall('0.0.2');
    extraInstalls.push(install);
    const before = await sessionWithDaemon(install, '0.0.2', {
      CODEGRAPH_DAEMON_INSTALL_CHECK_MS: '200',
      CODEGRAPH_DAEMON_RETRY_MS: '0',
    });
    fs.writeFileSync(path.join(install, 'package.json'), JSON.stringify({ name: '@colbymchenry/codegraph', version: '0.0.3' }) + '\n');
    expect(await waitProcessExit(before.pid, 10000)).toBe(true);
    expect(daemonLog()).toContain('Install replaced by v0.0.3');
    await waitFor(
      () => before.session.stderr.some((l) => l.includes('Shared daemon connection lost')),
      10000, 'the session to lose its daemon',
    );

    await status(before.session, 3, 'a tool call after the daemon exited');
    const stderr = before.session.stderr.join('\n');
    expect(stderr).toContain("this session's CodeGraph install was upgraded or removed");
    expect(stderr).not.toContain('File watcher active');
    expect(writerLock()).toBeNull();

    // So a session from the current install gets a daemon straight away.
    const after = startSession(null);
    await waitFor(() => attachedLine(after), 20000, 'the new session to attach');
    const lock = daemonLock();
    daemonPids.add(lock.pid);
    expect(writerLock()).toMatchObject({ pid: lock.pid, mode: 'daemon' });
    // The common afterEach owns close/EOF and daemon exit; afterAll removes
    // this install only if every fixture completed that terminal audit.
  }, 90000);
});
