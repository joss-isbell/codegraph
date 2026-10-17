/**
 * Issue #1740 — concurrent direct-mode serve --mcp must refuse the second
 * primary writer instead of silently degrading auto-sync. A rootless shell
 * now reports the terminal typed refusal after actual client selection.
 */

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { ChildProcessWithoutNullStreams, spawn } from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { createHash } from 'crypto';
import { pathToFileURL } from 'url';
import { CodeGraph } from '../src';
import type { MCPEngine } from '../src/mcp/engine';

import { createDatabase } from '../src/db/sqlite-adapter';
import { getWriterPidPath, readWriterLock } from '../src/mcp/writer-lock';
import { isProcessAlive, stopDaemonAt } from '../src/mcp/daemon-registry';
import { recordSpawns, removeSpawnLog, settleLosingCandidates, spawnLogFor } from './daemon-candidates';

// Exercise the shipped lazy CommonJS loader as well as the engine lifecycle.
const { MCPEngine: BuiltMCPEngine } = require('../dist/mcp/engine') as typeof import('../src/mcp/engine');

const BIN = path.resolve(__dirname, '../dist/bin/codegraph.js');

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

interface WriterResponse {
  jsonrpc?: string;
  method?: string;
  id?: number;
  error?: { code: number; message: string; data?: { code?: string; reason?: string; projectRoot?: string; guidance?: string } };
  result?: { protocolVersion?: string; isError?: boolean; content?: Array<{ text?: string }> };
}
function spawnMcp(
  cwd: string,
  env: NodeJS.ProcessEnv,
  preloads: string[] = [],
) {
  // Record the daemon candidates this launcher spawns, for the teardown.
  const recorder = recordSpawns(cwd);
  const child = spawn(process.execPath, [...recorder.args, ...preloads.flatMap(file => ['--require', file]), BIN, 'serve', '--mcp'], {
    cwd,
    stdio: ['pipe', 'pipe', 'pipe'],
    env: { ...process.env, ...recorder.env, ...env },
  }) as ChildProcessWithoutNullStreams;
  const limit = 65_536, errors: string[] = [];
  const fail = (error: unknown): void => { if (errors.length < 8) errors.push(String(error).slice(0, 2048)); };
  const capture = (stream: NodeJS.ReadableStream) => {
    let bytes = 0, retained = Buffer.alloc(0), ended = false, closed = false;
    stream.on('data', (chunk: Buffer) => {
      bytes += chunk.length;
      const available = limit - retained.length;
      if (available > 0) retained = Buffer.concat([retained, chunk.subarray(0, available)]);
      if (bytes > limit && bytes - chunk.length <= limit) fail('Writer-lock native stream overflow');
    });
    stream.once('end', () => { ended = true; }); stream.once('close', () => { closed = true; });
    stream.on('error', fail);
    return { text: () => retained.toString('utf8'), facts: () => ({ bytes, ended, closed,
      truncated: bytes > retained.length, sha256: createHash('sha256').update(retained).digest('hex'),
      text: retained.toString('utf8'), base64: retained.toString('base64') }) };
  };
  child.on('error', fail); child.stdin.on('error', fail);
  const stdout = capture(child.stdout), stderr = capture(child.stderr);
  const issued = new Set<number>();
  const replies = () => stdout.text().split('\n').slice(0, -1).map(line => JSON.parse(line) as WriterResponse);
  const nativeLog = () => {
    const file = env.CODEGRAPH_OWNERSHIP_NATIVE_LOG;
    try {
      const bytes = file && fs.existsSync(file) ? fs.statSync(file).size : 0;
      if (bytes > limit) throw new Error('Writer-lock native constructor log overflow');
      const text = bytes && file ? fs.readFileSync(file, 'utf8') : '';
      return { file, bytes, text, readFailure: null as string | null };
    } catch (error) { return { file, bytes: null, text: '', readFailure: String(error).slice(0, 2048) }; }
  };
  let nativeExit: { code: number | null; signal: NodeJS.Signals | null } | null = null;
  let nativeClose: { code: number | null; signal: NodeJS.Signals | null } | null = null;
  let gracefulFailure: string | null = null;
  const forced: NodeJS.Signals[] = [];
  child.once('exit', (code, signal) => { nativeExit = { code, signal }; });
  const closed = new Promise<void>(resolve => child.once('close', (code, signal) => { nativeClose = { code, signal }; resolve(); }));
  const absence = () => {
    if (!child.pid) return { kind: 'no-pid' };
    try { process.kill(child.pid, 0); return { kind: 'alive' }; }
    catch (error) { const code = (error as NodeJS.ErrnoException).code; return { kind: code === 'ESRCH' ? 'absent' : 'unproved', code }; }
  };
  const waitClosed = async (ms = 3000): Promise<void> => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try { await Promise.race([closed, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Writer-lock child ${child.pid} did not close`)), ms);
    })]); } finally { if (timer) clearTimeout(timer); }
  };
  return { child, getStderr: stderr.text, waitClosed,
    send: (message: object) => {
      if ('id' in message && typeof message.id === 'number') {
        if (issued.has(message.id) || issued.size >= 8) throw new Error('Writer-lock request ledger bound/duplicate');
        issued.add(message.id);
      }
      return child.stdin.write(JSON.stringify(message) + '\n');
    },
    response: async (id: number, deadline: number): Promise<WriterResponse> => {
      while (Date.now() < deadline) {
        if (errors.length) throw new Error(errors.join('; '));
        const reply = replies().find(message => message.id === id); if (reply) return reply;
        if (nativeExit) throw new Error(`Writer-lock child exited before response ${id}: ${stderr.text()}`);
        await sleep(25);
      }
      throw new Error(`Writer-lock child ${child.pid} response ${id} deadline`);
    },
    stop: async () => {
      if (!child.stdin.destroyed && !child.stdin.writableEnded) child.stdin.end();
      try { await waitClosed(); }
      catch (error) {
        gracefulFailure = String(error).slice(0, 2048);
        for (const signal of ['SIGTERM', 'SIGKILL'] as const) {
          if (nativeClose) break;
          forced.push(signal); child.kill(signal);
          try { await waitClosed(); } catch { /* retain failed graceful closure and try final owned cleanup */ }
        }
      }
      expect(nativeClose, `Child ${child.pid} native close missing or failed`).toEqual({ code: 0, signal: null });
      expect(nativeExit, `Child ${child.pid} native exit missing or failed`).toEqual({ code: 0, signal: null });
      expect(stdout.facts()).toMatchObject({ ended: true, closed: true, truncated: false });
      expect(stderr.facts()).toMatchObject({ ended: true, closed: true, truncated: false });
      expect(stdout.text().endsWith('\n') || !stdout.text()).toBe(true);
      const complete = replies();
      for (const reply of complete) {
        expect(reply).toMatchObject({ jsonrpc: '2.0' });
        if ('method' in reply && !('id' in reply)) { expect(typeof reply.method).toBe('string'); continue; }
        expect(typeof reply.id, 'Native response ID must be an issued number').toBe('number');
        expect(issued.has(reply.id!), 'Native response ID was not issued by this client').toBe(true);
        expect('method' in reply).toBe(false);
        expect(('result' in reply) !== ('error' in reply)).toBe(true);
      }
      for (const id of issued) expect(complete.filter(reply => reply.id === id)).toHaveLength(1);
      expect(absence()).toEqual({ kind: 'absent', code: 'ESRCH' });
      expect(nativeLog().readFailure).toBeNull();
      expect(errors).toEqual([]); expect(gracefulFailure).toBeNull(); expect(forced).toEqual([]);
    },
    snapshot: () => ({ pid: child.pid, nativeExit, nativeClose, stdout: stdout.facts(), stderr: stderr.facts(),
      issued: [...issued], nativeLog: nativeLog(),
      stdin: { ended: child.stdin.writableEnded, destroyed: child.stdin.destroyed },
      absence: absence(), errors: [...errors], gracefulFailure, forced: [...forced] }) };
}

function readWriterPid(root: string): number | undefined {
  try {
    const { pid } = JSON.parse(fs.readFileSync(getWriterPidPath(root), 'utf8')) as { pid?: number };
    return typeof pid === 'number' && Number.isSafeInteger(pid) && pid > 0 && pid !== process.pid
      ? pid : undefined;
  } catch {
    return undefined;
  }
}

describe('issue #1740 — direct-mode writer lock', () => {
  let tempDir: string;
  let realRoot: string;
  const children: ReturnType<typeof spawnMcp>[] = [], retained: unknown[] = [];
  let cleanupResult: Promise<void> | undefined, testName: string, ownerBaseline: string | null;

  beforeEach(async context => {
    cleanupResult = undefined; testName = context.task.name; ownerBaseline = null;
    tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'cg1740-mcp-'));
    realRoot = fs.realpathSync(tempDir);
    fs.mkdirSync(path.join(realRoot, 'src'));
    fs.writeFileSync(path.join(realRoot, 'src/a.ts'), 'export function a() { return 1; }\n');
    const cg = await CodeGraph.init(realRoot);
    await cg.indexAll();
    cg.close();
  });

  function cleanup(primary: string[] = []): Promise<void> {
    return cleanupResult ??= cleanupOwned(primary);
  }
  async function cleanupOwned(primary: string[]): Promise<void> {
    // Capture the fixture writer before stopping its proxies (#1782). The
    // daemon stop helper verifies its socket identity before signaling it.
    const owned = children.splice(0), childPids = new Set(owned.map(value => value.child.pid));
    const writerPid = readWriterPid(realRoot);
    // With the launchers gone no new daemon candidate can appear. A loser can
    // still be starting on a loaded machine; stopping the winner first would
    // let it take over the fixture being removed (#1773).
    const stopWriter = async (): Promise<void> => {
      await settleLosingCandidates(realRoot, () => readWriterPid(realRoot));
      if (writerPid === undefined || childPids.has(writerPid)) return;
      const result = await stopDaemonAt(realRoot);
      expect(result.pid).toBe(writerPid);
      expect(result.outcome).not.toBe('unverified');
    };
    const results = [
      ...await Promise.allSettled(owned.map(value => value.stop())),
      ...await Promise.allSettled([stopWriter()]),
    ];
    const failures = results.filter((result): result is PromiseRejectedResult => result.status === 'rejected')
      .map(result => String(result.reason).slice(0, 2048));
    if (writerPid !== undefined) {
      if (isProcessAlive(writerPid)) failures.push(`Writer ${writerPid} did not exit`);
    }
    // Observe the exact authority file after actual native closure, before
    // recursive disposal could hide a missing release or stale owner record.
    const writerAfterClosure = { path: getWriterPidPath(realRoot), absent: false, record: null as string | null, readFailure: null as string | null };
    try { writerAfterClosure.record = fs.readFileSync(writerAfterClosure.path, 'utf8'); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ENOENT') writerAfterClosure.absent = true;
      else writerAfterClosure.readFailure = String(error).slice(0, 2048);
    }
    try { expect(writerAfterClosure.absent, 'Owned writer.pid must be removed after native closure, before fixture disposal').toBe(true); }
    catch (error) { failures.push(String(error).slice(0, 2048)); }
    let spawnLog: string | null = null;
    try {
      const file = spawnLogFor(realRoot);
      if (fs.existsSync(file)) {
        if (fs.statSync(file).size > 65_536) throw new Error('Writer-lock spawn log overflow');
        spawnLog = fs.readFileSync(file, 'utf8');
      }
    } catch (error) { failures.push(String(error).slice(0, 2048)); }
    retained.push({ test: testName, root: realRoot, primary, cleanup: failures, writerPid, ownerBaseline, writerAfterClosure, spawnLog,
      children: owned.map(value => value.snapshot()) });
    // Preserve both clients' actual streams and closure facts before any
    // successful disposal; failed roots and the external spawn log stay inert.
    const evidence = process.env.VALIDATION_OUT ? path.join(process.env.VALIDATION_OUT, 'writer-lock-native-facts.json') : tempDir + '.native-facts.json';
    try {
      const encoded = JSON.stringify(retained);
      if (Buffer.byteLength(encoded) > 1024 * 1024) throw new Error('Writer-lock native evidence overflow');
      fs.writeFileSync(evidence, encoded);
    } catch (error) {
      failures.push(String(error).slice(0, 2048));
      process.stderr.write(`Writer-lock evidence failure: ${JSON.stringify({ root: realRoot, primary, failures })}\n`);
    }
    if (failures.length) throw new Error(`Writer-lock cleanup/evidence failed: ${failures.join('; ')}`);
    if (primary.length) return;
    removeSpawnLog(realRoot);
    // A stopped daemon's files can stay held for a moment (handle rundown, an
    // antivirus scan on close) with nothing to wait on. fs.rmSync gives up on
    // the first Windows access-denied error despite maxRetries (Node 24), so
    // use the asynchronous removal, which does back off and retry (#1773).
    await fs.promises.rm(tempDir, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    expect(fs.existsSync(tempDir)).toBe(false);
  }

  afterEach(context => cleanup(context.task.result?.errors?.slice(0, 4).map(error => String(error.message).slice(0, 2048))), 45_000);

  it('second CODEGRAPH_NO_DAEMON serve --mcp exits with writer-lock error', async () => {
    const env = {
      CODEGRAPH_NO_DAEMON: '1',
      CODEGRAPH_MCP_DEBUG: '1',
      CODEGRAPH_NO_WATCHDOG: '1',
      CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS: '0',
      // Avoid wasm --liftoff-only re-exec so lock.pid matches the spawned pid.
      CODEGRAPH_NO_RELAUNCH: '1',
      CODEGRAPH_WASM_RELAUNCHED: '1',
    };
    // The symbol is absent from the seeded index: a useful answer proves the
    // first primary's real catch-up, not just its initialize acknowledgement.
    fs.writeFileSync(path.join(realRoot, 'src/a.ts'), 'export function firstWriterReady() { return 2; }\n');
    const first = spawnMcp(realRoot, env);
    children.push(first);

    const lockPath = getWriterPidPath(realRoot);
    const deadline = Date.now() + 10000;
    // Rootless direct startup is intentionally inert. A real client chooses
    // the indexed default and joins an actual tool result before lock checks.
    first.send({ jsonrpc: '2.0', id: 0, method: 'initialize', params: { protocolVersion: '2025-11-25',
      capabilities: {}, clientInfo: { name: 'writer-lock-test', version: '1' }, rootUri: pathToFileURL(realRoot).href } });
    const initialized = await first.response(0, deadline);
    expect(initialized.error).toBeUndefined(); expect(initialized.result?.protocolVersion).toBeDefined();
    first.send({ jsonrpc: '2.0', method: 'notifications/initialized' });
    first.send({ jsonrpc: '2.0', id: 1, method: 'tools/call', params: { name: 'codegraph_search', arguments: { query: 'firstWriterReady' } } });
    const ready = await first.response(1, deadline);
    expect(ready.error).toBeUndefined(); expect(ready.result?.isError).not.toBe(true);
    expect(ready.result?.content?.[0]?.text).toContain('firstWriterReady');
    while (Date.now() < deadline && readWriterLock(realRoot)?.ready !== true) {
      await sleep(50);
    }
    expect(fs.existsSync(lockPath)).toBe(true);
    expect(first.child.exitCode).toBeNull();
    const ownerBefore = fs.readFileSync(lockPath, 'utf8');
    ownerBaseline = ownerBefore;
    expect(JSON.parse(ownerBefore)).toMatchObject({ pid: first.child.pid, ready: true });

    const nativePath = path.join(realRoot, 'second-native-opens.jsonl');
    const second = spawnMcp(realRoot, { ...env, CODEGRAPH_OWNERSHIP_NATIVE_LOG: nativePath },
      [path.join(__dirname, 'fixtures/mcp-project-ownership-observer.cjs')]);
    children.push(second);
    const secondDeadline = Date.now() + 10000;
    second.send({ jsonrpc: '2.0', id: 0, method: 'initialize', params: { protocolVersion: '2025-11-25',
      capabilities: {}, clientInfo: { name: 'writer-lock-second', version: '1' }, rootUri: pathToFileURL(realRoot).href } });
    const secondInitialized = await second.response(0, secondDeadline);
    expect(secondInitialized.error).toBeUndefined(); expect(secondInitialized.result?.protocolVersion).toBeDefined();
    second.send({ jsonrpc: '2.0', method: 'notifications/initialized' });
    const errors: WriterResponse['error'][] = [];
    for (const id of [1, 2]) {
      // No arguments.projectPath: explicit secondary readers are a separate,
      // valid RO contract and must not mask a refused primary selection.
      second.send({ jsonrpc: '2.0', id, method: 'tools/call', params: { name: 'codegraph_search', arguments: { query: 'firstWriterReady' } } });
      const refused = await second.response(id, secondDeadline);
      expect(refused.result, '#1740 second selected primary must refuse, not answer through a read-only default').toBeUndefined();
      expect(refused.error).toMatchObject({ code: -32603, data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: realRoot } });
      expect(refused.error?.message).toMatch(/writer lock held/i);
      expect(refused.error?.message).toMatch(/CODEGRAPH_NO_DAEMON/);
      expect(typeof refused.error?.data?.guidance).toBe('string'); expect(refused.error?.data?.guidance?.length).toBeGreaterThan(0);
      errors.push(refused.error);
      expect(fs.readFileSync(lockPath, 'utf8')).toBe(ownerBefore);
    }
    expect(errors[1]).toEqual(errors[0]);
    await second.stop(); // Exact-once replies are checked after real EOF.
    const observed = second.snapshot().nativeLog;
    expect(observed.readFailure).toBeNull();
    const opens = observed.text.split('\n').filter(Boolean).map(line => JSON.parse(line));
    expect(opens.length).toBeLessThanOrEqual(1);
    for (const open of opens) expect(open).toMatchObject({ file: path.join(realRoot, '.codegraph', 'codegraph.db'),
      readOnly: true, pid: second.child.pid, threadId: 0, writer: JSON.parse(ownerBefore) });
    expect(fs.readFileSync(lockPath, 'utf8')).toBe(ownerBefore);
    expect(first.child.exitCode).toBeNull();
    first.send({ jsonrpc: '2.0', id: 2, method: 'tools/call', params: { name: 'codegraph_search', arguments: { query: 'firstWriterReady' } } });
    const survivor = await first.response(2, secondDeadline);
    expect(survivor.error).toBeUndefined(); expect(survivor.result?.isError).not.toBe(true);
    expect(survivor.result?.content?.[0]?.text).toContain('firstWriterReady');
    expect(fs.readFileSync(lockPath, 'utf8')).toBe(ownerBefore);
    const lock = JSON.parse(fs.readFileSync(lockPath, 'utf8')) as { pid: number };
    expect(lock.pid).toBe(first.child.pid);
  }, 20000);

  it('default daemon mode still allows two proxies to share one writer', async () => {
    const env = {
      CODEGRAPH_NO_DAEMON: '0',
      CODEGRAPH_MCP_LOG_ATTACH: '1',
      CODEGRAPH_NO_WATCHDOG: '1',
      CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS: '0',
      CODEGRAPH_NO_RELAUNCH: '1',
      CODEGRAPH_WASM_RELAUNCHED: '1',
    };
    const a = spawnMcp(realRoot, env);
    const b = spawnMcp(realRoot, env);
    children.push(a, b);

    const lockPath = getWriterPidPath(realRoot);
    const deadline = Date.now() + 15000;
    while (Date.now() < deadline && !fs.existsSync(lockPath)) {
      await sleep(50);
    }
    expect(fs.existsSync(lockPath)).toBe(true);
    // writer.pid appears before the daemon binds and rewrites daemon.pid.
    // Attaching proves both happened, so cleanup never probes a daemon that is
    // still starting (#1773).
    const attached = () => [a, b].every((p) => p.getStderr().includes('Attached to shared daemon'));
    const attachDeadline = Date.now() + 30000;
    while (Date.now() < attachDeadline && !attached()) {
      await sleep(50);
    }
    expect(attached()).toBe(true);
    await sleep(1000);
    expect(a.child.exitCode).toBeNull();
    expect(b.child.exitCode).toBeNull();

    const lock = JSON.parse(fs.readFileSync(lockPath, 'utf8')) as { pid: number; mode: string };
    expect(lock.mode).toBe('daemon');
    expect(lock.pid).not.toBe(a.child.pid);
    expect(lock.pid).not.toBe(b.child.pid);

    // This must catch a live writer even on POSIX, where unlinking its open
    // database would otherwise hide the leak that blocks removal on Windows.
    await cleanup();
    expect(isProcessAlive(lock.pid)).toBe(false);
    expect(isProcessAlive(a.child.pid!)).toBe(false);
    expect(isProcessAlive(b.child.pid!)).toBe(false);
    expect(fs.existsSync(tempDir)).toBe(false);
  }, 90_000);
});


describe('reader-only MCP engine (#1963)', () => {
  let root: string;
  let engine: MCPEngine | undefined;
  const ordinaryOwners: MCPEngine[] = [];
  const retainedFacts: unknown[] = [];
  let bodyFailed = false;
  let bodyFailure: string | undefined;
  let secondary: string[] = [];
  const retainSecondary = (error: unknown): void => { if (secondary.length < 16) secondary.push(String(error).slice(0, 2048)); };
  const cleanupPhase = async (work: () => void | Promise<void>): Promise<void> => {
    try { await work(); } catch (error) { retainSecondary(error); }
  };

  beforeEach(async () => {
    engine = undefined;
    bodyFailed = false; bodyFailure = undefined; secondary = [];
    root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg1963-reader-')));
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function originalSymbol() { return 1; }\n');
    const cg = await CodeGraph.init(root);
    try { await cg.indexAll(); } finally { cg.close(); }
  });

  afterEach(async context => {
    await cleanupPhase(() => engine?.stop());
    for (const owner of ordinaryOwners.splice(0)) await cleanupPhase(() => owner.stop());
    const primary = bodyFailure ?? context.task.result?.errors?.slice(0, 4).map(error => String(error.message).slice(0, 2048)).join('; ').slice(0, 2048);
    let index: Record<string, string> | undefined;
    try {
      const dir = path.join(root, '.codegraph');
      index = Object.fromEntries(fs.readdirSync(dir).sort().map(name => [name,
        createHash('sha256').update(fs.readFileSync(path.join(dir, name))).digest('hex')]));
    } catch (error) { retainSecondary(error); }
    // Keep every reader workspace inert after cleanup; failed roots/captures
    // cannot be erased by a later successful afterEach.
    const row = { test: context.task.name, root, primary, secondary: [...secondary], index, retained: true };
    retainedFacts.push(row);
    try {
      if (process.env.VALIDATION_OUT) {
        const encoded = JSON.stringify(retainedFacts);
        if (Buffer.byteLength(encoded) > 1024 * 1024) throw new Error('Reader/writer facts exceeded 1 MiB');
        fs.writeFileSync(path.join(process.env.VALIDATION_OUT, 'writer-lock-reader-facts.json'), encoded);
      }
    } catch (error) {
      retainSecondary(error); row.secondary = [...secondary];
      // At most sixteen 2KiB errors plus the 2KiB primary: bounded fallback
      // retains all collected failures if the ordinary artifact cannot be saved.
      process.stderr.write(`Reader/writer evidence persistence failed: ${JSON.stringify({ root, primary, secondary })}\n`);
    }
    // The original body error keeps its original throw; every cleanup failure
    // remains separately retained. Cleanup alone is still an acceptance failure.
    if (secondary.length && !primary && !bodyFailed) throw new Error(`Owned reader/writer cleanup failed: ${secondary.join('; ')}`);
  });

  it.each(['async', 'retry', 'explicit'] as const)('%s initialization never updates the index or owner lock', async (mode) => {
    const lockPath = getWriterPidPath(root);
    // A real ordinary owner holds the slot; it makes no daemon claim and does
    // not watch subsequent edits. The separate reader must never take it over.
    const ordinaryOwner = new BuiltMCPEngine({ watch: false, queryPool: false, writerLockRoot: root });
    ordinaryOwners.push(ordinaryOwner);
    await ordinaryOwner.ensureInitialized(root);
    await ordinaryOwner.getToolHandler().execute('codegraph_status', {});
    const owner = fs.readFileSync(lockPath, 'utf8');
    expect(JSON.parse(owner).mode).toBe('fallback');
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function changedSymbol() { return 2; }\n');
    const { db } = createDatabase(path.join(root, '.codegraph', 'codegraph.db'), { readOnly: true });
    // readOnly overrides watching, pool workers, and writer acquisition.
    try {
      const version = db.pragma('data_version', { simple: true });
      engine = new BuiltMCPEngine({ readOnly: true, watch: true, queryPool: true, writerLockRoot: root });
      if (mode === 'async') await engine.ensureInitialized(root);
      if (mode === 'retry') {
        const empty = path.join(root, 'empty');
        fs.mkdirSync(empty);
        await engine.retryInitialize(empty);
      }
      const args = mode === 'explicit' ? { projectPath: root } : {};
      const result = await engine.getToolHandler().execute('codegraph_search', { ...args, query: 'originalSymbol' });
      expect(result.isError).not.toBe(true);
      expect(result.content[0].text).toContain('originalSymbol');
      fs.writeFileSync(path.join(root, 'added.ts'), 'export function addedSymbol() {}\n');
      // Includes the lifecycle's periodic ownership retry and watcher debounce.
      await sleep(2200);
      expect(db.pragma('data_version', { simple: true })).toBe(version);
      expect(fs.readFileSync(lockPath, 'utf8')).toBe(owner);
      await engine.stop();
      expect(db.pragma('data_version', { simple: true })).toBe(version);
      expect(fs.readFileSync(lockPath, 'utf8')).toBe(owner);
    } catch (error) {
      bodyFailed = true; bodyFailure = String(error).slice(0, 2048); throw error;
    } finally {
      await cleanupPhase(() => engine?.stop());
      await cleanupPhase(() => db.close());
      await cleanupPhase(() => ordinaryOwner.stop());
    }
  });

  it.each(['async', 'retry', 'explicit'] as const)('%s refuses an unproved live daemon without any index or owner byte change', async mode => {
    const owner = JSON.stringify({ pid: process.pid, mode: 'daemon', startedAt: Date.now() });
    fs.writeFileSync(getWriterPidPath(root), owner);
    const indexDir = path.join(root, '.codegraph');
    const snapshot = () => Object.fromEntries(fs.readdirSync(indexDir).sort().map(name => [name,
      createHash('sha256').update(fs.readFileSync(path.join(indexDir, name))).digest('hex')]));
    const before = snapshot();
    engine = new BuiltMCPEngine({ readOnly: true, watch: true, queryPool: true });
    const pending = mode === 'async' ? engine.ensureInitialized(root)
      : mode === 'retry' ? engine.retryInitialize(root)
      : engine.getToolHandler().execute('codegraph_search', { projectPath: root, query: 'originalSymbol' });
    await expect(pending).rejects.toMatchObject({ reason: 'unknown', projectRoot: root });
    expect(snapshot()).toEqual(before);
    await engine.stop();
    expect(snapshot()).toEqual(before);
  });

  it('watch:false still performs the normal startup catch-up', async () => {
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function changedSymbol() {}\n');
    engine = new BuiltMCPEngine({ watch: false });
    await engine.ensureInitialized(root);
    const result = await engine.getToolHandler().execute('codegraph_search', { query: 'changedSymbol' });
    expect(result.isError).not.toBe(true);
    expect(result.content[0].text).toContain('changedSymbol');
  });
});
