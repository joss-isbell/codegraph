/**
 * Project writer lock (#1740) — unit coverage for acquire / re-entrant /
 * stale-dead-pid / live-holder refusal.
 */

import { afterAll, afterEach, describe, expect, it } from 'vitest';
import { spawn, type ChildProcess } from 'child_process';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CodeGraph } from '../src';
import { captureMcpIndexRoute, type McpIndexRoute } from '../src/directory';
import { MCPEngine } from '../src/mcp/engine';
import {
  decodeWriterLockInfo,
  getWriterPidPath,
  markWriterReady,
  readWriterLock,
  releaseWriterLock,
  tryAcquireWriterLock,
  writerLockHeldMessage,
} from '../src/mcp/writer-lock';

// Observe real constructors without replacing SQLite or engine behavior.
const native: {
  begin(roots: string[]): void;
  facts(): { total: number; overflow: boolean; records: unknown[] };
  restore(): void;
} = require('./fixtures/query-admission-sqlite-observer.cjs');

afterAll(() => native.restore());

describe('writer lock (#1740)', () => {
  let dir: string | undefined;
  let engine: MCPEngine | null = null;
  let holder: { child: ChildProcess; closed: Promise<void>; errors: Error[] } | null = null;

  async function boundedCleanup(work: Promise<void>, label: string): Promise<void> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await Promise.race([work, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Writer-lock ${label} did not settle`)), 3000);
      })]);
    } finally { if (timer) clearTimeout(timer); }
  }

  afterEach(async () => {
    const failures: unknown[] = [];
    if (engine) {
      try { await boundedCleanup(engine.stop(), 'engine drain'); engine = null; }
      catch (error) { failures.push(error); }
    }
    if (holder) {
      const owned = holder;
      try {
        if (owned.child.exitCode === null && owned.child.signalCode === null) owned.child.kill('SIGKILL');
        await boundedCleanup(owned.closed, `holder ${owned.child.pid} close`);
        holder = null;
        if (owned.errors.length) throw new AggregateError(owned.errors, 'Writer-lock holder failed');
      } catch (error) { failures.push(error); }
    }
    // Never erase a fixture while an owned engine or native child is unjoined.
    if (dir && !engine && !holder && !failures.length) {
      try {
        const stat = fs.lstatSync(dir);
        if (path.dirname(dir) !== fs.realpathSync(os.tmpdir()) || !path.basename(dir).startsWith('cg1740-lock-')
          || !stat.isDirectory() || stat.isSymbolicLink() || fs.realpathSync(dir) !== dir) {
          throw new Error(`Refusing unsafe writer-lock fixture cleanup: ${dir}`);
        }
        releaseWriterLock(dir);
        await fs.promises.rm(dir, { recursive: true, maxRetries: 10, retryDelay: 100 });
        dir = undefined;
      } catch (error) { failures.push(error); }
    }
    // Vitest retains the original body error separately from this hook error.
    if (failures.length) throw new AggregateError(failures, 'Writer-lock fixture cleanup failed');
  }, 15_000);

  function makeProject(): string {
    if (dir || engine || holder) throw new Error('Previous writer-lock fixture cleanup is incomplete');
    dir = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg1740-lock-')));
    fs.mkdirSync(path.join(dir, '.codegraph'), { recursive: true });
    return dir;
  }

  function makeEngineProject(): McpIndexRoute {
    const root = makeProject();
    native.begin([root]);
    // Only engine cases need an actual own index; primitive lock fixtures stay bare.
    const graph = CodeGraph.initSync(root);
    graph.close();
    expect(native.facts().total).toBeGreaterThan(0);
    expect(native.facts().overflow).toBe(false);
    const route = captureMcpIndexRoute(root);
    if (!route) throw new Error('Writer-lock fixture did not produce its own index route');
    return route;
  }

  function spawnHolder(script: string): ChildProcess {
    const child = spawn(process.execPath, ['-e', script], { stdio: 'ignore' });
    const errors: Error[] = [];
    const closed = new Promise<void>(resolve => child.once('close', () => resolve()));
    child.on('error', error => errors.push(error));
    holder = { child, closed, errors };
    if (!child.pid) throw new Error('Failed to spawn writer-lock holder');
    return child;
  }

  it('acquires and releases writer.pid', () => {
    const root = makeProject();
    const r = tryAcquireWriterLock(root, 'direct');
    expect(r.kind).toBe('acquired');
    expect(fs.existsSync(getWriterPidPath(root))).toBe(true);
    const info = decodeWriterLockInfo(fs.readFileSync(getWriterPidPath(root), 'utf8'));
    expect(info?.pid).toBe(process.pid);
    expect(info?.mode).toBe('direct');
    releaseWriterLock(root);
    expect(fs.existsSync(getWriterPidPath(root))).toBe(false);
  });

  it('is re-entrant for the same pid', () => {
    const root = makeProject();
    expect(tryAcquireWriterLock(root, 'daemon').kind).toBe('acquired');
    const again = tryAcquireWriterLock(root, 'fallback');
    expect(again.kind).toBe('acquired');
    releaseWriterLock(root);
  });

  it('reports taken when a live foreign pid holds the lock', () => {
    const root = makeProject();
    // A foreign process that is genuinely alive, rather than a pid assumed to
    // be: PID 1 is init on Linux but does not exist on Windows, where the lock
    // then reads the holder as dead and correctly acquires — the assertion was
    // failing on the fixture, not on the lock. A parked child is alive
    // everywhere, and it is what the lock actually promises not to steal from.
    // Uses the shared holder so afterEach reaps it (same as the fallback-
    // engine case below).
    const child = spawnHolder('setTimeout(() => {}, 60000)');
    fs.writeFileSync(
      getWriterPidPath(root),
      JSON.stringify({ pid: child.pid, mode: 'direct', startedAt: Date.now() }) + '\n',
      { flag: 'wx' },
    );
    const r = tryAcquireWriterLock(root, 'direct');
    expect(r.kind).toBe('taken');
    if (r.kind === 'taken') {
      expect(r.existing?.pid).toBe(child.pid);
      const msg = writerLockHeldMessage(r.existing, r.pidPath);
      expect(msg).toMatch(/writer lock held/i);
      expect(msg).toMatch(/CODEGRAPH_NO_DAEMON/);
      expect(msg).toMatch(/daemon stop/);
    }
  });

  it('clears a stale dead-pid lock and acquires', () => {
    const root = makeProject();
    // Pick a pid that is extremely unlikely to be alive.
    const deadPid = 2147483646;
    fs.writeFileSync(
      getWriterPidPath(root),
      JSON.stringify({ pid: deadPid, mode: 'direct', startedAt: Date.now() }) + '\n',
    );
    const r = tryAcquireWriterLock(root, 'direct');
    expect(r.kind).toBe('acquired');
    releaseWriterLock(root);
  });

  it('publishes catch-up completion for readers without changing writer identity', () => {
    const root = makeProject();
    const acquired = tryAcquireWriterLock(root, 'direct');
    expect(acquired.kind).toBe('acquired');
    const before = readWriterLock(root);
    expect(before?.ready).toBe(false);
    markWriterReady(root);
    expect(readWriterLock(root)).toEqual({ ...before, ready: true });
    tryAcquireWriterLock(root, 'fallback');
    expect(readWriterLock(root)).toEqual({ ...before, ready: true });
  });

  it('lets a fallback engine atomically claim and release writer ownership', async () => {
    const route = makeEngineProject();
    const root = route.root;
    engine = new MCPEngine({ projectRoute: route, writerLockRoot: root, watch: false, queryPool: false });

    expect(decodeWriterLockInfo(fs.readFileSync(getWriterPidPath(root), 'utf8'))).toMatchObject({
      pid: process.pid,
      mode: 'fallback',
    });

    await engine.stop();
    engine = null;
    expect(fs.existsSync(getWriterPidPath(root))).toBe(false);
  });

  it('rejects a fallback engine before opening when another process owns writer.pid', () => {
    const route = makeEngineProject();
    const root = route.root;
    const child = spawnHolder('setInterval(() => {}, 1000)');
    const owner = JSON.stringify({ pid: child.pid, mode: 'daemon', startedAt: Date.now() }) + '\n';
    fs.writeFileSync(getWriterPidPath(root), owner, { flag: 'wx' });
    const indexDir = path.dirname(route.index);
    const snapshot = () => Object.fromEntries(fs.readdirSync(indexDir).sort()
      .map(name => [name, fs.readFileSync(path.join(indexDir, name))]));
    const before = snapshot();
    expect(() => process.kill(child.pid!, 0)).not.toThrow();
    native.begin([root]);

    expect(() => {
      engine = new MCPEngine({ projectRoute: route, writerLockRoot: root, watch: false, queryPool: false });
    }).toThrow(/writer lock held/i);
    expect(native.facts()).toEqual({ total: 0, overflow: false, records: [] });
    expect(child.exitCode).toBeNull();
    expect(child.signalCode).toBeNull();
    expect(() => process.kill(child.pid!, 0)).not.toThrow();
    const ownerAfter = fs.readFileSync(getWriterPidPath(root), 'utf8');
    expect(ownerAfter).toBe(owner);
    expect(decodeWriterLockInfo(ownerAfter)).toMatchObject({ pid: child.pid, mode: 'daemon' });
    expect(snapshot()).toEqual(before);
  });
});
