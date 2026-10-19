/**
 * Daemon bind-failure cleanup — issue #974.
 *
 * A detached daemon acquires the `.codegraph/daemon.pid` lock (via
 * `tryAcquireDaemonLock`) BEFORE it binds its socket. If the bind then fails —
 * e.g. AF_UNIX is unsupported/unreliable on the filesystem (the WSL2 DrvFs
 * hazard behind #974) — `Daemon.start()` must release that lockfile before it
 * propagates the error and exits. Otherwise the next launcher reads a stale lock
 * pointing at the now-dead pid and the process pileup the issue reported recurs.
 *
 * We force a deterministic bind failure by planting a *directory* at the socket
 * path: `unlinkSync` (the daemon's stale-socket clear) can't remove a directory,
 * so it survives and `listen()` fails with EADDRINUSE.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CodeGraph } from '../src';
import { Daemon, tryAcquireDaemonLock, finalizeDaemonExit } from '../src/mcp/daemon';
import { getDaemonPidPath, getDaemonSocketPath } from '../src/mcp/daemon-paths';

const tmpRoots: string[] = [];
const retainedFixtures = new Set<string>();
afterEach(() => {
  while (tmpRoots.length) {
    const root = tmpRoots.pop()!;
    if (retainedFixtures.has(root)) continue;
    try { fs.rmSync(root, { recursive: true, force: true }); } catch { /* best-effort */ }
  }
});

describe('Daemon.start() bind failure (#974)', () => {
  it.runIf(process.platform !== 'win32')('releases the lockfile it acquired when the socket cannot bind', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-bind-')));
    tmpRoots.push(root);

    let sockPath: string | undefined, daemon: Daemon | undefined;
    let restoreExit: (() => void) | undefined, previousExitCode: typeof process.exitCode;
    let failed = false, primary: unknown, drained = false;
    const cleanupFailure = (phase: string, error: unknown): void => {
      if (!failed) { failed = true; primary = error; }
      else try { process.stderr.write(`Bind fixture ${phase}: ${String(error).slice(0, 4096)}\n`); } catch { /* preserve primary */ }
    };
    try {
      // An actual daemon requires its own existing DB. Prepare only this
      // disposable fixture so refusal-before-schema cannot mask the bind path.
      const graph = await CodeGraph.init(root); graph.close();

      // Keep preparation inside the same failure-retention boundary as start.
      const lock = tryAcquireDaemonLock(root);
      expect(lock.kind).toBe('acquired');
      const pidPath = getDaemonPidPath(root);
      expect(fs.existsSync(pidPath)).toBe(true);

      // A directory survives stale-socket clearing and makes listen fail.
      sockPath = getDaemonSocketPath(root);
      // Track the exact selected path even if mkdir creates it only partially.
      tmpRoots.push(sockPath);
      fs.mkdirSync(sockPath, { recursive: true });

      // Install the restoration handle before configuring the spy or creating
      // a daemon that must later be stopped without exiting the test process.
      previousExitCode = process.exitCode;
      const exit = vi.spyOn(process, 'exit');
      restoreExit = () => exit.mockRestore();
      exit.mockImplementation(() => undefined as never);
      daemon = new Daemon(root);
      await expect(daemon.start()).rejects.toThrow(/EADDRINUSE/);
      // The lockfile must be gone so the next launcher doesn't spin on a stale lock.
      expect(fs.existsSync(pidPath)).toBe(false);
    } catch (error) { failed = true; primary = error; }
    finally {
      try { if (daemon) { await daemon.stop('bind fixture cleanup'); drained = true; } }
      catch (error) { cleanupFailure('native cleanup failure', error); }
      finally {
        if (restoreExit) {
          try { restoreExit(); } catch (error) { cleanupFailure('exit spy restore failure', error); }
          try { process.exitCode = previousExitCode; } catch (error) { cleanupFailure('exitCode restore failure', error); }
        }
      }
      if (failed || !drained) { retainedFixtures.add(root); if (sockPath) retainedFixtures.add(sockPath); }
    }
    if (failed) throw primary;
  });
});

/**
 * Windows shutdown must not force `process.exit()` while the recursive file
 * watcher is still tearing down — that aborts the daemon with a libuv
 * `UV_HANDLE_CLOSING` assertion (0xC0000409), reproducible when the indexed tree
 * contains a nested repo. `finalizeDaemonExit` drains on Windows and exits
 * immediately elsewhere; both branches are exercised here by injecting the
 * platform + exit fn (so it runs on any host).
 */
describe('finalizeDaemonExit — Windows drains instead of aborting mid-watcher-close', () => {
  for (const platform of ['linux', 'darwin'] as const) {
    it(`exits immediately on ${platform}`, () => {
      const exit = vi.fn();
      const backstop = finalizeDaemonExit(platform, exit);
      expect(exit).toHaveBeenCalledTimes(1);
      expect(exit).toHaveBeenCalledWith(0);
      expect(backstop).toBeNull();
    });
  }

  it('on win32 defers exit (lets the loop drain), then force-exits via an unref\'d backstop', () => {
    vi.useFakeTimers();
    const prevExitCode = process.exitCode;
    const exit = vi.fn();
    try {
      const backstop = finalizeDaemonExit('win32', exit);
      // No synchronous exit — the process must drain its closing watch handles first.
      expect(exit).not.toHaveBeenCalled();
      expect(backstop).not.toBeNull();
      // Success code is set so a natural drain exits 0.
      expect(process.exitCode).toBe(0);
      // If a stray handle keeps the loop alive, the backstop still forces exit.
      vi.advanceTimersByTime(2_000);
      expect(exit).toHaveBeenCalledWith(0);
    } finally {
      vi.useRealTimers();
      process.exitCode = prevExitCode; // don't leak a 0 exit code into the runner
    }
  });
});
