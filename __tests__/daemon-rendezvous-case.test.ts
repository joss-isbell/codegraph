/**
 * One project must map to ONE daemon rendezvous key, however its root is
 * spelled — the regression behind "the MCP server starts but never shares the
 * daemon".
 *
 * The daemon's named pipe (Windows) / tmpdir socket (POSIX) is
 * `…codegraph-<sha256(canonical root)>.slice(0,16)`, and the lockfile is shared
 * while the SOCKET NAME is derived independently by each process. So the moment
 * two processes hash the same directory differently they stop meeting: the
 * proxy's probe finds nothing, it spawns a redundant daemon, and that daemon
 * exits on the lock the first one holds
 * (`Another daemon (pid N) already holds the lock; exiting.`) — the session then
 * serves in-process, without the shared watcher or auto-sync.
 *
 * That is reachable on Windows because the root arrives two ways: a cwd-derived
 * root is the on-disk casing (`D:\work\codegraph`), while a client-supplied
 * `rootUri`/`workspaceFolders` path arrives as `file:///d%3A/…` → `d:\…`, and
 * `path.resolve` preserves whichever it got (NTFS is case-insensitive, so both
 * name one directory). Pinned here at the level that actually broke: the key,
 * not the filename.
 *
 * The real-filesystem assertions are Windows-gated — on POSIX the two spellings
 * are genuinely different directories, so only the Windows case has a
 * case-insensitive filesystem to converge on.
 */

import { afterEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { canonicalProjectRoot } from '../src/directory';
import { getDaemonSocketPath } from '../src/mcp/daemon-paths';
import type { ProjectRequest } from '../src/mcp/project-lifecycle';
const { acquireProject, admitQueryProject }: typeof import('../src/mcp/project-lifecycle') = require('../dist/mcp/project-lifecycle');
const { captureMcpIndexRoute }: typeof import('../src/directory') = require('../dist/directory');
const { resolveQueryPolicy }: typeof import('../src/mcp/query-policy') = require('../dist/mcp/query-policy');
const { CodeGraph }: typeof import('../src') = require('../dist');

const tmpDirs: string[] = [];
function makeDir(): string {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cg-rendezvous-'));
  tmpDirs.push(dir);
  return dir;
}
afterEach(() => {
  while (tmpDirs.length) {
    try {
      fs.rmSync(tmpDirs.pop()!, { recursive: true, force: true });
    } catch {
      /* best-effort */
    }
  }
});

/** The same drive letter, forced to one case. No-op on a path without a drive. */
function withDriveCase(p: string, upper: boolean): string {
  return p.replace(/^[a-z]:/i, (drive) => (upper ? drive.toUpperCase() : drive.toLowerCase()));
}

describe('daemon rendezvous key', () => {
  it('collapses redundant path segments to one socket name', () => {
    const root = makeDir();
    const indirect = path.join(root, 'a', '..', 'b', '.');
    const direct = path.join(root, 'b');
    // `.`, `..` and a trailing separator all name the same directory, so both
    // spellings must resolve to the same socket — otherwise `--path` with a
    // stray separator is enough to miss a running daemon.
    expect(getDaemonSocketPath(indirect)).toBe(getDaemonSocketPath(direct));
  });

  it.runIf(process.platform === 'win32')(
    'maps both drive-letter cases of one directory onto one pipe',
    () => {
      const root = makeDir();
      const upper = withDriveCase(root, true);
      const lower = withDriveCase(root, false);

      expect(canonicalProjectRoot(upper)).toBe(canonicalProjectRoot(lower));
      expect(getDaemonSocketPath(upper)).toBe(getDaemonSocketPath(lower));
      // …and the converged key is the on-disk casing, lowercased, so it does not
      // depend on which of the two the caller happened to hold.
      expect(canonicalProjectRoot(upper)).toBe(fs.realpathSync.native(root).toLowerCase());
    },
  );

  it.runIf(process.platform === 'win32')(
    'converges case variants even when the root cannot be realpath’d',
    () => {
      // The `.codegraph/` root normally exists, so this is the fallback arm —
      // but a divergence there would fail identically, so pin it.
      const missing = path.join(os.tmpdir(), 'cg-rendezvous-absent', 'nested');
      expect(canonicalProjectRoot(missing)).toBe(canonicalProjectRoot(missing.toUpperCase()));
    },
  );
});

/** True when the temp filesystem ignores case (default macOS APFS, NTFS). */
function caseInsensitiveTmp(): boolean {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'cg-CaseProbe-'));
  try {
    return fs.existsSync(dir.replace('cg-CaseProbe-', 'cg-caseprobe-'));
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

describe('in-process project sharing (#2278)', () => {
  it.runIf(caseInsensitiveTmp())('opens one graph for two casings of one root', async () => {
    const root = path.join(makeDir(), 'Repo');
    fs.mkdirSync(root);
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function rendezvous() {}\n');
    const graph = await CodeGraph.init(root);
    let primary: { error: unknown } | undefined;
    try { await graph.indexAll(); } catch (error) { primary = { error }; }
    try { graph.close(); } catch (error) {
      if (primary) process.stderr.write(`Rendezvous fixture close after ${String(primary.error).slice(0, 2048)}: ${String(error).slice(0, 2048)}\n`);
      else primary = { error };
    }
    if (primary) throw primary.error;
    let opened = 0;
    const nativeOpen = CodeGraph.openSync;
    const observation = vi.spyOn(CodeGraph, 'openSync').mockImplementation((...args) => { opened++; return nativeOpen(...args); });
    const policy = resolveQueryPolicy({ poolSize: '0' }, 'shared-daemon', 3), lifetime = new AbortController();
    let a: Awaited<ReturnType<typeof acquireProject>> | undefined, b: Awaited<ReturnType<typeof acquireProject>> | undefined;
    try {
      const own = captureMcpIndexRoute(root), alias = captureMcpIndexRoute(path.join(path.dirname(root), 'REPO'));
      if (!own || !alias) throw new Error('Rendezvous fixture missing own route');
      const request: ProjectRequest = { route: own, intent: 'read', lifetime: lifetime.signal, watchOptions: {},
        admit: (selected, signal) => admitQueryProject(selected.root, policy, signal) };
      a = await acquireProject({ ...request, route: own });
      b = await acquireProject({ ...request, route: alias });
      expect(opened).toBe(1);
      expect(b.cg).toBe(a.cg);
    } catch (error) { primary = { error }; }
    finally {
      const releases = await Promise.allSettled([a?.release(), b?.release()]);
      observation.mockRestore();
      for (const result of releases) if (result.status === 'rejected') {
        if (primary) process.stderr.write(`Rendezvous lease cleanup after ${String(primary.error).slice(0, 2048)}: ${String(result.reason).slice(0, 2048)}\n`);
        else primary = { error: result.reason };
      }
    }
    if (primary) throw primary.error;
  });
});
