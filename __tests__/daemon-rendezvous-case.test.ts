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
  it.runIf(caseInsensitiveTmp())('opens one graph for two casings of one root', async ({ task }) => {
    // Production refusals may be frozen. Retain only bounded scalar facts,
    // never the original object, before cleanup and Vitest can obscure it.
    type Scalar = { value: string | number | boolean | null; truncated: boolean };
    const scalar = (value: unknown, budget: number): Scalar => {
      if (value === null || value === undefined) return { value: null, truncated: false };
      if (typeof value === 'number' || typeof value === 'boolean') return { value, truncated: false };
      let text: string;
      try { text = String(value); } catch { text = '[unprintable thrown value]'; }
      const fits = (length: number): boolean => Buffer.byteLength(JSON.stringify(text.slice(0, length))) <= budget;
      if (fits(text.length)) return { value: text, truncated: false };
      let low = 0, high = text.length;
      while (low < high) {
        const middle = Math.ceil((low + high) / 2);
        if (fits(middle)) low = middle; else high = middle - 1;
      }
      return { value: text.slice(0, low), truncated: true };
    };
    const snapshot = (error: unknown, secondary = false) => {
      const field = (name: string, budget: number): Scalar => {
        try { return scalar(error !== null && (typeof error === 'object' || typeof error === 'function')
          ? (error as Record<string, unknown>)[name] : undefined, secondary ? Math.min(budget, 256) : budget); }
        catch { return scalar('[unreadable ' + name + ']', 256); }
      };
      return { type: typeof error, name: field('name', 256), message: field('message', 4096),
        stack: field('stack', secondary ? 1024 : 12288), code: field('code', 256), reason: field('reason', 256),
        projectRoot: field('projectRoot', 2048), guidance: field('guidance', 2048), thrown: scalar(error, secondary ? 256 : 2048),
        frozen: Object.isFrozen(error), extensible: Object.isExtensible(error) };
    };
    type Failure = ReturnType<typeof snapshot>;
    const facts = { schemaVersion: 1, lastPhase: 'body.enter', primary: null as Failure | null,
      secondary: [] as Array<{ phase: string; failure: Failure }>,
      routes: null as Record<string, Scalar> | null,
      releases: [] as Array<{ caller: string; status: string; failure?: Failure }>,
      persistenceErrors: [] as Scalar[] };
    const persist = (): void => {
      if (!process.env.VALIDATION_OUT) return;
      try { fs.writeFileSync(path.join(process.env.VALIDATION_OUT, 'daemon-rendezvous-case-facts.json'), JSON.stringify(facts) + '\n'); }
      catch (error) {
        if (facts.persistenceErrors.length < 3) facts.persistenceErrors.push(scalar(error, 256));
        try { process.stderr.write('[rendezvous-evidence-failure] ' + JSON.stringify(scalar(error, 256)) + '\n'); } catch { /* retain primary */ }
      }
    };
    const remember = (error: unknown): void => {
      if (!facts.primary) facts.primary = snapshot(error);
      else if (facts.secondary.length < 3) facts.secondary.push({ phase: facts.lastPhase, failure: snapshot(error, true) });
      persist();
    };
    let nonce = '', sequence = 0;
    const phase = (name: string): void => {
      facts.lastPhase = name;
      if (facts.primary) persist();
      try {
        if (sequence >= 20) return;
        nonce ||= Math.random().toString(36).slice(2, 14);
        const marker = { id: task.id, pid: process.pid, nonce, sequence: ++sequence, phase: name };
        Object.assign(task.meta, { rendezvousDiagnostic: marker });
        process.stderr.write(('[rendezvous-phase] ' + JSON.stringify(marker)).slice(0, 511) + '\n');
      } catch { /* Observation must not replace the original test/error disposition. */ }
    };
    phase('body.enter');
    try {
      const root = path.join(makeDir(), 'Repo');
      fs.mkdirSync(root);
      fs.writeFileSync(path.join(root, 'app.ts'), 'export function rendezvous() {}\n');
      let graph: Awaited<ReturnType<typeof CodeGraph.init>>;
      phase('fixture.init.begin');
      try { graph = await CodeGraph.init(root); } finally { phase('fixture.init.settled'); }
      phase('fixture.index.begin');
      try { await graph.indexAll(); } catch (error) { remember(error); }
      finally { phase('fixture.index.settled'); }
      phase('fixture.close.begin');
      try { graph.close(); } catch (error) { remember(error); }
      finally { phase('fixture.close.settled'); }
      if (!facts.primary) {
        let opened = 0;
        const nativeOpen = CodeGraph.openSync;
        const observation = vi.spyOn(CodeGraph, 'openSync').mockImplementation((...args) => { opened++; return nativeOpen(...args); });
        const policy = resolveQueryPolicy({ poolSize: '0' }, 'shared-daemon', 3), lifetime = new AbortController();
        let a: Awaited<ReturnType<typeof acquireProject>> | undefined, b: Awaited<ReturnType<typeof acquireProject>> | undefined;
        try {
          phase('routes.begin');
          const own = captureMcpIndexRoute(root), alias = captureMcpIndexRoute(path.join(path.dirname(root), 'REPO'));
          facts.routes = { ownRoot: scalar(own?.root, 2048), ownIndex: scalar(own?.index, 2048),
            aliasRoot: scalar(alias?.root, 2048), aliasIndex: scalar(alias?.index, 2048) };
          phase('routes.return');
          if (!own || !alias) throw new Error('Rendezvous fixture missing own route');
          const request: ProjectRequest = { route: own, intent: 'read', lifetime: lifetime.signal, watchOptions: {},
            admit: (selected, signal) => admitQueryProject(selected.root, policy, signal) };
          phase('acquire.a.begin');
          try { a = await acquireProject({ ...request, route: own }); } finally { phase('acquire.a.settled'); }
          phase('acquire.b.begin');
          try { b = await acquireProject({ ...request, route: alias }); } finally { phase('acquire.b.settled'); }
          expect(opened).toBe(1);
          expect(b.cg).toBe(a.cg);
        } catch (error) { remember(error); }
        finally {
          phase('release.begin');
          try {
            const releases = await Promise.allSettled([Promise.resolve().then(() => a?.release()), Promise.resolve().then(() => b?.release())]);
            facts.releases = releases.map((result, index) => ({ caller: index === 0 ? 'own' : 'alias', status: result.status,
              ...(result.status === 'rejected' ? { failure: snapshot(result.reason, true) } : {}) }));
            for (const result of releases) if (result.status === 'rejected') remember(result.reason);
          } finally { phase('release.settled'); observation.mockRestore(); }
        }
      }
    } catch (error) { remember(error); }
    phase(facts.primary ? 'body.throw' : 'body.return');
    if (facts.primary) {
      persist();
      try { process.stderr.write('[rendezvous-failure] ' + JSON.stringify(facts) + '\n'); } catch { /* preserve relay */ }
      const original = facts.primary, relay = new Error(String(original.message.value ?? original.thrown.value));
      if (original.name.value !== null) relay.name = String(original.name.value);
      if (original.stack.value !== null) relay.stack = String(original.stack.value);
      Object.assign(relay, { code: original.code.value, reason: original.reason.value,
        projectRoot: original.projectRoot.value, guidance: original.guidance.value, rendezvousObservation: facts });
      throw relay;
    }
  });
});
