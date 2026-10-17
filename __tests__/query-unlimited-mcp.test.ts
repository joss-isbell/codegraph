import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { CodeGraph } from '../src';
import { getWriterPidPath } from '../src/mcp/writer-lock';
import { parseQueryDiagnostics as diagnostic } from './query-diagnostics-helper';
import { removeSpawnLog, settleLosingCandidates, spawnLogFor } from './daemon-candidates';
import { CompatibilityClient, alive as compatibilityAlive, saveCompatibilityEvidence } from './query-compatibility-host';
import { queryUnlimitedCases, runUnlimitedMCPCase } from './query-unlimited-mcp-fixture';

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean, ms: number, detail: () => string = () => '') {
  const end = Date.now() + ms;
  while (!check()) { if (Date.now() > end) throw new Error(`Owned unlimited proof deadline: ${detail()}`); await wait(20); }
}

describe('independent unlimited concurrency through actual MCP', () => {
  it.runIf(process.platform === 'win32')('starts a shared worker service, reuses equivalent sessions, refuses conflicts and executes the strict direct preset', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-query-startup-join-')));
    const roots = [path.join(fixture, 'shared'), path.join(fixture, 'direct')];
    const observations = path.join(fixture, 'workers.jsonl');
    const release = path.join(fixture, 'release');
    const clients: CompatibilityClient[] = [];
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    const evidence: Record<string, unknown> = { fixture, roots };
    const daemonPids = new Set<number>();
    let candidatePids: number[] = [];
    let primary: unknown;
    let fixtureRemoved = false;
    let phase = 'fixture setup';
    const events = (): any[] => fs.existsSync(observations)
      ? fs.readFileSync(observations, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)) : [];
    const launch = (root: string, name: string, env: NodeJS.ProcessEnv) => {
      const client = new CompatibilityClient(root, name, {
        QUERY_UNLIMITED_OBSERVATIONS: observations, QUERY_UNLIMITED_RELEASE: release,
        QUERY_UNLIMITED_FAIL_DAEMON: '0', ...env,
      }, 15000, [path.join(__dirname, 'fixtures/query-unlimited-preload.cjs')]);
      clients.push(client); return client;
    };
    const search = async (client: CompatibilityClient, symbol: string) => {
      const reply = await client.tool('codegraph_search', { query: symbol });
      expect(reply.error, client.stderr).toBeUndefined();
      expect(reply.result.isError).toBeFalsy();
      expect(reply.result.content[0].text).toContain(symbol);
      expect(reply.result.content[0].text).toContain('app.ts:1');
      return reply;
    };
    const status = async (client: CompatibilityClient) => {
      const reply = await client.tool('codegraph_status');
      expect(reply.error, client.stderr).toBeUndefined();
      return diagnostic(reply.result);
    };
    try {
      for (const [index, root] of roots.entries()) {
        fs.mkdirSync(root);
        fs.writeFileSync(path.join(root, 'app.ts'), `export function ${index ? 'directJoinSymbol' : 'sharedJoinSymbol'}() { return ${index ? 907 : 701}; }\n`);
        const graph = await CodeGraph.init(root);
        try { await graph.indexAll(); } finally { graph.close(); }
      }
      // No timing/synthetic-result shim: the ordinary worker's SQLite handler
      // receives the request immediately and must return the indexed symbol.
      fs.writeFileSync(release, 'startup join does not gate query delivery');
      phase = 'ordinary shared startup and SQLite response';
      const owner = launch(roots[0], 'join-owner', {
        CODEGRAPH_QUERY_MODE: 'default', CODEGRAPH_QUERY_POOL_SIZE: '1', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
      });
      await owner.initialize();
      evidence.initial = await search(owner, 'sharedJoinSymbol');
      const before = await status(owner);
      evidence.before = before;
      daemonPids.add(before.ownership.pid);
      expect(before.effective).toMatchObject({ executionMode: 'shared-daemon', concurrency: { kind: 'finite', maximum: 1 },
        deadline: { kind: 'absent' }, retention: 'ordinary' });
      expect(before.ownership).toMatchObject({ resourceScope: 'shared-project', capability: 'read-write', writerAuthority: true, synchronizationAuthority: true });
      expect(before.ownership.pid).not.toBe(owner.child.pid);
      expect(before.lifecycle).toMatchObject({ state: 'active', dispatch: 'worker', ready: true, workers: 1 });
      const daemonLock = fs.readFileSync(path.join(roots[0], '.codegraph/daemon.pid'), 'utf8');
      const writerLock = fs.readFileSync(getWriterPidPath(roots[0]), 'utf8');
      const candidatesBefore = fs.readFileSync(spawnLogFor(roots[0]), 'utf8');
      const sharedWorkersBefore = events().filter(event => event.event === 'spawn');
      expect(sharedWorkersBefore).toHaveLength(1);
      expect(sharedWorkersBefore[0].pid).toBe(before.ownership.pid);

      phase = 'behavior-equivalent session reuses the same worker service';
      const peer = launch(roots[0], 'join-peer', {
        CODEGRAPH_QUERY_POOL_SIZE: '01', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: ' unlimited ',
      });
      await peer.initialize();
      evidence.peerResult = await search(peer, 'sharedJoinSymbol');
      const matching = await status(peer);
      evidence.matching = matching;
      expect(matching.ownership).toEqual(before.ownership);
      expect(matching.effective.concurrency).toEqual(before.effective.concurrency);
      expect(matching.effective.deadline).toEqual(before.effective.deadline);
      expect(matching.effective.retention).toBe(before.effective.retention);
      expect(matching.lifecycle).toMatchObject({ state: 'active', workers: 1 });
      expect(events().filter(event => event.event === 'spawn')).toEqual(sharedWorkersBefore);

      phase = 'incompatible acquisition refuses without replacement or fallback';
      const conflict = launch(roots[0], 'join-conflict', {
        CODEGRAPH_QUERY_POOL_SIZE: '2', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
      });
      await conflict.initialize();
      const refusal = await conflict.tool('codegraph_search', { query: 'sharedJoinSymbol' });
      evidence.refusal = refusal;
      expect(refusal.error).toMatchObject({ code: -32603, data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'incompatible', projectRoot: roots[0] } });
      expect(refusal.error.data.guidance).toContain(roots[0]);
      expect(refusal.result).toBeUndefined();
      expect(conflict.stderr).not.toMatch(/serving this session in-process|local read-only fallback|Shared daemon unavailable/);
      expect(fs.readFileSync(path.join(roots[0], '.codegraph/daemon.pid'), 'utf8')).toBe(daemonLock);
      expect(fs.readFileSync(getWriterPidPath(roots[0]), 'utf8')).toBe(writerLock);
      expect(fs.readFileSync(spawnLogFor(roots[0]), 'utf8')).toBe(candidatesBefore);
      expect(events().filter(event => event.event === 'spawn')).toEqual(sharedWorkersBefore);
      await conflict.close();

      phase = 'surviving session remains usable after the original session exits';
      await owner.close();
      evidence.survivingResult = await search(peer, 'sharedJoinSymbol');
      const surviving = await status(peer);
      evidence.surviving = surviving;
      expect(surviving.ownership).toEqual(before.ownership);
      expect(surviving.effective).toEqual(matching.effective);
      expect(surviving.lifecycle).toMatchObject({ state: 'active', workers: 1 });

      phase = 'ordinary direct startup uses the strict combined preset';
      const direct = launch(roots[1], 'join-direct-strict', {
        CODEGRAPH_NO_DAEMON: '1', CODEGRAPH_QUERY_MODE: 'unlimited',
      });
      await direct.initialize();
      evidence.directResult = await search(direct, 'directJoinSymbol');
      const strict = await status(direct);
      evidence.strict = strict;
      expect(strict.effective).toMatchObject({ source: 'strict', executionMode: 'direct', preset: 'unlimited', classification: 'unlimited',
        concurrency: { kind: 'unlimited' }, deadline: { kind: 'absent' }, retention: 'retire-idle' });
      expect(strict.implementation.support.combinedPreset).toBe(true);
      expect(strict.ownership).toMatchObject({ pid: direct.child.pid, resourceScope: 'local-engine', capability: 'read-write' });
      expect(strict.lifecycle).toMatchObject({ state: 'active', dispatch: 'worker', workers: 1 });
      const observed = events();
      const workers = observed.filter(event => event.event === 'spawn');
      expect(workers).toHaveLength(2);
      expect(new Set(workers.map(event => event.pid))).toEqual(new Set([before.ownership.pid, direct.child.pid]));
      expect(workers.find(event => event.pid === direct.child.pid).execution.policy).toMatchObject({ source: 'strict', preset: 'unlimited', deadline: { kind: 'absent' } });
      for (const worker of workers) {
        expect(observed.some(event => event.event === 'call-start' && event.pid === worker.pid && event.threadId === worker.threadId)).toBe(true);
        expect(observed.some(event => event.event === 'result' && event.pid === worker.pid && event.threadId === worker.threadId)).toBe(true);
      }
      phase = 'owned native cleanup';
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (name: string, action: () => unknown | Promise<unknown>) => {
        try { await action(); cleanup.push({ phase: name, error: null }); }
        catch (error) { cleanup.push({ phase: name, error: error instanceof Error ? error.stack ?? error.message : String(error) }); }
      };
      await attempt('capture daemon and candidate ownership', () => {
        for (const root of roots) {
          const pidFile = path.join(root, '.codegraph/daemon.pid');
          if (fs.existsSync(pidFile)) daemonPids.add(JSON.parse(fs.readFileSync(pidFile, 'utf8')).pid);
          if (fs.existsSync(spawnLogFor(root))) candidatePids.push(...fs.readFileSync(spawnLogFor(root), 'utf8').split(/\r?\n/)
            .map(Number).filter(pid => Number.isSafeInteger(pid) && pid > 0));
        }
      });
      await Promise.all(clients.map(client => attempt(`client ${client.name} exit`, () => client.close())));
      await attempt('shared daemon natural exit', () => until(() => [...daemonPids].every(pid => !compatibilityAlive(pid)), 20000));
      for (const root of roots) await attempt(`candidate exits ${root}`, () => settleLosingCandidates(root, () => process.pid, 15000));
      let observed: any[] = [];
      await attempt('verify all owned native processes and workers exited', () => {
        observed = events();
        expect(clients.every(client => client.child.exitCode === 0 && client.child.signalCode === null)).toBe(true);
        expect([...daemonPids, ...candidatePids].filter(compatibilityAlive)).toEqual([]);
        const workerKeys = (event: any) => `${event.pid}:${event.threadId}`;
        expect(observed.filter(event => event.event === 'exit').map(workerKeys).sort())
          .toEqual(observed.filter(event => event.event === 'spawn').map(workerKeys).sort());
      });
      if (cleanup.every(row => row.error === null)) await attempt('owned fixture and spawn-log removal', async () => {
        for (const root of roots) removeSpawnLog(root);
        await fs.promises.rm(fixture, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
        expect(fs.existsSync(fixture)).toBe(false); fixtureRemoved = true;
      });
      saveCompatibilityEvidence('startup-join', { ...evidence, phase, primary: primary instanceof Error ? primary.stack : String(primary ?? ''),
        outcome: primary || cleanup.some(row => row.error) ? 'fail' : 'pass', cleanup, fixtureRemoved, events: observed,
        clients: clients.map(client => client.facts()), daemonPids: [...daemonPids], candidatePids });
      const failures = cleanup.filter(row => row.error);
      if (failures.length && !primary) throw new Error('Owned startup-join cleanup incomplete: ' + JSON.stringify(failures));
      if (failures.length && primary) process.stderr.write('Owned startup-join cleanup also failed: ' + JSON.stringify(failures) + '\n');
    }
  }, 120000);

  it.each(queryUnlimitedCases)
  ('reports real requested/effective execution and routes safely in %s with %s',
    async (mode, profile) => runUnlimitedMCPCase(mode, profile), 120000);
});
