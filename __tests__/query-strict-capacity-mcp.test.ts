import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { CodeGraph, getDatabasePath } from '../src';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { Client } from './query-unlimited-mcp-fixture';
import { removeSpawnLog, settleLosingCandidates } from './daemon-candidates';
import { PublicNativeEvidence } from './public-native-evidence';

const wait = (milliseconds: number) => new Promise(resolve => setTimeout(resolve, milliseconds));
async function until(check: () => boolean, milliseconds: number): Promise<void> {
  const deadline = Date.now() + milliseconds;
  while (!check()) { if (Date.now() > deadline) throw new Error('strict public capacity observation deadline'); await wait(20); }
}
const alive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };

describe('strict finite capacity through actual public startup', () => {
  it.each(['direct', 'writable-fallback'] as const)
  ('keeps exactly 20 occupied workers and four queued callers in %s', async mode => {
    const started = process.hrtime.bigint();
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-strict-capacity-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    const observations = path.join(fixture, 'workers.jsonl');
    const release = path.join(fixture, 'release');
    const maximum = 20;
    const demand = maximum + 4;
    let client: Client | undefined;
    let primaryFailure: unknown;
    let phase = 'fixture setup';
    let wave: Promise<any[]> | undefined;
    let answered = 0;
    const diagnostics: any[] = [];
    const routeProbes: any[] = [];
    const queueSamples: any[] = [];
    const inputs: Array<{ root: string; source: string }> = [];
    const evidence = new PublicNativeEvidence(roots, observations, `strict-capacity-${mode}.json`);
    const cleanup: Array<{ phase: string; outcome: string; error?: string }> = [];
    const events = () => evidence.events();
    const ownership = () => evidence.owners();
    const persist = (stage: string) => evidence.persist(stage, () => ({
        mode, phase, stage, maximum, demand, answered, primaryFailure: primaryFailure ? String(primaryFailure) : null,
        elapsedMs: Number(process.hrtime.bigint() - started) / 1e6, roots, inputs, diagnostics, routeProbes, queueSamples,
        fixtureRemoved: !fs.existsSync(fixture),
        capture: client?.capture(), replies: client?.replies, cleanup,
      }));
    try {
      for (const [index, root] of roots.entries()) {
        fs.mkdirSync(root);
        fs.writeFileSync(path.join(root, 'app.ts'), `export function overlapSymbol() { return 42; }\nexport function ${index ? 'beta' : 'alpha'}RouteSymbol() { return ${index}; }\n`);
        inputs.push({ root, source: fs.readFileSync(path.join(root, 'app.ts'), 'utf8') });
        const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      }
      client = new Client(roots[0], mode, observations, release, 'unlimited', { environment: {
        CODEGRAPH_QUERY_MODE: 'unlimited', CODEGRAPH_QUERY_POOL_SIZE: '20', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
      } });
      expect((await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {},
        clientInfo: { name: 'strict-capacity-proof', version: '1' } })).serverInfo.name).toBe('codegraph');
      const status = async (stage: string) => {
        const value = parseQueryDiagnostics(await client!.request('tools/call', { name: 'codegraph_status', arguments: {} }));
        diagnostics.push({ stage, value });
        expect(value.requested).toEqual({ mode: 'unlimited', poolSize: '20', busyTimeoutMs: 'unlimited' });
        expect(value.effective).toMatchObject({ executionMode: mode, source: 'strict', preset: 'unlimited',
          concurrency: { kind: 'finite', maximum }, deadline: { kind: 'absent' }, retention: 'retire-idle', classification: 'custom' });
        expect(value.implementation.support.combinedPreset).toBe(true);
        expect(value.ownership).toMatchObject({ pid: client!.child.pid, writerAuthority: true,
          synchronizationAuthority: true, capability: 'read-write', resourceScope: 'local-engine' });
        return value;
      };
      phase = 'cold strict status';
      const cold = await status('cold');
      expect(cold.lifecycle).toMatchObject({ state: 'cold', ready: false, workers: 0, executing: 0, queued: 0 });
      expect(events().filter(event => event.event === 'spawn')).toHaveLength(0);
      const probeRoutes = async (stage: string) => {
        for (const index of [0, 1, 0]) {
          const query = `${index ? 'beta' : 'alpha'}RouteSymbol`;
          const arguments_ = { query, ...(index ? { projectPath: roots[index] } : {}) };
          const search = await client!.request('tools/call', { name: 'codegraph_search', arguments: arguments_ });
          const explore = await client!.request('tools/call', { name: 'codegraph_explore', arguments: arguments_ });
          for (const result of [search, explore]) {
            expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain(query);
            expect(result.content[0].text).toContain('app.ts');
            expect(result.content[0].text).not.toContain(`${index ? 'alpha' : 'beta'}RouteSymbol`);
          }
          routeProbes.push({ stage, root: roots[index], route: index ? 'explicit' : 'default', query, search, explore });
        }
      };
      phase = 'real routes before capacity'; await probeRoutes('before');
      phase = 'finite capacity with queued callers';
      wave = Promise.all(Array.from({ length: demand }, (_, index) => client!.query('overlapSymbol', index % 2 ? roots[1] : undefined)
        .then(result => { answered++; return result; })));
      void wave.catch(() => {});
      await until(() => events().filter(event => event.event === 'call-start' && event.query === 'overlapSymbol').length >= maximum, 30000);
      for (let index = 0; index < 3; index++) {
        const occupied = await status(`occupied ${index}`); queueSamples.push(occupied.lifecycle);
        expect(occupied.lifecycle).toMatchObject({ state: 'active', ready: true, workers: maximum, executing: maximum, queued: 4, starting: 0, retiring: 0 });
        expect(occupied.ownership).toEqual(cold.ownership);
        expect(answered).toBe(0);
        expect(events().filter(event => event.event === 'spawn')).toHaveLength(maximum);
        expect(events().filter(event => event.event === 'call-start' && event.query === 'overlapSymbol')).toHaveLength(maximum);
        await wait(100);
      }
      const initial = events();
      const firstCalls = initial.filter(event => event.event === 'call-start' && event.query === 'overlapSymbol');
      expect(new Set(firstCalls.map(event => `${event.pid}:${event.threadId}`)).size).toBe(maximum);
      const firstIds = new Set(firstCalls.map(event => `${event.pid}:${event.id}`));
      expect(initial.filter(event => event.event === 'result' && firstIds.has(`${event.pid}:${event.id}`))).toHaveLength(0);
      phase = 'release actual handlers'; fs.writeFileSync(release, 'allow unchanged SQLite handlers to finish all admitted work');
      const results = await wave;
      expect(results).toHaveLength(demand); expect(answered).toBe(demand);
      for (const result of results) {
        expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain('overlapSymbol');
        expect(result.content[0].text).toContain('app.ts:1'); expect(result.content[0].text).not.toContain('after admission');
      }
      const after = events();
      const calls = after.filter(event => event.event === 'call-start' && event.query === 'overlapSymbol');
      expect(calls).toHaveLength(demand);
      expect(new Set(calls.map(event => `${event.pid}:${event.id}`)).size).toBe(demand);
      expect(new Set(calls.map(event => `${event.pid}:${event.threadId}`)).size).toBe(maximum);
      expect(after.filter(event => event.event === 'spawn')).toHaveLength(maximum);
      for (const call of calls) {
        const index = call.context.project.route === 'explicit' ? 1 : 0;
        expect(call.context.project).toEqual({ route: index ? 'explicit' : 'default', root: roots[index], index: getDatabasePath(roots[index]) });
        expect(client.replies.filter(reply => reply.id === call.context.requestId)).toHaveLength(1);
        expect(after.filter(event => event.event === 'result' && event.pid === call.pid && event.id === call.id)).toHaveLength(1);
      }
      for (const event of after.filter(event => event.event === 'spawn')) expect(event.execution.policy).toMatchObject({
        executionMode: mode, source: 'strict', preset: 'unlimited', concurrency: { kind: 'finite', maximum },
        deadline: { kind: 'absent' }, retention: 'retire-idle' });
      let starting = 0; let peakStarting = 0;
      for (const event of after) {
        if (event.event === 'spawn') peakStarting = Math.max(peakStarting, ++starting);
        if (event.event === 'ready') starting--;
      }
      expect(starting).toBe(0); expect(peakStarting).toBeLessThanOrEqual(2);
      expect((await status('drained')).lifecycle).toMatchObject({ workers: maximum, queued: 0, executing: 0 });
      phase = 'real routes after capacity'; await probeRoutes('after');
      const history = events().filter(event => event.event === 'session-history');
      expect(history).toHaveLength(6); expect(new Set(history.map(event => event.historyId)).size).toBe(1);
      expect(history.map(event => event.projectRoot)).toEqual([roots[0], roots[1], roots[0], roots[0], roots[1], roots[0]]);
      expect(history.map(event => event.index)).toEqual([1, 1, 2, 3, 2, 4]);
      const candidates = events().filter(event => event.event === 'unavailable-candidate');
      if (mode === 'writable-fallback') {
        expect(candidates).toHaveLength(1); expect(candidates[0].candidatePid).toBeGreaterThan(0);
        expect(events().filter(event => event.event === 'unavailable-candidate-exit').map(event => [event.candidatePid, event.code]))
          .toEqual([[candidates[0].candidatePid, 78]]);
        expect(client.stderr).toContain('Shared daemon unavailable'); expect(client.stderr).not.toMatch(/version.*differs|Attached to shared daemon/);
      } else expect(candidates).toHaveLength(0);
      phase = 'success';
    } catch (error) { primaryFailure = error; throw error; }
    finally {
      const attempt = async (name: string, action: () => unknown | Promise<unknown>) => {
        const item: { phase: string; outcome: string; error?: string } = { phase: name, outcome: 'running' };
        cleanup.push(item); persist(`before ${name}`);
        try { await action(); item.outcome = 'pass'; }
        catch (error) { item.outcome = 'fail'; item.error = String(error); }
        persist(`after ${name}`);
      };
      persist('before cleanup');
      await attempt('release owned worker gate', () => fs.writeFileSync(release, 'cleanup release'));
      await attempt('settle admitted wave', async () => {
        if (!wave) return;
        let timer: NodeJS.Timeout | undefined;
        try {
          await Promise.race([wave, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('capacity wave cleanup deadline')), 10000); })]);
        } finally { if (timer) clearTimeout(timer); }
      });
      await attempt('native client close', async () => { if (client) await client.close(); });
      await attempt('native candidate cleanup', () => settleLosingCandidates(roots[0], () => process.pid, 10000));
      await attempt('native worker and candidate facts', () => {
        const observed = events();
        expect(observed.filter(event => event.event === 'exit')).toHaveLength(observed.filter(event => event.event === 'spawn').length);
        expect(observed.filter(event => event.event === 'unavailable-candidate').filter(event => alive(event.candidatePid))).toEqual([]);
        if (client) { expect(client.capture().failure).toBeNull(); expect(client.capture().nativeClose).toEqual({ code: 0, signal: null }); }
        expect(ownership().every(owner => owner.writer === null && owner.daemon === null)).toBe(true);
      });
      persist('after native cleanup');
      if (!primaryFailure && cleanup.every(item => item.outcome === 'pass') && !evidence.failed) {
        await attempt('candidate log removal', () => removeSpawnLog(roots[0]));
        if (cleanup.every(item => item.outcome === 'pass') && !evidence.failed) {
          await attempt('fixture removal', () => { evidence.allowObservationRemoval(); fs.rmSync(fixture, { recursive: true, force: true }); });
        }
      } else cleanup.push({ phase: 'retain failed fixture and candidate log', outcome: 'retained' });
      persist('closed');
      if (!primaryFailure && (cleanup.some(item => item.outcome === 'fail') || evidence.failed)) {
        throw new Error(`strict capacity cleanup/evidence failed: ${JSON.stringify({ cleanup, evidence: evidence.failures() })}`);
      }
    }
  }, 110000);
});
