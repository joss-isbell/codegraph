import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { pathToFileURL } from 'url';
import { CodeGraph } from '../src';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { Client } from './query-unlimited-mcp-fixture';
import { removeSpawnLog, settleLosingCandidates } from './daemon-candidates';
import { PublicNativeEvidence } from './public-native-evidence';

const wait = (milliseconds: number) => new Promise(resolve => setTimeout(resolve, milliseconds));
async function until(check: () => boolean, milliseconds: number): Promise<void> {
  const deadline = Date.now() + milliseconds;
  while (!check()) { if (Date.now() > deadline) throw new Error('owned waiting process did not settle'); await wait(20); }
}
const alive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };

describe('independent unlimited waiting through actual MCP requests', () => {
  it.each(['direct', 'writable-fallback', 'shared-daemon'] as const)
  ('returns the real 47-second worker result and ordinary project routes in %s', async mode => {
    // Per experiment: real worker release at 47s, complete case bound 90s,
    // independent calling-client deadline 80s. Other hosts may impose limits.
    const started = process.hrtime.bigint();
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-waiting-mcp-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    const observations = path.join(fixture, 'workers.jsonl');
    const release = path.join(fixture, 'unused-file-gate');
    const inputs: Array<{ root: string; source: string }> = [];
    const diagnostics: any[] = [];
    const routeProbes: any[] = [];
    const daemonPids = new Set<number>();
    const cleanup: Array<{ phase: string; outcome: string; error?: string }> = [];
    const evidence = new PublicNativeEvidence(roots, observations, `waiting-${mode}.json`);
    let client: Client | undefined;
    let primaryFailure: unknown;
    let phase = 'fixture setup';
    let elapsed: number | undefined;
    const events = () => evidence.events();
    const ownership = () => evidence.owners();
    const discoverDaemons = () => {
      for (const owner of ownership()) if (owner.daemon) daemonPids.add(JSON.parse(owner.daemon).pid);
    };
    const persist = (stage: string) => evidence.persist(stage, () => ({
        mode, phase, stage, primaryFailure: primaryFailure ? String(primaryFailure) : null,
        clock: 'process.hrtime.bigint', releaseMs: 47000, caseBoundMs: 90000, clientDeadlineMs: 80000,
        elapsed, caseElapsedMs: Number(process.hrtime.bigint() - started) / 1e6, roots, inputs,
        diagnostics, routeProbes, replies: client?.replies, capture: client?.capture(),
        daemonPids: [...daemonPids], daemonsStillAlive: [...daemonPids].filter(alive),
        cleanup, fixtureRemoved: !fs.existsSync(fixture),
      }));
    try {
      for (const [index, root] of roots.entries()) {
        fs.mkdirSync(root);
        fs.writeFileSync(path.join(root, 'app.ts'), `export function ${index ? 'betaTimingSymbol' : 'alphaTimingSymbol'}() { return 42; }\nexport function delayedTimingSymbol() { return 47; }\n`);
        inputs.push({ root, source: fs.readFileSync(path.join(root, 'app.ts'), 'utf8') });
        const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      }
      client = new Client(roots[0], mode, observations, release, 'default', {
        preload: path.join(__dirname, 'fixtures/query-waiting-preload.cjs'), requestDeadlineMs: 80000, closeDeadlineMs: 7500, noWatch: true,
        environment: { CODEGRAPH_QUERY_MODE: 'default', CODEGRAPH_QUERY_POOL_SIZE: undefined,
          CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited', QUERY_WAITING_OBSERVATIONS: observations },
      });
      const initialize = await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {},
        clientInfo: { name: 'waiting-proof', version: '1' }, rootUri: pathToFileURL(roots[0]).href });
      expect(initialize.serverInfo.name).toBe('codegraph');
      const maximum = Math.max(1, Math.min(os.cpus().length - 1, mode === 'shared-daemon' ? 16 : 2));
      const status = async (stage: string) => {
        const value = parseQueryDiagnostics(await client!.request('tools/call', { name: 'codegraph_status', arguments: {} }));
        diagnostics.push({ stage, value });
        expect(value.requested).toEqual({ mode: 'default', busyTimeoutMs: 'unlimited' });
        expect(value.effective).toMatchObject({ executionMode: mode, source: 'strict', preset: 'default',
          concurrency: { kind: 'finite', maximum }, deadline: { kind: 'absent' }, retention: 'ordinary', classification: 'custom' });
        expect(value.implementation.support.combinedPreset).toBe(true);
        expect(value.ownership).toMatchObject({ writerAuthority: true, synchronizationAuthority: true,
          capability: 'read-write', resourceScope: mode === 'shared-daemon' ? 'shared-project' : 'local-engine' });
        if (mode === 'shared-daemon') daemonPids.add(value.ownership.pid);
        else expect(value.ownership.pid).toBe(client!.child.pid);
        return value;
      };
      phase = 'cold actual selected policy';
      const cold = await status('cold');
      expect(cold.lifecycle).toMatchObject({ state: 'cold', ready: false, workers: 0, queued: 0, executing: 0 });
      expect(events().filter(event => event.event === 'spawn')).toHaveLength(0);
      phase = 'ordinary real project routes';
      for (const index of [0, 1, 0]) {
        const query = `${index ? 'beta' : 'alpha'}TimingSymbol`;
        const arguments_ = { query, ...(index ? { projectPath: roots[index] } : {}) };
        const result = await client.request('tools/call', { name: 'codegraph_search', arguments: arguments_ });
        expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain(query); expect(result.content[0].text).toContain('app.ts:1');
        expect(result.content[0].text).not.toContain(`${index ? 'alpha' : 'beta'}TimingSymbol`);
        routeProbes.push({ route: index ? 'explicit' : 'default', root: roots[index], query, result });
      }
      phase = 'real completion beyond former deadline';
      const admitted = process.hrtime.bigint();
      const result = await client.query('delayedTimingSymbol');
      elapsed = Number(process.hrtime.bigint() - admitted) / 1e6;
      expect(elapsed).toBeGreaterThanOrEqual(47000); expect(elapsed).toBeLessThan(80000);
      expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain('delayedTimingSymbol');
      expect(result.content[0].text).toContain('app.ts:2'); expect(result.content[0].text).not.toContain('after admission');
      const after = await status('after real completion');
      expect(after.effective).toEqual(cold.effective); expect(after.ownership).toEqual(cold.ownership);
      expect(after.lifecycle).toMatchObject({ state: 'active', ready: true, queued: 0, executing: 0 });
      const observed = events();
      const spawned = observed.filter(event => event.event === 'spawn');
      expect(spawned.length).toBeGreaterThan(0);
      for (const event of spawned) expect(event.execution.policy).toMatchObject({ executionMode: mode,
        source: 'strict', preset: 'default', concurrency: { kind: 'finite', maximum },
        deadline: { kind: 'absent' }, retention: 'ordinary' });
      const controlled = observed.filter(event => event.event.startsWith('controlled-'));
      expect(controlled.map(event => event.event)).toEqual(['controlled-start', 'controlled-release']);
      expect(controlled[1].elapsedMs).toBeGreaterThanOrEqual(47000);
      expect(BigInt(controlled[1].monotonicNs) - BigInt(controlled[0].monotonicNs)).toBeGreaterThanOrEqual(47000000000n);
      expect(observed.filter(event => event.event === 'result' && event.threadId === controlled[0].threadId && event.id === controlled[0].id
        && BigInt(event.monotonicNs) >= BigInt(controlled[1].monotonicNs))).toHaveLength(1);
      expect(client.replies.filter(reply => reply.id === controlled[0].context.requestId)).toHaveLength(1);
      const candidates = observed.filter(event => event.event === 'unavailable-candidate');
      if (mode === 'writable-fallback') {
        expect(candidates).toHaveLength(1); expect(candidates[0].candidatePid).toBeGreaterThan(0);
        expect(observed.filter(event => event.event === 'unavailable-candidate-exit').map(event => [event.candidatePid, event.code]))
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
      await attempt('capture owned daemon identities', discoverDaemons);
      persist('before native cleanup');
      await attempt('native client close', async () => { if (client) await client.close(); });
      await attempt('native daemon idle close', () => until(() => [...daemonPids].every(pid => !alive(pid)), 10000));
      await attempt('native candidate cleanup', () => settleLosingCandidates(roots[0], () => process.pid, 7500));
      await attempt('native close and worker facts', () => {
        const observed = events();
        expect(observed.filter(event => event.event === 'exit')).toHaveLength(observed.filter(event => event.event === 'spawn').length);
        expect(observed.filter(event => event.event === 'unavailable-candidate').filter(event => alive(event.candidatePid))).toEqual([]);
        if (client) { expect(client.capture().failure).toBeNull(); expect(client.capture().nativeClose).toEqual({ code: 0, signal: null }); }
        expect(ownership().every(owner => owner.writer === null && owner.daemon === null)).toBe(true);
      });
      persist('after native cleanup');
      if (!primaryFailure) await attempt('90-second bound before fixture removal', () => expect(Number(process.hrtime.bigint() - started) / 1e6).toBeLessThan(90000));
      if (!primaryFailure && cleanup.every(item => item.outcome === 'pass') && !evidence.failed) {
        await attempt('candidate log removal', () => removeSpawnLog(roots[0]));
        if (cleanup.every(item => item.outcome === 'pass') && !evidence.failed) {
          await attempt('fixture removal', () => { evidence.allowObservationRemoval(); fs.rmSync(fixture, { recursive: true, force: true }); });
        }
      } else cleanup.push({ phase: 'retain failed fixture and candidate log', outcome: 'retained' });
      if (!primaryFailure) await attempt('complete original 90-second case bound', () => expect(Number(process.hrtime.bigint() - started) / 1e6).toBeLessThan(90000));
      persist('closed');
      if (!primaryFailure && (cleanup.some(item => item.outcome === 'fail') || evidence.failed)) {
        throw new Error(`waiting native cleanup/evidence failed: ${JSON.stringify({ cleanup, evidence: evidence.failures() })}`);
      }
    }
  }, 90000);
});
