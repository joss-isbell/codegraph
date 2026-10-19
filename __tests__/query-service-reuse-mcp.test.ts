import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CodeGraph } from '../src';
import type { QueryDiagnostics } from '../src/mcp/query-diagnostics';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { removeSpawnLog, settleLosingCandidates } from './daemon-candidates';
import { CompatibilityClient, alive, until, saveCompatibilityEvidence } from './query-compatibility-host';

const profiles = [
  { name: 'defaults versus equal explicit overrides', owner: {}, peer: (actual: QueryDiagnostics): NodeJS.ProcessEnv => ({
    CODEGRAPH_QUERY_MODE: ' default ', CODEGRAPH_QUERY_POOL_SIZE: ` 00${actual.effective.concurrency.kind === 'finite' ? actual.effective.concurrency.maximum : 'invalid'} `,
    CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: ' 045000 ',
  }) },
  { name: 'trimmed decimal spellings', owner: { CODEGRAPH_QUERY_POOL_SIZE: '1', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: '47000' },
    peer: (): NodeJS.ProcessEnv => ({ CODEGRAPH_QUERY_MODE: ' default ', CODEGRAPH_QUERY_POOL_SIZE: ' 0001 ', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: ' 0047000 ' }) },
  { name: 'zero pool with inapplicable deadlines', owner: { CODEGRAPH_QUERY_POOL_SIZE: '0', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: '1000' },
    peer: (): NodeJS.ProcessEnv => ({ CODEGRAPH_QUERY_POOL_SIZE: ' 000 ', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: ' unlimited ' }) },
  { name: 'independent unlimited controls', owner: { CODEGRAPH_QUERY_POOL_SIZE: 'unlimited', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited' },
    peer: (): NodeJS.ProcessEnv => ({ CODEGRAPH_QUERY_MODE: ' default ', CODEGRAPH_QUERY_POOL_SIZE: ' unlimited ', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: ' unlimited ' }) },
] as const;

describe('behavior-equivalent shared services through actual MCP', () => {
  it.each(profiles)('reuses $name with independent session histories and explicit project reads', async profile => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-equivalent-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    const observations = path.join(fixture, 'workers.jsonl');
    const clients: CompatibilityClient[] = [];
    let daemonPid: number | undefined;
    let primary: unknown;
    const evidence: Record<string, unknown> = { profile: profile.name, roots, ownerSettings: profile.owner };
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    const events = (): any[] => fs.existsSync(observations) ? fs.readFileSync(observations, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)) : [];
    try {
      for (const [index, root] of roots.entries()) {
        fs.mkdirSync(root);
        const prefix = index ? 'beta' : 'alpha';
        fs.writeFileSync(path.join(root, 'app.ts'), ['Left', 'Right', 'LeftAgain', 'RightAgain', 'LeftSurvives'].map((side, n) =>
          `export function ${prefix}${side}() { return ${index * 100 + n + 17}; }`).join('\n') + '\n');
        const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      }
      const observationEnv = { QUERY_UNLIMITED_OBSERVATIONS: observations, QUERY_UNLIMITED_RELEASE: path.join(fixture, 'unused-release') };
      const preload = path.join(__dirname, 'fixtures/query-unlimited-preload.cjs');
      const first = new CompatibilityClient(roots[0]!, 'equivalent-left', { ...observationEnv, ...profile.owner }, 80000, [preload]);
      clients.push(first); await first.initialize();
      const before = parseQueryDiagnostics((await first.tool('codegraph_status')).result);
      daemonPid = before.ownership.pid;
      expect(before.effective.executionMode).toBe('shared-daemon');
      expect(before.implementation.support).toEqual({ independentControls: true, combinedPreset: true, idleRetirement: true });
      const peerSettings = profile.peer(before);
      const second = new CompatibilityClient(roots[0]!, 'equivalent-right', { ...observationEnv, ...peerSettings }, 80000, [preload]);
      clients.push(second); await second.initialize();
      const lockBefore = ['daemon.pid', 'writer.pid'].map(name => fs.readFileSync(path.join(roots[0]!, '.codegraph', name), 'utf8'));
      const queried = [
        ['alphaLeft', 'alphaRight', undefined],
        ['betaLeft', 'betaRight', roots[1]],
        ['alphaLeftAgain', 'alphaRightAgain', undefined],
      ] as const;
      const replies: any[] = [];
      for (const [offset, [left, right, projectPath]] of queried.entries()) {
        const pair = await Promise.all([first.tool('codegraph_explore', { query: left, ...(projectPath ? { projectPath } : {}) }, 100 + offset),
          second.tool('codegraph_explore', { query: right, ...(projectPath ? { projectPath } : {}) }, 100 + offset)]);
        for (const [index, reply] of pair.entries()) {
          expect(reply.error).toBeUndefined(); expect(reply.result.isError).toBeFalsy();
          expect(reply.result.content[0].text).toContain(index ? right : left);
          if (offset === 0) expect(reply.result.content[0].text).toContain(`export function ${index ? right : left}()`);
        }
        replies.push(pair);
      }
      const after = parseQueryDiagnostics((await second.tool('codegraph_status')).result);
      expect(after.ownership).toEqual(before.ownership); expect(after.effective).toEqual(before.effective);
      expect(['daemon.pid', 'writer.pid'].map(name => fs.readFileSync(path.join(roots[0]!, '.codegraph', name), 'utf8'))).toEqual(lockBefore);
      for (const client of clients) {
        expect(client.stderr).toContain(`(pid ${daemonPid},`);
        expect(client.stderr).not.toContain('serving this session in-process');
        for (const id of [100, 101, 102]) expect(client.replies.filter(reply => reply.id === id)).toHaveLength(1);
      }
      const observed = events();
      const histories = observed.filter(event => event.event === 'session-history');
      expect(histories).toHaveLength(6);
      const historyIds = [...new Set(histories.map(event => event.historyId))];
      expect(historyIds).toHaveLength(2);
      for (const side of ['Left', 'Right']) {
        const firstEmission = histories.find(event => event.query === 'alpha' + side);
        expect(firstEmission).toBeDefined();
        expect(histories.filter(event => event.historyId === firstEmission.historyId).map(event =>
          ({ query: event.query, root: event.projectRoot, index: event.index }))).toEqual([
          { query: 'alpha' + side, root: roots[0], index: 1 }, { query: 'beta' + side, root: roots[1], index: 1 },
          { query: 'alpha' + side + 'Again', root: roots[0], index: 2 },
        ]);
      }
      // A disconnected matching peer cannot retire or replace the shared pool
      // still serving this admitted session, or mix its delivered history.
      await second.close();
      const surviving = await first.tool('codegraph_explore', { query: 'alphaLeftSurvives' }, 103);
      expect(surviving.error).toBeUndefined(); expect(surviving.result.isError).toBeFalsy();
      expect(surviving.result.content[0].text).toContain('alphaLeftSurvives');
      expect(first.replies.filter(reply => reply.id === 103)).toHaveLength(1);
      const afterDisconnect = parseQueryDiagnostics((await first.tool('codegraph_status')).result);
      expect(afterDisconnect.ownership).toEqual(before.ownership); expect(afterDisconnect.effective).toEqual(before.effective);
      expect(['daemon.pid', 'writer.pid'].map(name => fs.readFileSync(path.join(roots[0]!, '.codegraph', name), 'utf8'))).toEqual(lockBefore);
      const afterDisconnectEvents = events();
      const afterDisconnectHistories = afterDisconnectEvents.filter(event => event.event === 'session-history');
      expect(afterDisconnectHistories).toHaveLength(7);
      const leftHistory = histories.find(event => event.query === 'alphaLeft').historyId;
      expect(afterDisconnectHistories.filter(event => event.historyId === leftHistory).map(event =>
        ({ query: event.query, root: event.projectRoot, index: event.index }))).toEqual([
        { query: 'alphaLeft', root: roots[0], index: 1 }, { query: 'betaLeft', root: roots[1], index: 1 },
        { query: 'alphaLeftAgain', root: roots[0], index: 2 }, { query: 'alphaLeftSurvives', root: roots[0], index: 3 },
      ]);
      expect(afterDisconnectHistories.filter(event => event.historyId !== leftHistory)).toEqual(histories.filter(event => event.historyId !== leftHistory));
      const spawned = afterDisconnectEvents.filter(event => event.event === 'spawn');
      const calls = afterDisconnectEvents.filter(event => event.event === 'call-start');
      if (profile.name === 'zero pool with inapplicable deadlines') {
        expect(before.effective).toMatchObject({ concurrency: { kind: 'disabled' }, deadline: { kind: 'inapplicable' } });
        expect(spawned).toEqual([]); expect(calls).toEqual([]);
      } else {
        expect(spawned.length).toBeGreaterThan(0); expect(calls).toHaveLength(7);
        expect(new Set(calls.map(event => event.context.sessionId)).size).toBe(2);
        expect(new Set(calls.map(event => event.context.admissionId)).size).toBe(7);
        expect(calls.filter(event => event.context.project.route === 'explicit').map(event => event.context.project.root)).toEqual([roots[1], roots[1]]);
        for (const spawn of spawned) { expect(spawn.pid).toBe(daemonPid); expect(spawn.execution.generation).toBe(before.ownership.generation); }
        for (const call of calls) expect(call.context.execution.generation).toBe(before.ownership.generation);
      }
      Object.assign(evidence, { before, after, afterDisconnect, peerSettings, replies, surviving,
        peerExitBeforeSurvivingQuery: second.child.exitCode, events: afterDisconnectEvents });
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (phase: string, action: () => Promise<void>) => { try { await action(); cleanup.push({ phase, error: null }); } catch (error) { cleanup.push({ phase, error: String(error) }); } };
      for (const client of clients) await attempt('client-' + client.name, () => client.close());
      if (daemonPid) await attempt('shared-daemon-exit', () => until(() => !alive(daemonPid!), 20000));
      for (const root of roots) await attempt('losing-candidates-' + path.basename(root), () => settleLosingCandidates(root, () => process.pid));
      const finalEvents = events();
      await attempt('all-query-workers-exited', async () => {
        expect(finalEvents.filter(event => event.event === 'exit')).toHaveLength(finalEvents.filter(event => event.event === 'spawn').length);
        expect(finalEvents.filter(event => event.event === 'exit').every(event => event.code === 0 || event.code === 1)).toBe(true);
      });
      saveCompatibilityEvidence('reuse-' + profile.name.replace(/\W+/g, '-'), { ...evidence, events: finalEvents,
        primary: primary instanceof Error ? primary.stack : String(primary ?? ''), cleanup, clients: clients.map(client => client.facts()) });
      const failures = cleanup.filter(item => item.error);
      if (!failures.length) { for (const root of roots) removeSpawnLog(root); fs.rmSync(fixture, { recursive: true, force: true }); }
      if (failures.length && !primary) throw new Error('Owned equivalent-service cleanup incomplete: ' + JSON.stringify(failures));
    }
  }, 120000);
});
