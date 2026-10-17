import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CodeGraph } from '../src';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { removeSpawnLog, settleLosingCandidates } from './daemon-candidates';
import { CompatibilityClient, alive, until, saveCompatibilityEvidence } from './query-compatibility-host';

describe('explicit-project service compatibility at the public MCP boundary', () => {
  it('refuses only incompatible beta work while admitted alpha/default and beta sessions keep their own owners and histories', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-explicit-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    const observations = path.join(fixture, 'workers.jsonl');
    const events = (): any[] => fs.existsSync(observations) ? fs.readFileSync(observations, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)) : [];
    const clients: CompatibilityClient[] = [];
    const daemonPids: number[] = [];
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    const evidence: Record<string, unknown> = { roots };
    let primary: unknown;
    try {
      for (const [index, root] of roots.entries()) {
        fs.mkdirSync(root);
        const prefix = index ? 'beta' : 'alpha';
        fs.writeFileSync(path.join(root, 'app.ts'), ['First', 'Survives', 'Refused'].map((name, n) =>
          `export function ${prefix}${name}() { return ${index * 100 + n + 41}; }`).join('\n') + '\n');
        const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      }
      const preload = path.join(__dirname, 'fixtures/query-unlimited-preload.cjs');
      for (const [index, root] of roots.entries()) {
        const client = new CompatibilityClient(root, index ? 'beta-admitted' : 'alpha-admitted', {
          CODEGRAPH_QUERY_POOL_SIZE: String(index + 1), CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
          QUERY_UNLIMITED_OBSERVATIONS: observations, QUERY_UNLIMITED_RELEASE: path.join(fixture, 'unused-release'),
        }, 80000, [preload]);
        clients.push(client); await client.initialize();
        const prefix = index ? 'beta' : 'alpha';
        const first = await client.tool('codegraph_explore', { query: prefix + 'First' }, 100);
        expect(first.error).toBeUndefined(); expect(first.result.content[0].text).toContain(`export function ${prefix}First()`);
        const before = parseQueryDiagnostics((await client.tool('codegraph_status')).result);
        daemonPids.push(before.ownership.pid);
        evidence[prefix + 'Before'] = before;
        evidence[prefix + 'Locks'] = ['daemon.pid', 'writer.pid'].map(name => fs.readFileSync(path.join(root, '.codegraph', name), 'utf8'));
      }
      const refused = await clients[0]!.tool('codegraph_explore', { query: 'betaRefused', projectPath: roots[1] }, 200);
      evidence.refused = refused;
      expect(refused.error).toMatchObject({ code: -32603, data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'incompatible', projectRoot: roots[1] } });
      expect(refused.error.data.guidance).toContain(roots[1]);
      const repeated = await clients[0]!.tool('codegraph_explore', { query: 'betaRefused', projectPath: roots[1] }, 201);
      expect(repeated.error).toMatchObject({ code: -32603, data: { reason: 'incompatible', projectRoot: roots[1] } });
      for (const id of [200, 201]) expect(clients[0]!.replies.filter(reply => reply.id === id)).toHaveLength(1);
      // Root decision: explicit-target refusal is scoped to that request. It
      // cannot poison the admitted default session or the target's own session.
      const survivors = await Promise.all(clients.map((client, index) => client.tool('codegraph_explore', { query: (index ? 'beta' : 'alpha') + 'Survives' }, 101)));
      for (const [index, reply] of survivors.entries()) {
        const prefix = index ? 'beta' : 'alpha';
        expect(reply.error).toBeUndefined(); expect(reply.result.content[0].text).toContain(prefix + 'Survives');
        const after = parseQueryDiagnostics((await clients[index]!.tool('codegraph_status')).result);
        expect(after.ownership).toEqual((evidence[prefix + 'Before'] as any).ownership);
        expect(after.effective).toEqual((evidence[prefix + 'Before'] as any).effective);
        expect(['daemon.pid', 'writer.pid'].map(name => fs.readFileSync(path.join(roots[index]!, '.codegraph', name), 'utf8'))).toEqual(evidence[prefix + 'Locks']);
        evidence[prefix + 'After'] = after;
        expect(clients[index]!.stderr).not.toContain('serving this session in-process');
      }
      const observed = events();
      const calls = observed.filter(event => event.event === 'call-start');
      expect(calls).toHaveLength(4);
      expect(calls.some(event => event.query === 'betaRefused')).toBe(false);
      const histories = observed.filter(event => event.event === 'session-history');
      expect(histories).toHaveLength(4);
      for (const [index, pid] of daemonPids.entries()) {
        const prefix = index ? 'beta' : 'alpha';
        expect(calls.filter(event => event.pid === pid).map(event => event.query)).toEqual([prefix + 'First', prefix + 'Survives']);
        expect(histories.filter(event => event.pid === pid).map(event => ({ query: event.query, index: event.index }))).toEqual([
          { query: prefix + 'First', index: 1 }, { query: prefix + 'Survives', index: 2 },
        ]);
        const ownedWorkers = observed.filter(event => event.event === 'spawn' && event.pid === pid);
        expect(ownedWorkers).toHaveLength(1);
        expect(ownedWorkers[0].execution.generation).toBe((evidence[prefix + 'Before'] as any).ownership.generation);
      }
      expect(new Set(histories.map(event => event.historyId)).size).toBe(2);
      Object.assign(evidence, { repeated, survivors, events: observed });
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (phase: string, action: () => Promise<void>) => { try { await action(); cleanup.push({ phase, error: null }); } catch (error) { cleanup.push({ phase, error: String(error) }); } };
      for (const client of clients) await attempt('client-' + client.name, () => client.close());
      for (const pid of daemonPids) await attempt('shared-daemon-exit-' + pid, () => until(() => !alive(pid), 20000));
      for (const root of roots) await attempt('losing-candidates-' + path.basename(root), () => settleLosingCandidates(root, () => process.pid));
      const observed = events();
      await attempt('all-query-workers-exited', async () => {
        expect(observed.filter(event => event.event === 'exit')).toHaveLength(observed.filter(event => event.event === 'spawn').length);
        expect(observed.filter(event => event.event === 'exit').every(event => event.code === 0 || event.code === 1)).toBe(true);
      });
      saveCompatibilityEvidence('compatibility-explicit', { ...evidence, events: observed,
        primary: primary instanceof Error ? primary.stack : String(primary ?? ''), cleanup, clients: clients.map(client => client.facts()) });
      const failures = cleanup.filter(item => item.error);
      if (!failures.length) { for (const root of roots) removeSpawnLog(root); fs.rmSync(fixture, { recursive: true, force: true }); }
      if (failures.length && !primary) throw new Error('Owned explicit-project cleanup incomplete: ' + JSON.stringify(failures));
    }
  }, 120000);
});
