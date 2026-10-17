import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CodeGraph } from '../src';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { removeSpawnLog, settleLosingCandidates } from './daemon-candidates';
import { CompatibilityClient, alive, until, saveCompatibilityEvidence } from './query-compatibility-host';

describe('shared service compatibility after an actual transport loss', () => {
  it('classifies an unknown retry before replaying buffered or new explicit-project work locally', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-retry-')));
    const root = path.join(fixture, 'project'); fs.mkdirSync(root);
    const observations = path.join(fixture, 'workers.jsonl');
    const network = path.join(fixture, 'network.jsonl');
    const unknownHello = path.join(fixture, 'unknown-hello');
    const dropRequest = path.join(fixture, 'drop-request');
    const events = (file: string): any[] => fs.existsSync(file) ? fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)) : [];
    const clients: CompatibilityClient[] = [];
    let daemonPid: number | undefined;
    let primary: unknown;
    const evidence: Record<string, unknown> = { root };
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    try {
      fs.writeFileSync(path.join(root, 'app.ts'), 'export function ownerFirst() { return 1; }\nexport function peerFirst() { return 2; }\nexport function refusedOrphan() { return 3; }\nexport function refusedExplicit() { return 4; }\nexport function ownerSurvives() { return 5; }\n');
      const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      const preloads = ['query-unlimited-preload.cjs', 'query-compatibility-network.cjs'].map(name => path.join(__dirname, 'fixtures', name));
      const env = { CODEGRAPH_QUERY_POOL_SIZE: '2', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
        QUERY_UNLIMITED_OBSERVATIONS: observations, QUERY_UNLIMITED_RELEASE: path.join(fixture, 'unused-release'),
        QUERY_COMPAT_NETWORK_OBSERVATIONS: network, QUERY_COMPAT_UNKNOWN_HELLO: unknownHello };
      const owner = new CompatibilityClient(root, 'healthy-owner', env, 80000, preloads);
      clients.push(owner); await owner.initialize();
      expect((await owner.tool('codegraph_explore', { query: 'ownerFirst' }, 100)).result.content[0].text).toContain('ownerFirst');
      const before = parseQueryDiagnostics((await owner.tool('codegraph_status')).result);
      daemonPid = before.ownership.pid;
      const locks = ['daemon.pid', 'writer.pid'].map(name => fs.readFileSync(path.join(root, '.codegraph', name), 'utf8'));
      const peer = new CompatibilityClient(root, 'retrying-peer', { ...env, QUERY_COMPAT_DROP_REQUEST: dropRequest,
        CODEGRAPH_DAEMON_RETRY_MS: '100', CODEGRAPH_DAEMON_RETRY_MAX_MS: '100' }, 80000, preloads);
      clients.push(peer); await peer.initialize();
      expect((await peer.tool('codegraph_explore', { query: 'peerFirst' }, 100)).result.content[0].text).toContain('peerFirst');
      fs.writeFileSync(unknownHello, 'owned fixture: subsequent hello omits effective policy\n');
      fs.writeFileSync(dropRequest, 'owned fixture: drop request 200 before forward\n');
      const refused = await peer.tool('codegraph_explore', { query: 'refusedOrphan' }, 200);
      evidence.refused = refused;
      expect(refused.error).toMatchObject({ code: -32603, data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root } });
      expect(refused.error.data.guidance).toContain(root);
      const explicit = await peer.tool('codegraph_explore', { query: 'refusedExplicit', projectPath: root }, 201);
      expect(explicit.error).toMatchObject({ code: -32603, data: { reason: 'unknown', projectRoot: root } });
      for (const id of [200, 201]) expect(peer.replies.filter(reply => reply.id === id)).toHaveLength(1);
      expect(peer.stderr).not.toContain('serving this session in-process');
      const healthy = await owner.tool('codegraph_explore', { query: 'ownerSurvives' }, 101);
      expect(healthy.error).toBeUndefined(); expect(healthy.result.content[0].text).toContain('ownerSurvives');
      const after = parseQueryDiagnostics((await owner.tool('codegraph_status')).result);
      expect(after.ownership).toEqual(before.ownership); expect(after.effective).toEqual(before.effective);
      expect(['daemon.pid', 'writer.pid'].map(name => fs.readFileSync(path.join(root, '.codegraph', name), 'utf8'))).toEqual(locks);
      const workerEvents = events(observations), networkEvents = events(network);
      expect(networkEvents.filter(event => event.event === 'owned-link-drop')).toEqual([expect.objectContaining({ pid: peer.child.pid, id: 200, forwarded: false })]);
      expect(networkEvents.filter(event => event.event === 'owned-link-closed')).toHaveLength(1);
      expect(networkEvents.filter(event => event.event === 'unknown-hello').length).toBeGreaterThan(0);
      expect(workerEvents.filter(event => event.event === 'call-start').map(event => event.query)).toEqual(['ownerFirst', 'peerFirst', 'ownerSurvives']);
      expect(workerEvents.filter(event => event.event === 'spawn').every(event => event.pid === daemonPid && event.execution.generation === before.ownership.generation)).toBe(true);
      const histories = workerEvents.filter(event => event.event === 'session-history');
      expect(histories.map(event => ({ query: event.query, index: event.index }))).toEqual([
        { query: 'ownerFirst', index: 1 }, { query: 'peerFirst', index: 1 }, { query: 'ownerSurvives', index: 2 },
      ]);
      expect(new Set(histories.map(event => event.historyId)).size).toBe(2);
      expect(histories[0].historyId).toBe(histories[2].historyId);
      Object.assign(evidence, { before, after, locks, explicit, healthy, workerEvents, networkEvents });
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (phase: string, action: () => Promise<void>) => { try { await action(); cleanup.push({ phase, error: null }); } catch (error) { cleanup.push({ phase, error: String(error) }); } };
      for (const client of clients) await attempt('client-' + client.name, () => client.close());
      if (daemonPid) await attempt('shared-daemon-exit', () => until(() => !alive(daemonPid!), 20000));
      await attempt('losing-candidates', () => settleLosingCandidates(root, () => process.pid));
      const workerEvents = events(observations);
      await attempt('all-query-workers-exited', async () => {
        expect(workerEvents.filter(event => event.event === 'exit')).toHaveLength(workerEvents.filter(event => event.event === 'spawn').length);
        expect(workerEvents.filter(event => event.event === 'exit').every(event => event.code === 0 || event.code === 1)).toBe(true);
      });
      saveCompatibilityEvidence('compatibility-retry', { ...evidence, workerEvents, networkEvents: events(network),
        primary: primary instanceof Error ? primary.stack : String(primary ?? ''), cleanup, clients: clients.map(client => client.facts()) });
      const failures = cleanup.filter(item => item.error);
      if (!failures.length) { removeSpawnLog(root); fs.rmSync(fixture, { recursive: true, force: true }); }
      if (failures.length && !primary) throw new Error('Owned retry cleanup incomplete: ' + JSON.stringify(failures));
    }
  }, 120000);
});
