import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { createHash } from 'crypto';
import type { Socket } from 'net';
import { CodeGraph } from '../src';
import { connectForProjectFreshness } from '../src/mcp/proxy';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { settleLosingCandidates, removeSpawnLog, spawnLogFor } from './daemon-candidates';
import { CompatibilityClient, alive, until, saveCompatibilityEvidence } from './query-compatibility-host';

describe('shared query-service compatibility through actual MCP', () => {
  it('refuses copied live foreign owners for default, explicit and policy-neutral freshness routes without changing either project', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-project-')));
    const alpha = path.join(fixture, 'alpha'), beta = path.join(fixture, 'beta');
    const clients: CompatibilityClient[] = [], freshnessSockets: Socket[] = [];
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    const evidence: Record<string, any> = { fixture, alpha, beta };
    const env = { CODEGRAPH_QUERY_POOL_SIZE: '1', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited' };
    let daemonPid: number | undefined, primary: unknown, completed = false;
    const snapshot = (root: string) => Object.fromEntries(['app.ts', '.codegraph/codegraph.db', '.codegraph/codegraph.db-wal',
      '.codegraph/codegraph.db-shm', '.codegraph/daemon.pid', '.codegraph/writer.pid'].map(name => {
      const file = path.join(root, name);
      if (!fs.existsSync(file)) return [name, null];
      const stat = fs.statSync(file, { bigint: true });
      return [name, { size: stat.size.toString(), birthtimeNs: stat.birthtimeNs.toString(), ctimeNs: stat.ctimeNs.toString(),
        mtimeNs: stat.mtimeNs.toString(), sha256: createHash('sha256').update(fs.readFileSync(file)).digest('hex') }];
    }));
    const isUnknown = (reply: any, id: number) => {
      expect(reply).toMatchObject({ jsonrpc: '2.0', id, error: { code: -32603,
        data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: beta } } });
      expect(reply).not.toHaveProperty('result'); expect(reply).not.toHaveProperty('method');
      expect(reply.error.data.guidance).toContain(beta);
    };
    try {
      for (const [root, symbol] of [[alpha, 'copiedOwnerAlphaOnly'], [beta, 'copiedOwnerBetaOnly']]) {
        fs.mkdirSync(root); fs.writeFileSync(path.join(root, 'app.ts'), `export function ${symbol}() { return 731; }\n`);
        const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      }
      const owner = new CompatibilityClient(alpha, 'correct-project-owner', env); clients.push(owner); await owner.initialize();
      const first = await owner.tool('codegraph_search', { query: 'copiedOwnerAlphaOnly' }, 200);
      expect(first.error).toBeUndefined(); expect(first.result.content[0].text).toContain('copiedOwnerAlphaOnly');
      const before = parseQueryDiagnostics((await owner.tool('codegraph_status', {}, 201)).result); daemonPid = before.ownership.pid;
      expect(before.effective).toMatchObject({ executionMode: 'shared-daemon', concurrency: { kind: 'finite', maximum: 1 } });
      expect(before.lifecycle.workers).toBe(1);
      for (const name of ['daemon.pid', 'writer.pid']) fs.copyFileSync(path.join(alpha, '.codegraph', name), path.join(beta, '.codegraph', name));
      const socketPath = JSON.parse(fs.readFileSync(path.join(alpha, '.codegraph/daemon.pid'), 'utf8')).socketPath;
      evidence.before = before; evidence.beforeState = { alpha: snapshot(alpha), beta: snapshot(beta) };
      const peer = new CompatibilityClient(beta, 'copied-project-requester', env); clients.push(peer); await peer.initialize();
      // Capture both public routes before asserting: a pre-fix run retains the
      // actual foreign answer rather than stopping before the explicit route.
      const defaultReply = await peer.tool('codegraph_search', { query: 'copiedOwnerAlphaOnly' }, 100);
      const explicitReply = await peer.tool('codegraph_search', { query: 'copiedOwnerAlphaOnly', projectPath: beta }, 101);
      let freshnessError: unknown;
      try {
        const socket = await connectForProjectFreshness(socketPath, beta);
        if (socket) { freshnessSockets.push(socket); socket.destroy(); }
        evidence.freshnessReturnedSocket = !!socket;
      } catch (error) { freshnessError = error; }
      evidence.defaultReply = defaultReply; evidence.explicitReply = explicitReply;
      evidence.freshnessError = freshnessError instanceof Error ? { message: freshnessError.message, ...freshnessError } : freshnessError;
      evidence.afterState = { alpha: snapshot(alpha), beta: snapshot(beta) };
      const after = parseQueryDiagnostics((await owner.tool('codegraph_status', {}, 202)).result); evidence.after = after;
      const healthy = await owner.tool('codegraph_search', { query: 'copiedOwnerAlphaOnly' }, 203); evidence.healthy = healthy;
      isUnknown(defaultReply, 100); isUnknown(explicitReply, 101);
      expect(freshnessError).toMatchObject({ code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: beta });
      expect(evidence.freshnessReturnedSocket).toBeUndefined();
      expect(evidence.afterState).toEqual(evidence.beforeState);
      expect(after.ownership).toEqual(before.ownership); expect(after.effective).toEqual(before.effective);
      expect(after.lifecycle.workers).toBe(1); expect(alive(daemonPid)).toBe(true);
      expect(healthy.error).toBeUndefined(); expect(healthy.result.content[0].text).toContain('copiedOwnerAlphaOnly');
      expect(peer.stderr).not.toContain('serving this session in-process'); expect(peer.stderr).not.toContain('[CodeGraph MCP] Stopped');
      expect(fs.existsSync(spawnLogFor(beta)) ? fs.readFileSync(spawnLogFor(beta), 'utf8').trim() : '').toBe('');
      completed = true;
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (phase: string, action: () => Promise<void>) => {
        try { await action(); cleanup.push({ phase, error: null }); } catch (error) { cleanup.push({ phase, error: String(error) }); }
      };
      await attempt('freshness-native-close', async () => {
        for (const socket of freshnessSockets) { socket.destroy(); await until(() => socket.closed, 10000); }
      });
      // Refused host closes first: a passing route never owns alpha's service.
      for (const client of [...clients].reverse()) await attempt('client-' + client.name, () => client.close());
      if (daemonPid) await attempt('shared-daemon-exit', () => until(() => !alive(daemonPid!), 20000));
      for (const root of [alpha, beta]) await attempt('all-candidates-' + path.basename(root), () => settleLosingCandidates(root, () => process.pid));
      if (completed && cleanup.every(value => value.error === null)) await attempt('final-refused-replies', async () => {
        const peer = clients[1];
        for (const id of [100, 101]) {
          const final = peer.replies.filter(reply => reply.id === id);
          expect(final).toHaveLength(1); isUnknown(final[0], id);
        }
        const owner = clients[0];
        for (const id of [200, 201, 202, 203]) {
          const final = owner.replies.filter(reply => reply.id === id);
          expect(final).toHaveLength(1); expect(final[0]).not.toHaveProperty('method'); expect(final[0]).not.toHaveProperty('error');
          if (id === 201 || id === 202) expect(parseQueryDiagnostics(final[0].result).ownership.pid).toBe(daemonPid);
          else expect(final[0].result.content[0].text).toContain('copiedOwnerAlphaOnly');
        }
      });
      saveCompatibilityEvidence('compatibility-project-identity', { ...evidence,
        primary: primary instanceof Error ? primary.stack : String(primary ?? ''), cleanup, clients: clients.map(client => client.facts()) });
      const failures = cleanup.filter(item => item.error);
      if (!primary && !failures.length) { for (const root of [alpha, beta]) removeSpawnLog(root); fs.rmSync(fixture, { recursive: true, force: true }); }
      if (failures.length && !primary) throw new Error('Owned project-identity cleanup incomplete: ' + JSON.stringify(failures));
    }
  }, 90000);

  it('refuses a same-version conflicting client without changing the healthy shared service or creating a local pool', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-reuse-')));
    const clients: CompatibilityClient[] = [];
    let daemonPid: number | undefined;
    let primary: unknown;
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    const evidence: Record<string, unknown> = { root };
    try {
      fs.writeFileSync(path.join(root, 'app.ts'), 'export function compatibilitySymbol() { return 701; }\n');
      const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      const owner = new CompatibilityClient(root, 'admitted', { CODEGRAPH_QUERY_POOL_SIZE: '1', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited' });
      clients.push(owner); await owner.initialize();
      const initial = await owner.tool('codegraph_search', { query: 'compatibilitySymbol' });
      expect(initial.error).toBeUndefined(); expect(initial.result.content[0].text).toContain('compatibilitySymbol');
      const before = parseQueryDiagnostics((await owner.tool('codegraph_status')).result);
      expect(before.effective).toMatchObject({ executionMode: 'shared-daemon', concurrency: { kind: 'finite', maximum: 1 }, deadline: { kind: 'absent' } });
      daemonPid = before.ownership.pid;
      const daemonLock = fs.readFileSync(path.join(root, '.codegraph/daemon.pid'), 'utf8');
      const writerLock = fs.readFileSync(path.join(root, '.codegraph/writer.pid'), 'utf8');
      const conflict = new CompatibilityClient(root, 'conflicting', { CODEGRAPH_QUERY_POOL_SIZE: '2', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited' });
      clients.push(conflict); await conflict.initialize();
      const refused = await conflict.tool('codegraph_search', { query: 'compatibilitySymbol' });
      evidence.before = before; evidence.refused = refused;
      expect(refused.error).toMatchObject({ code: -32603, data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'incompatible', projectRoot: root } });
      expect(refused.error.data.guidance).toContain(root);
      const second = await conflict.tool('codegraph_search', { query: 'compatibilitySymbol' });
      expect(second.error).toMatchObject({ code: -32603, data: { reason: 'incompatible' } });
      expect(conflict.stderr).not.toContain('serving this session in-process');
      const after = parseQueryDiagnostics((await owner.tool('codegraph_status')).result);
      evidence.after = after;
      expect(after.ownership).toEqual(before.ownership);
      expect(after.effective).toEqual(before.effective);
      expect(fs.readFileSync(path.join(root, '.codegraph/daemon.pid'), 'utf8')).toBe(daemonLock);
      expect(fs.readFileSync(path.join(root, '.codegraph/writer.pid'), 'utf8')).toBe(writerLock);
      const healthy = await owner.tool('codegraph_search', { query: 'compatibilitySymbol' });
      expect(healthy.error).toBeUndefined(); expect(healthy.result.content[0].text).toContain('compatibilitySymbol');
      evidence.healthy = healthy;
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (phase: string, action: () => Promise<void>) => { try { await action(); cleanup.push({ phase, error: null }); } catch (error) { cleanup.push({ phase, error: String(error) }); } };
      for (const client of clients) await attempt('client-' + client.name, () => client.close());
      if (daemonPid) await attempt('shared-daemon-exit', () => until(() => !alive(daemonPid!), 20000));
      await attempt('losing-candidates', () => settleLosingCandidates(root, () => process.pid));
      saveCompatibilityEvidence('compatibility-initial', { ...evidence, primary: primary instanceof Error ? primary.stack : String(primary ?? ''), cleanup, clients: clients.map(client => client.facts()) });
      const failures = cleanup.filter(item => item.error);
      if (!failures.length) { removeSpawnLog(root); fs.rmSync(root, { recursive: true, force: true }); }
      if (failures.length && !primary) throw new Error('Owned compatibility cleanup incomplete: ' + JSON.stringify(failures));
    }
  }, 90000);
});
