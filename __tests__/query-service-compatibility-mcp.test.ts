import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CodeGraph } from '../src';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { settleLosingCandidates, removeSpawnLog } from './daemon-candidates';
import { CompatibilityClient, alive, until, saveCompatibilityEvidence } from './query-compatibility-host';

describe('shared query-service compatibility through actual MCP', () => {
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
