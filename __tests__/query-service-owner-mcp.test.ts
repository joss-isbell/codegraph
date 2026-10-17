import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { CodeGraph } from '../src';
import { getDaemonSocketCandidates } from '../src/mcp/daemon-paths';
import { removeSpawnLog, settleLosingCandidates, spawnLogFor } from './daemon-candidates';
import { CompatibilityClient, alive, until, saveCompatibilityEvidence } from './query-compatibility-host';

describe('ordinary acquisition preserves existing daemon owners', () => {
  it('refuses an older live project-lock owner when every usual socket probe fails, without signaling or replacement', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-owner-')));
    const root = path.join(fixture, 'project'); fs.mkdirSync(root);
    const socketPath = process.platform === 'win32' ? `\\\\.\\pipe\\${path.basename(fixture)}-legacy` : path.join(fixture, 'legacy.sock');
    const clients: CompatibilityClient[] = [];
    let owner: ChildProcessWithoutNullStreams | undefined;
    let ownerClosed = false;
    const prefixes = { stdout: Buffer.alloc(65536), stderr: Buffer.alloc(65536) };
    const capture = { stdout: { seenBytes: 0, retainedBytes: 0, truncated: false },
      stderr: { seenBytes: 0, retainedBytes: 0, truncated: false }, firstError: null as string | null };
    const failCapture = (detail: string): void => { capture.firstError ??= detail.slice(0, 2048); };
    const retain = (stream: 'stdout' | 'stderr', data: Buffer): void => {
      const state = capture[stream]; state.seenBytes += data.length;
      const count = Math.min(data.length, prefixes[stream].length - state.retainedBytes);
      data.copy(prefixes[stream], state.retainedBytes, 0, count); state.retainedBytes += count;
      if (count < data.length) { state.truncated = true; failCapture('Owned legacy ' + stream + ' capture overflow'); }
      // Listeners remain attached to drain every byte after the retained cap.
    };
    const capturedText = (stream: 'stdout' | 'stderr'): string => prefixes[stream].subarray(0, capture[stream].retainedBytes).toString('utf8');
    let nativeClose: { exitCode: number | null; signal: NodeJS.Signals | null } | null = null;
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    const evidence: Record<string, unknown> = { root, socketPath };
    let primary: unknown;
    try {
      fs.writeFileSync(path.join(root, 'app.ts'), 'export function protectedOwnerSymbol() { return 71; }\n');
      const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      expect(getDaemonSocketCandidates(root)).not.toContain(socketPath);
      owner = spawn(process.execPath, [path.join(__dirname, 'fixtures/query-owned-legacy-daemon.cjs'), root, '0.0.1', socketPath],
        { cwd: root, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env } });
      owner.on('error', error => failCapture('Owned legacy spawn error: ' + String(error)));
      owner.stdin.on('error', error => failCapture('Owned legacy stdin error: ' + String(error)));
      owner.stdout.on('error', error => failCapture('Owned legacy stdout error: ' + String(error)));
      owner.stderr.on('error', error => failCapture('Owned legacy stderr error: ' + String(error)));
      owner.on('close', (exitCode, signal) => { ownerClosed = true; nativeClose = { exitCode, signal }; });
      owner.stdout.on('data', data => retain('stdout', data));
      owner.stderr.on('data', data => retain('stderr', data));
      await until(() => { if (capture.firstError) throw new Error(capture.firstError); return capturedText('stdout').includes('"event":"ready"'); }, 10000, () => capturedText('stderr'));
      const locks = ['daemon.pid', 'writer.pid'].map(name => fs.readFileSync(path.join(root, '.codegraph', name), 'utf8'));
      evidence.locks = locks; evidence.ownerPid = owner.pid;
      const peer = new CompatibilityClient(root, 'new-client', { CODEGRAPH_DAEMON_RETRY_MS: '100', CODEGRAPH_DAEMON_RETRY_MAX_MS: '100' });
      clients.push(peer); await peer.initialize();
      const refused = await peer.tool('codegraph_search', { query: 'protectedOwnerSymbol' }, 100);
      evidence.refused = refused;
      expect(refused.error).toMatchObject({ code: -32603, data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root } });
      expect(refused.error.data.guidance).toContain(root);
      const next = await peer.tool('codegraph_explore', { query: 'protectedOwnerSymbol', projectPath: root }, 101);
      expect(next.error).toMatchObject({ code: -32603, data: { reason: 'unknown' } });
      expect(alive(owner.pid!)).toBe(true); expect(owner.exitCode).toBeNull(); expect(owner.signalCode).toBeNull();
      expect(['daemon.pid', 'writer.pid'].map(name => fs.readFileSync(path.join(root, '.codegraph', name), 'utf8'))).toEqual(locks);
      expect(peer.stderr).not.toContain('[CodeGraph MCP] Stopped');
      expect(peer.stderr).not.toContain('serving this session in-process');
      expect(fs.existsSync(spawnLogFor(root)) ? fs.readFileSync(spawnLogFor(root), 'utf8').trim() : '').toBe('');
      for (const id of [100, 101]) expect(peer.replies.filter(reply => reply.id === id)).toHaveLength(1);
      expect(capture.firstError).toBeNull();
      expect(capture.stdout.truncated || capture.stderr.truncated).toBe(false);
      evidence.next = next;
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (phase: string, action: () => Promise<void>) => { try { await action(); cleanup.push({ phase, error: null }); } catch (error) { cleanup.push({ phase, error: String(error) }); } };
      for (const client of clients) await attempt('client-' + client.name, () => client.close());
      await attempt('owned-legacy-native-close', async () => {
        if (owner && owner.exitCode === null && owner.signalCode === null) owner.stdin.end();
        if (owner) await until(() => ownerClosed, 10000);
        // An illegal product signal is retained as the primary red's native
        // evidence. A passing case must have remained alive until owned EOF.
        if (!primary && owner) { expect(nativeClose).toEqual({ exitCode: 0, signal: null }); expect(capturedText('stdout')).toContain('"event":"owned-close"'); }
        expect(capture.firstError).toBeNull();
        expect(capture.stdout.truncated || capture.stderr.truncated).toBe(false);
      });
      await attempt('all-candidates-exited', () => settleLosingCandidates(root, () => process.pid));
      saveCompatibilityEvidence('compatibility-owner', { ...evidence, primary: primary instanceof Error ? primary.stack : String(primary ?? ''),
        owner: owner ? { pid: owner.pid, exitCode: owner.exitCode, signal: owner.signalCode, closed: ownerClosed, nativeClose,
          stdout: capturedText('stdout'), stderr: capturedText('stderr'),
          prefixBase64: { stdout: prefixes.stdout.subarray(0, capture.stdout.retainedBytes).toString('base64'),
            stderr: prefixes.stderr.subarray(0, capture.stderr.retainedBytes).toString('base64') }, capture } : null,
        cleanup, clients: clients.map(client => client.facts()) });
      const failures = cleanup.filter(item => item.error);
      if (!failures.length) { removeSpawnLog(root); fs.rmSync(fixture, { recursive: true, force: true }); }
      if (failures.length && !primary) throw new Error('Owned prior-service cleanup incomplete: ' + JSON.stringify(failures));
    }
  }, 120000);
});
