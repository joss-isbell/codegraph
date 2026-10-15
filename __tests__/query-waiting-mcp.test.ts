import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as net from 'net';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { pathToFileURL } from 'url';
import { CodeGraph } from '../src';
import { getDaemonSocketPath } from '../src/mcp/daemon-paths';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean, milliseconds: number): Promise<void> {
  const limit = Date.now() + milliseconds;
  while (!check()) { if (Date.now() > limit) throw new Error('owned process/event did not settle'); await wait(20); }
}
function alive(pid: number): boolean { try { process.kill(pid, 0); return true; } catch { return false; } }
class Client {
  child: ChildProcessWithoutNullStreams;
  stderr = '';
  replies: any[] = [];
  private pending = new Map<number, { resolve: (result: any) => void; reject: (error: Error) => void; timer: NodeJS.Timeout }>();
  private nextId = 1;
  constructor(root: string, mode: string, observations: string) {
    this.child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, '--require', path.join(__dirname, 'fixtures/query-waiting-preload.cjs'),
      path.resolve(__dirname, '../dist/bin/codegraph.js'), 'serve', '--mcp', '--path', root, '--no-watch'], {
      cwd: root, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env,
        CODEGRAPH_NO_DAEMON: mode === 'direct' ? '1' : '0', CODEGRAPH_QUERY_MODE: 'default',
        // Leave capacity unset: this independent control must preserve each mode's default.
        CODEGRAPH_QUERY_POOL_SIZE: '', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
        CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '1000', CODEGRAPH_DAEMON_INSTALL_CHECK_MS: '0',
        CODEGRAPH_PROXY_RETRY_MS: '0', CODEGRAPH_MCP_LOG_ATTACH: '1',
        QUERY_WAITING_OBSERVATIONS: observations,
      },
    });
    let buffer = '';
    this.child.stdout.on('data', data => {
      buffer += String(data);
      if (buffer.length > 1024 * 1024) return this.fail(new Error('client stdout overflow'));
      let end: number;
      while ((end = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
        let reply: any; try { reply = JSON.parse(line); } catch { continue; }
        this.replies.push(reply);
        const pending = this.pending.get(reply.id);
        if (pending) { this.pending.delete(reply.id); clearTimeout(pending.timer); reply.error ? pending.reject(new Error(JSON.stringify(reply.error))) : pending.resolve(reply.result); }
      }
    });
    this.child.stderr.on('data', data => { this.stderr += String(data); if (this.stderr.length > 1024 * 1024) this.fail(new Error('client stderr overflow')); });
    this.child.on('error', error => this.fail(error));
    this.child.stdin.on('error', error => this.fail(error));
    this.child.on('exit', () => this.fail(new Error('client exited')));
  }
  fail(error: Error): void { for (const request of this.pending.values()) { clearTimeout(request.timer); request.reject(error); } this.pending.clear(); }
  request(method: string, params: unknown = {}): Promise<any> {
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error('independent calling client deadline: 80000 ms')); }, 80000);
      this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
    });
  }
  async close(): Promise<void> {
    this.child.stdin.end();
    await until(() => this.child.exitCode !== null || this.child.signalCode !== null, 10000);
    expect(this.child.exitCode).toBe(0);
  }
}

describe('independent unlimited waiting through actual MCP requests', () => {
  it.each(['direct', 'writable-fallback', 'shared-daemon'] as const)
  ('returns the real 47-second worker result and ordinary project routes in %s', async mode => {
    // Per experiment: release at 47s, 90s complete case bound, separate client
    // deadline 80s. This proves CodeGraph timing, not every host's waiting limit.
    const started = Date.now();
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-waiting-mcp-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    const observations = path.join(fixture, 'workers.jsonl');
    let server: net.Server | undefined;
    let client: Client | undefined;
    let daemonPid: number | undefined;
    const facts = () => fs.existsSync(observations) ? fs.readFileSync(observations, 'utf8').trim().split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)) : [];
    try {
      for (const [index, root] of roots.entries()) {
        fs.mkdirSync(root);
        fs.writeFileSync(path.join(root, 'app.ts'), `export function ${index ? 'betaTimingSymbol' : 'alphaTimingSymbol'}() { return 42; }\nexport function delayedTimingSymbol() { return 47; }\n`);
        const graph = await CodeGraph.init(root);
        try { await graph.indexAll(); } finally { graph.close(); }
      }
      if (mode === 'writable-fallback') {
        // Existing fallback seam: an owned incompatible handshake with no
        // writer. #16 replaces compatibility behavior and adapts this fixture.
        const socketPath = getDaemonSocketPath(roots[0]);
        server = net.createServer(socket => socket.end(JSON.stringify({ codegraph: '0.0.0-owned-test', pid: process.pid, socketPath, protocol: 1 }) + '\n'));
        await new Promise<void>(resolve => server!.listen(socketPath, resolve));
      }
      client = new Client(roots[0], mode, observations);
      const initialize = await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {},
        clientInfo: { name: 'waiting-proof', version: '1' }, rootUri: pathToFileURL(roots[0]).href });
      expect(initialize.serverInfo.name).toBe('codegraph');
      const query = (symbol: string, projectPath?: string) => client!.request('tools/call', { name: 'codegraph_search', arguments: { query: symbol, ...(projectPath ? { projectPath } : {}) } });
      for (const [symbol, projectPath] of [['alphaTimingSymbol', undefined], ['betaTimingSymbol', roots[1]], ['alphaTimingSymbol', undefined]]) {
        const result = await query(symbol!, projectPath);
        expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain(`${symbol}`); expect(result.content[0].text).toContain('app.ts:1');
      }
      const admitted = Date.now();
      const result = await query('delayedTimingSymbol');
      const elapsed = Date.now() - admitted;
      expect(elapsed).toBeGreaterThanOrEqual(47000);
      expect(elapsed).toBeLessThan(80000);
      expect(result.isError).toBeFalsy();
      expect(result.content[0].text).toContain('delayedTimingSymbol');
      expect(result.content[0].text).toContain('app.ts:2');
      expect(result.content[0].text).not.toContain('after admission');
      if (mode === 'shared-daemon') daemonPid = JSON.parse(fs.readFileSync(path.join(roots[0], '.codegraph/daemon.pid'), 'utf8')).pid;
      if (mode === 'writable-fallback') expect(client.stderr).toContain('serving this session in-process');
      await client.close();
      if (daemonPid) await until(() => !alive(daemonPid!), 10000);
      const events = facts();
      const spawned = events.filter(event => event.event === 'spawn');
      expect(spawned.length).toBeGreaterThan(0);
      for (const event of spawned) expect(event.execution.policy).toMatchObject({ executionMode: mode, preset: 'default',
        concurrency: { kind: 'finite', maximum: Math.max(1, Math.min(os.cpus().length - 1, mode === 'shared-daemon' ? 16 : 2)) },
        deadline: { kind: 'absent' }, retention: 'ordinary' });
      expect(events.filter(event => event.event === 'exit')).toHaveLength(spawned.length);
      const controlled = events.filter(event => event.event.startsWith('controlled-'));
      expect(controlled.map(event => event.event)).toEqual(['controlled-start', 'controlled-release']);
      expect(controlled[1].at - controlled[0].at).toBeGreaterThanOrEqual(47000);
      expect(events.some(event => event.event === 'result' && event.threadId === controlled[0].threadId && event.id === controlled[0].id && event.at >= controlled[1].at)).toBe(true);
      expect(client.replies.filter(reply => reply.id === controlled[0].context.requestId)).toHaveLength(1);
      expect(Date.now() - started).toBeLessThan(90000);
      if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, `waiting-${mode}.json`), JSON.stringify({
        mode, releaseMs: 47000, caseBoundMs: 90000, clientDeadlineMs: 80000, elapsed, roots,
        inputs: roots.map(root => ({ root, source: fs.readFileSync(path.join(root, 'app.ts'), 'utf8') })), events, replies: client.replies,
      }, null, 2));
    } finally {
      if (client && client.child.exitCode === null && client.child.signalCode === null) await client.close();
      if (daemonPid && alive(daemonPid)) await until(() => !alive(daemonPid!), 10000);
      if (server) await new Promise<void>(resolve => server!.close(() => resolve()));
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  }, 90000);
});
