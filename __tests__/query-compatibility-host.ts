import * as fs from 'fs';
import * as path from 'path';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { recordSpawns } from './daemon-candidates';

export const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
export async function until(check: () => boolean, ms: number, detail: () => string = () => ''): Promise<void> {
  const deadline = Date.now() + ms;
  while (!check()) {
    if (Date.now() >= deadline) throw new Error('Owned compatibility fixture deadline: ' + detail());
    await wait(20);
  }
}
export const alive = (pid: number): boolean => {
  try { process.kill(pid, 0); return true; }
  catch (error) { return (error as NodeJS.ErrnoException).code === 'EPERM'; }
};

/** Real CLI/stdio boundary; cumulative bounded capture remains available on failure. */
export class CompatibilityClient {
  readonly child: ChildProcessWithoutNullStreams;
  readonly replies: any[] = [];
  readonly capture = { stdout: { seenBytes: 0, retainedBytes: 0 }, stderr: { seenBytes: 0, retainedBytes: 0 } };
  stdout = '';
  stderr = '';
  private buffer = '';
  private nextId = 0;
  private failure: Error | undefined;
  private pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void; timer: NodeJS.Timeout }>();
  constructor(readonly root: string, readonly name: string, env: NodeJS.ProcessEnv = {}, readonly clientDeadline = 80000,
    preloads: string[] = []) {
    const recorder = recordSpawns(root);
    const childEnv: NodeJS.ProcessEnv = { ...process.env,
      CODEGRAPH_QUERY_MODE: undefined, CODEGRAPH_QUERY_POOL_SIZE: undefined, CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: undefined,
      CODEGRAPH_NO_DAEMON: '0', CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '300', CODEGRAPH_DAEMON_INSTALL_CHECK_MS: '0',
      CODEGRAPH_DAEMON_RETRY_MS: '0', CODEGRAPH_MCP_LOG_ATTACH: '1', CODEGRAPH_NO_WATCHDOG: '1',
      CODEGRAPH_MCP_TOOLS: 'explore,node,search,callers,callees,impact,files,status',
      CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS: '0', CODEGRAPH_NO_RELAUNCH: '1', CODEGRAPH_WASM_RELAUNCHED: '1',
      CODEGRAPH_PPID_POLL_MS: '0', ...recorder.env, ...env };
    this.child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, ...recorder.args, ...preloads.flatMap(file => ['--require', file]),
      path.resolve(__dirname, '../dist/bin/codegraph.js'), 'serve', '--mcp', '--path', root],
    { cwd: root, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'], env: childEnv });
    this.child.stdout.on('data', data => {
      if (!this.retain('stdout', data, 10 * 1024 * 1024)) return;
      this.buffer += data.toString();
      let end: number;
      while ((end = this.buffer.indexOf('\n')) >= 0) {
        const line = this.buffer.slice(0, end); this.buffer = this.buffer.slice(end + 1);
        let reply: any; try { reply = JSON.parse(line); } catch { continue; }
        this.replies.push(reply);
        const entry = this.pending.get(reply.id);
        if (entry) { clearTimeout(entry.timer); this.pending.delete(reply.id); entry.resolve(reply); }
      }
    });
    this.child.stderr.on('data', data => this.retain('stderr', data, 1024 * 1024));
    this.child.on('error', error => this.fail(error));
    this.child.stdin.on('error', error => this.fail(error));
    this.child.on('exit', (code, signal) => this.fail(new Error(`Owned MCP client exited ${code}/${signal}`)));
  }
  private retain(stream: 'stdout' | 'stderr', data: Buffer, maximum: number): boolean {
    const capture = this.capture[stream]; capture.seenBytes += data.length;
    const kept = data.subarray(0, Math.max(0, maximum - capture.retainedBytes));
    this[stream] += kept.toString(); capture.retainedBytes += kept.length;
    if (capture.seenBytes > maximum) { this.fail(new Error('Cumulative MCP ' + stream + ' overflow')); return false; }
    return true;
  }
  private fail(error: Error): void {
    this.failure ??= error;
    for (const entry of this.pending.values()) { clearTimeout(entry.timer); entry.reject(this.failure); }
    this.pending.clear();
  }
  request(method: string, params: unknown = {}, explicitId?: number): Promise<any> {
    if (this.failure) return Promise.reject(this.failure);
    const id = explicitId ?? ++this.nextId;
    if (this.pending.has(id)) throw new Error('Fixture request id already pending');
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`Independent client deadline ${this.clientDeadline}ms (${method}): ${this.stderr}`)); }, this.clientDeadline);
      this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
    });
  }
  async initialize(): Promise<void> {
    const reply = await this.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: this.name, version: '1' } });
    if (reply.error || !reply.result?.serverInfo) throw new Error('Actual CLI initialize did not succeed');
    this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
  }
  tool(name: string, args: Record<string, unknown> = {}, id?: number): Promise<any> {
    return this.request('tools/call', { name, arguments: args }, id);
  }
  facts() { return { name: this.name, root: this.root, pid: this.child.pid, exitCode: this.child.exitCode,
    signal: this.child.signalCode, capture: this.capture, stdout: this.stdout, stderr: this.stderr, replies: this.replies }; }
  async close(): Promise<void> {
    if (this.child.exitCode === null && this.child.signalCode === null) {
      const closed = new Promise<void>(resolve => this.child.once('close', () => resolve()));
      this.child.stdin.end();
      let timer: NodeJS.Timeout | undefined;
      try { await Promise.race([closed, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error('Owned client graceful-close deadline')), 35000); })]); }
      finally { if (timer) clearTimeout(timer); }
    }
    if (this.child.exitCode !== 0 || this.child.signalCode !== null) throw new Error(`Owned client cleanup exit ${this.child.exitCode}/${this.child.signalCode}: ${this.stderr}`);
  }
}

export function saveCompatibilityEvidence(name: string, value: unknown): void {
  if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, name + '.json'), JSON.stringify(value, null, 2) + '\n');
}
