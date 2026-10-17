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
  readonly capture = { stdout: { seenBytes: 0, retainedBytes: 0, eof: false }, stderr: { seenBytes: 0, retainedBytes: 0, eof: false } };
  stdout = '';
  stderr = '';
  private buffer = '';
  private nextId = 0;
  private failure: Error | undefined;
  private readonly decoders = { stdout: new TextDecoder('utf-8', { fatal: true }), stderr: new TextDecoder('utf-8', { fatal: true }) };
  private readonly issued = new Map<number, number>();
  private readonly nativeClose: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  private nativeExit: { code: number | null; signal: NodeJS.Signals | null } | undefined;
  private closing = false;
  private closePromise: Promise<void> | undefined;
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
    // Register immediately, not when teardown starts: exit can precede stdio
    // EOF and a caller may close a child which has already natively closed.
    this.nativeClose = new Promise(resolve => this.child.once('close', (code, signal) => {
      this.nativeExit = { code, signal };
      if (!this.capture.stdout.eof || !this.capture.stderr.eof) this.fail(new Error('Owned MCP close before stdout/stderr EOF'));
      if (this.pending.size) this.fail(new Error('Owned MCP close with unsettled requests'));
      if (code !== 0 || signal !== null) this.fail(new Error(`Owned MCP client exited ${code}/${signal}`));
      resolve(this.nativeExit);
    }));
    this.child.stdout.on('data', data => {
      const text = this.retain('stdout', data, 10 * 1024 * 1024);
      if (text === undefined || this.failure) return;
      this.buffer += text;
      let end: number;
      while ((end = this.buffer.indexOf('\n')) >= 0) {
        const line = this.buffer.slice(0, end); this.buffer = this.buffer.slice(end + 1);
        let reply: any;
        try { reply = JSON.parse(line); }
        catch (error) { this.fail(new Error('Malformed owned MCP stdout: ' + String(error).slice(0, 1024))); return; }
        if (!reply || typeof reply !== 'object' || Array.isArray(reply) || reply.jsonrpc !== '2.0'
          || ('method' in reply ? typeof reply.method !== 'string' || 'result' in reply || 'error' in reply
            : !('id' in reply) || ('result' in reply) === ('error' in reply))) {
          this.fail(new Error('Malformed owned MCP stdout JSON-RPC object')); return;
        }
        // Legal server requests/notifications remain in raw stdout, not the
        // client response ledger: the two directions may legally share an ID.
        if ('method' in reply) continue;
        this.replies.push(reply);
        if (!this.issued.has(reply.id)) {
          this.fail(new Error('Owned MCP response has no issued request ID')); return;
        }
        const entry = this.pending.get(reply.id);
        if (entry) { clearTimeout(entry.timer); this.pending.delete(reply.id); entry.resolve(reply); }
      }
    });
    this.child.stderr.on('data', data => this.retain('stderr', data, 1024 * 1024));
    for (const stream of ['stdout', 'stderr'] as const) {
      this.child[stream].on('error', error => this.fail(error));
      this.child[stream].once('end', () => {
        this.capture[stream].eof = true;
        const tail = this.decode(stream);
        if (stream === 'stdout') {
          this.buffer += tail ?? '';
          if (this.buffer.length) this.fail(new Error('Unterminated owned MCP stdout at EOF'));
        }
      });
    }
    this.child.on('error', error => this.fail(error));
    this.child.stdin.on('error', error => this.fail(error));
  }
  private decode(stream: 'stdout' | 'stderr', data?: Buffer): string | undefined {
    try {
      const text = this.decoders[stream].decode(data, { stream: data !== undefined });
      this[stream] += text; return text;
    } catch (error) { this.fail(new Error(`Invalid owned MCP ${stream} UTF-8: ${String(error).slice(0, 1024)}`)); return undefined; }
  }
  private retain(stream: 'stdout' | 'stderr', data: Buffer, maximum: number): string | undefined {
    const capture = this.capture[stream]; capture.seenBytes += data.length;
    const kept = data.subarray(0, Math.max(0, maximum - capture.retainedBytes));
    capture.retainedBytes += kept.length;
    const text = this.decode(stream, kept);
    if (capture.seenBytes > maximum) { this.fail(new Error('Cumulative MCP ' + stream + ' overflow')); return undefined; }
    return text;
  }
  private fail(error: Error): void {
    this.failure ??= error;
    for (const entry of this.pending.values()) { clearTimeout(entry.timer); entry.reject(this.failure); }
    this.pending.clear();
  }
  request(method: string, params: unknown = {}, explicitId?: number): Promise<any> {
    if (this.failure) return Promise.reject(this.failure);
    if (this.closing || this.nativeExit || this.child.exitCode !== null || this.child.signalCode !== null)
      return Promise.reject(new Error('Owned MCP client is closing or already exited'));
    let id = explicitId ?? ++this.nextId;
    if (explicitId === undefined) while (this.issued.has(id)) id = ++this.nextId;
    if (this.pending.has(id)) throw new Error('Fixture request id already pending');
    this.issued.set(id, (this.issued.get(id) ?? 0) + 1);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => this.fail(new Error(`Independent client deadline ${this.clientDeadline}ms (${method}): ${this.stderr}`)), this.clientDeadline);
      this.pending.set(id, { resolve, reject, timer });
      try { this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n'); }
      catch (error) { this.fail(error instanceof Error ? error : new Error(String(error))); }
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
    signal: this.child.signalCode, nativeClose: this.nativeExit ?? null, firstFailure: this.failure?.message ?? null,
    issued: [...this.issued].map(([id, count]) => ({ id, count })), pending: [...this.pending.keys()],
    unterminatedStdoutChars: this.buffer.length, capture: this.capture, stdout: this.stdout, stderr: this.stderr, replies: this.replies }; }
  close(): Promise<void> {
    return this.closePromise ??= (async () => {
      this.closing = true;
      try { if (!this.child.stdin.destroyed && !this.child.stdin.writableEnded) this.child.stdin.end(); }
      catch (error) { this.fail(error instanceof Error ? error : new Error(String(error))); }
      let timer: NodeJS.Timeout | undefined;
      try { await Promise.race([this.nativeClose, new Promise<never>((_, reject) => {
        timer = setTimeout(() => { this.fail(new Error('Owned client graceful-close deadline')); reject(this.failure); }, 35000);
      })]); }
      finally { if (timer) clearTimeout(timer); }
      if (this.failure) throw this.failure;
      const closed = this.nativeExit;
      if (!closed || closed.code !== 0 || closed.signal !== null || !this.capture.stdout.eof || !this.capture.stderr.eof || this.buffer.length)
        throw new Error('Owned client cleanup lacks successful native close and complete stdout/stderr EOF');
      for (const [id, count] of this.issued) {
        const received = this.replies.filter(reply => reply.id === id && !('method' in reply)).length;
        if (received !== count) {
          this.fail(new Error(`Owned MCP request ${id} received ${received} replies after EOF; expected ${count}`));
          throw this.failure;
        }
      }
    })();
  }
}

export function saveCompatibilityEvidence(name: string, value: unknown): void {
  if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, name + '.json'), JSON.stringify(value, null, 2) + '\n');
}
