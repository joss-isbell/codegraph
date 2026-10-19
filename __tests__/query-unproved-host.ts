import { expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { recordSpawns } from './daemon-candidates';
import { alive, until } from './query-compatibility-host';

/** Fixed byte prefixes, sticky failures, continued drain and native close facts
 * survive every product assertion path. Normal owned EOF is not an error. */
export class UnprovedClient {
  readonly child: ChildProcessWithoutNullStreams;
  readonly values: any[] = [];
  readonly prefixes = { stdout: Buffer.alloc(8 * 1024 * 1024), stderr: Buffer.alloc(1024 * 1024) };
  readonly capture = { stdout: { seenBytes: 0, retainedBytes: 0, truncated: false }, stderr: { seenBytes: 0, retainedBytes: 0, truncated: false } };
  firstError: string | null = null;
  nativeClose: { exitCode: number | null; signal: NodeJS.Signals | null } | null = null;
  private parsed = 0;
  private readonly pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void; timer: NodeJS.Timeout }>();
  constructor(readonly name: string, root: string, args: string[], env: NodeJS.ProcessEnv) {
    this.child = spawn(process.execPath, args, { cwd: root, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'], env });
    const fail = (error: unknown) => {
      this.firstError ??= String(error).slice(0, 2048);
      for (const entry of this.pending.values()) { clearTimeout(entry.timer); entry.reject(new Error(this.firstError)); }
      this.pending.clear();
    };
    this.child.on('error', fail); this.child.stdin.on('error', fail);
    for (const stream of ['stdout', 'stderr'] as const) {
      this.child[stream].on('error', fail);
      this.child[stream].on('data', (chunk: Buffer) => {
        const state = this.capture[stream], prefix = this.prefixes[stream]; state.seenBytes += chunk.length;
        const count = Math.min(chunk.length, prefix.length - state.retainedBytes);
        chunk.copy(prefix, state.retainedBytes, 0, count); state.retainedBytes += count;
        if (count < chunk.length) { state.truncated = true; fail('Owned ' + name + ' ' + stream + ' capture overflow'); }
        if (stream !== 'stdout') return;
        let end: number;
        while ((end = prefix.indexOf(10, this.parsed)) >= 0 && end < state.retainedBytes) {
          const start = this.parsed; this.parsed = end + 1;
          if (end - start > 2 * 1024 * 1024 || this.values.length >= 10000) { fail('Owned native JSON line/event cap'); continue; }
          try {
            const value = JSON.parse(prefix.subarray(start, end).toString('utf8')); this.values.push(value);
            const entry = this.pending.get(value.id);
            if (entry) { clearTimeout(entry.timer); this.pending.delete(value.id); entry.resolve(value); }
          } catch (error) { fail('Owned native JSON: ' + error); }
        }
        if (state.retainedBytes - this.parsed > 2 * 1024 * 1024) fail('Owned native partial line cap');
      });
    }
    this.child.on('close', (exitCode, signal) => {
      this.nativeClose = { exitCode, signal };
      if (this.parsed !== this.capture.stdout.retainedBytes) fail('Owned native partial final stdout line');
      if (this.pending.size) fail('Owned native close with unsettled requests');
    });
  }
  static cli(root: string, name: string, env: NodeJS.ProcessEnv, preloads: string[]): UnprovedClient {
    const recorder = recordSpawns(root);
    return new UnprovedClient(name, root, [...WASM_RUNTIME_FLAGS, ...recorder.args,
      ...preloads.flatMap(file => ['--require', path.join(__dirname, 'fixtures', file)]),
      path.resolve(__dirname, '../dist/bin/codegraph.js'), 'serve', '--mcp', '--path', root], { ...process.env,
      CODEGRAPH_QUERY_MODE: undefined, CODEGRAPH_QUERY_POOL_SIZE: '2', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
      CODEGRAPH_NO_DAEMON: '0', CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '300', CODEGRAPH_DAEMON_INSTALL_CHECK_MS: '0',
      CODEGRAPH_DAEMON_RETRY_MS: '100', CODEGRAPH_DAEMON_RETRY_MAX_MS: '100', CODEGRAPH_MCP_LOG_ATTACH: '1',
      CODEGRAPH_NO_WATCHDOG: '1', CODEGRAPH_PPID_POLL_MS: '0', CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS: '0',
      CODEGRAPH_NO_RELAUNCH: '1', CODEGRAPH_WASM_RELAUNCHED: '1',
      CODEGRAPH_MCP_TOOLS: 'explore,node,search,callers,callees,impact,files,status', ...recorder.env, ...env });
  }
  text(stream: 'stdout' | 'stderr'): string { return this.prefixes[stream].subarray(0, this.capture[stream].retainedBytes).toString('utf8'); }
  async event(name: string): Promise<any> {
    await until(() => {
      if (this.firstError) throw new Error(this.firstError);
      const failure = this.values.find(value => value.event === 'fixture-failure');
      if (failure) throw new Error(failure.error);
      if (this.nativeClose) throw new Error('Owned ' + this.name + ' closed before ' + name + ': ' + this.text('stderr'));
      return this.values.some(value => value.event === name);
    }, 10000, () => this.text('stderr'));
    return this.values.find(value => value.event === name);
  }
  request(method: string, params: unknown, id: number): Promise<any> {
    if (this.firstError || this.nativeClose) return Promise.reject(new Error(this.firstError ?? 'Owned client already closed'));
    if (this.pending.has(id)) throw new Error('Owned duplicate pending request ID');
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.firstError ??= 'Independent owned RPC deadline35000ms: ' + method;
        this.pending.delete(id); reject(new Error(this.firstError));
      }, 35000);
      this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
    });
  }
  tool(query: string, id: number, projectPath?: string): Promise<any> {
    return this.request('tools/call', { name: 'codegraph_explore', arguments: { query, ...(projectPath ? { projectPath } : {}) } }, id);
  }
  async initialize(): Promise<void> {
    const value = await this.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: this.name, version: '1' } }, 1);
    expect(value.error).toBeUndefined(); expect(value.result.serverInfo).toBeDefined();
    this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
    const tools = await this.request('tools/list', {}, 2);
    expect(tools.error).toBeUndefined(); expect(tools.result.tools.some((tool: any) => tool.name === 'codegraph_explore')).toBe(true);
  }
  async close(): Promise<void> {
    if (!this.nativeClose) this.child.stdin.end();
    await until(() => !!this.nativeClose, 35000, () => this.text('stderr'));
    expect(this.nativeClose).toEqual({ exitCode: 0, signal: null }); expect(this.firstError).toBeNull();
    expect(this.capture.stdout.truncated || this.capture.stderr.truncated).toBe(false);
    expect(this.text('stderr')).not.toContain('[owned-transport-fixture-failure]');
    expect(this.pending.size).toBe(0); expect(alive(this.child.pid!)).toBe(false);
  }
  facts(): object { return { name: this.name, pid: this.child.pid, nativeClose: this.nativeClose, firstError: this.firstError,
    capture: this.capture, pending: this.pending.size, stdout: this.text('stdout'), stderr: this.text('stderr'), values: this.values,
    prefixBase64: Object.fromEntries((['stdout', 'stderr'] as const).map(stream => [stream,
      this.prefixes[stream].subarray(0, this.capture[stream].retainedBytes).toString('base64')])) }; }
}

/** Bounded incremental reads retain malformed/partial/read failure evidence and
 * never throw from finally. Missing empty trace means no observed activity. */
export class UnprovedTrace {
  private readonly prefix = Buffer.alloc(8 * 1024 * 1024);
  private retained = 0;
  private parsed = 0;
  private readonly values: any[] = [];
  seenBytes = 0;
  firstError: string | null = null;
  truncated = false;
  constructor(private readonly file: string) {}
  read(final = false): any[] {
    let fd: number | undefined;
    try {
      fd = fs.openSync(this.file, 'r'); const size = fs.fstatSync(fd).size; this.seenBytes = Math.max(this.seenBytes, size);
      if (size < this.retained) this.firstError ??= 'Owned trace shrank';
      if (size > this.prefix.length) { this.truncated = true; this.firstError ??= 'Owned trace byte cap'; }
      const count = Math.min(size, this.prefix.length) - this.retained;
      if (count > 0) {
        const read = fs.readSync(fd, this.prefix, this.retained, count, this.retained); this.retained += read;
        if (read !== count) this.firstError ??= 'Owned trace incomplete prefix read';
      }
      let end: number;
      while ((end = this.prefix.indexOf(10, this.parsed)) >= 0 && end < this.retained) {
        const start = this.parsed; this.parsed = end + 1;
        if (end - start > 65536 || this.values.length >= 10000) { this.firstError ??= 'Owned trace line/event cap'; continue; }
        if (end === start) continue;
        try { this.values.push(JSON.parse(this.prefix.subarray(start, end).toString('utf8'))); }
        catch (error) { this.firstError ??= 'Owned trace JSON: ' + String(error).slice(0, 2048); }
      }
      if (this.retained - this.parsed > 65536) this.firstError ??= 'Owned trace partial line cap';
      if (final && this.parsed !== this.retained) this.firstError ??= 'Owned trace incomplete final line';
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT' || this.retained) this.firstError ??= 'Owned trace read: ' + String(error).slice(0, 2048);
    } finally {
      if (fd !== undefined) try { fs.closeSync(fd); } catch (error) { this.firstError ??= 'Owned trace close: ' + String(error).slice(0, 2048); }
    }
    return this.values;
  }
  facts(): object { return { file: this.file, seenBytes: this.seenBytes, retainedBytes: this.retained, parsedBytes: this.parsed,
    firstError: this.firstError, truncated: this.truncated, prefixBase64: this.prefix.subarray(0, this.retained).toString('base64') }; }
}
