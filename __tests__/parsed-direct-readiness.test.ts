// Proposal only: run against an independently built complete frozen overlay,
// never against the archive's old dist or by importing this private directory.
import { describe, expect, it } from 'vitest';
import { spawn, type ChildProcess } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as net from 'net';
import { createHash } from 'crypto';
import { TextDecoder } from 'util';
import { pathToFileURL } from 'url';
import { CodeGraph } from '../src';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { MCPEngine } from '../src/mcp/engine';
import { MCPSession } from '../src/mcp/session';
import { SocketTransport, type JsonRpcResponse } from '../src/mcp/transport';
import { resolveQueryPolicy } from '../src/mcp/query-policy';
import { getWriterPidPath } from '../src/mcp/writer-lock';

type Message = Record<string, any>;
const wait = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean, timeoutMs = 10000): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (!check()) { if (performance.now() >= deadline) throw new Error('owned native observation deadline'); await wait(20); }
}
function exists(pid: number): boolean { try { process.kill(pid, 0); return true; } catch (error) { return (error as NodeJS.ErrnoException).code === 'EPERM'; } }
function sampledIdentity(pid: number, secondary: (message: string) => void): Message | undefined {
  if (!process.env.VALIDATION_OUT) return undefined;
  const file = path.join(process.env.VALIDATION_OUT, 'resources.jsonl');
  if (!fs.existsSync(file)) return undefined;
  const descriptor = fs.openSync(file, 'r');
  let primaryFailed = false;
  try {
    const size = fs.fstatSync(descriptor).size;
    const offset = Math.max(0, size - 1048576), bytes = Buffer.alloc(size - offset);
    const count = fs.readSync(descriptor, bytes, 0, bytes.length, offset);
    const lines = bytes.subarray(0, count).toString('utf8').split('\n');
    if (offset) lines.shift();
    lines.pop(); // ignore a final in-progress row; supervisor flushes each LF
    for (const line of lines.slice(-5).reverse()) {
      const sample = JSON.parse(line);
      const row = sample.processes?.find((entry: Message) => entry.pid === pid && typeof entry.creationIdentity === 'string');
      if (row) return { pid, creationIdentity: row.creationIdentity, elapsedSeconds: sample.elapsedSeconds };
    }
    return undefined;
  } catch (error) { primaryFailed = true; throw error; }
  finally {
    try { fs.closeSync(descriptor); }
    catch (error) { secondary(`native census close: ${String(error).slice(0, 256)}`); if (!primaryFailed) throw error; }
  }
}
class NativeHost {
  readonly child: ChildProcess;
  readonly messages: Message[] = [];
  readonly events: Message[] = [];
  readonly failures: string[] = [];
  failureCount = 0;
  readonly streams = { stdout: false, stderr: false };
  readonly streamEnds = { stdout: false, stderr: false };
  readonly captures = {
    stdout: { seenBytes: 0, retainedBytes: 0, truncated: false, prefix: Buffer.alloc(0) },
    stderr: { seenBytes: 0, retainedBytes: 0, truncated: false, prefix: Buffer.alloc(0) },
  };
  exit: { code: number | null; signal: NodeJS.Signals | null } | null = null;
  rawClose = false;
  disconnected = false;
  nativeIdentity: Message | null = null;
  readonly watchdogIdentities: Message[] = [];
  readonly progressWrites: Message[] = [];
  readonly watchdogCloseFacts: Message[] = [];
  readonly expectedExitCode: number;
  private decodeBuffer = '';
  private readonly decoders = { stdout: new TextDecoder('utf-8', { fatal: true }), stderr: new TextDecoder('utf-8', { fatal: true }) };
  constructor(readonly root: string, readonly scenario: string, readonly evidence: string, longWork = false, peer?: net.AddressInfo) {
    this.expectedExitCode = ['progress-missing-close', 'proxy-drain-failure'].includes(scenario) ? 1 : 0;
    this.child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS,
      ...(longWork ? ['--require', path.join(__dirname, 'fixtures/query-waiting-preload.cjs')] : []),
      path.join(__dirname, 'fixtures/parsed-direct-readiness-host.cjs'), root, scenario, evidence], {
      cwd: root, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
      env: { ...process.env, CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS: '30000',
        ...(scenario.startsWith('progress-') || scenario === 'proxy-drain-failure' ? { CODEGRAPH_NO_WATCHDOG: '0' } : {}),
        ...(scenario === 'proxy-drain-failure' ? { CODEGRAPH_DAEMON_RETRY_MS: '5000', CODEGRAPH_DAEMON_RETRY_MAX_MS: '5000' } : {}),
        ...(['progress-db-only', 'progress-wal-only'].includes(scenario) ? { CODEGRAPH_WATCHDOG_TIMEOUT_MS: '500' } : {}),
        ...(peer ? { PARSED_READINESS_PEER: JSON.stringify({ host: '127.0.0.1', port: peer.port }) } : {}),
        QUERY_WAITING_OBSERVATIONS: path.join(root, 'waiting.jsonl') },
    });
    for (const channel of ['stdout', 'stderr'] as const) {
      this.child[channel]!.on('data', (value: Buffer) => {
        const bytes = Buffer.from(value), capture = this.captures[channel];
        capture.seenBytes += bytes.length;
        const retained = bytes.subarray(0, Math.max(0, 65536 - capture.retainedBytes));
        capture.prefix = Buffer.concat([capture.prefix, retained]); capture.retainedBytes += retained.length;
        if (capture.seenBytes > 65536 && !capture.truncated) { capture.truncated = true; this.fail(`${channel} overflow`); }
        if (capture.truncated) return;
        let text: string;
        try { text = this.decoders[channel].decode(bytes, { stream: true }); }
        catch (error) { this.fail(`${channel} UTF8: ${String(error).slice(0, 256)}`); return; }
        if (channel !== 'stdout') return;
        this.decodeBuffer += text;
        let end: number;
        while ((end = this.decodeBuffer.indexOf('\n')) >= 0) {
          const line = this.decodeBuffer.slice(0, end); this.decodeBuffer = this.decodeBuffer.slice(end + 1);
          if (!line.trim()) continue;
          try {
            if (this.messages.length >= 128) throw new Error('message count overflow');
            this.messages.push(JSON.parse(line));
          } catch (error) { this.fail(`stdout decode: ${String(error).slice(0, 256)}`); }
        }
      });
      this.child[channel]!.on('end', () => {
        this.streamEnds[channel] = true;
        try {
          const tail = this.decoders[channel].decode();
          if (channel === 'stdout') {
            this.decodeBuffer += tail;
            if (this.decodeBuffer.length) this.fail('stdout EOF contained an unterminated frame');
          }
        } catch (error) { this.fail(`${channel} UTF8 EOF: ${String(error).slice(0, 256)}`); }
      });
      this.child[channel]!.on('close', () => {
        this.streams[channel] = true;
        if (!this.streamEnds[channel]) this.fail(`${channel} closed without end`);
      });
      this.child[channel]!.on('error', error => { this.fail(`${channel}: ${String(error).slice(0, 256)}`); });
    }
    this.child.stdin!.on('error', error => { this.fail(`stdin: ${String(error).slice(0, 256)}`); });
    this.child.on('message', message => {
      if (this.events.length >= 64) { this.fail('IPC count overflow'); return; }
      if (Buffer.byteLength(JSON.stringify(message)) > 8192) { this.fail('IPC payload overflow'); return; }
      this.events.push(message as Message);
    });
    this.child.on('error', error => { this.fail(`child: ${String(error).slice(0, 256)}`); });
    this.child.on('exit', (code, signal) => { this.exit = { code, signal }; });
    this.child.on('close', () => { this.rawClose = true; });
    this.child.on('disconnect', () => { this.disconnected = true; });
  }
  fail(message: string): void { this.failureCount++; if (this.failures.length < 24) this.failures.push(message.slice(0, 512)); }
  command(command: string, detail: object = {}): void { this.child.send({ command, ...detail }, error => { if (error) this.fail(`IPC send: ${String(error).slice(0, 256)}`); }); }
  send(id: string | number | undefined, method: string, params: unknown = {}): void {
    this.child.stdin!.write(JSON.stringify({ jsonrpc: '2.0', ...(id === undefined ? {} : { id }), method, params }) + '\n');
  }
  async event(event: string): Promise<Message> { await until(() => this.events.some(value => value.event === event)); return this.events.find(value => value.event === event)!; }
  async reply(id: string | number, timeoutMs = 10000): Promise<Message> {
    await until(() => this.messages.some(value => value.id === id && ('result' in value || 'error' in value)), timeoutMs);
    return this.messages.find(value => value.id === id && ('result' in value || 'error' in value))!;
  }
  async begin(): Promise<void> {
    await this.event('online');
    // Acquisition is separately bounded while the fixture is inert. Linux must
    // use its native inner supervisor output, never the Docker host's PIDs.
    if (process.env.VALIDATION_OUT) await until(() => {
      const identity = sampledIdentity(this.child.pid!, message => this.fail(message));
      if (identity) this.nativeIdentity = identity;
      return !!identity;
    }, 15000);
    // Do not await the new prepare callback here. A future red against the old
    // proxy must reach its actual handshake/tool result, not a setup timeout.
    this.command('begin');
  }
  async acquireWatchdogIdentity(pid: number): Promise<void> {
    if (!process.env.VALIDATION_OUT) return;
    await until(() => {
      const identity = sampledIdentity(pid, message => this.fail(message));
      if (identity && !this.watchdogIdentities.some(value => value.pid === pid)) this.watchdogIdentities.push(identity);
      return !!identity;
    }, 15000);
  }
  async close(): Promise<void> {
    if (!this.exit && !this.child.stdin!.writableEnded) this.child.stdin!.end();
    // This is an independent cleanup bound, not a CodeGraph request deadline.
    await until(() => !!this.exit && this.rawClose && this.streams.stdout && this.streams.stderr && this.disconnected, 40000);
    await until(() => !exists(this.child.pid!), 3000);
    const evidenceSize = fs.statSync(this.evidence).size;
    if (evidenceSize > 65536) throw new Error('native final events exceed bounded capture');
    const finalEvents = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(fs.readFileSync(this.evidence))).events as Message[];
    const watchdogPids = finalEvents.filter(value => value.event === 'watchdog-spawned').map(value => value.pid);
    await until(() => watchdogPids.every(pid => !exists(pid)), 5000);
    for (const pid of watchdogPids) {
      const observedClose = finalEvents.some(value => value.event === 'watchdog-closed' && value.pid === pid);
      this.watchdogCloseFacts.push({ pid, observedClose, pidAbsent: !exists(pid),
        observerExited: this.scenario === 'proxy-drain-failure' && !observedClose });
      if (this.scenario === 'proxy-drain-failure' && !observedClose) {
        // Sole failure-row boundary: the poisoned drain retains protection to
        // host exit. A later watchdog close is no longer JS-observable there.
        // Its separately acquired exact identity and native absence remain
        // mandatory; this is never a successful watchdog-drain claim.
        expect(this.watchdogIdentities.some(value => value.pid === pid)).toBe(true);
      } else expect(observedClose).toBe(true);
    }
    expect(this.exit).toEqual({ code: this.expectedExitCode, signal: null });
  }
  facts(): object {
    return { pid: this.child.pid, nativeIdentity: this.nativeIdentity, watchdogIdentities: this.watchdogIdentities, watchdogCloseFacts: this.watchdogCloseFacts, progressWrites: this.progressWrites,
      scenario: this.scenario, expectedExitCode: this.expectedExitCode, exit: this.exit, rawClose: this.rawClose,
      streams: this.streams, streamEnds: this.streamEnds, disconnected: this.disconnected, pidAbsent: !exists(this.child.pid!),
      failureCount: this.failureCount, captures: Object.fromEntries(Object.entries(this.captures).map(([key, value]) => [key,
        { seenBytes: value.seenBytes, retainedBytes: value.retainedBytes, truncated: value.truncated,
          prefixBase64: value.prefix.toString('base64'), sha256: createHash('sha256').update(value.prefix).digest('hex') }])),
      messages: this.messages, events: this.events, failures: this.failures };
  }
}
class NativePeer {
  readonly sockets = new Set<net.Socket>();
  readonly sessions: MCPSession[] = [];
  readonly failures: string[] = [];
  failureCount = 0;
  readonly responses: JsonRpcResponse[] = [];
  engineStopCompleted = false;
  fail(error: unknown): void { this.failureCount++; if (this.failures.length < 24) this.failures.push(String(error).slice(0, 512)); }
  readonly engine = new MCPEngine({ queryPolicy: resolveQueryPolicy({}, 'shared-daemon', 4),
    executionMode: 'shared-daemon', queryPool: true, watch: false });
  readonly server = net.createServer(socket => {
    this.sockets.add(socket);
    socket.on('error', error => this.fail(error));
    socket.once('close', () => this.sockets.delete(socket));
    const peer = this;
    const transport = new class extends SocketTransport {
      override send(response: JsonRpcResponse): boolean {
        if (peer.responses.length >= 32) peer.fail('peer response count overflow');
        else peer.responses.push(response);
        return super.send(response);
      }
    }(socket);
    const session = new MCPSession(transport, this.engine);
    this.sessions.push(session); session.start();
  });
  async listen(): Promise<net.AddressInfo> {
    await new Promise<void>((resolve, reject) => {
      this.server.once('error', reject);
      this.server.listen(0, '127.0.0.1', () => { this.server.off('error', reject); resolve(); });
    });
    this.server.on('error', error => this.fail(error));
    return this.server.address() as net.AddressInfo;
  }
  private stopping: Promise<void> | null = null;
  stop(): Promise<void> {
    if (!this.stopping) this.stopping = this.stopResources();
    return this.stopping;
  }
  private async stopResources(): Promise<void> {
    const failures: unknown[] = [];
    for (const session of this.sessions) try { session.stop(); } catch (error) { failures.push(error); }
    for (const socket of this.sockets) socket.destroy();
    try { await until(() => this.sockets.size === 0, 5000); } catch (error) { failures.push(error); }
    try { await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('owned peer server close deadline')), 5000);
      this.server.close(error => { clearTimeout(timer); if (error) reject(error); else resolve(); });
    }); }
    catch (error) { failures.push(error); }
    let timer: NodeJS.Timeout | undefined;
    const stopping = Promise.resolve().then(() => this.engine.stop()).then(() => { this.engineStopCompleted = true; });
    void stopping.catch(() => { /* retained by the independently bounded join */ });
    try { await Promise.race([stopping, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error('owned peer engine stop deadline')), 25000);
    })]); } catch (error) { failures.push(error); }
    finally { if (timer) clearTimeout(timer); }
    if (failures.length) throw new AggregateError(failures, 'owned real peer cleanup');
  }
}
async function nativeCase(name: string, body: (host: NativeHost, roots: string[], peer?: NativePeer) => Promise<void>, longWork = false): Promise<void> {
  // Retain inert input/index/raw evidence even when a primary assertion or
  // persistence fails. Process/resource cleanup is mandatory and independent.
  // Qualified Linux exports this output subtree before --rm, including FAIL.
  const fixtureBase = process.env.VALIDATION_OUT ? path.join(process.env.VALIDATION_OUT, 'parsed-direct-fixtures') : os.tmpdir();
  fs.mkdirSync(fixtureBase, { recursive: true });
  const fixture = fs.realpathSync(fs.mkdtempSync(path.join(fixtureBase, 'cg-parsed-direct-')));
  const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
  let host: NativeHost | undefined;
  let peer: NativePeer | undefined;
  let primary: unknown;
  let primaryFailed = false;
  const secondary: string[] = [];
  let secondaryCount = 0;
  const lost = (phase: string, error: unknown): void => {
    secondaryCount++;
    if (secondary.length < 32) secondary.push(`${phase}: ${String(error).slice(0, 512)}`);
  };
  try {
    for (const [index, root] of roots.entries()) {
      fs.mkdirSync(root);
      fs.writeFileSync(path.join(root, 'app.ts'), `export function ${index ? 'betaReadinessSymbol' : 'alphaReadinessSymbol'}() { return 42; }\nexport function delayedTimingSymbol() { return 47; }\n`);
      const graph = await CodeGraph.init(root);
      let indexingFailed = false;
      try { await graph.indexAll(); }
      catch (error) { indexingFailed = true; throw error; }
      finally {
        try { graph.close(); }
        catch (error) { lost('fixture graph close', error); if (!indexingFailed) throw error; }
      }
    }
    let endpoint: net.AddressInfo | undefined;
    if (name === 'proxy-healthy' || name === 'proxy-drain-failure') { peer = new NativePeer(); endpoint = await peer.listen(); }
    host = new NativeHost(roots[0], name, path.join(fixture, 'native-events.json'), longWork, endpoint);
    await host.begin();
    await body(host, roots, peer);
  } catch (error) { primaryFailed = true; primary = error; }
  finally {
    const capture = async (phase: string, action: () => unknown | Promise<unknown>) => {
      try { await action(); } catch (error) { lost(phase, error); }
    };
    if (host) {
      // Only preparation has a parent-controlled inert gate. An acquired
      // factory can settle ONLY through descriptor.stop, never parent release.
      await capture('release', () => { if (host!.child.connected) host!.command('release-preparation'); });
      if (name === 'proxy-drain-failure') await capture('release injected drain', () => {
        if (host!.child.connected) host!.command('reject-injected-drain');
      });
      await capture('close', () => host!.close());
      if (!host.exit || !host.rawClose || exists(host.child.pid!)) await capture('owned fallback', async () => {
        if (exists(host!.child.pid!)) host!.child.kill('SIGKILL');
        await until(() => !!host!.exit && host!.rawClose && host!.streams.stdout && host!.streams.stderr && host!.disconnected, 5000);
        await until(() => !exists(host!.child.pid!), 3000);
      });
    }
    if (peer) {
      await capture('owned real peer cleanup', () => peer!.stop());
      for (const failure of peer.failures) lost('peer', failure);
    }
    const retained: Message[] = [];
    const raw = (label: string, bytes: Buffer, seenBytes: number, truncated: boolean): void => {
      const file = path.join(fixture, `${label}.raw`);
      fs.writeFileSync(file, bytes);
      retained.push({ label, file, seenBytes, retainedBytes: bytes.length, truncated,
        sha256: createHash('sha256').update(bytes).digest('hex') });
    };
    if (host) {
      for (const channel of ['stdout', 'stderr'] as const) await capture(`${channel} raw persistence`, () => {
        const value = host!.captures[channel]; raw(channel, value.prefix, value.seenBytes, value.truncated);
      });
      for (const [label, file] of [['native-events', path.join(fixture, 'native-events.json')],
        ...(longWork ? [['waiting', path.join(roots[0], 'waiting.jsonl')]] : []),
        ...(['progress-db-only', 'progress-wal-only'].includes(name) ? [['native-block', `${host.evidence}.block.json`]] : [])]) {
        await capture(`${label} raw persistence`, () => {
          let descriptor: number | undefined;
          try {
            descriptor = fs.openSync(file, 'r');
            const size = fs.fstatSync(descriptor).size;
            const bytes = Buffer.alloc(Math.min(size, 65536));
            const count = fs.readSync(descriptor, bytes, 0, bytes.length, 0);
            raw(label, bytes.subarray(0, count), size, size > count);
            if (size !== count) throw new Error(`${label} capture overflow or short read`);
            if (label === 'native-events') {
              const observation = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
              if (observation.observerFailureCount !== 0 || !observation.events?.some((event: Message) => event.event === 'native-exit')) {
                throw new Error('native fixture observer loss or missing native-exit tail');
              }
            }
          } finally {
            if (descriptor !== undefined) try { fs.closeSync(descriptor); } catch (error) { lost(`${label} file close`, error); }
          }
        });
      }
      for (const failure of host.failures) lost('native', failure);
    }
    const evidenceRoot = process.env.VALIDATION_OUT || fixture;
    await capture('case evidence', () => {
      const bytes = Buffer.from(JSON.stringify({ name, fixture, primaryFailed,
        primary: primaryFailed ? String(primary).slice(0, 2048) : null, secondaryCount, secondary,
        native: host?.facts(), peer: peer ? { ownerPid: process.pid, sessions: peer.sessions.length, remainingSockets: peer.sockets.size,
          serverListening: peer.server.listening, engineStopCompleted: peer.engineStopCompleted,
          failureCount: peer.failureCount, failures: peer.failures, responses: peer.responses } : null, retained }, null, 2));
      if (bytes.length > 1048576) throw new Error('native case evidence exceeded 1 MiB');
      fs.writeFileSync(path.join(evidenceRoot, `parsed-direct-${name}.json`), bytes);
    });
    await capture('cleanup tail', () => {
      fs.writeFileSync(path.join(evidenceRoot, `parsed-direct-${name}-cleanup.json`),
        JSON.stringify({ name, fixture, fixtureRetained: fs.existsSync(fixture),
          disposition: primaryFailed || secondaryCount ? 'failed-inputs-and-raw-evidence-retained' : 'successful-inputs-and-raw-evidence-retained',
          primaryFailed, secondaryCount, secondary, retained, native: host ? { pid: host.child.pid, exit: host.exit,
            rawClose: host.rawClose, streams: host.streams, streamEnds: host.streamEnds, disconnected: host.disconnected,
            pidAbsent: !exists(host.child.pid!) } : null }, null, 2));
    });
    if (secondaryCount) await capture('bounded fallback stderr', () => process.stderr.write(JSON.stringify({ name, fixture,
      primaryFailed, primary: primaryFailed ? String(primary).slice(0, 1024) : null, secondaryCount, secondary }) + '\n'));
  }
  if (primaryFailed) throw primary;
  expect(secondaryCount, JSON.stringify(secondary)).toBe(0);
}
// A completed JSONL prefix is immutable even while the worker appends its next
// row. Only a final in-progress row is withheld; malformed completed rows fail.
function workerObservations(file: string): Message[] {
  if (!fs.existsSync(file)) return [];
  const bytes = fs.readFileSync(file);
  if (bytes.length > 65536) throw new Error('owned worker observation bound exceeded');
  const end = bytes.lastIndexOf(10);
  if (end < 0) return [];
  return new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, end + 1))
    .trim().split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
}
async function indexOwnedFixture(graph: CodeGraph, host: NativeHost): Promise<void> {
  let primaryFailed = false;
  try { await graph.indexAll(); }
  catch (error) { primaryFailed = true; throw error; }
  finally {
    try { graph.close(); }
    catch (error) { host.fail(`owned fixture graph close: ${String(error).slice(0, 256)}`); if (!primaryFailed) throw error; }
  }
}
const init = (root?: string) => ({ protocolVersion: '2024-11-05', capabilities: { roots: {} },
  clientInfo: { name: 'parsed-direct-proof', version: '1' }, ...(root ? { rootUri: pathToFileURL(root).href } : {}) });
const tool = (name: string, args: object = {}) => ({ name, arguments: args });

describe('late prepared direct routes retain one real MCP session', () => {
  it('answers static startup, then replays rootUri once and preserves wire IDs/history/explicit targets', async () => {
    await nativeCase('direct', async (host, roots) => {
      host.send(7, 'initialize', init(roots[0])); host.send(8, 'tools/list');
      const handshake = await host.reply(7); await host.reply(8);
      expect(host.events.some(value => value.event === 'factory-entered')).toBe(false);
      host.send(9, 'tools/call', tool('codegraph_search', { query: 'alphaReadinessSymbol' }));
      host.command('release-preparation');
      const first = await host.reply(9);
      // Intended first old-proxy tracer: an ACTUAL wire tool result, never a
      // missing-prepare setup wait. An internal error is retained as that result.
      expect(first.result, JSON.stringify(first)).toBeDefined();
      expect(first.result.content[0].text).toContain('alphaReadinessSymbol');
      expect(handshake.result.instructions).toContain('verification is pending');
      host.send(7, 'ping'); await until(() => host.messages.filter(value => value.id === 7).length === 2);
      expect(host.messages.filter(value => value.id === 7)[1].result).toEqual({});
      expect(host.messages.some(value => value.method === 'roots/list')).toBe(false);
      for (const [id, projectPath, query] of [[10, undefined, 'alphaReadinessSymbol'], [11, roots[1], 'betaReadinessSymbol'], [12, undefined, 'alphaReadinessSymbol']] as const) {
        host.send(id, 'tools/call', tool('codegraph_explore', { query, ...(projectPath ? { projectPath } : {}) }));
        expect((await host.reply(id)).result.isError).toBeFalsy();
      }
      host.command('inspect'); const observed = await host.event('inspection');
      expect(observed.execution.policy.executionMode).toBe('direct');
      expect(observed.history.map((value: Message) => value.callCount).sort()).toEqual([1, 2]);
      expect(observed.stdinDataListeners).toBe(1);
      expect(host.events.filter(value => value.event === 'factory-entered')).toHaveLength(1);
      expect(host.events.filter(value => value.event === 'engine-created')).toHaveLength(1);
    });
  }, 120000);

  it('does not revive a cancelled queued object when its ID and JSON are reused identically', async () => {
    await nativeCase('cancel-reuse', async (host, roots) => {
      host.send(1, 'initialize', init(roots[0])); await host.reply(1);
      const args = tool('codegraph_explore', { query: 'alphaReadinessSymbol', projectPath: roots[0] });
      host.send('reuse', 'tools/call', args);
      host.send(undefined, 'notifications/cancelled', { requestId: 'reuse' });
      host.send('reuse', 'tools/call', args); // exact same ID AND serialized text
      host.send('queued-barrier', 'tools/list');
      expect((await host.reply('queued-barrier')).result.tools.length).toBeGreaterThan(0);
      host.command('release-preparation');
      expect((await host.reply('reuse')).result.isError).toBeFalsy();
      host.send('barrier', 'ping'); await host.reply('barrier');
      host.command('inspect'); const observed = await host.event('inspection');
      expect(host.events.filter(value => value.event === 'tool-dispatched')).toHaveLength(1);
      expect(host.messages.filter(value => value.id === 'reuse')).toHaveLength(1);
      expect(observed.history.map((value: Message) => value.callCount)).toEqual([1]);
    });
  }, 120000);

  it('keeps the first queued live ID when a different action duplicates it', async () => {
    await nativeCase('duplicate-id', async (host, roots) => {
      host.send(1, 'initialize', init(roots[0])); await host.reply(1);
      host.send('duplicate', 'tools/call', tool('codegraph_explore', { query: 'alphaReadinessSymbol', projectPath: roots[0] }));
      host.send('duplicate', 'tools/call', tool('codegraph_explore', { query: 'betaReadinessSymbol', projectPath: roots[1] }));
      host.send('queued-barrier', 'tools/list');
      expect((await host.reply('queued-barrier')).result.tools.length).toBeGreaterThan(0);
      host.command('release-preparation');
      expect((await host.reply('duplicate')).result.content[0].text).toContain('alphaReadinessSymbol');
      host.send('barrier', 'ping'); await host.reply('barrier');
      host.command('inspect'); const observed = await host.event('inspection');
      expect(host.events.filter(value => value.event === 'tool-dispatched')).toHaveLength(1);
      expect(host.messages.filter(value => value.id === 'duplicate')).toHaveLength(1);
      expect(observed.history.map((value: Message) => value.projectRoot)).toEqual([roots[0]]);
      expect(observed.history.map((value: Message) => value.callCount)).toEqual([1]);
    });
  }, 120000);

  it('suppresses only the actual priming initialize reply on a healthy prepared proxy attachment', async () => {
    await nativeCase('proxy-healthy', async (host, roots, peer) => {
      host.send(7, 'initialize', init(roots[0])); await host.reply(7);
      host.command('release-preparation'); await host.event('proxy-acquisition');
      await until(() => peer!.responses.some(value => value.id === 7));
      host.send(7, 'ping');
      await until(() => host.messages.filter(value => value.id === 7).length === 2);
      expect(host.messages.filter(value => value.id === 7)[1].result).toEqual({});
      host.send(8, 'ping'); expect((await host.reply(8)).result).toEqual({});
      expect(peer!.sessions).toHaveLength(1);
      expect(peer!.responses.filter(value => value.id === 7)).toHaveLength(2);
      expect(host.events.filter(value => value.event === 'proxy-fallback-entered')).toEqual([]);
      expect(host.events.filter(value => value.event === 'factory-entered')).toEqual([]);
    });
  }, 120000);

  it('keeps roots responses and cancellation flowing while a real session request awaits roots', async () => {
    await nativeCase('roots', async (host, roots) => {
      host.send('init', 'initialize', init()); await host.reply('init');
      host.send('cancel-before', 'tools/call', tool('codegraph_explore', { query: 'alphaReadinessSymbol' }));
      host.send(undefined, 'notifications/cancelled', { requestId: 'cancel-before' });
      host.command('release-preparation'); await host.event('session-created');
      host.send('cancel-during', 'tools/call', tool('codegraph_explore', { query: 'alphaReadinessSymbol' }));
      await until(() => host.messages.some(value => value.method === 'roots/list'));
      const rootsRequest = host.messages.find(value => value.method === 'roots/list')!;
      host.send(undefined, 'notifications/cancelled', { requestId: 'cancel-during' });
      host.send('init', 'ping'); await until(() => host.messages.filter(value => value.id === 'init').length === 2);
      host.child.stdin!.write(JSON.stringify({ jsonrpc: '2.0', id: rootsRequest.id,
        result: { roots: [{ uri: pathToFileURL(roots[0]).href, name: 'alpha' }] } }) + '\n');
      host.send('after', 'tools/call', tool('codegraph_explore', { query: 'alphaReadinessSymbol' }));
      expect((await host.reply('after')).result.isError).toBeFalsy();
      host.command('inspect'); const observed = await host.event('inspection');
      expect(observed.history.map((value: Message) => value.callCount)).toEqual([1]);
      expect(host.messages.filter(value => value.id === 'cancel-before' || value.id === 'cancel-during')).toEqual([]);
      expect(host.messages.filter(value => value.method === 'roots/list')).toHaveLength(1);
    });
  }, 120000);

  it.each(['refusal', 'factory-refusal', 'proxy-refusal'])('retains terminal %s for buffered and new actions without fallback or repeated factories', async scenario => {
    await nativeCase(scenario, async host => {
      host.send(1, 'initialize', init()); await host.reply(1);
      host.send(2, 'tools/call', tool('codegraph_search', { query: 'alphaReadinessSymbol' }));
      host.command('release-preparation');
      expect((await host.reply(2)).error.data.code).toBe('CODEGRAPH_QUERY_POLICY_REFUSED');
      host.send(3, 'tools/call', tool('codegraph_search', { query: 'alphaReadinessSymbol' }));
      expect((await host.reply(3)).error.data.reason).toBe('unknown');
      expect(host.events.filter(value => value.event === 'factory-entered')).toHaveLength(scenario === 'factory-refusal' ? 1 : 0);
      expect(host.events.filter(value => value.event === 'proxy-acquisition')).toHaveLength(scenario === 'proxy-refusal' ? 1 : 0);
      expect(host.events.filter(value => value.event === 'proxy-fallback-entered')).toEqual([]);
      expect(host.events.filter(value => value.event === 'engine-created')).toEqual([]);
    });
  }, 120000);

  it('stops a late prepared resource without invoking its factory after stdin closes', async () => {
    await nativeCase('late', async host => {
      host.send(1, 'initialize', init()); await host.reply(1);
      host.child.stdin!.end();
      await host.event('stdin-ended');
      host.command('release-preparation');
      await host.event('resources-stopped'); await host.close();
      expect(host.events.filter(value => value.event === 'factory-entered')).toEqual([]);
      expect(host.events.filter(value => value.event === 'resources-stopped')).toHaveLength(1);
    });
  }, 120000);

  it('stops an acquired real engine before joining the factory that only resource stop can settle', async () => {
    await nativeCase('held-factory', async host => {
      host.send(1, 'initialize', init()); await host.reply(1);
      host.command('release-preparation'); await host.event('factory-awaiting-resource-stop');
      host.child.stdin!.end(); await host.event('stdin-ended');
      await host.event('resources-stopped'); await host.close();
      expect(host.events.filter(value => value.event === 'engine-created')).toHaveLength(1);
      expect(host.events.filter(value => value.event === 'engine-initialized')).toHaveLength(1);
      expect(host.events.filter(value => value.event === 'session-created')).toHaveLength(1);
      expect(host.events.filter(value => value.event === 'session-transport-closed')).toHaveLength(1);
      expect(host.events.filter(value => value.event === 'resources-stopped')).toHaveLength(1);
      expect(host.events.findIndex(value => value.event === 'resource-stop-entered'))
        .toBeLessThan(host.events.findIndex(value => value.event === 'session-created'));
      expect(host.messages.filter(value => value.method === 'roots/list')).toEqual([]);
    });
  }, 120000);

  it('returns actual worker/SQLite work after 47 seconds without occupying the stdin parser', async () => {
    await nativeCase('long-work', async (host, roots) => {
      host.send(1, 'initialize', init(roots[0])); await host.reply(1);
      host.command('release-preparation');
      host.send(2, 'tools/call', tool('codegraph_search', { query: 'delayedTimingSymbol' }));
      const observations = path.join(roots[0], 'waiting.jsonl');
      await until(() => fs.existsSync(observations) && fs.readFileSync(observations, 'utf8').includes('controlled-start'));
      host.send(3, 'ping'); expect((await host.reply(3)).result).toEqual({});
      const result = await host.reply(2, 80000);
      expect(result.result.isError).toBeFalsy(); expect(result.result.content[0].text).toContain('delayedTimingSymbol');
      const events = fs.readFileSync(observations, 'utf8').trim().split(/\r?\n/).map(line => JSON.parse(line));
      const started = events.find(event => event.event === 'controlled-start');
      const released = events.find(event => event.event === 'controlled-release');
      expect(BigInt(released.monotonicNs) - BigInt(started.monotonicNs)).toBeGreaterThanOrEqual(47000000000n);
      expect(events.some(event => event.event === 'result' && event.threadId === started.threadId && event.id === started.id)).toBe(true);
      expect(host.messages.filter(value => value.id === 2)).toHaveLength(1);
    }, true);
  }, 180000);

  it.each(['rootUri', 'workspaceFolders', 'roots'] as const)('retains client %s B after an ordinary missing-index result rather than retrying CLI/cwd A', async selection => {
    await nativeCase(`hint-${selection}`, async (host, roots) => {
      const late = path.join(path.dirname(roots[0]), 'late-B'); fs.mkdirSync(late);
      fs.writeFileSync(path.join(late, 'late.ts'), 'export function selectedLateBSymbol() { return 73; }\n');
      const params = selection === 'rootUri' ? init(late) : selection === 'workspaceFolders'
        ? { ...init(), workspaceFolders: [{ uri: pathToFileURL(late).href, name: 'B' }] } : init();
      host.send('init', 'initialize', params); await host.reply('init');
      host.command('release-preparation'); await host.event('session-created');
      host.send('missing', 'tools/call', tool('codegraph_search', { query: 'selectedLateBSymbol' }));
      if (selection === 'roots') {
        await until(() => host.messages.some(value => value.method === 'roots/list'));
        const request = host.messages.find(value => value.method === 'roots/list')!;
        host.child.stdin!.write(JSON.stringify({ jsonrpc: '2.0', id: request.id,
          result: { roots: [{ uri: pathToFileURL(late).href, name: 'B' }] } }) + '\n');
      }
      expect((await host.reply('missing')).result).toBeDefined();
      const graph = await CodeGraph.init(late);
      await indexOwnedFixture(graph, host);
      host.send('retry', 'tools/call', tool('codegraph_search', { query: 'selectedLateBSymbol' }));
      const retried = await host.reply('retry');
      expect(retried.result.content[0].text).toContain('selectedLateBSymbol');
      expect(retried.result.content[0].text).not.toContain('alphaReadinessSymbol');
      expect(host.messages.filter(value => value.method === 'roots/list')).toHaveLength(selection === 'roots' ? 1 : 0);
    });
  }, 120000);

  it('settles pending explicit beta and the first-default refusal once, fences history, and retains static discovery', async () => {
    await nativeCase('primary-refusal', async (host, roots) => {
      // A real live but unverified owner: actual admission must produce the
      // refusal, and the actual engine callback must carry it to the shell.
      fs.writeFileSync(getWriterPidPath(roots[0]), JSON.stringify({ pid: process.pid, mode: 'daemon', startedAt: Date.now() }));
      host.send('init', 'initialize', init()); await host.reply('init');
      host.command('release-preparation'); await host.event('session-created');
      host.send('beta', 'tools/call', tool('codegraph_search', { query: 'delayedTimingSymbol', projectPath: roots[1] }));
      const observations = path.join(roots[0], 'waiting.jsonl');
      let started: Message | undefined;
      await until(() => { started = workerObservations(observations).find(value => value.event === 'controlled-start'); return !!started; });
      host.send('primary', 'tools/call', tool('codegraph_search', { query: 'alphaReadinessSymbol' }));
      await until(() => host.messages.some(value => value.method === 'roots/list'));
      const request = host.messages.find(value => value.method === 'roots/list')!;
      host.child.stdin!.write(JSON.stringify({ jsonrpc: '2.0', id: request.id,
        result: { roots: [{ uri: pathToFileURL(roots[0]).href }] } }) + '\n');
      for (const id of ['beta', 'primary']) expect((await host.reply(id)).error.data)
        .toMatchObject({ code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: roots[0] });
      host.send('new', 'tools/call', tool('codegraph_explore', { query: 'betaReadinessSymbol', projectPath: roots[1] }));
      expect((await host.reply('new')).error.data.projectRoot).toBe(roots[0]);
      host.send('static', 'tools/list'); expect((await host.reply('static')).result.tools.length).toBeGreaterThan(0);
      // The real pool cancels this worker at its existing 15s cleanup bound;
      // cancellation must not wait for the healthy-only 47s controlled release.
      await until(() => workerObservations(observations).some(value => value.event === 'exit' && value.threadId === started!.threadId), 80000);
      const ended = workerObservations(observations);
      expect(ended.find(value => value.event === 'exit' && value.threadId === started!.threadId)?.code).toBe(1);
      expect(ended.some(value => ['controlled-release', 'result'].includes(value.event) && value.threadId === started!.threadId && value.id === started!.id)).toBe(false);
      host.send('barrier', 'ping'); await host.reply('barrier');
      host.command('inspect'); const observed = await host.event('inspection');
      expect(observed.history).toEqual([]);
      expect(host.events.filter(value => value.event === 'initial-default-outcome').map(value => value.kind)).toEqual(['refused']);
      for (const id of ['beta', 'primary', 'new']) expect(host.messages.filter(value => value.id === id)).toHaveLength(1);
    }, true);
  }, 180000);

  it('ends the initial phase only on a genuine no-route and keeps later default refusal scoped away from beta', async () => {
    await nativeCase('primary-no-route', async (host, roots) => {
      const late = path.join(path.dirname(roots[0]), 'no-default'); fs.mkdirSync(late);
      host.send('init', 'initialize', init(late)); await host.reply('init');
      host.command('release-preparation');
      host.send('missing', 'tools/call', tool('codegraph_search', { query: 'noDefaultSymbol' }));
      expect((await host.reply('missing')).result).toBeDefined();
      await host.event('initial-default-outcome');
      const graph = await CodeGraph.init(late);
      await indexOwnedFixture(graph, host);
      fs.writeFileSync(getWriterPidPath(late), JSON.stringify({ pid: process.pid, mode: 'daemon', startedAt: Date.now() }));
      host.send('later-default', 'tools/call', tool('codegraph_search', { query: 'noDefaultSymbol' }));
      expect((await host.reply('later-default')).error.data.projectRoot).toBe(fs.realpathSync(late));
      host.send('beta', 'tools/call', tool('codegraph_explore', { query: 'betaReadinessSymbol', projectPath: roots[1] }));
      expect((await host.reply('beta')).result.isError).toBeFalsy();
      expect(host.events.filter(value => value.event === 'initial-default-outcome').map(value => value.kind)).toEqual(['no-route']);
    });
  }, 120000);

  it('retains the real failed-drain engine, settles buffered calls once, and reaches the original failure backstop without replacement', async () => {
    await nativeCase('proxy-drain-failure', async (host, roots, peer) => {
      host.send('init', 'initialize', init()); await host.reply('init');
      await host.acquireWatchdogIdentity((await host.event('watchdog-spawned')).pid);
      host.command('release-preparation');
      host.send('peer-ping', 'ping'); expect((await host.reply('peer-ping')).result).toEqual({});
      expect(peer!.responses.some(value => value.id === 'peer-ping')).toBe(true);
      // The real peer's resources and socket actually drain before fallback.
      await peer!.stop(); await host.event('proxy-unavailable');
      host.send('indexed', 'tools/call', tool('codegraph_search', { query: 'alphaReadinessSymbol' }));
      expect((await host.reply('indexed')).result.content[0].text).toContain('alphaReadinessSymbol');
      const draining = await host.event('injected-drain-entered');
      expect(draining.diagnostics.lifecycle).toMatchObject({ ready: true, workers: 1 });
      expect(draining.route.index).toBe(fs.realpathSync(path.join(roots[0], '.codegraph/codegraph.db')));
      expect(draining.watchdogs.at(-1).paths.sort()).toEqual([draining.route.index, `${draining.route.index}-wal`].sort());
      expect(draining.watchdogs.at(-1).closed).toBe(false);
      await host.acquireWatchdogIdentity(draining.watchdogs.at(-1).pid);
      for (const id of ['queued-a', 'queued-b']) host.send(id, 'tools/call', tool('codegraph_explore', { query: 'alphaReadinessSymbol' }));
      // SAME stdin ordering proves these requests entered the connecting queue
      // while the actual engine's drain remains held by the inert fault seam.
      host.send('queued-barrier', 'tools/list'); expect((await host.reply('queued-barrier')).result.tools.length).toBeGreaterThan(0);
      expect(host.messages.some(value => ['queued-a', 'queued-b'].includes(value.id))).toBe(false);
      host.command('reject-injected-drain');
      for (const id of ['queued-a', 'queued-b']) {
        const reply = await host.reply(id);
        expect(reply.error.code).toBe(-32603);
        expect(reply.error.message).toContain('owned injected native-engine drain failure');
        expect(reply.error.data).toEqual({ code: 'CODEGRAPH_CLEANUP_INCOMPLETE' });
      }
      const rejected = await host.event('injected-drain-rejected');
      expect(rejected.diagnostics.ownership.generation).toBe(draining.diagnostics.ownership.generation);
      expect(rejected.diagnostics.lifecycle.workers).toBe(1);
      expect(rejected.watchdogs.at(-1).closed).toBe(false);
      // Both actual cleanup-error replies above can only follow production
      // receiving the rejection. Observe the real retained watchdog AFTER that
      // causal boundary, then require its protection through native host exit.
      const retainedWatchdogPid = draining.watchdogs.at(-1).pid;
      host.command('inspect');
      const afterProduction = await host.event('inspection');
      expect(afterProduction.watchdogs.find((value: Message) => value.pid === retainedWatchdogPid))
        .toMatchObject({ pid: retainedWatchdogPid, stopRequested: false, closed: false });
      await host.close(); // production one-shot35s backstop, not a parent kill
      const final = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(fs.readFileSync(host.evidence)));
      const facts = final.events.find((value: Message) => value.event === 'native-exit');
      expect(facts.engineIdentityRetained).toBe(true);
      expect(facts.history).toEqual([]);
      expect(facts.watchdogs.find((value: Message) => value.pid === retainedWatchdogPid))
        .toMatchObject({ pid: retainedWatchdogPid, stopRequested: false, closed: false });
      const afterRejection = final.events.filter((value: Message) => value.pid === retainedWatchdogPid
        && ['watchdog-stop-requested', 'watchdog-stop-error', 'watchdog-closed'].includes(value.event)
        && BigInt(value.monotonicNs) >= BigInt(rejected.monotonicNs)
        && BigInt(value.monotonicNs) <= BigInt(facts.monotonicNs));
      expect(afterRejection).toEqual([]);
      expect(final.events.filter((value: Message) => value.event === 'proxy-fallback-entered')).toHaveLength(1);
      expect(final.events.filter((value: Message) => value.event === 'proxy-acquisition')).toHaveLength(2);
      expect(final.events.filter((value: Message) => value.event === 'injected-drain-entered')).toHaveLength(1);
      expect(final.events.some((value: Message) => value.event === 'proxy-caller-stop')).toBe(false);
      for (const id of ['queued-a', 'queued-b']) expect(host.messages.filter(value => value.id === id)).toHaveLength(1);
      expect(host.captures.stderr.prefix.toString('utf8')).toContain('did not stop within 35000ms');
      // Whole-host identity absence proves OS resources gone. A worker JS exit
      // not observable before process exit is retained as absent, never invented.
      expect(host.exit).toEqual({ code: 1, signal: null });
      const workerFacts = workerObservations(path.join(roots[0], 'waiting.jsonl'));
      expect(workerFacts.filter(value => value.event === 'spawn')).toHaveLength(1);
      // Raw worker exit callbacks are retained as observed (possibly absent
      // when the original process backstop exits first), never synthesized.
    }, true);
  }, 120000);

  it('retains exact alpha/beta progress references and closes each real watchdog before replacing it', async () => {
    await nativeCase('progress-references', async (host, roots) => {
      host.send('init', 'initialize', init()); await host.reply('init');
      const transition = async (command: string, name: string, root?: string) => {
        host.command(command, { name, root });
        await until(() => host.events.some(value => value.event === (command === 'progress-acquire' ? 'progress-acquired' : 'progress-released') && value.name === name));
        const observed = host.events.find(value => value.event === (command === 'progress-acquire' ? 'progress-acquired' : 'progress-released') && value.name === name)!;
        await host.acquireWatchdogIdentity(observed.watchdogs.at(-1).pid);
        return observed;
      };
      await host.acquireWatchdogIdentity((await host.event('watchdog-spawned')).pid);
      const alpha = await transition('progress-acquire', 'alpha-one', roots[0]);
      const same = await transition('progress-acquire', 'alpha-two', roots[0]);
      expect(same.watchdogs.length).toBe(alpha.watchdogs.length);
      const both = await transition('progress-acquire', 'beta', roots[1]);
      const dbA = fs.realpathSync(path.join(roots[0], '.codegraph/codegraph.db'));
      const dbB = fs.realpathSync(path.join(roots[1], '.codegraph/codegraph.db'));
      expect(both.watchdogs.at(-1).paths.sort()).toEqual([dbA, `${dbA}-wal`, dbB, `${dbB}-wal`].sort());
      const one = await transition('progress-release', 'alpha-one');
      expect(one.watchdogs.length).toBe(both.watchdogs.length);
      const beta = await transition('progress-release', 'alpha-two');
      expect(beta.watchdogs.at(-1).paths.sort()).toEqual([dbB, `${dbB}-wal`].sort());
      await transition('progress-release', 'beta');
      const spawned = host.events.filter(value => value.event === 'watchdog-spawned');
      expect(spawned[0].paths).toEqual([]); // pure relay/preparation starts heartbeat-only
      for (let i = 1; i < spawned.length; i++) expect(host.events.findIndex(value => value.event === 'watchdog-closed' && value.pid === spawned[i - 1].pid))
        .toBeLessThan(host.events.findIndex(value => value.event === 'watchdog-spawned' && value.pid === spawned[i].pid));
      host.child.stdin!.end(); await host.event('stdin-ended'); host.command('release-preparation'); await host.close();
      expect(host.events.filter(value => value.event === 'factory-entered')).toEqual([]);
    });
  }, 120000);

  it.each(['db-only', 'wal-only'] as const)('defers a real silent main thread on selected %s SQLite transactions alone', async file => {
    await nativeCase(`progress-${file}`, async (host, roots) => {
      host.send('init', 'initialize', init()); await host.reply('init');
      await host.acquireWatchdogIdentity((await host.event('watchdog-spawned')).pid);
      const databasePath = fs.realpathSync(path.join(roots[0], '.codegraph/codegraph.db'));
      const walPath = `${databasePath}-wal`;
      const { DatabaseSync } = require('node:sqlite');
      const database = new DatabaseSync(databasePath);
      let writer: NodeJS.Timeout | undefined;
      let primaryFailed = false;
      const fingerprint = (target: string): string => {
        try { const stat = fs.statSync(target); return `${stat.size}:${stat.mtimeMs}`; }
        catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return 'absent'; throw error; }
      };
      try {
        database.exec(`PRAGMA journal_mode=${file === 'wal-only' ? 'WAL' : 'DELETE'}; PRAGMA wal_autocheckpoint=0; CREATE TABLE readiness_progress (value TEXT);`);
        if (file === 'wal-only') database.exec('PRAGMA wal_checkpoint(TRUNCATE);');
        const insert = database.prepare('INSERT INTO readiness_progress (value) VALUES (?)');
        insert.run('initial');
        const baseline = { db: fingerprint(databasePath), wal: fingerprint(walPath) };
        host.progressWrites.push({ kind: 'baseline', monotonicNs: process.hrtime.bigint().toString(), ...baseline });
        host.command('progress-acquire', { name: 'sqlite', root: roots[0] });
        const acquired = await host.event('progress-acquired');
        expect(acquired.watchdogs.at(-1).paths.sort()).toEqual([databasePath, walPath].sort());
        await host.acquireWatchdogIdentity(acquired.watchdogs.at(-1).pid);
        let sequence = 0;
        const commit = (): void => {
          try {
            if (host.progressWrites.length >= 32) throw new Error('SQLite progress sample bound exceeded');
            const value = insert.run(`${++sequence}:${'x'.repeat(3000)}`);
            host.progressWrites.push({ kind: 'commit', monotonicNs: process.hrtime.bigint().toString(), changes: Number(value.changes),
              db: fingerprint(databasePath), wal: fingerprint(walPath) });
          } catch (error) {
            host.fail(`real SQLite progress: ${String(error).slice(0, 256)}`);
            if (writer) clearInterval(writer);
          }
        };
        commit(); writer = setInterval(commit, 125);
        host.command('block-main-thread');
        // Synchronous evidence is written before the block; this observation
        // does not depend on delivery of IPC while the child loop is silent.
        let blocked: Message | undefined;
        await until(() => {
          const publication = `${host.evidence}.block.json`;
          if (!fs.existsSync(publication)) return false;
          const bytes = fs.readFileSync(publication);
          if (bytes.length > 65536) throw new Error('native work evidence bound exceeded');
          blocked = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
          if (blocked?.event !== 'main-thread-blocking') throw new Error('malformed completed native block publication');
          return !!blocked;
        });
        await until(() => Number(process.hrtime.bigint() - BigInt(blocked!.monotonicNs)) / 1e6 >= 1100);
        expect(exists(host.child.pid!)).toBe(true);
        const resumed = await host.event('main-thread-resumed');
        clearInterval(writer); writer = undefined;
        expect(Number(BigInt(resumed.monotonicNs) - BigInt(blocked!.monotonicNs)) / 1e6).toBeGreaterThanOrEqual(2500);
        const during = host.progressWrites.filter(value => value.kind === 'commit'
          && BigInt(value.monotonicNs) >= BigInt(blocked!.monotonicNs) && BigInt(value.monotonicNs) <= BigInt(resumed.monotonicNs));
        expect(during.length).toBeGreaterThanOrEqual(3);
        expect(during.every(value => value.changes === 1)).toBe(true);
        const advancing = file === 'db-only' ? 'db' : 'wal', unchanged = file === 'db-only' ? 'wal' : 'db';
        expect(new Set(during.map(value => value[advancing])).size).toBeGreaterThanOrEqual(3);
        expect(during.every(value => value[unchanged] === baseline[unchanged])).toBe(true);
        // The raw resume event proves the synchronous work finished while
        // preparation is still held; a routed ping cannot answer at this gate.
        expect(host.events.filter(value => value.event === 'factory-entered')).toEqual([]);
        host.command('progress-release', { name: 'sqlite' });
        const released = await host.event('progress-released');
        expect(released.watchdogs).toHaveLength(acquired.watchdogs.length + 1);
        expect(released.watchdogs.slice(0, -1).map((value: Message) => value.pid))
          .toEqual(acquired.watchdogs.map((value: Message) => value.pid));
        const previous = acquired.watchdogs.at(-1), replacement = released.watchdogs.at(-1);
        // Cumulative snapshots must not let the old selected-path watchdog,
        // including a reused PID/census identity, stand in for a replacement.
        expect(new Set(released.watchdogs.map((value: Message) => value.pid)).size).toBe(released.watchdogs.length);
        expect(replacement).toMatchObject({ paths: [], closed: false });
        expect(released.watchdogs.find((value: Message) => value.pid === previous.pid)?.closed).toBe(true);
        const previousClose = host.events.findIndex(value => value.event === 'watchdog-closed' && value.pid === previous.pid);
        const replacementSpawn = host.events.findIndex(value => value.event === 'watchdog-spawned' && value.pid === replacement.pid);
        expect(previousClose).toBeGreaterThanOrEqual(0);
        expect(replacementSpawn).toBeGreaterThan(previousClose);
        expect(host.watchdogIdentities.some(value => value.pid === replacement.pid)).toBe(false);
        await host.acquireWatchdogIdentity(replacement.pid);
        if (process.env.VALIDATION_OUT) {
          const replacementIdentity = host.watchdogIdentities.find(value => value.pid === replacement.pid);
          expect(replacementIdentity).toMatchObject({ pid: replacement.pid, creationIdentity: expect.any(String) });
          expect(host.watchdogIdentities.filter(value => value.pid !== replacement.pid)
            .some(value => value.creationIdentity === replacementIdentity!.creationIdentity)).toBe(false);
        }
        expect(host.events.filter(value => value.event === 'factory-entered')).toEqual([]);
        // Retain the real public ping as the final route-health check, only
        // after the preparing-phase progress/watchdog boundary is established.
        host.command('release-preparation');
        host.send('after-work', 'ping'); await host.reply('after-work');
        host.child.stdin!.end(); await host.event('stdin-ended'); await host.close();
      } catch (error) { primaryFailed = true; throw error; }
      finally {
        if (writer) clearInterval(writer);
        try { database.close(); }
        catch (error) { host.fail(`owned SQLite close: ${String(error).slice(0, 256)}`); if (!primaryFailed) throw error; }
      }
    });
  }, 120000);

  it.runIf(process.platform !== 'win32')('fails closed at the original actual-native-close deadline and never overlaps a replacement', async () => {
    await nativeCase('progress-missing-close', async (host, roots) => {
      host.send('init', 'initialize', init()); await host.reply('init');
      const watchdog = await host.event('watchdog-spawned');
      await host.acquireWatchdogIdentity(watchdog.pid);
      process.kill(watchdog.pid, 'SIGSTOP');
      try {
        host.command('progress-acquire', { name: 'incomplete', root: roots[0] });
        const rejected = await host.event('progress-rejected');
        const stop = await host.event('watchdog-stop-requested');
        expect(rejected.message).toContain('Native watchdog close incomplete');
        const elapsed = Number(BigInt(rejected.monotonicNs) - BigInt(rejected.observerStartedNs)) / 1e6;
        expect(BigInt(stop.startedNs)).toBeGreaterThanOrEqual(BigInt(rejected.observerStartedNs));
        expect(elapsed).toBeGreaterThanOrEqual(5000);
        // This is the existing bounded event-observation window, not a new
        // product grace: the actual close allowance remains absolute 5000ms.
        expect(elapsed).toBeLessThan(10000);
        expect(rejected.watchdogs).toHaveLength(1);
        expect(rejected.watchdogs[0].closed).toBe(false);
        expect(exists(watchdog.pid)).toBe(true);
        expect(host.events.filter(value => value.event === 'watchdog-spawned')).toHaveLength(1);
      } finally {
        try { if (exists(watchdog.pid)) process.kill(watchdog.pid, 'SIGCONT'); }
        catch (error) { host.fail(`owned watchdog resume: ${String(error).slice(0, 256)}`); }
      }
      await host.event('watchdog-closed');
      host.child.stdin!.end(); await host.event('stdin-ended'); host.command('release-preparation'); await host.close();
      expect(host.exit).toEqual({ code: 1, signal: null }); // declared incomplete, never a success inference
      expect(host.events.filter(value => value.event === 'progress-acquired')).toEqual([]);
      expect(host.events.filter(value => value.event === 'watchdog-spawned')).toHaveLength(1);
      expect(host.events.filter(value => value.event === 'factory-entered')).toEqual([]);
    });
  }, 120000);

  it.runIf(process.platform !== 'win32').each(['abort', 'eof'] as const)('fences a pending real watchdog replacement on %s without admitting late index progress', async ending => {
    await nativeCase(`progress-pending-${ending}`, async (host, roots) => {
      host.send('init', 'initialize', init()); await host.reply('init');
      const watchdog = await host.event('watchdog-spawned');
      await host.acquireWatchdogIdentity(watchdog.pid);
      // Real POSIX stop, not a fabricated close promise or patched kill. The
      // pending actual-close wait cannot finish until this owned child resumes.
      process.kill(watchdog.pid, 'SIGSTOP');
      try {
        host.command('progress-acquire', { name: 'pending', root: roots[0] });
        await host.event('progress-registering');
        if (ending === 'abort') {
          host.command('progress-abort', { name: 'pending' });
          await host.event('progress-aborted'); // actual abort precedes native resume
          // Same stdin static reply keeps the real outer parser responsive.
          host.send('barrier', 'tools/list'); await host.reply('barrier');
        } else {
          host.child.stdin!.end(); await host.event('stdin-ended');
        }
      } finally {
        try { if (exists(watchdog.pid)) process.kill(watchdog.pid, 'SIGCONT'); }
        catch (error) { host.fail(`owned watchdog resume: ${String(error).slice(0, 256)}`); }
      }
      const rejected = await host.event('progress-rejected'); expect(rejected.message).toContain('cancelled');
      if (ending === 'abort') {
        await host.acquireWatchdogIdentity(host.events.filter(value => value.event === 'watchdog-spawned').at(-1)!.pid);
        host.child.stdin!.end(); await host.event('stdin-ended');
      }
      host.command('release-preparation'); await host.close();
      expect(host.events.filter(value => value.event === 'progress-acquired')).toEqual([]);
      expect(host.events.filter(value => value.event === 'watchdog-spawned').every(value => value.paths.length === 0)).toBe(true);
      if (ending === 'eof') expect(host.events.filter(value => value.event === 'watchdog-spawned')).toHaveLength(1);
      expect(host.events.filter(value => value.event === 'factory-entered')).toEqual([]);
    });
  }, 120000);

  it.runIf(process.platform !== 'win32').each([
    ['signal-preparation', 'SIGTERM'], ['signal-factory', 'SIGINT'],
  ] as const)('joins one outer %s cleanup lifetime with native %s', async (scenario, signal) => {
    await nativeCase(scenario, async host => {
      host.send(1, 'initialize', init()); await host.reply(1);
      if (scenario === 'signal-factory') { host.command('release-preparation'); await host.event('factory-awaiting-resource-stop'); }
      expect(host.child.kill(signal)).toBe(true);
      await host.event('preparation-aborted');
      if (scenario === 'signal-preparation') host.command('release-preparation');
      await host.event('resources-stopped'); await host.close();
      expect(host.events.filter(value => value.event === 'engine-created')).toHaveLength(scenario === 'signal-factory' ? 1 : 0);
      expect(host.events.filter(value => value.event === 'engine-initialized')).toHaveLength(scenario === 'signal-factory' ? 1 : 0);
      expect(host.events.filter(value => value.event === 'session-transport-closed')).toHaveLength(scenario === 'signal-factory' ? 1 : 0);
      expect(host.events.filter(value => value.event === 'resources-stopped')).toHaveLength(1);
    });
  }, 120000);
});
