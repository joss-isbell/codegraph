import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { CodeGraph } from '../src';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { alive, until, saveCompatibilityEvidence } from './query-compatibility-host';

/** Bounded byte prefixes plus native close facts survive every assertion path. */
class OwnedHost {
  readonly child: ChildProcessWithoutNullStreams;
  readonly events: any[] = [];
  readonly capture = { stdout: { seenBytes: 0, retainedBytes: 0, truncated: false }, stderr: { seenBytes: 0, retainedBytes: 0, truncated: false } };
  readonly prefixes = { stdout: Buffer.alloc(8 * 1024 * 1024), stderr: Buffer.alloc(1024 * 1024) };
  firstError: string | null = null;
  nativeClose: { exitCode: number | null; signal: NodeJS.Signals | null } | null = null;
  private parsedBytes = 0;
  constructor(readonly role: string, workspace: string, missingRoot: string, release: string, observations: string, maximum = '1') {
    this.child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, '--require', path.join(__dirname, 'fixtures/query-unlimited-preload.cjs'),
      path.join(__dirname, 'fixtures/query-shared-lease-host.cjs'), role, workspace, missingRoot, release],
    { cwd: workspace, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env,
      CODEGRAPH_QUERY_MODE: undefined, CODEGRAPH_QUERY_POOL_SIZE: maximum, CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
      CODEGRAPH_PROJECT_IDLE_TIMEOUT_MS: '0', CODEGRAPH_DAEMON_INSTALL_CHECK_MS: '0', CODEGRAPH_DAEMON_SWEEP_MS: '0',
      CODEGRAPH_NO_WATCHDOG: '1', CODEGRAPH_PPID_POLL_MS: '0', CODEGRAPH_MCP_LOG_ATTACH: '1',
      QUERY_UNLIMITED_OBSERVATIONS: observations, QUERY_UNLIMITED_RELEASE: path.join(workspace, 'unused-worker-release') } });
    const fail = (error: unknown) => { this.firstError ??= String(error); };
    this.child.on('error', fail); this.child.stdin.on('error', fail);
    for (const stream of ['stdout', 'stderr'] as const) {
      this.child[stream].on('error', fail);
      this.child[stream].on('data', (chunk: Buffer) => {
        const state = this.capture[stream], prefix = this.prefixes[stream];
        state.seenBytes += chunk.length;
        const count = Math.min(chunk.length, prefix.length - state.retainedBytes);
        chunk.copy(prefix, state.retainedBytes, 0, count); state.retainedBytes += count;
        if (count < chunk.length) { state.truncated = true; fail('Owned ' + role + ' ' + stream + ' capture overflow'); }
        if (stream === 'stdout') {
          let end: number;
          while ((end = prefix.indexOf(10, this.parsedBytes)) >= 0 && end < state.retainedBytes) {
            const line = prefix.subarray(this.parsedBytes, end).toString('utf8'); this.parsedBytes = end + 1;
            try { this.events.push(JSON.parse(line)); } catch (error) { fail('Owned fixture output parse: ' + error); }
          }
        }
      });
    }
    this.child.on('close', (exitCode, signal) => { this.nativeClose = { exitCode, signal }; });
  }
  text(stream: 'stdout' | 'stderr'): string { return this.prefixes[stream].subarray(0, this.capture[stream].retainedBytes).toString('utf8'); }
  async event(name: string, token?: string): Promise<any> {
    await until(() => {
      if (this.firstError) throw new Error(this.firstError);
      const failed = this.events.find(value => value.event === 'fixture-failure');
      if (failed) throw new Error(failed.error);
      if (this.nativeClose) throw new Error('Owned ' + this.role + ' exited before ' + name + ': ' + this.text('stderr'));
      return this.events.some(value => value.event === name && (token === undefined || value.token === token));
    }, 40000, () => this.text('stderr'));
    return this.events.find(value => value.event === name && (token === undefined || value.token === token));
  }
  command(value: object): void { this.child.stdin.write(JSON.stringify(value) + '\n'); }
  async call(engine: string, query: string, projectPath: string | undefined, id: number): Promise<any> {
    const token = engine + '-' + id;
    this.command({ kind: 'tool', token, engine, id, args: { query, ...(projectPath ? { projectPath } : {}) } });
    return this.event('reply', token);
  }
  async close(): Promise<void> {
    if (!this.nativeClose) this.child.stdin.end();
    await until(() => !!this.nativeClose, 35000, () => this.text('stderr'));
    expect(this.nativeClose).toEqual({ exitCode: 0, signal: null });
    expect(this.firstError).toBeNull();
    expect(this.capture.stdout.truncated || this.capture.stderr.truncated).toBe(false);
    expect(this.events.some(value => value.event === 'owned-close')).toBe(true);
    expect(this.events.filter(value => value.event === 'fixture-failure')).toEqual([]);
    const final = [...this.events].reverse().find(value => value.event === 'owned-closed' || value.event === 'owned-close');
    expect(final.snapshot.cleanupErrors).toEqual([]);
    for (const value of final.snapshot.engines) {
      expect(value.resourceErrors).toEqual([]);
      if (value.wire) expect(value.wire).toMatchObject({ firstError: null, pending: 0, closed: true });
    }
    if (final.snapshot.daemonProbe) expect(final.snapshot.daemonProbe).toMatchObject({ firstError: null, pending: 0, closed: true });
    expect(alive(this.child.pid!)).toBe(false);
  }
  facts(): object { return { role: this.role, pid: this.child.pid, nativeClose: this.nativeClose, firstError: this.firstError, capture: this.capture,
    stdout: this.text('stdout'), stderr: this.text('stderr'), events: this.events,
    prefixBase64: Object.fromEntries((['stdout', 'stderr'] as const).map(stream => [stream, this.prefixes[stream].subarray(0, this.capture[stream].retainedBytes).toString('base64')])) }; }
}

/** The unchanged native observer writes a disposable JSONL file. Read a fixed
 * prefix only; malformed/truncated evidence is sticky and never prevents the
 * finally block from retaining the raw prefix and all native cleanup facts. */
class BoundedTrace {
  private readonly prefix = Buffer.alloc(8 * 1024 * 1024);
  private retainedBytes = 0;
  private parsedBytes = 0;
  private readonly values: any[] = [];
  seenBytes = 0;
  firstError: string | null = null;
  truncated = false;
  constructor(private readonly file: string) {}
  read(final = false): any[] {
    let fd: number | undefined;
    try {
      fd = fs.openSync(this.file, 'r');
      const size = fs.fstatSync(fd).size;
      this.seenBytes = Math.max(this.seenBytes, size);
      if (size < this.retainedBytes) this.firstError ??= 'Owned worker trace shrank';
      if (size > this.prefix.length) { this.truncated = true; this.firstError ??= 'Owned worker trace exceeds byte cap'; }
      const available = Math.min(size, this.prefix.length) - this.retainedBytes;
      if (available > 0) this.retainedBytes += fs.readSync(fd, this.prefix, this.retainedBytes, available, this.retainedBytes);
      let end: number;
      while ((end = this.prefix.indexOf(10, this.parsedBytes)) >= 0 && end < this.retainedBytes) {
        const length = end - this.parsedBytes, start = this.parsedBytes; this.parsedBytes = end + 1;
        if (length > 65536 || this.values.length >= 10000) { this.firstError ??= 'Owned worker trace line/event cap'; continue; }
        if (!length) continue;
        try { this.values.push(JSON.parse(this.prefix.subarray(start, end).toString('utf8'))); }
        catch (error) { this.firstError ??= 'Owned worker trace JSON: ' + String(error); }
      }
      if (this.retainedBytes - this.parsedBytes > 65536) this.firstError ??= 'Owned worker trace unterminated line cap';
      if (final && this.retainedBytes !== this.parsedBytes) this.firstError ??= 'Owned worker trace has incomplete final line';
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT' || this.retainedBytes) this.firstError ??= 'Owned worker trace read: ' + String(error);
    } finally {
      if (fd !== undefined) { try { fs.closeSync(fd); } catch (error) { this.firstError ??= 'Owned worker trace close: ' + String(error); } }
    }
    return this.values;
  }
  facts(): object { return { seenBytes: this.seenBytes, retainedBytes: this.retainedBytes, parsedBytes: this.parsedBytes,
    truncated: this.truncated, firstError: this.firstError, prefixBase64: this.prefix.subarray(0, this.retainedBytes).toString('base64') }; }
}
const engineFact = (value: any, name: string) => value.snapshot.engines.find((engine: any) => engine.name === name);
const historyCount = (value: any, name: string, root: string) => engineFact(value, name).history.find((project: any) => project.projectRoot === root)?.callCount ?? 0;
const betaGraph = (value: any, root: string) => value.snapshot.graphs.filter((graph: any) => graph.root === root);
async function index(root: string, source: string) {
  fs.mkdirSync(root, { recursive: true }); fs.writeFileSync(path.join(root, 'app.ts'), source);
  const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
}

describe('caller admission with shared native freshness resources', () => {
  it('keeps A/P1 lease while beta becomes P2 and B/P2 independently queries the same process-global graph', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-leases-')));
    const alpha = path.join(fixture, 'alpha'), beta = path.join(fixture, 'beta'), release = path.join(fixture, 'release-restart');
    const observations = path.join(fixture, 'workers.jsonl'), trace = new BoundedTrace(observations), events = () => trace.read();
    const children: OwnedHost[] = [], cleanup: Array<{ phase: string; error: string | null }> = [];
    const evidence: Record<string, any> = { fixture, alpha, beta };
    let primary: unknown;
    try {
      await index(alpha, 'export function alphaSurvives() { return 501; }\n');
      await index(beta, ['First', 'Second', 'Third', 'AfterA', 'Refused'].map((suffix, i) => `export function beta${suffix}() { return ${601 + i}; }`).join('\n') + '\n');
      const peer1 = new OwnedHost('peer', beta, '', release, observations, '1'); children.push(peer1);
      const ready1 = await peer1.event('ready'); evidence.peer1Ready = ready1;
      expect(parseQueryDiagnostics(ready1.status.result).effective.concurrency).toEqual({ kind: 'finite', maximum: 1 });
      const host = new OwnedHost('shared', fixture, '', release, observations); children.push(host);
      const ready = await host.event('ready'); evidence.hostReady = ready;
      expect(ready.snapshot.modulePath).toBe(path.resolve(__dirname, '../dist/mcp/engine.js'));
      expect(ready.pid).toBe(host.child.pid);
      expect(ready.snapshot.engines.every((value: any) => value.diagnostics.ownership.pid === host.child.pid)).toBe(true);
      expect(ready.snapshot.engines.map((value: any) => value.loaded)).toEqual([false, false]);
      const generations = ready.snapshot.engines.map((value: any) => value.diagnostics.ownership.generation);
      expect(new Set(generations).size).toBe(2);
      const first = await host.call('A', 'betaFirst', beta, 100); evidence.first = first;
      expect(first.reply.error).toBeUndefined(); expect(first.reply.result.content[0].text).toContain('export function betaFirst()');
      expect(historyCount(first, 'A', beta)).toBe(1); expect(historyCount(first, 'B', beta)).toBe(0);
      expect(betaGraph(first, beta)).toEqual([{ root: beta, closeCalls: 0 }]);
      host.command({ kind: 'barrier', token: 'restart' });
      const held = await host.event('barrier-held', 'restart'); evidence.held = held;
      expect(betaGraph(held, beta)).toEqual([{ root: beta, closeCalls: 0 }]);
      expect(held.snapshot.connections.some((value: any) => !value.closed)).toBe(true);
      await peer1.close();
      const peer2 = new OwnedHost('peer', beta, '', release, observations, '2'); children.push(peer2);
      const ready2 = await peer2.event('ready'); evidence.peer2Ready = ready2;
      expect(peer2.child.pid).not.toBe(peer1.child.pid);
      const status1 = parseQueryDiagnostics(ready1.status.result), status2 = parseQueryDiagnostics(ready2.status.result);
      expect(status2.ownership.generation).not.toBe(status1.ownership.generation);
      expect(status2.effective.concurrency).toEqual({ kind: 'finite', maximum: 2 });
      expect(ready2.hello.queryExecution.ownership.pid).toBe(peer2.child.pid);
      expect(ready2.hello.queryExecution.effective.concurrency).toEqual({ kind: 'finite', maximum: 2 });
      const locks = ready2.locks;
      fs.writeFileSync(release, 'release only this owned sequencing barrier');
      const released = await host.event('barrier-released', 'restart'); evidence.released = released;
      const oldIds = held.snapshot.connections.filter((value: any) => !value.closed).map((value: any) => value.id);
      expect(released.snapshot.connections.filter((value: any) => oldIds.includes(value.id)).every((value: any) => value.closed)).toBe(true);
      const second = await host.call('B', 'betaSecond', beta, 200); evidence.second = second;
      expect(second.reply.error).toBeUndefined(); expect(second.reply.result.content[0].text).toContain('export function betaSecond()');
      expect(historyCount(second, 'B', beta)).toBe(1);
      expect(betaGraph(second, beta)).toEqual([{ root: beta, closeCalls: 0 }]);
      for (const id of [101, 102]) {
        const refused = await host.call('A', 'betaRefused', beta, id); evidence['refused' + id] = refused;
        expect(refused.reply.error).toMatchObject({ code: -32603, data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'incompatible', projectRoot: beta } });
        expect(refused.reply.error.data.guidance).toContain(beta);
        expect(engineFact(refused, 'A').wire.replies.filter((reply: any) => reply.id === id)).toHaveLength(1);
        expect(historyCount(refused, 'A', beta)).toBe(1);
      }
      const alphaSurvives = await host.call('A', 'alphaSurvives', alpha, 103); evidence.alphaSurvives = alphaSurvives;
      expect(alphaSurvives.reply.error).toBeUndefined(); expect(alphaSurvives.reply.result.content[0].text).toContain('alphaSurvives');
      const third = await host.call('B', 'betaThird', beta, 201); evidence.third = third;
      expect(third.reply.error).toBeUndefined(); expect(third.reply.result.content[0].text).toContain('betaThird');
      expect(historyCount(third, 'B', beta)).toBe(2); expect(historyCount(third, 'A', beta)).toBe(1);
      host.command({ kind: 'stop-engine', token: 'stop-A', engine: 'A' });
      const stoppedA = await host.event('engine-stopped', 'stop-A'); evidence.stoppedA = stoppedA;
      expect(betaGraph(stoppedA, beta)).toEqual([{ root: beta, closeCalls: 0 }]);
      const afterA = await host.call('B', 'betaAfterA', beta, 202); evidence.afterA = afterA;
      expect(afterA.reply.error).toBeUndefined(); expect(afterA.reply.result.content[0].text).toContain('betaAfterA');
      expect(historyCount(afterA, 'B', beta)).toBe(3);
      expect(betaGraph(afterA, beta)).toEqual([{ root: beta, closeCalls: 0 }]);
      expect(afterA.snapshot.engines.map((value: any) => value.diagnostics.ownership.generation)).toEqual(generations);
      expect(['daemon.pid', 'writer.pid'].map(name => fs.readFileSync(path.join(beta, '.codegraph', name), 'utf8'))).toEqual(locks);
      expect(alive(peer2.child.pid!)).toBe(true);
      const observed = events(), calls = observed.filter(value => value.event === 'call-start'); evidence.events = observed;
      expect(calls.map(value => value.query)).toEqual(['betaFirst', 'betaSecond', 'alphaSurvives', 'betaThird', 'betaAfterA']);
      expect(calls.every(value => value.pid === host.child.pid)).toBe(true);
      expect(calls.map(value => value.context.execution.generation)).toEqual([generations[0], generations[1], generations[0], generations[1], generations[1]]);
      expect(observed.filter(value => value.event === 'session-history')).toHaveLength(5);
      expect(new Set(observed.filter(value => value.event === 'session-history').map(value => value.historyId)).size).toBe(2);
      expect(observed.filter(value => value.event === 'spawn' && value.pid === host.child.pid)).toHaveLength(2);
      expect(observed.filter(value => value.event === 'spawn' && value.pid !== host.child.pid)).toHaveLength(0);
      expect(trace.firstError).toBeNull(); expect(trace.truncated).toBe(false);
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (phase: string, action: () => Promise<void>) => { try { await action(); cleanup.push({ phase, error: null }); } catch (error) { cleanup.push({ phase, error: String(error) }); } };
      try {
        await attempt('restart-barrier-release', async () => { fs.writeFileSync(release, 'unconditional owned cleanup release'); });
        // Host releases retained beta graph/links before the current owned peer.
        for (const child of [...children].reverse().sort((a, b) => Number(b.role === 'shared') - Number(a.role === 'shared'))) await attempt('native-close-' + child.role + '-' + child.child.pid, async () => {
          await child.close();
          if (child.role === 'shared' && !primary) expect(betaGraph(child.events.find(value => value.event === 'owned-closed'), beta)).toEqual([{ root: beta, closeCalls: 1 }]);
        });
        await attempt('all-query-workers-exited', async () => { const observed = events(); expect(observed.filter(value => value.event === 'exit')).toHaveLength(observed.filter(value => value.event === 'spawn').length);
          expect(observed.filter(value => value.event === 'exit').every(value => value.code === 0 || value.code === 1)).toBe(true); });
      } catch (error) {
        cleanup.push({ phase: 'unexpected-cleanup-error', error: String(error) });
      } finally {
        // Release/cleanup failure cannot bypass bounded raw evidence or replace
        // the original product/fixture failure with another filesystem error.
        const finalEvents = trace.read(true);
        if (trace.firstError || trace.truncated) cleanup.push({ phase: 'bounded-worker-trace', error: trace.firstError ?? 'truncated trace' });
        saveCompatibilityEvidence('compatibility-shared-lease', { ...evidence, events: finalEvents, trace: trace.facts(), primary: primary instanceof Error ? primary.stack : String(primary ?? ''), cleanup, children: children.map(child => child.facts()) });
      }
      const failures = cleanup.filter(value => value.error);
      if (!failures.length) fs.rmSync(fixture, { recursive: true, force: true });
      if (failures.length && !primary) throw new Error('Owned shared-lease cleanup incomplete: ' + JSON.stringify(failures));
    }
  }, 180000);

  it('returns recoverable own-index guidance when the actual loaded default database disappears without waking a worker', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-missing-'))), root = path.join(fixture, 'default');
    const observations = path.join(fixture, 'workers.jsonl'), trace = new BoundedTrace(observations), events = () => trace.read(), release = path.join(fixture, 'release');
    let host: OwnedHost | undefined, primary: unknown;
    const evidence: Record<string, any> = { root }, cleanup: Array<{ phase: string; error: string | null }> = [];
    try {
      await index(root, 'export function missingOwnSymbol() { return 701; }\n');
      host = new OwnedHost('missing', fixture, root, release, observations);
      const ready = await host.event('ready'); evidence.ready = ready;
      const before = engineFact(ready, 'C').diagnostics;
      expect(engineFact(ready, 'C').loaded).toBe(true);
      expect(before.lifecycle).toMatchObject({ state: 'cold', workers: 0, queued: 0, executing: 0, starting: 0 });
      expect(parseQueryDiagnostics(ready.status.result).lifecycle.workers).toBe(0);
      host.command({ kind: 'remove-loaded-index', token: 'remove' });
      const removed = await host.event('removed-loaded-index', 'remove'); evidence.removed = removed;
      expect(removed.fixtureClosedHandles).toEqual([
        { ordinal: 0, root, closeCallsBefore: 0, closeCallsAfter: 1 },
        { ordinal: 1, root, closeCallsBefore: 0, closeCallsAfter: 1 },
      ]);
      expect(betaGraph(removed, root)).toEqual([{ root, closeCalls: 1 }, { root, closeCalls: 1 }]);
      expect(removed.stillDefaultLoaded).toBe(true);
      expect(fs.existsSync(removed.database)).toBe(false);
      const reply = await host.call('C', 'missingOwnSymbol', undefined, 100); evidence.reply = reply;
      expect(reply.reply.error).toBeUndefined(); expect(reply.reply.result.isError).toBeFalsy();
      expect(reply.reply.result.content[0].text).toContain(root);
      expect(reply.reply.result.content[0].text).toContain('own codegraph index');
      expect(reply.reply.result.content[0].text).not.toMatch(/internal error|retry once|ENOENT|export function/i);
      expect(fs.existsSync(removed.database)).toBe(false);
      expect(engineFact(reply, 'C').diagnostics.lifecycle).toMatchObject({ state: 'cold', workers: 0, queued: 0, executing: 0, starting: 0 });
      expect(engineFact(reply, 'C').history).toEqual([]);
      const observed = events();
      expect(observed).toHaveLength(1);
      expect(observed[0]).toMatchObject({ event: 'engine-constructor', pid: host.child.pid, threadId: 0 });
      expect(trace.firstError).toBeNull(); expect(trace.truncated).toBe(false);
    } catch (error) { primary = error; throw error; }
    finally {
      try { if (host) await host.close(); cleanup.push({ phase: 'C-native-close', error: null }); } catch (error) { cleanup.push({ phase: 'C-native-close', error: String(error) }); }
      const finalEvents = trace.read(true);
      if (trace.firstError || trace.truncated) cleanup.push({ phase: 'bounded-worker-trace', error: trace.firstError ?? 'truncated trace' });
      saveCompatibilityEvidence('compatibility-missing-index', { ...evidence, events: finalEvents, trace: trace.facts(), primary: primary instanceof Error ? primary.stack : String(primary ?? ''), cleanup, child: host?.facts() });
      const failures = cleanup.filter(value => value.error);
      if (!failures.length) fs.rmSync(fixture, { recursive: true, force: true });
      if (failures.length && !primary) throw new Error('Owned missing-index cleanup incomplete: ' + JSON.stringify(failures));
    }
  }, 90000);
});
