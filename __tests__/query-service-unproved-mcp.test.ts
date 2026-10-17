import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { createHash } from 'crypto';
import { CodeGraph } from '../src';
import { MAX_HELLO_LINE_BYTES } from '../src/mcp/daemon';
import { getDaemonSocketCandidates } from '../src/mcp/daemon-paths';
import { spawnLogFor } from './daemon-candidates';
import { alive, until, wait, saveCompatibilityEvidence } from './query-compatibility-host';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { UnprovedClient, UnprovedTrace } from './query-unproved-host';

/** Small record reads also fail closed and remain nonthrowing in final evidence. */
function record(file: string): { text: string | null; seenBytes: number; error: string | null } {
  let fd: number | undefined, seenBytes = 0, error: string | null = null, text: string | null = null;
  try {
    fd = fs.openSync(file, 'r'); const stat = fs.fstatSync(fd); seenBytes = stat.size;
    if (!stat.isFile()) throw new Error('Owned record is not a regular file');
    const prefix = Buffer.alloc(Math.min(seenBytes, 65536));
    const read = fs.readSync(fd, prefix, 0, prefix.length, 0); text = prefix.subarray(0, read).toString('utf8');
    if (seenBytes > 65536 || read !== seenBytes) error = 'Owned record exceeds cap or was incompletely read';
  } catch (value) { if ((value as NodeJS.ErrnoException).code !== 'ENOENT') error = String(value).slice(0, 2048); }
  finally { if (fd !== undefined) try { fs.closeSync(fd); } catch (value) { error ??= String(value).slice(0, 2048); } }
  return { text, seenBytes, error };
}
const owners = (root: string) => ['daemon.pid', 'writer.pid'].map(name => record(path.join(root, '.codegraph', name)));
const spawns = (root: string) => record(spawnLogFor(root));
const requireAbsentOwners = (root: string): void => { expect(owners(root)).toEqual([{ text: null, seenBytes: 0, error: null }, { text: null, seenBytes: 0, error: null }]); };
const requireRefusal = (reply: any, root: string): void => {
  expect(reply.error).toMatchObject({ code: -32603, data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root } });
  expect(reply.error.data.guidance).toContain(root); expect(reply.result).toBeUndefined();
};
async function index(root: string): Promise<void> {
  fs.mkdirSync(root); fs.writeFileSync(path.join(root, 'app.ts'),
    'export function refusedFirst() { return 71; }\nexport function refusedBuffered() { return 72; }\nexport function refusedNew() { return 73; }\nexport function ownerFirst() { return 74; }\nexport function ownerSurvives() { return 75; }\n');
  const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
}
type Cleanup = Array<{ phase: string; error: string | null }>;
async function attempt(cleanup: Cleanup, phase: string, action: () => unknown | Promise<unknown>): Promise<void> {
  try { await action(); cleanup.push({ phase, error: null }); } catch (error) { cleanup.push({ phase, error: String(error).slice(0, 8192) }); }
}
/** This new proof never interprets an unreadable, partial or malformed spawn
 * record as an empty candidate set. Signal zero observes presence only. */
function candidatePids(raw: ReturnType<typeof record>): number[] {
  if (raw.error) throw new Error(raw.error);
  if (raw.text === null) return [];
  if (!/^(?:[1-9]\d*\r?\n)*$/.test(raw.text)) throw new Error('Malformed owned candidate PID record');
  const pids = raw.text.split(/\r?\n/).filter(Boolean).map(Number);
  if (pids.some(pid => !Number.isSafeInteger(pid) || pid <= 0)) throw new Error('Invalid owned candidate PID');
  return pids;
}
async function requireCandidateAbsence(root: string): Promise<void> {
  await until(() => {
    const pids = candidatePids(spawns(root));
    let absent = true;
    for (const pid of pids) {
      try { process.kill(pid, 0); absent = false; }
      catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        if (code === 'EPERM') absent = false;
        else if (code !== 'ESRCH') throw new Error('Unknown owned candidate presence: ' + String(error).slice(0, 2048));
      }
    }
    return absent;
  }, 30000);
}
function requireObservedSockets(values: any[], final = false): void {
  const events = (name: string): any[] => values.filter(value => value.event === name);
  const installed = events('actual-observed-socket-installed');
  expect(installed.length, 'actual IO trace must observe its native connection').toBeGreaterThan(0);
  const verified = events('actual-verified-socket');
  expect(installed.map(value => value.purpose).sort()).toEqual(verified.length
    ? ['prepare-admission', 'attempt-admission', 'attachment'].sort() : ['prepare-admission']);
  expect(new Set(installed.map(value => value.socketId)).size).toBe(installed.length);
  for (const identity of installed) {
    expect(identity.stack).toContain('connectObservedService');
    expect(identity.stack).toContain(identity.purpose === 'attachment' ? 'connectHeldService' : 'admitQueryProject');
    const own = (name: string): any[] => events(name).filter(value => value.socketId === identity.socketId);
    expect(own('actual-observed-socket-connect')).toEqual([expect.objectContaining({ endpoint: identity.endpoint, purpose: identity.purpose })]);
    if (final || identity.purpose !== 'attachment') expect(own('actual-observed-socket-close'))
      .toEqual([expect.objectContaining({ hadError: false, destroyed: true, endpoint: identity.endpoint, purpose: identity.purpose })]);
    const chunks = own('actual-observed-socket-hello'); let raw = Buffer.alloc(0);
    for (const chunk of chunks) {
      const bytes = Buffer.from(chunk.base64, 'base64'); expect(bytes.length).toBe(chunk.bytes);
      raw = Buffer.concat([raw, bytes]); expect(raw.length).toBe(chunk.totalBytes);
      expect(createHash('sha256').update(raw).digest('hex')).toBe(chunk.sha256);
    }
    if (verified.length) {
      expect(raw.indexOf(10)).toBe(raw.length - 1); expect(raw.length).toBeGreaterThan(1);
      const hello = JSON.parse(raw.toString('utf8')), owner = verified[0].owners;
      expect(hello).toMatchObject({ pid: owner.daemon.value.pid, socketPath: identity.endpoint, protocol: 1 });
      expect(hello.queryExecution.ownership.pid).toBe(owner.writer.value.pid);
      expect(hello.fixturePadding).toBeUndefined();
    }
  }
  const ids = installed.map(value => value.socketId).sort();
  expect(events('actual-observed-socket-connect').map(value => value.socketId).sort()).toEqual(ids);
  if (final) {
    expect(events('actual-observed-socket-close').map(value => value.socketId).sort()).toEqual(ids);
    expect(events('returned-close-observer-final')).toEqual([expect.objectContaining({ sockets: installed.length, mutations: verified.length, firstError: null })]);
  }
  if (verified.length) {
    expect(verified).toHaveLength(1);
    const selected = installed.filter(value => value.purpose === 'attachment'); expect(selected).toHaveLength(1);
    const socketId = selected[0].socketId;
    for (const name of ['actual-verified-socket', 'actual-socket-close-release-observed', 'actual-socket-native-close', 'returned-destroyed-socket']) {
      expect(events(name)).toEqual([expect.objectContaining({ socketId, purpose: 'attachment', endpoint: selected[0].endpoint })]);
    }
    expect(events('actual-socket-close-release-observed')[0].consumed).toEqual([100, 101]);
    expect(events('actual-preparation-rejected')).toEqual([]);
    expect(events('fallback-prepared-deps-observer-installed')).toEqual([expect.objectContaining({ branch: 'prepared-deps' })]);
  } else {
    // The actual prepare throws before a proxy/factory can be published. The
    // constructor-independent worker trace is separately required to be empty.
    expect(events('actual-preparation-rejected')).toEqual([expect.objectContaining({ code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown' })]);
    expect(events('fallback-prepared-deps-observer-installed')).toEqual([]);
  }
}
function requireTrace(trace: UnprovedTrace, values: any[]): void {
  expect(trace.firstError).toBeNull(); expect(trace.truncated).toBe(false);
  const identity = (value: any) => value.pid + ':' + value.threadId;
  const spawned = values.filter(value => value.event === 'spawn').map(identity).sort();
  const exited = values.filter(value => value.event === 'exit');
  expect(exited.map(identity).sort()).toEqual(spawned);
  expect(exited.every(value => value.code === 0 || value.code === 1)).toBe(true);
  expect(values.filter(value => value.event === 'fixture-failure')).toEqual([]);
  expect(values.filter(value => value.event === 'fallback-factory-invoked')).toEqual([]);
  if (values.some(value => value.event === 'actual-verified-socket')) {
    for (const event of ['actual-verified-socket', 'actual-socket-native-close', 'returned-destroyed-socket']) {
      expect(values.filter(value => value.event === event)).toHaveLength(1);
    }
    expect(values.filter(value => value.event === 'owned-socket-failure-cleanup')).toEqual([]);
  }
  if (values.some(value => value.event === 'actual-actionable-input-consumed')) {
    expect(values.filter(value => value.event === 'fallback-factory-observer-installed')).toEqual([expect.objectContaining({ branch: 'prepare-entry' })]);
    expect(values.filter(value => value.event === 'actual-input-EOF')).toEqual([expect.objectContaining({ partialBytes: 0, firstError: null })]);
    requireObservedSockets(values, true);
  }
}
async function settle(root: string, clients: UnprovedClient[], traces: UnprovedTrace[], cleanup: Cleanup): Promise<void> {
  for (const client of clients) await attempt(cleanup, 'native-close-' + client.name, () => client.close());
  await attempt(cleanup, 'all-candidates-exited', () => requireCandidateAbsence(root));
  for (const trace of traces) await attempt(cleanup, 'final-trace-' + traces.indexOf(trace), () => {
    requireTrace(trace, trace.read(true));
  });
}
async function bufferedRequests(peer: UnprovedClient, root: string, io: UnprovedTrace, release: string, evidence: Record<string, unknown>): Promise<any[]> {
  const waiting = Promise.all([peer.tool('refusedFirst', 100), peer.tool('refusedBuffered', 101, root)]);
  // Attach immediately: a fixture barrier failure must not create an unhandled
  // rejection later when cleanup closes the actual child with pending calls.
  void waiting.catch(() => {});
  await until(() => {
    const values = io.read();
    if (io.firstError) throw new Error(io.firstError);
    const failure = values.find(value => value.event === 'fixture-failure');
    if (failure) throw new Error(failure.error);
    return [100, 101].every(id => values.some(value => value.event === 'actual-actionable-input-consumed' && value.id === id));
  }, 10000);
  evidence.consumedBeforeRelease = io.read().filter(value => value.event === 'actual-actionable-input-consumed');
  fs.writeFileSync(release, 'Owned fixture release after both actual actionable inputs were consumed\n');
  return waiting;
}
function save(name: string, root: string, fixture: string, evidence: object, clients: UnprovedClient[], traces: UnprovedTrace[], cleanup: Cleanup, primary: unknown): void {
  // Native cleanup has already run. Raw fixture/index/trace/spawn files stay
  // available for every outcome, including an evidence-storage failure. There
  // is no post-persistence deletion that can erase a newly discovered failure.
  const failPersistence = (phase: string, error: unknown): void => {
    cleanup.push({ phase, error: String(error).slice(0, 8192) });
    // Bounded fallback survives a failed JSON write and never replaces the
    // primary assertion. The original raw fixture and spawn log remain intact.
    try {
      const fallback = JSON.stringify({ name, fixture, spawnLog: spawnLogFor(root),
        primary: primary instanceof Error ? primary.stack : String(primary ?? ''), cleanup: cleanup.filter(value => value.error) }).slice(0, 16384);
      process.stderr.write('[owned-transport-evidence-failure] ' + fallback + '\n');
    } catch { /* retain raw files and original failure even if fallback storage also fails */ }
  };
  try {
    const finalOwners = owners(root), finalSpawns = spawns(root);
    const finalTraces = traces.map(trace => ({ values: trace.read(true), facts: trace.facts() }));
    for (const [index, value] of [...finalOwners, finalSpawns].entries()) {
      cleanup.push({ phase: 'final-record-' + index, error: value.error });
    }
    try { candidatePids(finalSpawns); cleanup.push({ phase: 'final-candidate-record-schema', error: null }); }
    catch (error) { cleanup.push({ phase: 'final-candidate-record-schema', error: String(error).slice(0, 8192) }); }
    traces.forEach((trace, index) => {
      try { requireTrace(trace, finalTraces[index].values); cleanup.push({ phase: 'persisted-trace-' + index, error: null }); }
      catch (error) { cleanup.push({ phase: 'persisted-trace-' + index, error: String(error).slice(0, 8192) }); }
    });
    cleanup.push({ phase: 'raw-fixture-and-spawn-evidence-retained', error: null });
    const retained = { root, fixture, spawnLog: spawnLogFor(root), retention: {
      disposition: 'Raw disposable fixture/index/trace/spawn files intentionally retained; native resource cleanup is separately asserted and recorded',
      fixtureRoot: fixture, spawnLog: spawnLogFor(root), traceFiles: finalTraces.map(value => (value.facts as { file: string }).file),
    }, ...evidence,
      primary: primary instanceof Error ? primary.stack : String(primary ?? ''), ownersAfterCleanup: finalOwners,
      spawns: finalSpawns, cleanup, clients: clients.map(client => client.facts()), traces: finalTraces };
    try {
      if (!process.env.VALIDATION_OUT) throw new Error('Qualified evidence output is unavailable');
      saveCompatibilityEvidence(name, retained);
    } catch (error) { failPersistence('final-evidence-persistence', error); }
  } catch (error) { failPersistence('final-evidence-snapshot', error); }
  const failed = cleanup.filter(value => value.error);
  if (failed.length && primary === undefined) throw new Error('Owned transport final evidence/cleanup incomplete: ' + JSON.stringify(failed));
}

describe('unproved transport observation cannot become unavailable', () => {
  it.each(['malformed', 'oversized', 'no-hello'] as const)('refuses initial connected %s peer before spawn or local replay', async mode => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-unproved-'))), root = path.join(fixture, 'project');
    const observations = path.join(fixture, 'workers.jsonl'), trace = new UnprovedTrace(observations);
    const ioFile = path.join(fixture, 'input.jsonl'), io = new UnprovedTrace(ioFile), traces = [trace, io], release = path.join(fixture, 'release');
    const clients: UnprovedClient[] = [], cleanup: Cleanup = [], evidence: Record<string, unknown> = { mode, configuredLimit: MAX_HELLO_LINE_BYTES, configuredHelloTimeoutMs: 3000 };
    let primary: unknown, negative: UnprovedClient | undefined;
    try {
      await index(root); expect(MAX_HELLO_LINE_BYTES).toBe(4096); requireAbsentOwners(root);
      const endpoints = getDaemonSocketCandidates(root); evidence.endpoints = endpoints;
      negative = new UnprovedClient('negative-' + mode, root, [path.join(__dirname, 'fixtures/query-unproved-peer.cjs'), JSON.stringify(endpoints), mode, '4096', release], { ...process.env });
      clients.push(negative); await negative.event('ready');
      const peer = UnprovedClient.cli(root, 'requesting-' + mode, { QUERY_UNLIMITED_OBSERVATIONS: observations,
        QUERY_UNLIMITED_RELEASE: path.join(fixture, 'unused-release'), QUERY_UNPROVED_SOCKET_OBSERVATIONS: ioFile },
      ['query-unlimited-preload.cjs', 'query-returned-socket-close.cjs']);
      clients.unshift(peer); await peer.initialize();
      const replies = await bufferedRequests(peer, root, io, release, evidence);
      const next = await peer.tool('refusedNew', 102, root); evidence.replies = [...replies, next];
      const connection = await negative.event('connected-probe'); expect(connection.observation).toBe(mode);
      expect(alive(negative.child.pid!)).toBe(true); evidence.ownersAtResponse = owners(root);
      for (const reply of [...replies, next]) requireRefusal(reply, root);
      await wait(350); // more than three configured100ms retry intervals
      expect(negative.values.filter(value => value.event === 'connected-probe')).toHaveLength(1);
      requireAbsentOwners(root); expect(spawns(root)).toEqual({ text: null, seenBytes: 0, error: null });
      for (const id of [100, 101, 102]) expect(peer.values.filter(value => value.id === id)).toHaveLength(1);
      expect(trace.read()).toEqual([]); expect(trace.firstError).toBeNull();
      expect(io.read().filter(value => value.event === 'actual-actionable-input-consumed').map(value => value.id)).toEqual([100, 101, 102]);
      expect(io.read().filter(value => value.event === 'fallback-factory-invoked')).toEqual([]);
      expect(peer.text('stderr')).not.toContain('serving this session in-process');
      expect(peer.text('stderr')).not.toContain('Serving reads in-process');
      expect(peer.text('stderr')).not.toContain('[CodeGraph MCP] Stopped');
      expect(peer.firstError).toBeNull(); expect(negative.firstError).toBeNull();
    } catch (error) { primary = error; throw error; }
    finally {
      try {
        await attempt(cleanup, 'release-negative-delivery', () => fs.writeFileSync(release, 'Owned cleanup release\n'));
        await settle(root, clients, traces, cleanup);
      }
      finally {
        if (negative) await attempt(cleanup, 'negative-final-observation', () => {
          const final = negative!.values.find(value => value.event === 'owned-close');
          expect(final).toBeDefined(); expect(final.snapshot.errors).toEqual([]); expect(final.snapshot.listening).toBe(0);
          expect(final.snapshot.connections).toHaveLength(1);
          expect(final.snapshot.connections.every((value: any) => value.closed)).toBe(true);
          const first = final.snapshot.connections[0];
          expect(first.observation).toBe(mode); expect(first.receivedBytes).toBe(0);
          if (mode === 'malformed') {
            const literal = Buffer.from('{unproved codegraph hello}\n');
            expect(first.emittedBytes).toBe(literal.length); expect(first.emittedPrefixBase64).toBe(literal.toString('base64'));
            expect(first.delivery).toEqual(expect.objectContaining({ completed: true, error: null }));
            expect(negative!.values.filter(value => value.event === 'payload-delivered' && value.connection === first.id)).toEqual([
              expect.objectContaining({ connection: first.id, endpoint: first.endpoint, observation: 'malformed', emittedBytes: literal.length,
                emittedPrefixBase64: literal.toString('base64') }),
            ]);
          }
          if (mode === 'oversized') {
            expect(first.emittedBytes).toBe(4097); expect(first.emittedPrefixBase64).toBe(Buffer.alloc(64, 120).toString('base64'));
            expect(first.delivery).toEqual(expect.objectContaining({ completed: true, error: null }));
          }
          if (mode === 'no-hello') { expect(first.emittedBytes).toBe(0); expect(BigInt(first.closedNs) - BigInt(first.acceptedNs)).toBeGreaterThanOrEqual(2900000000n); }
        });
        await attempt(cleanup, 'negative-final-no-worker-work', () => { expect(trace.read(true)).toEqual([]); });
        save('compatibility-unproved-' + mode, root, fixture, evidence, clients, traces, cleanup, primary);
      }
    }
  }, 120000);

  it('refuses an actually verified socket returned after native close, preserving the healthy real owner', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-returned-close-'))), root = path.join(fixture, 'project');
    const observations = path.join(fixture, 'workers.jsonl'), seamFile = path.join(fixture, 'socket-seam.jsonl'), release = path.join(fixture, 'release');
    const trace = new UnprovedTrace(observations), seam = new UnprovedTrace(seamFile), traces = [trace, seam];
    const clients: UnprovedClient[] = [], cleanup: Cleanup = [], evidence: Record<string, unknown> = { controlledSeam: 'same actual verified prepared.deps.getDaemonSocket attachment result closed before dependency return; admission sockets untouched' };
    let primary: unknown, daemonPid: number | undefined;
    try {
      await index(root);
      const env = { QUERY_UNLIMITED_OBSERVATIONS: observations, QUERY_UNLIMITED_RELEASE: path.join(fixture, 'unused-release') };
      const owner = UnprovedClient.cli(root, 'healthy-actual-owner', env, ['query-unlimited-preload.cjs']);
      clients.push(owner); await owner.initialize();
      const first = await owner.tool('ownerFirst', 100); expect(first.error).toBeUndefined(); expect(first.result.content[0].text).toContain('ownerFirst');
      const before = parseQueryDiagnostics((await owner.request('tools/call', { name: 'codegraph_status', arguments: {} }, 3)).result);
      daemonPid = before.ownership.pid; const locks = owners(root), candidates = spawns(root);
      expect(locks.every(value => value.error === null && typeof value.text === 'string')).toBe(true);
      evidence.before = before; evidence.locks = locks; evidence.ownerFirst = first;
      const peer = UnprovedClient.cli(root, 'returned-closed-requester', { ...env, QUERY_UNPROVED_RETURNED_SOCKET_ROOT: root,
        QUERY_UNPROVED_SOCKET_OBSERVATIONS: seamFile, QUERY_UNPROVED_RELEASE: release }, ['query-unlimited-preload.cjs', 'query-returned-socket-close.cjs']);
      clients.unshift(peer); await peer.initialize();
      const replies = await bufferedRequests(peer, root, seam, release, evidence);
      const next = await peer.tool('refusedNew', 102, root); evidence.replies = [...replies, next];
      // Collect actual owner's post-seam query/history even for intended red.
      const healthy = await owner.tool('ownerSurvives', 101);
      const after = parseQueryDiagnostics((await owner.request('tools/call', { name: 'codegraph_status', arguments: {} }, 4)).result);
      Object.assign(evidence, { healthy, after });
      const events = seam.read();
      expect(events.find(value => value.event === 'actual-verified-socket')).toMatchObject({ pid: peer.child.pid,
        owners: { daemon: expect.objectContaining({ kind: 'live', value: expect.objectContaining({ pid: daemonPid }) }),
          writer: expect.objectContaining({ kind: 'live', value: expect.objectContaining({ pid: daemonPid }) }) } });
      expect(events.find(value => value.event === 'actual-socket-native-close')).toMatchObject({ hadError: false, destroyed: true });
      expect(events.find(value => value.event === 'returned-destroyed-socket')).toMatchObject({ destroyed: true });
      expect(events.filter(value => value.event === 'actual-actionable-input-consumed').map(value => value.id)).toEqual([100, 101, 102]);
      expect(healthy.error).toBeUndefined(); expect(healthy.result.content[0].text).toContain('ownerSurvives');
      expect(after.ownership).toEqual(before.ownership); expect(after.effective).toEqual(before.effective);
      expect(owners(root)).toEqual(locks); expect(spawns(root)).toEqual(candidates); expect(alive(daemonPid!)).toBe(true);
      for (const reply of [...replies, next]) requireRefusal(reply, root);
      await wait(350); // terminal refusal cannot arm the configured100ms retry
      const finalSeam = seam.read();
      requireObservedSockets(finalSeam);
      for (const name of ['actual-verified-socket', 'actual-socket-native-close', 'returned-destroyed-socket']) expect(finalSeam.filter(value => value.event === name)).toHaveLength(1);
      for (const id of [100, 101, 102]) expect(peer.values.filter(value => value.id === id)).toHaveLength(1);
      expect(finalSeam.filter(value => value.event === 'fallback-factory-invoked')).toEqual([]);
      expect(peer.text('stderr')).not.toContain('Serving reads in-process'); expect(peer.text('stderr')).not.toContain('serving this session in-process');
      const workers = trace.read();
      expect(workers.filter(value => value.event === 'call-start').map(value => value.query)).toEqual(['ownerFirst', 'ownerSurvives']);
      expect(workers.filter(value => value.event === 'spawn').every(value => value.pid === daemonPid && value.execution.generation === before.ownership.generation)).toBe(true);
      const history = workers.filter(value => value.event === 'session-history');
      expect(history.map(value => ({ query: value.query, index: value.index }))).toEqual([{ query: 'ownerFirst', index: 1 }, { query: 'ownerSurvives', index: 2 }]);
      expect(new Set(history.map(value => value.historyId)).size).toBe(1); expect(trace.firstError).toBeNull(); expect(seam.firstError).toBeNull();
    } catch (error) { primary = error; throw error; }
    finally {
      try {
        await attempt(cleanup, 'release-returned-socket-close', () => fs.writeFileSync(release, 'Owned cleanup release\n'));
        await settle(root, clients, [], cleanup);
        if (daemonPid) await attempt(cleanup, 'healthy-daemon-native-exit', () => until(() => !alive(daemonPid!), 20000));
        await settle(root, [], traces, cleanup);
        await attempt(cleanup, 'final-healthy-history-and-worker-work', () => {
          const workers = trace.read(true), before = evidence.before as ReturnType<typeof parseQueryDiagnostics>;
          expect(workers.filter(value => value.event === 'call-start').map(value => value.query)).toEqual(['ownerFirst', 'ownerSurvives']);
          expect(workers.filter(value => value.event === 'spawn').every(value => value.pid === daemonPid && value.execution.generation === before.ownership.generation)).toBe(true);
          const history = workers.filter(value => value.event === 'session-history');
          expect(history.map(value => ({ query: value.query, index: value.index }))).toEqual([{ query: 'ownerFirst', index: 1 }, { query: 'ownerSurvives', index: 2 }]);
          expect(new Set(history.map(value => value.historyId)).size).toBe(1);
        });
      } finally { save('compatibility-returned-socket-close', root, fixture, evidence, clients, traces, cleanup, primary); }
    }
  }, 120000);
});
