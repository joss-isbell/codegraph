import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { createHash } from 'crypto';
import { CodeGraph } from '../src';
import { MAX_HELLO_LINE_BYTES } from '../src/mcp/daemon';
import { getDaemonSocketCandidates } from '../src/mcp/daemon-paths';
import { until, wait } from './query-compatibility-host';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { UnprovedClient, UnprovedTrace } from './query-unproved-host';
import { finishFraming, framingOwners, framingSpawns, requireFramingTrace, type FramingCleanup } from './query-hello-framing-host';

const modes = ['ascii-terminated', 'utf8-terminated', 'utf8-prefix-dangling', 'utf8-boundary', 'crlf-boundary', 'crlf-overflow', 'utf8-tail-split'] as const;
type Mode = typeof modes[number];
const opaqueTail = Buffer.from('{"jsonrpc":"2.0","method":"notifications/codegraph_fixture_opaque_framing_v1"}\n');
const utf8Tail = Buffer.from('{"jsonrpc":"2.0","method":"notifications/codegraph_fixture_utf8_tail_v1","params":{"marker":"é"}}\n');
const sha256 = (bytes: Buffer): string => createHash('sha256').update(bytes).digest('hex');
const events = (values: any[], name: string): any[] => values.filter(value => value.event === name);
const one = (values: any[], name: string): any => {
  expect(events(values, name), 'exact actual ' + name).toHaveLength(1); return events(values, name)[0];
};
const positive = (mode: Mode): boolean => mode === 'utf8-boundary' || mode === 'crlf-boundary' || mode === 'utf8-tail-split';
const expectedOpaque = (mode: Mode): Buffer => mode === 'utf8-tail-split' ? utf8Tail : opaqueTail;
const attachment = (input: any[]): any => one(input.filter(value => value.purpose === 'attachment'), 'actual-native-socket-observer-installed');
/** Roles come from the executed preparation/acquisition seams, never ordinals.
 * Every observed inspection remains accountable, not discarded as extra noise. */
function requireSocketLifecycle(input: any[], requesterPid: number, endpoint: string, final = false): any {
  const installed = events(input, 'actual-native-socket-observer-installed'), selected = attachment(input);
  expect(installed.every(value => ['prepare-admission', 'attempt-admission', 'attachment'].includes(value.purpose)),
    'every observed connection has a recognized acquisition role').toBe(true);
  expect(new Set(installed.map(value => value.socketId)).size).toBe(installed.length);
  for (const value of installed) {
    expect(value).toMatchObject({ pid: requesterPid, endpoint });
    expect(value.stack).toContain('connectObservedService');
    expect(value.stack).toContain(value.purpose === 'attachment' ? 'connectHeldService' : 'admitQueryProject');
    const own = input.filter(event => event.socketId === value.socketId);
    expect(one(own, 'actual-requester-socket-native-connect')).toMatchObject({ pid: requesterPid, endpoint });
    expect(one(own, 'actual-hello-timer-installed')).toMatchObject({ delay: 3000, purpose: value.purpose, endpoint });
    expect(events(own, 'actual-hello-timer-fired')).toEqual([]);
    if (final || value.purpose !== 'attachment') expect(one(own, 'actual-requester-socket-native-close')).toMatchObject({ hadError: false, destroyed: true });
  }
  expect(events(input, 'actual-requester-socket-native-connect').map(value => value.socketId).sort())
    .toEqual(installed.map(value => value.socketId).sort());
  expect(events(input, 'actual-hello-timer-installed').map(value => value.socketId).sort())
    .toEqual(installed.map(value => value.socketId).sort());
  if (final) expect(events(input, 'actual-requester-socket-native-close').map(value => value.socketId).sort())
    .toEqual(installed.map(value => value.socketId).sort());
  return selected;
}

async function indexed(root: string): Promise<void> {
  fs.mkdirSync(root);
  fs.writeFileSync(path.join(root, 'app.ts'), 'export function frameFirst() { return 81; }\nexport function frameBuffered() { return 82; }\nexport function frameNew() { return 83; }\nexport function ownerFirst() { return 84; }\nexport function ownerSurvives() { return 85; }\n');
  const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
}
function result(reply: any, query: string): void {
  expect(reply.error).toBeUndefined(); expect(reply.result.isError).toBeFalsy();
  expect(reply.result.content[0].text).toContain(query);
  expect(reply.result.content[0].text).toContain('return ');
}
function refusal(reply: any, root: string): void {
  expect(reply.error).toMatchObject({ code: -32603,
    data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root } });
  expect(reply.error.data.guidance).toContain(root); expect(reply.result).toBeUndefined();
}

/** Preconditions prove the actual received frame, independently of writes or
 * a fabricated positive hello. The dangling row intentionally has no LF until
 * the old parser incorrectly permits its controlled continuation. */
function requireNativeFrame(mode: Mode, daemon: any[], input: any[], before: ReturnType<typeof parseQueryDiagnostics>, requesterPid: number, root: string): Record<string, unknown> {
  const decorated = one(daemon, 'actual-decorated-hello'), frame = Buffer.from(decorated.frameBase64, 'base64');
  const selected = attachment(input), socketId = selected.socketId, target = input.filter(value => value.socketId === socketId);
  const nl = frame.indexOf(10), original = Buffer.from(decorated.originalBase64, 'base64');
  const actual = JSON.parse(original.toString('utf8')), modified = JSON.parse(frame.subarray(0, nl).toString('utf8'));
  expect(decorated).toMatchObject({ mode, requesterPid, socketId, purpose: 'attachment', authorityUnchanged: true });
  expect(sha256(frame)).toBe(decorated.frameSha256); expect(sha256(original)).toBe(decorated.originalSha256);
  delete modified.fixturePadding; expect(modified).toEqual(actual);
  expect(actual.pid).toBe(before.ownership.pid); expect(actual.protocol).toBe(1);
  expect(decorated.pid).toBe(actual.pid);
  expect(actual.codegraph).toBe(before.implementation.packageVersion);
  expect(getDaemonSocketCandidates(root)).toContain(actual.socketPath);
  expect(actual.queryExecution.ownership).toEqual(before.ownership);
  expect(actual.queryExecution.effective).toEqual(before.effective);
  expect(actual.queryExecution.implementation).toEqual(before.implementation);
  expect(decorated.owners.daemon).toMatchObject({ kind: 'live', value: { pid: before.ownership.pid, socketPath: actual.socketPath } });
  expect(decorated.owners.writer).toMatchObject({ kind: 'live', value: { pid: before.ownership.pid } });
  requireSocketLifecycle(input, requesterPid, actual.socketPath);
  const armed = one(input, 'actual-attachment-decoration-armed');
  expect(armed).toMatchObject({ pid: requesterPid, socketId, purpose: 'attachment', endpoint: actual.socketPath });
  expect(BigInt(armed.monotonicNs)).toBeGreaterThan(BigInt(one(input, 'actual-acquisition-released-after-buffered-input').monotonicNs));
  for (const inspection of events(input, 'actual-native-socket-observer-installed').filter(value => value.socketId !== socketId)) {
    const chunks = events(input.filter(value => value.socketId === inspection.socketId), 'actual-predecoder-chunk');
    expect(chunks.length).toBeGreaterThan(0);
    expect(chunks.every(value => Buffer.from(value.base64, 'base64').length === value.bytes)).toBe(true);
    const received = Buffer.concat(chunks.map(value => Buffer.from(value.base64, 'base64')));
    expect(received.at(-1)).toBe(10); expect(received.indexOf(10)).toBe(received.length - 1);
    const hello = JSON.parse(received.toString('utf8')); expect(hello.fixturePadding).toBeUndefined();
    expect(hello).toMatchObject({ codegraph: actual.codegraph, pid: actual.pid, protocol: actual.protocol, socketPath: actual.socketPath,
      queryExecution: { implementation: actual.queryExecution.implementation, requested: actual.queryExecution.requested,
        effective: actual.queryExecution.effective, ownership: actual.queryExecution.ownership, project: actual.queryExecution.project } });
    expect(events(daemon, 'actual-unmodified-hello').some(value => value.sha256 === sha256(received)
      && value.originalBase64 === received.toString('base64') && value.reason === 'not-armed')).toBe(true);
  }
  expect(one(input, 'actual-acquisition-released-after-buffered-input').consumed).toEqual([100, 101]);
  expect(events(input, 'actual-actionable-input-consumed').map(value => value.id)).toEqual([100, 101, 102]);
  expect(one(target, 'actual-hello-timer-installed').delay).toBe(3000);
  expect(events(input, 'actual-hello-timer-fired')).toEqual([]);
  const chunks = events(target, 'actual-predecoder-chunk'); expect(chunks.length).toBeGreaterThan(0);
  let received = Buffer.concat(chunks.map(value => Buffer.from(value.base64, 'base64')));
  // Each observed native chunk is retained completely in this tiny fixture.
  expect(chunks.every(value => Buffer.from(value.base64, 'base64').length === value.bytes)).toBe(true);
  expect(chunks.every(value => value.pid === requesterPid && value.socketId === socketId)).toBe(true);
  if (mode === 'utf8-tail-split') {
    const native = events(target, 'actual-tail-native-chunk');
    expect(native.every(value => value.pid === requesterPid && value.socketId === socketId
      && Buffer.from(value.base64, 'base64').length === value.bytes)).toBe(true);
    const all = Buffer.concat(native.map(value => Buffer.from(value.base64, 'base64')));
    received = all.subarray(0, frame.length);
    expect(received, 'complete literal post-LF UTF8 notification at the actual native receiver').toEqual(frame);
    expect(nl).toBe(4096); expect(frame.subarray(nl + 1)).toEqual(utf8Tail);
    expect(frame[decorated.split - 1]).toBe(195); expect(frame[decorated.split]).toBe(169);
    const terminal = chunks.find(value => value.lfOffset >= 0);
    expect(terminal, 'actual received hello LF with held post-LF C3 prefix').toBeDefined();
    expect(Buffer.from(terminal.tailBase64, 'base64')).toEqual(frame.subarray(nl + 1, decorated.split));
    expect(Buffer.from(terminal.base64, 'base64').at(-1)).toBe(195);
    let nativeOffset = 0;
    const nativeEnds = native.map(value => nativeOffset += Buffer.from(value.base64, 'base64').length);
    const prefixNativeIndex = nativeEnds.indexOf(decorated.split);
    expect(prefixNativeIndex, 'actual native chunk ending at the exact held frame split').toBeGreaterThanOrEqual(0);
    const prefixNative = native[prefixNativeIndex];
    expect(Buffer.from(prefixNative.base64, 'base64'), 'same terminal hello LF and post-LF C3 chunk')
      .toEqual(Buffer.from(terminal.base64, 'base64'));
    const continuationNative = native[prefixNativeIndex + 1]; expect(continuationNative).toBeDefined();
    expect(Buffer.from(continuationNative.base64, 'base64')[0]).toBe(169);
    const calls = events(target, 'actual-tail-set-encoding-return');
    expect(calls.length).toBeGreaterThan(0); expect(calls.length).toBeLessThanOrEqual(2);
    expect(calls[0]).toMatchObject({ encoding: 'utf8', call: 1, sawLf: true });
    expect(calls[0].stack).toContain('connectObservedService');
    expect(BigInt(prefixNative.monotonicNs)).toBeLessThan(BigInt(calls[0].monotonicNs));
    for (const value of calls.slice(1)) { expect(value.encoding).toBe('utf8'); expect(value.stack).toContain('attachDaemon'); }
    const registered = one(input, 'actual-tail-consumer-registered'), released = one(input, 'actual-tail-consumer-barrier-released');
    expect(registered).toMatchObject({ root, requesterPid, socketId, rawBytes: decorated.split, sawLf: true, encodingCalls: calls.length });
    expect(registered.stack).toContain('attachDaemon');
    expect(BigInt(registered.monotonicNs)).toBeGreaterThan(BigInt(calls.at(-1).monotonicNs));
    expect(BigInt(released.monotonicNs)).toBeGreaterThan(BigInt(registered.monotonicNs));
    expect(BigInt(continuationNative.monotonicNs)).toBeGreaterThan(BigInt(released.monotonicNs));
    const forwarded = events(input, 'actual-tail-actionable-forwarded');
    expect(forwarded.map(value => value.id)).toEqual([100, 101, 102]);
    for (const value of forwarded) {
      expect(value.consumerBarrierReleased).toBe(true);
      expect(BigInt(value.monotonicNs)).toBeGreaterThan(BigInt(released.monotonicNs));
    }
    const consumerAck = one(daemon, 'actual-tail-consumer-acknowledgement').ack;
    expect(consumerAck).toMatchObject({ root, requesterPid, socketId, rawBytes: decorated.split, sawLf: true });
    expect(one(daemon, 'actual-prefix-acknowledgement').ack).toMatchObject({ requesterPid, socketId, rawBytes: decorated.split, sawLf: true });
    const continuation = one(daemon, 'held-continuation-released'); expect(continuation.firstByte).toBe(169);
    const completed = events(daemon, 'native-frame-write-complete');
    expect(completed.map(value => value.phase)).toEqual(['prefix', 'continuation']);
    expect(completed.every(value => value.error === null)).toBe(true);
    expect(completed.reduce((sum, value) => sum + value.bytes, 0)).toBe(frame.length);
    expect(one(daemon, 'actual-tail-consumer-release-written').continuationSha256).toBe(sha256(frame.subarray(decorated.split)));
    expect(released.release).toMatchObject({ socketId, phase: 'actual-native-continuation-write-completed', continuationSha256: sha256(frame.subarray(decorated.split)) });
    expect(BigInt(registered.monotonicNs)).toBeLessThan(BigInt(consumerAck.monotonicNs));
  } else if (mode === 'utf8-prefix-dangling') {
    const prefix = one(daemon, 'native-prefix-submitted');
    expect(prefix).toMatchObject({ bytes: 4097, lastByte: 195, hasLf: false });
    expect(frame.subarray(0, 4097).includes(10)).toBe(false); expect(frame[4096]).toBe(195); expect(frame[4097]).toBe(169);
    expect(nl).toBe(4100); expect(frame.subarray(0, nl).toString('utf8').length).toBeLessThan(4096);
    expect(received.subarray(0, 4097)).toEqual(frame.subarray(0, 4097));
    expect(received).toEqual(events(daemon, 'held-continuation-released').length ? frame : frame.subarray(0, 4097));
    const ack = one(daemon, 'actual-prefix-acknowledgement');
    expect(ack.ack).toMatchObject({ requesterPid, rawBytes: 4097, sawLf: false });
    expect(events(daemon, 'native-frame-write-complete').find(value => value.phase === 'prefix')).toMatchObject({ bytes: 4097, error: null });
  } else {
    expect(received, 'exact decorated frame received by the intended native requester').toEqual(frame);
    const terminal = chunks.find(value => value.lfOffset >= 0); expect(terminal, 'native received hello LF').toBeDefined();
    expect(Buffer.from(terminal.rawFrameBase64, 'base64')).toEqual(frame.subarray(0, nl + 1));
    expect(terminal.rawPreLfBytes).toBe(nl);
    if (!positive(mode)) {
      const crossing = chunks.find(value => value.rawPreLfBytes > 4096);
      expect(crossing, 'actual raw receive first crosses budget').toBeDefined();
      expect(crossing.lfOffset, 'overshoot must arrive with LF, not an earlier unterminated rejection').toBeGreaterThanOrEqual(0);
      expect(chunks.filter(value => value.lfOffset < 0).every(value => value.rawPreLfBytes <= 4096)).toBe(true);
    }
    if (mode === 'ascii-terminated') {
      expect(nl).toBe(4097); expect(frame.subarray(0, nl).every(byte => byte < 128)).toBe(true);
      expect(one(daemon, 'native-prefix-submitted')).toMatchObject({ bytes: 4096, hasLf: false });
      expect(one(daemon, 'actual-prefix-acknowledgement').ack).toMatchObject({ requesterPid, rawBytes: 4096, sawLf: false });
    }
    if (mode === 'utf8-terminated') {
      expect(nl).toBe(4097); expect(frame.subarray(0, nl).toString('utf8')).toContain('é');
      expect(frame.subarray(0, nl).toString('utf8').length).toBeLessThan(4096);
    }
    if (mode === 'utf8-boundary') {
      expect(nl).toBe(4096); expect(frame[decorated.split - 1]).toBe(195); expect(frame[decorated.split]).toBe(169);
      expect(one(daemon, 'native-prefix-submitted').bytes).toBe(decorated.split);
      const prefixRead = chunks.find(value => value.rawPreLfBytes === decorated.split && value.lfOffset < 0);
      expect(prefixRead, 'actual receive stops inside the literal UTF8 codepoint').toBeDefined();
      expect(Buffer.from(prefixRead.rawFrameBase64, 'base64').at(-1)).toBe(195);
      const continuationRead = chunks[chunks.indexOf(prefixRead) + 1]; expect(continuationRead).toBeDefined();
      expect(Buffer.from(continuationRead.base64, 'base64')[0]).toBe(169);
      expect(one(daemon, 'held-continuation-released').firstByte).toBe(169);
    }
    if (mode.startsWith('crlf-')) {
      expect(nl).toBe(mode === 'crlf-boundary' ? 4096 : 4097); expect(frame[nl - 1]).toBe(13);
      expect(frame.subarray(0, nl - 1).length).toBe(mode === 'crlf-boundary' ? 4095 : 4096);
    }
    if (positive(mode)) {
      expect(frame.subarray(nl + 1)).toEqual(opaqueTail);
      expect(Buffer.from(terminal.tailBase64, 'base64'), 'opaque notification glued to actual hello native receive').toEqual(opaqueTail);
    } else expect(frame.subarray(nl + 1)).toEqual(Buffer.alloc(0));
    expect(events(daemon, 'native-frame-write-complete').every(value => value.error === null)).toBe(true);
    expect(events(daemon, 'native-frame-write-complete').reduce((sum, value) => sum + value.bytes, 0)).toBe(frame.length);
  }
  const delegated = events(daemon, 'actual-unmodified-hello').filter(value => value.reason === 'one-shot-consumed');
  expect(delegated.length, 'later actual admission/healthy greetings delegate unchanged').toBeGreaterThan(0);
  for (const value of delegated) {
    const bytes = Buffer.from(value.originalBase64, 'base64');
    expect(sha256(bytes)).toBe(value.sha256); expect(bytes.length).toBe(value.bytes);
    const hello = JSON.parse(bytes.toString('utf8')); expect(hello.fixturePadding).toBeUndefined();
    expect(hello).toEqual(value.hello); expect(hello.pid).toBe(actual.pid); expect(hello.socketPath).toBe(actual.socketPath);
  }
  return { requesterPid, socketId, daemonPid: actual.pid, root, socketPath: actual.socketPath,
    transmittedSha256: sha256(frame), receivedSha256: sha256(received), receivedBytes: received.length,
    expectedReceivedPrefixSha256: sha256(frame.subarray(0, received.length)), completeFrameReceived: received.equals(frame) };
}

function requireByteRefusalCausality(daemon: any[], input: any[]): void {
  const socketId = attachment(input).socketId, target = input.filter(value => value.socketId === socketId);
  const close = one(target, 'actual-requester-socket-native-close'), timer = one(target, 'actual-hello-timer-installed');
  const withheld = one(daemon, 'continuation-withheld-after-native-byte-refusal');
  expect(close).toMatchObject({ hadError: false, rawBytes: 4097, sawLf: false, destroyed: true });
  expect(BigInt(close.monotonicNs)).toBeLessThan(BigInt(timer.installedNs) + 3000000000n);
  expect(events(input, 'actual-hello-timer-fired')).toEqual([]);
  expect(events(target, 'actual-native-read-EOF').filter(value => !value.destroyed)).toEqual([]);
  expect(events(daemon, 'held-continuation-released')).toEqual([]);
  expect(events(daemon, 'native-prefix-still-open-before-continuation')).toEqual([]);
  expect(withheld.close).toMatchObject({ requesterPid: close.pid, socketId, rawBytes: 4097, sawLf: false, hadError: false });
  expect(BigInt(withheld.monotonicNs)).toBeGreaterThanOrEqual(BigInt(close.monotonicNs));
  const frame = Buffer.from(one(daemon, 'actual-decorated-hello').frameBase64, 'base64');
  expect(Buffer.from(withheld.continuationBase64, 'base64')).toEqual(frame.subarray(4097));
}

function requireHistory(workers: any[], mode: Mode, daemonPid: number, generation: string): void {
  const expected = positive(mode) ? ['ownerFirst', 'ownerSurvives', 'frameFirst', 'frameBuffered', 'frameNew'] : ['ownerFirst', 'ownerSurvives'];
  expect(events(workers, 'call-start').map(value => value.query).sort()).toEqual([...expected].sort());
  expect(events(workers, 'spawn').every(value => value.pid === daemonPid && value.execution.generation === generation)).toBe(true);
  const history = events(workers, 'session-history'); expect(history.map(value => value.query).sort()).toEqual([...expected].sort());
  const owner = history.filter(value => value.query.startsWith('owner'));
  expect(owner.map(value => ({ query: value.query, index: value.index }))).toEqual([{ query: 'ownerFirst', index: 1 }, { query: 'ownerSurvives', index: 2 }]);
  expect(new Set(owner.map(value => value.historyId)).size).toBe(1);
  const requester = history.filter(value => value.query.startsWith('frame'));
  if (positive(mode)) {
    expect(requester.map(value => value.index).sort()).toEqual([1, 2, 3]);
    expect(new Set(requester.map(value => value.historyId)).size).toBe(1);
    expect(requester[0].historyId).not.toBe(owner[0].historyId);
  } else expect(requester).toEqual([]);
}

describe('actual shared-service hello has a raw 4096-byte pre-LF budget', () => {
  it.each(modes)('preserves actual authority and enforces %s framing', async mode => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-hello-bytes-'))), root = path.join(fixture, 'project');
    const workerFile = path.join(fixture, 'workers.jsonl'), daemonFile = path.join(fixture, 'daemon-framing.jsonl'), inputFile = path.join(fixture, 'requester-framing.jsonl');
    const control = path.join(fixture, 'armed.json'), acknowledgement = path.join(fixture, 'prefix-ack.json'), closeAcknowledgement = path.join(fixture, 'close-ack.json');
    const workers = new UnprovedTrace(workerFile), daemon = new UnprovedTrace(daemonFile), input = new UnprovedTrace(inputFile), traces = [workers, daemon, input];
    const clients: UnprovedClient[] = [], cleanup: FramingCleanup = [], evidence: Record<string, unknown> = {
      mode, budget: { maximum: 4096, unit: 'raw bytes before first LF; optional CR counted; LF and opaque tail excluded', timeoutMs: 3000 },
      seam: 'actual built daemon appends only ignored padding; predecoder Socket.push and existing parser data callback delegate unchanged; one armed requester after owner attachment',
      opaqueTail: positive(mode) ? { base64: expectedOpaque(mode).toString('base64'), purpose: 'Controlled opaque framing evidence only; no authority, query, result or diagnostic claim' } : null,
      control, acknowledgement, closeAcknowledgement,
    };
    let primary: unknown, before: ReturnType<typeof parseQueryDiagnostics> | undefined;
    let peer: UnprovedClient | undefined;
    const final = (): void => {
      const observed = input.read(true), framed = daemon.read(true);
      requireFramingTrace(input, observed); requireFramingTrace(daemon, framed);
      const selected = attachment(observed), installed = events(observed, 'actual-native-socket-observer-installed');
      expect(one(observed, 'requester-observer-final')).toMatchObject({ sockets: installed.length, targetSocketId: selected.socketId, firstError: null });
      expect(one(framed, 'daemon-observer-final')).toMatchObject({ decorated: 1, firstError: null });
      expect(one(observed, 'actual-input-EOF')).toMatchObject({ partialBytes: 0, firstError: null });
      requireSocketLifecycle(observed, peer!.child.pid!, selected.endpoint, true);
      expect(one(framed, 'decorated-socket-native-close').hadError).toBe(false);
      expect(one(observed, 'requester-factory-observer-installed')).toMatchObject({ branch: 'prepared-deps', root });
      expect(events(observed, 'requester-factory-invoked')).toEqual([]);
      if (!before || !peer) throw new Error('Framing prefix did not establish owned actual service/requester');
      requireNativeFrame(mode, framed, observed, before, peer.child.pid!, root);
      if (mode === 'utf8-prefix-dangling') requireByteRefusalCausality(framed, observed);
      requireHistory(workers.read(true), mode, before.ownership.pid, before.ownership.generation);
      for (const id of [100, 101, 102]) expect(peer.values.filter(value => value.id === id)).toHaveLength(1);
      if (positive(mode)) expect(peer.text('stdout').split(expectedOpaque(mode).toString('utf8')).length - 1).toBe(1);
      else expect(peer.text('stdout')).not.toContain('notifications/codegraph_fixture_opaque_framing_v1');
    };
    try {
      await indexed(root); expect(MAX_HELLO_LINE_BYTES).toBe(4096);
      const common: Record<string, string> = { QUERY_UNLIMITED_OBSERVATIONS: workerFile, QUERY_UNLIMITED_RELEASE: path.join(fixture, 'unused-worker-release'),
        QUERY_HELLO_FRAMING_ROOT: root, QUERY_HELLO_FRAMING_CONTROL: control, QUERY_HELLO_FRAMING_ACK: acknowledgement,
        QUERY_HELLO_FRAMING_CLOSE_ACK: closeAcknowledgement,
        ...(mode === 'utf8-tail-split' ? { QUERY_HELLO_FRAMING_MODE: mode,
          QUERY_HELLO_FRAMING_CONSUMER_ACK: path.join(fixture, 'consumer-ack.json'),
          QUERY_HELLO_FRAMING_CONSUMER_RELEASE: path.join(fixture, 'consumer-release.json') } : {}) };
      if (mode === 'utf8-tail-split') evidence.utf8TailBarrier = { root,
        acknowledgement: common.QUERY_HELLO_FRAMING_CONSUMER_ACK, release: common.QUERY_HELLO_FRAMING_CONSUMER_RELEASE };
      const owner = UnprovedClient.cli(root, 'framing-owner-' + mode, { ...common, QUERY_HELLO_FRAMING_ROLE: 'owner', QUERY_HELLO_FRAMING_TRACE: daemonFile },
        ['query-unlimited-preload.cjs', 'query-hello-framing-preload.cjs']);
      clients.push(owner); await owner.initialize();
      const ownerFirst = await owner.tool('ownerFirst', 100); result(ownerFirst, 'ownerFirst');
      before = parseQueryDiagnostics((await owner.request('tools/call', { name: 'codegraph_status', arguments: {} }, 3)).result);
      const locks = framingOwners(root), candidates = framingSpawns(root);
      expect(locks.every(value => value.error === null && value.text !== null)).toBe(true);
      expect(candidates.error).toBeNull(); expect(candidates.text).toBe(before.ownership.pid + '\n');
      Object.assign(evidence, { before, locks, candidates, ownerFirst });
      peer = UnprovedClient.cli(root, 'framing-requester-' + mode, { ...common, QUERY_HELLO_FRAMING_ROLE: 'requester', QUERY_HELLO_FRAMING_TRACE: inputFile },
        ['query-unlimited-preload.cjs', 'query-hello-framing-preload.cjs']);
      clients.unshift(peer);
      if (mode === 'utf8-tail-split') (evidence.utf8TailBarrier as Record<string, unknown>).requesterPid = peer.child.pid;
      await peer.initialize();
      const armed = { root, mode, requesterPid: peer.child.pid };
      evidence.armed = armed;
      fs.writeFileSync(control + '.request', JSON.stringify(armed) + '\n');
      const waiting = Promise.all([peer.tool('frameFirst', 100), peer.tool('frameBuffered', 101, root)]); void waiting.catch(() => {});
      await until(() => {
        const values = input.read(); requireFramingTrace(input, values);
        return [100, 101].every(id => events(values, 'actual-actionable-input-consumed').some(value => value.id === id));
      }, 10000);
      const replies = await waiting, next = await peer.tool('frameNew', 102, root);
      evidence.replies = [...replies, next];
      await until(() => {
        const values = daemon.read(); requireFramingTrace(daemon, values);
        if (mode === 'utf8-prefix-dangling') return values.some(value => value.event === 'continuation-withheld-after-native-byte-refusal' || value.event === 'held-continuation-released');
        return values.some(value => value.event === 'native-frame-write-complete' && value.phase === (mode === 'ascii-terminated' || mode === 'utf8-boundary' || mode === 'utf8-tail-split' ? 'continuation' : 'whole'));
      }, 10000);
      // Retain the actual owner's complete continued work even when the
      // intended negative assertions expose the old byte parser's bypass.
      fs.renameSync(control, control + '.retained'); evidence.retainedArmingControl = control + '.retained';
      const ownerSurvives = await owner.tool('ownerSurvives', 101);
      const after = parseQueryDiagnostics((await owner.request('tools/call', { name: 'codegraph_status', arguments: {} }, 4)).result);
      Object.assign(evidence, { ownerSurvives, after, ownersAtResponse: framingOwners(root), spawnsAtResponse: framingSpawns(root) });
      result(ownerSurvives, 'ownerSurvives'); expect(after.ownership).toEqual(before.ownership); expect(after.effective).toEqual(before.effective);
      expect(framingOwners(root)).toEqual(locks); expect(framingSpawns(root)).toEqual(candidates);
      const framed = daemon.read(), observed = input.read(); requireFramingTrace(daemon, framed); requireFramingTrace(input, observed);
      const socketsAtResponse = events(observed, 'actual-native-socket-observer-installed');
      evidence.frameCorrelation = requireNativeFrame(mode, framed, observed, before, peer.child.pid!, root); evidence.nativeFramePreconditionsPassed = true;
      if (positive(mode)) {
        [replies[0], replies[1], next].forEach((reply, index) => result(reply, ['frameFirst', 'frameBuffered', 'frameNew'][index]));
        if (mode === 'utf8-tail-split') evidence.nativeUtf8TailCausalityPassed = true;
        evidence.opaqueTailRelay = { expectedBase64: expectedOpaque(mode).toString('base64'),
          exactOccurrences: peer.text('stdout').split(expectedOpaque(mode).toString('utf8')).length - 1,
          replacementCharacterObserved: peer.text('stdout').includes('\uFFFD') };
        expect(peer.text('stdout').split(expectedOpaque(mode).toString('utf8')).length - 1).toBe(1);
        if (mode === 'utf8-tail-split') {
          expect(peer.text('stdout')).not.toContain('\uFFFD');
          expect(events(observed.filter(value => value.socketId === attachment(observed).socketId), 'actual-tail-set-encoding-return')).toHaveLength(1);
        }
      } else {
        for (const reply of [...replies, next]) refusal(reply, root);
        if (mode === 'utf8-prefix-dangling') requireByteRefusalCausality(framed, observed);
      }
      await wait(350); // greater than three configured100ms retry intervals
      expect(events(input.read(), 'actual-native-socket-observer-installed')).toEqual(socketsAtResponse);
      expect(events(input.read(), 'requester-factory-invoked')).toEqual([]);
      expect(framingOwners(root)).toEqual(locks); expect(framingSpawns(root)).toEqual(candidates);
      requireHistory(workers.read(), mode, before.ownership.pid, before.ownership.generation);
      expect(peer.text('stderr')).not.toContain('serving this session in-process'); expect(peer.text('stderr')).not.toContain('Serving reads in-process');
      expect(peer.text('stderr')).not.toContain('[CodeGraph MCP] Stopped');
    } catch (error) { primary = error; throw error; }
    finally { await finishFraming('compatibility-hello-framing-' + mode, root, fixture, control, evidence, clients, traces, workers, cleanup, primary, final); }
  }, 180000);
});
