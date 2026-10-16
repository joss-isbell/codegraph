import { describe, it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import { closeSync, existsSync, fstatSync, mkdtempSync, openSync, readSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { awaitDaemonBootstrap } from '../src/mcp/query-bootstrap';

const LIMIT_BYTES = 65_536;
const detail = (value: unknown) => {
  try { return Buffer.from(String(value), 'utf8').subarray(0, 4096).toString('utf8'); }
  catch { return 'Failure detail could not be formatted'; }
};

function capture(stream: NodeJS.ReadableStream) {
  const pieces: Buffer[] = [];
  let seenBytes = 0, retainedBytes = 0, failure: string | null = null, closed = false;
  stream.on('data', (chunk: Buffer | string) => {
    const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    seenBytes += bytes.length;
    const keep = Math.min(bytes.length, LIMIT_BYTES - retainedBytes);
    if (keep > 0) { pieces.push(Buffer.from(bytes.subarray(0, keep))); retainedBytes += keep; }
    if (seenBytes > LIMIT_BYTES) failure ??= 'native stream exceeded 65536 bytes';
  });
  stream.on('error', error => { failure ??= detail(error); });
  stream.on('close', () => { closed = true; });
  return () => ({ text: Buffer.concat(pieces, retainedBytes).toString('utf8'), seenBytes, retainedBytes,
    limitBytes: LIMIT_BYTES, truncated: seenBytes > retainedBytes, failure, closed });
}

function nativeCandidate(fixture = 'query-bootstrap-native.cjs', args: string[] = [], ipc = true, executable = process.execPath) {
  const child = spawn(executable, [path.join(__dirname, 'fixtures', fixture), ...args], {
    stdio: ipc ? ['pipe', 'pipe', 'pipe', 'ipc'] : ['pipe', 'pipe', 'pipe'], env: { ...process.env }, windowsHide: true,
  });
  const stdout = capture(child.stdout!), stderr = capture(child.stderr!);
  let stdinError: string | null = null, stdinClosed = child.stdin === null;
  child.stdin?.on('error', error => { stdinError ??= detail(error); });
  child.stdin?.on('close', () => { stdinClosed = true; });
  let begun = false, beginElapsedMs: number | null = null, observerStartedElapsedMs: number | null = null;
  let observerSettledElapsedMs: number | null = null;
  const begin = () => {
    if (begun || child.pid === undefined) return;
    begun = true;
    beginElapsedMs = performance.now() - started;
    try { child.stdin!.write('begin\n'); }
    catch (error) { stdinError ??= detail(error); throw error; }
  };
  const releaseInput = () => {
    if (!child.stdin || child.stdin.destroyed || child.stdin.writableEnded) return;
    try {
      if (!begun) child.stdin.end('cancel\n');
      else if (args[0] === 'held-refusal' || args[0] === 'live-disconnect') child.stdin.end('release\n');
      else child.stdin.end();
    } catch (error) { stdinError ??= detail(error); throw error; }
  };
  const statuses: Array<{ prefix: string; seenBytes: number; retainedBytes: number; truncated: boolean }> = [];
  let statusBytes = 0, statusFailure: string | null = null;
  child.on('message', value => {
    if (statuses.length >= 16) { statusFailure ??= 'native status observation count exceeded 16'; return; }
    const bytes = Buffer.from(JSON.stringify(value), 'utf8');
    const keep = Math.min(bytes.length, LIMIT_BYTES - statusBytes);
    statusBytes += keep;
    statuses.push({ prefix: bytes.subarray(0, keep).toString('utf8'), seenBytes: bytes.length,
      retainedBytes: keep, truncated: keep < bytes.length });
  });
  const started = performance.now();
  let exit: { exitCode: number | null; signal: NodeJS.Signals | null; elapsedMs: number } | null = null;
  let close: { exitCode: number | null; signal: NodeJS.Signals | null; elapsedMs: number } | null = null;
  let disconnect: { elapsedMs: number } | null = null;
  let spawnError: string | null = null;
  let absenceError: string | null = null;
  let acquisitionError: string | null = null;
  let nativeIdentity: { pid: number; creationIdentity: string; sampleElapsedSeconds: number; acquiredElapsedMs: number } | null = null;
  const absence = () => {
    if (child.pid === undefined) return spawnError !== null;
    try { process.kill(child.pid, 0); return false; }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code === 'ESRCH') return true;
      absenceError ??= detail(error); return false;
    }
  };
  child.on('error', error => { spawnError ??= detail(error); });
  child.on('exit', (exitCode, signal) => { exit = { exitCode, signal, elapsedMs: performance.now() - started }; });
  child.on('close', (exitCode, signal) => { close = { exitCode, signal, elapsedMs: performance.now() - started }; });
  child.on('disconnect', () => { disconnect = { elapsedMs: performance.now() - started }; });
  const acquire = async () => {
    try {
      if (child.pid === undefined) { await until(() => spawnError !== null, 'actual native spawn error'); return; }
      if (!process.env.VALIDATION_OUT) return;
      const resourceFile = path.join(process.env.VALIDATION_OUT, 'resources.jsonl');
      await until(() => {
        if (exit !== null || absence()) throw new Error('Native fixture exited before creation identity acquisition');
        let fd: number | undefined;
        try {
          fd = openSync(resourceFile, 'r');
          const size = fstatSync(fd).size;
          if (size > 1_048_576) throw new Error('Native census acquisition exceeded 1048576 bytes');
          const bytes = Buffer.alloc(size);
          let read = 0;
          while (read < size) {
            const count = readSync(fd, bytes, read, size - read, read);
            if (count === 0) throw new Error('Native census was truncated during acquisition');
            read += count;
          }
          const text = bytes.toString('utf8');
          for (const line of text.slice(0, text.lastIndexOf('\n') + 1).split('\n')) {
            if (!line) continue;
            const sample = JSON.parse(line) as { elapsedSeconds: number; processes: Array<{ pid: number; creationIdentity: string }> };
            for (const row of sample.processes) {
              if (row.pid !== child.pid) continue;
              if (typeof row.creationIdentity !== 'string' || !/^\d+$/.test(row.creationIdentity)
                || !Number.isFinite(sample.elapsedSeconds)) throw new Error('Owned native census identity is malformed');
              if (nativeIdentity && nativeIdentity.creationIdentity !== row.creationIdentity) throw new Error('Owned native PID was reused during acquisition');
              nativeIdentity ??= { pid: row.pid, creationIdentity: row.creationIdentity, sampleElapsedSeconds: sample.elapsedSeconds,
                acquiredElapsedMs: performance.now() - started };
            }
          }
          return nativeIdentity !== null;
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === 'ENOENT') return false;
          throw error;
        } finally { if (fd !== undefined) closeSync(fd); }
      }, 'exact owned native creation identity before protocol release');
    } catch (error) { acquisitionError ??= detail(error); throw error; }
  };
  return { child, stdout, stderr, acquire, begin, releaseInput,
    observerStarted: () => { observerStartedElapsedMs = performance.now() - started; },
    observerSettled: () => { observerSettledElapsedMs = performance.now() - started; },
    facts: () => ({ pid: child.pid, exit, close, disconnect, spawnError, stdinError, stdinClosed,
    begun, beginElapsedMs, observerStartedElapsedMs, observerSettledElapsedMs, nativeIdentity, acquisitionError,
    ipc, connected: child.connected, channelReleased: child.channel == null, pidAbsent: absence(), absenceError,
    statuses, statusFailure }) };
}

async function until(check: () => boolean, description: string, milliseconds = 5_000) {
  const deadline = performance.now() + milliseconds;
  while (!check()) {
    if (performance.now() >= deadline) throw new Error('Timed out waiting for ' + description);
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}

type NativeCandidate = ReturnType<typeof nativeCandidate>;

function nativeResourcesReleased(native: NativeCandidate) {
  const facts = native.facts();
  if (facts.absenceError) throw new Error(facts.absenceError);
  return (facts.exit !== null || (facts.pid === undefined && facts.spawnError !== null))
    && native.stdout().closed && native.stderr().closed && facts.stdinClosed
    && (!facts.ipc || facts.disconnect !== null) && !facts.connected && facts.channelReleased && facts.pidAbsent;
}

interface PhaseFailure { phase: string; detail: string }

async function closeOwned(native: NativeCandidate): Promise<PhaseFailure[]> {
  const failures: PhaseFailure[] = [];
  const attempt = (phase: string, action: () => void) => {
    try { action(); } catch (error) { failures.push({ phase, detail: detail(error) }); }
  };
  attempt('release-input', () => native.releaseInput());
  attempt('release-ipc', () => { if (native.child.connected) native.child.disconnect(); });
  try { await until(() => nativeResourcesReleased(native), 'owned native exit, pipe closes, IPC release and PID absence'); }
  catch (error) {
    failures.push({ phase: 'initial-native-release', detail: detail(error) });
    attempt('owned-cleanup-signal', () => {
      const facts = native.facts();
      if (facts.exit === null && !facts.pidAbsent && !facts.absenceError) native.child.kill('SIGKILL');
    });
    try { await until(() => nativeResourcesReleased(native), 'owned native resources after cleanup signal'); }
    catch (secondary) { failures.push({ phase: 'final-native-release', detail: detail(secondary) }); }
  }
  return failures;
}

async function finishNativeCase(name: string, native: NativeCandidate, projectRoot: string,
  observations: Record<string, unknown>, primaryFailure: string | null,
  expectedExit: { exitCode: number | null; signal: NodeJS.Signals | null } | null) {
  const failures: PhaseFailure[] = [];
  const attempt = (phase: string, action: () => void) => {
    try { action(); } catch (error) { failures.push({ phase, detail: detail(error) }); }
  };
  try { failures.push(...await closeOwned(native)); }
  catch (error) { failures.push({ phase: 'native-cleanup', detail: detail(error) }); }
  attempt('fixture-removal', () => {
    if (path.dirname(path.resolve(projectRoot)) !== path.resolve(tmpdir())
      || !path.basename(projectRoot).startsWith('codegraph-bootstrap-')) throw new Error('Unexpected owned fixture cleanup target');
    rmSync(projectRoot, { recursive: true, force: true });
  });
  // Each assertion and fact is independent: a secondary failure never skips the
  // remaining cleanup observations or replaces the original operation exception.
  attempt('assert-native-release', () => expect(nativeResourcesReleased(native)).toBe(true));
  if (expectedExit !== null) attempt('assert-native-exit', () => expect(native.facts().exit).toMatchObject(expectedExit));
  attempt('assert-stdin', () => expect(native.facts().stdinError).toBeNull());
  if (process.env.VALIDATION_OUT && native.child.pid !== undefined)
    attempt('assert-native-identity', () => expect(native.facts().nativeIdentity?.pid).toBe(native.child.pid));
  attempt('assert-acquisition', () => expect(native.facts().acquisitionError).toBeNull());
  attempt('assert-stdout-capture', () => expect(native.stdout().failure).toBeNull());
  attempt('assert-stderr-capture', () => expect(native.stderr().failure).toBeNull());
  attempt('assert-stderr-empty', () => expect(native.stderr().text).toBe(''));
  attempt('assert-status-capture', () => expect(native.facts().statusFailure).toBeNull());
  const evidence: Record<string, unknown> = { scenario: name, projectRoot, observations, primaryFailure,
    secondaryFailures: failures, fixtureRemoved: null, native: null, stdout: null, stderr: null };
  attempt('fixture-presence', () => { evidence.fixtureRemoved = !existsSync(projectRoot); });
  attempt('native-facts', () => { evidence.native = native.facts(); });
  attempt('stdout-facts', () => { evidence.stdout = native.stdout(); });
  attempt('stderr-facts', () => { evidence.stderr = native.stderr(); });
  evidence.cleanupFailure = failures.length ? 'Native cleanup/evidence failed; see secondaryFailures.' : null;
  if (process.env.VALIDATION_OUT) {
    attempt('evidence-persistence', () => writeFileSync(path.join(process.env.VALIDATION_OUT!, 'bootstrap-' + name + '-native.json'),
      JSON.stringify(evidence, null, 2)));
  }
  if (failures.length) {
    evidence.cleanupFailure = 'Native cleanup/evidence failed; see secondaryFailures.';
    attempt('bounded-stderr-fallback', () => {
      const bytes = Buffer.from(JSON.stringify(evidence), 'utf8');
      const prefix = bytes.subarray(0, LIMIT_BYTES - 512);
      const header = Buffer.from(JSON.stringify({ event: 'bootstrap-native-secondary-failure', scenario: name,
        seenBytes: bytes.length, retainedBytes: prefix.length, truncated: bytes.length > prefix.length }) + '\n');
      if (header.length > 511) throw new Error('Native fallback header exceeded its byte bound');
      process.stderr.write(Buffer.concat([header, prefix, Buffer.from('\n')]));
    });
    if (primaryFailure === null) throw new Error('Native cleanup/evidence failed in phases: ' + failures.map(item => item.phase).join(', '));
  }
}

function observeBootstrap(native: NativeCandidate, projectRoot: string) {
  native.observerStarted();
  const result = awaitDaemonBootstrap(native.child, projectRoot);
  native.begin();
  return result.then(value => { native.observerSettled(); return value; });
}

describe('guarded candidate bootstrap transport', () => {
  it('acknowledges a real permitted candidate before promotion and releases the IPC channel', async () => {
    const projectRoot = mkdtempSync(path.join(tmpdir(), 'codegraph-bootstrap-'));
    const native = nativeCandidate();
    let primaryFailure: string | null = null;
    let result: unknown = null;
    try {
      await native.acquire();
      result = await observeBootstrap(native, projectRoot);
      expect(result).toEqual({ kind: 'permitted', candidatePid: native.child.pid });
      await until(() => nativeResourcesReleased(native), 'permitted candidate native exit and resource closure');
      expect(native.stdout().text.trim().split(/\r?\n/).map(line => JSON.parse(line))).toEqual([
        { event: 'promoted', pid: native.child.pid },
      ]);
      expect(native.facts()).toMatchObject({ exit: { exitCode: 0, signal: null }, spawnError: null,
        connected: false, channelReleased: true, pidAbsent: true, absenceError: null });
      expect(native.stdout().failure).toBeNull(); expect(native.stderr().failure).toBeNull();
      expect(native.facts().statusFailure).toBeNull();
      expect(native.stderr().text).toBe('');
    } catch (error) { primaryFailure = detail(error); throw error; }
    finally {
      await finishNativeCase('permitted-native', native, projectRoot, { result }, primaryFailure, { exitCode: 0, signal: null });
    }
  }, 30_000);
});

type NativeOperation = (native: NativeCandidate, root: string, observations: Record<string, unknown>) => Promise<void>;
interface NativeCaseOptions {
  fixture: string;
  args: (root: string) => string[];
  ipc?: boolean;
  executable?: (root: string) => string;
  expectedExit?: { exitCode: number | null; signal: NodeJS.Signals | null } | null;
}

async function nativeCase(name: string, operation: NativeOperation, options: NativeCaseOptions) {
  const projectRoot = mkdtempSync(path.join(tmpdir(), 'codegraph-bootstrap-'));
  const native = nativeCandidate(options.fixture, options.args(projectRoot), options.ipc ?? true, options.executable?.(projectRoot));
  const observations: Record<string, unknown> = {};
  let primaryFailure: string | null = null;
  try {
    // Failed spawn has no identity to acquire. Install its real observer in this
    // synchronous sequence, before any await can consume the actual error event.
    if (native.child.pid !== undefined) await native.acquire();
    await operation(native, projectRoot, observations);
  }
  catch (error) { primaryFailure = detail(error); throw error; }
  finally {
    await finishNativeCase(name, native, projectRoot, observations, primaryFailure,
      options.expectedExit === null ? null : options.expectedExit ?? { exitCode: 0, signal: null });
  }
}

function producerCase(name: string, operation: NativeOperation, ipc = true) {
  return nativeCase(name, operation, { fixture: 'query-bootstrap-producer.cjs', ipc,
    args: root => [name === 'held-refusal' || name === 'oversized-refusal' ? name : 'confirm', root] });
}

describe('compiled child bootstrap producer', () => {
  it('promotes only after the real parent acknowledges actual child permission', async () => {
    await producerCase('confirm-ack', async (native, root, observations) => {
      const result = await observeBootstrap(native, root);
      observations.result = result;
      expect(result).toEqual({ kind: 'permitted', candidatePid: native.child.pid });
      await until(() => nativeResourcesReleased(native), 'confirmed child native cleanup');
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'promoted', pid: native.child.pid });
    });
  }, 30_000);

  it('delivers explicit refusal terminally while the actual child is still alive', async () => {
    await producerCase('held-refusal', async (native, root, observations) => {
      const result = await observeBootstrap(native, root);
      observations.result = result;
      expect(result).toMatchObject({ kind: 'refused', refusal: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED',
        reason: 'unsupported-mode', projectRoot: root, guidance: 'Shared combined preset is not supported.', message: 'Shared policy refused.' } });
      expect(native.facts().exit).toBeNull(); expect(native.facts().pidAbsent).toBe(false);
      native.releaseInput();
      await until(() => nativeResourcesReleased(native), 'refused child native cleanup');
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'refusal-reported', pid: native.child.pid });
    });
  }, 30_000);

  it.each([
    ['malformed-ack', { protocol: 1, kind: 'not-acknowledged' }],
    ['oversized-ack', { protocol: 1, kind: 'acknowledged', padding: '€'.repeat(30_000) }],
  ])('denies promotion for %s received over real IPC', async (name, acknowledgment) => {
    await producerCase(name, async native => {
      native.begin();
      await until(() => native.facts().statuses.length === 1, 'child permission status');
      await new Promise<void>((resolve, reject) => native.child.send(acknowledgment, error => error ? reject(error) : resolve()));
      await until(() => nativeResourcesReleased(native), 'denied child native cleanup');
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'promotion-denied', pid: native.child.pid,
        code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown' });
    });
  }, 30_000);

  it('denies promotion when the real parent disconnects before acknowledgment', async () => {
    await producerCase('lost-ack', async native => {
      native.begin();
      await until(() => native.facts().statuses.length === 1, 'child permission status');
      native.child.disconnect();
      await until(() => nativeResourcesReleased(native), 'disconnected child native cleanup');
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'promotion-denied', pid: native.child.pid,
        code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown' });
    });
  }, 30_000);

  it('accepts a real acknowledgment exactly at the payload byte bound', async () => {
    await producerCase('exact-ack', async native => {
      native.begin();
      await until(() => native.facts().statuses.length === 1, 'child permission status');
      const acknowledgment = { protocol: 1, kind: 'acknowledged', padding: '' };
      acknowledgment.padding = 'x'.repeat(65_536 - Buffer.byteLength(JSON.stringify(acknowledgment), 'utf8'));
      await new Promise<void>((resolve, reject) => native.child.send(acknowledgment, error => error ? reject(error) : resolve()));
      await until(() => nativeResourcesReleased(native), 'exact-bound acknowledgment native cleanup');
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'promoted', pid: native.child.pid });
    });
  }, 30_000);

  it('denies promotion at its fixed deadline when the real parent sends no acknowledgment', async () => {
    await producerCase('missing-ack', async native => {
      native.begin();
      await until(() => nativeResourcesReleased(native), 'child acknowledgment deadline and native cleanup', 20_000);
      const facts = native.facts();
      expect(facts.exit!.elapsedMs - facts.beginElapsedMs!).toBeGreaterThanOrEqual(15_000);
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'promotion-denied', pid: native.child.pid,
        code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown' });
    });
  }, 30_000);

  it('reports a multi-byte refusal above the payload bound as exact compact terminal status', async () => {
    await producerCase('oversized-refusal', async native => {
      native.begin();
      await until(() => nativeResourcesReleased(native), 'oversized refusal native cleanup');
      expect(native.facts().statuses).toHaveLength(1);
      expect(JSON.parse(native.facts().statuses[0].prefix)).toEqual({ protocol: 1, kind: 'refused', detailsOmitted: true });
      expect(native.facts().statuses[0].seenBytes).toBeLessThanOrEqual(65_536);
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'refusal-reported', pid: native.child.pid });
    });
  }, 30_000);

  it.each([
    ['optional detail', 'oversized-refusal'],
    ['projectRoot', 'oversized-root-refusal'],
  ])('keeps an explicit refusal with oversized %s terminal at the real parent after native child exit', async (_field, mode) => {
    await nativeCase(mode + '-parent', async (native, root, observations) => {
      const result = await observeBootstrap(native, root);
      observations.result = result;
      expect(result).toMatchObject({ kind: 'refused', refusal: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED',
        reason: 'unknown', projectRoot: root } });
      await until(() => nativeResourcesReleased(native), 'oversized refusal parent native cleanup');
      expect(native.facts().statuses).toHaveLength(1);
      expect(JSON.parse(native.facts().statuses[0].prefix)).toEqual({ protocol: 1, kind: 'refused', detailsOmitted: true });
      expect(native.facts().statuses[0].seenBytes).toBeLessThanOrEqual(65_536);
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'refusal-reported', pid: native.child.pid });
    }, { fixture: 'query-bootstrap-producer.cjs', args: root => [mode, root] });
  }, 30_000);

  it('leaves the no-IPC standalone caller to its existing external support gate', async () => {
    await producerCase('standalone', async native => {
      native.begin();
      await until(() => nativeResourcesReleased(native), 'standalone child native cleanup');
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'promoted', pid: native.child.pid });
      expect(native.facts().statuses).toEqual([]);
    }, false);
  }, 30_000);
});

describe('native parent bootstrap protocol outcomes', () => {
  it('refuses a permitted status mixed with the compact-refusal marker without acknowledgment', async () => {
    await nativeCase('malformed-compact', async (native, root, observations) => {
      const result = await observeBootstrap(native, root);
      observations.result = result;
      expect(result).toMatchObject({ kind: 'refused', refusal: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root } });
      await until(() => nativeResourcesReleased(native), 'mixed compact marker native cleanup');
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'promotion-denied', pid: native.child.pid });
    }, { fixture: 'query-bootstrap-native.cjs', args: () => ['malformed-compact'] });
  }, 30_000);

  it.each(['malformed', 'malformed-refusal', 'oversized'])('refuses %s status without promotion', async mode => {
    await nativeCase('status-' + mode, async (native, root, observations) => {
      const result = await observeBootstrap(native, root);
      observations.result = result;
      expect(result).toMatchObject({ kind: 'refused', refusal: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root } });
      await until(() => nativeResourcesReleased(native), 'malformed candidate native cleanup');
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'promotion-denied', pid: native.child.pid });
      if (mode === 'oversized') expect(native.facts().statuses[0]).toMatchObject({ truncated: true, retainedBytes: 65_536 });
    }, { fixture: 'query-bootstrap-native.cjs', args: () => [mode] });
  }, 30_000);

  it.each(['exact-bound', 'duplicate'])('permits %s status once with immutable spawned identity', async mode => {
    await nativeCase('status-' + mode, async (native, root, observations) => {
      const result = await observeBootstrap(native, root);
      observations.result = result;
      expect(result).toEqual({ kind: 'permitted', candidatePid: native.child.pid });
      expect(Object.isFrozen(result)).toBe(true);
      await until(() => nativeResourcesReleased(native), 'permitted candidate native cleanup');
      expect(JSON.parse(native.stdout().text)).toEqual({ event: 'promoted', pid: native.child.pid });
      if (mode === 'exact-bound') expect(native.facts().statuses[0]).toMatchObject({ seenBytes: 65_536, retainedBytes: 65_536, truncated: false });
    }, { fixture: 'query-bootstrap-native.cjs', args: () => [mode] });
  }, 30_000);

  it('reports actual exit before status as pre-service unavailable', async () => {
    await nativeCase('pre-service-exit', async (native, root, observations) => {
      const result = await observeBootstrap(native, root);
      observations.result = result;
      expect(result).toEqual({ kind: 'unavailable', cause: 'pre-service-exit', exitCode: 78, signal: null });
    }, { fixture: 'query-bootstrap-native.cjs', args: () => ['pre-service-exit'], expectedExit: { exitCode: 78, signal: null } });
  }, 30_000);

  it('never changes a valid permitted observation to unavailable when native exit races ACK', async () => {
    await nativeCase('permitted-exit-race', async (native, root, observations) => {
      const result = await observeBootstrap(native, root);
      observations.result = result;
      expect(native.facts().statuses[0].prefix).toContain('"kind":"permitted"');
      expect(['permitted', 'refused']).toContain(result.kind);
      if (result.kind === 'refused') expect(result.refusal.reason).toBe('unknown');
      if (result.kind === 'permitted') expect(result.candidatePid).toBe(native.child.pid);
      expect(Object.isFrozen(result)).toBe(true);
    }, { fixture: 'query-bootstrap-native.cjs', args: () => ['permitted-exit'], expectedExit: { exitCode: 78, signal: null } });
  }, 30_000);

  it('refuses a still-live candidate that disconnects without status', async () => {
    await nativeCase('live-disconnect', async (native, root, observations) => {
      const result = await observeBootstrap(native, root);
      observations.result = result;
      const facts = native.facts();
      observations.elapsedMs = facts.observerSettledElapsedMs! - facts.observerStartedElapsedMs!;
      expect(result).toMatchObject({ kind: 'refused', refusal: { reason: 'unknown' } });
      expect(observations.elapsedMs).toBeGreaterThanOrEqual(15_000);
      expect(native.facts().pidAbsent).toBe(false); expect(native.facts().exit).toBeNull();
      native.releaseInput();
    }, { fixture: 'query-bootstrap-native.cjs', args: () => ['live-disconnect'] });
  }, 30_000);

  it('refuses a silent live candidate at the fixed parent deadline', async () => {
    await nativeCase('parent-deadline', async (native, root, observations) => {
      const started = performance.now();
      const result = await observeBootstrap(native, root);
      observations.result = result; observations.elapsedMs = performance.now() - started;
      expect(result).toMatchObject({ kind: 'refused', refusal: { reason: 'unknown' } });
      expect(observations.elapsedMs).toBeGreaterThanOrEqual(15_000);
      expect(native.facts().statuses).toEqual([]);
    }, { fixture: 'query-bootstrap-native.cjs', args: () => ['silent'] });
  }, 30_000);

  it('reports only an actual native spawn error as spawn-error unavailable', async () => {
    await nativeCase('spawn-error', async (native, root, observations) => {
      const result = await observeBootstrap(native, root);
      observations.result = result;
      expect(result).toEqual({ kind: 'unavailable', cause: 'spawn-error', exitCode: native.child.exitCode, signal: native.child.signalCode });
      expect(native.child.pid).toBeUndefined(); expect(native.facts().spawnError).toContain('ENOENT');
    }, { fixture: 'query-bootstrap-native.cjs', args: () => [], executable: root => path.join(root, 'missing-native-node.exe'), expectedExit: null });
  }, 30_000);
});
