import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { createHash } from 'crypto';
import { spawn } from 'child_process';
import { createConnection, type Socket } from 'net';
import { TextDecoder } from 'util';
import { pathToFileURL } from 'url';
import { CodeGraph, getDatabasePath } from '../src';
import { getWriterPidPath } from '../src/mcp/writer-lock';
import { Client } from './query-unlimited-mcp-fixture';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { spawnLogFor, removeSpawnLog } from './daemon-candidates';

// Six source-authored public epochs share one native evidence/cleanup harness.
// Only the labelled absent-direct bridge is internal. No run or RED is claimed.
const EPOCHS = [
  { name: 'finite-direct', family: 'finite', mode: 'direct', caseMs: 90000 },
  { name: 'finite-writable', family: 'finite', mode: 'writable-fallback', caseMs: 120000 },
  { name: 'absent-direct', family: 'absent', mode: 'direct', caseMs: 150000 },
  { name: 'absent-writable', family: 'absent', mode: 'writable-fallback', caseMs: 180000 },
  { name: 'timer-free-recovery-direct', family: 'recovery', mode: 'direct', caseMs: 150000 },
  { name: 'timer-free-recovery-writable', family: 'recovery', mode: 'writable-fallback', caseMs: 180000 },
] as const;
const CLOSE_MS = 15000;
const PREFIX_BYTES = 1048576;
const OBSERVATION_BYTES = 4194304;
const RECEIPT_BYTES = 33554432;
const RETAINED_BYTES = 67108864;
const RETAINED_ENTRIES = 256;
const RETAINED_MANIFEST_BYTES = 1048576;
const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const detail = (value: unknown) => { try { return String(value).slice(0, 4096); } catch { return 'unformattable failure'; } };
const digest = (value: Buffer | string) => createHash('sha256').update(value).digest('hex');
async function until(check: () => boolean | Promise<boolean>, ms: number, label: string) {
  const deadline = performance.now() + ms;
  for (;;) {
    if (performance.now() >= deadline) throw new Error(`lifetime deadline: ${label}`);
    const complete = await check();
    if (performance.now() >= deadline) throw new Error(`lifetime deadline: ${label}`);
    if (complete) return;
    await wait(10);
  }
}
// Serial epochs reset these bounded secondary facts. A close failure must
// never replace an original thrown value, including `throw undefined`.
const fileCloseFailures: Array<{ file: string; detail: string }> = [];
let fileCloseFailureOverflow = false;
function descriptor<T>(file: string, flags: string, read: (fd: number) => T, missing = false): T | null {
  let fd: number | undefined, failed = false, primary: unknown, value: T | null = null;
  try { fd = fs.openSync(file, flags); value = read(fd); }
  catch (error) { if (!(missing && fd === undefined && (error as NodeJS.ErrnoException)?.code === 'ENOENT')) { failed = true; primary = error; } }
  if (fd !== undefined) try { fs.closeSync(fd); } catch (error) {
    if (failed) {
      if (fileCloseFailures.length < 64) fileCloseFailures.push({ file, detail: detail(error) });
      else fileCloseFailureOverflow = true;
    } else { failed = true; primary = error; }
  }
  if (failed) throw primary;
  return value;
}
function boundedFile(file: string, maximum: number): Buffer | null {
  return descriptor(file, 'r', fd => {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.size > maximum) throw new Error(`bounded regular evidence file exceeded ${maximum}: ${file}`);
    const bytes = Buffer.alloc(stat.size);
    let offset = 0;
    while (offset < bytes.length) {
      const count = fs.readSync(fd, bytes, offset, bytes.length - offset, offset);
      if (!count) throw new Error(`evidence short read: ${file}`);
      offset += count;
    }
    return bytes;
  }, true);
}
function observationPrefix(file: string) {
  return descriptor(file, 'r', fd => {
    const stat = fs.fstatSync(fd);
    if (!stat.isFile()) throw new Error('lifetime observations are not a regular file');
    const bytes = Buffer.alloc(Math.min(stat.size, OBSERVATION_BYTES));
    let offset = 0;
    while (offset < bytes.length) {
      const count = fs.readSync(fd, bytes, offset, bytes.length - offset, offset);
      if (!count) throw new Error('lifetime observations ended before their sampled length');
      offset += count;
    }
    return { bytes, seenBytes: stat.size };
  }, true);
}
function procFile(file: string, maximum: number) {
  return descriptor(file, 'r', fd => {
    const bytes = Buffer.alloc(maximum + 1); let length = 0;
    while (length < bytes.length) { const count = fs.readSync(fd, bytes, length, bytes.length - length, null); if (!count) break; length += count; }
    if (length > maximum) throw new Error(`native proc file exceeded ${maximum}: ${file}`);
    return bytes.subarray(0, length);
  }, true);
}
function inventory(directory: string) {
  let entries: fs.Dirent[];
  try { entries = fs.readdirSync(directory, { recursive: true, withFileTypes: true }); }
  catch (error) { if ((error as NodeJS.ErrnoException)?.code === 'ENOENT') return []; throw error; }
  if (entries.length > 128) throw new Error('lifetime index/registry inventory exceeded 128 entries');
  let total = 0;
  return entries.map(entry => {
    const file = path.join(entry.parentPath, entry.name);
    const relative = path.relative(directory, file).replace(/\\/g, '/');
    if (!entry.isFile()) return { relative, kind: entry.isDirectory() ? 'directory' : 'other' };
    const bytes = boundedFile(file, 67108864)!;
    total += bytes.length;
    if (total > 67108864) throw new Error('lifetime index/registry inventory exceeded 64MiB');
    return { relative, kind: 'file', bytes: bytes.length, sha256: digest(bytes) };
  }).sort((left, right) => left.relative.localeCompare(right.relative));
}
// The retained evidence tree has its own bounded membership. Closing a directory
// must preserve an earlier thrown value just as closing a file descriptor does.
function retainedNames(directory: string) {
  let handle: fs.Dir | undefined, failed = false, primary: unknown;
  const names: string[] = [];
  try {
    handle = fs.opendirSync(directory);
    for (let entry = handle.readSync(); entry; entry = handle.readSync()) {
      names.push(entry.name);
      if (names.length > RETAINED_ENTRIES) throw new Error('retained directory exceeded 256 entries');
    }
  } catch (error) { failed = true; primary = error; }
  if (handle) try { handle.closeSync(); } catch (error) {
    if (failed) {
      if (fileCloseFailures.length < 64) fileCloseFailures.push({ file: directory, detail: detail(error) });
      else fileCloseFailureOverflow = true;
    } else { failed = true; primary = error; }
  }
  if (failed) throw primary;
  return names.sort();
}
function retainedMetadata(stat: fs.BigIntStats) {
  return Object.fromEntries(['dev', 'ino', 'mode', 'nlink', 'uid', 'gid', 'rdev', 'size', 'blksize', 'blocks',
    'atimeNs', 'mtimeNs', 'ctimeNs', 'birthtimeNs'].map(name => [name, String(stat[name as keyof fs.BigIntStats])]));
}
function stableRetainedMetadata(metadata: Record<string, string>) {
  // Our own evidence reads can update access time. Preserve its original value
  // in the manifest; every other captured native stat value must stay equal.
  const { atimeNs, ...stable } = metadata;
  return stable;
}
function retainedAbsent(file: string) {
  try { fs.lstatSync(file); return false; }
  catch (error) { if ((error as NodeJS.ErrnoException)?.code === 'ENOENT') return true; throw error; }
}
function pipeCapture(stream: NodeJS.ReadableStream) {
  let raw = Buffer.alloc(0), seenBytes = 0, eof = false, closed = false, failure: string | null = null;
  const decoder = new TextDecoder('utf-8', { fatal: true });
  stream.on('data', data => {
    const bytes = Buffer.isBuffer(data) ? data : Buffer.from(data);
    seenBytes += bytes.length;
    raw = Buffer.concat([raw, bytes.subarray(0, Math.max(0, PREFIX_BYTES - raw.length))]);
    if (seenBytes > PREFIX_BYTES) failure ??= 'native pipe exceeded 1MiB';
    try { decoder.decode(bytes, { stream: true }); } catch (error) { failure ??= detail(error); }
  });
  stream.on('error', error => { failure ??= detail(error); });
  stream.on('end', () => { eof = true; try { decoder.decode(); } catch (error) { failure ??= detail(error); } });
  stream.on('close', () => { closed = true; });
  return () => ({ base64: raw.toString('base64'), sha256: digest(raw), seenBytes, retainedBytes: raw.length,
    truncated: seenBytes > raw.length, eof, closed, failure });
}
interface NativeSample {
  token: number; elapsedSeconds: number; processes: Array<{ pid: number; creationIdentity: string }>;
  provenance: unknown; completedNs: string;
}
interface NativeIdentity {
  pid: number; creationIdentity: string; sampleElapsedSeconds: number; sampleToken: number;
  provenance: unknown; acquiredNs: string;
}
// Read only the trailing 2MiB, even when a qualified full-suite census grows
// for hours. Ignore the leading partial frame and the incomplete append tail;
// each accepted frame is complete, bounded, and tied to its exact byte end.
function runnerCensus(file: string) {
  let identity: string | undefined, previousSize = 0, observed = false;
  return (): NativeSample[] => {
    const samples = descriptor(file, 'r', fd => {
      observed = true;
      const stat = fs.fstatSync(fd);
      if (!stat.isFile()) throw new Error('runner census is not a regular file');
      const currentIdentity = `${stat.dev}:${stat.ino}`;
      if ((identity && identity !== currentIdentity) || stat.size < previousSize) throw new Error('runner census replaced or shrank');
      identity = currentIdentity; previousSize = stat.size;
      const start = Math.max(0, stat.size - 2097152), bytes = Buffer.alloc(stat.size - start);
      let offset = 0;
      while (offset < bytes.length) {
        const count = fs.readSync(fd, bytes, offset, bytes.length - offset, start + offset);
        if (!count) throw new Error('runner census short window read');
        offset += count;
      }
      let cursor = start ? bytes.indexOf(10) + 1 : 0;
      if (start && cursor === 0) throw new Error('runner census has no complete frame in its 2MiB window');
      const samples: NativeSample[] = [];
      for (let end = bytes.indexOf(10, cursor); end !== -1; end = bytes.indexOf(10, cursor)) {
        const frame = bytes.subarray(cursor, end);
        if (!frame.length || frame.length > 1048576) throw new Error('runner census frame is empty or exceeds 1MiB');
        const sample = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(frame));
        if (!Number.isFinite(sample.elapsedSeconds) || !Array.isArray(sample.processes)
          || sample.processes.some((row: any) => !Number.isSafeInteger(row.pid) || row.pid <= 0
            || typeof row.creationIdentity !== 'string' || !/^\d+$/.test(row.creationIdentity))) throw new Error('malformed runner native census');
        samples.push({ token: start + end + 1, elapsedSeconds: sample.elapsedSeconds, processes: sample.processes,
          completedNs: process.hrtime.bigint().toString(), provenance: { provider: 'qualified-runner', file, fileIdentity: identity,
            birthtimeMs: stat.birthtimeMs, frameStart: start + cursor, frameEnd: start + end + 1,
            frameBytes: frame.length, frameSha256: digest(frame), windowBytes: bytes.length } });
        cursor = end + 1;
      }
      if (bytes.length - cursor > 1048576) throw new Error('runner census unfinished frame exceeds 1MiB');
      return samples;
    }, true);
    if (samples === null && observed) throw new Error('previously observed runner census disappeared');
    return samples ?? [];
  };
}
// Native utilities are observers of this actual ChildProcess, never alternate
// CLI/engine factories. Each owned probe has bounded capture, time, and close.
async function nativeProbe(command: string, argv: string[], ledger: any[]) {
  if (ledger.length >= 64) throw new Error('native identity probes exceeded 64');
  const fact: any = { command, argv, startedNs: process.hrtime.bigint().toString(), platform: process.platform,
    osRelease: os.release(), architecture: process.arch,
    exit: null, close: null, failure: null, killFailure: null, forcedPipeRelease: false };
  ledger.push(fact);
  const child = spawn(command, argv, { windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'],
    env: { ...process.env, LC_ALL: 'C', LANG: 'C', TZ: 'UTC' } });
  fact.pid = child.pid ?? null;
  const capture = (stream: NodeJS.ReadableStream) => {
    let raw = Buffer.alloc(0), seenBytes = 0, eof = false, closed = false, failure: string | null = null;
    stream.on('data', data => { const bytes = Buffer.isBuffer(data) ? data : Buffer.from(data); seenBytes += bytes.length;
      raw = Buffer.concat([raw, bytes.subarray(0, Math.max(0, 65536 - raw.length))]);
      if (seenBytes > 65536) { failure ??= 'native identity probe exceeded 64KiB per pipe'; stop(failure); } });
    stream.on('error', error => { failure ??= detail(error); stop(failure); });
    stream.on('end', () => { eof = true; }); stream.on('close', () => { closed = true; });
    return () => ({ raw, base64: raw.toString('base64'), sha256: digest(raw), seenBytes, retainedBytes: raw.length,
      truncated: seenBytes > raw.length, eof, closed, failure });
  };
  let timer: NodeJS.Timeout | undefined, joinTimer: NodeJS.Timeout | undefined, finished = false;
  let finish: () => void = () => {};
  const stop = (reason: string) => {
    fact.failure ??= reason;
    try { if (!child.kill('SIGKILL')) fact.killFailure ??= 'native probe kill returned false'; } catch (error) { fact.killFailure ??= detail(error); }
    joinTimer ??= setTimeout(() => {
      // Last-resort handle cleanup cannot manufacture native close evidence.
      // Keep missing exit/close and forced pipe release as acceptance failures.
      fact.closeDeadlineExceeded = true; fact.forcedPipeRelease = true;
      child.stdout.destroy(); child.stderr.destroy(); child.unref(); finish();
    }, 1000);
  };
  const out = capture(child.stdout), err = capture(child.stderr);
  await new Promise<void>(resolve => {
    finish = () => { if (!finished) { finished = true; resolve(); } };
    child.on('error', error => { fact.failure ??= detail(error); });
    child.on('exit', (code, signal) => { fact.exit = { code, signal, atNs: process.hrtime.bigint().toString() }; });
    child.on('close', (code, signal) => { fact.close = { code, signal, atNs: process.hrtime.bigint().toString() }; finish(); });
    timer = setTimeout(() => stop('native identity probe exceeded 3000ms'), 3000);
  });
  if (timer) clearTimeout(timer); if (joinTimer) clearTimeout(joinTimer);
  const stdout = out(), stderr = err();
  fact.stdout = { ...stdout, raw: undefined }; fact.stderr = { ...stderr, raw: undefined };
  fact.completedNs = process.hrtime.bigint().toString();
  if (fact.failure || !fact.exit || !fact.close || fact.close.signal !== null
    || !stdout.eof || !stdout.closed || !stderr.eof || !stderr.closed || stdout.failure || stderr.failure) {
    throw new Error(`native identity probe failed: ${detail(fact.failure ?? 'incomplete native exit/close/pipes')}`);
  }
  return { stdout: stdout.raw, stderr: stderr.raw, code: fact.close.code, fact };
}
function localCensus(ledger: any[]) {
  let sequence = 0;
  return async (pid: number): Promise<NativeSample> => {
    if (!Number.isSafeInteger(pid) || pid <= 0) throw new Error('invalid native host PID');
    let creationIdentity: string | null, provenance: any;
    if (process.platform === 'linux') {
      const file = `/proc/${pid}/stat`;
      // procfs reports a zero stat size; read to EOF under a strict byte cap.
      const raw = procFile(file, 65536);
      creationIdentity = null;
      if (raw) {
        const text = new TextDecoder('utf-8', { fatal: true }).decode(raw), end = text.lastIndexOf(')');
        if (!text.startsWith(`${pid} (`) || end < 0) throw new Error('ambiguous native proc PID record');
        const fields = text.slice(end + 1).trim().split(/\s+/);
        if (!/^\d+$/.test(fields[19] ?? '')) throw new Error('malformed native proc start ticks');
        creationIdentity = fields[19];
      }
      const boot = procFile('/proc/sys/kernel/random/boot_id', 128)?.toString('ascii').trim() ?? null;
      if (boot !== null && !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(boot)) throw new Error('malformed Linux boot identity');
      provenance = { provider: 'linux-procfs', platform: process.platform, osRelease: os.release(), architecture: process.arch, file, bootId: boot,
        precision: 'kernel clock ticks as string', rawBase64: raw?.toString('base64') ?? null, rawSha256: raw ? digest(raw) : null };
    } else if (process.platform === 'win32') {
      const command = path.join(process.env.SystemRoot ?? 'C:\\Windows', 'System32', 'WindowsPowerShell', 'v1.0', 'powershell.exe');
      const script = `[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false; $ErrorActionPreference = 'Stop'; $p = $null; try { try { $p = [Diagnostics.Process]::GetProcessById(${pid}) } catch { if ($_.Exception -is [ArgumentException] -or $_.Exception.InnerException -is [ArgumentException]) { [Console]::Write('{"present":false}'); exit 0 }; throw }; if ($p.HasExited) { [Console]::Write('{"present":false}') } else { [Console]::Write(('{"present":true,"pid":${pid},"creationIdentity":"' + $p.StartTime.ToUniversalTime().ToFileTimeUtc().ToString([Globalization.CultureInfo]::InvariantCulture) + '"}')) } } finally { if ($null -ne $p) { $p.Dispose() } }`;
      const probe = await nativeProbe(command, ['-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-Command', script], ledger);
      if (probe.code !== 0 || probe.stderr.length) throw new Error('Windows native identity query did not complete cleanly');
      const row = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(probe.stdout));
      if (row.present === false && Object.keys(row).length === 1) creationIdentity = null;
      else if (row.present === true && row.pid === pid && typeof row.creationIdentity === 'string' && /^\d+$/.test(row.creationIdentity)) creationIdentity = row.creationIdentity;
      else throw new Error('malformed Windows native creation identity');
      provenance = { provider: 'windows-Process.StartTime-FileTimeUtc', platform: process.platform, osRelease: os.release(), architecture: process.arch,
        precision: 'native FILETIME as string', probe: ledger.length - 1 };
    } else if (process.platform === 'darwin') {
      const probe = await nativeProbe('/bin/ps', ['-p', String(pid), '-o', 'pid=', '-o', 'lstart='], ledger);
      const text = new TextDecoder('utf-8', { fatal: true }).decode(probe.stdout);
      if (probe.stderr.length) throw new Error('macOS native identity query emitted an error');
      if (probe.code === 1 && !text.trim()) creationIdentity = null;
      else {
        const rows = text.split(/\r?\n/).filter(line => line.trim());
        const row = rows.length === 1 ? /^\s*(\d+)\s+([A-Z][a-z]{2} [A-Z][a-z]{2} [ \d]\d \d\d:\d\d:\d\d \d{4})\s*$/.exec(rows[0]) : null;
        if (probe.code !== 0 || !row || row[1] !== String(pid)) throw new Error('ambiguous or malformed macOS native PID/lstart');
        creationIdentity = row[2]; // Literal OS string, never parsed into a JS date/number.
      }
      // lstart has one-second precision. It alone cannot disambiguate reuse
      // within that second: actual child/pipe continuity and final no-PID plus
      // independent ESRCH are required; any live reused PID fails absence.
      provenance = { provider: 'darwin-ps-lstart', platform: process.platform, osRelease: os.release(), architecture: process.arch,
        precision: 'one second; not a subsecond unique token',
        timezone: 'UTC', locale: 'C', probe: ledger.length - 1 };
    } else throw new Error(`native creation identity provider unavailable on ${process.platform}`);
    return { token: ++sequence, elapsedSeconds: performance.now() / 1000, completedNs: process.hrtime.bigint().toString(),
      processes: creationIdentity === null ? [] : [{ pid, creationIdentity }], provenance };
  };
}
const absent = (pid: number) => { try { process.kill(pid, 0); return false; } catch (error) {
  if ((error as NodeJS.ErrnoException).code === 'ESRCH') return true; throw error;
} };

describe('selected public query lifetime', () => {
  for (const epoch of EPOCHS) it(epoch.name, async () => {
    const CASE_MS = epoch.caseMs, CLIENT_MS = epoch.family === 'finite' ? 15000 : epoch.family === 'absent' ? 90000 : 30000;
    const poolSize = epoch.family === 'finite' ? '1' : epoch.family === 'absent' ? '2' : 'unlimited';
    const busyTimeout = epoch.family === 'finite' ? '5000' : 'unlimited';
    fileCloseFailures.length = 0; fileCloseFailureOverflow = false;
    const started = performance.now();
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-public-lifetime-')));
    const root = path.join(fixture, 'project'), home = path.join(fixture, 'home');
    const observations = path.join(fixture, 'observations.jsonl');
    const begin = path.join(fixture, 'begin'), release = path.join(fixture, 'release');
    const gates = path.join(fixture, 'gates'), candidateRelease = path.join(fixture, 'candidate-release');
    // Ordinary npm/CI needs no wrapper. Its bounded receipt is a sibling of
    // the disposable fixture; qualified runs retain the canonical artifact.
    const receipt = process.env.VALIDATION_OUT ? path.join(process.env.VALIDATION_OUT, `public-lifetime-${epoch.name}.json`) : `${fixture}.evidence.json`;
    const retainedFixture = path.join(path.dirname(receipt), `${path.basename(fixture)}.retained-fixture`);
    const retention: any = { path: retainedFixture, state: 'not-created', failure: null, manifestPath: null,
      manifestSha256: null, manifestBytes: null, rawBytes: null, totalBytes: null, entries: null, files: null,
      originalFixtureRemoved: false, originalSpawnLogRemoved: false,
      lastVerifiedNs: null, verifiedAfterOriginalRemoval: false,
      attributeProbeIndices: [],
      metadata: 'literal source stat values in manifest; copied mode and timestamps use native fs API precision',
      disposition: 'archive retained through final mandatory publications and outer acceptance decision' };
    let retainedManifest: any = null;
    const source = 'export function finiteRunningSymbol() { return 11; }\nexport function finiteQueuedSymbol() { return 22; }\nexport function finitePeerSymbol() { return 33; }\n'
      + 'export function absentHealthySymbol() { return 44; }\nexport function absentRunningSymbol() { return 55; }\nexport function absentQueuedSymbol() { return 66; }\n'
      + 'export function bridgeHealthySymbol() { return 77; }\nexport function bridgeRunningSymbol() { return 88; }\nexport function bridgeQueuedSymbol() { return 99; }\n'
      + 'export function shutdownRunningSymbol() { return 110; }\nexport function recoveryRecoveredSymbol() { return 121; }\n'
      + 'export function exhaustionDemand0() { return 200; }\nexport function exhaustionDemand1() { return 201; }\nexport function exhaustionDemand2() { return 202; }\n'
      + 'export function exhaustionDemand3() { return 203; }\nexport function exhaustionDemand4() { return 204; }\nexport function exhaustionDemand5() { return 205; }\n';
    const diagnostics: any[] = [], cleanup: any[] = [], secondary: any[] = [], ownershipSnapshots: any[] = [], inventorySnapshots: any[] = [], nativePredicates: any[] = [];
    const nativeProbes: any[] = [], nativeSamples: NativeSample[] = [], collectorFailures: any[] = [];
    const qualifiedCensus = process.env.VALIDATION_OUT ? runnerCensus(path.join(process.env.VALIDATION_OUT, 'resources.jsonl')) : null;
    const ordinaryCensus = localCensus(nativeProbes);
    let nativeFailure: string | null = null, collectorFailureOverflow = false;
    const nativeSample = async (pid: number): Promise<NativeSample | null> => {
      if (nativeFailure) throw new Error(nativeFailure);
      try {
        const sample = qualifiedCensus ? qualifiedCensus().at(-1) ?? null : await ordinaryCensus(pid);
        if (sample && sample.token !== nativeSamples.at(-1)?.token) {
          // Retain the first sample plus the 31 newest complete samples;
          // the original qualified full census stays in its runner artifact.
          if (nativeSamples.length === 32) nativeSamples.splice(1, 1);
          nativeSamples.push({ ...sample, processes: sample.processes.filter(row => row.pid === pid),
            provenance: { source: sample.provenance, projection: 'selected host PID only', sourceProcessCount: sample.processes.length } });
        }
        return sample;
      } catch (error) { nativeFailure ??= detail(error); throw error; }
    };
    let client: Client | undefined, hostIdentity: NativeIdentity | null = null;
    let stdout: ReturnType<typeof pipeCapture> | undefined, stderr: ReturnType<typeof pipeCapture> | undefined;
    let nativeExit: { code: number | null; signal: NodeJS.Signals | null; atNs: string } | null = null;
    let nativeClose: { code: number | null; signal: NodeJS.Signals | null; atNs: string } | null = null;
    let stdinClosed = false, stdinFailure: string | null = null, absenceObserved = false;
    let postCloseCheckpoint: NativeSample | null = null, postCloseAbsence: NativeSample | null = null;
    let failed = false, primary: unknown, phase = 'fixture setup', observationFailure: string | null = null, persistenceFailure: string | null = null;
    let observationRaw = Buffer.alloc(0), observationSeenBytes = 0, retainedEvents: any[] = [], observationRemoved = false;
    let ordinal = 0, publication = 0;
    let running: Promise<any> | undefined, queued: Promise<any> | undefined;
    let runningCall: any, queuedCall: any, runningResult: any, queuedResult: any, peerResult: any;
    const extended: any = { epoch: epoch.name, state: 'not-started', wireDemands: [], recoveries: [],
      candidates: [], bridge: { classification: 'INTERNAL same actual CLI engine; not public stdio multiplexing', clients: [] }, shutdown: null };
    const admitted: Promise<unknown>[] = [];
    let assertExtendedTail: (() => void) | null = null;
    const bridgeClients: Array<{ label: string; socket: Socket; capture: ReturnType<typeof pipeCapture>; replies: any[];
      writes: string[]; failure: string | null; ended: boolean; closed: boolean }> = [];
    const events = () => {
      if (observationFailure) throw new Error(observationFailure);
      try {
        const capture = observationPrefix(observations);
        if (!capture) {
          if (observationSeenBytes && !observationRemoved) throw new Error('lifetime observation disappeared before verified removal');
          return retainedEvents;
        }
        if (capture.seenBytes < observationSeenBytes) throw new Error('lifetime observation length decreased');
        observationSeenBytes = capture.seenBytes; observationRaw = capture.bytes;
        if (capture.seenBytes > OBSERVATION_BYTES) throw new Error('lifetime observations exceeded 4MiB');
        const text = new TextDecoder('utf-8', { fatal: true }).decode(capture.bytes.subarray(0, capture.bytes.lastIndexOf(10) + 1));
        // A concurrent append can expose an unfinished final frame. Retain its
        // raw bytes; parse only complete LF frames, and require no tail at exit.
        retainedEvents = text.split('\n').filter(Boolean).map(line => JSON.parse(line));
        return retainedEvents;
      } catch (error) { observationFailure ??= detail(error); throw error; }
    };
    const owner = () => Object.fromEntries([['writer', getWriterPidPath(root)], ['daemon', path.join(root, '.codegraph', 'daemon.pid')]]
      .map(([name, file]) => [name, boundedFile(file, 4096)?.toString('utf8') ?? null]));
    const collect = (label: string, read: () => unknown, maximum: number, unavailable?: string) => {
      if (unavailable) return { state: 'unavailable', reason: unavailable };
      try {
        const value = read();
        if (Buffer.byteLength(JSON.stringify(value) ?? 'null') > maximum) throw new Error(`${label} payload exceeded ${maximum} bytes`);
        return { state: 'available', value };
      } catch (error) {
        const failure = { label, detail: detail(error), thrownType: typeof error };
        if (!collectorFailures.some(item => item.label === label)) {
          if (collectorFailures.length < 64) collectorFailures.push(failure); else collectorFailureOverflow = true;
        }
        return { state: 'failed', error: failure };
      }
    };
    const persist = (stage: string) => {
      try {
        // No collector is a prerequisite of another. An unreadable inventory
        // still publishes actual native, pipe, close, and observation facts.
        const writer = collect('writer owner', () => boundedFile(getWriterPidPath(root), 4096)?.toString('utf8') ?? null, 32768);
        const daemon = collect('daemon owner', () => boundedFile(path.join(root, '.codegraph', 'daemon.pid'), 4096)?.toString('utf8') ?? null, 32768);
        const index = collect('index inventory', () => inventory(path.join(root, '.codegraph')), 131072);
        const registry = collect('registry inventory', () => inventory(path.join(home, '.codegraph', 'daemons')), 131072);
        const observationEvents = collect('observation events', events, 8388608);
        const observationBytes = collect('raw observations', () => ({ base64: observationRaw.toString('base64'), sha256: digest(observationRaw),
          seenBytes: observationSeenBytes, retainedBytes: observationRaw.length, truncated: observationSeenBytes > observationRaw.length,
          failure: observationFailure }), 6291456);
        const native = collect('native facts', () => ({ hostIdentity, nativeExit, nativeClose, stdinClosed, stdinFailure,
          absenceObserved, postCloseCheckpoint, postCloseAbsence, samples: nativeSamples, probes: nativeProbes, failure: nativeFailure,
          ipc: { configured: false, connected: client?.child.connected ?? false, channelReleased: client?.child.channel == null } }), 12582912);
        const rawStdout = collect('raw stdout', () => stdout!(), 1572864, stdout ? undefined : 'CLI stdout observer not created');
        const rawStderr = collect('raw stderr', () => stderr!(), 1572864, stderr ? undefined : 'CLI stderr observer not created');
        const publicCapture = collect('public client capture', () => client!.capture(), 4194304, client ? undefined : 'CLI client not created');
        const publicReplies = collect('public client replies', () => client!.replies, 4194304, client ? undefined : 'CLI client not created');
        const fixtureState = collect('fixture state', () => {
          return { fixtureRemoved: retainedAbsent(fixture), retention };
        }, 4096);
        const snapshotState = collect('bounded snapshots', () => {
          if (ownershipSnapshots.length >= 64 || inventorySnapshots.length >= 64) throw new Error('lifetime snapshots exceeded 64');
          ownershipSnapshots.push({ stage, atNs: process.hrtime.bigint().toString(), writer, daemon });
          inventorySnapshots.push({ stage, index, registry });
          return { ownershipCount: ownershipSnapshots.length, inventoryCount: inventorySnapshots.length };
        }, 4096);
        extended.bridge.clients = bridgeClients.map(item => ({ label: item.label, replies: item.replies, writes: item.writes,
          failure: item.failure, ended: item.ended, closed: item.closed, capture: item.capture() }));
        const value = { schemaVersion: 4, epoch: epoch.name, stage, phase, failed,
          primary: failed ? detail(primary) : null, secondary, cleanup, fixture, root, home,
          source, sourceSha256: digest(source), caseBoundMs: CASE_MS, clientDeadlineMs: CLIENT_MS, closeDeadlineMs: CLOSE_MS,
          receiptSequence: ++publication, elapsedMs: performance.now() - started, receipt,
          evidenceRoute: qualifiedCensus ? 'qualified runner census' : 'ordinary native OS observer',
          collectors: { writer, daemon, index, registry, observationEvents, observationBytes, native,
            rawStdout, rawStderr, publicCapture, publicReplies, fixtureState, snapshotState },
          runningCall, queuedCall, runningResult, queuedResult, peerResult, extended, diagnostics, ownershipSnapshots, inventorySnapshots, nativePredicates,
          collectorFailures, collectorFailureOverflow, fileCloseFailures, fileCloseFailureOverflow, persistenceFailure };
        const bytes = Buffer.from(JSON.stringify(value, null, 2));
        if (bytes.length > RECEIPT_BYTES) throw new Error('lifetime receipt exceeded 32MiB');
        // A fixed exclusive pending file preserves the first failed write and
        // bounds publication storage to the last complete receipt plus one
        // pending prefix. Later attempts never truncate that failed evidence.
        const temporary = `${receipt}.pending`;
        descriptor(temporary, 'wx', fd => { fs.writeFileSync(fd, bytes); fs.fsyncSync(fd); });
        fs.renameSync(temporary, receipt);
      } catch (error) {
        persistenceFailure ??= detail(error);
        try { process.stderr.write(`Lifetime evidence publication failed: ${persistenceFailure}\n`); } catch { /* cleanup continues */ }
        if (stage === 'closed') {
          // Supplement only: missing complete raw evidence remains a failure.
          // Keep final native facts visible if filesystem publication failed.
          const summary = (capture: ReturnType<ReturnType<typeof pipeCapture>> | undefined) => capture && {
            sha256: capture.sha256, seenBytes: capture.seenBytes, retainedBytes: capture.retainedBytes,
            eof: capture.eof, closed: capture.closed, truncated: capture.truncated, failure: capture.failure };
          try { const supplement = JSON.stringify({ event: 'lifetime-final-evidence-publication-failed',
            fixture, retention, failed, primary: failed ? detail(primary) : null, persistenceFailure, hostIdentity,
            nativeExit, nativeClose, stdinClosed, stdinFailure, absenceObserved,
            stdout: summary(stdout?.()), stderr: summary(stderr?.()), observationFailure,
            nativePredicates: nativePredicates.map(item => ({ label: item.label, state: item.state,
              ...(item.detail ? { detail: item.detail.slice(0, 512) } : {}) })),
          }) + '\n';
            if (Buffer.byteLength(supplement) > 32768) throw new Error('lifetime final failure supplement exceeded 32768 bytes');
            process.stderr.write(supplement);
          } catch { /* sticky failure remains; original or verified archive is retained */ }
        }
      }
    };
    const rpc = (method: string, params: unknown = {}) => {
      const call = { id: ++ordinal, sentNs: process.hrtime.bigint().toString(), settledNs: null as string | null, method, params };
      const result = client!.request(method, params).then(value => { call.settledNs = process.hrtime.bigint().toString(); return value; },
        error => { call.settledNs = process.hrtime.bigint().toString(); throw error; });
      return { call, result };
    };
    const status = async (stage: string) => {
      const value = parseQueryDiagnostics(await rpc('tools/call', { name: 'codegraph_status', arguments: {} }).result);
      diagnostics.push({ stage, atNs: process.hrtime.bigint().toString(), value });
      expect(value.requested).toEqual({ mode: 'unlimited', poolSize, busyTimeoutMs: busyTimeout });
      expect(value.effective).toMatchObject({ executionMode: epoch.mode, source: 'strict', preset: 'unlimited',
        concurrency: epoch.family === 'recovery' ? { kind: 'unlimited' } : { kind: 'finite', maximum: Number(poolSize) },
        deadline: epoch.family === 'finite' ? { kind: 'finite', milliseconds: 5000 } : { kind: 'absent' },
        retention: 'retire-idle', classification: epoch.family === 'recovery' ? 'unlimited' : 'custom' });
      expect(value.ownership).toMatchObject({ pid: client!.child.pid, capability: 'read-write',
        writerAuthority: true, synchronizationAuthority: true, resourceScope: 'local-engine' });
      expect(value.project).toEqual({ route: 'default', root: fs.realpathSync(root), index: fs.realpathSync(getDatabasePath(root)) });
      return value;
    };
    const attempts = async (name: string, action: () => unknown | Promise<unknown>) => {
      const item: any = { phase: name, outcome: 'running' }; cleanup.push(item); persist(`before ${name}`);
      try { await action(); item.outcome = 'pass'; } catch (error) { item.outcome = 'fail'; item.detail = detail(error); }
      persist(`after ${name}`);
    };
    const removalReady = () => !failed && !secondary.length && !observationFailure && !persistenceFailure && !nativeFailure
      && !collectorFailures.length && !collectorFailureOverflow && !fileCloseFailures.length && !fileCloseFailureOverflow
      && !retention.failure && cleanup.every(item => item.outcome === 'pass'
        || (item.phase === 'remove successful private fixture' && item.outcome === 'running'));
    // Windows lstat alone does not expose every ReparsePoint tag. This native
    // observer checks attributes before recursive enumeration and byte opens.
    // It shares the existing <=64-probe, 3s+1s, 64KiB/pipe transport and records
    // its own native PID/creation FILETIME, raw output, real exit/close and pipes.
    const nativeAttributes = async (requests: Array<{ path: string; recursive: boolean; optional?: boolean }>) => {
      if (process.platform !== 'win32') return null;
      try {
        const payload = Buffer.from(JSON.stringify(requests));
        if (requests.length > 4 || payload.length > 8192) throw new Error('native attribute request exceeded 4 roots/8KiB');
        const encoded = payload.toString('base64');
        const script = `[Console]::OutputEncoding = New-Object System.Text.UTF8Encoding $false; $ErrorActionPreference = 'Stop'; $self = [Diagnostics.Process]::GetCurrentProcess(); try { $identity = $self.StartTime.ToUniversalTime().ToFileTimeUtc().ToString([Globalization.CultureInfo]::InvariantCulture); [Console]::WriteLine((([ordered]@{ event = 'attribute-probe-identity'; pid = $self.Id; creationIdentity = $identity }) | ConvertTo-Json -Compress)) } finally { $self.Dispose() }; $requests = [Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('${encoded}')) | ConvertFrom-Json; $rows = New-Object 'Collections.Generic.List[object]'; $seen = New-Object 'Collections.Generic.HashSet[string]'; function ReadMember([string]$file, [bool]$recursive, [bool]$optional, [int]$depth) { if ($depth -gt 32) { throw 'attribute depth exceeded 32' }; $file = [IO.Path]::GetFullPath($file); if (-not $seen.Add($file)) { return }; if ($rows.Count -ge 512) { throw 'attribute rows exceeded 512' }; try { $attributes = [IO.File]::GetAttributes($file) } catch { $e = $_.Exception; if ($null -ne $e.InnerException) { $e = $e.InnerException }; if ($optional -and ($e -is [IO.FileNotFoundException] -or $e -is [IO.DirectoryNotFoundException])) { $rows.Add([pscustomobject]@{ path = $file; present = $false; attributes = $null }); return }; throw }; $bits = [uint32]$attributes; $rows.Add([pscustomobject]@{ path = $file; present = $true; attributes = $bits.ToString([Globalization.CultureInfo]::InvariantCulture) }); if (($bits -band 1024) -ne 0 -or ($bits -band 4294770696) -ne 0) { throw 'unknown or ReparsePoint evidence member refused' }; if ($recursive -and (($bits -band 16) -ne 0)) { foreach ($child in [IO.Directory]::EnumerateFileSystemEntries($file)) { ReadMember $child $true $false ($depth + 1) } } }; foreach ($request in $requests) { $ancestors = New-Object 'Collections.Generic.List[string]'; $parent = [IO.Directory]::GetParent([IO.Path]::GetFullPath([string]$request.path)); while ($null -ne $parent) { if ($ancestors.Count -ge 32) { throw 'attribute ancestors exceeded 32' }; $ancestors.Add($parent.FullName); $parent = $parent.Parent }; for ($i = $ancestors.Count - 1; $i -ge 0; $i--) { ReadMember $ancestors[$i] $false $false 0 }; ReadMember ([string]$request.path) ([bool]$request.recursive) ([bool]$request.optional) 0 }; $output = [ordered]@{ rows = @($rows.ToArray()) }; [Console]::Write(($output | ConvertTo-Json -Depth 5 -Compress))`;
        const command = path.join(process.env.SystemRoot ?? 'C:\\Windows', 'System32', 'WindowsPowerShell', 'v1.0', 'powershell.exe');
        const probeIndex = nativeProbes.length;
        let probe: Awaited<ReturnType<typeof nativeProbe>>;
        try { probe = await nativeProbe(command, ['-NoLogo', '-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-Command', script], nativeProbes); }
        finally {
          // The first bounded stdout frame reports native creation identity
          // before attribute work. Preserve it even when later observation or
          // native transport fails; missing/invalid identity remains unavailable.
          const fact = nativeProbes[probeIndex];
          if (fact) {
            retention.attributeProbeIndices.push(probeIndex);
            fact.purpose = 'retained fixture all-tag Windows attribute observation'; fact.nativeIdentity = { state: 'unavailable' };
            try {
              const first = new TextDecoder('utf-8', { fatal: true }).decode(Buffer.from(fact.stdout?.base64 ?? '', 'base64')).split(/\r?\n/)[0];
              const identity = JSON.parse(first);
              if (identity.event === 'attribute-probe-identity' && identity.pid === fact.pid
                && typeof identity.creationIdentity === 'string' && /^\d+$/.test(identity.creationIdentity))
                fact.nativeIdentity = { state: 'available', pid: identity.pid, creationIdentity: identity.creationIdentity,
                  source: 'Process.StartTime FILETIME string' };
            } catch { /* original raw prefix/status remain; no identity is invented */ }
          }
        }
        const frames = new TextDecoder('utf-8', { fatal: true }).decode(probe.stdout).split(/\r?\n/);
        if (probe.code !== 0 || probe.stderr.length || frames.length !== 2 || probe.fact.nativeIdentity.state !== 'available')
          throw new Error('native attribute observation or creation identity incomplete');
        const value = JSON.parse(frames[1]);
        if (!Array.isArray(value.rows) || !value.rows.length || value.rows.length > 512) throw new Error('malformed native attribute rows');
        const rows = new Map<string, any>();
        for (const row of value.rows) {
          if (typeof row.path !== 'string' || !path.isAbsolute(row.path) || path.resolve(row.path) !== row.path || rows.has(row.path)
            || (row.present !== true && row.present !== false)) throw new Error('ambiguous native attribute membership');
          if (row.present) {
            if (typeof row.attributes !== 'string' || !/^\d+$/.test(row.attributes)
              || BigInt(row.attributes) > 4294967295n || (BigInt(row.attributes) & ~0x2fff7n) !== 0n
              || (BigInt(row.attributes) & 1024n) !== 0n) throw new Error('unknown or ReparsePoint native attributes');
          } else if (row.attributes !== null || !requests.some(request => request.optional && request.path === row.path))
            throw new Error('unexpected absent native attribute member');
          rows.set(row.path, row);
        }
        for (const request of requests) {
          if (!rows.has(request.path)) throw new Error('native attribute root observation incomplete');
          let parent = path.dirname(request.path), depth = 0;
          for (;;) {
            const ancestor = rows.get(parent);
            if (++depth > 32 || ancestor?.present !== true || (BigInt(ancestor.attributes) & 16n) === 0n)
              throw new Error('native attribute ancestor observation incomplete');
            const next = path.dirname(parent); if (next === parent) break; parent = next;
          }
        }
        probe.fact.attributeRows = value.rows;
        return rows;
      } catch (error) { nativeFailure ??= detail(error); retention.failure ??= detail(error); throw error; }
    };
    const requireAttributes = (file: string, rows: Map<string, any> | null) => {
      if (process.platform === 'win32' && rows?.get(file)?.present !== true) throw new Error(`native attributes not observed before open: ${file}`);
    };
    const attributeMembership = (directory: string, members: any[], rows: Map<string, any> | null) => {
      if (process.platform !== 'win32') return;
      // Validated optional-absence rows remain in the probe evidence, not the
      // directory membership: they have no native attributes to classify.
      const observed = [...rows!.values()].filter(row => row.present === true
        && (row.path === directory || row.path.startsWith(`${directory}${path.sep}`)))
        .map(row => ({ relative: path.relative(directory, row.path).replace(/\\/g, '/') || '.',
          kind: (BigInt(row.attributes) & 16n) !== 0n ? 'directory' : 'file' })).sort((left, right) => left.relative.localeCompare(right.relative));
      const expected = members.map(member => ({ relative: member.relative, kind: member.kind }))
        .sort((left, right) => left.relative.localeCompare(right.relative));
      if (JSON.stringify(observed) !== JSON.stringify(expected)) throw new Error('native attribute enumeration and captured membership differ');
    };
    const retainedSnapshot = (directory: string, attributes: Map<string, any> | null) => {
      const members: any[] = []; let total = 0;
      const visit = (file: string, relative: string) => {
        requireAttributes(file, attributes);
        const stat = fs.lstatSync(file, { bigint: true });
        if (stat.isSymbolicLink() || (!stat.isDirectory() && !stat.isFile())) throw new Error(`unsupported retained evidence member: ${file}`);
        const member: any = { relative, kind: stat.isDirectory() ? 'directory' : 'file', metadata: retainedMetadata(stat) };
        members.push(member);
        if (members.length > RETAINED_ENTRIES) throw new Error('retained evidence exceeded 256 entries');
        if (stat.isFile()) {
          const bytes = boundedFile(file, RETAINED_BYTES);
          if (!bytes) throw new Error(`retained evidence member disappeared: ${file}`);
          total += bytes.length;
          if (total > RETAINED_BYTES) throw new Error('retained evidence exceeded 64MiB');
          member.bytes = bytes.length; member.sha256 = digest(bytes);
        } else for (const name of retainedNames(file)) visit(path.join(file, name), relative === '.' ? name : `${relative}/${name}`);
      };
      visit(directory, '.');
      attributeMembership(directory, members, attributes);
      return members.sort((left, right) => left.relative.localeCompare(right.relative));
    };
    const comparableRetained = (members: any[], metadataName: string) => members.map(member => ({ relative: member.relative,
      kind: member.kind, ...(member.kind === 'file' ? { bytes: member.bytes, sha256: member.sha256 } : {}),
      metadata: stableRetainedMetadata(member[metadataName]) })).sort((left, right) => left.relative.localeCompare(right.relative));
    const verifyRetained = async () => {
      try {
        if (!retainedManifest || !retention.manifestPath) throw new Error('retained fixture manifest is unavailable');
        if (fs.realpathSync(retainedFixture) !== path.resolve(retainedFixture)) throw new Error('retained fixture path changed or traverses a link');
        const attributes = await nativeAttributes([{ path: retainedFixture, recursive: true }]);
        requireAttributes(retention.manifestPath, attributes);
        const manifestBytes = boundedFile(retention.manifestPath, RETAINED_MANIFEST_BYTES);
        if (!manifestBytes || manifestBytes.length !== retention.manifestBytes || digest(manifestBytes) !== retention.manifestSha256)
          throw new Error('retained fixture manifest bytes changed');
        const actual = retainedSnapshot(retainedFixture, attributes).filter(member => member.relative !== '.' && member.relative !== 'manifest.json');
        const expected = retainedManifest.fixtureMembers.map((member: any) => ({ ...member,
          relative: member.relative === '.' ? 'fixture' : `fixture/${member.relative}` }));
        if (retainedManifest.spawnLog.state === 'present') expected.push({ ...retainedManifest.spawnLog.member, relative: 'spawn-log' });
        if (JSON.stringify(comparableRetained(actual, 'metadata')) !== JSON.stringify(comparableRetained(expected, 'copyMetadata')))
          throw new Error('retained fixture membership, metadata or raw bytes changed');
        retention.lastVerifiedNs = process.hrtime.bigint().toString();
        return true;
      } catch (error) { retention.failure ??= detail(error); throw error; }
    };
    const preserveRetained = async () => {
      try {
        const parent = path.dirname(retainedFixture), canonicalParent = fs.realpathSync(parent);
        if (path.resolve(parent) !== canonicalParent || fs.lstatSync(parent).isSymbolicLink()
          || path.basename(retainedFixture) !== `${path.basename(fixture)}.retained-fixture`
          || !path.relative(fixture, retainedFixture).startsWith(`..${path.sep}`)) throw new Error('unexpected retained fixture destination');
        if (path.dirname(fixture) !== fs.realpathSync(os.tmpdir()) || !path.basename(fixture).startsWith('cg-public-lifetime-')
          || fs.lstatSync(fixture).isSymbolicLink()) throw new Error('unexpected original lifetime fixture');
        // Non-recursive mkdir and exclusive file creation refuse an existing
        // destination. A failed/partial archive is never removed or overwritten.
        await nativeAttributes([{ path: parent, recursive: false }]);
        fs.mkdirSync(retainedFixture); retention.state = 'partial';
        const spawnFile = spawnLogFor(root);
        const attributes = await nativeAttributes([{ path: fixture, recursive: true }, { path: spawnFile, recursive: false, optional: true },
          { path: retainedFixture, recursive: false }]);
        let members = 2, files = 1, rawBytes = 0; // archive root and manifest are included
        const fixtureMembers: any[] = [];
        const copy = (original: string, destination: string, relative: string, output: any[]) => {
          requireAttributes(original, attributes);
          const stat = fs.lstatSync(original, { bigint: true });
          if (stat.isSymbolicLink() || (!stat.isDirectory() && !stat.isFile())) throw new Error(`unsupported original evidence member: ${original}`);
          if (++members > RETAINED_ENTRIES) throw new Error('retained fixture exceeded 256 entries including manifest');
          const member: any = { relative, kind: stat.isDirectory() ? 'directory' : 'file', metadata: retainedMetadata(stat) };
          output.push(member);
          if (stat.isDirectory()) {
            fs.mkdirSync(destination);
            for (const name of retainedNames(original)) copy(path.join(original, name), path.join(destination, name),
              relative === '.' ? name : `${relative}/${name}`, output);
          } else {
            const bytes = boundedFile(original, RETAINED_BYTES);
            if (!bytes) throw new Error(`original evidence member disappeared: ${original}`);
            rawBytes += bytes.length; files++;
            if (rawBytes > RETAINED_BYTES || files > RETAINED_ENTRIES) throw new Error('retained fixture raw bytes/files exceeded bounds');
            descriptor(destination, 'wx', fd => { fs.writeFileSync(fd, bytes); fs.fsyncSync(fd); });
            member.bytes = bytes.length; member.sha256 = digest(bytes);
          }
          // The manifest preserves all original native stat values as strings.
          // Destination mode/times are also applied and captured, without a
          // claim that newly created objects retain original inode identities.
          fs.chmodSync(destination, Number(stat.mode & 0o777n));
          fs.utimesSync(destination, Number(stat.atimeMs) / 1000, Number(stat.mtimeMs) / 1000);
          member.copyMetadata = retainedMetadata(fs.lstatSync(destination, { bigint: true }));
        };
        copy(fixture, path.join(retainedFixture, 'fixture'), '.', fixtureMembers);
        attributeMembership(fixture, fixtureMembers, attributes);
        const spawnMembers: any[] = [];
        let spawnPresent = false;
        try { fs.lstatSync(spawnFile); spawnPresent = true; }
        catch (error) { if ((error as NodeJS.ErrnoException)?.code !== 'ENOENT') throw error; }
        if (spawnPresent) copy(spawnFile, path.join(retainedFixture, 'spawn-log'), 'spawn-log', spawnMembers);
        const spawnLog = spawnPresent ? { state: 'present', original: spawnFile, member: spawnMembers[0] }
          : { state: 'absent', original: spawnFile };
        if (spawnMembers.length > 1 || (spawnPresent && spawnMembers[0]?.kind !== 'file')) throw new Error('spawn log is not one regular file');
        // Re-read the complete original membership and bytes independently;
        // our reads may change atime only. No WAL/SHM/raw member is filtered.
        const recheckedAttributes = await nativeAttributes([{ path: fixture, recursive: true }, { path: spawnFile, recursive: false, optional: true }]);
        if (JSON.stringify(comparableRetained(retainedSnapshot(fixture, recheckedAttributes), 'metadata'))
          !== JSON.stringify(comparableRetained(fixtureMembers, 'metadata'))) throw new Error('original fixture changed during retention');
        if (spawnPresent) {
          requireAttributes(spawnFile, recheckedAttributes);
          const stat = fs.lstatSync(spawnFile, { bigint: true }), bytes = boundedFile(spawnFile, RETAINED_BYTES);
          if (!stat.isFile() || stat.isSymbolicLink() || !bytes || bytes.length !== spawnMembers[0].bytes
            || digest(bytes) !== spawnMembers[0].sha256
            || JSON.stringify(stableRetainedMetadata(retainedMetadata(stat))) !== JSON.stringify(stableRetainedMetadata(spawnMembers[0].metadata)))
            throw new Error('original spawn log changed during retention');
        } else if (!retainedAbsent(spawnFile)) throw new Error('original absent spawn log appeared during retention');
        retainedManifest = { schemaVersion: 1, originalFixture: fixture, archive: retainedFixture,
          capturedNs: process.hrtime.bigint().toString(), fixtureMembers, spawnLog,
          metadataMeaning: retention.metadata, maximumTotalBytes: RETAINED_BYTES, maximumEntriesIncludingManifest: RETAINED_ENTRIES,
          receipt: { path: receipt, outsideBothTrees: true },
          nativeAttributeProbeIndicesBeforeArchiveVerification: [...retention.attributeProbeIndices],
          disposition: 'retained through all after-removal and closed publications; outer accepted cleanup is separate' };
        const bytes = Buffer.from(JSON.stringify(retainedManifest, null, 2));
        if (bytes.length > RETAINED_MANIFEST_BYTES || rawBytes + bytes.length > RETAINED_BYTES) throw new Error('retained fixture including manifest exceeded byte bounds');
        retention.manifestPath = path.join(retainedFixture, 'manifest.json');
        descriptor(retention.manifestPath, 'wx', fd => { fs.writeFileSync(fd, bytes); fs.fsyncSync(fd); });
        retention.manifestSha256 = digest(bytes); retention.manifestBytes = bytes.length;
        retention.rawBytes = rawBytes; retention.totalBytes = rawBytes + bytes.length; retention.entries = members; retention.files = files;
        await verifyRetained(); retention.state = 'verified';
      } catch (error) { retention.failure ??= detail(error); throw error; }
    };
    try {
      fs.mkdirSync(root); fs.mkdirSync(home); fs.mkdirSync(gates);
      fs.writeFileSync(path.join(root, 'app.ts'), source);
      const graph = await CodeGraph.init(root);
      let indexFailed = false, indexPrimary: unknown;
      try { await graph.indexAll(); } catch (error) { indexFailed = true; indexPrimary = error; }
      try { graph.close(); } catch (error) { secondary.push({ phase: 'fixture-graph-close', detail: detail(error) }); if (!indexFailed) { indexFailed = true; indexPrimary = error; } }
      if (indexFailed) throw indexPrimary;
      const environment: NodeJS.ProcessEnv = { HOME: home, USERPROFILE: home,
        XDG_CONFIG_HOME: path.join(home, '.config'), APPDATA: path.join(home, 'AppData', 'Roaming'), LOCALAPPDATA: path.join(home, 'AppData', 'Local'),
        GIT_CONFIG_GLOBAL: path.join(home, '.gitconfig'), CODEX_HOME: undefined, CLAUDE_CONFIG_DIR: undefined,
        CODEGRAPH_NO_UPDATE_CHECK: '1', CODEGRAPH_TELEMETRY: '0', CODEGRAPH_NO_WATCH: '1',
        CODEGRAPH_QUERY_MODE: 'unlimited', CODEGRAPH_QUERY_POOL_SIZE: poolSize, CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: busyTimeout,
        QUERY_PUBLIC_LIFETIME_OBSERVATIONS: observations, QUERY_PUBLIC_LIFETIME_BEGIN: begin, QUERY_PUBLIC_LIFETIME_RELEASE: release,
        QUERY_PUBLIC_LIFETIME_EPOCH: epoch.name, QUERY_PUBLIC_LIFETIME_ROOT: root, QUERY_PUBLIC_LIFETIME_GATES: gates,
        QUERY_PUBLIC_LIFETIME_CANDIDATE_RELEASE: candidateRelease,
        // Do not use the old helper that replaces a daemon candidate's argv.
        QUERY_UNLIMITED_FAIL_DAEMON: '0' };
      if (process.platform === 'win32') { const drive = /^([A-Za-z]:)(\\.*)$/.exec(home)!; environment.HOMEDRIVE = drive[1]; environment.HOMEPATH = drive[2]; }
      else { environment.APPDATA = undefined; environment.LOCALAPPDATA = undefined; }
      fs.writeFileSync(environment.GIT_CONFIG_GLOBAL!, '[user]\n name = CodeGraph Lifetime Test\n email = lifetime@codegraph.invalid\n');
      phase = 'native host prebegin identity';
      client = new Client(root, epoch.mode, observations, release, 'default', {
        preload: path.join(__dirname, 'fixtures/query-public-lifetime-preload.cjs'), requestDeadlineMs: CLIENT_MS,
        closeDeadlineMs: CLOSE_MS, noWatch: true, environment });
      stdout = pipeCapture(client.child.stdout); stderr = pipeCapture(client.child.stderr);
      client.child.stdin.on('close', () => { stdinClosed = true; });
      client.child.stdin.on('error', error => { stdinFailure ??= detail(error); });
      client.child.on('exit', (code, signal) => { nativeExit = { code, signal, atNs: process.hrtime.bigint().toString() }; });
      client.child.on('close', (code, signal) => { nativeClose = { code, signal, atNs: process.hrtime.bigint().toString() }; });
      await until(async () => {
        if (nativeExit) throw new Error('CLI exited before creation identity acquisition');
        const sample = await nativeSample(client!.child.pid!);
        for (const row of sample?.processes ?? []) if (row.pid === client!.child.pid) {
          if (typeof row.creationIdentity !== 'string' || !row.creationIdentity.length) throw new Error('invalid native creation identity');
          if (hostIdentity && hostIdentity.creationIdentity !== row.creationIdentity) throw new Error('host PID reused before begin');
          hostIdentity ??= { pid: row.pid, creationIdentity: row.creationIdentity,
            sampleElapsedSeconds: sample!.elapsedSeconds, sampleToken: sample!.token, provenance: sample!.provenance,
            acquiredNs: process.hrtime.bigint().toString() };
        }
        if (nativeExit || client!.child.exitCode !== null || stdout!().closed || stderr!().closed) throw new Error('actual CLI exited or released pipes before begin');
        return hostIdentity !== null && events().some(event => event.event === 'host-awaiting-identity'
          && event.pid === client!.child.pid && event.threadId === 0);
      }, 7500, 'native host identity before CLI begin');
      persist('native identity acquired before begin'); fs.writeFileSync(begin, 'begin actual CLI');
      const initialized = await rpc('initialize', { protocolVersion: '2024-11-05', capabilities: {},
        clientInfo: { name: `public-lifetime-${epoch.name}`, version: '1' }, rootUri: pathToFileURL(root).href }).result;
      expect(initialized.serverInfo.name).toBe('codegraph');
      phase = 'selected constructor context and cold policy';
      const coldPending = status('cold'); void coldPending.catch(() => {});
      if (epoch.mode === 'writable-fallback') {
        await until(() => events().some(event => event.event === 'candidate-awaiting-exit78'), 7500, 'original native candidate awaiting identity');
        const candidate = events().find(event => event.event === 'candidate-created');
        expect(candidate).toMatchObject({ originalArguments: true, originalOptions: true, detached: true });
        expect(events().filter(event => event.event === 'engine-created')).toEqual([]);
        expect(owner()).toEqual({ writer: null, daemon: null });
        let identity: NativeIdentity | null = null;
        await until(async () => {
          const sample = await nativeSample(candidate.candidatePid);
          const row = sample?.processes.find(item => item.pid === candidate.candidatePid);
          if (row) identity = { ...row, sampleElapsedSeconds: sample!.elapsedSeconds, sampleToken: sample!.token,
            provenance: sample!.provenance, acquiredNs: process.hrtime.bigint().toString() };
          return identity !== null;
        }, 5000, 'candidate native creation identity before intentional exit');
        extended.candidates.push({ created: candidate, identity, exit: null, close: null, absent: false });
        persist('candidate creation identity acquired before service permission');
        fs.writeFileSync(candidateRelease, 'allow original candidate safe pre-service exit78');
      }
      const cold = await coldPending;
      expect(cold.lifecycle).toMatchObject({ state: 'cold', workers: 0, queued: 0, executing: 0 });
      const engines = events().filter(event => event.event === 'engine-created');
      expect(engines).toHaveLength(1); expect(engines[0]).toMatchObject({ engineOrdinal: 1,
        originalPolicyReference: true, optionsPolicyFrozen: true, contextFrozen: true });
      expect(engines[0].execution.generation).toBe(cold.ownership.generation);
      expect(engines[0].optionsPolicy).toEqual(engines[0].execution.policy);
      expect(events().filter(event => event.event === 'worker-created')).toHaveLength(0);
      if (epoch.mode === 'writable-fallback') {
        const candidate = extended.candidates[0];
        await until(() => events().some(event => event.event === 'candidate-close'), 5000, 'genuine candidate actual exit and close');
        candidate.exit = events().find(event => event.event === 'candidate-exit');
        candidate.close = events().find(event => event.event === 'candidate-close');
        expect(candidate.exit).toMatchObject({ candidatePid: candidate.identity.pid, code: 78, signal: null });
        expect(candidate.close).toMatchObject({ candidatePid: candidate.identity.pid, code: 78, signal: null,
          connected: false, channelReleased: true });
        expect(BigInt(candidate.exit.monotonicNs)).toBeLessThan(BigInt(engines[0].monotonicNs));
        expect(events().filter(event => event.event === 'candidate-awaiting-exit78')).toHaveLength(1);
        expect(events().filter(event => event.event === 'candidate-exiting-before-cli'))
          .toEqual([expect.objectContaining({ pid: candidate.identity.pid, preCli: true, code: 78 })]);
      }
      const releaseQuery = (query: string) => fs.writeFileSync(path.join(gates, `${query}.release`), 'release original native listener');
      const startsFor = (query: string) => events().filter(event => event.event === 'incoming-call' && event.query === query);
      const histories = () => events().filter(event => event.event === 'delivered-history');
      const awaitLifecycle = async (stage: string, expected: Record<string, unknown>) => {
        const deadline = performance.now() + 5000;
        const observation: any = { stage, budgetMs: 5000, startedNs: process.hrtime.bigint().toString(),
          timeoutNs: null, clientFailReturnedNs: null, finishedNs: null, samples: [],
          scope: 'local Client settlement only; no wire cancellation or server execution release implied' };
        (extended.lifecycleObservations ??= []).push(observation);
        let latest: any, samples = 0;
        let expired: Error | null = null;
        const expire = () => {
          if (expired) return;
          expired = new Error(`lifetime public lifecycle observation exceeded5000ms: ${stage}`);
          observation.timeoutNs = process.hrtime.bigint().toString();
          // Public Client.fail synchronously rejects/clears ALL pending requests
          // and clears their original30s/90s timers. The current status await
          // therefore settles through its actual owned Client promise, not a
          // detached race. Other pending healthy calls fail locally too; actual
          // server/worker ownership remains until independent finally cleanup.
          client!.fail(expired);
          observation.clientFailReturnedNs = process.hrtime.bigint().toString();
        };
        const withinDeadline = () => {
          if (expired || performance.now() >= deadline) { expire(); throw expired; }
        };
        // Armed before the first actual status request, for one shared absolute
        // budget across every sample and spacing wait. Never reset per sample.
        const watchdog = setTimeout(expire, Math.max(0, deadline - performance.now()));
        try {
          for (;;) {
            withinDeadline();
            if (++samples > 25) throw new Error(`lifetime public lifecycle observation exceeded25 samples: ${stage}`);
            const sample: any = { rpcOrdinal: ordinal + 1, startedNs: process.hrtime.bigint().toString(),
              statusAwait: 'pending', settledNs: null };
            observation.samples.push(sample);
            try { latest = await status(stage); sample.statusAwait = 'fulfilled'; }
            catch (error) { sample.statusAwait = 'rejected'; sample.failure = detail(error); throw error; }
            finally { sample.settledNs = process.hrtime.bigint().toString(); }
            withinDeadline(); // Even an already-queued late reply cannot pass.
            if (Object.entries(expected).every(([key, value]) => latest.lifecycle[key] === value)) return latest;
            await wait(Math.min(200, Math.max(0, deadline - performance.now())));
          }
        } finally {
          clearTimeout(watchdog);
          observation.finishedNs = process.hrtime.bigint().toString();
        }
      };
      const realResult = async (query: string, literal: string) => {
        await until(() => events().some(event => event.event === 'read-handler-result' && event.query === query), 5000, `real SQLite result ${query}`);
        const start = startsFor(query);
        expect(start).toHaveLength(1);
        expect(start[0].context).toMatchObject({ execution: engines[0].execution, project: cold.project });
        expect(start[0].context.sessionId).toEqual(expect.any(String));
        expect(start[0].context.admissionId).toEqual(expect.any(String));
        expect(events().filter(event => event.event === 'read-handler-start' && event.query === query))
          .toEqual([expect.objectContaining({ threadId: start[0].threadId, contextFrozen: true })]);
        expect(start[0].threadId).toBeGreaterThan(0);
        const worker = events().find(event => event.event === 'worker-created' && event.threadId === start[0].threadId);
        expect(worker).toMatchObject({ pid: hostIdentity!.pid, execution: engines[0].execution });
        expect(worker.creationOrdinal).toBeGreaterThan(0);
        const result = events().filter(event => event.event === 'read-handler-result' && event.query === query);
        expect(result).toEqual([expect.objectContaining({ isError: false, threadId: start[0].threadId })]);
        expect(result[0].text).toContain(literal);
        await until(() => events().some(event => event.event === 'worker-result' && event.id === start[0].id
          && event.context?.sessionId === start[0].context.sessionId), 5000, `actual Worker result ${query}`);
        return start[0];
      };
      const wireExplore = (id: string, query: string) => {
        const frame = JSON.stringify({ jsonrpc: '2.0', id, method: 'tools/call', params: { name: 'codegraph_explore', arguments: { query } } }) + '\n';
        const demand = { id, query, sentNs: process.hrtime.bigint().toString(), frame, disposition: 'admitted' };
        extended.wireDemands.push(demand); client!.child.stdin.write(frame); return demand;
      };
      const notifyCancel = (demand: any) => {
        demand.cancellation = { sentNs: process.hrtime.bigint().toString(), frame: JSON.stringify({ jsonrpc: '2.0',
          method: 'notifications/cancelled', params: { requestId: demand.id } }) + '\n' };
        client!.child.stdin.write(demand.cancellation.frame); demand.disposition = 'cancelled-no-delivery';
      };
      const assertNoEscape = () => {
        expect(events().filter(event => event.event === 'read-handler-start' && event.threadId === 0)).toEqual([]);
        expect(events().filter(event => event.event === 'engine-created')).toHaveLength(1);
        expect(events().filter(event => event.event === 'gate-deadline' || event.event === 'fixture-failure')).toEqual([]);
      };
      if (epoch.family === 'finite') {
      phase = 'warm unchanged built SQLite search';
      const warm = await rpc('tools/call', { name: 'codegraph_search', arguments: { query: 'finitePeerSymbol' } }).result;
      expect(warm.isError).toBeFalsy(); expect(warm.content[0].text).toContain('finitePeerSymbol'); expect(warm.content[0].text).toContain('app.ts:3');
      expect((await status('warm')).lifecycle).toMatchObject({ state: 'active', workers: 1, queued: 0, executing: 0 });
      const historyBefore = events().filter(event => event.event === 'delivered-history');
      expect(historyBefore).toEqual([]);
      phase = 'running and queued public explores before either finite expiry';
      const first = rpc('tools/call', { name: 'codegraph_explore', arguments: { query: 'finiteRunningSymbol' } });
      runningCall = first.call; running = first.result; void running.catch(() => {});
      await until(() => events().some(event => event.event === 'incoming-call' && event.query === 'finiteRunningSymbol'), 2000, 'real running call gate');
      const second = rpc('tools/call', { name: 'codegraph_explore', arguments: { query: 'finiteQueuedSymbol' } });
      queuedCall = second.call; queued = second.result; void queued.catch(() => {});
      const occupied = await status('one executing and one queued before expiry');
      expect(occupied.lifecycle).toMatchObject({ state: 'active', workers: 1, queued: 1, executing: 1, starting: 0, retiring: 0 });
      expect(Number(process.hrtime.bigint() - BigInt(runningCall.sentNs)) / 1e6).toBeLessThan(5000);
      expect(client.replies.filter(reply => reply.id === runningCall.id || reply.id === queuedCall.id)).toEqual([]);
      persist('observed one executing and one queued before expiry');
      [runningResult, queuedResult] = await Promise.all([running, queued]);
      for (const [call, result] of [[runningCall, runningResult], [queuedCall, queuedResult]]) {
        expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain('after admission (including queueing and execution)');
        expect(result.content[0].text).toContain('retry this exact call');
        expect(Number(BigInt(call.settledNs) - BigInt(call.sentNs)) / 1e6).toBeGreaterThanOrEqual(5000);
        expect(client.replies.filter(reply => reply.id === call.id)).toHaveLength(1);
      }
      phase = 'expired caller retains its occupied native worker';
      const expired = await status('both callers expired while actual worker remains held');
      expect(expired.lifecycle).toMatchObject({ workers: 1, queued: 0, executing: 1, starting: 0, retiring: 0 });
      const beforeRelease = events();
      expect(beforeRelease.filter(event => event.event === 'worker-created')).toHaveLength(1);
      expect(beforeRelease.filter(event => event.event === 'worker-exit')).toEqual([]);
      expect(beforeRelease.filter(event => event.event === 'incoming-call' && event.query === 'finiteQueuedSymbol')).toEqual([]);
      expect(beforeRelease.filter(event => event.event === 'delivered-history')).toEqual(historyBefore);
      persist('expired callers retained one occupied native slot');
      phase = 'late real SQLite completion is suppressed'; fs.writeFileSync(release, 'release original incoming listener');
      await until(() => events().some(event => event.event === 'read-handler-result' && event.query === 'finiteRunningSymbol'), 5000, 'late unchanged SQLite handler result');
      await until(() => events().some(event => event.event === 'worker-result'
        && event.context?.requestId === runningCall.id), 5000, 'late actual Worker result');
      expect((await status('late worker result drained')).lifecycle).toMatchObject({ workers: 1, queued: 0, executing: 0 });
      const late = events();
      const runningStart = late.find(event => event.event === 'incoming-call' && event.query === 'finiteRunningSymbol');
      expect(runningStart.context.requestId).toBe(runningCall.id);
      expect(runningStart.context.sessionId).toEqual(expect.any(String)); expect(runningStart.context.sessionId.length).toBeGreaterThan(0);
      expect(runningStart.context.admissionId).toEqual(expect.any(String)); expect(runningStart.context.admissionId.length).toBeGreaterThan(0);
      expect(runningStart.context.execution).toEqual(engines[0].execution);
      expect(runningStart.context.project).toEqual(cold.project);
      const actualWorker = late.find(event => event.event === 'worker-created');
      expect(actualWorker).toMatchObject({ creationOrdinal: 1, pid: hostIdentity!.pid, threadId: runningStart.threadId });
      expect(actualWorker.threadId).toBeGreaterThan(0); expect(actualWorker.execution).toEqual(engines[0].execution);
      expect(late.filter(event => event.event === 'read-handler-start' && event.query === 'finiteRunningSymbol'))
        .toEqual([expect.objectContaining({ threadId: actualWorker.threadId, contextFrozen: true })]);
      expect(late.filter(event => event.event === 'worker-result' && event.id === runningStart.id)).toHaveLength(1);
      const actualLate = late.filter(event => event.event === 'read-handler-result' && event.query === 'finiteRunningSymbol');
      expect(actualLate).toHaveLength(1); expect(actualLate[0].isError).toBe(false);
      expect(actualLate[0].text).toContain('export function finiteRunningSymbol() { return 11; }');
      expect(late.filter(event => event.event === 'incoming-call' && event.query === 'finiteQueuedSymbol')).toEqual([]);
      expect(late.filter(event => event.event === 'delivered-history')).toEqual(historyBefore);
      for (const call of [runningCall, queuedCall]) expect(client.replies.filter(reply => reply.id === call.id)).toHaveLength(1);
      phase = 'subsequent public explore delivers source and exactly one history';
      const peer = rpc('tools/call', { name: 'codegraph_explore', arguments: { query: 'finitePeerSymbol' } });
      peerResult = await peer.result;
      expect(peerResult.isError).toBeFalsy(); expect(peerResult.content[0].text).toContain('export function finitePeerSymbol() { return 33; }');
      expect(peerResult.content[0].text).toContain('app.ts'); expect(peerResult.content[0].text).not.toContain('after admission');
      await rpc('ping').result; // A same-wire barrier after successful delivery/history commit.
      const final = events();
      const peerStarts = final.filter(event => event.event === 'incoming-call'
        && event.toolName === 'codegraph_explore' && event.query === 'finitePeerSymbol');
      expect(peerStarts).toHaveLength(1);
      expect(peerStarts[0].context).toMatchObject({ execution: engines[0].execution,
        sessionId: runningStart.context.sessionId, requestId: peer.call.id, project: cold.project });
      expect(peerStarts[0].context.admissionId).toEqual(expect.any(String));
      expect(peerStarts[0].context.admissionId).not.toBe(runningStart.context.admissionId);
      expect(final.filter(event => event.event === 'delivered-history')).toEqual([expect.objectContaining({
        historyOrdinal: 1, projectRoot: fs.realpathSync(root), query: 'finitePeerSymbol', index: 1 })]);
      expect(client.replies.filter(reply => reply.id === peer.call.id)).toHaveLength(1);
      expect(final.filter(event => event.event === 'read-handler-start' && event.threadId === 0)).toEqual([]);
      expect(final.filter(event => event.event === 'engine-created')).toHaveLength(1);
      expect(final.filter(event => event.event === 'worker-created')).toHaveLength(1);
      expect(final.filter(event => event.event === 'gate-deadline' || event.event === 'worker-error')).toEqual([]);
      for (const call of [runningCall, queuedCall]) expect(client.replies.filter(reply => reply.id === call.id)).toHaveLength(1);
      phase = 'behavior complete'; persist(`all ${epoch.name} behavior predicates`);
      } else if (epoch.family === 'absent') {
        phase = 'absent policy warm built SQLite, no explore history';
        const warm = await rpc('tools/call', { name: 'codegraph_search', arguments: { query: 'finitePeerSymbol' } }).result;
        expect(warm.isError).toBeFalsy(); expect(warm.content[0].text).toContain('finitePeerSymbol');
        expect((await status('warm absent policy')).lifecycle).toMatchObject({ workers: 1, queued: 0, executing: 0 });
        expect(histories()).toEqual([]);
        phase = 'absent queued and running public cancellation with healthy occupied peer';
        const healthy = rpc('tools/call', { name: 'codegraph_explore', arguments: { query: 'absentHealthySymbol' } });
        admitted.push(healthy.result); void healthy.result.catch(() => {});
        await until(() => startsFor('absentHealthySymbol').length === 1, 5000, 'healthy original occupied worker');
        const victim = wireExplore('public-running-victim', 'absentRunningSymbol');
        await until(() => startsFor('absentRunningSymbol').length === 1, 5000, 'second actual occupied worker');
        const queuedVictim = wireExplore('public-queued-victim', 'absentQueuedSymbol');
        expect((await awaitLifecycle('two occupied plus one queued before cancellation', { executing: 2, queued: 1, starting: 0 })).lifecycle)
          .toMatchObject({ workers: 2, executing: 2, queued: 1, starting: 0 });
        notifyCancel(queuedVictim); notifyCancel(victim); await rpc('ping').result;
        expect((await awaitLifecycle('cancelled queued request removed; occupied native slots retained', { executing: 2, queued: 0 })).lifecycle)
          .toMatchObject({ workers: 2, executing: 2, queued: 0, starting: 0 });
        expect(startsFor('absentQueuedSymbol')).toEqual([]); expect(histories()).toEqual([]);
        expect(events().filter(event => event.event === 'worker-exit')).toEqual([]);
        releaseQuery('absentRunningSymbol');
        const victimStart = await realResult('absentRunningSymbol', 'export function absentRunningSymbol() { return 55; }');
        expect(victimStart.context.requestId).toBe(victim.id);
        expect((await status('cancelled running SQLite completed but healthy remains occupied')).lifecycle)
          .toMatchObject({ workers: 2, executing: 1, queued: 0 });
        expect(histories()).toEqual([]);
        expect(client.replies.filter(reply => reply.id === victim.id || reply.id === queuedVictim.id)).toEqual([]);
        if (epoch.mode === 'writable-fallback') {
          phase = 'real 30 second idle retirement does not retire occupied healthy original worker';
          const healthyStart = startsFor('absentHealthySymbol')[0];
          const occupiedWorker = events().find(event => event.event === 'worker-created' && event.threadId === healthyStart.threadId);
          const idleWorker = events().find(event => event.event === 'worker-created' && event.threadId === victimStart.threadId);
          const idleResult = events().find(event => event.event === 'worker-result' && event.id === victimStart.id);
          expect(occupiedWorker.creationOrdinal).not.toBe(idleWorker.creationOrdinal);
          for (const elapsedMs of [10000, 20000, 30000]) {
            await until(() => Number(process.hrtime.bigint() - BigInt(idleResult.monotonicNs)) / 1e6 >= elapsedMs,
              12000, `native idle grace checkpoint ${elapsedMs}`);
            await rpc('ping').result;
            const observed = await status(`occupied original identity retained at idle grace ${elapsedMs}`);
            expect(observed.lifecycle.executing).toBe(1);
            expect(events().filter(event => event.event === 'worker-exit' && event.creationOrdinal === occupiedWorker.creationOrdinal)).toEqual([]);
            expect(client.replies.filter(reply => reply.id === healthy.call.id)).toEqual([]);
          }
          await until(() => events().some(event => event.event === 'worker-exit' && event.creationOrdinal === idleWorker.creationOrdinal),
            5000, 'unused native peer actually retires after 30 seconds');
          const idleExit = events().find(event => event.event === 'worker-exit' && event.creationOrdinal === idleWorker.creationOrdinal);
          expect(Number(BigInt(idleExit.monotonicNs) - BigInt(idleResult.monotonicNs)) / 1e6).toBeGreaterThanOrEqual(30000);
          expect(idleExit.threadId).toBe(idleWorker.threadId);
          expect((await status('idle peer actually exited; original healthy still occupied')).lifecycle)
            .toMatchObject({ workers: 1, executing: 1, queued: 0, starting: 0 });
          extended.idleWitness = { occupiedWorker, idleWorker, idleResult, idleExit };
          persist('native idle peer exit with original occupied identity retained');
        }
        releaseQuery('absentHealthySymbol'); const healthyResult = await healthy.result;
        expect(healthyResult.isError).toBeFalsy();
        expect(healthyResult.content[0].text).toContain('export function absentHealthySymbol() { return 44; }');
        const healthyStart = await realResult('absentHealthySymbol', 'export function absentHealthySymbol() { return 44; }');
        expect(healthyStart.context.requestId).toBe(healthy.call.id);
        expect(healthyStart.context.sessionId).toBe(victimStart.context.sessionId);
        expect(healthyStart.context.admissionId).not.toBe(victimStart.context.admissionId);
        await rpc('ping').result;
        expect(histories()).toEqual([expect.objectContaining({ query: 'absentHealthySymbol', index: 1 })]);
        expect(client.replies.filter(reply => reply.id === healthy.call.id)).toHaveLength(1);
        if (epoch.mode === 'direct') {
          phase = 'INTERNAL same actual CLI engine two-socket session disconnect bridge';
          const listener = events().find(event => event.event === 'bridge-listening');
          expect(listener).toMatchObject({ engineOrdinal: 1, sameEngineReference: true, host: '127.0.0.1' });
          const connectBridge = async (label: string) => {
            const socket = createConnection({ host: '127.0.0.1', port: listener.port });
            const item = { label, socket, capture: pipeCapture(socket), replies: [] as any[], writes: [] as string[],
              failure: null as string | null, ended: false, closed: false };
            bridgeClients.push(item); let pending = '', seen = 0;
            const decoder = new TextDecoder('utf-8', { fatal: true });
            socket.on('data', data => {
              seen += data.length;
              if (seen > PREFIX_BYTES) { item.failure ??= 'bridge frame capture overflow'; pending = ''; return; }
              if (item.failure) return;
              try { pending += decoder.decode(data, { stream: true }); }
              catch (error) { item.failure ??= detail(error); pending = ''; return; }
              let end: number;
              while ((end = pending.indexOf('\n')) >= 0) {
                const line = pending.slice(0, end); pending = pending.slice(end + 1);
                try { item.replies.push(JSON.parse(line)); }
                catch (error) { item.failure ??= detail(error); pending = ''; return; }
              }
            });
            socket.on('error', error => { item.failure ??= detail(error); });
            socket.on('end', () => { item.ended = true; });
            socket.on('close', () => {
              item.closed = true;
              try { pending += decoder.decode(); } catch (error) { item.failure ??= detail(error); }
              if (pending.length) item.failure ??= 'unterminated final bridge frame';
            });
            await until(() => !socket.connecting || item.failure !== null, 5000, `bridge socket ${label} actual connection`);
            expect(item.failure).toBeNull();
            return item;
          };
          const sendBridge = (item: typeof bridgeClients[number], id: string | number, method: string, params: unknown = {}) => {
            const frame = JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n';
            if (item.writes.length >= 32) throw new Error('bridge writes exceeded 32 frames');
            item.writes.push(frame); item.socket.write(frame);
          };
          const a = await connectBridge('A'), b = await connectBridge('B');
          for (const item of [a, b]) {
            sendBridge(item, 'initialize', 'initialize', { protocolVersion: '2024-11-05', capabilities: {},
              clientInfo: { name: `INTERNAL-lifetime-${item.label}`, version: '1' }, rootUri: pathToFileURL(root).href });
            await until(() => item.replies.some(reply => reply.id === 'initialize'), 5000, `bridge ${item.label} initialize`);
            expect(item.replies.find(reply => reply.id === 'initialize').result.serverInfo.name).toBe('codegraph');
          }
          sendBridge(b, 1, 'tools/call', { name: 'codegraph_explore', arguments: { query: 'bridgeHealthySymbol' } });
          await until(() => startsFor('bridgeHealthySymbol').length === 1, 5000, 'bridge B healthy id1 occupied');
          sendBridge(a, 1, 'tools/call', { name: 'codegraph_explore', arguments: { query: 'bridgeRunningSymbol' } });
          await until(() => startsFor('bridgeRunningSymbol').length === 1, 5000, 'bridge A running id1 occupied');
          sendBridge(a, 2, 'tools/call', { name: 'codegraph_explore', arguments: { query: 'bridgeQueuedSymbol' } });
          expect((await awaitLifecycle('INTERNAL A queued plus both sessions occupied', { queued: 1, executing: 2 })).lifecycle)
            .toMatchObject({ workers: 2, queued: 1, executing: 2 });
          const aStart = startsFor('bridgeRunningSymbol')[0], bStart = startsFor('bridgeHealthySymbol')[0];
          expect(aStart.context.requestId).toBe(1); expect(bStart.context.requestId).toBe(1);
          expect(aStart.context.sessionId).not.toBe(bStart.context.sessionId);
          expect(aStart.context.execution).toEqual(engines[0].execution); expect(bStart.context.execution).toEqual(engines[0].execution);
          a.socket.end();
          await until(() => a.closed && events().some(event => event.event === 'bridge-transport-close' && event.label === 'A'),
            5000, 'only actual A socket and transport close');
          expect((await awaitLifecycle('INTERNAL A disconnect cancels its queue and retains both occupied slots', { queued: 0, executing: 2 })).lifecycle)
            .toMatchObject({ queued: 0, executing: 2 });
          expect(client.child.stdin.writableEnded).toBe(false); expect(nativeExit).toBeNull();
          releaseQuery('bridgeRunningSymbol'); await realResult('bridgeRunningSymbol', 'export function bridgeRunningSymbol() { return 88; }');
          expect((await status('INTERNAL A late result drained while B survives')).lifecycle).toMatchObject({ queued: 0, executing: 1 });
          expect(startsFor('bridgeQueuedSymbol')).toEqual([]); expect(a.replies.filter(reply => reply.id === 1 || reply.id === 2)).toEqual([]);
          expect(histories().map(event => event.query)).toEqual(['absentHealthySymbol']);
          releaseQuery('bridgeHealthySymbol');
          await until(() => b.replies.some(reply => reply.id === 1), 5000, 'INTERNAL B id1 real result once');
          const reply = b.replies.filter(reply => reply.id === 1); expect(reply).toHaveLength(1);
          expect(reply[0].result.isError).toBeFalsy();
          expect(reply[0].result.content[0].text).toContain('export function bridgeHealthySymbol() { return 77; }');
          await realResult('bridgeHealthySymbol', 'export function bridgeHealthySymbol() { return 77; }');
          sendBridge(b, 'barrier', 'ping'); await until(() => b.replies.some(reply => reply.id === 'barrier'), 5000, 'B delivered-history barrier');
          expect(histories().map(event => event.query)).toEqual(['absentHealthySymbol', 'bridgeHealthySymbol']);
          expect(histories()[1].historyOrdinal).not.toBe(histories()[0].historyOrdinal);
          b.socket.end(); await until(() => b.closed, 5000, 'bridge B natural socket close');
          fs.writeFileSync(path.join(gates, 'bridge-stop'), 'close fixture-owned listener');
          await until(() => events().some(event => event.event === 'bridge-listener-close'), 5000, 'fixture loopback listener actual close');
          extended.bridge.witness = { listener, aStart, bStart, engines: engines.length, publicStdinStayedLive: !client.child.stdin.writableEnded };
          persist('INTERNAL same-engine bridge complete; public stdin remained live');
        }
        phase = 'separate PUBLIC stdin EOF while an actual read is admitted means whole-host shutdown';
        const historyBeforeShutdown = histories();
        const shutdown = wireExplore('public-stdin-victim', 'shutdownRunningSymbol');
        await until(() => startsFor('shutdownRunningSymbol').length === 1, 5000, 'public shutdown call actually executing');
        expect((await status('public admitted read before whole-host stdin EOF')).lifecycle.executing).toBe(1);
        const shutdownClose = client.close(); admitted.push(shutdownClose); void shutdownClose.catch(() => {});
        await until(() => events().some(event => event.event === 'public-stdin-end'), 5000, 'actual original stdin EOF observed');
        // EOF is deliberately before release; the worker still runs the genuine
        // SQLite read if natural stop drains it. No post-host survivor is claimed.
        const eof = events().find(event => event.event === 'public-stdin-end');
        releaseQuery('shutdownRunningSymbol'); await shutdownClose;
        extended.shutdown = { demand: shutdown, eof, closed: nativeClose, historyBaseline: historyBeforeShutdown,
          meaning: 'PUBLIC whole-host shutdown, not per-session peer survival' };
        assertExtendedTail = () => {
          for (const id of [victim.id, queuedVictim.id, shutdown.id]) expect(client!.replies.filter(reply => reply.id === id)).toEqual([]);
          expect(startsFor('absentQueuedSymbol')).toEqual([]); expect(startsFor('bridgeQueuedSymbol')).toEqual([]);
          expect(histories()).toEqual(historyBeforeShutdown);
          expect(client!.replies.filter(reply => reply.id === healthy.call.id)).toHaveLength(1);
          for (const item of bridgeClients) {
            expect(item.failure).toBeNull(); expect(item.ended).toBe(true); expect(item.closed).toBe(true);
            expect(item.capture()).toMatchObject({ eof: true, closed: true, truncated: false, failure: null });
          }
          if (epoch.mode === 'direct') {
            expect(events().filter(event => event.event === 'bridge-session-created')).toEqual([
              expect.objectContaining({ label: 'A', engineOrdinal: 1, sameEngineReference: true, samePolicyReference: true }),
              expect.objectContaining({ label: 'B', engineOrdinal: 1, sameEngineReference: true, samePolicyReference: true })]);
            expect(events().filter(event => event.event === 'bridge-transport-close').map(event => event.label)).toEqual(['A', 'B']);
            expect(events().filter(event => event.event === 'bridge-socket-close')).toHaveLength(2);
            expect(events().filter(event => event.event === 'bridge-socket-close').every(event => !event.hadError)).toBe(true);
            expect(events().filter(event => event.event === 'bridge-listener-close')).toHaveLength(1);
          }
          assertNoEscape();
        };
        assertExtendedTail(); extended.state = 'behavior-complete';
        phase = 'behavior complete'; persist(`all ${epoch.name} behavioral predicates`);
      } else {
        phase = 'timer-free cold failure and observed native recovery before real SQLite success';
        expect(engines[0].execution.policy.deadline).toEqual({ kind: 'absent' });
        expect(engines[0].execution.policy.concurrency).toEqual({ kind: 'unlimited' });
        const recovered = rpc('tools/call', { name: 'codegraph_explore', arguments: { query: 'recoveryRecoveredSymbol' } });
        admitted.push(recovered.result); void recovered.result.catch(() => {});
        await until(() => events().some(event => event.event === 'startup-exit-awaiting' && event.reason === 'first-pre-ready'),
          5000, 'actual first native startup gated before safe exit');
        expect((await status('first real native worker starting with admitted call')).lifecycle)
          .toMatchObject({ state: 'starting', ready: false, workers: 1, queued: 1, executing: 0, starting: 1 });
        fs.writeFileSync(path.join(gates, 'recovery-first-release'), 'allow first pre-ready collection and self-exit');
        await until(() => events().some(event => event.event === 'startup-ready-awaiting'), 5000, 'replacement actually reached native ready boundary');
        expect((await status('actual replacement held before ready; observation does not allocate')).lifecycle)
          .toMatchObject({ state: 'recovering', ready: false, queued: 1, executing: 0, starting: 1 });
        const beforeReady = events().filter(event => event.event === 'worker-created').length;
        await rpc('ping').result; await status('recovering repeated observation no demand allocation');
        expect(events().filter(event => event.event === 'worker-created')).toHaveLength(beforeReady);
        fs.writeFileSync(path.join(gates, 'recovery-ready-release'), 'delegate original ready handshake');
        const recoveredResult = await recovered.result;
        expect(recoveredResult.isError).toBeFalsy();
        expect(recoveredResult.content[0].text).toContain('export function recoveryRecoveredSymbol() { return 121; }');
        await realResult('recoveryRecoveredSymbol', 'export function recoveryRecoveredSymbol() { return 121; }');
        await rpc('ping').result;
        expect((await status('real built SQLite recovered result active')).lifecycle).toMatchObject({ state: 'active', workers: 1, executing: 0, queued: 0 });
        expect(histories()).toEqual([expect.objectContaining({ query: 'recoveryRecoveredSymbol', index: 1 })]);
        const recoveredHistory = histories();
        extended.recoveries.push({ call: recovered.call, result: recoveredResult, ready: events().filter(event => event.event === 'worker-ready') });
        phase = 'six tagged public demands exhaust native recovery without any busy timer or serial escape';
        fs.writeFileSync(path.join(gates, 'recovery-exhaust'), 'all subsequent original native startups self-exit pre-ready');
        const demands = Array.from({ length: 6 }, (_, index) => rpc('tools/call', {
          name: 'codegraph_explore', arguments: { query: `exhaustionDemand${index}` } }));
        for (const demand of demands) { admitted.push(demand.result); void demand.result.catch(() => {}); }
        extended.exhaustion = demands.map(demand => ({ call: demand.call, result: null }));
        await until(() => startsFor('exhaustionDemand0').length === 1, 5000, 'original ready worker admitted first exhaustion demand');
        const loss = startsFor('exhaustionDemand0')[0];
        const beforeLoss = await awaitLifecycle('six admitted demands; native ready loss and pending failures still gated', { executing: 1, queued: 5, starting: 2 });
        expect(beforeLoss.lifecycle).toMatchObject({ executing: 1, queued: 5, starting: 2 });
        fs.writeFileSync(path.join(gates, 'recovery-loss-release'), 'safe self-exit of original ready worker');
        await until(() => events().some(event => event.event === 'worker-exit' && event.threadId === loss.threadId),
          5000, 'actual controlled ready-worker loss before failure burst');
        fs.writeFileSync(path.join(gates, 'recovery-fail-release'), 'allow cumulative pre-ready native failures');
        const results = await Promise.all(demands.map(demand => demand.result));
        results.forEach((result, index) => { extended.exhaustion[index].result = result;
          expect(result.isError).toBe(true); expect(result.content[0].text).toContain('query worker capacity is unavailable');
          expect(result.content[0].text).not.toContain('after admission'); expect(result.content[0].text).not.toContain('busy guidance');
          expect(client!.replies.filter(reply => reply.id === demands[index].call.id)).toHaveLength(1);
          expect(demands[index].call.settledNs).toEqual(expect.any(String));
        });
        const terminal = await status('terminal recovery unavailable; all six caller ids settled');
        expect(terminal.lifecycle).toMatchObject({ state: 'unavailable', ready: false, queued: 0, executing: 0 });
        expect(terminal.lifecycle.reason).toContain('recovery exhausted');
        const createdBeforeRefusal = events().filter(event => event.event === 'worker-created').length;
        const refused = rpc('tools/call', { name: 'codegraph_explore', arguments: { query: 'exhaustionDemand5' } });
        const refusedResult = await refused.result;
        expect(refusedResult.isError).toBe(true); expect(refusedResult.content[0].text).toContain('query worker capacity is unavailable');
        expect(client.replies.filter(reply => reply.id === refused.call.id)).toHaveLength(1);
        await rpc('ping').result; await status('unavailable status cannot resurrect'); await rpc('ping').result;
        await status('unavailable repeated status cannot allocate');
        expect(events().filter(event => event.event === 'worker-created')).toHaveLength(createdBeforeRefusal);
        // Conditional source proof: budget12 plus <=2 terminal starters.
        // Demand throttle2 can become3 pending after the one ready-worker loss;
        // replacement preserves that count until the terminal death removes1.
        // This is a maximum evidence bound, never an invented actual count.
        expect(createdBeforeRefusal).toBeLessThanOrEqual(14); expect(createdBeforeRefusal).toBeGreaterThanOrEqual(12);
        expect(events().filter(event => event.event === 'worker-ready' && event.ok === true)).toHaveLength(1);
        expect(histories()).toEqual(recoveredHistory);
        extended.exhaustionBound = { maximumCreated: 14, cumulativeBudget: 12, startupThrottle: 2,
          actualCreated: createdBeforeRefusal, proof: '12 counted losses plus at most2 terminal starters; demand throttle2, ready-loss replacement may leave3 pending, terminal death removes1' };
        extended.terminalRefusal = { call: refused.call, result: refusedResult };
        assertExtendedTail = () => {
          for (const demand of [...demands, refused]) expect(client!.replies.filter(reply => reply.id === demand.call.id)).toHaveLength(1);
          expect(client!.replies.filter(reply => reply.id === recovered.call.id)).toHaveLength(1);
          expect(histories()).toEqual(recoveredHistory);
          expect(events().filter(event => event.event === 'read-handler-start' && /^exhaustionDemand/.test(event.query))).toEqual([]);
          expect(events().filter(event => event.event === 'worker-created')).toHaveLength(createdBeforeRefusal);
          for (const failure of events().filter(event => event.event === 'safe-self-exit')) expect(failure.collected).toBe(true);
          expect(events().filter(event => event.event === 'safe-self-exit' && event.reason === 'first-pre-ready')).toHaveLength(1);
          expect(events().filter(event => event.event === 'safe-self-exit' && event.reason === 'controlled-ready-loss')).toHaveLength(1);
          assertNoEscape();
        };
        assertExtendedTail(); extended.state = 'behavior-complete';
        phase = 'behavior complete'; persist(`all ${epoch.name} behavioral predicates`);
      }
    } catch (error) { failed = true; primary = error; }
    finally {
      // Every cleanup and evidence action runs independently. Native backstop
      // cleanup never turns an earlier product/capture failure into a pass.
      await attempts('release prebegin gate', () => { fs.writeFileSync(begin, 'cleanup begin'); });
      await attempts('release actual worker gate', () => { fs.writeFileSync(release, 'cleanup release'); });
      if (epoch.mode === 'writable-fallback') await attempts('release genuine candidate before permission', () => { fs.writeFileSync(candidateRelease, 'cleanup candidate'); });
      const cleanupGates = epoch.family === 'absent' ? ['absentHealthySymbol.release', 'absentRunningSymbol.release',
        'bridgeHealthySymbol.release', 'bridgeRunningSymbol.release', 'shutdownRunningSymbol.release']
        : epoch.family === 'recovery' ? ['recovery-first-release', 'recovery-ready-release', 'recovery-loss-release', 'recovery-fail-release'] : [];
      for (const gate of cleanupGates) {
        await attempts(`release independent native gate ${gate}`, () => { fs.writeFileSync(path.join(gates, gate), 'cleanup release'); });
      }
      for (const item of bridgeClients) await attempts(`close actual INTERNAL ${item.label} socket`, async () => {
        if (!item.closed) item.socket.end();
        await until(() => item.closed, 5000, `INTERNAL ${item.label} actual native socket close`);
      });
      if (epoch.name === 'absent-direct') await attempts('close fixture-owned INTERNAL listener and sessions', async () => {
        fs.writeFileSync(path.join(gates, 'bridge-stop'), 'cleanup listener');
        if (events().some(event => event.event === 'bridge-listening')) await until(() => events().some(event => event.event === 'bridge-listener-close'),
          5000, 'INTERNAL actual listener close');
      });
      await attempts('settle admitted callers', async () => {
        if (running || queued || admitted.length) {
          let timer: NodeJS.Timeout | undefined;
          try { await Promise.race([Promise.allSettled([running, queued, ...admitted].filter(Boolean)),
            new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('lifetime caller cleanup deadline')), CLIENT_MS); })]); }
          finally { if (timer) clearTimeout(timer); }
        }
      });
      await attempts('preclose native sample', async () => { if (client) await nativeSample(client.child.pid!); });
      await attempts('actual public stdin EOF and native client close', async () => { if (client) await client.close(); });
      if (epoch.mode === 'writable-fallback') await attempts('every genuine candidate actual native identity absence', async () => {
        for (const candidate of extended.candidates) {
          candidate.exit = events().find(event => event.event === 'candidate-exit' && event.candidatePid === candidate.identity.pid) ?? null;
          candidate.close = events().find(event => event.event === 'candidate-close' && event.candidatePid === candidate.identity.pid) ?? null;
          expect(candidate.exit).toMatchObject({ code: 78, signal: null });
          expect(candidate.close).toMatchObject({ code: 78, signal: null, connected: false, channelReleased: true });
          const checkpoint = await nativeSample(candidate.identity.pid);
          if (!checkpoint) throw new Error('missing postclose candidate native checkpoint');
          await until(async () => {
            const sample = await nativeSample(candidate.identity.pid);
            candidate.absent = !!sample && sample.token > checkpoint.token && sample.elapsedSeconds > checkpoint.elapsedSeconds
              && !sample.processes.some(row => row.pid === candidate.identity.pid) && absent(candidate.identity.pid);
            if (candidate.absent) candidate.absenceSample = sample;
            return candidate.absent;
          }, 5000, 'strictly newer candidate native no-PID plus ESRCH');
        }
      });
      await attempts('independent exact host identity absence', async () => {
        if (!client) return;
        if (!hostIdentity) throw new Error('missing prebegin native host creation identity');
        if (!nativeClose) throw new Error('missing actual CLI close before absence sampling');
        // Establish the checkpoint AFTER actual close, then require a newer
        // complete sample. A live reused PID fails the independent ESRCH and
        // no-PID checks, even if its creation string differs or is coarse.
        const absenceDeadline = performance.now() + 5000;
        const acquisition = { sample: null as NativeSample | null };
        await until(async () => {
          acquisition.sample = await nativeSample(hostIdentity!.pid);
          return acquisition.sample !== null;
        }, Math.max(0, absenceDeadline - performance.now()), 'complete postclose native census checkpoint');
        const checkpoint = acquisition.sample;
        if (!checkpoint) throw new Error('missing complete postclose native census checkpoint');
        postCloseCheckpoint = { ...checkpoint, processes: checkpoint.processes.filter(row => row.pid === hostIdentity!.pid) };
        await until(async () => {
          const latest = await nativeSample(hostIdentity!.pid);
          absenceObserved = !!latest && latest.token > checkpoint.token
            && latest.elapsedSeconds > checkpoint.elapsedSeconds
            && latest.token > hostIdentity!.sampleToken && BigInt(latest.completedNs) > BigInt(nativeClose!.atNs)
            && absent(hostIdentity!.pid) && !latest.processes.some(row => row.pid === hostIdentity!.pid);
          if (absenceObserved && latest) postCloseAbsence = { ...latest, processes: latest.processes.filter(row => row.pid === hostIdentity!.pid) };
          return absenceObserved;
        }, Math.max(0, absenceDeadline - performance.now()), 'newer postclose native census and OS host absence');
      });
      await attempts('all actual native resource and worker closes', () => {
        if (!client) return;
        const checks: Array<[string, () => void]> = [
          ['actual host exit', () => expect(nativeExit).toMatchObject({ code: 0, signal: null })],
          ['actual host close', () => expect(nativeClose).toMatchObject({ code: 0, signal: null })],
          ['actual stdin close', () => expect(stdinClosed).toBe(true)],
          ['stdin failure absent', () => expect(stdinFailure).toBeNull()],
          ['IPC absent and released', () => { expect(client!.child.connected).toBe(false); expect(client!.child.channel == null).toBe(true); }],
          ['stdout complete native capture', () => expect(stdout!()).toMatchObject({ eof: true, closed: true, truncated: false, failure: null })],
          ['stderr complete native capture', () => expect(stderr!()).toMatchObject({ eof: true, closed: true, truncated: false, failure: null })],
          ['public client capture complete', () => expect(client!.capture().failure).toBeNull()],
          ['native OS observer complete', () => {
            expect(nativeFailure).toBeNull();
            for (const probe of nativeProbes) {
              expect(probe.failure).toBeNull(); expect(probe.killFailure).toBeNull(); expect(probe.forcedPipeRelease).toBe(false);
              expect(probe.exit).toMatchObject({ signal: null }); expect(probe.close).toMatchObject({ signal: null });
              expect(probe.stdout).toMatchObject({ eof: true, closed: true, truncated: false, failure: null });
              expect(probe.stderr).toMatchObject({ eof: true, closed: true, truncated: false, failure: null });
            }
          }],
          ['every created worker actual exit', () => {
            const observed = events(), created = observed.filter(event => event.event === 'worker-created');
            for (const worker of created) expect(observed.filter(event => event.event === 'worker-exit'
              && event.creationOrdinal === worker.creationOrdinal && event.threadId === worker.threadId)).toHaveLength(1);
            expect(observed.filter(event => event.event === 'worker-exit')).toHaveLength(created.length);
          }],
          ['complete final observation frame', () => expect(observationRaw.length === 0 || observationRaw.at(-1) === 10).toBe(true)],
          ['project ownership released', () => expect(owner()).toEqual({ writer: null, daemon: null })],
          ['actual selected-mode candidate ownership complete', () => {
            const log = boundedFile(spawnLogFor(root), 4096)?.toString('utf8') ?? '';
            if (epoch.mode === 'direct') expect(log).toBe('');
            else {
              expect(extended.candidates).toHaveLength(1);
              expect(log.split(/\r?\n/).filter(Boolean)).toEqual([String(extended.candidates[0].identity.pid)]);
              expect(extended.candidates[0].absent).toBe(true);
            }
          }],
          ['private daemon registry empty', () => expect(inventory(path.join(home, '.codegraph', 'daemons'))).toEqual([])],
          ['final expiry replies and history unchanged', () => {
            if (epoch.family !== 'finite') { if (!assertExtendedTail) throw new Error('extended behavioral oracle unavailable'); assertExtendedTail(); return; }
            for (const call of [runningCall, queuedCall]) expect(client!.replies.filter(reply => reply.id === call.id)).toHaveLength(1);
            const observed = events();
            expect(observed.filter(event => event.event === 'incoming-call' && event.query === 'finiteQueuedSymbol')).toEqual([]);
            expect(observed.filter(event => event.event === 'delivered-history')).toEqual([expect.objectContaining({
              historyOrdinal: 1, query: 'finitePeerSymbol', index: 1 })]);
          }],
        ];
        for (const [label, check] of checks) {
          if (failed && label === 'final expiry replies and history unchanged') {
            nativePredicates.push({ label, state: 'unrun', reason: 'behavioral episode did not complete' }); continue;
          }
          try { check(); nativePredicates.push({ label, state: 'pass' }); }
          catch (error) { nativePredicates.push({ label, state: 'fail', detail: detail(error) }); }
        }
        if (nativePredicates.some(item => item.state === 'fail')) throw new Error('Native lifetime predicates failed; see complete nativePredicates.');
      });
      persist('native cleanup complete before fixture retention and possible removal');
      await attempts('complete case bound before fixture retention', () => expect(performance.now() - started).toBeLessThan(CASE_MS));
      if (removalReady()) await attempts('preserve complete private fixture and external spawn log', preserveRetained);
      if (removalReady() && retention.state === 'verified') {
        await attempts('remove successful private fixture', async () => {
          // attempts() publishes first. A first collector/publication/close
          // failure in that pre-action publication must prevent destruction.
          if (!removalReady()) throw new Error('sticky failure prevents original fixture removal');
          await verifyRetained();
          persist('retained archive verified immediately before original removal');
          if (!removalReady()) throw new Error('retained-copy verification prevents original fixture removal');
          if (path.dirname(fixture) !== fs.realpathSync(os.tmpdir()) || !path.basename(fixture).startsWith('cg-public-lifetime-')) throw new Error('unexpected lifetime cleanup target');
          removeSpawnLog(root); retention.originalSpawnLogRemoved = retainedAbsent(spawnLogFor(root));
          fs.rmSync(fixture, { recursive: true, force: true }); observationRemoved = true;
          retention.originalFixtureRemoved = retainedAbsent(fixture);
          if (!retention.originalFixtureRemoved || !retention.originalSpawnLogRemoved) throw new Error('original fixture or spawn log remains after removal');
        });
      } else cleanup.push({ phase: 'retain failed fixture, observations and spawn log', outcome: 'retained' });
      cleanup.push({ phase: 'retain evidence archive through final publication and outer decision', outcome: 'retained',
        path: retainedFixture, state: retention.state });
      if (retention.state === 'verified') await attempts('verify retained archive through final mandatory evidence tail', async () => {
        await verifyRetained(); retention.verifiedAfterOriginalRemoval = retention.originalFixtureRemoved;
      });
      persist('closed');
    }
    if (failed) throw primary;
    expect(secondary).toEqual([]); expect(observationFailure).toBeNull(); expect(persistenceFailure).toBeNull();
    expect(collectorFailures).toEqual([]); expect(collectorFailureOverflow).toBe(false);
    expect(fileCloseFailures).toEqual([]); expect(fileCloseFailureOverflow).toBe(false); expect(nativeFailure).toBeNull();
    expect(cleanup.filter(item => item.outcome === 'fail')).toEqual([]);
    expect(retention.state).toBe('verified'); expect(retention.failure).toBeNull();
    expect(retention.originalFixtureRemoved).toBe(true); expect(retention.originalSpawnLogRemoved).toBe(true);
    expect(retention.verifiedAfterOriginalRemoval).toBe(true); expect(retention.lastVerifiedNs).toEqual(expect.any(String));
    expect(performance.now() - started).toBeLessThan(CASE_MS);
  }, epoch.caseMs);
});
