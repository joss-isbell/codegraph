import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { createHash } from 'crypto';
import { spawn } from 'child_process';
import { TextDecoder } from 'util';

const detail = (value: unknown) => { try { return String(value).slice(0, 4096); } catch { return 'unformattable failure'; } };
const digest = (value: Buffer | string) => createHash('sha256').update(value).digest('hex');
type SecondaryClose = (file: string, error: unknown) => void;

function procFile(file: string, maximum: number, secondaryClose: SecondaryClose): Buffer | null {
  let fd: number | undefined, failed = false, primary: unknown, value: Buffer | null = null;
  try {
    fd = fs.openSync(file, 'r');
    const bytes = Buffer.alloc(maximum + 1); let length = 0;
    while (length < bytes.length) { const count = fs.readSync(fd, bytes, length, bytes.length - length, null); if (!count) break; length += count; }
    if (length > maximum) throw new Error(`native proc file exceeded ${maximum}: ${file}`);
    value = bytes.subarray(0, length);
  } catch (error) { if (!(fd === undefined && (error as NodeJS.ErrnoException)?.code === 'ENOENT')) { failed = true; primary = error; } }
  if (fd !== undefined) try { fs.closeSync(fd); } catch (error) {
    if (failed) secondaryClose(file, error);
    else { failed = true; primary = error; }
  }
  if (failed) throw primary;
  return value;
}

export interface NativeSample {
  token: number; elapsedSeconds: number; processes: Array<{ pid: number; creationIdentity: string }>;
  provenance: unknown; completedNs: string;
}
// Native utilities are observers of this actual ChildProcess, never alternate
// CLI/engine factories. Each owned probe has bounded capture, time, and close.
export async function nativeProbe(command: string, argv: string[], ledger: any[]) {
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
export function localCensus(ledger: any[], secondaryClose: SecondaryClose) {
  let sequence = 0;
  return async (pid: number): Promise<NativeSample> => {
    if (!Number.isSafeInteger(pid) || pid <= 0) throw new Error('invalid native host PID');
    let creationIdentity: string | null, provenance: any;
    if (process.platform === 'linux') {
      const file = `/proc/${pid}/stat`;
      // procfs reports a zero stat size; read to EOF under a strict byte cap.
      const raw = procFile(file, 65536, secondaryClose);
      creationIdentity = null;
      if (raw) {
        const text = new TextDecoder('utf-8', { fatal: true }).decode(raw), end = text.lastIndexOf(')');
        if (!text.startsWith(`${pid} (`) || end < 0) throw new Error('ambiguous native proc PID record');
        const fields = text.slice(end + 1).trim().split(/\s+/);
        if (!/^\d+$/.test(fields[19] ?? '')) throw new Error('malformed native proc start ticks');
        creationIdentity = fields[19];
      }
      const boot = procFile('/proc/sys/kernel/random/boot_id', 128, secondaryClose)?.toString('ascii').trim() ?? null;
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
export const absent = (pid: number) => { try { process.kill(pid, 0); return false; } catch (error) {
  if ((error as NodeJS.ErrnoException).code === 'ESRCH') return true; throw error;
} };
