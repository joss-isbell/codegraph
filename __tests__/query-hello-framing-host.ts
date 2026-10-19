import { expect } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import { spawnLogFor } from './daemon-candidates';
import { until, saveCompatibilityEvidence } from './query-compatibility-host';
import { UnprovedClient, UnprovedTrace } from './query-unproved-host';

export type FramingCleanup = Array<{ phase: string; error: string | null }>;
export async function framingAttempt(cleanup: FramingCleanup, phase: string, action: () => unknown | Promise<unknown>): Promise<void> {
  try { await action(); cleanup.push({ phase, error: null }); }
  catch (error) { cleanup.push({ phase, error: String(error).slice(0, 8192) }); }
}
export function framingRecord(file: string): { text: string | null; prefixBase64: string | null; seenBytes: number; error: string | null } {
  let fd: number | undefined, seenBytes = 0, text: string | null = null, prefixBase64: string | null = null, error: string | null = null;
  try {
    fd = fs.openSync(file, 'r'); const stat = fs.fstatSync(fd); seenBytes = stat.size;
    if (!stat.isFile()) throw new Error('Framing record is not a regular file');
    const prefix = Buffer.alloc(Math.min(seenBytes, 65536));
    const count = fs.readSync(fd, prefix, 0, prefix.length, 0); text = prefix.subarray(0, count).toString('utf8');
    prefixBase64 = prefix.subarray(0, count).toString('base64');
    if (seenBytes > 65536 || count !== seenBytes) error = 'Framing record exceeds cap or has a short read';
  } catch (value) { if ((value as NodeJS.ErrnoException).code !== 'ENOENT') error = String(value).slice(0, 2048); }
  finally { if (fd !== undefined) try { fs.closeSync(fd); } catch (value) { error ??= String(value).slice(0, 2048); } }
  return { text, prefixBase64, seenBytes, error };
}
export const framingOwners = (root: string) => ['daemon.pid', 'writer.pid'].map(name => framingRecord(path.join(root, '.codegraph', name)));
export const framingSpawns = (root: string) => framingRecord(spawnLogFor(root));
export function framingCandidatePids(raw: ReturnType<typeof framingRecord>): number[] {
  if (raw.error) throw new Error(raw.error);
  if (raw.text === null) return [];
  if (!/^(?:[1-9]\d*\r?\n)*$/.test(raw.text)) throw new Error('Malformed framing candidate PID record');
  const values = raw.text.split(/\r?\n/).filter(Boolean).map(Number);
  if (values.some(pid => !Number.isSafeInteger(pid) || pid <= 0)) throw new Error('Invalid framing candidate PID');
  return values;
}
function absent(pid: number): boolean {
  try { process.kill(pid, 0); return false; }
  catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    if (code === 'ESRCH') return true;
    if (code === 'EPERM') return false;
    throw new Error('Unproved framing native absence: ' + String(error).slice(0, 2048));
  }
}
export async function framingCandidateAbsence(root: string): Promise<void> {
  await until(() => framingCandidatePids(framingSpawns(root)).every(absent), 30000);
}
export function requireFramingTrace(trace: UnprovedTrace, values: any[]): void {
  expect(trace.firstError).toBeNull(); expect(trace.truncated).toBe(false);
  expect(values.filter(value => value.event === 'fixture-failure')).toEqual([]);
}
export function requireFramingWorkers(trace: UnprovedTrace, values: any[]): void {
  requireFramingTrace(trace, values);
  const identity = (value: any) => value.pid + ':' + value.threadId;
  expect(values.filter(value => value.event === 'exit').map(identity).sort())
    .toEqual(values.filter(value => value.event === 'spawn').map(identity).sort());
  expect(values.filter(value => value.event === 'exit').every(value => value.code === 0 || value.code === 1)).toBe(true);
}

/** Every native cleanup phase and evidence read is independent. Inert roots,
 * index, traces and spawn log are retained on every outcome. */
export async function finishFraming(name: string, root: string, fixture: string, control: string,
  evidence: Record<string, unknown>, clients: UnprovedClient[], traces: UnprovedTrace[], workers: UnprovedTrace,
  cleanup: FramingCleanup, primary: unknown, assertFinal: () => void): Promise<void> {
  const secondary = (phase: string, error: unknown): void => {
    cleanup.push({ phase, error: String(error).slice(0, 8192) });
    try { process.stderr.write('[owned-framing-evidence-failure] ' + JSON.stringify({ phase,
      primary: String(primary ?? '').slice(0, 2048), error: String(error).slice(0, 4096), fixture }).slice(0, 8192) + '\n'); }
    catch { /* raw files and original primary remain */ }
  };
  try {
    await framingAttempt(cleanup, 'preserve-and-disarm-actual-owner-decoration', () => {
      const retainedPath = control + '.retained', source = framingRecord(control), retainedBefore = framingRecord(retainedPath);
      const disposition = { sourcePath: control, retainedPath, sourceBefore: source, retainedBefore,
        action: 'absent-or-already-retained', sourceAfter: source, retainedAfter: retainedBefore };
      evidence.armingControlDisposition = disposition;
      if (source.text !== null || source.error) {
        // Never overwrite an existing retained file or delete partial failed
        // writes. A failed rename leaves exact source bytes available.
        if (retainedBefore.text !== null || retainedBefore.error) throw new Error('Retained arming destination is occupied or unreadable');
        disposition.action = 'rename-attempted';
        try { fs.renameSync(control, retainedPath); disposition.action = 'renamed-and-retained'; }
        finally { disposition.sourceAfter = framingRecord(control); disposition.retainedAfter = framingRecord(retainedPath); }
      }
      if ([source, retainedBefore, disposition.sourceAfter, disposition.retainedAfter].some(value => value.error)) throw new Error('Arming control retention record read failed');
      if (disposition.sourceAfter.text !== null) throw new Error('Actual owner decoration remains armed');
      if (source.prefixBase64 !== null && disposition.retainedAfter.prefixBase64 !== source.prefixBase64) throw new Error('Arming control retained bytes differ');
    });
    if (evidence.utf8TailBarrier) await framingAttempt(cleanup, 'release-owned-utf8-tail-fixture-barrier-if-pending', () => {
      const barrier = evidence.utf8TailBarrier as { root: string; requesterPid?: number; acknowledgement: string; release: string };
      const before = framingRecord(barrier.release);
      const disposition = { ...barrier, before, action: 'already-released', after: before,
        acknowledgement: framingRecord(barrier.acknowledgement) };
      evidence.utf8TailBarrierDisposition = disposition;
      if (before.error) throw new Error(before.error);
      if (before.text === null && barrier.requesterPid !== undefined) {
        disposition.action = 'cleanup-abort-release-attempted';
        try {
          fs.writeFileSync(barrier.release, JSON.stringify({ root: barrier.root, requesterPid: barrier.requesterPid,
            socketId: 1, abort: true, phase: 'failed-fixture-cleanup' }) + '\n', { flag: 'wx' });
          disposition.action = 'cleanup-abort-released-and-retained';
        } finally { disposition.after = framingRecord(barrier.release); }
      }
      if (disposition.after.error || disposition.acknowledgement.error) throw new Error('UTF8 tail barrier retention read failed');
    });
    for (const client of clients) await framingAttempt(cleanup, 'native-close-' + client.name, () => client.close());
    await framingAttempt(cleanup, 'all-owned-candidates-native-absent', () => framingCandidateAbsence(root));
    for (const [index, trace] of traces.entries()) await framingAttempt(cleanup, 'final-trace-' + index,
      () => requireFramingTrace(trace, trace.read(true)));
    await framingAttempt(cleanup, 'all-native-workers-exited', () => requireFramingWorkers(workers, workers.read(true)));
    await framingAttempt(cleanup, 'final-scenario-observations', assertFinal);
  } catch (error) { secondary('unexpected-cleanup-control-failure', error); }
  finally {
    // Failure here cannot bypass native cleanup, nor replace a primary test
    // failure. A bounded fallback records a failed save independently.
    try {
      const finalOwners = framingOwners(root), finalSpawn = framingSpawns(root);
      finalOwners.forEach((value, index) => cleanup.push({ phase: 'final-owner-record-' + index, error: value.error }));
      cleanup.push({ phase: 'final-spawn-record-read', error: finalSpawn.error });
      await framingAttempt(cleanup, 'final-candidate-record-schema', () => framingCandidatePids(finalSpawn));
      await framingAttempt(cleanup, 'final-owner-records-absent', () => expect(finalOwners)
        .toEqual([{ text: null, prefixBase64: null, seenBytes: 0, error: null }, { text: null, prefixBase64: null, seenBytes: 0, error: null }]));
      const finalTraces = traces.map(trace => ({ values: trace.read(true), facts: trace.facts() }));
      for (const [index, trace] of traces.entries()) await framingAttempt(cleanup, 'persisted-trace-' + index,
        () => requireFramingTrace(trace, finalTraces[index].values));
      const retained = { root, fixture, ...evidence, primary: primary instanceof Error ? primary.stack : String(primary ?? ''),
        cleanup, ownersAfterCleanup: finalOwners, spawns: finalSpawn, clients: clients.map(client => client.facts()), traces: finalTraces,
        retention: { disposition: 'All inert disposable fixture/index/control/trace/spawn evidence retained; arming control is renamed without overwriting retained bytes to disarm decoration; native cleanup asserted separately',
          fixtureRoot: fixture, spawnLog: spawnLogFor(root), traceFiles: finalTraces.map(value => (value.facts as { file: string }).file) } };
      try {
        saveCompatibilityEvidence(name, retained, `${fixture}.evidence.json`);
      } catch (error) { secondary('final-evidence-persistence', error); }
    } catch (error) { secondary('final-evidence-snapshot', error); }
  }
  if (primary === undefined && cleanup.some(value => value.error)) {
    throw new Error('Owned framing cleanup/evidence incomplete: ' + JSON.stringify(cleanup.filter(value => value.error)));
  }
}
