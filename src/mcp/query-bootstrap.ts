import type { ChildProcess } from 'node:child_process';
import type { QueryPolicyRefusal, QueryPolicyRefusalData } from './query-policy-refusal';
import { QueryPolicyRefusalError, isQueryPolicyRefusal } from './query-policy-refusal';

/** Startup-policy permission only; never service authority or compatible attachment. */
export type BootstrapStatus =
  | { protocol: 1; kind: 'permitted' }
  | { protocol: 1; kind: 'refused'; refusal: QueryPolicyRefusalData }
  | { protocol: 1; kind: 'refused'; detailsOmitted: true };

export type DaemonBootstrapResult =
  | { kind: 'permitted'; candidatePid: number }
  | { kind: 'refused'; refusal: QueryPolicyRefusal }
  | { kind: 'unavailable'; cause: 'spawn-error' | 'pre-service-exit';
      exitCode: number | null; signal: NodeJS.Signals | null };

const BOOTSTRAP_MILLISECONDS = 15_000;
const STATUS_BYTES = 65_536;

/** Observe an inert candidate without acquiring service authority. */
export function awaitDaemonBootstrap(child: ChildProcess, projectRoot: string): Promise<DaemonBootstrapResult> {
  return new Promise(resolve => {
    let settled = false, permittedObserved = false;
    const unknown = (detail: string): DaemonBootstrapResult => ({ kind: 'refused', refusal: new QueryPolicyRefusalError(
      'unknown', projectRoot, 'Inspect the candidate startup and retry only after its policy and ownership are known.', detail) });
    const settle = (result: DaemonBootstrapResult) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      child.removeListener('message', onMessage);
      child.removeListener('exit', onExit);
      child.removeListener('error', onError);
      child.removeListener('disconnect', onDisconnect);
      child.channel?.unref();
      if (child.connected) child.disconnect();
      child.unref();
      resolve(Object.freeze(result));
    };
    const onExit = (exitCode: number | null, signal: NodeJS.Signals | null) => settle(permittedObserved
      ? unknown('Candidate exited after advertising startup permission; its service state is unknown.')
      : { kind: 'unavailable', cause: 'pre-service-exit', exitCode, signal });
    const onError = () => settle(!permittedObserved && child.pid === undefined
      ? { kind: 'unavailable', cause: 'spawn-error', exitCode: child.exitCode, signal: child.signalCode }
      : unknown('Candidate bootstrap failed without proved pre-service unavailability.'));
    const onDisconnect = () => {
      if (permittedObserved) settle(unknown('Candidate bootstrap IPC disconnected before acknowledgment.'));
      else if (child.exitCode !== null || child.signalCode !== null) onExit(child.exitCode, child.signalCode);
      // A channel can close just before native exit. Keep the original deadline
      // while waiting for actual pre-permission exit; a still-live child is unknown.
    };
    const onMessage = (value: unknown) => {
      if (settled) return;
      let encoded: string | undefined;
      try { encoded = JSON.stringify(value); } catch { /* invalid wire value */ }
      if (encoded === undefined || Buffer.byteLength(encoded, 'utf8') > STATUS_BYTES
        || !value || typeof value !== 'object' || Array.isArray(value)) {
        settle(unknown('Candidate bootstrap status is malformed or exceeds 65536 bytes.')); return;
      }
      const status = value as Record<string, unknown>;
      if (status.protocol !== 1) { settle(unknown('Candidate bootstrap protocol is unknown.')); return; }
      if (Object.prototype.hasOwnProperty.call(status, 'detailsOmitted')) {
        const keys = Object.keys(status);
        settle(unknown(status.kind === 'refused' && status.detailsOmitted === true && keys.length === 3
          && keys.every(key => key === 'protocol' || key === 'kind' || key === 'detailsOmitted')
          ? 'Candidate explicitly refused startup; refusal details exceeded the transport bound.'
          : 'Candidate compact bootstrap refusal is malformed.'));
        return;
      }
      if (status.kind === 'refused' && isQueryPolicyRefusal(status.refusal)) {
        const refusal = status.refusal;
        settle({ kind: 'refused', refusal: new QueryPolicyRefusalError(refusal.reason, refusal.projectRoot,
          refusal.guidance, refusal.message) }); return;
      }
      if (status.kind === 'permitted') {
        if (permittedObserved) return;
        permittedObserved = true;
        const candidatePid = child.pid;
        if (candidatePid === undefined || !Number.isInteger(candidatePid) || candidatePid <= 0) {
          settle(unknown('Candidate startup permission has no proved spawned process identity.')); return;
        }
        try {
          child.send({ protocol: 1, kind: 'acknowledged' }, error => settle(error
            ? unknown('Candidate bootstrap acknowledgment could not be sent; its service state is unknown.')
            : { kind: 'permitted', candidatePid }));
        } catch {
          settle(unknown('Candidate bootstrap acknowledgment failed; its service state is unknown.'));
        }
        return;
      }
      settle(unknown('Candidate bootstrap status is unknown.'));
    };
    const timer = setTimeout(() => settle(unknown('Candidate bootstrap acknowledgment did not complete within 15000ms.')),
      BOOTSTRAP_MILLISECONDS);
    child.on('message', onMessage);
    child.on('exit', onExit);
    child.on('error', onError);
    child.on('disconnect', onDisconnect);
    if (child.exitCode !== null || child.signalCode !== null) onExit(child.exitCode, child.signalCode);
    else if (!child.connected && child.pid !== undefined) onDisconnect();
  });
}

/** A child stays inert until its parent acknowledges the bounded policy status. */
export function confirmDaemonBootstrap(projectRoot: string): Promise<void> {
  if (typeof process.send !== 'function') return Promise.resolve();
  return childBootstrapExchange({ protocol: 1, kind: 'permitted' }, projectRoot, true);
}

/** Explicit policy refusal is terminal status, never stderr interpretation. */
export function reportDaemonBootstrapRefusal(refusal: QueryPolicyRefusal): Promise<void> {
  if (typeof process.send !== 'function') return Promise.resolve();
  const data: QueryPolicyRefusalData = { code: refusal.code, reason: refusal.reason, projectRoot: refusal.projectRoot,
    guidance: refusal.guidance, message: refusal.message };
  const fullStatus: BootstrapStatus = { protocol: 1, kind: 'refused', refusal: data };
  const status: BootstrapStatus = Buffer.byteLength(JSON.stringify(fullStatus), 'utf8') > STATUS_BYTES
    ? { protocol: 1, kind: 'refused', detailsOmitted: true } : fullStatus;
  return childBootstrapExchange(status, refusal.projectRoot, false);
}

function childBootstrapExchange(status: BootstrapStatus, projectRoot: string, acknowledgment: boolean): Promise<void> {
  const unknown = (message: string) => new QueryPolicyRefusalError('unknown', projectRoot,
    'Inspect the candidate startup and retry only after its policy and ownership are known.', message);
  return new Promise((resolve, reject) => {
    let settled = false;
    const settle = (error?: QueryPolicyRefusal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      process.removeListener('message', onMessage);
      process.removeListener('error', onError);
      process.removeListener('disconnect', onDisconnect);
      try {
        process.channel?.unref();
        if (process.connected) process.disconnect();
      } catch {
        error ??= unknown('Candidate bootstrap IPC release failed.');
      }
      if (error) reject(error); else resolve();
    };
    const onError = () => settle(unknown('Candidate bootstrap IPC failed.'));
    const onDisconnect = () => settle(unknown('Candidate bootstrap IPC disconnected before acknowledgment.'));
    const onMessage = (value: unknown) => {
      if (settled || !acknowledgment) return;
      let encoded: string | undefined;
      try { encoded = JSON.stringify(value); } catch { /* invalid acknowledgment */ }
      if (encoded === undefined || Buffer.byteLength(encoded, 'utf8') > STATUS_BYTES
        || !value || typeof value !== 'object' || Array.isArray(value)
        || (value as { protocol?: unknown }).protocol !== 1
        || (value as { kind?: unknown }).kind !== 'acknowledged') {
        settle(unknown('Candidate bootstrap acknowledgment is malformed or exceeds 65536 bytes.'));
      } else settle();
    };
    const timer = setTimeout(() => settle(unknown('Candidate bootstrap exchange did not complete within 15000ms.')),
      BOOTSTRAP_MILLISECONDS);
    process.on('message', onMessage);
    process.on('error', onError);
    process.on('disconnect', onDisconnect);
    if (!process.connected || typeof process.send !== 'function') {
      settle(unknown('Candidate bootstrap IPC is unavailable without proved permission.')); return;
    }
    let encoded: string;
    try { encoded = JSON.stringify(status); }
    catch { settle(unknown('Candidate bootstrap status cannot be serialized.')); return; }
    if (Buffer.byteLength(encoded, 'utf8') > STATUS_BYTES) {
      settle(unknown('Candidate bootstrap status exceeds 65536 bytes.')); return;
    }
    try {
      process.send(status, error => {
        if (error) settle(unknown('Candidate bootstrap status could not be sent.'));
        else if (!acknowledgment) settle();
      });
    } catch {
      settle(unknown('Candidate bootstrap status send failed.'));
    }
  });
}
