/** Read-only owner evidence for ordinary service acquisition. Never acquires,
 * clears, rewrites or signals an owner to make compatibility inspection work. */
import * as fs from 'fs';
import { decodeLockInfo, getDaemonPidPath, type DaemonLockInfo } from './daemon-paths';
import { decodeWriterLockInfo, getWriterPidPath, type WriterLockInfo } from './writer-lock';
import { assertQueryServiceProject, servicePolicyRefusal } from './query-service-compatibility';
import type { DaemonHello } from './daemon';

export type OwnerRecord<T> =
  | { readonly kind: 'absent' }
  | { readonly kind: 'unknown'; readonly detail: string }
  | { readonly kind: 'live' | 'dead'; readonly value: T; readonly raw: string };
export interface QueryServiceOwners {
  readonly daemon: OwnerRecord<DaemonLockInfo>;
  readonly writer: OwnerRecord<WriterLockInfo>;
}

export function processPresence(pid: number): 'alive' | 'absent' | 'unknown' {
  if (!Number.isSafeInteger(pid) || pid <= 0) return 'unknown';
  try { process.kill(pid, 0); return 'alive'; }
  catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    return code === 'ESRCH' ? 'absent' : code === 'EPERM' ? 'alive' : 'unknown';
  }
}

function readOwner<T extends { pid: number }>(file: string, decode: (raw: string) => T | null): OwnerRecord<T> {
  try {
    const read = (): Buffer => {
      const fd = fs.openSync(file, 'r');
      try {
        const buffer = Buffer.alloc(65537);
        return buffer.subarray(0, fs.readSync(fd, buffer, 0, buffer.length, 0));
      } finally { fs.closeSync(fd); }
    };
    const bytes = read(), length = bytes.length;
    if (length > 65536) return { kind: 'unknown', detail: 'owner record exceeds the inspection bound' };
    const raw = bytes.subarray(0, length).toString('utf8');
    const value = decode(raw);
    if (!value) return { kind: 'unknown', detail: 'owner record is malformed' };
    const presence = processPresence(value.pid);
    if (presence === 'unknown') return { kind: 'unknown', detail: 'owner process liveness is unproved' };
    return { kind: presence === 'alive' ? 'live' : 'dead', value, raw };
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    return code === 'ENOENT' ? { kind: 'absent' } : { kind: 'unknown', detail: `owner record cannot be read (${code ?? 'unknown error'})` };
  }
}

export function readQueryServiceOwners(root: string): QueryServiceOwners {
  return { daemon: readOwner(getDaemonPidPath(root), decodeLockInfo), writer: readOwner(getWriterPidPath(root), decodeWriterLockInfo) };
}

export function assertReadableQueryOwners(root: string, owners: QueryServiceOwners): void {
  for (const [name, owner] of Object.entries(owners)) {
    if (owner.kind === 'unknown') throw servicePolicyRefusal('unknown', root, `${name} ${owner.detail}`);
  }
}

/** Only these separate writer modes can justify exceptional read-only reads.
 * A daemon/handover/unknown writer is still unproved service ownership. */
export function assertNoLiveQueryDaemon(root: string, owners = readQueryServiceOwners(root)): void {
  assertReadableQueryOwners(root, owners);
  if (owners.daemon.kind === 'live') throw servicePolicyRefusal('unknown', root, `live daemon PID ${owners.daemon.value.pid} has no proved compatible attachment`);
  if (owners.writer.kind === 'live' && !['direct', 'fallback', 'rebuild'].includes(owners.writer.value.mode)) {
    throw servicePolicyRefusal('unknown', root, `live ${owners.writer.value.mode} writer PID ${owners.writer.value.pid} has unproved daemon ownership`);
  }
}

/** A compatible descriptor is an observation, not proof of a held writer slot.
 * Check actual project records before granting the responding service access. */
export function assertQueryServiceOwner(root: string, hello: DaemonHello): void {
  assertQueryServiceProject(hello, root);
  const owners = readQueryServiceOwners(root);
  assertReadableQueryOwners(root, owners);
  const daemon = owners.daemon, writer = owners.writer;
  if (daemon.kind !== 'live' || writer.kind !== 'live'
    || daemon.value.pid !== hello.pid || daemon.value.version !== hello.codegraph || daemon.value.socketPath !== hello.socketPath
    || writer.value.pid !== hello.pid || writer.value.mode !== 'daemon') {
    throw servicePolicyRefusal('unknown', root, 'responding service identity does not match the live project daemon and writer records');
  }
}
