import * as fs from 'fs';
import * as path from 'path';
import { getCodeGraphDir } from '../directory';
import type { DaemonHello } from './daemon';
import { readQueryDiagnostics, type QueryDiagnostics } from './query-diagnostics';
import type { ResolvedQueryPolicy } from './query-policy';
import { QueryPolicyRefusalError, isQueryPolicyRefusal, type QueryPolicyRefusal } from './query-policy-refusal';

export type ServicePolicyRefusal = QueryPolicyRefusal;
export { isQueryPolicyRefusal as isServicePolicyRefusal };

export function servicePolicyRefusal(reason: ServicePolicyRefusal['reason'], projectRoot: string, detail: string): ServicePolicyRefusal {
  const guidance = `Use the same supported CODEGRAPH_QUERY_MODE, CODEGRAPH_QUERY_POOL_SIZE and CODEGRAPH_QUERY_BUSY_TIMEOUT_MS settings for ${projectRoot}; allow its existing service to drain naturally before reconnecting. No service was stopped.`;
  return new QueryPolicyRefusalError(reason, projectRoot, guidance,
    `CodeGraph query policy refused for ${projectRoot}: ${detail}. ${guidance}`);
}

/** A copied live owner record cannot authorize queries against another index.
 * Filesystem identity only: never open SQLite, discover an ancestor, or mutate
 * either project's records while proving the service's default route. */
export function assertQueryServiceProject(hello: DaemonHello, projectRoot: string): void {
  const project = readQueryDiagnostics(hello.queryExecution).project;
  if (project === 'unknown' || project === null || project.route !== 'default'
    || !path.isAbsolute(project.root) || !path.isAbsolute(project.index)) {
    throw servicePolicyRefusal('unknown', projectRoot, 'the live service has no proved canonical default project and index');
  }
  const identity = (file: string): string => {
    const native = fs.realpathSync.native(file);
    return process.platform === 'win32' ? native.toLowerCase() : native;
  };
  let matches = false;
  try {
    const root = fs.realpathSync.native(projectRoot);
    const index = path.join(getCodeGraphDir(root), 'codegraph.db');
    matches = fs.statSync(index).isFile()
      && identity(project.root) === identity(root) && identity(project.index) === identity(index);
  } catch { /* Missing/unreadable identity is unknown, never fallback permission. */ }
  if (!matches) throw servicePolicyRefusal('unknown', projectRoot, 'the live service default project and index do not match the selected own index');
}

/** Shared transport observation only. It never grants caller policy permission. */
export function readSharedQueryService(hello: DaemonHello, expectedVersion: string, projectRoot: string):
  Pick<QueryDiagnostics, 'implementation' | 'effective' | 'ownership' | 'lifecycle'> {
  const refuse = (reason: ServicePolicyRefusal['reason'], detail: string): never => { throw servicePolicyRefusal(reason, projectRoot, detail); };
  if (hello.protocol !== 1 || hello.codegraph !== expectedVersion) {
    refuse('unknown', `service version ${hello.codegraph} has no proved wire compatibility with ${expectedVersion}`);
  }
  assertQueryServiceProject(hello, projectRoot);
  const observed = readQueryDiagnostics(hello.queryExecution);
  if (observed.implementation === 'unknown' || observed.effective === 'unknown' || observed.ownership === 'unknown'
    || observed.lifecycle === 'unknown') return refuse('unknown', 'the live service has incomplete query execution diagnostics');
  const { implementation, effective, ownership } = observed;
  if (implementation.packageVersion !== expectedVersion || !implementation.support.independentControls
    || (effective.retention === 'retire-idle' && !implementation.support.idleRetirement)
    || (effective.preset === 'unlimited' && !implementation.support.combinedPreset)) {
    refuse('unknown', 'the live service has no proved implementation support for the requested query controls');
  }
  if (effective.executionMode !== 'shared-daemon'
    || ownership.resourceScope !== 'shared-project' || ownership.pid !== hello.pid
    || ownership.capability !== 'read-write' || !ownership.writerAuthority || !ownership.synchronizationAuthority) {
    refuse('unknown', 'the responding service does not prove the expected shared execution identity');
  }
  return { implementation, effective, ownership, lifecycle: observed.lifecycle };
}

/** Compare actual behavior; provenance labels and raw request spellings are observations only. */
export function assertCompatibleQueryService(hello: DaemonHello, expectedVersion: string,
  prospective: ResolvedQueryPolicy, projectRoot: string): void {
  const refuse = (reason: ServicePolicyRefusal['reason'], detail: string): never => { throw servicePolicyRefusal(reason, projectRoot, detail); };
  const { implementation, effective } = readSharedQueryService(hello, expectedVersion, projectRoot);
  if (prospective.retention === 'retire-idle' && !implementation.support.idleRetirement) {
    refuse('unknown', 'the live service has no proved implementation support for the requested reader retention');
  }
  if (prospective.preset === 'unlimited' && !implementation.support.combinedPreset) {
    refuse('unsupported-mode', 'the live shared service does not support the combined unlimited preset');
  }
  if (prospective.executionMode !== 'shared-daemon') {
    refuse('unknown', 'the request does not describe prospective shared execution');
  }
  const concurrencyMatches = effective.concurrency.kind === prospective.concurrency.kind
    && (effective.concurrency.kind !== 'finite' || (prospective.concurrency.kind === 'finite'
      && effective.concurrency.maximum === prospective.concurrency.maximum));
  const deadlineMatches = effective.deadline.kind === prospective.deadline.kind
    && (effective.deadline.kind !== 'finite' || (prospective.deadline.kind === 'finite'
      && effective.deadline.milliseconds === prospective.deadline.milliseconds));
  if (!concurrencyMatches || !deadlineMatches || effective.retention !== prospective.retention) {
    refuse('incompatible', 'the live shared service uses different query concurrency, deadline or reader retention');
  }
}
