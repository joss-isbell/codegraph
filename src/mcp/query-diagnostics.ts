import type { QueryRequestContext } from './query-context';
import type { ResolvedQueryPolicy } from './query-policy';
import { immutable } from './query-policy';

/** Versioned observations, not a promise that a peer is safe to reuse. */
export interface QueryDiagnostics {
  readonly schemaVersion: 1;
  readonly implementation: Readonly<{
    packageVersion: string;
    policySchemaVersion: 1;
    support: Readonly<{ independentControls: boolean; combinedPreset: boolean; idleRetirement: boolean }>;
  }>;
  readonly requested: ResolvedQueryPolicy['requested'];
  readonly effective: Omit<ResolvedQueryPolicy, 'requested'>;
  /** Engine authority is not a claim that a project writer lease is held. */
  readonly ownership: Readonly<{
    capability: 'read-only' | 'read-write'; writerAuthority: boolean; synchronizationAuthority: boolean;
    resourceScope: 'shared-project' | 'local-engine'; pid: number; generation: string;
  }>;
  readonly lifecycle: Readonly<{
    state: 'disabled' | 'cold' | 'starting' | 'active' | 'recovering' | 'retiring' | 'retired' | 'draining' | 'unavailable' | 'stopped';
    dispatch: 'worker' | 'main-thread' | 'unavailable'; ready: boolean; workers: number;
    queued: number; executing: number; starting: number; retiring: number; reason: string | null;
  }>;
  readonly project: QueryRequestContext['project'];
}

export type ObservedQueryDiagnostics = { readonly [K in keyof QueryDiagnostics]: QueryDiagnostics[K] | 'unknown' };

const record = (value: unknown): Record<string, unknown> | null =>
  value !== null && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : null;
const oneOf = (value: unknown, values: readonly string[]): boolean => typeof value === 'string' && values.includes(value);
const count = (value: unknown): boolean => typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
const concurrency = (value: unknown): boolean => {
  const item = record(value);
  return !!item && (oneOf(item.kind, ['disabled', 'unlimited']) || (item.kind === 'finite' && count(item.maximum) && Number(item.maximum) > 0));
};
const deadline = (value: unknown): boolean => {
  const item = record(value);
  return !!item && (oneOf(item.kind, ['absent', 'inapplicable']) || (item.kind === 'finite'
    && count(item.milliseconds) && Number(item.milliseconds) >= 1000 && Number(item.milliseconds) <= 2147483647));
};
const defaults = (value: unknown): boolean => {
  const item = record(value);
  return !!item && concurrency(item.concurrency) && deadline(item.deadline) && oneOf(item.retention, ['ordinary', 'retire-idle']);
};

/**
 * Read an optional peer observation. Each incomplete section stays unknown;
 * neither package equality nor absent fields implies policy compatibility.
 */
export function readQueryDiagnostics(value: unknown): ObservedQueryDiagnostics {
  const unknown: ObservedQueryDiagnostics = { schemaVersion: 'unknown', implementation: 'unknown', requested: 'unknown',
    effective: 'unknown', ownership: 'unknown', lifecycle: 'unknown', project: 'unknown' };
  const item = record(value);
  if (item?.schemaVersion !== 1) return immutable(unknown);
  const implementation = record(item.implementation), support = record(implementation?.support);
  const requested = record(item.requested), effective = record(item.effective), ownership = record(item.ownership), lifecycle = record(item.lifecycle), project = record(item.project);
  const known = <K extends keyof QueryDiagnostics>(key: K, valid: boolean): QueryDiagnostics[K] | 'unknown' => {
    if (!valid) return 'unknown';
    try { return immutable(structuredClone(item[key])) as QueryDiagnostics[K]; } catch { return 'unknown'; }
  };
  return immutable({ schemaVersion: 1,
    implementation: known('implementation', !!implementation && typeof implementation.packageVersion === 'string' && implementation.packageVersion.length > 0
      && implementation.policySchemaVersion === 1 && !!support && ['independentControls', 'combinedPreset', 'idleRetirement'].every(key => typeof support[key] === 'boolean')),
    requested: known('requested', !!requested && ['mode', 'poolSize', 'busyTimeoutMs'].every(key => requested[key] === undefined || typeof requested[key] === 'string')),
    effective: known('effective', !!effective && defaults(effective) && defaults(effective.defaults)
      && oneOf(effective.executionMode, ['direct', 'writable-fallback', 'read-only-fallback', 'shared-daemon'])
      && oneOf(effective.preset, ['default', 'unlimited']) && oneOf(effective.classification, ['default', 'custom', 'unlimited']) && oneOf(effective.source, ['legacy', 'strict'])),
    ownership: known('ownership', !!ownership && oneOf(ownership.capability, ['read-only', 'read-write'])
      && typeof ownership.writerAuthority === 'boolean' && typeof ownership.synchronizationAuthority === 'boolean'
      && oneOf(ownership.resourceScope, ['shared-project', 'local-engine']) && count(ownership.pid) && Number(ownership.pid) > 0
      && typeof ownership.generation === 'string' && ownership.generation.length > 0),
    lifecycle: known('lifecycle', !!lifecycle && oneOf(lifecycle.state, ['disabled', 'cold', 'starting', 'active', 'recovering', 'retiring', 'retired', 'draining', 'unavailable', 'stopped'])
      && oneOf(lifecycle.dispatch, ['worker', 'main-thread', 'unavailable']) && typeof lifecycle.ready === 'boolean'
      && ['workers', 'queued', 'executing', 'starting', 'retiring'].every(key => count(lifecycle[key]))
      && (lifecycle.reason === null || typeof lifecycle.reason === 'string')),
    project: known('project', item.project === null || (!!project && oneOf(project.route, ['default', 'explicit'])
      && typeof project.root === 'string' && typeof project.index === 'string')) });
}
