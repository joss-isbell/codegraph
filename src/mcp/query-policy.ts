/** Internal query contract, captured once before public startup inspects a project. */
export type QueryExecutionMode = 'direct' | 'writable-fallback' | 'read-only-fallback' | 'shared-daemon';
export type QueryConcurrency = Readonly<{ kind: 'disabled' }> | Readonly<{ kind: 'finite'; maximum: number }> | Readonly<{ kind: 'unlimited' }>;
export type QueryDeadline = Readonly<{ kind: 'finite'; milliseconds: number }> | Readonly<{ kind: 'absent' }> | Readonly<{ kind: 'inapplicable' }>;
export type QueryRetention = 'ordinary' | 'retire-idle';
export interface RequestedQueryPolicy {
  readonly mode?: string;
  readonly poolSize?: string;
  readonly busyTimeoutMs?: string;
}
export interface QueryDefaults {
  readonly concurrency: QueryConcurrency;
  readonly deadline: QueryDeadline;
  readonly retention: QueryRetention;
}
export interface ResolvedQueryPolicy extends QueryDefaults {
  readonly requested: RequestedQueryPolicy;
  readonly executionMode: QueryExecutionMode;
  readonly defaults: QueryDefaults;
  readonly preset: 'default' | 'unlimited';
  readonly classification: 'default' | 'custom' | 'unlimited';
  readonly source: 'strict' | 'legacy';
}
export interface QueryStartupRequest {
  readonly requested: RequestedQueryPolicy;
  readonly cpuCount: number;
}

export const QUERY_BUSY_TIMEOUT_MS = 45_000;
export const MAX_QUERY_POOL_SIZE = Number.MAX_SAFE_INTEGER;
export const MAX_QUERY_BUSY_TIMEOUT_MS = 2_147_483_647;

/** Freeze clone-safe values recursively, including values received by workers. */
export function immutable<T>(value: T): T {
  if (value && typeof value === 'object') {
    for (const child of Object.values(value)) immutable(child);
    Object.freeze(value);
  }
  return value;
}

export function requestedQueryPolicy(env: NodeJS.ProcessEnv): RequestedQueryPolicy {
  return immutable({
    ...(env.CODEGRAPH_QUERY_MODE !== undefined ? { mode: env.CODEGRAPH_QUERY_MODE } : {}),
    ...(env.CODEGRAPH_QUERY_POOL_SIZE !== undefined ? { poolSize: env.CODEGRAPH_QUERY_POOL_SIZE } : {}),
    ...(env.CODEGRAPH_QUERY_BUSY_TIMEOUT_MS !== undefined ? { busyTimeoutMs: env.CODEGRAPH_QUERY_BUSY_TIMEOUT_MS } : {}),
  });
}

/** Syntax preflight is independent of the execution mode selected later. */
export function captureQueryStartup(env: NodeJS.ProcessEnv, cpuCount: number): QueryStartupRequest {
  const requested = requestedQueryPolicy(env);
  resolveQueryPolicy(requested, 'direct', cpuCount);
  return immutable({ requested, cpuCount });
}

export function queryDefaults(mode: QueryExecutionMode, cpuCount: number): QueryDefaults {
  if (mode === 'read-only-fallback') return immutable({ concurrency: { kind: 'disabled' }, deadline: { kind: 'inapplicable' }, retention: 'ordinary' });
  const maximum = Math.max(1, Math.min(cpuCount - 1, mode === 'shared-daemon' ? 16 : 2));
  return immutable({ concurrency: { kind: 'finite', maximum }, deadline: { kind: 'finite', milliseconds: QUERY_BUSY_TIMEOUT_MS }, retention: 'ordinary' });
}

function invalid(setting: string, raw: string, alternatives: string): never {
  throw new Error(`Invalid ${setting} value ${JSON.stringify(raw)}; expected ${alternatives}.`);
}

/** Resolve a captured request for the selected execution mode. */
export function resolveQueryPolicy(requested: RequestedQueryPolicy, executionMode: QueryExecutionMode, cpuCount: number): ResolvedQueryPolicy {
  const mode = requested.mode?.trim() ?? 'default';
  if (mode !== 'default' && mode !== 'unlimited') invalid('CODEGRAPH_QUERY_MODE', requested.mode!, 'default or unlimited');
  // The preset supplies defaults only. Explicit controls keep the existing
  // Number/floor/fallback semantics, including empty and malformed values.
  const policy = resolveLegacyQueryPolicy(mode === 'unlimited' ? {
    ...requested, poolSize: requested.poolSize ?? 'unlimited', busyTimeoutMs: requested.busyTimeoutMs ?? 'unlimited',
  } : requested, executionMode, cpuCount);
  return immutable({ ...policy, requested: { ...requested }, preset: mode,
    retention: mode === 'unlimited' ? 'retire-idle' : policy.retention,
    // Retain the captured-startup diagnostic tag; it is not a numeric grammar.
    source: 'strict' });
}

/** Original permissive parser retained for existing public settings and callers. */
export function resolveLegacyPoolSize(raw: string | undefined, cpuCount: number): number {
  if (raw !== undefined && raw !== '') {
    const n = Number(raw);
    if (Number.isFinite(n) && n >= 0 && Number.isSafeInteger(Math.floor(n))) return Math.floor(n);
  }
  return Math.max(1, Math.min(cpuCount - 1, 16));
}

/** Snapshot independent public controls once; neither sentinel activates a preset. */
export function resolveLegacyQueryPolicy(requested: RequestedQueryPolicy, executionMode: QueryExecutionMode, cpuCount: number,
  options: { poolEnabled?: boolean; defaultMaximum?: number } = {}): ResolvedQueryPolicy {
  const defaults = queryDefaults(executionMode, cpuCount);
  const unlimitedPool = requested.poolSize?.trim() === 'unlimited';
  let size = resolveLegacyPoolSize(requested.poolSize, cpuCount);
  const defaultMaximum = options.defaultMaximum ?? (executionMode === 'shared-daemon' ? 16 : 2);
  if (requested.poolSize === undefined || requested.poolSize === '') size = Math.min(size, defaultMaximum);
  // Read-only fallback has no ordinary pool. An explicit unlimited or positive
  // finite selection opts these exceptional local readers in.
  const explicitReadOnlyPool = unlimitedPool || (requested.poolSize !== undefined && requested.poolSize !== ''
    && Number.isFinite(Number(requested.poolSize)) && Number(requested.poolSize) > 0
    && Number.isSafeInteger(Math.floor(Number(requested.poolSize))));
  if (options.poolEnabled === false || (executionMode === 'read-only-fallback' && !explicitReadOnlyPool)) size = 0;
  const pooledUnlimited = unlimitedPool && options.poolEnabled !== false;
  const n = Number(requested.busyTimeoutMs);
  // Node clamps an overflowing timeout to 1 ms. Preserve the ordinary fallback
  // for malformed or out-of-range inputs rather than scheduling that timer.
  const milliseconds = requested.busyTimeoutMs !== undefined && requested.busyTimeoutMs !== '' && Number.isFinite(n) && n >= 1000 && n <= MAX_QUERY_BUSY_TIMEOUT_MS ? Math.floor(n) : QUERY_BUSY_TIMEOUT_MS;
  return immutable({ requested: { ...requested }, executionMode, defaults, preset: 'default', source: 'legacy',
    concurrency: pooledUnlimited ? { kind: 'unlimited' } : size === 0 ? { kind: 'disabled' } : { kind: 'finite', maximum: size },
    deadline: size === 0 && !pooledUnlimited ? { kind: 'inapplicable' } : requested.busyTimeoutMs?.trim() === 'unlimited' ? { kind: 'absent' } : { kind: 'finite', milliseconds },
    retention: unlimitedPool ? 'retire-idle' : 'ordinary',
    classification: pooledUnlimited && requested.busyTimeoutMs?.trim() === 'unlimited' ? 'unlimited'
      : requested.poolSize !== undefined || requested.busyTimeoutMs !== undefined ? 'custom' : 'default' });
}
