/** Internal query contract. Public startup uses the legacy adapter until #15. */
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

export function queryDefaults(mode: QueryExecutionMode, cpuCount: number): QueryDefaults {
  if (mode === 'read-only-fallback') return immutable({ concurrency: { kind: 'disabled' }, deadline: { kind: 'inapplicable' }, retention: 'ordinary' });
  const maximum = Math.max(1, Math.min(cpuCount - 1, mode === 'shared-daemon' ? 16 : 2));
  return immutable({ concurrency: { kind: 'finite', maximum }, deadline: { kind: 'finite', milliseconds: QUERY_BUSY_TIMEOUT_MS }, retention: 'ordinary' });
}

function invalid(setting: string, raw: string, alternatives: string): never {
  throw new Error(`Invalid ${setting} value ${JSON.stringify(raw)}; expected ${alternatives}.`);
}

function numericOverride(setting: string, raw: string, min: number, max: number): number | 'unlimited' {
  const value = raw.trim();
  if (value === 'unlimited') return value;
  const alternatives = `decimal whole numbers ${min.toLocaleString('en-US')}–${max.toLocaleString('en-US')} or unlimited`;
  if (!/^\d+$/.test(value)) invalid(setting, raw, alternatives);
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < min || number > max) invalid(setting, raw, alternatives);
  return number;
}

/** Strict final resolver, intentionally not wired to public environment input. */
export function resolveQueryPolicy(requested: RequestedQueryPolicy, executionMode: QueryExecutionMode, cpuCount: number): ResolvedQueryPolicy {
  const defaults = queryDefaults(executionMode, cpuCount);
  const mode = requested.mode?.trim() ?? 'default';
  if (mode !== 'default' && mode !== 'unlimited') invalid('CODEGRAPH_QUERY_MODE', requested.mode!, 'default or unlimited');
  let concurrency: QueryConcurrency = mode === 'unlimited' ? { kind: 'unlimited' } : defaults.concurrency;
  let deadline: QueryDeadline = mode === 'unlimited' ? { kind: 'absent' } : defaults.deadline;
  let unlimitedPool = false;
  if (requested.poolSize !== undefined) {
    const size = numericOverride('CODEGRAPH_QUERY_POOL_SIZE', requested.poolSize, 0, MAX_QUERY_POOL_SIZE);
    unlimitedPool = size === 'unlimited';
    concurrency = size === 'unlimited' ? { kind: 'unlimited' } : size === 0 ? { kind: 'disabled' } : { kind: 'finite', maximum: size };
    // Explicit pooling in the read-only mode uses the ordinary pooled deadline.
    if (deadline.kind === 'inapplicable' && concurrency.kind !== 'disabled') deadline = { kind: 'finite', milliseconds: QUERY_BUSY_TIMEOUT_MS };
  }
  if (requested.busyTimeoutMs !== undefined) {
    const timeout = numericOverride('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', requested.busyTimeoutMs, 1_000, MAX_QUERY_BUSY_TIMEOUT_MS);
    deadline = timeout === 'unlimited' ? { kind: 'absent' } : { kind: 'finite', milliseconds: timeout };
  }
  if (concurrency.kind === 'disabled') deadline = { kind: 'inapplicable' };
  const unlimited = concurrency.kind === 'unlimited' && deadline.kind === 'absent';
  const classification = unlimited ? 'unlimited' : mode === 'unlimited' || requested.poolSize !== undefined || requested.busyTimeoutMs !== undefined ? 'custom' : 'default';
  return immutable({ requested: { ...requested }, executionMode, defaults, preset: mode, concurrency, deadline,
    retention: mode === 'unlimited' || unlimitedPool ? 'retire-idle' : 'ordinary', classification, source: 'strict' });
}

/** Original permissive parser retained for existing public settings and callers. */
export function resolveLegacyPoolSize(raw: string | undefined, cpuCount: number): number {
  if (raw !== undefined && raw !== '') {
    const n = Number(raw);
    if (Number.isFinite(n) && n >= 0 && Number.isSafeInteger(Math.floor(n))) return Math.floor(n);
  }
  return Math.max(1, Math.min(cpuCount - 1, 16));
}

/** Snapshot public settings once; the independent waiting sentinel does not activate a preset. */
export function resolveLegacyQueryPolicy(requested: RequestedQueryPolicy, executionMode: QueryExecutionMode, cpuCount: number,
  options: { poolEnabled?: boolean; defaultMaximum?: number } = {}): ResolvedQueryPolicy {
  const defaults = queryDefaults(executionMode, cpuCount);
  let size = resolveLegacyPoolSize(requested.poolSize, cpuCount);
  const defaultMaximum = options.defaultMaximum ?? (executionMode === 'shared-daemon' ? 16 : 2);
  if (requested.poolSize === undefined || requested.poolSize === '') size = Math.min(size, defaultMaximum);
  if (options.poolEnabled === false || executionMode === 'read-only-fallback') size = 0;
  const n = Number(requested.busyTimeoutMs);
  // Node clamps an overflowing timeout to 1 ms. Until strict public parsing
  // lands, preserve malformed-input fallback rather than scheduling that timer.
  const milliseconds = requested.busyTimeoutMs !== undefined && requested.busyTimeoutMs !== '' && Number.isFinite(n) && n >= 1000 && n <= MAX_QUERY_BUSY_TIMEOUT_MS ? Math.floor(n) : QUERY_BUSY_TIMEOUT_MS;
  return immutable({ requested: { ...requested }, executionMode, defaults, preset: 'default', source: 'legacy',
    concurrency: size === 0 ? { kind: 'disabled' } : { kind: 'finite', maximum: size },
    deadline: size === 0 ? { kind: 'inapplicable' } : requested.busyTimeoutMs?.trim() === 'unlimited' ? { kind: 'absent' } : { kind: 'finite', milliseconds },
    retention: 'ordinary', classification: requested.poolSize !== undefined || requested.busyTimeoutMs !== undefined ? 'custom' : 'default' });
}
