/** Internal terminal policy refusal, shared by acquisition and bootstrap. */
export type QueryPolicyRefusalReason = 'incompatible' | 'unknown' | 'unsupported-mode';

export interface QueryPolicyRefusalData {
  readonly code: 'CODEGRAPH_QUERY_POLICY_REFUSED';
  readonly reason: QueryPolicyRefusalReason;
  readonly projectRoot: string;
  readonly guidance: string;
  readonly message: string;
}

export interface QueryPolicyRefusal extends Error, QueryPolicyRefusalData {}

export class QueryPolicyRefusalError extends Error implements QueryPolicyRefusal {
  readonly code = 'CODEGRAPH_QUERY_POLICY_REFUSED' as const;

  constructor(readonly reason: QueryPolicyRefusalReason, readonly projectRoot: string,
    readonly guidance: string, message = `Query policy ${reason} for ${projectRoot}. ${guidance}`) {
    super(message);
    this.name = 'QueryPolicyRefusalError';
    Object.freeze(this);
  }
}

/** Structural recognition also works across IPC deserialization and bundles. */
export function isQueryPolicyRefusal(value: unknown): value is QueryPolicyRefusal {
  if (!value || typeof value !== 'object') return false;
  const candidate = value as Partial<QueryPolicyRefusalData>;
  return candidate.code === 'CODEGRAPH_QUERY_POLICY_REFUSED'
    && (candidate.reason === 'incompatible' || candidate.reason === 'unknown' || candidate.reason === 'unsupported-mode')
    && typeof candidate.projectRoot === 'string' && typeof candidate.guidance === 'string'
    && typeof candidate.message === 'string';
}
