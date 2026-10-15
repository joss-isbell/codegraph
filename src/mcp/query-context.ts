import { randomUUID } from 'crypto';
import { immutable, type ResolvedQueryPolicy } from './query-policy';

/** Engine identity and authority; values are clone-safe, never session globals. */
export interface QueryExecutionContext {
  readonly generation: string;
  readonly policy: ResolvedQueryPolicy;
  readonly capability: 'read-only' | 'read-write';
  readonly ownership: Readonly<{ writer: boolean; synchronization: boolean }>;
}
export interface QueryRequestContext {
  readonly execution: QueryExecutionContext;
  readonly sessionId: string;
  readonly requestId: string | number;
  readonly admissionId: string;
  readonly project: Readonly<{ route: 'default' | 'explicit'; root: string; index: string }> | null;
}

/** Owner-only lifetime, deliberately separate from the clone-safe worker context. */
export interface QueryRequestLifetime {
  readonly signal: AbortSignal;
  /** The session commits history only after handing a response to its live transport. */
  deferDelivery?: (commit: () => void) => void;
}

export function createQueryExecutionContext(policy: ResolvedQueryPolicy,
  authority: { readOnly: boolean; writer: boolean; synchronization: boolean }): QueryExecutionContext {
  return immutable({ generation: randomUUID(), policy, capability: authority.readOnly ? 'read-only' : 'read-write',
    ownership: { writer: authority.writer, synchronization: authority.synchronization } });
}

/** Allocate before any initialization/catch-up wait; routing is resolved later. */
export function createQueryRequestContext(execution: QueryExecutionContext, sessionId: string, requestId: string | number): QueryRequestContext {
  return immutable({ execution, sessionId, requestId, admissionId: randomUUID(), project: null });
}

export function routeQueryRequest(request: QueryRequestContext, route: 'default' | 'explicit', root: string, index: string): QueryRequestContext {
  return immutable({ ...request, project: { route, root, index } });
}
