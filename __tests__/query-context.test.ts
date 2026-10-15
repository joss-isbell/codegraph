import { describe, expect, it } from 'vitest';
import { createQueryExecutionContext, createQueryRequestContext, routeQueryRequest } from '../src/mcp/query-context';
import { resolveQueryPolicy } from '../src/mcp/query-policy';

describe('immutable query execution and request context', () => {
  it('keeps identical RPC ids in different sessions separate through routing and cloning', () => {
    const execution = createQueryExecutionContext(resolveQueryPolicy({}, 'read-only-fallback', 8), {
      readOnly: true, writer: false, synchronization: false,
    });
    const first = createQueryRequestContext(execution, 'session-a', 7);
    const second = createQueryRequestContext(execution, 'session-b', 7);
    const routed = routeQueryRequest(first, 'explicit', 'project-beta', 'index-beta');
    expect(routed).toMatchObject({ sessionId: 'session-a', requestId: 7, project: { route: 'explicit', root: 'project-beta', index: 'index-beta' }, execution: {
      capability: 'read-only', ownership: { writer: false, synchronization: false },
    } });
    expect(second.admissionId).not.toBe(first.admissionId);
    expect(first.project).toBeNull();
    expect(structuredClone(routed)).toEqual(routed);
    expect(Object.isFrozen(routed.project)).toBe(true);
    expect(Object.isFrozen(routed.execution.policy.defaults)).toBe(true);
  });
});
