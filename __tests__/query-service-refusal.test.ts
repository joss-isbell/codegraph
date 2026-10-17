import { describe, expect, it } from 'vitest';
import * as os from 'os';
import * as path from 'path';
import { QueryPolicyRefusalError, isQueryPolicyRefusal } from '../src/mcp/query-policy-refusal';
import { servicePolicyRefusal, isServicePolicyRefusal } from '../src/mcp/query-service-compatibility';

// PRIVATE UNRUN composition proof: no service/index/engine or availability fake.
// Native compatibility-MCP proof remains mandatory in the actual joined build.
const root = path.resolve(os.tmpdir(), 'codegraph-refusal-contract');
const detail = 'the live shared service uses different query concurrency';
const guidance = `Use the same supported CODEGRAPH_QUERY_MODE, CODEGRAPH_QUERY_POOL_SIZE and CODEGRAPH_QUERY_BUSY_TIMEOUT_MS settings for ${root}; allow its existing service to drain naturally before reconnecting. No service was stopped.`;

describe('common service refusal boundary', () => {
  it.each(['incompatible', 'unknown', 'unsupported-mode'] as const)('constructs the actual common %s error without changing service guidance', reason => {
    const error = servicePolicyRefusal(reason, root, detail);
    expect(error).toBeInstanceOf(QueryPolicyRefusalError);
    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe('QueryPolicyRefusalError');
    expect(Object.isFrozen(error)).toBe(true);
    expect(error).toMatchObject({ code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason, projectRoot: root, guidance,
      message: `CodeGraph query policy refused for ${root}: ${detail}. ${guidance}` });
    expect(isServicePolicyRefusal).toBe(isQueryPolicyRefusal);
    expect(isServicePolicyRefusal(error)).toBe(true);
  });

  it('recognizes the complete actual error payload after JSON IPC serialization', () => {
    const error = servicePolicyRefusal('unknown', root, detail);
    const transported: unknown = JSON.parse(JSON.stringify({ code: error.code, reason: error.reason,
      projectRoot: error.projectRoot, guidance: error.guidance, message: error.message }));
    expect(transported).not.toBeInstanceOf(Error);
    expect(isQueryPolicyRefusal(transported)).toBe(true);
    expect(isServicePolicyRefusal(transported)).toBe(true);
  });

  it('rejects a serialized payload missing the non-enumerable Error message', () => {
    const error = servicePolicyRefusal('unknown', root, detail);
    // Raw Error serialization omits message; only the complete H payload is
    // terminal evidence. Do not reintroduce the temporary permissive guard.
    const incomplete: unknown = JSON.parse(JSON.stringify(error));
    expect(incomplete).not.toHaveProperty('message');
    expect(isQueryPolicyRefusal(incomplete)).toBe(false);
    expect(isServicePolicyRefusal(incomplete)).toBe(false);
  });
});
