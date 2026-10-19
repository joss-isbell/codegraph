import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import type { DaemonHello } from '../src/mcp/daemon';
import type { QueryDiagnostics } from '../src/mcp/query-diagnostics';
import { resolveQueryPolicy } from '../src/mcp/query-policy';
import { QueryPolicyRefusalError, isQueryPolicyRefusal } from '../src/mcp/query-policy-refusal';
import { servicePolicyRefusal, isServicePolicyRefusal, readSharedQueryService } from '../src/mcp/query-service-compatibility';

// Refusal serialization is policy-only; identity rows use disposable filesystem
// paths, never SQLite. Native compatibility-MCP proof remains mandatory.
const root = path.resolve(os.tmpdir(), 'codegraph-refusal-contract');
const detail = 'the live shared service uses different query concurrency';
const guidance = `Use the same supported CODEGRAPH_QUERY_MODE, CODEGRAPH_QUERY_POOL_SIZE and CODEGRAPH_QUERY_BUSY_TIMEOUT_MS settings for ${root}; allow its existing service to drain naturally before reconnecting. No service was stopped.`;

describe('common service refusal boundary', () => {
  it.each(['missing', 'null', 'explicit', 'foreign-root', 'foreign-index', 'relative-root', 'relative-index', 'missing-index'] as const)(
    'refuses an unproved %s default service project identity despite matching execution policy', kind => {
      const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-service-identity-')));
      const selected = path.join(fixture, 'selected'), foreign = path.join(fixture, 'foreign');
      try {
        for (const project of [selected, foreign]) {
          fs.mkdirSync(path.join(project, '.codegraph'), { recursive: true });
          // The compatibility observation is filesystem-only: no schema or
          // SQLite open belongs before admission at this boundary.
          fs.writeFileSync(path.join(project, '.codegraph/codegraph.db'), 'identity only, not a SQLite database');
        }
        const { requested, ...effective } = resolveQueryPolicy({}, 'shared-daemon', 2);
        const diagnostics: QueryDiagnostics = { schemaVersion: 1,
          implementation: { packageVersion: 'identity-test', policySchemaVersion: 1,
            support: { independentControls: true, combinedPreset: false, idleRetirement: true } },
          requested, effective, ownership: { capability: 'read-write', writerAuthority: true, synchronizationAuthority: true,
            resourceScope: 'shared-project', pid: process.pid, generation: 'identity-test' },
          lifecycle: { state: 'cold', dispatch: 'worker', ready: false, workers: 0, queued: 0, executing: 0, starting: 0,
            retiring: 0, reason: null },
          project: { route: 'default', root: selected, index: fs.realpathSync(path.join(selected, '.codegraph/codegraph.db')) } };
        const hello: DaemonHello = { codegraph: 'identity-test', pid: process.pid, protocol: 1,
          socketPath: 'identity-test-no-socket', queryExecution: diagnostics };
        expect(() => readSharedQueryService(hello, 'identity-test', selected)).not.toThrow();
        const project: any = { ...diagnostics.project! };
        if (kind === 'explicit') project.route = 'explicit';
        if (kind === 'foreign-root') project.root = foreign;
        if (kind === 'foreign-index') project.index = path.join(foreign, '.codegraph/codegraph.db');
        if (kind === 'relative-root') project.root = '.';
        if (kind === 'relative-index') project.index = '.codegraph/codegraph.db';
        if (kind === 'missing-index') project.index = path.join(selected, '.codegraph/missing.db');
        const advertised = kind === 'missing' ? undefined : kind === 'null' ? null : project;
        let refusal: unknown;
        try { readSharedQueryService({ ...hello, queryExecution: { ...diagnostics, project: advertised } } as DaemonHello, 'identity-test', selected); }
        catch (error) { refusal = error; }
        expect(refusal).toMatchObject({ code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: selected });
      } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
    });

  it.runIf(process.platform === 'win32')('accepts the same default root and index with Windows case-equivalent spelling', () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-service-case-')));
    try {
      fs.mkdirSync(path.join(fixture, '.codegraph')); fs.writeFileSync(path.join(fixture, '.codegraph/codegraph.db'), 'identity only');
      const { requested, ...effective } = resolveQueryPolicy({}, 'shared-daemon', 2);
      const hello: DaemonHello = { codegraph: 'identity-test', pid: process.pid, protocol: 1, socketPath: 'identity-test-no-socket',
        queryExecution: { schemaVersion: 1,
          implementation: { packageVersion: 'identity-test', policySchemaVersion: 1,
            support: { independentControls: true, combinedPreset: false, idleRetirement: true } },
          requested, effective, ownership: { capability: 'read-write', writerAuthority: true, synchronizationAuthority: true,
            resourceScope: 'shared-project', pid: process.pid, generation: 'identity-test' },
          lifecycle: { state: 'cold', dispatch: 'worker', ready: false, workers: 0, queued: 0, executing: 0, starting: 0,
            retiring: 0, reason: null },
          project: { route: 'default', root: fixture.toUpperCase(), index: path.join(fixture, '.codegraph/codegraph.db').toUpperCase() } } };
      expect(() => readSharedQueryService(hello, 'identity-test', fixture)).not.toThrow();
    } finally { fs.rmSync(fixture, { recursive: true, force: true }); }
  });

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
