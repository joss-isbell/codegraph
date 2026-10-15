import { afterEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { Worker } from 'worker_threads';
import { CodeGraph, getDatabasePath } from '../src';
import { createQueryExecutionContext, createQueryRequestContext, routeQueryRequest } from '../src/mcp/query-context';
import { resolveQueryPolicy } from '../src/mcp/query-policy';
import { QueryPool } from '../src/mcp/query-pool';
import type { MCPEngine } from '../src/mcp/engine';
const BuiltEngine: typeof import('../src/mcp/engine').MCPEngine = require('../dist/mcp/engine.js').MCPEngine;

const roots: string[] = [];
afterEach(() => { vi.unstubAllEnvs(); for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true }); });
async function fixture(symbol = 'contextSymbol'): Promise<string> {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-query-context-')));
  roots.push(root);
  fs.writeFileSync(path.join(root, 'app.ts'), `export function ${symbol}() { return 42; }\n`);
  const cg = await CodeGraph.init(root);
  try { await cg.indexAll(); } finally { cg.close(); }
  return root;
}
function message(worker: Worker, type: string): Promise<any> {
  return new Promise((resolve, reject) => {
    const listener = (m: any): void => { if (m.type === type) { worker.off('message', listener); resolve(m); } };
    worker.on('message', listener);
    worker.once('error', reject);
  });
}

describe('real query worker context', () => {
  it('constructs from the resolved exact policy without rereading or clamping legacy environment inputs', async () => {
    const root = await fixture();
    const execution = createQueryExecutionContext(resolveQueryPolicy({ poolSize: '23', busyTimeoutMs: 'unlimited' }, 'direct', 8), { readOnly: false, writer: true, synchronization: true });
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '0');
    vi.stubEnv('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', '1000');
    const pool = new QueryPool({ root, execution });
    try {
      expect(pool.size).toBe(23);
      expect(pool.execution.policy.deadline).toEqual({ kind: 'absent' });
      const response = await pool.run('codegraph_search', { query: 'contextSymbol' }, routeQueryRequest(createQueryRequestContext(execution, 'client-a', 1), 'default', root, getDatabasePath(root)));
      expect(response.content[0].text).toContain('app.ts:1');
    } finally { await pool.destroy(); }
  }, 30000);
  it('separates requested policy from actual read-only construction without activating pooling', async () => {
    const root = await fixture();
    const policy = resolveQueryPolicy({ mode: 'unlimited' }, 'read-only-fallback', 8);
    const engine: MCPEngine = new BuiltEngine({ readOnly: true, queryPool: true, queryPolicy: policy });
    try {
      await engine.ensureInitialized(root);
      expect(engine.getQueryExecutionContext()).toMatchObject({ capability: 'read-only', ownership: { writer: false, synchronization: false }, policy: { concurrency: { kind: 'unlimited' }, deadline: { kind: 'absent' } } });
      expect(engine.getQueryExecutionState().actual).toMatchObject({ dispatch: 'main-thread', concurrency: { kind: 'disabled' }, deadline: { kind: 'inapplicable' }, retention: 'ordinary', workers: 0 });
      const response = await engine.getToolHandler().execute('codegraph_search', { query: 'contextSymbol' });
      expect(response.content[0].text).toContain('app.ts:1');
      expect(fs.existsSync(path.join(root, '.codegraph', 'writer.pid'))).toBe(false);
    } finally { await engine.stop(); }
  }, 30000);
  it('returns real SQLite results with the immutable session/request/project envelope', async () => {
    const root = await fixture();
    const execution = createQueryExecutionContext(resolveQueryPolicy({}, 'direct', 8), { readOnly: false, writer: true, synchronization: true });
    const context = routeQueryRequest(createQueryRequestContext(execution, 'client-a', 1), 'default', root, getDatabasePath(root));
    const worker = new Worker(path.resolve(__dirname, '../dist/mcp/query-worker.js'), { workerData: { root, execution } });
    try {
      expect((await message(worker, 'ready')).ok).toBe(true);
      const result = message(worker, 'result');
      worker.postMessage({ type: 'call', id: 1, toolName: 'codegraph_search', args: { query: 'contextSymbol' }, context });
      const response = await result;
      expect(response.result.isError).toBeFalsy();
      expect(response.result.content[0].text).toContain('contextSymbol');
      expect(response.result.content[0].text).toContain('app.ts:1');
      expect(response.context).toEqual(context);
    } finally { await worker.terminate(); }
  }, 30000);
  it('uses the admitted canonical project rather than resolving a mutable wire path again', async () => {
    const alpha = await fixture('alphaSymbol');
    const beta = await fixture('betaSymbol');
    const execution = createQueryExecutionContext(resolveQueryPolicy({}, 'direct', 8), { readOnly: false, writer: true, synchronization: true });
    const context = routeQueryRequest(createQueryRequestContext(execution, 'client-b', 1), 'explicit', beta, getDatabasePath(beta));
    const worker = new Worker(path.resolve(__dirname, '../dist/mcp/query-worker.js'), { workerData: { root: alpha, execution } });
    try {
      expect((await message(worker, 'ready')).ok).toBe(true);
      const result = message(worker, 'result');
      worker.postMessage({ type: 'call', id: 1, toolName: 'codegraph_search', args: { projectPath: alpha, query: 'betaSymbol' }, context });
      const response = await result;
      expect(response.result.content[0].text).toContain('betaSymbol');
      expect(response.result.content[0].text).toContain('app.ts:1');
      expect(response.context.project).toEqual({ route: 'explicit', root: beta, index: getDatabasePath(beta) });
    } finally { await worker.terminate(); }
  }, 30000);
});
