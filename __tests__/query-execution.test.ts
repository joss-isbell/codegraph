import { afterEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { Worker } from 'worker_threads';
import { execFileSync } from 'child_process';
import { CodeGraph, getDatabasePath } from '../src';
import { createQueryExecutionContext, createQueryRequestContext, routeQueryRequest } from '../src/mcp/query-context';
import { resolveQueryPolicy, type QueryExecutionMode } from '../src/mcp/query-policy';
import type { MCPEngine } from '../src/mcp/engine';
const BuiltEngine: typeof import('../src/mcp/engine').MCPEngine = require('../dist/mcp/engine.js').MCPEngine;
const BuiltQueryPool: typeof import('../src/mcp/query-pool').QueryPool = require('../dist/mcp/query-pool.js').QueryPool;

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
async function linkedFixture(): Promise<string> {
  const parent = await fixture('ancestorSymbol');
  fs.writeFileSync(path.join(parent, '.gitignore'), '.codegraph*\nlinked/\n');
  const git = (...args: string[]): void => { execFileSync('git', ['-C', parent, ...args], { stdio: 'pipe' }); };
  git('init');
  git('add', 'app.ts', '.gitignore');
  git('-c', 'user.name=Query fixture', '-c', 'user.email=query@example.invalid', 'commit', '-m', 'fixture');
  const linked = path.join(parent, 'linked');
  git('worktree', 'add', '--detach', linked, 'HEAD');
  fs.writeFileSync(path.join(linked, 'app.ts'), 'export function ownSymbol() { return 1; }\n');
  const cg = await CodeGraph.init(linked);
  try { await cg.indexAll(); } finally { cg.close(); }
  return linked;
}

describe('real query worker context', () => {
  it('preserves default and explicit queries through a directory alias', async () => {
    const root = await fixture('aliasSymbol');
    const aliases = fs.mkdtempSync(path.join(os.tmpdir(), 'codegraph-query-alias-'));
    roots.push(aliases);
    const alias = path.join(aliases, 'project');
    fs.symlinkSync(root, alias, process.platform === 'win32' ? 'junction' : 'dir');
    const engine: MCPEngine = new BuiltEngine({ executionMode: 'direct', queryPool: true });
    try {
      await engine.ensureInitialized(alias);
      const execution = engine.getQueryExecutionContext();
      const deadline = Date.now() + 10000;
      while (engine.getQueryExecutionState().actual.dispatch !== 'worker' && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 25));
      expect(engine.getQueryExecutionState().actual.dispatch).toBe('worker');
      for (const [requestId, projectPath] of [[1, undefined], [2, alias], [3, root]] as const) {
        const result = await engine.getToolHandler().execute('codegraph_search', { query: 'aliasSymbol', ...(projectPath ? { projectPath } : {}) }, undefined,
          createQueryRequestContext(execution, 'alias-client', requestId));
        expect(result.isError).toBeFalsy();
        expect(result.content[0].text).toContain('app.ts:1');
      }
    } finally { await engine.stop(); }
  }, 30000);
  it.each(['direct', 'writable-fallback', 'shared-daemon', 'read-only-fallback'] as QueryExecutionMode[])
  ('preserves public defaults and actual execution facts through default/explicit routing in %s', async (mode) => {
    const alpha = await fixture('alphaSymbol');
    const beta = await fixture('betaSymbol');
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', undefined);
    vi.stubEnv('CODEGRAPH_QUERY_MODE', 'unlimited');
    vi.stubEnv('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', 'unlimited');
    const readOnly = mode === 'read-only-fallback';
    const engine: MCPEngine = new BuiltEngine({ executionMode: mode, readOnly, queryPool: true });
    try {
      await engine.ensureInitialized(alpha);
      const execution = engine.getQueryExecutionContext();
      const maximum = Math.max(1, Math.min(os.cpus().length - 1, mode === 'shared-daemon' ? 16 : 2));
      expect(execution).toMatchObject({ capability: readOnly ? 'read-only' : 'read-write',
        ownership: { writer: !readOnly, synchronization: !readOnly }, policy: { source: 'legacy', preset: 'default',
          executionMode: mode, concurrency: readOnly ? { kind: 'disabled' } : { kind: 'finite', maximum },
          deadline: readOnly ? { kind: 'inapplicable' } : { kind: 'finite', milliseconds: 45000 }, retention: 'ordinary' } });
      if (!readOnly) {
        const deadline = Date.now() + 10000;
        while (engine.getQueryExecutionState().actual.dispatch !== 'worker' && Date.now() < deadline) {
          await new Promise(resolve => setTimeout(resolve, 25));
        }
      }
      for (const [requestId, projectPath, query] of [[1, undefined, 'alphaSymbol'], [2, beta, 'betaSymbol'], [3, undefined, 'alphaSymbol']] as const) {
        const request = createQueryRequestContext(execution, `mode-${mode}`, requestId);
        const result = await engine.getToolHandler().execute('codegraph_search', { query, ...(projectPath ? { projectPath } : {}) }, undefined, request);
        expect(result.isError).toBeFalsy();
        expect(result.content[0].text).toContain('app.ts:1');
      }
      const state = engine.getQueryExecutionState();
      expect(state.actual).toMatchObject({ executionMode: mode, dispatch: readOnly ? 'main-thread' : 'worker',
        concurrency: readOnly ? { kind: 'disabled' } : { kind: 'finite', maximum },
        deadline: readOnly ? { kind: 'inapplicable' } : { kind: 'finite', milliseconds: 45000 }, retention: 'ordinary' });
      if (readOnly) for (const root of [alpha, beta]) expect(fs.existsSync(path.join(root, '.codegraph', 'writer.pid'))).toBe(false);
      if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'query-execution-facts.jsonl'), JSON.stringify({ mode, alpha, beta, ...state }) + '\n');
    } finally { await engine.stop(); }
  }, 30000);
  it('constructs from the resolved exact policy without rereading or clamping legacy environment inputs', async () => {
    const root = await fixture();
    const execution = createQueryExecutionContext(resolveQueryPolicy({ poolSize: '23', busyTimeoutMs: 'unlimited' }, 'direct', 8), { readOnly: false, writer: true, synchronization: true });
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '0');
    vi.stubEnv('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', '1000');
    const pool = new BuiltQueryPool({ root, execution });
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
  it.each(['missing', 'invalid'] as const)('fails closed when a linked worktree admitted own index becomes %s', async (change) => {
    const root = await linkedFixture();
    expect(fs.statSync(path.join(root, '.git')).isFile()).toBe(true);
    const execution = createQueryExecutionContext(resolveQueryPolicy({}, 'direct', 8), { readOnly: false, writer: true, synchronization: true });
    const context = routeQueryRequest(createQueryRequestContext(execution, 'linked-client', 1), 'explicit', root, getDatabasePath(root));
    // A cold worker has no own-index handle to prevent removal on Windows.
    const worker = new Worker(path.resolve(__dirname, '../dist/mcp/query-worker.js'), { workerData: { root: null, execution } });
    try {
      expect((await message(worker, 'ready')).ok).toBe(true);
      fs.rmSync(path.dirname(getDatabasePath(root)), { recursive: true });
      if (change === 'invalid') {
        fs.mkdirSync(path.dirname(getDatabasePath(root)));
        fs.writeFileSync(getDatabasePath(root), 'a replacement that is not a SQLite database');
      }
      const result = message(worker, 'result');
      worker.postMessage({ type: 'call', id: 1, toolName: 'codegraph_search', args: { query: 'ancestorSymbol' }, context });
      const response = await result;
      expect(response.result.isError).toBeFalsy();
      expect(response.result.content[0].text).toContain('admitted');
      expect(response.result.content[0].text).not.toContain('app.ts:1');
      expect(response.context).toEqual(context);
    } finally { await worker.terminate(); }
  }, 30000);
  it('accepts a valid replacement at the same admitted own index', async () => {
    const root = await fixture('oldSymbol');
    const execution = createQueryExecutionContext(resolveQueryPolicy({}, 'direct', 8), { readOnly: false, writer: true, synchronization: true });
    const context = routeQueryRequest(createQueryRequestContext(execution, 'replacement-client', 1), 'default', root, getDatabasePath(root));
    fs.rmSync(path.dirname(getDatabasePath(root)), { recursive: true });
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function replacementSymbol() { return 1; }\n');
    const cg = await CodeGraph.init(root);
    try { await cg.indexAll(); } finally { cg.close(); }
    const worker = new Worker(path.resolve(__dirname, '../dist/mcp/query-worker.js'), { workerData: { root: null, execution } });
    try {
      expect((await message(worker, 'ready')).ok).toBe(true);
      const result = message(worker, 'result');
      worker.postMessage({ type: 'call', id: 1, toolName: 'codegraph_search', args: { query: 'replacementSymbol' }, context });
      const response = await result;
      expect(response.result.isError).toBeFalsy();
      expect(response.result.content[0].text).toContain('app.ts:1');
      expect(response.context).toEqual(context);
    } finally { await worker.terminate(); }
  }, 30000);
});
