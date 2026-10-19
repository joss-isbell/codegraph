import { afterAll, afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { createHash } from 'crypto';
import type { MCPEngineOptions } from '../src/mcp/engine';

// Observe the real native boundary before the built facade captures SQLite.
// The observer delegates every constructor and result; it supplies no graph,
// permission, policy, or engine behavior.
const observer: {
  begin(roots: string[]): void;
  facts(): { total: number; overflow: boolean; records: unknown[] };
  restore(): void;
} = require('./fixtures/query-admission-sqlite-observer.cjs');
const { CodeGraph }: typeof import('../src') = require('../dist');
const { MCPEngine }: typeof import('../src/mcp/engine') = require('../dist/mcp/engine');
const { captureMcpIndexRoute }: typeof import('../src/directory') = require('../dist/directory');
const { getDaemonPidPath }: typeof import('../src/mcp/daemon-paths') = require('../dist/mcp/daemon-paths');
const { resolveQueryPolicy }: typeof import('../src/mcp/query-policy') = require('../dist/mcp/query-policy');
const nativeOs: typeof import('os') = require('os');

const roots: string[] = [];
const engines: Array<InstanceType<typeof MCPEngine>> = [];
const facts: unknown[] = [];
let poison: string | undefined;
let body: Promise<void> | undefined;
let bodySettled = true;
let cleanupStarted = false;

function owned(work: () => Promise<void>): () => Promise<void> {
  return () => {
    body = (async () => { try { await work(); } finally { bodySettled = true; } })();
    void body.catch(() => {}); // Preserve ownership after a framework timeout.
    return body;
  };
}
function active(): void {
  if (cleanupStarted || poison) throw new Error('Held-route fixture cannot acquire resources after cleanup');
}
function temp(): string {
  active();
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-hr-')));
  roots.push(root);
  return root;
}
async function indexed(root: string, symbol: string): Promise<void> {
  active();
  fs.mkdirSync(root, { recursive: true });
  fs.writeFileSync(path.join(root, 'app.ts'), `export function ${symbol}() { return 7; }\n`);
  const cg = await CodeGraph.init(root);
  let primary: { error: unknown } | undefined, closeFailure: { error: unknown } | undefined;
  try { active(); await cg.indexAll(); } catch (error) { primary = { error }; }
  try { cg.close(); } catch (error) { closeFailure = { error }; }
  if (primary || closeFailure) {
    facts.push({ kind: 'indexed-fixture-failure', root,
      primary: primary ? String(primary.error).slice(0, 2048) : null,
      secondary: closeFailure ? [{ phase: 'graph close', error: String(closeFailure.error).slice(0, 2048) }] : [] });
  }
  if (primary) throw primary.error;
  if (closeFailure) throw closeFailure.error;
}
function route(root: string) {
  const held = captureMcpIndexRoute(root);
  if (!held) throw new Error('Fixture did not produce its own canonical index');
  return held;
}
function engine(options: MCPEngineOptions = {}): InstanceType<typeof MCPEngine> {
  active();
  const value = new MCPEngine({ readOnly: true, watch: false, queryPool: false, ...options });
  engines.push(value);
  if (engines.length > 1) { poison = 'Held-route engine cleanup budget exceeded'; throw new Error(poison); }
  return value;
}
function membership(root: string): Record<string, string> {
  const files: Record<string, string> = {};
  const walk = (dir: string): void => {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
      const file = path.join(dir, entry.name);
      if (entry.isDirectory()) walk(file);
      else if (entry.isFile()) files[path.relative(root, file)] = createHash('sha256').update(fs.readFileSync(file)).digest('hex');
      else throw new Error('Held-route fixture has unexpected non-file membership');
    }
  };
  walk(root);
  return files;
}
async function bound(work: Promise<unknown>, label: string): Promise<void> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => { poison ??= `${label} did not settle`; reject(new Error(poison)); }, 5000);
    })]);
  } finally { if (timer) clearTimeout(timer); }
}

beforeEach(() => {
  if (poison || !bodySettled) throw new Error(`Held-route fixture cannot reuse incomplete work: ${poison ?? 'body pending'}`);
  body = undefined; bodySettled = false; cleanupStarted = false;
  vi.stubEnv('CODEGRAPH_DIR', '.codegraph');
  vi.stubEnv('CODEGRAPH_QUERY_MODE', 'default');
  vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '0');
  vi.stubEnv('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', '45000');
  observer.begin([]);
});
afterEach(async context => {
  if (cleanupStarted) return;
  cleanupStarted = true;
  const pending = body && !bodySettled ? body : undefined;
  if (pending) poison ??= 'Framework ended while the owned held-route body was pending';
  vi.restoreAllMocks();
  const cleanup: string[] = [];
  for (const value of engines.splice(0)) {
    try { await bound(value.stop(), 'engine stop'); } catch (error) { cleanup.push(String(error).slice(0, 2048)); }
  }
  if (pending) {
    try { await bound(pending, 'owned body join'); } catch (error) { cleanup.push(String(error).slice(0, 2048)); }
  }
  const retained = roots.splice(0).map(root => {
    try { return { root, files: membership(root) }; }
    catch (error) { cleanup.push(String(error).slice(0, 2048)); return { root, failedMembership: true }; }
  });
  facts.push({ test: context.task.name, primary: context.task.result?.errors?.slice(0, 4).map(error => String(error.message).slice(0, 2048)),
    roots: retained, sqlite: observer.facts(), cleanup, poison, bodySettled, incompleteBodyAtCleanup: !!pending });
  try {
    const output = process.env.VALIDATION_OUT;
    if (output) {
      const encoded = JSON.stringify(facts);
      if (Buffer.byteLength(encoded) > 1024 * 1024) throw new Error('Held-route evidence exceeds 1 MiB');
      fs.writeFileSync(path.join(output, 'mcp-held-route-facts.json'), encoded);
    }
  } catch (error) {
    cleanup.push(String(error).slice(0, 2048));
    process.stderr.write(`Held-route evidence persistence failed: ${String(error).slice(0, 2048)}\n`);
  } finally { vi.unstubAllEnvs(); }
  // Retain every fixture, including failed indexes and their native sidecars.
  expect(cleanup).toEqual([]);
  expect(observer.facts().overflow).toBe(false);
  expect(poison).toBeUndefined();
}, 30_000);
afterAll(() => { if (bodySettled) observer.restore(); });

describe('MCP engine holds an acknowledged default route before first initialization', () => {
  it('construction only snapshots its route, without opening or verifying a nonexistent index', owned(async () => {
    const root = temp(), input = { root, index: path.join(root, '.codegraph', 'codegraph.db') };
    observer.begin([root]);
    const value = engine({ projectRoute: input });
    expect(value.getProjectPath()).toBe(root);
    expect(value.hasDefaultCodeGraph()).toBe(false);
    expect(value.hasVerifiedDefaultRoute()).toBe(false);
    expect(observer.facts()).toMatchObject({ total: 0, overflow: false });
    expect(fs.readdirSync(root)).toEqual([]);
    expect(Object.isFrozen(input)).toBe(false); // The caller retains its own object.
  }));

  it('ignores caller route mutation and the initialize search path, then queries the acknowledged own graph', owned(async () => {
    const ancestor = temp(), child = path.join(ancestor, 'child');
    await indexed(ancestor, 'ancestorOriginal'); await indexed(child, 'heldOriginal');
    const held = route(child), other = route(ancestor), input = { ...held };
    const value = engine({ projectRoute: input });
    input.root = other.root; input.index = other.index;
    observer.begin([ancestor]);
    await value.ensureInitialized(ancestor);
    expect(value.getProjectPath()).toBe(held.root);
    expect(value.hasVerifiedDefaultRoute()).toBe(true);
    const result = await value.getToolHandler().execute('codegraph_search', { query: 'heldOriginal' });
    expect(result.isError).not.toBe(true);
    expect(result.content[0]?.text).toContain('heldOriginal');
    expect(value.getQueryDiagnostics().project).toEqual({ route: 'default', ...held });
    expect(observer.facts().total).toBeGreaterThan(0);
  }), 30_000);

  it.each(['missing', 'changed'])('%s held index cannot select an indexed ancestor or new own target', async action => {
    await owned(async () => {
      const ancestor = temp(), child = path.join(ancestor, 'child');
      await indexed(ancestor, 'ancestorOriginal'); await indexed(child, 'heldOriginal');
      const held = route(child), value = engine({ projectRoute: held });
      if (action === 'missing') fs.renameSync(held.index, held.index + '-retained');
      else {
        vi.stubEnv('CODEGRAPH_DIR', '.codegraph-next');
        await indexed(ancestor, 'ancestorReplacement'); await indexed(child, 'ownReplacement');
      }
      const before = membership(ancestor);
      observer.begin([ancestor]);
      await value.ensureInitialized(ancestor);
      await value.retryInitialize(ancestor);
      expect(value.getProjectPath()).toBe(held.root);
      expect(value.hasDefaultCodeGraph()).toBe(false);
      expect(value.hasVerifiedDefaultRoute()).toBe(false);
      const result = await value.getToolHandler().execute('codegraph_search', { query: 'ancestorOriginal' });
      expect(result.isError).not.toBe(true);
      expect(result.content[0]?.text).toContain(`verified own codegraph index (${action})`);
      expect(result.content[0]?.text).toContain('cannot use an ancestor index');
      expect(observer.facts()).toMatchObject({ total: 0, overflow: false });
      expect(membership(ancestor)).toEqual(before);
    })();
  }, 30_000);

  it('a newly invalid acknowledged index stays unavailable instead of rediscovering the valid ancestor', owned(async () => {
    const ancestor = temp(), child = path.join(ancestor, 'child');
    await indexed(ancestor, 'ancestorOriginal'); await indexed(child, 'heldOriginal');
    const held = route(child), value = engine({ projectRoute: held });
    fs.renameSync(held.index, held.index + '-retained');
    fs.writeFileSync(held.index, Buffer.alloc(200));
    const before = membership(ancestor); observer.begin([ancestor]);
    await value.ensureInitialized(ancestor);
    const result = await value.getToolHandler().execute('codegraph_search', { query: 'ancestorOriginal' });
    expect(result.isError).not.toBe(true);
    expect(result.content[0]?.text).toContain('verified own codegraph index (invalid)');
    expect(value.hasDefaultCodeGraph()).toBe(false);
    // A >=100-byte unknown database needs an admitted native RO schema
    // validation attempt. Only this exact held child may be probed: no
    // ancestor fallback, writable constructor, or second graph is permitted.
    expect(observer.facts()).toEqual({ total: 1, overflow: false,
      records: [{ file: held.index, readOnly: true }] });
    expect(membership(ancestor)).toEqual(before);
  }), 30_000);

  it('refreshes real owner permission after construction and refuses unknown ownership before SQLite', owned(async () => {
    const root = temp(); await indexed(root, 'heldOriginal');
    const value = engine({ projectRoute: route(root) });
    fs.writeFileSync(getDaemonPidPath(root), '{'); // Actual unreadable identity, after the route was supplied.
    const before = membership(root); observer.begin([root]);
    await expect(value.ensureInitialized(root)).rejects.toMatchObject({ code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'unknown', projectRoot: root });
    expect(value.hasDefaultCodeGraph()).toBe(false);
    expect(observer.facts()).toMatchObject({ total: 0, overflow: false });
    expect(membership(root)).toEqual(before);
  }), 30_000);

  it('same-root hints are idempotent and conflicting hints cannot mutate a held route', owned(async () => {
    const root = temp(), other = temp(), value = engine({ projectRoute: { root, index: path.join(root, '.codegraph/codegraph.db') } });
    value.setProjectPathHint(root);
    expect(() => value.setProjectPathHint(other)).toThrow(/held project route/);
    expect(value.getProjectPath()).toBe(root);
    const result = await value.getToolHandler().execute('codegraph_search', { query: 'heldOriginal' });
    expect(result.content[0]?.text).toContain(root);
    expect(result.content[0]?.text).not.toContain(other);
  }));

  it('engines without a constructor route keep the existing replaceable path hints', owned(async () => {
    const first = temp(), second = temp(), value = engine();
    value.setProjectPathHint(first); value.setProjectPathHint(second);
    expect(value.getProjectPath()).toBe(second);
    expect(value.hasDefaultCodeGraph()).toBe(false);
  }));
});

describe('MCP engine captures legacy policy inputs only when needed', () => {
  it('fully injected policies do not consult CPU and retain supplied controls and defaults', owned(async () => {
    const actual = resolveQueryPolicy({ mode: 'unlimited', poolSize: '3', busyTimeoutMs: '5000' }, 'direct', 3);
    const admission = resolveQueryPolicy(actual.requested, 'shared-daemon', 3);
    const before = JSON.stringify({ actual, admission });
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '99'); vi.stubEnv('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', '99000');
    const cpu = vi.spyOn(nativeOs, 'cpus').mockImplementation(() => { throw new Error('Injected construction must not recapture CPU'); });
    const value = engine({ readOnly: false, executionMode: 'direct', queryPool: true, queryPolicy: actual, queryAdmissionPolicy: admission });
    expect(cpu).not.toHaveBeenCalled();
    expect(value.getQueryExecutionContext().policy).toBe(actual);
    expect(value.getQueryDiagnostics()).toMatchObject({ requested: actual.requested, effective: {
      concurrency: { kind: 'finite', maximum: 3 }, deadline: { kind: 'finite', milliseconds: 5000 }, defaults: actual.defaults } });
    expect(JSON.stringify({ actual, admission })).toBe(before);
  }));

  it.each(['actual', 'admission'])('one missing %s policy captures CPU exactly once', async missing => {
    await owned(async () => {
      const actual = resolveQueryPolicy({ poolSize: '3', busyTimeoutMs: '5000' }, 'direct', 3);
      const admission = resolveQueryPolicy(actual.requested, 'shared-daemon', 3);
      const originalCpus = nativeOs.cpus;
      const cpu = vi.spyOn(nativeOs, 'cpus').mockImplementation(() => originalCpus());
      const value = engine({ readOnly: false, executionMode: 'direct', queryPool: true,
        ...(missing === 'actual' ? {} : { queryPolicy: actual }), ...(missing === 'admission' ? {} : { queryAdmissionPolicy: admission }) });
      expect(cpu).toHaveBeenCalledTimes(1);
      expect(value.getQueryExecutionContext().policy).toMatchObject(missing === 'actual'
        ? { source: 'legacy', requested: { mode: 'default', poolSize: '0', busyTimeoutMs: '45000' } }
        : actual);
      if (missing === 'admission') expect(value.getQueryExecutionContext().policy).toBe(actual);
    })();
  });

  it('both missing policies share one pre-CPU request capture despite later environment changes', owned(async () => {
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '3'); vi.stubEnv('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', '5000');
    const originalCpus = nativeOs.cpus;
    const cpu = vi.spyOn(nativeOs, 'cpus').mockImplementation(() => {
      vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '9'); vi.stubEnv('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', '9000');
      return originalCpus();
    });
    const value = engine({ readOnly: false, executionMode: 'direct', queryPool: true });
    expect(cpu).toHaveBeenCalledTimes(1);
    expect(value.getQueryDiagnostics()).toMatchObject({ requested: { poolSize: '3', busyTimeoutMs: '5000' }, effective: {
      concurrency: { kind: 'finite', maximum: 3 }, deadline: { kind: 'finite', milliseconds: 5000 } } });
  }));
});
