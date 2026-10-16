import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as net from 'net';
import { CodeGraph, getDatabasePath } from '../src';
import type { MCPEngine } from '../src/mcp/engine';
import * as QueryDiagnosticSchema from '../src/mcp/query-diagnostics';
const { MCPEngine: BuiltEngine } = require('../dist/mcp/engine') as { MCPEngine: new (...args: any[]) => MCPEngine };
const { Daemon: BuiltDaemon, tryAcquireDaemonLock } = require('../dist/mcp/daemon');

const diagnostics = (result: { content: Array<{ text: string }> }) => {
  const match = result.content[0].text.match(/\*\*Query execution\*\*\n```json\n([\s\S]*?)\n```/);
  expect(match, 'status carries verifiable execution diagnostics').not.toBeNull();
  return JSON.parse(match![1]);
};

describe('query execution diagnostics', () => {
  it('advertises the actual cold engine identity and policy in the daemon hello without query demand', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-diagnostic-hello-')));
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function helloSymbol() {}\n');
    const graph = await CodeGraph.init(root);
    try { await graph.indexAll(); } finally { graph.close(); }
    const daemon = new BuiltDaemon(root, { idleTimeoutMs: 60000 });
    try {
      expect(tryAcquireDaemonLock(root).kind).toBe('acquired');
      const { socketPath } = await daemon.start();
      const hello = await new Promise<any>((resolve, reject) => {
        const socket = net.createConnection(socketPath);
        let buffer = '', result: any, failure: Error | undefined;
        const finish = (error?: Error) => { failure = error; clearTimeout(timer); socket.destroy(); };
        const timer = setTimeout(() => finish(new Error('independent hello deadline')), 10000);
        socket.on('data', chunk => {
          buffer += chunk.toString();
          if (buffer.length > 65536) return finish(new Error('bounded hello overflow'));
          if (!buffer.includes('\n')) return;
          try { result = JSON.parse(buffer.split('\n')[0]); finish(); } catch (error) { finish(error as Error); }
        });
        socket.on('error', finish);
        socket.once('close', () => { clearTimeout(timer); failure ? reject(failure) : resolve(result); });
      });
      expect(hello.queryExecution).toMatchObject({ schemaVersion: 1,
        implementation: { policySchemaVersion: 1, support: { combinedPreset: false } }, requested: {},
        effective: { executionMode: 'shared-daemon', concurrency: { kind: 'finite' }, deadline: { kind: 'finite', milliseconds: 45000 } },
        ownership: { pid: process.pid, resourceScope: 'shared-project' }, lifecycle: { state: 'cold', workers: 0, ready: false },
        project: { route: 'default', root, index: fs.realpathSync(getDatabasePath(root)) } });
      expect(QueryDiagnosticSchema.readQueryDiagnostics(hello.queryExecution)).toEqual(hello.queryExecution);
    } finally { await daemon.stop('owned diagnostic proof complete'); fs.rmSync(root, { recursive: true, force: true }); }
  }, 30000);
  it('keeps absent older policy, support and ownership unknown rather than inventing matching defaults', () => {
    const read = (QueryDiagnosticSchema as any).readQueryDiagnostics;
    expect(read(undefined)).toEqual({ schemaVersion: 'unknown', implementation: 'unknown', requested: 'unknown',
      effective: 'unknown', ownership: 'unknown', lifecycle: 'unknown', project: 'unknown' });
    expect(read({ schemaVersion: 1, requested: { poolSize: '0' } })).toEqual({ schemaVersion: 1,
      implementation: 'unknown', requested: { poolSize: '0' }, effective: 'unknown', ownership: 'unknown', lifecycle: 'unknown', project: 'unknown' });
    expect(read({ schemaVersion: 1, ownership: { capability: 'read-write', writer: true, synchronization: true,
      resourceScope: 'local-engine', pid: 1, generation: 'older-ambiguous-authority' } }).ownership).toBe('unknown');
  });
  it('reports a configured cold pool without disabling policy or starting workers, then serves first demand', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-diagnostics-')));
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function diagnosticSymbol() { return 42; }\n');
    const graph = await CodeGraph.init(root);
    try { await graph.indexAll(); } finally { graph.close(); }
    const engine = new BuiltEngine({ executionMode: 'direct', queryPool: true, watch: false });
    try {
      await engine.ensureInitialized(root);
      const handler = engine.getToolHandler();
      const cold = diagnostics(await handler.execute('codegraph_status', {}));
      expect(cold).toMatchObject({ schemaVersion: 1, requested: {},
        effective: { executionMode: 'direct', source: 'legacy', preset: 'default', concurrency: { kind: 'finite' }, deadline: { kind: 'finite', milliseconds: 45000 } },
        ownership: { capability: 'read-write', writerAuthority: true, synchronizationAuthority: true, resourceScope: 'local-engine' },
        lifecycle: { state: 'cold', ready: false, workers: 0, starting: 0, executing: 0 },
        project: { route: 'default', root, index: fs.realpathSync(getDatabasePath(root)) } });
      expect(cold.implementation).toMatchObject({ policySchemaVersion: 1, support: { independentControls: true, combinedPreset: false, idleRetirement: true } });
      const again = diagnostics(await handler.execute('codegraph_status', {}));
      expect(again.lifecycle).toEqual(cold.lifecycle);
      const result = await handler.execute('codegraph_search', { query: 'diagnosticSymbol' });
      expect(result.isError).toBeFalsy();
      expect(result.content[0].text).toContain('diagnosticSymbol');
      expect(diagnostics(await handler.execute('codegraph_status', {}))).toMatchObject({
        effective: cold.effective, lifecycle: { state: 'active', ready: true, workers: 1 } });
    } finally { await engine.stop(); fs.rmSync(root, { recursive: true, force: true }); }
  }, 30000);
});
