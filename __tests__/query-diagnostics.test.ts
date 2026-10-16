import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import * as net from 'net';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { CodeGraph, getDatabasePath } from '../src';
import type { MCPEngine } from '../src/mcp/engine';
import { getDaemonPidPath } from '../src/mcp/daemon-paths';
import { getWriterPidPath } from '../src/mcp/writer-lock';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import * as QueryDiagnosticSchema from '../src/mcp/query-diagnostics';
import { parseQueryDiagnostics as diagnostics } from './query-diagnostics-helper';
const { MCPEngine: BuiltEngine } = require('../dist/mcp/engine') as { MCPEngine: new (...args: any[]) => MCPEngine };

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean, ms: number, detail: () => string) {
  const end = Date.now() + ms;
  while (!check()) { if (Date.now() >= end) throw new Error(`Owned hello deadline: ${detail()}`); await wait(20); }
}

describe('query execution diagnostics', () => {
  it('advertises the actual cold engine identity and policy in the daemon hello without query demand', async () => {
    const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-diagnostic-hello-')));
    fs.writeFileSync(path.join(root, 'app.ts'), 'export function helloSymbol() {}\n');
    const graph = await CodeGraph.init(root);
    try { await graph.indexAll(); } finally { graph.close(); }
    let child: ChildProcessWithoutNullStreams | undefined;
    let closed = false, socketPath: string | undefined;
    let childError: Error | undefined, primaryFailure: unknown, cleanupFailure: unknown, hello: any;
    const outputLimit = 65536;
    const output = { stdout: { prefix: Buffer.alloc(outputLimit), seenBytes: 0, retainedBytes: 0, truncated: false },
      stderr: { prefix: Buffer.alloc(outputLimit), seenBytes: 0, retainedBytes: 0, truncated: false } };
    const capture = (stream: keyof typeof output, data: Buffer) => {
      const current = output[stream];
      current.seenBytes += data.length;
      const retained = Math.min(data.length, outputLimit - current.retainedBytes);
      data.copy(current.prefix, current.retainedBytes, 0, retained);
      current.retainedBytes += retained;
      if (retained < data.length) {
        current.truncated = true;
        childError ??= new Error(`bounded daemon ${stream} overflow`);
      }
    };
    const text = (stream: keyof typeof output) => output[stream].prefix.subarray(0, output[stream].retainedBytes).toString('utf8');
    const captured = (stream: keyof typeof output) => ({ text: text(stream), seenBytes: output[stream].seenBytes,
      retainedBytes: output[stream].retainedBytes, truncated: output[stream].truncated, limitBytes: outputLimit });
    const cleanupPhases: Array<{ phase: string; outcome: string; error?: string }> = [];
    const cleanup = async (phase: string, action: () => Promise<void> | void) => {
      try { await action(); cleanupPhases.push({ phase, outcome: 'pass' }); }
      catch (error) { cleanupPhases.push({ phase, outcome: 'fail', error: String(error) }); cleanupFailure ??= error; }
    };
    try {
      // The real daemon owns process exit. Keep that boundary in its own child,
      // where POSIX immediate exit and Windows natural drain are both real.
      child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, path.resolve(__dirname, '../dist/bin/codegraph.js'),
        'serve', '--mcp', '--path', root], { cwd: root, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'],
        env: { ...process.env, CODEGRAPH_DAEMON_INTERNAL: '1', CODEGRAPH_NO_DAEMON: '0',
          CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '1000', CODEGRAPH_DAEMON_INSTALL_CHECK_MS: '0',
          CODEGRAPH_NO_RELAUNCH: '1', CODEGRAPH_WASM_RELAUNCHED: '1',
          CODEGRAPH_QUERY_MODE: undefined, CODEGRAPH_QUERY_POOL_SIZE: undefined, CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: undefined } });
      child.once('error', error => { childError ??= error; });
      child.once('close', () => { closed = true; });
      child.stdin.on('error', error => { childError ??= error; });
      // Keep draining both pipes after overflow, but never retain past the byte
      // prefix. The sticky error makes the whole fixture fail after cleanup.
      child.stdout.on('data', (data: Buffer) => capture('stdout', data));
      child.stderr.on('data', (data: Buffer) => capture('stderr', data));
      await until(() => {
        if (childError) throw childError;
        if (closed) throw new Error(`Owned daemon exited before listening: ${text('stderr')}`);
        return text('stderr').includes('[CodeGraph daemon] Listening on ');
      }, 15000, () => text('stderr'));
      socketPath = JSON.parse(fs.readFileSync(getDaemonPidPath(root), 'utf8')).socketPath;
      expect(typeof socketPath).toBe('string');
      hello = await new Promise<any>((resolve, reject) => {
        const socket = net.createConnection(socketPath!);
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
        ownership: { pid: child.pid, resourceScope: 'shared-project' }, lifecycle: { state: 'cold', workers: 0, ready: false },
        project: { route: 'default', root, index: fs.realpathSync(getDatabasePath(root)) } });
      expect(QueryDiagnosticSchema.readQueryDiagnostics(hello.queryExecution)).toEqual(hello.queryExecution);
      expect(hello.queryExecution.ownership.generation).toEqual(expect.any(String));
      expect(hello.queryExecution.ownership.generation.length).toBeGreaterThan(0);
    } catch (error) { primaryFailure = error; }
    finally {
      await cleanup('natural owned daemon idle exit', async () => {
        child?.stdin.end();
        if (child) {
          await until(() => closed, 15000, () => text('stderr'));
          expect(childError).toBeUndefined(); expect(child.exitCode, text('stderr')).toBe(0); expect(child.signalCode).toBeNull();
        }
      });
      await cleanup('owned child fallback settled', async () => {
        if (child && !closed) { child.kill('SIGKILL'); await until(() => closed, 10000, () => text('stderr')); }
      });
      await cleanup('daemon writer locks and POSIX socket removed', () => {
        expect(fs.existsSync(getDaemonPidPath(root))).toBe(false);
        expect(fs.existsSync(getWriterPidPath(root))).toBe(false);
        if (process.platform !== 'win32' && socketPath) expect(fs.existsSync(socketPath)).toBe(false);
      });
      await cleanup('fixture removed after actual owned exit', () => {
        expect(!child || closed).toBe(true); fs.rmSync(root, { recursive: true, force: true });
      });
      if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, 'hello-cleanup.json'), JSON.stringify({
        root, socketPath, hello, primaryFailure: primaryFailure ? String(primaryFailure) : null,
        cleanupFailure: cleanupFailure ? String(cleanupFailure) : null, cleanupPhases, stderr: captured('stderr'), stdout: captured('stdout'),
        child: child ? { pid: child.pid, closed, exitCode: child.exitCode, signalCode: child.signalCode } : null,
        fixtureRemoved: !fs.existsSync(root), outcome: primaryFailure || cleanupFailure ? 'fail' : 'pass',
      }, null, 2));
    }
    if (primaryFailure) throw primaryFailure;
    if (cleanupFailure) throw cleanupFailure;
  }, 60000);
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
