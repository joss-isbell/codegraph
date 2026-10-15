import { describe, expect, it } from 'vitest';
import { spawn } from 'child_process';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { CodeGraph } from '../src';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean, timeout = 10000): Promise<void> {
  const end = Date.now() + timeout;
  while (!check()) { if (Date.now() > end) throw new Error('Owned proxy did not reach its declared state'); await sleep(20); }
}
const line = (message: object) => JSON.stringify({ jsonrpc: '2.0', ...message }) + '\n';
const tool = (id: number) => line({ id, method: 'tools/call', params: { name: 'codegraph_search', arguments: { query: 'proxySymbol' } } });
const cancel = (id: number) => line({ method: 'notifications/cancelled', params: { requestId: id } });
const responses = (output: string) => output.trim().split('\n').filter(Boolean).map(value => JSON.parse(value))
  .filter(value => value.jsonrpc === '2.0' && ('result' in value || 'error' in value));

async function startProxy(scenario: string, readOnly = false) {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-proxy-abandon-')));
  fs.writeFileSync(path.join(root, 'app.ts'), 'export function proxySymbol() { return 1; }\n');
  const cg = await CodeGraph.init(root); await cg.indexAll(); cg.close();
  const dist = path.resolve(__dirname, '../dist');
  const script = `
    const {runLocalHandshakeProxy} = require(${JSON.stringify(path.join(dist, 'mcp/proxy.js'))});
    const {MCPEngine} = require(${JSON.stringify(path.join(dist, 'mcp/engine.js'))});
    const {QueryPool} = require(${JSON.stringify(path.join(dist, 'mcp/query-pool.js'))});
    const {resolveQueryPolicy} = require(${JSON.stringify(path.join(dist, 'mcp/query-policy.js'))});
    const {Worker} = require('worker_threads');
    const fs = require('fs'), net = require('net');
    const scenario=${JSON.stringify(scenario)}, readOnly=${JSON.stringify(readOnly)};
    const say = value => fs.writeSync(1, JSON.stringify(value)+'\\n');
    const server=net.createServer(socket=>{
      let buffer='';socket.on('data',chunk=>{buffer+=chunk;let cut;while((cut=buffer.indexOf('\\n'))>=0){
        const message=JSON.parse(buffer.slice(0,cut));buffer=buffer.slice(cut+1);
        if(message.method==='tools/call'){
          say({forwarded:message.id});
          if(scenario==='handover')socket.write(JSON.stringify({jsonrpc:'2.0',id:message.id,result:{content:[{type:'text',text:'daemon replacement'}]}})+'\\n');
        }
        if(message.method==='notifications/cancelled'){
          socket.write(JSON.stringify({jsonrpc:'2.0',id:message.params.requestId,result:{content:[{type:'text',text:'cancelled late daemon result'}]}})+'\\n');
          setTimeout(()=>socket.destroy(),100);
        }
      }});
    });
    let attempts=0;
    server.listen(0,'127.0.0.1',()=>runLocalHandshakeProxy({root:${JSON.stringify(root)},
      getDaemonSocket:async()=>{
        attempts++;
        if(scenario==='buffered'){await new Promise(r=>setTimeout(r,500));return null;}
        if(scenario!=='inflight'&&(attempts===1||scenario!=='handover'))return null;
        if(scenario==='inflight'&&attempts!==1)return null;
        const socket=net.createConnection(server.address().port,'127.0.0.1');
        await new Promise((resolve,reject)=>{socket.once('connect',resolve);socket.once('error',reject);});
        return socket;
      },
      makeEngine:()=>{
        say({engineCreated:true});
        const engine = new MCPEngine({readOnly,watch:false,writerLockRoot:readOnly?undefined:${JSON.stringify(root)},queryPolicy:resolveQueryPolicy({poolSize:'1',busyTimeoutMs:'unlimited'},readOnly?'read-only-fallback':'writable-fallback',8)});
        const pool = new QueryPool({root:${JSON.stringify(root)},execution:engine.getQueryExecutionContext(),createWorker:()=>{
          const worker=new Worker(${JSON.stringify(path.resolve(__dirname, 'fixtures/query-abandonment-worker.cjs'))},{workerData:{root:${JSON.stringify(root)},execution:engine.getQueryExecutionContext(),delayMs:scenario==='handover'?16500:200,stuck:scenario==='stuck'}});
          worker.on('message',message=>{if(message.type==='observation')say({observed:message});});
          worker.on('exit',()=>say({observedExit:true}));return worker;
        }});
        engine.queryPool=pool;engine.getToolHandler().setQueryPool(pool);
        const stop=engine.stop.bind(engine);engine.stop=async()=>{await stop();say({engineStopped:true});};
        return engine;
      }}));
  `;
  const child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, '-e', script], { stdio: ['pipe', 'pipe', 'pipe'], env: {
    ...process.env, CODEGRAPH_NO_WATCHDOG: '1', CODEGRAPH_PPID_POLL_MS: '0', CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS: '0',
    CODEGRAPH_DAEMON_RETRY_MS: scenario === 'handover' ? '100' : '0', CODEGRAPH_NO_UPDATE_CHECK: '1', CODEGRAPH_TELEMETRY: '0',
  } });
  let stdout = '', stderr = '';
  child.stdout!.on('data', chunk => { stdout += chunk; }); child.stderr!.on('data', chunk => { stderr += chunk; });
  const exited = () => child.exitCode !== null || child.signalCode !== null;
  const exit = new Promise<void>(resolve => child.once('exit', () => resolve()));
  child.stdin!.on('error', () => {});
  return { child, out: () => stdout, err: () => stderr, exited,
    write: (value: string) => child.stdin!.write(value),
    dispose: async () => { if (!exited()) child.kill('SIGKILL'); await exit; child.stdin!.destroy(); fs.rmSync(root, { recursive: true, force: true }); },
  };
}

describe('proxy request abandonment', () => {
  it.each(['buffered', 'inflight', 'local'])('does not execute or replay an abandoned %s request and suppresses its late response', async scenario => {
    const proxy = await startProxy(scenario);
    try {
      proxy.write(tool(2));
      if (scenario === 'inflight') await until(() => proxy.out().includes('"forwarded":2'));
      if (scenario === 'local') await until(() => proxy.out().includes('"event":"start"'));
      proxy.write(cancel(2)); proxy.write(tool(3));
      await until(() => proxy.out().includes('"id":3') || proxy.exited());
      expect(proxy.out(), proxy.err()).toContain('proxySymbol');
      expect(responses(proxy.out()).some(response => response.id === 2)).toBe(false);
      expect(proxy.out().match(/"event":"start"/g)).toHaveLength(scenario === 'local' ? 2 : 1);
      proxy.child.stdin!.end(); await until(proxy.exited);
      expect(proxy.child.exitCode, proxy.err()).toBe(0);
      expect(proxy.out()).toContain('observedExit');
    } finally { await proxy.dispose(); }
  }, 20000);

  it.each([false, true])('preserves healthy real SQLite execution beyond the drain allowance during readOnly=%s fallback handover', async readOnly => {
    const proxy = await startProxy('handover', readOnly);
    const start = Date.now();
    try {
      proxy.write(tool(2));
      await until(() => proxy.out().includes('"event":"start"'));
      proxy.write(tool(3));
      await sleep(15500);
      expect(proxy.out(), proxy.err()).not.toContain('observedExit');
      expect(proxy.out()).not.toContain('engineStopped');
      expect(responses(proxy.out()).some(response => response.id === 2)).toBe(false);
      await until(() => proxy.out().includes('engineStopped') && proxy.out().includes('"id":3'), 10000);
      expect(proxy.out(), proxy.err()).toContain('proxySymbol');
      expect(proxy.out()).toContain('daemon replacement');
      expect(responses(proxy.out()).filter(response => response.id === 2)).toHaveLength(1);
      expect(responses(proxy.out()).filter(response => response.id === 3)).toHaveLength(1);
      expect(proxy.out().match(/engineCreated/g)).toHaveLength(1);
      expect(proxy.out().match(/observedExit/g)).toHaveLength(1);
      if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'handover-facts.jsonl'), JSON.stringify({readOnly,elapsedMs:Date.now()-start,stdout:proxy.out(),stderr:proxy.err()})+'\n');
      proxy.child.stdin!.end(); await until(proxy.exited);
      expect(proxy.child.exitCode, proxy.err()).toBe(0);
    } finally { await proxy.dispose(); }
  }, 35000);

  it('cleans up an intentionally abandoned proxy with stuck real SQLite work and awaits worker exit', async () => {
    const proxy = await startProxy('stuck');
    try {
      proxy.write(tool(2)); await until(() => proxy.out().includes('"event":"start"'));
      const start = Date.now(); proxy.child.stdin!.end();
      await until(proxy.exited, 36000);
      expect(proxy.child.exitCode, proxy.err()).toBe(0);
      expect(Date.now()-start).toBeGreaterThanOrEqual(14500);
      expect(Date.now()-start).toBeLessThan(35000);
      expect(proxy.out()).toContain('engineStopped');
      expect(proxy.out()).toContain('observedExit');
      expect(responses(proxy.out()).some(response => response.id === 2)).toBe(false);
      if (process.env.VALIDATION_OUT) fs.appendFileSync(path.join(process.env.VALIDATION_OUT, 'handover-facts.jsonl'), JSON.stringify({scenario:'stuck-proxy',elapsedMs:Date.now()-start,stdout:proxy.out(),stderr:proxy.err()})+'\n');
    } finally { await proxy.dispose(); }
  }, 45000);
});
