"use strict";
// PRIVATE PROPOSAL, UNRUN. Delegate actual constructors, process spawn, IPC and
// socket bytes. Only a declared owned pre-service fault and owned cleanup stop
// are controlled; no policy, hello, acquisition result or query is fabricated.
const fs = require('node:fs'), path = require('node:path');
const threads = require('node:worker_threads');
const traceDir = process.env.CG_DAEMON_RECON_TRACE;
const controlPath = process.env.CG_DAEMON_RECON_CONTROL;
const roots = JSON.parse(process.env.CG_DAEMON_RECON_ROOTS || '[]');
const MAX_TRACE = 8 * 1024 * 1024, MAX_RECORD = 131072;
let seen = 0, sequence = 0, firstFailure = null;
function failure(error) {
  firstFailure ||= String(error).slice(0, 2048);
  process.exitCode = 1;
  try { fs.writeFileSync(path.join(traceDir, `${process.pid}-${threads.threadId}.failure`), firstFailure); } catch {}
}
function record(value) {
  if (!traceDir) return;
  try {
    const bytes = Buffer.from(JSON.stringify({ pid: process.pid, threadId: threads.threadId,
      parentPid: process.ppid, sequence: sequence++, monotonicNs: process.hrtime.bigint().toString(), ...value }) + '\n');
    if (bytes.length > MAX_RECORD || seen + bytes.length > MAX_TRACE) throw new Error('Bounded native observation overflow');
    seen += bytes.length;
    fs.appendFileSync(path.join(traceDir, `${process.pid}-${threads.threadId}.jsonl`), bytes);
  } catch (error) { failure(error); }
}
function readJson(file) {
  let fd, primary, value;
  try {
    fd = fs.openSync(file, 'r');
    const stat = fs.fstatSync(fd);
    if (stat.size > 65536) throw new Error('Bounded fixture control overflow');
    const bytes = Buffer.alloc(stat.size);
    if (fs.readSync(fd, bytes, 0, bytes.length, 0) !== bytes.length) throw new Error('Short fixture control read');
    value = JSON.parse(bytes.toString('utf8'));
  } catch (error) { primary = error; }
  if (fd !== undefined) try { fs.closeSync(fd); } catch (error) {
    record({ event: 'fixture-secondary', phase: 'control-close', error: String(error).slice(0, 2048) });
    primary ||= error;
  }
  if (primary) throw primary;
  return value;
}
function control() {
  if (!controlPath || !fs.existsSync(controlPath)) return null;
  return readJson(controlPath);
}
const nativeProcessEmit = process.emit;
process.emit = function(event, ...args) {
  if (event === 'message') record({ event: 'native-ipc-message', message: args[0] });
  return nativeProcessEmit.call(this, event, ...args);
};
function underRoot(file) {
  return roots.some(root => file === root || file.startsWith(root + path.sep));
}
const sqlite = require('node:sqlite'), NativeDatabase = sqlite.DatabaseSync;
sqlite.DatabaseSync = new Proxy(NativeDatabase, { construct(target, args, newTarget) {
  if (underRoot(String(args[0]))) record({ event: 'sqlite-constructor', file: String(args[0]), readOnly: args[1]?.readOnly === true });
  return Reflect.construct(target, args, newTarget);
} });
if (sqlite.DatabaseSync === NativeDatabase) throw new Error('Native constructor observer not installed');
// Observe raw native push before StringDecoder and native close, without adding
// a flowing reader. Retain this/arguments/return/errors unchanged.
const net = require('node:net'), identities = new WeakMap(), nativeSockets = new Set(); let nextSocket = 0;
const socketId = socket => { if (!identities.has(socket)) { identities.set(socket, ++nextSocket); nativeSockets.add(socket); } return identities.get(socket); };
const push = net.Socket.prototype.push, emit = net.Socket.prototype.emit, write = net.Socket.prototype.write;
net.Socket.prototype.write = function(...args) {
  if ((typeof args[0] === 'string' && args[0].startsWith('{')) || Buffer.isBuffer(args[0])) socketId(this);
  return write.apply(this, args);
};
net.Socket.prototype.push = function(chunk, ...rest) {
  if (Buffer.isBuffer(chunk)) record({ event: 'socket-raw', socket: socketId(this), bytes: chunk.length, base64: chunk.toString('base64') });
  return push.call(this, chunk, ...rest);
};
net.Socket.prototype.emit = function(event, ...args) {
  const identity = event === 'connect' || event === 'close' || event === 'error' ? socketId(this) : undefined;
  if (event === 'close') nativeSockets.delete(this);
  if (identity !== undefined) record({ event: `socket-${event}`, socket: identity,
    ...(event === 'close' ? { hadError: args[0] === true } : event === 'error' ? { error: String(args[0]).slice(0, 2048), code: args[0]?.code } : {}) });
  return emit.call(this, event, ...args);
};
const { CodeGraph } = require('../../dist/index');
for (const name of ['open', 'openSync']) {
  const original = CodeGraph[name];
  CodeGraph[name] = function(...args) { record({ event: `graph-${name}`, root: args[0] }); return original.apply(this, args); };
}
for (const name of ['sync', 'watch', 'unwatch']) {
  const original = CodeGraph.prototype[name];
  CodeGraph.prototype[name] = function(...args) { record({ event: `graph-${name}`, root: this.getProjectRoot() }); return original.apply(this, args); };
}
const engineModule = require('../../dist/mcp/engine'), Engine = engineModule.MCPEngine;
const handlerEngines = new WeakMap();
engineModule.MCPEngine = new Proxy(Engine, { construct(target, args, newTarget) {
  const held = roots.map(root => {
    const directory = path.join(root, '.codegraph'), owners = {};
    for (const name of ['daemon.pid','writer.pid']) {
      const file = path.join(directory, name);
      if (!fs.existsSync(file)) { owners[name] = { present: false }; continue; }
      const stat = fs.lstatSync(file);
      if (!stat.isFile() || stat.size > 65536) throw new Error('Invalid/bounded owner record at factory boundary');
      const bytes = fs.readFileSync(file); owners[name] = { present: true, base64: bytes.toString('base64') };
    }
    const index = path.join(directory, 'codegraph.db');
    return { root, index: fs.existsSync(index) ? fs.realpathSync(index) : null, owners };
  });
  record({ event: 'engine-before-constructor', held });
  const engine = Reflect.construct(target, args, newTarget);
  handlerEngines.set(engine.getToolHandler(), engine);
  record({ event: 'engine-constructor', diagnostics: engine.getQueryDiagnostics() });
  return engine;
} });
// Observe the REAL retirement operation and generation. Never manufacture a
// replacement engine or change the actual drain/stop result.
for (const name of ['drain', 'stop']) {
  const original = Engine.prototype[name];
  Engine.prototype[name] = async function(...args) {
    const generation = this.getQueryExecutionContext().generation;
    record({ event: `engine-${name}-start`, generation });
    try { const value = await original.apply(this, args); record({ event: `engine-${name}-complete`, generation }); return value; }
    catch (error) { record({ event: `engine-${name}-error`, generation, error: String(error).slice(0, 2048) }); throw error; }
  };
}
const { ToolHandler } = require('../../dist/mcp/tools'), execute = ToolHandler.prototype.execute;
ToolHandler.prototype.execute = async function(...args) {
  const owner = handlerEngines.get(this), context = args[3];
  record({ event: 'tool-start', name: args[0], args: args[1], requestId: context?.requestId,
    sessionId: context?.sessionId, generation: context?.execution?.generation,
    diagnostics: owner?.getQueryDiagnostics() });
  try { return await execute.apply(this, args); }
  finally { const engine = handlerEngines.get(this); if (engine) record({ event: 'engine-diagnostics', diagnostics: engine.getQueryDiagnostics() }); }
};
const { ExploreSessionState } = require('../../dist/mcp/explore-session-state');
const historyRecord = ExploreSessionState.prototype.record;
const historyIds = new WeakMap(); let nextHistory = 0;
ExploreSessionState.prototype.record = function(...args) {
  const result = historyRecord.apply(this, args);
  if (result) {
    if (!historyIds.has(this)) historyIds.set(this, `${process.pid}:${++nextHistory}`);
    record({ event: 'history', historyId: historyIds.get(this), root: result.projectRoot, index: result.index, query: result.query });
  }
  return result;
};
if (threads.isMainThread) {
  const Worker = threads.Worker;
  threads.Worker = class extends Worker {
    constructor(file, options) {
      super(file, options);
      if (!String(file).replaceAll('\\', '/').endsWith('/mcp/query-worker.js')) return;
      const id = this.threadId;
      record({ event: 'worker-spawn', workerThreadId: id, execution: options.workerData.execution });
      this.on('message', message => { if (['ready', 'result'].includes(message.type)) record({ event: `worker-${message.type}`, workerThreadId: id, id: message.id, ok: message.ok }); });
      this.on('exit', code => record({ event: 'worker-exit', workerThreadId: id, code }));
      this.on('error', error => record({ event: 'worker-error', workerThreadId: id, error: String(error).slice(0, 2048) }));
    }
  };
  const cp = require('node:child_process'), spawn = cp.spawn;
  cp.spawn = function(command, args, options) {
    const child = spawn.call(this, command, args, options);
    if (!options?.detached || options.env?.CODEGRAPH_DAEMON_INTERNAL !== '1') return child;
    const declared = control(); let statuses = 0, acknowledgements = 0;
    const selectedRoot = args[args.lastIndexOf('--path') + 1];
    if (!Number.isInteger(child.pid) || child.pid <= 0 || !roots.includes(selectedRoot)) { failure('Actual candidate identity/root is not owned'); return child; }
    try { fs.writeFileSync(path.join(traceDir, `candidate-spawn-${child.pid}.json`), JSON.stringify({ candidatePid: child.pid, launcherPid: process.pid, root: selectedRoot, command, args, identity: 'actual-delegated-child-spawn' }), { flag: 'wx' }); }
    catch (error) { failure(error); }
    record({ event: 'candidate-spawn', candidatePid: child.pid, args, command, attempt: declared?.attempt,
      startupCpu: options.env.CODEGRAPH_INTERNAL_QUERY_STARTUP_CPU_V1 ?? null,
      controls: Object.fromEntries(['CODEGRAPH_QUERY_MODE','CODEGRAPH_QUERY_POOL_SIZE','CODEGRAPH_QUERY_BUSY_TIMEOUT_MS'].map(key => [key, options.env[key] ?? null])) });
    child.on('message', message => { statuses++; record({ event: 'candidate-status', candidatePid: child.pid, message }); });
    const send = child.send;
    if (send) child.send = function(...values) {
      const message = values[0];
      if (message?.protocol === 1 && message.kind === 'acknowledged') acknowledgements++;
      record({ event: 'candidate-parent-send', candidatePid: child.pid, message });
      return send.apply(this, values);
    };
    child.on('exit', (code, signal) => record({ event: 'candidate-exit', candidatePid: child.pid, code, signal, statuses, acknowledgements }));
    child.on('close', (code, signal) => record({ event: 'candidate-close', candidatePid: child.pid, code, signal, statuses, acknowledgements }));
    child.on('error', error => record({ event: 'candidate-error', candidatePid: child.pid, error: String(error).slice(0, 2048) }));
    if (declared && ['stop-before-status', 'exit78-before-status', 'hold-before-status'].includes(declared.fault)) {
      try {
        fs.writeFileSync(`${controlPath}.${declared.attempt}.claim`, JSON.stringify({ pid: child.pid, attempt: declared.attempt }), { flag: 'wx' });
        const deadline = Date.now() + 3000;
        const timer = setInterval(() => {
          if (fs.existsSync(path.join(traceDir, `candidate-${child.pid}-entered.json`))) {
            clearInterval(timer);
            if (statuses || acknowledgements) { failure('Candidate acquired bootstrap status/ACK before controlled pre-service stop'); return; }
            record({ event: 'candidate-pre-service-barrier', candidatePid: child.pid, attempt: declared.attempt, statuses, acknowledgements });
            if (declared.fault === 'stop-before-status') {
              record({ event: 'candidate-controlled-stop', candidatePid: child.pid, signal: 'SIGKILL', statuses, acknowledgements });
              child.kill('SIGKILL');
            }
          } else if (Date.now() > deadline) { clearInterval(timer); failure('Owned candidate never reached pre-service barrier'); }
        }, 10);
        child.once('close', () => clearInterval(timer));
      } catch (error) {
        if (error.code === 'EEXIST') record({ event: 'candidate-later-attempt-no-fault', candidatePid: child.pid, attempt: declared.attempt });
        else failure(error);
      }
    }
    return child;
  };
  // Owned cleanup is a typed exact-root/PID filesystem command. It delegates
  // actual Daemon.stop, never a launch/permission/liveness surrogate.
  const { Daemon } = require('../../dist/mcp/daemon'), start = Daemon.prototype.start;
  Daemon.prototype.start = async function(...args) {
    const value = await start.apply(this, args), daemon = this;
    const timer = setInterval(() => {
      const file = traceDir && path.join(traceDir, `stop-daemon-${process.pid}.json`);
      if (!file || !fs.existsSync(file)) return;
      try {
        const request = readJson(file);
        if (request.kind !== 'stop-owned-daemon' || request.pid !== process.pid || request.root !== daemon.projectRoot || !roots.includes(request.root)) throw new Error('Invalid owned daemon stop identity');
        clearInterval(timer);
        // Product stop owns finalization: POSIX may exit inside stop; Windows
        // returns with exitCode and its native watcher-drain/backstop intact.
        // A returned stop is NOT a fabricated native process/socket close.
        Promise.resolve(daemon.stop()).then(() => {
          record({ event: 'owned-daemon-stop-returned', root: request.root, openObservedSockets: nativeSockets.size });
        }).catch(failure);
      } catch (error) { clearInterval(timer); failure(error); }
    }, 10); timer.unref();
    return value;
  };
}
// Candidate barrier runs at actual CLI preload entry, before the real module's
// policy status/ACK or any schema/authority. Parent retains original argv.
if (threads.isMainThread && process.env.CODEGRAPH_DAEMON_INTERNAL === '1') {
  const declared = control();
  if (declared && ['stop-before-status', 'exit78-before-status', 'hold-before-status'].includes(declared.fault)) {
    const existingClaim = `${controlPath}.${declared.attempt}.claim`;
    const belongsToEarlierCandidate = fs.existsSync(existingClaim) && readJson(existingClaim).pid !== process.pid;
    if (!belongsToEarlierCandidate) {
    const gate = new Int32Array(new SharedArrayBuffer(4)), deadline = Date.now() + 5000;
    let claim;
    while (Date.now() < deadline) {
      const file = `${controlPath}.${declared.attempt}.claim`;
      if (fs.existsSync(file)) { claim = readJson(file); break; }
      Atomics.wait(gate, 0, 0, 10);
    }
    if (claim?.pid !== process.pid) throw new Error('Owned candidate barrier identity mismatch');
    fs.writeFileSync(path.join(traceDir, `candidate-${process.pid}-entered.json`), JSON.stringify({ pid: process.pid, attempt: declared.attempt, argv: process.argv, startupCpu: process.env.CODEGRAPH_INTERNAL_QUERY_STARTUP_CPU_V1 ?? null, controls: Object.fromEntries(['CODEGRAPH_QUERY_MODE','CODEGRAPH_QUERY_POOL_SIZE','CODEGRAPH_QUERY_BUSY_TIMEOUT_MS'].map(key => [key, process.env[key] ?? null])) }));
    record({ event: 'candidate-entered-before-status', attempt: declared.attempt, argv: process.argv, startupCpu: process.env.CODEGRAPH_INTERNAL_QUERY_STARTUP_CPU_V1 ?? null });
    if (declared.fault === 'exit78-before-status') {
      record({ event: 'candidate-controlled-exit', attempt: declared.attempt, code: 78, meaning: 'controlled native pre-service failure; real CLI body has not executed' });
      process.exit(78);
    }
    while (!fs.existsSync(`${controlPath}.${declared.attempt}.release`) && Date.now() < deadline) Atomics.wait(gate, 0, 0, 10);
    if (Date.now() >= deadline) throw new Error('Owned pre-service barrier expired without native stop/release');
    record({ event: 'candidate-barrier-released', attempt: declared.attempt });
    }
  }
}
process.once('exit', code => record({ event: 'process-exit', code, firstFailure }));
if (require.main === module && process.argv[2] === '--ordinary-writer') {
  const root = fs.realpathSync(process.argv[3]);
  const engine = new engineModule.MCPEngine({ watch: true, queryPool: false });
  let stopping;
  process.on('message', request => {
    if (request?.kind !== 'stop-ordinary-writer' || request.pid !== process.pid || request.root !== root) { failure('Invalid ordinary-writer IPC stop'); return; }
    stopping ||= Promise.resolve(engine.stop()).then(() => {
      process.send?.({ kind: 'ordinary-writer-stopped', root, pid: process.pid }); process.disconnect();
    }, failure);
  });
  engine.ensureInitialized(root).then(() => {
    const writer = readJson(path.join(root, '.codegraph/writer.pid'));
    if (writer.pid !== process.pid || writer.mode !== 'fallback') throw new Error('Ordinary writer did not hold actual fallback lease');
    process.send?.({ kind: 'ordinary-writer-ready', root, pid: process.pid, writer });
  }).catch(failure);
}
