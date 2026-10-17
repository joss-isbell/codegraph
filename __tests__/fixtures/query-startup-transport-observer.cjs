// Observe the actual CLI/candidate/SQLite boundary; delegate every product call.
// The only controlled changes are a later parent environment/CPU mutation and
// explicit corruption of one native candidate's private transport for negatives.
const fs = require('node:fs');
const threads = require('node:worker_threads');
const os = require('node:os');
const cp = require('node:child_process');
const metadata = 'CODEGRAPH_INTERNAL_QUERY_STARTUP_CPU_V1';
const controls = ['CODEGRAPH_QUERY_MODE', 'CODEGRAPH_QUERY_POOL_SIZE', 'CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', metadata];
const raw = env => Object.fromEntries(controls.filter(key => env[key] !== undefined).map(key => [key, env[key]]));
const record = value => {
  const line = Buffer.from(JSON.stringify({ eventTimeNs: process.hrtime.bigint().toString(),
    pid: process.pid, threadId: threads.threadId, ...value }) + '\n');
  const file = process.env.QUERY_UNLIMITED_OBSERVATIONS;
  if (line.length > 65536 || (fs.existsSync(file) ? fs.statSync(file).size : 0) + line.length > 4194304) {
    throw new Error('bounded startup observer overflow');
  }
  fs.appendFileSync(file, line);
};

// This constructor is the real native SQLite implementation, including schema
// probes. No handle, SQL, result or policy is substituted.
const sqlite = require('node:sqlite');
const NativeDatabase = sqlite.DatabaseSync;
sqlite.DatabaseSync = class extends NativeDatabase {
  constructor(file, ...args) { record({ event: 'startup-sqlite-open', file: String(file) }); super(file, ...args); }
};

if (!threads.isMainThread && threads.workerData?.execution) {
  record({ event: 'startup-worker-env', raw: raw(process.env), execution: threads.workerData?.execution });
} else if (threads.isMainThread) {
  // Exercise the real CLI pipe adapter. Valid output is only fragmented;
  // negative rows deliberately damage native wire bytes, never invent results.
  const wire = process.env.QUERY_STARTUP_WIRE;
  if (wire === 'stderr-invalid' || wire === 'stderr-incomplete') {
    process.once('exit', () => {
      const bytes = Buffer.from(wire === 'stderr-invalid' ? [0xff] : [0xe2, 0x82]);
      record({ event: 'startup-wire', wire, stream: 'stderr', injectedBase64: bytes.toString('base64') });
      fs.writeSync(2, bytes);
    });
  } else if (wire) {
    const nativeWrite = process.stdout.write.bind(process.stdout);
    let selected = false, splitting = false, queuedBytes = 0, bytesBefore = 0;
    const queue = [];
    process.stdout.write = function(chunk, encoding, callback) {
      const done = typeof encoding === 'function' ? encoding : callback;
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk, typeof encoding === 'string' ? encoding : 'utf8');
      if (splitting) {
        queuedBytes += bytes.length;
        if (queuedBytes > 65536) throw new Error('bounded native wire queue overflow');
        queue.push({ bytes, done }); return true;
      }
      const boundary = bytes.findIndex(byte => byte >= 0xc2 && byte <= 0xf4);
      if (selected || boundary < 0) { bytesBefore += bytes.length; return nativeWrite(bytes, done); }
      selected = true;
      record({ event: 'startup-wire', wire, stream: 'stdout', boundary: boundary + 1,
        bytesBefore, absoluteBoundary: bytesBefore + boundary + 1,
        originalBase64: bytes.toString('base64') });
      if (wire === 'invalid') { const broken = Buffer.from(bytes); broken[boundary] = 0xff; return nativeWrite(broken, done); }
      if (wire === 'incomplete') return nativeWrite(bytes.subarray(0, boundary + 1), done);
      if (wire !== 'split') throw new Error('unknown native wire fixture mode');
      splitting = true;
      const accepted = nativeWrite(bytes.subarray(0, boundary + 1));
      setTimeout(() => {
        nativeWrite(bytes.subarray(boundary + 1), done);
        splitting = false;
        for (const item of queue) nativeWrite(item.bytes, item.done);
        queue.length = 0;
      }, 25);
      return accepted;
    };
  }
  const qualified = process.env.CODEGRAPH_DAEMON_INTERNAL === '1' && process.connected === true;
  const verifiedRelease = process.env.QUERY_STARTUP_VERIFIED_SOCKET_RELEASE;
  if (!qualified && verifiedRelease) {
    process.stdin.once('end', () => record({ event: 'startup-stdin-end' }));
    const proxy = require('../../dist/mcp/proxy');
    const originalConnect = proxy.connectWithHello;
    let held = false;
    proxy.connectWithHello = async function(...args) {
      const socket = await originalConnect(...args);
      if (!socket || held) return socket;
      held = true;
      const socketId = `${process.pid}:verified:1`;
      socket.once('close', hadError => record({ event: 'startup-held-socket-close', socketId, hadError }));
      record({ event: 'startup-held-verified-socket', socketId });
      // Delay only the return of a real verified socket. Native hello, owner
      // evidence and close events remain unmodified; the real outer EOF aborts.
      const deadline = Date.now() + 5000;
      try {
        while (!fs.existsSync(verifiedRelease)) {
          if (Date.now() >= deadline) throw new Error('owned verified socket release deadline');
          await new Promise(resolve => setTimeout(resolve, 10));
        }
      } catch (primary) {
        socket.destroy();
          record({ event: 'startup-held-socket-wait-failed', socketId, failure: String(primary).slice(0, 1024) });
          throw primary;
        }
        record({ event: 'startup-held-socket-return', socketId });
        return socket;
    };
  }
  if (qualified) process.once('exit', code => record({ event: 'startup-candidate-own-exit', code }));
  if (qualified && process.env.QUERY_STARTUP_DISCONNECT === 'before-capture') {
    process.once('disconnect', () => record({ event: 'startup-native-disconnect' }));
    process.disconnect();
  }
  let constructingEngine = false;
  const observedCpu = count => () => {
    if (constructingEngine) record({ event: 'startup-engine-cpu-recapture', qualified, count });
    return Array(count).fill({});
  };
  os.cpus = observedCpu(qualified ? 97 : 3);
  const capture = require('../../dist/mcp/query-startup-transport');
  const originalCapture = capture.captureMcpQueryStartup;
  let server;
  capture.captureMcpQueryStartup = function(env, lazyCpu, liveIpc, root, nativeIpc) {
    let cpuObservations = 0;
    try {
      const startup = originalCapture(env, () => { cpuObservations++; return lazyCpu(); }, liveIpc, root, nativeIpc);
      record({ event: 'startup-capture', qualified, startup, cpuObservations, rawAfter: raw(env), root });
      if (!qualified) {
        env.CODEGRAPH_QUERY_MODE = 'unlimited';
        env.CODEGRAPH_QUERY_POOL_SIZE = '20';
        env.CODEGRAPH_QUERY_BUSY_TIMEOUT_MS = '1000';
        os.cpus = observedCpu(97);
        record({ event: 'startup-later-parent-env', raw: raw(env), cpuCount: 97 });
      } else if (process.env.QUERY_STARTUP_DESCENDANT === '1') {
        // One inert owned native descendant proves actual env scrubbing. It
        // stays alive briefly for the independent runner's creation-ID census.
        const child = cp.spawn(process.execPath, ['-e',
          'console.log(JSON.stringify(Object.fromEntries(Object.entries(process.env).filter(([k])=>["CODEGRAPH_QUERY_MODE","CODEGRAPH_QUERY_POOL_SIZE","CODEGRAPH_QUERY_BUSY_TIMEOUT_MS","CODEGRAPH_INTERNAL_QUERY_STARTUP_CPU_V1"].includes(k)))));setTimeout(()=>{},500)'],
        { env, windowsHide: true, stdio: ['ignore', 'pipe', 'pipe'] });
        record({ event: 'startup-descendant-spawn', candidatePid: child.pid });
        let stdout = Buffer.alloc(0), stderr = Buffer.alloc(0), seen = 0, failure;
        const retain = (data, which) => {
          const bytes = Buffer.from(data); seen += bytes.length;
          if (seen > 65536) failure ??= 'inert descendant capture overflow';
          if (which === 'stdout') stdout = Buffer.concat([stdout, bytes.subarray(0, Math.max(0, 32768 - stdout.length))]);
          else stderr = Buffer.concat([stderr, bytes.subarray(0, Math.max(0, 32768 - stderr.length))]);
        };
        child.stdout.on('data', data => retain(data, 'stdout'));
        child.stderr.on('data', data => retain(data, 'stderr'));
        child.on('error', error => { failure ??= String(error).slice(0, 1024); });
        const deadline = setTimeout(() => { failure ??= 'inert descendant 5000ms close deadline'; child.kill(); }, 5000);
        child.once('close', (code, signal) => {
          clearTimeout(deadline);
          record({ event: 'startup-descendant-close', candidatePid: child.pid, code, signal,
            stdout: stdout.toString(), stderr: stderr.toString(), seenBytes: seen,
            retainedBytes: stdout.length + stderr.length, failure: failure ?? null });
        });
      }
      return startup;
    } catch (error) {
      record({ event: 'startup-capture-refused', qualified, cpuObservations, rawAfter: raw(env),
        nativeIpc, liveIpc,
        refusal: { code: error.code, reason: error.reason, projectRoot: error.projectRoot } });
      throw error;
    }
  };
  const engine = require('../../dist/mcp/engine');
  const OriginalEngine = engine.MCPEngine;
  engine.MCPEngine = class extends OriginalEngine {
    constructor(options) {
      record({ event: 'startup-engine', options,
        hasResourceObserver: !!options.resourceObserver,
        hasDefaultOutcomeCallback: typeof options.onInitialDefaultOutcome === 'function',
        sameProspective: options.queryAdmissionPolicy === server?.queryAdmissionPolicy,
        sameDirect: options.executionMode === 'direct' ? options.queryPolicy === server?.directQueryPolicy : null });
      constructingEngine = true;
      try { super(options); } finally { constructingEngine = false; }
    }
  };
  const mcp = require('../../dist/mcp');
  const OriginalServer = mcp.MCPServer;
  mcp.MCPServer = class extends OriginalServer {
    constructor(...args) {
      super(...args); server = this;
      record({ event: 'startup-server', startup: this.queryStartup, prospective: this.queryAdmissionPolicy,
        direct: this.directQueryPolicy, frozen: Object.isFrozen(this.queryStartup) && Object.isFrozen(this.queryStartup.requested) });
    }
  };
  const admission = require('../../dist/mcp/project-lifecycle');
  const originalAdmit = admission.admitQueryProject;
  admission.admitQueryProject = async function(root, policy, signal) {
    const result = await originalAdmit(root, policy, signal);
    record({ event: 'startup-admitted', root, policy, sameProspective: policy === server?.queryAdmissionPolicy });
    return result;
  };
  const bootstrap = require('../../dist/mcp/query-bootstrap');
  const originalConfirm = bootstrap.confirmDaemonBootstrap;
  bootstrap.confirmDaemonBootstrap = async function(root) {
    const result = await originalConfirm(root); record({ event: 'startup-acknowledged', root }); return result;
  };
  const originalObserve = bootstrap.awaitDaemonBootstrap;
  bootstrap.awaitDaemonBootstrap = async function(child, root, options) {
    const result = await originalObserve(child, root, options);
    record({ event: 'startup-bootstrap-result', candidatePid: child.pid, result, options }); return result;
  };
  const originalReport = bootstrap.reportDaemonBootstrapRefusal;
  bootstrap.reportDaemonBootstrapRefusal = async function(refusal) {
    record({ event: 'startup-refusal-produced', refusal }); return originalReport(refusal);
  };
  require('./query-unlimited-preload.cjs');
  const originalSpawn = cp.spawn;
  cp.spawn = function(command, args, options) {
    if (!options?.detached || options.env?.CODEGRAPH_DAEMON_INTERNAL !== '1') return originalSpawn.call(this, command, args, options);
    const corruption = process.env.QUERY_STARTUP_CORRUPT;
    if (corruption === 'missing') delete options.env[metadata];
    if (corruption === 'extra-key') options.env[metadata] = '{"protocol":1,"cpuCount":3,"extra":true}';
    if (corruption === 'control') options.env.CODEGRAPH_QUERY_POOL_SIZE = '';
    const child = originalSpawn.call(this, command, args, options);
    record({ event: 'startup-candidate-spawn', candidatePid: child.pid, raw: raw(options.env), selector: options.env.CODEGRAPH_DIR });
    child.on('message', status => record({ event: 'startup-bootstrap-status', candidatePid: child.pid, status }));
    const originalSend = child.send;
    child.send = function(status, ...rest) {
      if (status?.protocol === 1 && status.kind === 'acknowledged') {
        record({ event: 'startup-bootstrap-ack-sent', candidatePid: child.pid, status });
      }
      return originalSend.call(this, status, ...rest);
    };
    child.once('close', (code, signal) => record({ event: 'startup-candidate-close', candidatePid: child.pid, code, signal }));
    return child;
  };
}
