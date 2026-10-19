// Observation-only native boundary fixture. No policy, factory, pool, worker
// filename, workerData, handler result, or history decision is substituted.
const fs = require('node:fs');
const crypto = require('node:crypto');
const threads = require('node:worker_threads');
const path = require('node:path');
const epoch = process.env.QUERY_PUBLIC_LIFETIME_EPOCH;
const gates = process.env.QUERY_PUBLIC_LIFETIME_GATES;
const observations = process.env.QUERY_PUBLIC_LIFETIME_OBSERVATIONS;
const record = value => {
  const line = JSON.stringify({ monotonicNs: process.hrtime.bigint().toString(),
    pid: process.pid, threadId: threads.threadId, ...value }) + '\n';
  if (Buffer.byteLength(line) > 131072) throw new Error('lifetime observation frame exceeded 131072 bytes');
  fs.appendFileSync(observations, line);
};
const deeplyFrozen = value => !value || typeof value !== 'object'
  || Object.isFrozen(value) && Object.values(value).every(deeplyFrozen);
const tags = new Set(['finiteRunningSymbol', 'finiteQueuedSymbol', 'finitePeerSymbol',
  'absentHealthySymbol', 'absentRunningSymbol', 'absentQueuedSymbol', 'bridgeHealthySymbol',
  'bridgeRunningSymbol', 'bridgeQueuedSymbol', 'shutdownRunningSymbol', 'recoveryRecoveredSymbol',
  'exhaustionDemand0', 'exhaustionDemand1', 'exhaustionDemand2', 'exhaustionDemand3', 'exhaustionDemand4', 'exhaustionDemand5']);
const awaitFile = (file, milliseconds, label, facts = {}) => {
  const deadline = Date.now() + milliseconds, pause = new Int32Array(new SharedArrayBuffer(4));
  while (!fs.existsSync(file)) {
    if (Date.now() >= deadline) {
      record({ event: 'gate-deadline', label, ...facts });
      throw new Error(`lifetime ${label} release deadline`);
    }
    Atomics.wait(pause, 0, 0, 10);
  }
};

// This IS the original real detached CLI candidate with its original argv/env/
// stdio. Inherited --require executes before the CLI module. No service code,
// engine construction, authority grant, substitute script or listener runs.
if (threads.isMainThread && process.env.CODEGRAPH_DAEMON_INTERNAL === '1'
    && epoch?.endsWith('-writable')) {
  record({ event: 'candidate-awaiting-exit78', preCli: true, argv: process.argv, execArgv: process.execArgv });
  awaitFile(process.env.QUERY_PUBLIC_LIFETIME_CANDIDATE_RELEASE, 12000, 'candidate identity before pre-CLI exit');
  record({ event: 'candidate-exiting-before-cli', preCli: true, code: 78 });
  process.exit(78);
}

if (threads.isMainThread) {
  // The independent native OS/runner observer acquires host creation identity
  // before this gate allows the actual CLI to construct an engine or serve MCP.
  record({ event: 'host-awaiting-identity' });
  const deadline = Date.now() + 10000;
  const pause = new Int32Array(new SharedArrayBuffer(4));
  while (!fs.existsSync(process.env.QUERY_PUBLIC_LIFETIME_BEGIN)) {
    if (Date.now() >= deadline) throw new Error('lifetime prebegin identity deadline');
    Atomics.wait(pause, 0, 0, 10);
  }
  record({ event: 'host-begin' });
  process.stdin.once('end', () => record({ event: 'public-stdin-end' }));

  // Observe the exact original real spawn call, without replacing/copying its
  // arguments or options. The inherited preload above owns only exit stimulus.
  const childProcess = require('node:child_process');
  const originalSpawn = childProcess.spawn;
  childProcess.spawn = function(...args) {
    const child = originalSpawn.apply(this, args);
    const options = args[2];
    if (options?.detached && options.env?.CODEGRAPH_DAEMON_INTERNAL === '1') {
      const candidatePid = child.pid;
      record({ event: 'candidate-created', candidatePid, command: args[0], argv: args[1],
        stdio: options.stdio, detached: options.detached, originalArguments: true, originalOptions: true,
        ipcConfigured: Array.isArray(options.stdio) && options.stdio.includes('ipc') });
      child.on('error', error => record({ event: 'fixture-failure', scope: 'candidate', candidatePid, detail: String(error).slice(0, 4096) }));
      child.on('exit', (code, signal) => record({ event: 'candidate-exit', candidatePid, code, signal }));
      child.on('close', (code, signal) => record({ event: 'candidate-close', candidatePid, code, signal,
        connected: child.connected ?? false, channelReleased: child.channel == null }));
    }
    return child;
  };

  const OriginalWorker = threads.Worker;
  let ordinal = 0;
  threads.Worker = class extends OriginalWorker {
    constructor(...args) {
      super(...args);
      if (!String(args[0]).replaceAll('\\', '/').endsWith('/mcp/query-worker.js')) return;
      const creationOrdinal = ++ordinal;
      const threadId = this.threadId;
      record({ event: 'worker-created', creationOrdinal, threadId,
        filename: String(args[0]), execution: args[1]?.workerData?.execution });
      this.on('message', message => {
        if (message?.type === 'ready') record({ event: 'worker-ready', creationOrdinal, threadId, ok: message.ok });
        if (message?.type === 'result') record({ event: 'worker-result', creationOrdinal, threadId,
          id: message.id, context: message.context });
      });
      this.on('error', error => record({ event: 'worker-error', creationOrdinal, threadId, detail: String(error).slice(0, 4096) }));
      this.on('exit', code => record({ event: 'worker-exit', creationOrdinal, threadId, code }));
    }
  };
  const engineModule = require('../../dist/mcp/engine');
  const OriginalEngine = engineModule.MCPEngine;
  let engineOrdinal = 0;
  engineModule.MCPEngine = class extends OriginalEngine {
    constructor(...args) {
      super(...args);
      const options = args[0];
      const execution = this.getQueryExecutionContext();
      record({ event: 'engine-created', engineOrdinal: ++engineOrdinal,
        originalPolicyReference: options?.queryPolicy === execution.policy,
        optionsPolicyFrozen: deeplyFrozen(options?.queryPolicy), contextFrozen: deeplyFrozen(execution),
        optionsPolicy: options?.queryPolicy, execution });
      if (epoch === 'absent-direct') {
        // Explicit INTERNAL bridge. The only engine is this actual CLI-created
        // original instance; original sessions/transports own all dispatch,
        // cancellation, delivery and history. No public stdio multiplexing claim.
        const retained = this;
        const net = require('node:net');
        const { MCPSession } = require('../../dist/mcp/session');
        const { SocketTransport } = require('../../dist/mcp/transport');
        const sessions = new Set();
        let connections = 0, stopRequested = false;
        const server = net.createServer(socket => {
          const label = ++connections === 1 ? 'A' : 'B';
          if (connections > 2) {
            record({ event: 'fixture-failure', scope: 'bridge', detail: 'more than2 bridge sockets' }); socket.destroy(); return;
          }
          const transport = new SocketTransport(socket);
          const session = new MCPSession(transport, retained, { explicitProjectPath: process.env.QUERY_PUBLIC_LIFETIME_ROOT });
          sessions.add(session);
          record({ event: 'bridge-session-created', label, engineOrdinal, sameEngineReference: retained === this,
            samePolicyReference: retained.getQueryExecutionContext().policy === execution.policy });
          transport.onClose(() => record({ event: 'bridge-transport-close', label, engineOrdinal }));
          socket.on('end', () => record({ event: 'bridge-socket-end', label }));
          socket.on('close', hadError => { record({ event: 'bridge-socket-close', label, hadError }); sessions.delete(session); });
          socket.on('error', error => record({ event: 'fixture-failure', scope: `bridge-${label}`, detail: String(error).slice(0, 4096) }));
          session.start();
        });
        server.on('error', error => record({ event: 'fixture-failure', scope: 'bridge-listener', detail: String(error).slice(0, 4096) }));
        server.on('close', () => record({ event: 'bridge-listener-close', engineOrdinal }));
        server.listen(0, '127.0.0.1', () => record({ event: 'bridge-listening', engineOrdinal,
          sameEngineReference: retained === this, host: '127.0.0.1', port: server.address().port }));
        const observer = setInterval(() => {
          if (stopRequested || !fs.existsSync(path.join(gates, 'bridge-stop'))) return;
          stopRequested = true; clearInterval(observer);
          for (const session of sessions) session.stop();
          server.close();
        }, 10);
      }
    }
  };
  const { ExploreSessionState } = require('../../dist/mcp/explore-session-state');
  const originalRecord = ExploreSessionState.prototype.record;
  const histories = new WeakMap();
  let historyOrdinal = 0;
  ExploreSessionState.prototype.record = function(...args) {
    const result = originalRecord.apply(this, args);
    if (result) {
      if (!histories.has(this)) histories.set(this, ++historyOrdinal);
      record({ event: 'delivered-history', historyOrdinal: histories.get(this),
        projectRoot: result.projectRoot, query: result.query, index: result.index });
    }
    return result;
  };
} else if (threads.workerData?.execution) {
  const port = threads.parentPort;
  const safeExit = reason => {
    const { collectBeforeExit } = require('../../dist/worker-teardown');
    const collected = collectBeforeExit();
    record({ event: 'safe-self-exit', reason, collected, code: 73 });
    process.exit(73);
  };
  if (epoch?.startsWith('timer-free-recovery-')) {
    // The original built worker has loaded and is about to publish its real
    // ready handshake. Failed-start stimulus safely self-exits instead; no fake
    // ready/result is emitted, and parent filename/options/workerData are intact.
    const originalPost = port.postMessage;
    port.postMessage = function(...args) {
      if (args[0]?.type === 'ready') {
        let first = false;
        try { const fd = fs.openSync(path.join(gates, 'recovery-first-claimed'), 'wx'); fs.closeSync(fd); first = true; }
        catch (error) { if (error.code !== 'EEXIST') throw error; }
        if (first || fs.existsSync(path.join(gates, 'recovery-exhaust'))) {
          const reason = first ? 'first-pre-ready' : 'persistent-pre-ready';
          record({ event: 'startup-exit-awaiting', reason, originalReady: args[0] });
          awaitFile(path.join(gates, first ? 'recovery-first-release' : 'recovery-fail-release'), 12000, reason);
          safeExit(reason);
        }
        record({ event: 'startup-ready-awaiting', originalReady: args[0] });
        awaitFile(path.join(gates, 'recovery-ready-release'), 12000, 'actual replacement ready');
      }
      return originalPost.apply(this, args);
    };
  }
  const originalOn = port.on;
  port.on = function(event, listener) {
    if (event !== 'message') return originalOn.call(this, event, listener);
    return originalOn.call(this, event, function(...args) {
      const message = args[0];
      if (message?.type === 'call') {
        record({ event: 'incoming-call', id: message.id, toolName: message.toolName,
          query: message.args?.query, context: message.context });
        if (message.toolName === 'codegraph_explore' && message.args?.query === 'finiteRunningSymbol') {
          const deadline = Date.now() + 25000;
          const pause = new Int32Array(new SharedArrayBuffer(4));
          while (!fs.existsSync(process.env.QUERY_PUBLIC_LIFETIME_RELEASE)) {
            if (Date.now() >= deadline) {
              record({ event: 'gate-deadline', id: message.id });
              throw new Error('lifetime incoming-call release deadline');
            }
            Atomics.wait(pause, 0, 0, 10);
          }
          record({ event: 'incoming-call-released', id: message.id, context: message.context });
        }
        const query = message.args?.query;
        if (message.toolName === 'codegraph_explore' && ['absentHealthySymbol', 'absentRunningSymbol',
          'bridgeHealthySymbol', 'bridgeRunningSymbol', 'shutdownRunningSymbol'].includes(query)) {
          awaitFile(path.join(gates, `${query}.release`), query === 'absentHealthySymbol' ? 75000 : query === 'bridgeHealthySymbol' ? 25000 : 12000,
            `original incoming call ${query}`, { id: message.id, query });
          record({ event: 'incoming-call-released', id: message.id, query, context: message.context });
        }
        if (epoch?.startsWith('timer-free-recovery-') && query === 'exhaustionDemand0') {
          awaitFile(path.join(gates, 'recovery-loss-release'), 12000, 'original ready worker controlled loss', { id: message.id, query });
          safeExit('controlled-ready-loss');
        }
      }
      return listener.apply(this, args);
    });
  };
}

// This wrapper delegates the unchanged built ToolHandler in both native places.
// It makes a tagged owner-thread serial escape visible without fabricating output.
const { ToolHandler } = require('../../dist/mcp/tools');
const originalRead = ToolHandler.prototype.executeReadTool;
ToolHandler.prototype.executeReadTool = async function(...args) {
  const [toolName, arguments_, context] = args;
  const tagged = tags.has(arguments_?.query);
  if (tagged) record({ event: 'read-handler-start', toolName, query: arguments_.query, context,
    contextFrozen: deeplyFrozen(context) });
  const result = await originalRead.apply(this, args);
  if (tagged) {
    const text = result.content?.[0]?.text ?? '';
    record({ event: 'read-handler-result', toolName, query: arguments_.query, context,
      isError: result.isError ?? false, text, textBytes: Buffer.byteLength(text),
      textSha256: crypto.createHash('sha256').update(text).digest('hex') });
  }
  return result;
};
