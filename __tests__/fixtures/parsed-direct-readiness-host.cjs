// Native acquisition control only. MCP bytes are consumed solely by the
// production proxy's stdin parser; all direct requests use the real session.
const fs = require('node:fs');
const path = require('node:path');
const net = require('node:net');
const childProcesses = require('node:child_process');
// Observe the actual native watchdog, preserving spawn/kill/close semantics.
// This fixture never fabricates a close event or substitutes a watchdog.
const watchdogs = [];
const spawn = childProcesses.spawn;
childProcesses.spawn = function (...args) {
  const child = Reflect.apply(spawn, this, args);
  if (args[1]?.[0] === '-e' && String(args[1][1]).includes('Main thread unresponsive')) {
    const row = { pid: child.pid, paths: args[1].slice(5), stopRequested: false, closed: false };
    watchdogs.push(row);
    const kill = child.kill;
    child.kill = function (...killArgs) {
      // Timestamp precedes the unchanged native operation; persistence must
      // never extend the product's absolute close allowance.
      const startedNs = process.hrtime.bigint().toString();
      // Sticky observation of the unchanged native kill attempt, not a fake
      // WatchdogHandle state or close. A failed attempt must remain visible too.
      row.stopRequested = true;
      let result;
      try { result = Reflect.apply(kill, this, killArgs); }
      catch (error) {
        try { record('watchdog-stop-error', { pid: child.pid, startedNs, message: String(error).slice(0, 256) }); }
        catch (observationError) { rememberFailure(observationError); }
        throw error;
      }
      try { record('watchdog-stop-requested', { pid: child.pid, startedNs, signal: killArgs[0], result }); }
      catch (error) { rememberFailure(error); }
      return result;
    };
    child.once('close', (code, signal) => {
      row.closed = true; row.code = code; row.signal = signal;
      try { record('watchdog-closed', row); } catch (error) { rememberFailure(error); }
    });
    try { record('watchdog-spawned', row); } catch (error) { rememberFailure(error); }
  }
  return child;
};
const { runLocalHandshakeProxy } = require('../../dist/mcp/proxy');
const { MCPSession } = require('../../dist/mcp/session');
const { MCPEngine } = require('../../dist/mcp/engine');
const { resolveQueryPolicy } = require('../../dist/mcp/query-policy');
const { servicePolicyRefusal } = require('../../dist/mcp/query-service-compatibility');
const { captureMcpIndexRoute } = require('../../dist/directory');

const [root, scenario, evidencePath] = process.argv.slice(2);
const events = [];
const observerFailures = [];
let observerFailureCount = 0;
let engine;
let session;
let resourceStop;
let controls;
let fallbackHistory;
let failedDrainEngine;
let resolveInjectedDrain;
const injectedDrainGate = new Promise(resolve => { resolveInjectedDrain = resolve; });
const progressLeases = new Map();
const progressRequests = new Map();
const watchdogSnapshot = () => watchdogs.map(value => ({ ...value, paths: [...value.paths] }));
let resolvePreparation;
const preparationGate = new Promise(resolve => { resolvePreparation = resolve; });
let resolveResourceStopped;
const resourceStopped = new Promise(resolve => { resolveResourceStopped = resolve; });
function rememberFailure(error) {
  observerFailureCount++;
  if (observerFailures.length < 8) observerFailures.push(String(error).slice(0, 256));
  try { process.stderr.write('PARSED_READINESS_OBSERVER_FAILURE ' + String(error).slice(0, 256) + '\n'); }
  catch { /* native-exit evidence still carries sticky loss */ }
}
function record(event, detail = {}) {
  if (events.length >= 64) throw new Error('native fixture event bound exceeded');
  const value = { event, pid: process.pid, monotonicNs: process.hrtime.bigint().toString(), ...detail };
  events.push(value);
  const bytes = Buffer.from(JSON.stringify({ events, observerFailureCount, observerFailures }));
  if (bytes.length > 65536) throw new Error('native fixture evidence bound exceeded');
  fs.writeFileSync(evidencePath, bytes);
  if (event !== 'native-exit' && process.connected) process.send(value, error => {
    if (!error) return;
    rememberFailure(error);
    if (events.length < 64) events.push({ event: 'ipc-send-error', message: String(error).slice(0, 256) });
    try { fs.writeFileSync(evidencePath, JSON.stringify({ events, observerFailureCount, observerFailures })); }
    catch (writeError) { rememberFailure(writeError); }
    process.exitCode = 1;
  });
}
const refusal = () => servicePolicyRefusal('unknown', root, 'owned readiness refusal fixture');
function stop() {
  // The production caller must supply the same resource-only contract. There
  // is no process.exit here, including when no factory has run.
  if (!resourceStop) resourceStop = Promise.resolve().then(async () => {
    const failures = [];
    try { record('resource-stop-entered', { enginePresent: !!engine }); } catch (error) { failures.push(error); rememberFailure(error); }
    try { await engine?.stop(); } catch (error) { failures.push(error); }
    for (const lease of progressLeases.values()) {
      try { await lease.release(); } catch (error) { failures.push(error); }
    }
    progressLeases.clear();
    try { record('resources-stopped', { enginePresent: !!engine }); } catch (error) { failures.push(error); rememberFailure(error); }
    resolveResourceStopped();
    if (failures.length) throw new AggregateError(failures, 'owned native fixture resource cleanup');
  });
  return resourceStop;
}
process.on('message', message => {
  if (message?.command === 'block-main-thread') {
    if (scenario !== 'progress-db-only' && scenario !== 'progress-wal-only') {
      rememberFailure(new Error('unexpected synchronous-work fixture command'));
      return;
    }
    try {
      // Publish a complete immutable snapshot before blocking. The parent
      // never polls the concurrently rewritten aggregate events file.
      const blocked = { event: 'main-thread-blocking', pid: process.pid,
        monotonicNs: process.hrtime.bigint().toString(), durationMs: 2500 };
      const bytes = Buffer.from(JSON.stringify(blocked));
      if (bytes.length > 65536) throw new Error('native block publication bound exceeded');
      const pending = `${evidencePath}.block.pending`;
      fs.writeFileSync(pending, bytes, { flag: 'wx' });
      fs.renameSync(pending, `${evidencePath}.block.json`);
      record('main-thread-blocking', blocked);
      // Real synchronous silence: no IPC callback or heartbeat can run until
      // this returns. Only parent SQLite commits advance the selected files.
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2500);
      record('main-thread-resumed');
    } catch (error) { rememberFailure(error); }
  }
  if (message?.command === 'progress-acquire') {
    let observerStartedNs;
    void (async () => {
      const route = captureMcpIndexRoute(message.root);
      if (!route || !controls) throw new Error('fixture progress route/controls absent');
      const controller = new AbortController(); progressRequests.set(message.name, controller);
      record('progress-registering', { name: message.name, route });
      observerStartedNs = process.hrtime.bigint().toString();
      const lease = await controls.resourceObserver.acquireIndexProgress(route, controller.signal);
      progressRequests.delete(message.name);
      progressLeases.set(message.name, lease);
      record('progress-acquired', { name: message.name, route, watchdogs: watchdogSnapshot() });
    })().catch(error => {
      progressRequests.delete(message.name);
      try { record('progress-rejected', { name: message.name, observerStartedNs, message: String(error).slice(0, 512), watchdogs: watchdogSnapshot() }); }
      catch (observationError) { rememberFailure(observationError); }
    });
  }
  if (message?.command === 'progress-abort') {
    progressRequests.get(message.name)?.abort();
    try { record('progress-aborted', { name: message.name }); } catch (error) { rememberFailure(error); }
  }
  if (message?.command === 'progress-release') {
    void (async () => {
      const lease = progressLeases.get(message.name);
      if (!lease) throw new Error('fixture progress lease absent');
      await lease.release(); await lease.release(); // release is idempotent
      progressLeases.delete(message.name);
      record('progress-released', { name: message.name, watchdogs: watchdogSnapshot() });
    })().catch(rememberFailure);
  }
  if (message?.command === 'release-preparation') resolvePreparation();
  if (message?.command === 'reject-injected-drain') resolveInjectedDrain();
  if (message?.command === 'inspect') record('inspection', {
    execution: engine?.getQueryExecutionContext(),
    history: session?.getExploreSessionState().snapshot().map(project => ({
      projectRoot: project.projectRoot, callCount: project.callCount,
    })),
    stdinDataListeners: process.stdin.listenerCount('data'),
    watchdogs: watchdogSnapshot(),
  });
  if (message?.command !== 'begin') return;
  void runLocalHandshakeProxy({
    hint: root,
    prepare: async (signal, suppliedControls) => {
      controls = suppliedControls;
      record('preparation-entered');
      signal.addEventListener('abort', () => {
        try { record('preparation-aborted'); } catch (error) { rememberFailure(error); }
      }, { once: true });
      await preparationGate;
      if (scenario === 'refusal') throw refusal();
      if (scenario === 'proxy-refusal') return { kind: 'proxy', deps: {
        root,
        getDaemonSocket: async () => { record('proxy-acquisition'); throw refusal(); },
        makeEngine: () => { record('proxy-fallback-entered'); throw new Error('forbidden fallback after refusal'); },
      } };
      if (scenario === 'proxy-drain-failure') {
        let acquisitions = 0;
        return { kind: 'proxy', deps: {
          root,
          getDaemonSocket: async () => {
            record('proxy-acquisition', { ordinal: ++acquisitions });
            if (acquisitions > 1) { record('proxy-unavailable'); return null; }
            return new Promise((resolve, reject) => {
              const socket = net.createConnection(JSON.parse(process.env.PARSED_READINESS_PEER));
              socket.on('error', reject);
              socket.once('connect', () => { socket.pause(); resolve(socket); });
            });
          },
          makeEngine: () => {
            record('proxy-fallback-entered');
            const queryPolicy = resolveQueryPolicy({ busyTimeoutMs: 'unlimited' }, 'direct', 4);
            const queryAdmissionPolicy = resolveQueryPolicy({ busyTimeoutMs: 'unlimited' }, 'shared-daemon', 4);
            const actual = new MCPEngine({ queryPolicy, queryAdmissionPolicy, executionMode: 'direct', queryPool: true,
              watch: false, resourceObserver: controls.resourceObserver, onInitialDefaultOutcome: controls.onInitialDefaultOutcome });
            engine = actual;
            const handler = actual.getToolHandler();
            const execute = handler.execute.bind(handler);
            handler.execute = (...args) => {
              fallbackHistory = args[2];
              try { record('tool-dispatched', { tool: args[0], requestId: args[3]?.requestId }); }
              catch (error) { rememberFailure(error); }
              return execute(...args); // unchanged real indexed result/work
            };
            // Fixture-only fault at THIS real engine's drain boundary. Its
            // SQLite/ready query worker/progress must already exist. Reject
            // without disposal, modelling an actual rejected cleanup contract.
            actual.drain = async () => {
              failedDrainEngine = actual;
              record('injected-drain-entered', { diagnostics: actual.getQueryDiagnostics(),
                route: captureMcpIndexRoute(root), watchdogs: watchdogSnapshot() });
              await injectedDrainGate;
              record('injected-drain-rejected', { diagnostics: actual.getQueryDiagnostics(), watchdogs: watchdogSnapshot() });
              throw new Error('owned injected native-engine drain failure');
            };
            return actual;
          },
          stop: async () => { record('proxy-caller-stop'); await engine?.stop(); },
        } };
      }
      if (scenario === 'proxy-healthy') return { kind: 'proxy', deps: {
        root,
        getDaemonSocket: () => new Promise((resolve, reject) => {
          record('proxy-acquisition');
          const endpoint = JSON.parse(process.env.PARSED_READINESS_PEER);
          const socket = net.createConnection(endpoint);
          // This owned peer seam supplies an already selected usable socket;
          // it is not a substitute for main29's live-service policy/hello gate.
          socket.on('error', reject);
          socket.once('connect', () => { socket.pause(); resolve(socket); });
        }),
        makeEngine: () => { record('proxy-fallback-entered'); throw new Error('forbidden healthy-peer fallback'); },
      } };
      return {
        kind: 'direct', route: null, stop,
        makeSession: async (transport, factorySignal) => {
          record('factory-entered', { aborted: factorySignal.aborted });
          if (factorySignal.aborted) { record('factory-aborted'); throw new Error('owned factory cancelled'); }
          if (scenario === 'factory-refusal') throw refusal();
          // Fixed fixture input, not a second public environment-policy capture.
          const queryPolicy = resolveQueryPolicy({ busyTimeoutMs: 'unlimited' }, 'direct', 4);
          const queryAdmissionPolicy = resolveQueryPolicy({ busyTimeoutMs: 'unlimited' }, 'shared-daemon', 4);
          engine = new MCPEngine({ queryPolicy, queryAdmissionPolicy,
            resourceObserver: controls.resourceObserver,
            onInitialDefaultOutcome: outcome => {
              try { record('initial-default-outcome', { kind: outcome.kind,
                route: outcome.kind === 'ready' ? outcome.route : undefined,
                refusal: outcome.kind === 'refused' ? { code: outcome.refusal.code,
                  reason: outcome.refusal.reason, projectRoot: outcome.refusal.projectRoot } : undefined }); }
              catch (error) { rememberFailure(error); }
              controls.onInitialDefaultOutcome(outcome);
            },
            executionMode: 'direct', queryPool: true, watch: false });
          record('engine-created', { execution: engine.getQueryExecutionContext() });
          // Observe the real public handler boundary without replacing its
          // work/result. Explicit-target rows make an incorrectly revived
          // cancelled action visible before any awaited permission completes.
          const handler = engine.getToolHandler();
          const execute = handler.execute.bind(handler);
          handler.execute = (...args) => {
            try { record('tool-dispatched', { tool: args[0], requestId: args[3]?.requestId,
              projectPath: args[1]?.projectPath }); }
            catch (error) { rememberFailure(error); }
            // Preserve the exact arguments, this, returned promise/value and
            // thrown error even if the observation itself failed.
            return execute(...args);
          };
          if (scenario === 'held-factory' || scenario === 'signal-factory') {
            await engine.ensureInitialized(root); // acquire real own-index SQLite resources first
            record('engine-initialized', { diagnostics: engine.getQueryDiagnostics() });
            // Acquired REAL engine: only descriptor.stop can settle this
            // factory. EOF/signal alone, or a parent release, cannot hide a
            // stop-after-factory deadlock. A late real session is returned so
            // the outer shell must dispose it without starting it.
            record('factory-awaiting-resource-stop');
            await resourceStopped;
          }
          session = new MCPSession(transport, engine, {
            explicitProjectPath: scenario.startsWith('hint-') && scenario !== 'hint-roots' ? root : null,
          });
          record('session-created');
          transport.onClose(() => record('session-transport-closed'));
          return session;
        },
      };
    },
  }).catch(error => { record('outer-error', { message: String(error) }); process.exitCode = 1; });
});
process.on('exit', code => {
  // Synchronous evidence only: outer proxy owns process exit and its backstop.
  record('native-exit', { code, stdinDataListeners: process.stdin.listenerCount('data'),
    ...(scenario === 'proxy-drain-failure' ? {
      engineIdentityRetained: !!engine && engine === failedDrainEngine,
      history: fallbackHistory?.snapshot().map(project => ({ projectRoot: project.projectRoot, callCount: project.callCount })),
      diagnostics: engine?.getQueryDiagnostics(), watchdogs: watchdogSnapshot(),
    } : {}) });
});
process.stdin.once('end', () => {
  try { record('stdin-ended'); } catch (error) { rememberFailure(error); }
});
record('online', { scenario, root, fixture: path.basename(__filename) });
