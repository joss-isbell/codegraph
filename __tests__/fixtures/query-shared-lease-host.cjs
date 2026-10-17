// Owned native fixture. All graph/query/session calls delegate to the exact
// built product. The main-thread barrier sequences an owned peer restart only.
const fs = require('node:fs');
const path = require('node:path');
const net = require('node:net');
const readline = require('node:readline');
const { MCPEngine } = require('../../dist/mcp/engine');
const { MCPSession } = require('../../dist/mcp/session');
const { SocketTransport } = require('../../dist/mcp/transport');
const { resolveQueryPolicy } = require('../../dist/mcp/query-policy');
const CodeGraph = require('../../dist').default;
const { getDatabasePath } = require('../../dist');
const [role, workspace, missingRoot, releasePath] = process.argv.slice(2);
const { getDaemonSocketCandidates } = require('../../dist/mcp/daemon-paths');
const peerPaths = new Set(role === 'shared' ? getDaemonSocketCandidates(path.join(workspace, 'beta')) : []);
const connections = [];
const originalConnect = net.createConnection;
net.createConnection = function(...args) {
  const socket = originalConnect.apply(this, args), endpoint = typeof args[0] === 'string' ? args[0] : args[0]?.path;
  if (peerPaths.has(endpoint)) {
    const connection = { id: connections.length + 1, endpoint, closed: false };
    connections.push(connection); socket.once('close', () => { connection.closed = true; });
  }
  return socket;
};
const send = value => process.stdout.write(JSON.stringify({ pid: process.pid, ...value }) + '\n');
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
const until = async (test, ms = 10000) => {
  const end = Date.now() + ms;
  while (!test()) { if (Date.now() >= end) throw new Error('Owned host state deadline'); await sleep(10); }
};
const policy = (maximum, mode) => resolveQueryPolicy({ poolSize: String(maximum), busyTimeoutMs: 'unlimited' }, mode, 8);
const opened = [];
const observe = graph => {
  if (opened.some(entry => entry.graph === graph)) return graph;
  const entry = { graph, root: graph.getProjectRoot(), closeCalls: 0 };
  opened.push(entry);
  const close = graph.close.bind(graph);
  graph.close = (...args) => { entry.closeCalls++; return close(...args); };
  return graph;
};
const originalOpen = CodeGraph.open;
CodeGraph.open = async function(...args) { return observe(await originalOpen.apply(this, args)); };
const originalOpenSync = CodeGraph.openSync;
CodeGraph.openSync = function(...args) { return observe(originalOpenSync.apply(this, args)); };
const graphs = () => opened.map(({ root, closeCalls }) => ({ root, closeCalls }));

// This is a real native TCP client, not an injected ToolHandler or query mock.
function wire(socket, expectsHello = false) {
  const replies = [], pending = new Map();
  let buffer = Buffer.alloc(0), seenBytes = 0, firstError = null, closed = false, hello;
  const fail = error => {
    firstError ??= String(error);
    for (const entry of pending.values()) { clearTimeout(entry.timer); entry.reject(new Error(firstError)); }
    pending.clear();
  };
  socket.on('error', fail);
  socket.on('data', chunk => {
    seenBytes += chunk.length;
    if (seenBytes > 8 * 1024 * 1024) { fail('Owned TCP cumulative capture overflow'); return; }
    buffer = Buffer.concat([buffer, chunk]);
    let end;
    while ((end = buffer.indexOf(10)) >= 0) {
      const line = buffer.subarray(0, end); buffer = buffer.subarray(end + 1);
      let value; try { value = JSON.parse(line.toString('utf8')); } catch (error) { fail(error); continue; }
      if (expectsHello && !hello) { hello = value; continue; }
      replies.push(value);
      const entry = pending.get(value.id);
      if (entry) { clearTimeout(entry.timer); pending.delete(value.id); entry.resolve(value); }
    }
  });
  socket.on('close', () => { closed = true; if (pending.size) fail('Owned TCP closed with pending requests'); });
  return { socket, replies, facts: () => ({ seenBytes, firstError, closed, pending: pending.size, hello, replies }),
    assertHealthy: () => { if (firstError || pending.size) throw new Error(firstError ?? 'Owned TCP has unsettled requests'); },
    close: async () => { socket.destroy(); await until(() => closed, 5000); },
    call: (id, method, params) => new Promise((resolve, reject) => {
      if (firstError) { reject(new Error(firstError)); return; }
      const timer = setTimeout(() => { pending.delete(id); reject(new Error('Owned TCP request deadline')); }, 35000);
      pending.set(id, { resolve, reject, timer });
      socket.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
    }), hello: () => hello };
}

const engines = new Map(), endpoints = new Map();
const cleanupErrors = [];
async function endpoint(name, engine, root) {
  const value = { engine, client: null, session: null, server: null, accepted: new Set(), stopped: false, errors: [] };
  // Ownership is recorded before listen/connect/initialize can reject.
  engines.set(name, engine); endpoints.set(name, value);
  const server = net.createServer(socket => {
    value.accepted.add(socket); socket.once('close', () => value.accepted.delete(socket));
    value.session = new MCPSession(new SocketTransport(socket), engine, { explicitProjectPath: root }); value.session.start();
  });
  value.server = server;
  server.on('error', error => { value.errors.push(String(error)); });
  await new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const client = wire(net.createConnection(server.address().port, '127.0.0.1'));
  value.client = client;
  await until(() => !!value.session);
  await client.call(1, 'initialize', { clientInfo: { name: 'owned-same-process-' + name, version: '1' }, capabilities: {} });
  return value;
}
const snapshot = () => ({ modulePath: require.resolve('../../dist/mcp/engine'), graphs: graphs(), connections, cleanupErrors,
  daemonProbe: daemonProbe?.facts() ?? null, engines: [...engines].map(([name, engine]) => ({ name, diagnostics: engine.getQueryDiagnostics(),
    loaded: engine.hasDefaultCodeGraph(), history: endpoints.get(name)?.session?.getExploreSessionState().snapshot() ?? [],
    wire: endpoints.get(name)?.client?.facts() ?? null, resourceErrors: endpoints.get(name)?.errors ?? [] })) });
async function cleanupStep(phase, action) {
  try { await action(); } catch (error) { cleanupErrors.push({ phase, error: error.stack ?? String(error) }); }
}
async function stopEngine(name) {
  const value = endpoints.get(name);
  if (value?.stopped) return;
  if (value) {
    value.stopped = true;
    await cleanupStep(name + '-wire-before-close', () => value.client?.assertHealthy());
    await cleanupStep(name + '-client-close', () => value.client?.close());
    await cleanupStep(name + '-session-close', () => value.session?.stop());
    for (const socket of value.accepted) await cleanupStep(name + '-accepted-close', () => socket.destroy());
    await cleanupStep(name + '-server-close', () => value.server?.listening ? new Promise((resolve, reject) => value.server.close(error => error ? reject(error) : resolve())) : undefined);
  }
  await cleanupStep(name + '-engine-stop', () => engines.get(name)?.stop());
  await cleanupStep(name + '-wire-after-close', () => value?.client?.assertHealthy());
  if (value?.errors.length) cleanupErrors.push({ phase: name + '-server-errors', error: value.errors.join('\n') });
}
let daemon, daemonProbe, stopping = false;
async function shutdown() {
  if (stopping) return;
  stopping = true;
  if (daemon) {
    await cleanupStep('daemon-probe-before-close', () => daemonProbe?.assertHealthy());
    await cleanupStep('daemon-probe-close', () => daemonProbe?.close());
    await cleanupStep('daemon-probe-after-close', () => daemonProbe?.assertHealthy());
    send({ event: 'owned-close', snapshot: snapshot() });
    // Daemon.stop owns native process finalization, including POSIX exit. Do
    // not let that success exit conceal a previously retained fixture error.
    if (cleanupErrors.length) { process.stderr.write(JSON.stringify(cleanupErrors) + '\n'); process.exitCode = 1; }
    await daemon.stop('fixture-owned-stdin-EOF');
  } else {
    send({ event: 'owned-close', snapshot: snapshot() });
    for (const name of engines.keys()) await cleanupStep(name + '-all-resources', () => stopEngine(name));
    if (cleanupErrors.length) process.exitCode = 1;
    send({ event: 'owned-closed', snapshot: snapshot() });
  }
}
async function main() {
  if (role === 'peer') {
    const { Daemon, tryAcquireDaemonLock } = require('../../dist/mcp/daemon');
    const lock = tryAcquireDaemonLock(workspace);
    if (lock.kind !== 'acquired') throw new Error('Owned peer could not acquire its disposable daemon lock: ' + JSON.stringify(lock));
    daemon = new Daemon(workspace, { idleTimeoutMs: 0, maxIdleMs: 0 });
    const started = await daemon.start();
    daemonProbe = wire(net.createConnection(started.socketPath), true);
    await until(() => !!daemonProbe.hello());
    daemonProbe.socket.write(JSON.stringify({ codegraph_client: 1, pid: process.pid, hostPid: process.ppid }) + '\n');
    const status = await daemonProbe.call(2, 'tools/call', { name: 'codegraph_status', arguments: {} });
    send({ event: 'ready', role, hello: daemonProbe.hello(), status, locks: ['daemon.pid', 'writer.pid'].map(name => fs.readFileSync(path.join(workspace, '.codegraph', name), 'utf8')) });
  } else if (role === 'shared') {
    for (const [name, maximum] of [['A', 1], ['B', 2]]) {
      const engine = new MCPEngine({ watch: true, queryPool: true, executionMode: 'direct', queryPolicy: policy(maximum, 'direct'), queryAdmissionPolicy: policy(maximum, 'shared-daemon') });
      engines.set(name, engine);
      await engine.ensureInitialized(workspace);
      await endpoint(name, engine, workspace);
    }
    send({ event: 'ready', role, snapshot: snapshot() });
  } else if (role === 'missing') {
    const engine = new MCPEngine({ watch: false, queryPool: true, executionMode: 'direct', queryPolicy: policy(1, 'direct'), queryAdmissionPolicy: policy(1, 'shared-daemon') });
    engines.set('C', engine);
    await engine.ensureInitialized(missingRoot);
    const target = await endpoint('C', engine, missingRoot);
    const status = await target.client.call(2, 'tools/call', { name: 'codegraph_status', arguments: {} });
    send({ event: 'ready', role, status, snapshot: snapshot() });
  } else throw new Error('Unknown owned fixture role');

  let queued = Promise.resolve(), inputBytes = 0;
  const input = readline.createInterface({ input: process.stdin });
  input.on('line', line => {
    inputBytes += Buffer.byteLength(line);
    queued = queued.then(async () => {
      if (inputBytes > 65536 || Buffer.byteLength(line) > 16384) throw new Error('Owned command input overflow');
      const command = JSON.parse(line);
      if (command.kind === 'barrier') {
        const oldLinks = connections.filter(value => !value.closed);
        if (!oldLinks.length) throw new Error('No actual retained old beta link before barrier');
        send({ event: 'barrier-held', token: command.token, snapshot: snapshot() });
        const gate = new Int32Array(new SharedArrayBuffer(4)), end = Date.now() + 30000;
        while (!fs.existsSync(releasePath)) {
          if (Date.now() >= end) throw new Error('Owned restart barrier deadline');
          Atomics.wait(gate, 0, 0, 10);
        }
        // Let actual old peer socket-close notifications run before B's access.
        await until(() => oldLinks.every(value => value.closed));
        send({ event: 'barrier-released', token: command.token, snapshot: snapshot() });
      } else if (command.kind === 'remove-loaded-index') {
        const value = endpoints.get('C'), captured = opened.filter(entry => entry.root === missingRoot);
        if (!captured.length || !value.engine.hasDefaultCodeGraph()) throw new Error('Missing-index fixture did not capture actual loaded C');
        // Default watch:false still owns catch-up separately from its reader.
        // Release every captured exact-root SQLite handle, not just the writer.
        const fixtureClosedHandles = captured.map((entry, ordinal) => {
          const closeCallsBefore = entry.closeCalls;
          entry.graph.close();
          return { ordinal, root: entry.root, closeCallsBefore, closeCallsAfter: entry.closeCalls };
        });
        send({ event: 'loaded-index-handles-closed', token: command.token, fixtureClosedHandles,
          stillDefaultLoaded: value.engine.hasDefaultCodeGraph(), snapshot: snapshot() });
        fs.unlinkSync(getDatabasePath(missingRoot));
        send({ event: 'removed-loaded-index', token: command.token, database: getDatabasePath(missingRoot), fixtureClosedHandles,
          stillDefaultLoaded: value.engine.hasDefaultCodeGraph(), snapshot: snapshot() });
      } else if (command.kind === 'stop-engine') {
        await stopEngine(command.engine);
        send({ event: 'engine-stopped', token: command.token, snapshot: snapshot() });
      } else if (command.kind === 'tool') {
        const value = endpoints.get(command.engine);
        const reply = await value.client.call(command.id, 'tools/call', { name: command.name ?? 'codegraph_explore', arguments: command.args });
        send({ event: 'reply', token: command.token, reply, snapshot: snapshot() });
      } else throw new Error('Unknown owned command');
    }).catch(async error => { send({ event: 'fixture-failure', error: error.stack ?? String(error), snapshot: snapshot() }); process.exitCode = 1; await shutdown(); input.close(); });
  });
  input.on('close', () => { void queued.then(shutdown).catch(error => { process.stderr.write(String(error)); process.exitCode = 1; }); });
}
main().catch(async error => { process.stderr.write((error.stack ?? String(error)) + '\n'); process.exitCode = 1; await shutdown(); });
