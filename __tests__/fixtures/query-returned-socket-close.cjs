// Scoped dependency seam. The exact built parser/policy/owner validator obtains
// a real socket from the actual owned daemon. Close that SAME native socket
// before returning it; never fabricate any hello, handler or query result.
const fs = require('node:fs');
const threads = require('node:worker_threads');
if (threads.isMainThread && process.env.QUERY_UNPROVED_SOCKET_OBSERVATIONS) {
  let firstError = null, failureCount = 0, recordedBytes = 0, recordedEvents = 0;
  const rawRecord = value => {
    const line = JSON.stringify({ pid: process.pid, monotonicNs: process.hrtime.bigint().toString(), ...value }) + '\n';
    const bytes = Buffer.byteLength(line);
    if (bytes > 65536 || recordedBytes + bytes > 8 * 1024 * 1024 || recordedEvents >= 10000) throw new Error('Owned socket observation cap');
    fs.appendFileSync(process.env.QUERY_UNPROVED_SOCKET_OBSERVATIONS, line);
    recordedBytes += bytes; recordedEvents++;
  };
  const fixtureFailure = (phase, error) => {
    firstError ??= String(error).slice(0, 2048);
    if (failureCount++ >= 16) return;
    const value = { event: 'fixture-failure', phase, error: String(error).slice(0, 2048), firstError };
    try { rawRecord(value); } catch { /* the separate bounded stderr marker remains acceptance-critical */ }
    try { process.stderr.write('[owned-transport-fixture-failure] ' + JSON.stringify(value).slice(0, 8192) + '\n'); } catch { /* retain firstError for EOF/native cleanup */ }
  };
  const record = value => {
    try { rawRecord(value); }
    catch (error) { fixtureFailure('observation-write', error); throw error; }
  };
  // Observe only after an existing actual product stdin listener consumes the
  // same bytes. This never starts stdin early or changes the delivered input.
  const originalOn = process.stdin.on, seen = new Set();
  let input = Buffer.alloc(0), seenBytes = 0, observedListener = false;
  const observe = chunk => {
    try {
      const bytes = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      seenBytes += bytes.length;
      if (seenBytes > 1024 * 1024) throw new Error('Owned observed stdin cumulative byte cap');
      input = Buffer.concat([input, bytes]);
      let end;
      while ((end = input.indexOf(10)) >= 0) {
        const line = input.subarray(0, end); input = input.subarray(end + 1);
        if (line.length > 65536) throw new Error('Owned observed stdin line cap');
        const value = JSON.parse(line.toString('utf8'));
        if (value.method === 'tools/call' && value.params?.name === 'codegraph_explore' && !seen.has(value.id)) {
          seen.add(value.id); record({ event: 'actual-actionable-input-consumed', id: value.id, query: value.params.arguments.query });
        }
      }
      if (input.length > 65536) throw new Error('Owned observed stdin partial line cap');
    } catch (error) { fixtureFailure('actual-input-observer', error); }
  };
  process.stdin.on = function(event, listener) {
    if (event !== 'data' || observedListener) return originalOn.call(this, event, listener);
    observedListener = true;
    return originalOn.call(this, event, function(chunk) {
      try { return listener.call(this, chunk); } finally { observe(chunk); }
    });
  };
  process.stdin.once('end', () => {
    try { record({ event: 'actual-input-EOF', seenBytes, partialBytes: input.length, firstError }); }
    catch (error) { fixtureFailure('actual-input-EOF', error); }
  });
  const proxy = require('../../dist/mcp/proxy');
  // Factory observation is independent of the root-specific lifetime seam:
  // all three negative rendezvous cases must exclude a cold local engine too.
  const originalRun = proxy.runLocalHandshakeProxy;
  proxy.runLocalHandshakeProxy = function(deps) {
    return originalRun.call(this, { ...deps, makeEngine: () => {
      record({ event: 'fallback-factory-invoked' }); return deps.makeEngine();
    } });
  };
  record({ event: 'fallback-factory-observer-installed' });
  const root = process.env.QUERY_UNPROVED_RETURNED_SOCKET_ROOT;
  if (!root) return;
  const owners = require('../../dist/mcp/query-service-owner');
  const originalConnect = proxy.connectWithHello;
  proxy.connectWithHello = async function(socketPath, version, policy, projectRoot) {
    const socket = await originalConnect.apply(this, arguments);
    if (!socket || projectRoot !== root) return socket;
    // Take ownership before any record or release await can fail. Native close
    // observation never performs throwing I/O inside an EventEmitter callback.
    let nativeClose = null, returned = false;
    const closed = new Promise(resolve => socket.once('close', hadError => {
      nativeClose = { hadError, destroyed: socket.destroyed }; resolve();
    }));
    socket.on('error', error => fixtureFailure('owned-returned-socket-error', error));
    const closeOwned = async () => {
      let timer;
      try {
        socket.destroy();
        await Promise.race([closed, new Promise((_, reject) => {
          timer = setTimeout(() => reject(new Error('Owned returned-socket close deadline')), 5000);
        })]);
      } finally { clearTimeout(timer); }
    };
    try {
      record({ event: 'actual-verified-socket', socketPath, version, policy, owners: owners.readQueryServiceOwners(root) });
      const releaseDeadline = Date.now() + 10000;
      while (!fs.existsSync(process.env.QUERY_UNPROVED_RELEASE)) {
        if (Date.now() >= releaseDeadline) throw new Error('Owned returned-socket release deadline');
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      record({ event: 'actual-socket-close-release-observed' });
      await closeOwned();
      record({ event: 'actual-socket-native-close', ...nativeClose });
      if (firstError) throw new Error(firstError);
      record({ event: 'returned-destroyed-socket', destroyed: socket.destroyed });
      returned = true; return socket;
    } catch (error) {
      fixtureFailure('returned-socket-before-return', error); throw error;
    } finally {
      if (!returned) {
        try { await closeOwned(); } catch (error) { fixtureFailure('returned-socket-failure-native-close', error); }
        try { record({ event: 'owned-socket-failure-cleanup', nativeClose, destroyed: socket.destroyed, firstError }); }
        catch (error) { fixtureFailure('returned-socket-failure-cleanup-record', error); }
      }
    }
  };
}
