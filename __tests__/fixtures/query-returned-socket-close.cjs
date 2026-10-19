// Scoped dependency seam. The exact built parser/policy/owner validator obtains
// a real socket from the actual owned daemon. Close that SAME native socket
// before returning it; never fabricate any hello, handler or query result.
const fs = require('node:fs');
const net = require('node:net');
const crypto = require('node:crypto');
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
  const nativeRecord = value => {
    try { record(value); } catch (error) { fixtureFailure('native-socket-observation', error); }
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
  const requestedRoot = process.env.QUERY_UNPROVED_RETURNED_SOCKET_ROOT;
  const owners = require('../../dist/mcp/query-service-owner');
  const socketIdentities = new WeakMap();
  let phase = null, selectedRoot = null, candidates = null, sockets = 0, mutations = 0;
  const originalConnection = net.createConnection;
  net.createConnection = function(...args) {
    const endpoint = typeof args[0] === 'string' ? args[0] : args[0]?.path;
    if (!candidates?.has(endpoint)) return originalConnection.apply(this, args);
    const stack = (new Error().stack ?? '').replace(/\\/g, '/').slice(0, 8192);
    const purpose = phase === 'prepare' && stack.includes('admitQueryProject') ? 'prepare-admission'
      : phase === 'acquisition' && stack.includes('admitQueryProject') ? 'attempt-admission'
        : phase === 'acquisition' && stack.includes('connectHeldService') ? 'attachment' : null;
    if (!purpose || !stack.includes('connectObservedService')) {
      const error = new Error('Unclassified actual returned-close socket'); fixtureFailure('socket-purpose', error); throw error;
    }
    const socket = originalConnection.apply(this, args), identity = { socketId: ++sockets, purpose, endpoint };
    socketIdentities.set(socket, identity);
    let bytes = Buffer.alloc(0), sawLf = false;
    const originalPush = socket.push;
    socket.push = function(chunk, ...rest) {
      if (Buffer.isBuffer(chunk) && !sawLf) {
        const nl = chunk.indexOf(10), prefix = nl < 0 ? chunk : chunk.subarray(0, nl + 1);
        if (bytes.length + prefix.length > 16384) fixtureFailure('raw-hello-cap', new Error('Owned raw greeting cap'));
        else bytes = Buffer.concat([bytes, prefix]);
        sawLf = nl >= 0;
        nativeRecord({ event: 'actual-observed-socket-hello', ...identity, bytes: prefix.length, base64: prefix.toString('base64'),
          totalBytes: bytes.length, sawLf, sha256: crypto.createHash('sha256').update(bytes).digest('hex') });
      }
      if (chunk === null) nativeRecord({ event: 'actual-observed-socket-EOF', ...identity, rawBytes: bytes.length, sawLf });
      return originalPush.call(this, chunk, ...rest);
    };
    socket.on('connect', () => nativeRecord({ event: 'actual-observed-socket-connect', ...identity }));
    socket.on('close', hadError => nativeRecord({ event: 'actual-observed-socket-close', ...identity,
      hadError, destroyed: socket.destroyed, rawBytes: bytes.length, sawLf }));
    socket.on('error', error => fixtureFailure('actual-observed-socket-error', error));
    record({ event: 'actual-observed-socket-installed', ...identity, stack });
    return socket;
  };
  // Adapt the actual prepared API: a refusal during preparation publishes no
  // factory at all. Successful preparation exposes the real makeEngine hook.
  const originalRun = proxy.runLocalHandshakeProxy;
  function observedDeps(deps, branch) {
    if (deps.root !== selectedRoot || typeof deps.makeEngine !== 'function' || typeof deps.getDaemonSocket !== 'function') throw new Error('Unexpected actual unproved proxy dependencies');
    record({ event: 'fallback-prepared-deps-observer-installed', root: deps.root, branch });
    return { ...deps, makeEngine: function(...args) {
      record({ event: 'fallback-factory-invoked' }); return deps.makeEngine.apply(deps, args);
    }, getDaemonSocket: async function(...args) {
      phase = 'acquisition';
      let socket;
      try { socket = await deps.getDaemonSocket.apply(deps, args); }
      finally { phase = null; }
      if (!requestedRoot || !socket) return socket;
      return closeReturnedSocket(socket);
    } };
  }
  proxy.runLocalHandshakeProxy = function(input) {
    selectedRoot = fs.realpathSync(input.hint ?? input.root);
    if (requestedRoot && requestedRoot !== selectedRoot) throw new Error('Returned-close root differs from actual preparation hint');
    candidates = new Set(require('../../dist/mcp/daemon-paths').getDaemonSocketCandidates(selectedRoot));
    record({ event: 'fallback-factory-observer-installed', root: selectedRoot,
      branch: typeof input.prepare === 'function' ? 'prepare-entry' : 'legacy-deps' });
    if (typeof input.prepare !== 'function') return originalRun.call(this, observedDeps(input, 'legacy-deps'));
    return originalRun.call(this, { ...input, prepare: async function(...args) {
      phase = 'prepare';
      let prepared;
      try { prepared = await input.prepare.apply(input, args); }
      catch (error) { record({ event: 'actual-preparation-rejected', code: error?.code, reason: error?.reason, projectRoot: error?.projectRoot,
        message: String(error).slice(0, 2048) }); throw error; }
      finally { phase = null; }
      if (prepared.kind !== 'proxy') throw new Error('Unproved requester did not prepare actual proxy dependencies');
      return { ...prepared, deps: observedDeps(prepared.deps, 'prepared-deps') };
    } });
  };
  async function closeReturnedSocket(socket) {
    const identity = socketIdentities.get(socket);
    if (!identity || identity.purpose !== 'attachment' || ++mutations !== 1) {
      const error = new Error('Returned-close seam did not select exactly one actual attachment');
      fixtureFailure('returned-socket-identity', error); socket.destroy(); throw error;
    }
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
      record({ event: 'actual-verified-socket', ...identity, socketPath: identity.endpoint,
        owners: owners.readQueryServiceOwners(selectedRoot) });
      const releaseDeadline = Date.now() + 10000;
      while (!fs.existsSync(process.env.QUERY_UNPROVED_RELEASE)) {
        if (Date.now() >= releaseDeadline) throw new Error('Owned returned-socket release deadline');
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      record({ event: 'actual-socket-close-release-observed', ...identity, consumed: [...seen] });
      if (!seen.has(100) || !seen.has(101)) throw new Error('Attachment close released before actual buffered inputs');
      await closeOwned();
      record({ event: 'actual-socket-native-close', ...identity, ...nativeClose });
      if (firstError) throw new Error(firstError);
      record({ event: 'returned-destroyed-socket', ...identity, destroyed: socket.destroyed });
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
  }
  process.once('exit', () => record({ event: 'returned-close-observer-final', sockets, mutations, firstError }));
}
