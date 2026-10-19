// Scoped native framing decoration/observation. All authority and query results
// come from the actual owned built daemon; only ignored padding and an opaque
// fixture notification are synthetic. No reader is attached before the product.
const fs = require('node:fs');
const net = require('node:net');
const crypto = require('node:crypto');
const threads = require('node:worker_threads');
if (!threads.isMainThread || !process.env.QUERY_HELLO_FRAMING_ROOT) return;
const root = fs.realpathSync(process.env.QUERY_HELLO_FRAMING_ROOT);
const role = process.env.QUERY_HELLO_FRAMING_ROLE;
const file = process.env.QUERY_HELLO_FRAMING_TRACE;
const control = process.env.QUERY_HELLO_FRAMING_CONTROL;
const acknowledgement = process.env.QUERY_HELLO_FRAMING_ACK;
const closeAcknowledgement = process.env.QUERY_HELLO_FRAMING_CLOSE_ACK;
const tailSplitMode = process.env.QUERY_HELLO_FRAMING_MODE === 'utf8-tail-split';
const consumerAcknowledgement = process.env.QUERY_HELLO_FRAMING_CONSUMER_ACK;
const consumerRelease = process.env.QUERY_HELLO_FRAMING_CONSUMER_RELEASE;
const now = () => process.hrtime.bigint().toString();
const hash = bytes => crypto.createHash('sha256').update(bytes).digest('hex');
let firstError = null, errors = 0, retainedBytes = 0, events = 0;
function failure(phase, error) {
  firstError ??= String(error).slice(0, 2048);
  if (errors++ >= 16) return;
  const value = { event: 'fixture-failure', phase, error: String(error).slice(0, 2048), firstError };
  try { append(value); } catch { /* bounded independent stderr marker */ }
  try { process.stderr.write('[owned-transport-fixture-failure] ' + JSON.stringify(value).slice(0, 8192) + '\n'); } catch { /* sticky firstError retained */ }
}
function append(value) {
  const line = JSON.stringify({ pid: process.pid, role, monotonicNs: now(), ...value }) + '\n';
  const count = Buffer.byteLength(line);
  if (count > 65536 || retainedBytes + count > 8 * 1024 * 1024 || events >= 10000) throw new Error('Framing observation byte/event cap');
  fs.appendFileSync(file, line); retainedBytes += count; events++;
}
function record(value) { try { append(value); } catch (error) { failure('trace-write', error); } }
function read(filePath) {
  let fd, primary, closeError, result;
  try {
    fd = fs.openSync(filePath, 'r'); const stat = fs.fstatSync(fd);
    if (!stat.isFile() || stat.size > 65536) throw new Error('Framing control record cap/type');
    const bytes = Buffer.alloc(stat.size), count = fs.readSync(fd, bytes, 0, bytes.length, 0);
    if (count !== bytes.length) throw new Error('Framing control short read');
    result = JSON.parse(bytes.toString('utf8'));
  } catch (error) { if (error.code === 'ENOENT' && fd === undefined) return null; primary = error; }
  finally {
    if (fd !== undefined) try { fs.closeSync(fd); }
    catch (error) {
      closeError = error;
      if (primary) failure('control-read-primary', primary);
      failure('control-read-close', error);
    }
  }
  if (primary) throw primary;
  if (closeError) throw closeError;
  return result;
}
function write(filePath, value) {
  try { fs.writeFileSync(filePath, JSON.stringify({ pid: process.pid, monotonicNs: now(), ...value }) + '\n'); }
  catch (error) { failure('ack-write', error); }
}
async function waitFor(filePath, predicate) {
  const deadline = Date.now() + 5000;
  while (true) {
    const value = read(filePath);
    if (value && predicate(value)) return value;
    if (firstError) throw new Error(firstError);
    if (Date.now() >= deadline) throw new Error('Framing native acknowledgement deadline');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}
const tail = Buffer.from('{"jsonrpc":"2.0","method":"notifications/codegraph_fixture_opaque_framing_v1"}\n');
const utf8Tail = Buffer.from('{"jsonrpc":"2.0","method":"notifications/codegraph_fixture_utf8_tail_v1","params":{"marker":"é"}}\n');
function writeBarrier(filePath, value) {
  try { fs.writeFileSync(filePath, JSON.stringify({ pid: process.pid, monotonicNs: now(), ...value }) + '\n', { flag: 'wx' }); }
  catch (error) { failure('exclusive-barrier-write', error); throw error; }
}

if (role === 'owner' && process.env.CODEGRAPH_DAEMON_INTERNAL === '1') {
  const owners = require('../../dist/mcp/query-service-owner');
  const candidates = require('../../dist/mcp/daemon-paths').getDaemonSocketCandidates(root);
  const originalWrite = net.Socket.prototype.write;
  let decorated = 0;
  net.Socket.prototype.write = function(chunk, ...args) {
    const bytes = Buffer.isBuffer(chunk) ? chunk : typeof chunk === 'string' ? Buffer.from(chunk) : null;
    if (!bytes || bytes.length > 16384) return originalWrite.call(this, chunk, ...args);
    let hello; try { hello = JSON.parse(bytes.toString('utf8')); } catch { return originalWrite.call(this, chunk, ...args); }
    if (hello?.pid !== process.pid || hello.protocol !== 1 || !candidates.includes(hello.socketPath)) return originalWrite.call(this, chunk, ...args);
    // One consumed decoration must not alter later legitimate self-admission
    // or healthy-client greetings. Exact requester raw receive is correlated
    // with the first decorated frame by the parent suite, not by a PID label.
    if (decorated) {
      record({ event: 'actual-unmodified-hello', reason: 'one-shot-consumed', hello,
        originalBase64: bytes.toString('base64'), bytes: bytes.length, sha256: hash(bytes) });
      return originalWrite.call(this, chunk, ...args);
    }
    let armed;
    try { armed = read(control); } catch (error) { failure('arm-read', error); return originalWrite.call(this, chunk, ...args); }
    if (!armed) { record({ event: 'actual-unmodified-hello', reason: 'not-armed', hello,
      originalBase64: bytes.toString('base64'), bytes: bytes.length, sha256: hash(bytes) }); return originalWrite.call(this, chunk, ...args); }
    const socket = this;
    try {
      if (armed.root !== root || !Number.isSafeInteger(armed.requesterPid) || armed.requesterPid <= 0
        || !Number.isSafeInteger(armed.socketId) || armed.socketId <= 0 || armed.purpose !== 'attachment') throw new Error('Framing ownership/one-armed-requester mismatch');
      const actualOwners = owners.readQueryServiceOwners(root);
      if (actualOwners.daemon.kind !== 'live' || actualOwners.writer.kind !== 'live'
        || actualOwners.daemon.value.pid !== process.pid || actualOwners.writer.value.pid !== process.pid
        || actualOwners.daemon.value.socketPath !== hello.socketPath) throw new Error('Framing actual daemon ownership mismatch');
      const mode = armed.mode, positive = mode === 'utf8-boundary' || mode === 'crlf-boundary' || mode === 'utf8-tail-split';
      const jsonBytes = mode === 'utf8-prefix-dangling' ? 4100 : mode === 'utf8-boundary' || mode === 'utf8-tail-split' ? 4096 : mode === 'crlf-boundary' ? 4095 : mode === 'crlf-overflow' ? 4096 : 4097;
      if (!bytes.equals(Buffer.from(JSON.stringify(hello) + '\n'))) throw new Error('Framing original greeting shape differs');
      const header = bytes.subarray(0, bytes.length - 2).toString('utf8') + ',"fixturePadding":"';
      const available = jsonBytes - Buffer.byteLength(header) - 2;
      if (available < 8) throw new Error('Actual greeting too large for literal boundary fixture');
      const multibyte = mode !== 'ascii-terminated';
      const padding = multibyte ? (available % 2 ? 'a' : '') + 'é'.repeat(Math.floor(available / 2)) : 'x'.repeat(available);
      const body = Buffer.from(header + padding + '"}');
      const modified = JSON.parse(body.toString('utf8')); delete modified.fixturePadding;
      if (JSON.stringify(modified) !== JSON.stringify(hello) || body.length !== jsonBytes) throw new Error('Framing decoration changed actual authority/target size');
      const cr = mode.startsWith('crlf-') ? Buffer.from('\r') : Buffer.alloc(0);
      const opaque = mode === 'utf8-tail-split' ? utf8Tail : tail;
      const frame = Buffer.concat([body, cr, Buffer.from('\n'), positive ? opaque : Buffer.alloc(0)]);
      const split = mode === 'utf8-tail-split' ? body.length + 1 + utf8Tail.indexOf(Buffer.from('é')) + 1
        : mode === 'utf8-prefix-dangling' ? 4097 : mode === 'utf8-boundary' ? Buffer.byteLength(header) + (available % 2) + 1 : mode === 'ascii-terminated' ? 4096 : 0;
      decorated = 1;
      let nativeClose = false;
      socket.once('close', hadError => { nativeClose = true; record({ event: 'decorated-socket-native-close', hadError }); });
      socket.on('error', error => failure('decorated-native-socket-error', error));
      record({ event: 'actual-decorated-hello', mode, requesterPid: armed.requesterPid, socketId: armed.socketId, purpose: armed.purpose, hello, owners: actualOwners,
        originalBase64: bytes.toString('base64'), originalSha256: hash(bytes), frameBase64: frame.toString('base64'),
        frameSha256: hash(frame), preLfBytes: body.length + cr.length, utf16Units: body.toString('utf8').length + cr.length,
        split, tailBase64: positive ? opaque.toString('base64') : '', authorityUnchanged: true });
      const emit = (part, phase) => originalWrite.call(socket, part, error => {
        if (error) failure('native-write-' + phase, error);
        record({ event: 'native-frame-write-complete', phase, bytes: part.length, sha256: hash(part), error: error ? String(error).slice(0, 2048) : null });
      });
      if (!split) return emit(frame, 'whole');
      const prefix = frame.subarray(0, split), continuation = frame.subarray(split);
      record({ event: 'native-prefix-submitted', bytes: prefix.length, lastByte: prefix[prefix.length - 1], hasLf: prefix.includes(10) });
      const accepted = emit(prefix, 'prefix');
      void (async () => {
        const ack = await waitFor(acknowledgement, value => value.requesterPid === armed.requesterPid
          && value.socketId === armed.socketId && value.rawBytes >= split);
        record({ event: 'actual-prefix-acknowledgement', ack });
        if (mode === 'utf8-tail-split') {
          const consumer = await waitFor(consumerAcknowledgement, value => value.root === root && value.requesterPid === armed.requesterPid
            && value.socketId === armed.socketId && value.rawBytes === split && value.sawLf === true);
          record({ event: 'actual-tail-consumer-acknowledgement', ack: consumer });
          if (nativeClose || socket.destroyed) throw new Error('UTF8 tail socket closed before controlled continuation');
          record({ event: 'held-continuation-released', bytes: continuation.length, firstByte: continuation[0] });
          await new Promise((resolve, reject) => originalWrite.call(socket, continuation, error => {
            if (error) failure('native-write-continuation', error);
            record({ event: 'native-frame-write-complete', phase: 'continuation', bytes: continuation.length,
              sha256: hash(continuation), error: error ? String(error).slice(0, 2048) : null });
            if (error) reject(error); else resolve();
          }));
          writeBarrier(consumerRelease, { root, requesterPid: armed.requesterPid, socketId: armed.socketId,
            phase: 'actual-native-continuation-write-completed', continuationSha256: hash(continuation) });
          record({ event: 'actual-tail-consumer-release-written', requesterPid: armed.requesterPid, continuationSha256: hash(continuation) });
          return;
        }
        if (mode === 'utf8-prefix-dangling') {
          // Promise rejection destroys the socket after the synchronous data
          // listener returns. Hold the continuation while observing that real
          // close; ack.destroyed alone would race the caller's await reaction.
          const deadline = Date.now() + 500;
          let close;
          do {
            close = read(closeAcknowledgement);
            if (close?.requesterPid === armed.requesterPid && close.socketId === armed.socketId) break;
            if (firstError) throw new Error(firstError);
            await new Promise(resolve => setTimeout(resolve, 10));
          } while (Date.now() < deadline);
          if (close?.requesterPid === armed.requesterPid && close.socketId === armed.socketId) {
            if (close.hadError) throw new Error('Byte refusal native close had transmission error');
            record({ event: 'continuation-withheld-after-native-byte-refusal', close, continuationBase64: continuation.toString('base64') });
            return;
          }
          record({ event: 'native-prefix-still-open-before-continuation', heldMs: 500 });
        }
        if (nativeClose || socket.destroyed) throw new Error('Framing socket closed before controlled continuation');
        record({ event: 'held-continuation-released', bytes: continuation.length, firstByte: continuation[0] });
        emit(continuation, 'continuation');
      })().catch(error => { failure('controlled-continuation', error); socket.destroy(); });
      return accepted;
    } catch (error) { failure('actual-greeting-decoration', error); socket.destroy(); return false; }
  };
  process.once('exit', () => record({ event: 'daemon-observer-final', decorated, firstError }));
  return;
}
if (role !== 'requester') return;
const candidates = new Set(require('../../dist/mcp/daemon-paths').getDaemonSocketCandidates(root));
const originalConnect = net.createConnection;
let sockets = 0, phase = null, targetSocketId = null, pendingHello = null;
net.createConnection = function(...args) {
  const endpoint = typeof args[0] === 'string' ? args[0] : args[0]?.path;
  if (!candidates.has(endpoint)) return originalConnect.apply(this, args);
  const stack = (new Error().stack ?? '').replace(/\\/g, '/').slice(0, 8192);
  const purpose = phase === 'prepare' && stack.includes('admitQueryProject') ? 'prepare-admission'
    : phase === 'acquisition' && stack.includes('admitQueryProject') ? 'attempt-admission'
      : phase === 'acquisition' && stack.includes('connectHeldService') ? 'attachment' : null;
  const socketId = ++sockets;
  try {
    if (!purpose || !stack.includes('connectObservedService')) throw new Error('Unclassified actual framing socket');
    if (purpose === 'attachment') {
      if (targetSocketId !== null || !consumed.has(100) || !consumed.has(101)) throw new Error('Attachment armed before buffered input or more than once');
      const intent = read(control + '.request');
      if (intent?.root !== root || intent.requesterPid !== process.pid) throw new Error('Attachment arming intent does not select this requester');
      targetSocketId = socketId;
      writeBarrier(control, { ...intent, socketId, purpose });
      record({ event: 'actual-attachment-decoration-armed', socketId, purpose, endpoint, stack });
    }
  } catch (error) { failure('actual-socket-purpose', error); throw error; }
  const socket = originalConnect.apply(this, args);
  pendingHello = { socketId, purpose, endpoint };
  const originalPush = socket.push, originalOn = socket.on, originalRemove = socket.removeListener;
  const originalEncoding = socket.setEncoding;
  const originalSocketWrite = socket.write;
  const listeners = new Map(); let rawBytes = 0, rawFrame = Buffer.alloc(0), sawLf = false, parserObserved = false;
  let encodingCalls = 0, consumerObserved = false, consumerBarrierReleased = false;
  if (tailSplitMode) socket.write = function(chunk, ...values) {
    let message;
    try { message = JSON.parse(Buffer.isBuffer(chunk) ? chunk.toString('utf8') : chunk); } catch { /* identity-only observer */ }
    if (message?.method === 'tools/call' && message.params?.name === 'codegraph_explore') {
      record({ event: 'actual-tail-actionable-forwarded', socketId, id: message.id, consumerBarrierReleased });
    }
    try { return originalSocketWrite.call(this, chunk, ...values); }
    catch (error) { failure('actual-tail-socket-write', error); throw error; }
  };
  if (tailSplitMode) socket.setEncoding = function(...values) {
    const stack = (new Error().stack ?? '').slice(0, 8192);
    try {
      const result = originalEncoding.apply(this, values);
      record({ event: 'actual-tail-set-encoding-return', socketId, call: ++encodingCalls, encoding: values[0], rawBytes, sawLf, stack });
      return result;
    } catch (error) { failure('actual-tail-set-encoding', error); throw error; }
  };
  socket.push = function(chunk, ...rest) {
    if (Buffer.isBuffer(chunk)) {
      rawBytes += chunk.length;
      if (rawBytes > 2 * 1024 * 1024) failure('raw-receive', new Error('Raw native cumulative cap'));
      if (tailSplitMode) {
        if (chunk.length > 32768) failure('tail-native-receive', new Error('UTF8 tail native chunk prefix cap'));
        record({ event: 'actual-tail-native-chunk', socketId, bytes: chunk.length, base64: chunk.subarray(0, 32768).toString('base64') });
      }
      if (!sawLf) {
        const nl = chunk.indexOf(10); const part = nl < 0 ? chunk : chunk.subarray(0, nl + 1);
        if (rawFrame.length + part.length > 16384) failure('raw-frame', new Error('Raw greeting cap'));
        else rawFrame = Buffer.concat([rawFrame, part]);
        record({ event: 'actual-predecoder-chunk', socketId, bytes: chunk.length, base64: chunk.subarray(0, 32768).toString('base64'),
          rawPreLfBytes: nl < 0 ? rawFrame.length : rawFrame.length - 1, lfOffset: nl, rawFrameBase64: rawFrame.toString('base64'),
          tailBase64: nl < 0 ? '' : chunk.subarray(nl + 1).toString('base64') });
        if (nl >= 0) sawLf = true;
      }
    } else if (chunk === null) record({ event: 'actual-native-read-EOF', socketId, rawBytes, sawLf, destroyed: socket.destroyed });
    return originalPush.call(this, chunk, ...rest);
  };
  socket.on = function(event, listener) {
    if (tailSplitMode && event === 'data' && parserObserved && !consumerObserved) {
      const stack = (new Error().stack ?? '').replace(/\\/g, '/').slice(0, 8192);
      if (stack.includes('attachDaemon') && stack.includes(require.resolve('../../dist/mcp/proxy').replace(/\\/g, '/'))) {
        // Delegate the registration before the barrier, but do not return to
        // attachDaemon's buffered writes until the owned tail was transmitted.
        const result = originalOn.call(this, event, listener);
        consumerObserved = true;
        try {
          const value = { root, requesterPid: process.pid, socketId, rawBytes, sawLf, encodingCalls, stack };
          record({ event: 'actual-tail-consumer-registered', ...value });
          writeBarrier(consumerAcknowledgement, value);
          const deadline = Date.now() + 5000, sleeper = new Int32Array(new SharedArrayBuffer(4));
          let release;
          while (true) {
            release = read(consumerRelease);
            if (release && release.root === root && release.requesterPid === process.pid && release.socketId === socketId) break;
            if (firstError) throw new Error(firstError);
            if (Date.now() >= deadline) throw new Error('UTF8 tail MAIN-host consumer barrier deadline');
            Atomics.wait(sleeper, 0, 0, 10);
          }
          if (release.abort) throw new Error('UTF8 tail MAIN-host barrier released by failed-fixture cleanup');
          if (release.phase !== 'actual-native-continuation-write-completed') throw new Error('Unproved UTF8 tail release phase');
          consumerBarrierReleased = true;
          record({ event: 'actual-tail-consumer-barrier-released', socketId, release, rawBytes, sawLf, encodingCalls });
        } catch (error) { failure('actual-tail-consumer-barrier', error); throw error; }
        return result;
      }
    }
    if (event !== 'data' || parserObserved) return originalOn.call(this, event, listener);
    parserObserved = true;
    const observed = function(...args) {
      try { return listener.apply(this, args); }
      finally {
        const chunk = args[0]; record({ event: 'actual-parser-data-return', socketId, rawBytes, sawLf,
          receivedType: Buffer.isBuffer(chunk) ? 'Buffer' : typeof chunk,
          decodedBytes: typeof chunk === 'string' ? Buffer.byteLength(chunk) : null, destroyed: socket.destroyed });
        write(acknowledgement, { requesterPid: process.pid, socketId, rawBytes, sawLf, destroyed: socket.destroyed });
      }
    };
    listeners.set(listener, observed); return originalOn.call(this, event, observed);
  };
  socket.removeListener = function(event, listener) { return originalRemove.call(this, event, listeners.get(listener) ?? listener); };
  originalOn.call(socket, 'connect', () => record({ event: 'actual-requester-socket-native-connect', socketId, endpoint }));
  originalOn.call(socket, 'close', hadError => {
    record({ event: 'actual-requester-socket-native-close', socketId, hadError, destroyed: socket.destroyed, rawBytes, sawLf });
    write(closeAcknowledgement, { requesterPid: process.pid, socketId, hadError, rawBytes, sawLf });
  });
  originalOn.call(socket, 'error', error => failure('requester-native-socket-error', error));
  record({ event: 'actual-native-socket-observer-installed', socketId, purpose, endpoint, stack });
  return socket;
};
const originalTimeout = global.setTimeout;
global.setTimeout = function(callback, delay, ...args) {
  if (delay !== 3000 || !(new Error().stack ?? '').includes('readHelloLine')) return originalTimeout.call(this, callback, delay, ...args);
  const identity = pendingHello; pendingHello = null;
  if (!identity) { failure('hello-timer-correlation', new Error('Actual hello timer has no native socket')); throw new Error(firstError); }
  const installedNs = now(); record({ event: 'actual-hello-timer-installed', ...identity, delay, installedNs });
  return originalTimeout.call(this, function(...values) { record({ event: 'actual-hello-timer-fired', ...identity, installedNs }); return callback.apply(this, values); }, delay, ...args);
};
const consumed = new Set(), originalStdinOn = process.stdin.on;
let observedInput = false, input = Buffer.alloc(0), inputBytes = 0;
process.stdin.on = function(event, listener) {
  if (event !== 'data' || observedInput) return originalStdinOn.call(this, event, listener);
  observedInput = true;
  return originalStdinOn.call(this, event, function(...args) {
    try { return listener.apply(this, args); }
    finally {
      try {
        const chunk = Buffer.isBuffer(args[0]) ? args[0] : Buffer.from(args[0]); inputBytes += chunk.length;
        if (inputBytes > 1024 * 1024) throw new Error('Observed framing stdin byte cap');
        input = Buffer.concat([input, chunk]); let nl;
        while ((nl = input.indexOf(10)) >= 0) {
          if (nl > 65536) throw new Error('Observed framing stdin line cap');
          const value = JSON.parse(input.subarray(0, nl).toString('utf8')); input = input.subarray(nl + 1);
          if (value.method === 'tools/call' && value.params?.name === 'codegraph_explore') { consumed.add(value.id); record({ event: 'actual-actionable-input-consumed', id: value.id }); }
        }
        if (input.length > 65536) throw new Error('Observed framing stdin partial cap');
      } catch (error) { failure('actual-stdin-observer', error); }
    }
  });
};
process.stdin.once('end', () => record({ event: 'actual-input-EOF', partialBytes: input.length, firstError }));
const proxy = require('../../dist/mcp/proxy'), originalRun = proxy.runLocalHandshakeProxy;
function observedDeps(deps, branch) {
  if (deps.root !== root || typeof deps.makeEngine !== 'function' || typeof deps.getDaemonSocket !== 'function') throw new Error('Unexpected actual proxy dependencies');
  record({ event: 'requester-factory-observer-installed', branch, root: deps.root });
  return { ...deps, makeEngine: function(...args) { record({ event: 'requester-factory-invoked' }); return deps.makeEngine.apply(deps, args); },
    getDaemonSocket: async function(...args) {
      const deadline = Date.now() + 10000;
      while (!consumed.has(100) || !consumed.has(101)) {
        if (firstError) throw new Error(firstError);
        if (Date.now() >= deadline) { failure('acquisition-input-gate', new Error('Actual buffered input deadline')); throw new Error(firstError); }
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      record({ event: 'actual-acquisition-released-after-buffered-input', consumed: [...consumed] });
      phase = 'acquisition';
      try { return await deps.getDaemonSocket.apply(deps, args); }
      finally { phase = null; }
    } };
}
proxy.runLocalHandshakeProxy = function(input) {
  if (typeof input.prepare !== 'function') return originalRun.call(this, observedDeps(input, 'legacy-deps'));
  return originalRun.call(this, { ...input, prepare: async function(...args) {
    phase = 'prepare';
    try {
      const prepared = await input.prepare.apply(input, args);
      if (prepared.kind !== 'proxy') throw new Error('Framing requester did not prepare actual proxy dependencies');
      return { ...prepared, deps: observedDeps(prepared.deps, 'prepared-deps') };
    } finally { phase = null; }
  } });
};
process.once('exit', () => record({ event: 'requester-observer-final', sockets, targetSocketId, firstError }));
