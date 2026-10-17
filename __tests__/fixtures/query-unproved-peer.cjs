// Negative rendezvous only: no owner records, descriptor, graph or handler.
// Later empty EOFs bound a broken product's repeated probes. In no-hello mode
// the FIRST native connection stays silent until its client closes it.
const net = require('node:net');
const fs = require('node:fs');
const [encodedEndpoints, mode, encodedLimit, release] = process.argv.slice(2);
const endpoints = JSON.parse(encodedEndpoints), limit = Number(encodedLimit);
const send = value => process.stdout.write(JSON.stringify({ pid: process.pid, ...value }) + '\n');
const connections = [], servers = [], errors = [], sockets = new Set();
let stopping = false, nextId = 0, receivedTotal = 0, retainedTotal = 0;
const fail = (phase, error) => {
  if (errors.length < 16) errors.push({ phase, error: String(error).slice(0, 2048) });
  process.exitCode = 1;
};
const snapshot = () => ({ mode, limit, errors, receivedTotal, retainedTotal, connections, listening: servers.filter(server => server.listening).length });
if (!['malformed', 'oversized', 'no-hello'].includes(mode) || limit !== 4096 || !Array.isArray(endpoints) || !endpoints.length) {
  throw new Error('Invalid owned negative-peer recipe');
}
async function shutdown() {
  if (stopping) return;
  stopping = true;
  for (const socket of sockets) { try { socket.destroy(); } catch (error) { fail('socket-destroy', error); } }
  for (const server of servers) {
    try { if (server.listening) await new Promise((resolve, reject) => server.close(error => error ? reject(error) : resolve())); }
    catch (error) { fail('server-close', error); }
  }
  send({ event: 'owned-close', snapshot: snapshot() });
}
for (const endpoint of endpoints) {
  const server = net.createServer(socket => {
    if (stopping) { socket.on('error', error => fail('shutdown-socket', error)); socket.destroy(); return; }
    sockets.add(socket);
    const value = { id: ++nextId, endpoint, emittedBytes: 0, emittedPrefixBase64: '',
      delivery: { attempted: false, completed: false, completedNs: null, error: null }, receivedBytes: 0, receivedPrefixBase64: '',
      acceptedNs: process.hrtime.bigint().toString(), closedNs: null, end: false, closed: false, socketError: null,
      observation: mode === 'no-hello' && nextId > 1 ? 'subsequent-empty-EOF' : mode };
    if (connections.length >= 2000) { fail('connection-cap', new Error('Negative peer connection cap')); socket.destroy(); return; }
    connections.push(value);
    send({ event: 'connected-probe', connection: value.id, endpoint, observation: value.observation });
    const prefix = Buffer.alloc(65536); let retained = 0;
    socket.on('data', data => {
      value.receivedBytes += data.length; receivedTotal += data.length;
      const count = Math.min(data.length, prefix.length - retained, 65536 - retainedTotal);
      data.copy(prefix, retained, 0, count); retained += count; retainedTotal += count;
      value.receivedPrefixBase64 = prefix.subarray(0, retained).toString('base64');
      if (count < data.length) fail('socket-input-overflow', new Error('Negative peer cumulative input exceeded 65536 bytes'));
    });
    socket.on('end', () => { value.end = true; });
    // Native reset on rejected bytes is an observed transport fact. Other
    // errors are sticky fixture failures; none can become peer absence.
    socket.on('error', error => {
      value.socketError ??= { code: error.code ?? null, message: String(error).slice(0, 2048) };
      if (!['ECONNRESET', 'EPIPE'].includes(error.code)) fail('socket-error', error);
    });
    socket.once('close', () => {
      value.closed = true; value.closedNs = process.hrtime.bigint().toString(); sockets.delete(socket);
      send({ event: 'probe-closed', connection: value.id, emittedBytes: value.emittedBytes, receivedBytes: value.receivedBytes });
    });
    // Fixture sequencing only: the real proxy has consumed IDs100/101 before
    // the controller releases the first negative bytes. No latency claim.
    void (async () => {
      const deadline = Date.now() + 10000;
      while (!fs.existsSync(release)) {
        if (stopping || socket.destroyed) return;
        if (Date.now() >= deadline) throw new Error('Owned negative delivery release deadline');
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      if (stopping || socket.destroyed) return;
      const deliver = bytes => {
        value.emittedBytes = bytes.length; value.emittedPrefixBase64 = bytes.subarray(0, 64).toString('base64');
        value.delivery.attempted = true;
        socket.end(bytes, error => {
          if (error) {
            value.delivery.error = String(error).slice(0, 2048); fail('payload-delivery', error);
            send({ event: 'fixture-failure', error: value.delivery.error }); return;
          }
          // Native writable completion proves this literal payload was flushed
          // on this connection; it is not a remote application receipt.
          value.delivery.completed = true; value.delivery.completedNs = process.hrtime.bigint().toString();
          send({ event: 'payload-delivered', connection: value.id, endpoint, observation: value.observation,
            emittedBytes: value.emittedBytes, emittedPrefixBase64: value.emittedPrefixBase64 });
        });
      };
      if (mode === 'malformed') {
        deliver(Buffer.from('{unproved codegraph hello}\n'));
      } else if (mode === 'oversized') {
        deliver(Buffer.alloc(limit + 1, 120));
      } else if (value.id > 1) socket.end();
    })().catch(error => { fail('delivery', error); send({ event: 'fixture-failure', error: String(error) }); socket.destroy(); });
  });
  servers.push(server); server.on('error', error => fail('server-error', error));
}
Promise.all(servers.map((server, index) => new Promise((resolve, reject) => {
  server.once('error', reject); server.listen(endpoints[index], resolve);
}))).then(() => send({ event: 'ready', snapshot: snapshot() }), async error => {
  fail('listen', error); send({ event: 'fixture-failure', error: String(error) }); await shutdown();
});
process.stdin.on('error', error => { fail('stdin', error); void shutdown(); });
process.stdin.resume(); process.stdin.once('end', () => { void shutdown(); });
