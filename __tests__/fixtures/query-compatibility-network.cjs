// Change only owned transport boundaries. Real built engines, workers, SQLite,
// sessions and tool results remain in use throughout the compatibility tests.
const fs = require('node:fs');
const net = require('node:net');
const threads = require('node:worker_threads');
if (threads.isMainThread) {
  const observations = process.env.QUERY_COMPAT_NETWORK_OBSERVATIONS;
  const record = value => {
    const line = JSON.stringify({ monotonicNs: process.hrtime.bigint().toString(), pid: process.pid, ...value }) + '\n';
    if ((fs.existsSync(observations) ? fs.statSync(observations).size : 0) + Buffer.byteLength(line) > 1024 * 1024) {
      throw new Error('Owned network observation capture overflow');
    }
    fs.appendFileSync(observations, line);
  };
  const originalWrite = net.Socket.prototype.write;
  let dropped = false;
  net.Socket.prototype.write = function(chunk, ...args) {
    let message;
    try { message = JSON.parse(Buffer.isBuffer(chunk) ? chunk.toString('utf8') : String(chunk)); } catch { /* ordinary transport */ }
    if (message?.protocol === 1 && message.codegraph && process.env.QUERY_COMPAT_UNKNOWN_HELLO &&
        fs.existsSync(process.env.QUERY_COMPAT_UNKNOWN_HELLO)) {
      // This literal missing required section represents an older/unknown peer
      // descriptor. Never change the engine's actual selected policy or status.
      delete message.queryExecution.effective;
      record({ event: 'unknown-hello', version: message.codegraph, socketPath: message.socketPath });
      return originalWrite.call(this, JSON.stringify(message) + '\n', ...args);
    }
    if (!dropped && process.env.CODEGRAPH_DAEMON_INTERNAL !== '1' && message?.id === 200 &&
        message.method === 'tools/call' && process.env.QUERY_COMPAT_DROP_REQUEST &&
        fs.existsSync(process.env.QUERY_COMPAT_DROP_REQUEST)) {
      dropped = true;
      record({ event: 'owned-link-drop', id: message.id, forwarded: false });
      this.once('close', () => record({ event: 'owned-link-closed' }));
      this.destroy();
      return false;
    }
    return originalWrite.call(this, chunk, ...args);
  };
}
