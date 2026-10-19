// Control only native listening time. Every socket/hello, production worker,
// policy decision and SQLite query remains the ordinary implementation.
const fs = require('node:fs');
const net = require('node:net');
const { isMainThread } = require('node:worker_threads');

if (isMainThread && process.env.QUERY_STARTUP_OBSERVATIONS) {
  const record = value => fs.appendFileSync(process.env.QUERY_STARTUP_OBSERVATIONS,
    JSON.stringify({ pid: process.pid, daemon: process.env.CODEGRAPH_DAEMON_INTERNAL === '1', ...value }) + '\n');
  const isProjectPipe = value => typeof value === 'string' && value.startsWith('\\\\.\\pipe\\codegraph-');
  const createConnection = net.createConnection;
  let unavailableRecorded = false;
  net.createConnection = function(...args) {
    const socket = createConnection.apply(this, args);
    if (isProjectPipe(args[0])) socket.on('error', error => {
      if (!unavailableRecorded && ['ENOENT', 'ECONNREFUSED'].includes(error.code)) {
        unavailableRecorded = true;
        record({ event: 'connect-unavailable', code: error.code, socketPath: args[0] });
      }
    });
    return socket;
  };

  if (process.env.CODEGRAPH_DAEMON_INTERNAL === '1') {
    const listen = net.Server.prototype.listen;
    net.Server.prototype.listen = function(...args) {
      if (!isProjectPipe(args[0])) return listen.apply(this, args);
      const server = this;
      const deadline = Date.now() + 15000;
      record({ event: 'listen-held', socketPath: args[0] });
      const poll = () => {
        if (fs.existsSync(process.env.QUERY_STARTUP_RELEASE)) {
          record({ event: 'listen-released', socketPath: args[0] });
          listen.apply(server, args);
        } else if (Date.now() >= deadline) {
          record({ event: 'listen-gate-deadline', socketPath: args[0] });
          server.emit('error', new Error('Owned startup native-listen gate deadline'));
        } else setTimeout(poll, 10);
      };
      poll();
      return server;
    };
  }
}
