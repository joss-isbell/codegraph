// A deliberately old wire peer on a noncanonical rendezvous. It owns real
// locks/socket/process bytes, but has no query handler and is never positive
// evidence for current SQLite/query behavior. stdin EOF is owned cleanup only.
const fs = require('node:fs');
const net = require('node:net');
const path = require('node:path');
const [root, version, socketPath] = process.argv.slice(2);
const dir = path.join(root, '.codegraph');
const locks = new Map();
const writeLock = (name, value) => {
  const text = JSON.stringify(value) + '\n';
  locks.set(name, text); fs.writeFileSync(path.join(dir, name), text);
};
const server = net.createServer(socket => socket.end(JSON.stringify({ codegraph: version,
  pid: process.pid, protocol: 1, socketPath }) + '\n'));
server.listen(socketPath, () => {
  writeLock('writer.pid', { pid: process.pid, mode: 'daemon', startedAt: Date.now(), ready: true });
  writeLock('daemon.pid', { pid: process.pid, version, socketPath, startedAt: Date.now() });
  process.stdout.write(JSON.stringify({ event: 'ready', pid: process.pid, version, socketPath }) + '\n');
});
process.stdin.resume();
process.stdin.once('end', () => server.close(() => {
  for (const [name, text] of locks) {
    const file = path.join(dir, name);
    try { if (fs.readFileSync(file, 'utf8') === text) fs.unlinkSync(file); } catch { /* already replaced; preserve */ }
  }
  process.stdout.write(JSON.stringify({ event: 'owned-close', pid: process.pid }) + '\n');
  process.exitCode = 0;
}));
