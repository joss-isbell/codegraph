// Read-only identity evidence at Node's public process/Worker boundaries.
const fs = require('node:fs');
const path = require('node:path');
const crypto = require('node:crypto');
const { fileURLToPath } = require('node:url');
const threads = require('node:worker_threads');
const output = process.env.RECOVERY_IDENTITIES;
const hashFile = file => crypto.createHash('sha256').update(fs.readFileSync(file)).digest('hex');
const record = fields => fs.appendFileSync(output, JSON.stringify({ atEpochMs: Date.now(), pid: process.pid, ...fields }) + '\n');
if (output) {
  record({ type: 'runtime', threadId: threads.threadId, mainThread: threads.isMainThread,
    executable: fs.realpathSync(process.execPath), argv: process.argv, execArgv: process.execArgv,
    versions: process.versions, settings: Object.fromEntries(Object.entries(process.env).filter(([key]) => key.startsWith('CODEGRAPH_'))) });
  if (threads.isMainThread) {
    const RealWorker = threads.Worker;
    threads.Worker = class ObservedWorker extends RealWorker {
      constructor(filename, options) {
        super(filename, options);
        const threadId = this.threadId;
        const file = filename instanceof URL ? fileURLToPath(filename) : path.resolve(String(filename));
        record({ type: 'worker-spawn', threadId, file, sha256: hashFile(file) });
        this.once('message', message => record({ type: 'worker-ready', threadId, file, handshake: message?.type ?? null }));
        this.once('exit', code => record({ type: 'worker-exit', threadId, file, code }));
      }
    };
  }
}
