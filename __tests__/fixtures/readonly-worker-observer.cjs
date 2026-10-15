// Observe the public Worker interface only. This does not alter product policy.
const fs = require('node:fs');
const threads = require('node:worker_threads');
if (threads.isMainThread && process.env.CG_READONLY_WORKER_LOG) {
  const record = value => fs.appendFileSync(process.env.CG_READONLY_WORKER_LOG, JSON.stringify({ pid: process.pid, ...value }) + '\n');
  const RealWorker = threads.Worker;
  threads.Worker = class extends RealWorker {
    constructor(file, options) {
      super(file, options);
      if (!String(file).replaceAll('\\', '/').endsWith('/mcp/query-worker.js')) return;
      const threadId = this.threadId;
      record({ type: 'spawn', threadId, root: options?.workerData?.root, execution: options?.workerData?.execution });
      this.on('message', message => record({ threadId, ...message }));
      this.on('exit', code => record({ type: 'exit', threadId, code }));
    }
  };
}
