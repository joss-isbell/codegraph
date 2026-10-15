// Observe actual built query workers and control only their system-boundary
// delivery. The ordinary built handler still opens SQLite and returns results.
const fs = require('node:fs');
const threads = require('node:worker_threads');
const record = value => fs.appendFileSync(process.env.QUERY_UNLIMITED_OBSERVATIONS,
  JSON.stringify({ monotonicNs: process.hrtime.bigint().toString(), pid: process.pid,
    threadId: threads.threadId, ...value }) + '\n');
if (threads.isMainThread) {
  // Observe delivered main-thread history without changing emission or results.
  const { ExploreSessionState } = require('../../dist/mcp/explore-session-state');
  const originalRecord = ExploreSessionState.prototype.record;
  const identities = new WeakMap();
  let nextIdentity = 0;
  ExploreSessionState.prototype.record = function(emission) {
    const result = originalRecord.call(this, emission);
    if (result) {
      if (!identities.has(this)) identities.set(this, `${process.pid}:${++nextIdentity}`);
      record({ event: 'session-history', historyId: identities.get(this), projectRoot: result.projectRoot,
        query: result.query, index: result.index });
    }
    return result;
  };
  const OriginalWorker = threads.Worker;
  threads.Worker = class extends OriginalWorker {
    constructor(file, options) {
      super(file, options);
      if (!String(file).replaceAll('\\', '/').endsWith('/mcp/query-worker.js')) return;
      const threadId = this.threadId;
      record({ event: 'spawn', threadId, execution: options.workerData.execution });
      this.on('message', message => {
        if (message.type === 'ready') record({ event: 'ready', threadId, ok: message.ok });
        if (message.type === 'result') record({ event: 'result', threadId, id: message.id });
      });
      this.on('exit', code => record({ event: 'exit', threadId, code }));
    }
  };
  if (process.env.QUERY_UNLIMITED_FAIL_DAEMON === '1') {
    const childProcess = require('node:child_process');
    const spawn = childProcess.spawn;
    childProcess.spawn = function(command, args, options) {
      if (!options?.detached || options.env?.CODEGRAPH_DAEMON_INTERNAL !== '1') return spawn.call(this, command, args, options);
      // Own a real candidate process that exits before it can bind or acquire a
      // writer. This is genuine unavailability, never a compatibility mismatch.
      const child = spawn.call(this, command, ['-e', 'process.exit(78)'], options);
      record({ event: 'unavailable-candidate', candidatePid: child.pid, reason: 'controlled exit 78 before bind' });
      child.on('exit', code => record({ event: 'unavailable-candidate-exit', candidatePid: child.pid, code }));
      return child;
    };
  }
} else if (threads.workerData?.execution) {
  const port = threads.parentPort;
  const originalOn = port.on.bind(port);
  port.on = (event, listener) => originalOn(event, event !== 'message' ? listener : message => {
    if (message?.type === 'call') {
      record({ event: 'call-start', id: message.id, context: message.context, query: message.args.query });
      if (message.args.query === 'overlapSymbol') {
        const gate = new Int32Array(new SharedArrayBuffer(4));
        while (!fs.existsSync(process.env.QUERY_UNLIMITED_RELEASE)) Atomics.wait(gate, 0, 0, 10);
      }
      record({ event: 'call-released', id: message.id, context: message.context });
    }
    listener(message);
  });
}
