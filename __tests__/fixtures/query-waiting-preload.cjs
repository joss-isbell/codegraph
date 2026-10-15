// Test boundary only: hold a real query worker's incoming call, then let the
// unchanged built handler read its real SQLite index and produce the result.
const fs = require('node:fs');
const threads = require('node:worker_threads');
const record = value => fs.appendFileSync(process.env.QUERY_WAITING_OBSERVATIONS,
  JSON.stringify({ at: Date.now(), pid: process.pid, threadId: threads.threadId, ...value }) + '\n');
if (threads.isMainThread) {
  const OriginalWorker = threads.Worker;
  threads.Worker = class extends OriginalWorker {
    constructor(file, options) {
      super(file, options);
      if (!String(file).endsWith('query-worker.js')) return;
      const threadId = this.threadId;
      record({ event: 'spawn', threadId, execution: options.workerData.execution });
      this.on('exit', code => record({ event: 'exit', threadId, code }));
    }
  };
} else if (threads.workerData?.execution) {
  const port = threads.parentPort;
  const originalOn = port.on.bind(port);
  port.on = (event, listener) => originalOn(event, event !== 'message' ? listener : message => {
    if (message?.type === 'call' && message.toolName === 'codegraph_search' && message.args.query === 'delayedTimingSymbol') {
      record({ event: 'controlled-start', id: message.id, context: message.context });
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 47000);
      record({ event: 'controlled-release', id: message.id });
    }
    listener(message);
  });
  const originalPost = port.postMessage.bind(port);
  port.postMessage = (message, ...rest) => {
    if (message?.type === 'result') record({ event: 'result', id: message.id, context: message.context });
    return originalPost(message, ...rest);
  };
}
