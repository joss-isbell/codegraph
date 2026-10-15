// A real SQLite connection and synchronous work, controlled only by the owned test.
const { parentPort, workerData, threadId } = require('node:worker_threads');
const { CodeGraph } = require('../../dist');
const { ToolHandler } = require('../../dist/mcp/tools');
const graph = CodeGraph.openSync(workerData.root, { readOnly: true });
const handler = new ToolHandler(graph, workerData.execution);
const observe = value => parentPort.postMessage({ type: 'observation', threadId, ...value });
observe({ event: 'opened', root: graph.getProjectRoot(), readOnly: true });
if (workerData.readyDelay) setTimeout(() => parentPort.postMessage({ type: 'ready', ok: true }), workerData.readyDelay);
else parentPort.postMessage({ type: 'ready', ok: true });
parentPort.on('message', async message => {
  if (message.type !== 'call') return;
  observe({ event: 'start', id: message.id, context: message.context });
  if (workerData.gate) {
    const state = new Int32Array(workerData.gate);
    Atomics.add(state, 2, 1);
    Atomics.wait(state, 3, 0);
    Atomics.add(state, 4, 1);
  }
  if (workerData.delayMs) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, workerData.delayMs);
  if (workerData.stuck) Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0);
  const result = await handler.executeReadTool(message.toolName, message.args, message.context);
  result.content[0].text = `worker:${threadId}:job:${message.id}\n` + result.content[0].text;
  parentPort.postMessage({ type: 'result', id: message.id, result });
  observe({ event: 'complete', id: message.id });
});
