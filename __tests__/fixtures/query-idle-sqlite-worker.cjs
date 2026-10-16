const { parentPort, workerData, threadId } = require('node:worker_threads');
const { CodeGraph } = require('../../dist');
const { ToolHandler } = require('../../dist/mcp/tools');
const graph = CodeGraph.openSync(workerData.root, { readOnly: true });
const handler = new ToolHandler(graph, workerData.execution);
const observe = value => parentPort.postMessage({ type: 'observation', threadId, monotonicNs: process.hrtime.bigint().toString(), ...value });
parentPort.postMessage({ type: 'ready', ok: true });
parentPort.on('message', async message => {
  if (message.type !== 'call') return;
  observe({ event: 'start', id: message.id, query: message.args.query, context: message.context });
  const gate = workerData.gates?.[message.args.query];
  if (gate) Atomics.wait(new Int32Array(gate), 0, 0);
  const result = await handler.executeReadTool(message.toolName, message.args, message.context);
  parentPort.postMessage({ type: 'result', id: message.id, result, context: message.context });
  observe({ event: 'complete', id: message.id, query: message.args.query });
});
