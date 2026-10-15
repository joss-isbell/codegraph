const { parentPort, workerData, threadId } = require('node:worker_threads');
const { collectBeforeExit } = require('../../dist/worker-teardown');
const exit = code => { collectBeforeExit(); process.exit(code); };
const observation = value => (workerData.observations ?? parentPort).postMessage({ type: 'observation', threadId, ...value });
observation({ event: 'startup' });
if (workerData.mode === 'exit-start') exit(0);
if (workerData.mode === 'error-start') { collectBeforeExit(); throw new Error('controlled startup resource failure'); }
if (workerData.mode === 'never-ready') setInterval(() => {}, 1000);
else setTimeout(() => {
  parentPort.postMessage({ type: 'ready', ok: workerData.mode !== 'open-failure' });
  observation({ event: 'ready' });
  if (workerData.mode === 'duplicate') parentPort.postMessage({ type: 'ready', ok: true });
}, workerData.readyDelay ?? 0);
parentPort.on('message', message => {
  if (message.type !== 'call') return;
  observation({ event: 'start', id: message.id });
  if (workerData.mode === 'exit-call') exit(0);
  if (workerData.mode === 'error-call') { collectBeforeExit(); throw new Error('controlled running resource failure'); }
  if (workerData.mode === 'hang-call') return;
  if (workerData.mode === 'duplicate') {
    parentPort.postMessage({ type: 'result', id: message.id + 1000, result: { content: [{ type: 'text', text: 'wrong result' }] } });
  }
  if (workerData.gate) {
    const state = new Int32Array(workerData.gate);
    const active = Atomics.add(state, 0, 1) + 1;
    let peak;
    do { peak = Atomics.load(state, 1); } while (active > peak && Atomics.compareExchange(state, 1, peak, active) !== peak);
    Atomics.add(state, 2, 1);
    Atomics.notify(state, 2);
    Atomics.wait(state, 3, 0);
    Atomics.sub(state, 0, 1);
    Atomics.add(state, 4, 1);
  }
  const result = { content: [{ type: 'text', text: `worker:${threadId}:job:${message.id}` }] };
  parentPort.postMessage({ type: 'result', id: message.id, result });
  observation({ event: 'complete', id: message.id });
  if (workerData.mode === 'duplicate') parentPort.postMessage({ type: 'result', id: message.id, result });
});
