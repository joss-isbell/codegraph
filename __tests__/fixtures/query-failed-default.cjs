// A separate owned process makes even a malformed SQLite opener's OS handles
// observable and released before the parent removes its disposable fixtures.
const fs = require('node:fs');
const path = require('node:path');
const threads = require('node:worker_threads');
const { DatabaseSync } = require('node:sqlite');
const { CodeGraph, getDatabasePath } = require('../../dist');
const events = [];
const RealWorker = threads.Worker;
threads.Worker = class ObservedWorker extends RealWorker {
  constructor(file, options) {
    super(file, options);
    if (!String(file).replaceAll('\\', '/').endsWith('/mcp/query-worker.js')) return;
    const threadId = this.threadId;
    events.push({ event: 'spawn', threadId });
    this.on('message', message => events.push({ event: message.type, threadId, id: message.id, ok: message.ok }));
    this.on('exit', code => events.push({ event: 'exit', threadId, code }));
  }
};
const { MCPEngine } = require('../../dist/mcp/engine');
(async () => {
  const [root, mode, output] = process.argv.slice(2);
  const valid = path.join(root, 'valid');
  const broken = path.join(root, 'broken');
  fs.mkdirSync(valid);
  fs.mkdirSync(path.join(broken, '.codegraph'), { recursive: true });
  fs.writeFileSync(path.join(valid, 'app.ts'), 'export function survivingExplicitSymbol() { return 42; }\n');
  const cg = await CodeGraph.init(valid);
  try { await cg.indexAll(); } finally { cg.close(); }
  // The directory resolver recognizes the nodes table, while ordinary writable
  // open correctly refuses this incomplete/corrupt schema's missing tables.
  const db = new DatabaseSync(getDatabasePath(broken));
  db.exec('CREATE TABLE nodes(id TEXT PRIMARY KEY)');
  db.close();
  const recognized = CodeGraph.isInitialized(broken);
  const engine = new MCPEngine({ executionMode: 'direct', queryPool: true, watch: false });
  let result;
  let state;
  let defaultLoaded;
  try {
    if (mode === 'initial') await engine.ensureInitialized(broken);
    else engine.retryInitializeSync(broken);
    defaultLoaded = engine.hasDefaultCodeGraph();
    result = await engine.getToolHandler().execute('codegraph_search', { projectPath: valid, query: 'survivingExplicitSymbol' });
    state = engine.getQueryExecutionState();
  } finally { await engine.stop(); }
  fs.writeFileSync(output, JSON.stringify({ recognized, defaultLoaded, mode, broken, valid, result, state, events }, null, 2));
})().catch(error => { console.error(error); process.exitCode = 1; });
