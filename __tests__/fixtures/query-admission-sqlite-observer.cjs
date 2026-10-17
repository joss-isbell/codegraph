'use strict';
// Delegates every native constructor/result unchanged. Install before loading
// built engine modules; the bounded records are evidence, never query fixtures.
const sqlite = require('node:sqlite');
const original = sqlite.DatabaseSync;
let roots = [], records = [], total = 0, overflow = false;
const observed = new Proxy(original, {
  construct(target, args, newTarget) {
    const file = String(args[0]);
    if (roots.some(root => file === root || file.startsWith(root + require('node:path').sep))) {
      total++;
      if (records.length < 256) records.push({ file, readOnly: args[1]?.readOnly === true });
      else overflow = true;
    }
    return Reflect.construct(target, args, newTarget);
  },
});
sqlite.DatabaseSync = observed;
if (sqlite.DatabaseSync !== observed) throw new Error('Native SQLite observer was not installed');
module.exports = {
  begin(paths) { roots = paths.slice(); records = []; total = 0; overflow = false; },
  facts() { return { total, overflow, records: records.slice() }; },
  restore() { sqlite.DatabaseSync = original; },
};

// The dedicated proxy row uses real stdio and this same native observer in one
// owned child. Only acquisition unavailability is injected; engine permission,
// transport responses and all SQLite constructors remain actual built code.
if (require.main === module) {
  const fs = require('node:fs'), path = require('node:path');
  const root = process.argv[2], hint = process.argv[3] || root;
  const { MCPEngine } = require('../../dist/mcp/engine');
  const { ExploreSessionState } = require('../../dist/mcp/explore-session-state');
  const originalRecord = ExploreSessionState.prototype.record;
  let historyRecords = 0;
  ExploreSessionState.prototype.record = function (...args) { historyRecords++; return originalRecord.apply(this, args); };
  const { runLocalHandshakeProxy } = require('../../dist/mcp/proxy');
  let factories = 0, calls = 0, acquisitions = 0, engine, firstRefusal = null, captureError = null;
  const originalWrite = process.stdout.write;
  process.stdout.write = function (...args) {
    const delivered = originalWrite.apply(this, args);
    try {
      if (Buffer.byteLength(args[0]) > 65536) throw new Error('Owned proxy response observation overflow');
      const message = JSON.parse(String(args[0]).trim());
      if (!firstRefusal && message.error?.data?.code === 'CODEGRAPH_QUERY_POLICY_REFUSED') {
        firstRefusal = { acquisitions, factories, calls, historyRecords, id: message.id };
      }
    } catch (error) { captureError ||= String(error).slice(0, 2048); }
    return delivered;
  };
  module.exports.begin([root]);
  process.once('exit', code => {
    try {
      fs.writeFileSync(path.join(root, 'proxy-admission-facts.json'), JSON.stringify({ code, root, hint, factories, calls, acquisitions, historyRecords, firstRefusal, captureError,
        sqlite: module.exports.facts(), diagnostics: engine?.getQueryDiagnostics() }));
    } catch (error) { process.stderr.write(String(error).slice(0, 2048)); process.exitCode = 1; }
  });
  void runLocalHandshakeProxy({ root: hint, getDaemonSocket: async () => { acquisitions++; return null; }, makeEngine: () => {
    factories++; engine = new MCPEngine({ readOnly: true, watch: false, queryPool: false });
    const handler = engine.getToolHandler(), execute = handler.execute;
    handler.execute = function (...args) { calls++; return execute.apply(this, args); };
    return engine;
  } }).catch(error => { process.stderr.write(String(error).slice(0, 2048)); process.exitCode = 1; });
}
