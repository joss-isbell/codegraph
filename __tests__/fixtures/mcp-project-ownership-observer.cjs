'use strict';
// Native observation only: preserve every constructor, argument and result.
const fs = require('node:fs');
const path = require('node:path');
const sqlite = require('node:sqlite');
const workerThreads = require('node:worker_threads');
const { threadId } = workerThreads;
const originalWorker = workerThreads.Worker;
const workerRecords = [];
let workerOverflow = false;
workerThreads.Worker = new Proxy(originalWorker, {
  construct(target, args, newTarget) {
    const worker = Reflect.construct(target, args, newTarget);
    const record = { file: String(args[0]), workerId: worker.threadId, exited: false, code: null };
    if (workerRecords.length < 64) workerRecords.push(record); else workerOverflow = true;
    worker.once('exit', code => { record.exited = true; record.code = code; });
    return worker;
  },
});
const original = sqlite.DatabaseSync;
let roots = [], records = [], overflow = false, total = 0;
const observed = new Proxy(original, {
  construct(target, args, newTarget) {
    const file = String(args[0]);
    const enabled = roots.some(root => file.startsWith(root + path.sep)) || process.env.CODEGRAPH_OWNERSHIP_NATIVE_LOG;
    if (enabled) {
      let writer = null;
      try { writer = JSON.parse(fs.readFileSync(path.join(path.dirname(file), 'writer.pid'), 'utf8')); } catch {}
      const record = { file, readOnly: args[1]?.readOnly === true, pid: process.pid, threadId, writer };
      total++;
      if (records.length < 512) records.push(record); else overflow = true;
      const log = process.env.CODEGRAPH_OWNERSHIP_NATIVE_LOG;
      if (log) fs.appendFileSync(log, JSON.stringify(record) + '\n');
    }
    return Reflect.construct(target, args, newTarget);
  },
});
sqlite.DatabaseSync = observed;
if (sqlite.DatabaseSync !== observed) throw new Error('Ownership SQLite observer installation failed');
module.exports = {
  begin(paths) { roots = paths.slice(); records = []; total = 0; overflow = false; },
  facts() { return { total, overflow, records: records.slice() }; },
  workerFacts() { return { overflow: workerOverflow, records: workerRecords.map(record => ({ ...record })) }; },
  restore() { sqlite.DatabaseSync = original; workerThreads.Worker = originalWorker; },
};
