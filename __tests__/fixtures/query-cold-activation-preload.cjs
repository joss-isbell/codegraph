'use strict';
// Reuse the ordinary CLI/worker/history tracer. This observer changes no
// lifecycle, readiness, result, or native I/O; the test owns the real writer.
require('./query-unlimited-preload.cjs');
const fs = require('node:fs');
const sqlite = require('node:sqlite');
const { isMainThread, threadId } = require('node:worker_threads');
const writerFile = process.env.QUERY_COLD_ACTIVATION_WRITER;
const indexFile = process.env.QUERY_COLD_ACTIVATION_INDEX;
const record = value => fs.appendFileSync(process.env.QUERY_UNLIMITED_OBSERVATIONS,
  JSON.stringify({ monotonicNs: process.hrtime.bigint().toString(), pid: process.pid, threadId, ...value }) + '\n');
if (isMainThread) {
  const originalRead = fs.readFileSync;
  fs.readFileSync = function(...args) {
    const value = Reflect.apply(originalRead, this, args);
    if (String(args[0]) === writerFile) {
      const owner = JSON.parse(String(value));
      record({ event: 'explicit-writer-observed', file: writerFile, owner });
    }
    return value;
  };
  const OriginalDatabase = sqlite.DatabaseSync;
  sqlite.DatabaseSync = new Proxy(OriginalDatabase, {
    construct(target, args, newTarget) {
      const database = Reflect.construct(target, args, newTarget);
      if (String(args[0]) === indexFile) record({ event: 'explicit-native-open', file: indexFile, readOnly: args[1]?.readOnly === true });
      return database;
    },
  });
}
