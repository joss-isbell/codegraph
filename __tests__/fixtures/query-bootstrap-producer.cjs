'use strict';

const path = require('node:path');
const { confirmDaemonBootstrap, reportDaemonBootstrapRefusal } = require('../../dist/mcp/query-bootstrap.js');
const { QueryPolicyRefusalError } = require('../../dist/mcp/query-policy-refusal.js');
const mode = process.argv[2];
// An over-bound root is transport input only; never use it as a filesystem path.
const projectRoot = mode === 'oversized-root-refusal' ? '€'.repeat(30_000) : path.resolve(process.argv[3]);
const control = require('./query-bootstrap-control.cjs')();
const finish = (event, error) => process.stdout.write(JSON.stringify({ event, pid: process.pid,
  ...(error ? { code: error.code, reason: error.reason } : {}) }) + '\n', () => process.exit(0));

(async () => {
  if (!await control.begin) { finish('cancelled'); return; }
  if (mode === 'confirm') {
    try { await confirmDaemonBootstrap(projectRoot); finish('promoted'); }
    catch (error) { finish('promotion-denied', error); }
    return;
  }
  const refusal = new QueryPolicyRefusalError('unsupported-mode', projectRoot,
    mode === 'oversized-refusal' ? '€'.repeat(30_000) : 'Shared combined preset is not supported.', 'Shared policy refused.');
  try {
    await reportDaemonBootstrapRefusal(refusal);
    if (mode === 'held-refusal') await control.release;
    finish('refusal-reported');
  } catch (error) { finish('refusal-denied', error); }
})().catch(error => {
  process.stderr.write(Buffer.from(String(error), 'utf8').subarray(0, 4096), () => process.exit(1));
});
