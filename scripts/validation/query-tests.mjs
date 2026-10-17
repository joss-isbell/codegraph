import { startVitest } from 'vitest/node';
import path from 'node:path';
import fs from 'node:fs';
import { inspect } from 'node:util';

// No config bundling, workspace discovery, implicit build or dependency writes.
// The separate build receipt supplies dist for suites that use real workers.
const root = process.env.BASELINE_ROOT;
const out = process.env.VALIDATION_OUT;
const suites = process.argv.slice(2);
const testNamePattern = process.env.QUERY_TEST_NAME_PATTERN;
fs.writeFileSync(path.join(out, 'query-selection.json'), JSON.stringify({
  schemaVersion: 1, suites, testNamePattern: testNamePattern ?? null,
}, null, 2));
fs.writeFileSync(path.join(out, 'query-runtime.json'), JSON.stringify({ executable: process.execPath, versions: process.versions,
  platform: process.platform, arch: process.arch, flags: process.execArgv }, null, 2));
// Vitest 2's JSON reporter only counts test/suite failures. Unhandled errors can
// still set a failing process exit code while that report says success=true.
// Preserve those errors separately; the Python consumer requires both channels.
const diagnostics = { schemaVersion: 1, checkpoints: [] };
let context;
function recordDiagnostics(phase, errors = context?.state.getUnhandledErrors() ?? [], exitCode = process.exitCode ?? 0) {
  diagnostics.checkpoints.push({
    phase, exitCode, errorCount: errors.length,
    errors: errors.slice(0, 100).map(error => ({
      type: typeof error?.type === 'string' ? error.type : null,
      name: typeof error?.name === 'string' ? error.name : null,
      message: typeof error?.message === 'string' ? error.message.slice(0, 16384) : null,
      stack: typeof error?.stack === 'string' ? error.stack.slice(0, 32768) : null,
      detail: inspect(error, { depth: 5, maxArrayLength: 20, maxStringLength: 16384 }).slice(0, 65536),
    })),
    omittedErrors: Math.max(0, errors.length - 100),
  });
  fs.writeFileSync(path.join(out, 'query-diagnostics.json'), JSON.stringify(diagnostics, null, 2));
}
let rendezvousTask, rendezvousUpdates = 0, rendezvousLines = 0;
function findRendezvous(files) {
  const file = files?.find(file => path.basename(file.filepath) === 'daemon-rendezvous-case.test.ts');
  return file?.tasks.find(suite => suite.name === 'in-process project sharing (#2278)')
    ?.tasks?.find(test => test.name === 'opens one graph for two casings of one root');
}
function writeRendezvous(phase, task, result = task?.result, meta = task?.meta) {
  try {
    if (++rendezvousLines > 16) return;
    const errors = result?.errors ?? [];
    const record = { phase, reporterPid: process.pid, id: task.id, name: task.name, mode: task.mode,
      state: result?.state, startTime: result?.startTime, duration: result?.duration,
      errorCount: errors.length, errors: errors.slice(0, 2).map(error => inspect(error,
        { depth: 1, maxArrayLength: 2, maxStringLength: 512 }).slice(0, 768)),
      omittedErrors: Math.max(0, errors.length - 2), updatesSeen: rendezvousUpdates,
      marker: meta?.rendezvousDiagnostic };
    process.stderr.write(('[rendezvous-task] ' + inspect(record,
      { depth: 4, maxArrayLength: 2, maxStringLength: 768, breakLength: Infinity })).slice(0, 2047) + '\n');
  } catch { /* Observation must not replace the original test/error disposition. */ }
}
const diagnosticReporter = {
  onInit(ctx) { context = ctx; },
  onCollected(files) {
    try {
      const task = findRendezvous(files);
      if (task && !rendezvousTask) { rendezvousTask = task; writeRendezvous('collected', task); }
    } catch { /* Keep the existing diagnostic/acceptance channels authoritative. */ }
  },
  onTaskUpdate(packs) {
    try {
      for (const [id, result, meta] of packs) if (id === rendezvousTask?.id) {
        if (++rendezvousUpdates <= 8) writeRendezvous('update', rendezvousTask, result, meta);
      }
    } catch { /* A diagnostic failure must not become a task failure. */ }
  },
  onFinished(files, errors = []) {
    recordDiagnostics('onFinished', errors);
    try {
      let task = findRendezvous(files) ?? rendezvousTask;
      for (let depth = 0; task && depth < 4; depth++) {
        writeRendezvous('onFinished', task);
        task = task.suite ?? (task.file !== task ? task.file : undefined);
      }
    } catch { /* Preserve original results even if optional observation fails. */ }
  },
};
process.once('exit', code => recordDiagnostics('exit', undefined, code));
const ctx = await startVitest('test', suites, {
  root, config: false, workspace: path.join(root, 'scripts/validation/query-workspace.mjs'), run: true, watch: false,
  globals: true, environment: 'node', include: ['__tests__/**/*.test.ts'],
  exclude: ['__tests__/ui-package.test.ts'],
  setupFiles: [path.join(root, '__tests__/setup-home-sandbox.ts')],
  env: { CODEGRAPH_ALLOW_UNSAFE_NODE: '1', CODEGRAPH_TELEMETRY: '0', CODEGRAPH_NO_UPDATE_CHECK: '1' },
  pool: 'forks', poolOptions: { forks: { execArgv: ['--liftoff-only'] } },
  maxWorkers: 1, minWorkers: 1,
  ...(testNamePattern === undefined ? {} : { testNamePattern }),
  reporters: ['json', diagnosticReporter], outputFile: path.join(out, 'tests.json'),
}, { cacheDir: path.join(out, 'vite-cache') });
recordDiagnostics('afterStart');
await ctx?.close();
recordDiagnostics('afterClose');
