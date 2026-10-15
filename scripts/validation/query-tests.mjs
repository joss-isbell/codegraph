import { startVitest } from 'vitest/node';
import path from 'node:path';
import fs from 'node:fs';
import { inspect } from 'node:util';

// No config bundling, workspace discovery, implicit build or dependency writes.
// The separate build receipt supplies dist for suites that use real workers.
const root = process.env.BASELINE_ROOT;
const out = process.env.VALIDATION_OUT;
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
const diagnosticReporter = {
  onInit(ctx) { context = ctx; },
  onFinished(_files, errors = []) { recordDiagnostics('onFinished', errors); },
};
process.once('exit', code => recordDiagnostics('exit', undefined, code));
const ctx = await startVitest('test', process.argv.slice(2), {
  root, config: false, workspace: path.join(root, 'scripts/validation/query-workspace.mjs'), run: true, watch: false,
  globals: true, environment: 'node', include: ['__tests__/**/*.test.ts'],
  exclude: ['__tests__/ui-package.test.ts'],
  setupFiles: [path.join(root, '__tests__/setup-home-sandbox.ts')],
  env: { CODEGRAPH_ALLOW_UNSAFE_NODE: '1', CODEGRAPH_TELEMETRY: '0', CODEGRAPH_NO_UPDATE_CHECK: '1' },
  pool: 'forks', poolOptions: { forks: { execArgv: ['--liftoff-only'] } },
  maxWorkers: 1, minWorkers: 1,
  reporters: ['json', diagnosticReporter], outputFile: path.join(out, 'tests.json'),
}, { cacheDir: path.join(out, 'vite-cache') });
recordDiagnostics('afterStart');
await ctx?.close();
recordDiagnostics('afterClose');
