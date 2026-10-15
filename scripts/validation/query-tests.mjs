import { startVitest } from 'vitest/node';
import path from 'node:path';

// No config bundling, workspace discovery, implicit build or dependency writes.
// The separate build receipt supplies dist for suites that use real workers.
const root = process.env.BASELINE_ROOT;
const out = process.env.VALIDATION_OUT;
const ctx = await startVitest('test', process.argv.slice(2), {
  root, config: false, workspace: path.join(root, 'scripts/validation/query-workspace.mjs'), run: true, watch: false,
  globals: true, environment: 'node', include: ['__tests__/**/*.test.ts'],
  exclude: ['__tests__/ui-package.test.ts'],
  setupFiles: [path.join(root, '__tests__/setup-home-sandbox.ts')],
  env: { CODEGRAPH_ALLOW_UNSAFE_NODE: '1', CODEGRAPH_TELEMETRY: '0', CODEGRAPH_NO_UPDATE_CHECK: '1' },
  pool: 'forks', poolOptions: { forks: { execArgv: ['--liftoff-only'] } },
  maxWorkers: 1, minWorkers: 1,
  reporters: ['json'], outputFile: path.join(out, 'tests.json'),
}, { cacheDir: path.join(out, 'vite-cache') });
await ctx?.close();
