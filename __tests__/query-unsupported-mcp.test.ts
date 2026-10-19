import { describe, it } from 'vitest';
import { runPresetMCPCase } from './query-unlimited-mcp-fixture';

// Revised #29 requires the preset in every route; the former unsupported-mode
// oracle remains historical evidence, not the prospective product contract.
describe('combined preset through actual public MCP routes', () => {
  it('serves a shared preset request with its explicit finite concurrency override',
    async () => runPresetMCPCase('shared-daemon', 'finite-override'), 90000);

  it.each(['direct', 'writable-fallback', 'read-only-fallback', 'shared-daemon'] as const)(
    'executes overlapping preset workers with no busy timer in %s',
    async mode => runPresetMCPCase(mode, 'unlimited-overlap'), 120000);

  it('keeps a shared preset finite override at two executing workers while a third waits',
    async () => runPresetMCPCase('shared-daemon', 'finite-overlap'), 90000);
});
