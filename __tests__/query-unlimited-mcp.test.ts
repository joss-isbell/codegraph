import { describe, it } from 'vitest';
import { queryUnlimitedCases, runUnlimitedMCPCase } from './query-unlimited-mcp-fixture';

describe('independent unlimited concurrency through actual MCP', () => {
  it.each(queryUnlimitedCases)
  ('reports real requested/effective execution and routes safely in %s with %s',
    async (mode, profile) => runUnlimitedMCPCase(mode, profile), 120000);
});
