import { afterEach, describe, expect, it, vi } from 'vitest';
import { MCPServer } from '../src/mcp';

afterEach(() => vi.unstubAllEnvs());

describe('strict MCP startup query policy', () => {
  it('rejects invalid requested settings before a server can start', () => {
    vi.stubEnv('CODEGRAPH_QUERY_MODE', 'default');
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '1e3');
    vi.stubEnv('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', 'unlimited');
    expect(() => new MCPServer()).toThrow(
      'Invalid CODEGRAPH_QUERY_POOL_SIZE value "1e3"; expected decimal whole numbers 0–9,007,199,254,740,991 or unlimited.',
    );
  });
});
