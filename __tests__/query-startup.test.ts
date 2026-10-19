import { afterEach, describe, expect, it, vi } from 'vitest';
import { MCPServer } from '../src/mcp';

afterEach(() => vi.unstubAllEnvs());

describe('MCP startup query policy', () => {
  it('rejects an unsupported new preset before a server can start', () => {
    vi.stubEnv('CODEGRAPH_QUERY_MODE', 'turbo');
    vi.stubEnv('CODEGRAPH_QUERY_POOL_SIZE', '1e3');
    vi.stubEnv('CODEGRAPH_QUERY_BUSY_TIMEOUT_MS', 'unlimited');
    expect(() => new MCPServer()).toThrow(
      'Invalid CODEGRAPH_QUERY_MODE value "turbo"; expected default or unlimited.',
    );
  });
});
