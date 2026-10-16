import { expect } from 'vitest';
import { readQueryDiagnostics, type QueryDiagnostics } from '../src/mcp/query-diagnostics';

/** Parse the actual MCP text boundary and require a complete known observation. */
export function parseQueryDiagnostics(result: any): QueryDiagnostics {
  expect(result.isError).toBeFalsy();
  const match = result.content[0].text.match(/\*\*Query execution\*\*\n```json\n([\s\S]*?)\n```/);
  expect(match, 'actual status contains the versioned execution observation').not.toBeNull();
  const value = JSON.parse(match![1]);
  expect(readQueryDiagnostics(value)).toEqual(value);
  return value;
}
