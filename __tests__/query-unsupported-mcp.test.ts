import { describe, it } from 'vitest';
import { runUnlimitedMCPCase } from './query-unlimited-mcp-fixture';

describe('unwired combined preset through the actual public shared route', () => {
  it('refuses the original shared-daemon disabled preset row before service authority or engine construction',
    async () => runUnlimitedMCPCase('shared-daemon', 'disabled'), 120000);
});
