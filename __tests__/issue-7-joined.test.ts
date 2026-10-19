import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { pathToFileURL } from 'url';
import { Client } from './query-unlimited-mcp-fixture';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { removeSpawnLog, settleLosingCandidates } from './daemon-candidates';
import { createSelectedEmbeddedFixture } from './helpers/selected-embedded-fixture';

const delay = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
const errorText = (error: unknown) => error instanceof Error ? error.stack ?? error.message : String(error);

// Each attempt is additionally bounded by Client's 10s request deadline, so
// a failed operation takes at most 25s, not an unbounded series of requests.
async function eventually(label: string, assertion: () => void | Promise<void>, timeoutMs = 15000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try { await assertion(); return; }
    catch (error) {
      lastError = error;
      await delay(Math.min(100, Math.max(0, deadline - Date.now())));
    }
  }
  throw new Error(`Joined MCP/native watcher deadline: ${label}`, { cause: lastError });
}

describe('Issue 7 integrated query workers and selected embedded repository watching', () => {
  it.runIf(process.platform === 'win32')('serves source and ignore-rule changes through real MCP without losing unrelated facts', async () => {
    const base = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-i7j-')));
    const observations = path.join(base, 'workers.jsonl');
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    const facts: Record<string, unknown> = { base, runtime: process.execPath, version: process.version };
    let fixture: ReturnType<typeof createSelectedEmbeddedFixture> | undefined;
    let client: Client | undefined;
    let primary: unknown;
    let phase = 'fixture and evaluated index';
    let fixtureRemoved = false;
    const events = (): any[] => {
      if (!fs.existsSync(observations)) return [];
      if (fs.statSync(observations).size > 1048576) throw new Error('Joined worker observation overflow');
      return fs.readFileSync(observations, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line));
    };
    const search = async (name: string): Promise<string> => {
      const result = await client!.query(name);
      expect(result.isError, JSON.stringify(result)).toBeFalsy();
      expect(result.content[0]?.type).toBe('text');
      return result.content[0].text;
    };
    const found = async (name: string, location: string) => {
      const text = await search(name);
      expect(text).toContain(`**${name}** (function)`);
      expect(text).toContain(location);
      return text;
    };
    const autoSyncCount = () => (client!.stderr.match(/\[CodeGraph MCP\] Auto-synced \d+ file\(s\)/g) ?? []).length;
    try {
      fixture = createSelectedEmbeddedFixture(base);
      facts.root = fixture.root;
      expect(fs.existsSync(path.join(fixture.root, '.git'))).toBe(false);
      // Bootstrap the disposable index with the same built candidate the MCP
      // process and its real query workers load. Only MCP observes the facts.
      const { default: CodeGraph }: typeof import('../src/index') = require('../dist/index');
      const graph = await CodeGraph.init(fixture.root, { index: true });
      graph.close();

      client = new Client(fixture.root, 'direct', observations, path.join(base, 'unused-release'), 'default', {
        requestDeadlineMs: 10000, closeDeadlineMs: 15000,
        environment: { CODEGRAPH_QUERY_MODE: 'unlimited', CODEGRAPH_QUERY_POOL_SIZE: undefined,
          CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: undefined, CODEGRAPH_WATCH_DEBOUNCE_MS: '100' },
      });
      await client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {},
        clientInfo: { name: 'issue-7-native-join', version: '1' }, rootUri: pathToFileURL(fixture.root).href });
      phase = 'initial actual MCP worker results and native watcher readiness';
      facts.initial = {
        source: await found('nestedFact', 'hidden/source/nested/keep.ts:1'),
        ruleSelected: await found('negatedFact', 'hidden/source/nested/allowed.skip.ts:1'),
        dependency: await found('dependencyFact', 'hidden/deps/pkg/dep.ts:1'),
        unrelated: await found('unrelatedFact', 'ordinary.ts:1'),
      };
      await eventually('real native watcher active', () => {
        expect(client!.stderr).toContain(`File watcher active for ${fixture!.root}`);
      });
      const diagnostics = parseQueryDiagnostics(await client.request('tools/call', { name: 'codegraph_status', arguments: {} }));
      facts.diagnostics = diagnostics;
      expect(diagnostics.effective).toMatchObject({ executionMode: 'direct', preset: 'unlimited',
        concurrency: { kind: 'unlimited' }, deadline: { kind: 'absent' } });
      expect(diagnostics.lifecycle).toMatchObject({ dispatch: 'worker', ready: true });
      expect(diagnostics.lifecycle.workers).toBeGreaterThan(0);
      expect(events().some(event => event.event === 'call-start' && event.query === 'nestedFact')).toBe(true);

      phase = 'ordinary source edit through native Windows events';
      let priorSyncs = autoSyncCount();
      fixture.write('hidden/source/nested/keep.ts', 'export function joinedNativeFact() { return 41; }\n');
      await eventually('edited source visible in MCP', async () => {
        facts.edited = await found('joinedNativeFact', 'hidden/source/nested/keep.ts:1');
        expect(autoSyncCount()).toBeGreaterThan(priorSyncs);
      });
      expect(await search('nestedFact')).toContain('No results found for "nestedFact"');

      phase = 'applicable ignore-rule edit through native Windows events';
      priorSyncs = autoSyncCount();
      fixture.write('hidden/source/nested/.gitignore', '*.skip.ts\n');
      await eventually('new child rule removes the previously selected fact', async () => {
        facts.ignored = await search('negatedFact');
        expect(facts.ignored).toContain('No results found for "negatedFact"');
        expect(autoSyncCount()).toBeGreaterThan(priorSyncs);
      });

      phase = 'rule negation restores the fact without a manual sync';
      priorSyncs = autoSyncCount();
      fixture.write('hidden/source/nested/.gitignore', '*.skip.ts\n!allowed.skip.ts\n');
      await eventually('restored child rule visible in MCP', async () => {
        facts.restored = await found('negatedFact', 'hidden/source/nested/allowed.skip.ts:1');
        expect(autoSyncCount()).toBeGreaterThan(priorSyncs);
      });
      facts.preserved = {
        edited: await found('joinedNativeFact', 'hidden/source/nested/keep.ts:1'),
        dependency: await found('dependencyFact', 'hidden/deps/pkg/dep.ts:1'),
        unrelated: await found('unrelatedFact', 'ordinary.ts:1'),
      };
      expect(client.stderr).not.toContain('Auto-sync error:');
      phase = 'owned native cleanup';
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (name: string, action: () => unknown | Promise<unknown>) => {
        try { await action(); cleanup.push({ phase: name, error: null }); }
        catch (error) { cleanup.push({ phase: name, error: errorText(error) }); }
      };
      if (client) {
        await attempt('MCP EOF and graceful native worker/watcher drain', () => client!.close());
        if (!client.capture().nativeClose) await attempt('terminate only the owned direct child after failed drain', async () => {
          client!.child.kill();
          await eventually('owned direct child native close', () => expect(client!.capture().nativeClose).not.toBeNull(), 5000);
        });
        facts.client = client.capture();
      }
      if (fixture) await attempt('no detached candidate remains', () => settleLosingCandidates(fixture!.root, () => null, 5000));
      await attempt('retain bounded worker observations', () => { facts.workers = events(); });
      if ((!client || client.capture().nativeClose) && cleanup.every(row => row.error === null)) {
        await attempt('remove only the owned short temp fixture after native close', async () => {
          const relative = path.relative(fs.realpathSync(os.tmpdir()), fs.realpathSync(base));
          if (!relative || path.isAbsolute(relative) || relative.split(path.sep).includes('..') || !path.basename(base).startsWith('cg-i7j-')) {
            throw new Error(`Refusing fixture removal outside the owned temp root: ${base}`);
          }
          if (fixture) removeSpawnLog(fixture.root);
          await fs.promises.rm(base, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
          expect(fs.existsSync(base)).toBe(false); fixtureRemoved = true;
        });
      }
      const evidence = { ...facts, phase, primary: primary ? errorText(primary) : null, cleanup, fixtureRemoved,
        outcome: primary || cleanup.some(row => row.error) ? 'fail' : 'pass' };
      const evidenceDirectory = process.env.T21_JOINED_EVIDENCE_DIR;
      if (evidenceDirectory) {
        const evidencePath = path.join(evidenceDirectory, `${path.basename(base)}.json`);
        fs.mkdirSync(evidenceDirectory, { recursive: true });
        fs.writeFileSync(evidencePath, JSON.stringify(evidence, null, 2));
      }
      if (!primary && cleanup.some(row => row.error)) throw new Error(`Joined MCP/native cleanup failed: ${JSON.stringify(cleanup)}`);
    }
  }, 150000);
});
