import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { createHash } from 'crypto';
import { spawn, type ChildProcessWithoutNullStreams } from 'child_process';
import { pathToFileURL } from 'url';
import { CodeGraph, getDatabasePath } from '../src';
import { createDatabase } from '../src/db/sqlite-adapter';
import { getWriterPidPath, tryAcquireWriterLock, releaseWriterLock } from '../src/mcp/writer-lock';
import { WASM_RUNTIME_FLAGS } from '../src/extraction/wasm-runtime-flags';
import { recordSpawns, removeSpawnLog, settleLosingCandidates } from './daemon-candidates';

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean, ms: number, detail: () => string = () => '') {
  const end = Date.now() + ms;
  while (!check()) { if (Date.now() > end) throw new Error(`Owned unlimited proof deadline: ${detail()}`); await wait(20); }
}
const digest = (file: string) => fs.existsSync(file) ? createHash('sha256').update(fs.readFileSync(file)).digest('hex') : null;
const alive = (pid: number) => { try { process.kill(pid, 0); return true; } catch { return false; } };

class Client {
  child: ChildProcessWithoutNullStreams;
  replies: any[] = [];
  stderr = '';
  private id = 0;
  private pending = new Map<number, { resolve: (value: any) => void; reject: (error: Error) => void; timer: NodeJS.Timeout }>();
  constructor(root: string, mode: string, observations: string, release: string) {
    const recorder = recordSpawns(root);
    this.child = spawn(process.execPath, [...WASM_RUNTIME_FLAGS, ...recorder.args, '--require', path.join(__dirname, 'fixtures/query-unlimited-preload.cjs'),
      path.resolve(__dirname, '../dist/bin/codegraph.js'), 'serve', '--mcp', '--path', root], {
      cwd: root, windowsHide: true, stdio: ['pipe', 'pipe', 'pipe'], env: { ...process.env, ...recorder.env,
        CODEGRAPH_NO_DAEMON: mode === 'direct' ? '1' : '0', CODEGRAPH_QUERY_MODE: 'default',
        CODEGRAPH_QUERY_POOL_SIZE: 'unlimited', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: '',
        CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '1000', CODEGRAPH_DAEMON_INSTALL_CHECK_MS: '0',
        CODEGRAPH_DAEMON_RETRY_MS: '0', CODEGRAPH_MCP_LOG_ATTACH: '1', CODEGRAPH_NO_WATCHDOG: '1',
        CODEGRAPH_STARTUP_HANDSHAKE_TIMEOUT_MS: '0', CODEGRAPH_NO_RELAUNCH: '1', CODEGRAPH_WASM_RELAUNCHED: '1', CODEGRAPH_PPID_POLL_MS: '0',
        QUERY_UNLIMITED_OBSERVATIONS: observations, QUERY_UNLIMITED_RELEASE: release,
        QUERY_UNLIMITED_FAIL_DAEMON: mode === 'writable-fallback' ? '1' : '0',
      },
    });
    let buffer = '';
    this.child.stdout.on('data', data => {
      buffer += data;
      if (buffer.length > 1024 * 1024) return this.fail(new Error('bounded client stdout overflow'));
      let end: number;
      while ((end = buffer.indexOf('\n')) >= 0) {
        const line = buffer.slice(0, end); buffer = buffer.slice(end + 1);
        let reply: any; try { reply = JSON.parse(line); } catch { continue; }
        this.replies.push(reply);
        const current = this.pending.get(reply.id);
        if (current) { this.pending.delete(reply.id); clearTimeout(current.timer); reply.error ? current.reject(new Error(JSON.stringify(reply.error))) : current.resolve(reply.result); }
      }
    });
    this.child.stderr.on('data', data => { this.stderr += data; if (this.stderr.length > 1024 * 1024) this.fail(new Error('bounded client stderr overflow')); });
    this.child.on('error', error => this.fail(error));
    this.child.stdin.on('error', error => this.fail(error));
    this.child.on('exit', () => this.fail(new Error('owned client exited')));
  }
  fail(error: Error) { for (const current of this.pending.values()) { clearTimeout(current.timer); current.reject(error); } this.pending.clear(); }
  request(method: string, params: unknown = {}): Promise<any> {
    const id = ++this.id;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`independent client deadline 45000ms: ${this.stderr}`)); }, 45000);
      this.pending.set(id, { resolve, reject, timer });
      this.child.stdin.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
    });
  }
  query(query: string, projectPath?: string) { return this.request('tools/call', { name: 'codegraph_search', arguments: { query, ...(projectPath ? { projectPath } : {}) } }); }
  async close() {
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    this.child.stdin.end();
    await until(() => this.child.exitCode !== null || this.child.signalCode !== null, 35000, () => this.stderr);
    expect(this.child.exitCode, this.stderr).toBe(0);
  }
}

describe('independent unlimited concurrency through actual MCP', () => {
  it.each(['direct', 'writable-fallback', 'read-only-fallback', 'shared-daemon'] as const)
  ('exceeds 16 real workers, retires with live polling and recreates safely in %s', async mode => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-unlimited-mcp-')));
    const roots = [path.join(fixture, 'alpha'), path.join(fixture, 'beta')];
    const observations = path.join(fixture, 'workers.jsonl');
    const release = path.join(fixture, 'release');
    const clients: Client[] = [];
    const owners: CodeGraph[] = [];
    let observer: ReturnType<typeof createDatabase>['db'] | undefined;
    let daemonPid: number | undefined;
    let disappearingSession: any;
    const routeProbes: any[] = [];
    const events = (): any[] => fs.existsSync(observations) ? fs.readFileSync(observations, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)) : [];
    const activeWorkers = () => events().filter(event => event.event === 'spawn').length - events().filter(event => event.event === 'exit').length;
    const resultText = (result: any, symbol: string) => { expect(result.isError).toBeFalsy(); expect(result.content[0].text).toContain(symbol); expect(result.content[0].text).toContain('app.ts:1'); };
    try {
      for (const [index, root] of roots.entries()) {
        fs.mkdirSync(root);
        fs.writeFileSync(path.join(root, 'app.ts'), `export function overlapSymbol() { return 42; }\nexport function ${index ? 'beta' : 'alpha'}RouteSymbol() { return ${index}; }\n`);
        const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      }
      if (mode === 'read-only-fallback') {
        for (const root of roots) {
          const owner = await CodeGraph.open(root); owners.push(owner);
          expect(tryAcquireWriterLock(root, 'direct').kind).toBe('acquired'); owner.watch({ debounceMs: 50 });
        }
        observer = createDatabase(getDatabasePath(roots[0]), { readOnly: true }).db;
      }
      const locks = roots.map(root => fs.existsSync(getWriterPidPath(root)) ? fs.readFileSync(getWriterPidPath(root), 'utf8') : null);
      const readonlyBefore = observer ? { version: observer.pragma('data_version', { simple: true }),
        db: roots.map(root => digest(getDatabasePath(root))), wal: roots.map(root => digest(getDatabasePath(root) + '-wal')),
        metadata: roots.map(root => digest(path.join(root, '.codegraph/.gitignore'))), schema: observer.pragma('schema_version', { simple: true }) } : null;
      const client = new Client(roots[0], mode, observations, release); clients.push(client);
      const initialize = () => client.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'unlimited-proof', version: '1' }, rootUri: pathToFileURL(roots[0]).href });
      expect((await initialize()).serverInfo.name).toBe('codegraph');
      const probeRoutes = async (stage: 'before' | 'after') => {
        for (const index of [0, 1, 0]) {
          const query = `${index ? 'beta' : 'alpha'}RouteSymbol`;
          const projectPath = index ? roots[index] : undefined;
          const search = await client.query(query, projectPath);
          expect(search.isError).toBeFalsy();
          expect(search.content[0].text).toContain(query); expect(search.content[0].text).toContain('app.ts:2');
          expect(search.content[0].text).not.toContain(`${index ? 'alpha' : 'beta'}RouteSymbol`);
          const explore = await client.request('tools/call', { name: 'codegraph_explore', arguments: { query,
            ...(projectPath ? { projectPath } : {}) } });
          expect(explore.isError).toBeFalsy(); expect(explore.content[0].text).toContain('app.ts');
          expect(explore.content[0].text).not.toContain(`${index ? 'alpha' : 'beta'}RouteSymbol`);
          routeProbes.push({ stage, route: index ? 'explicit' : 'default', root: roots[index], query, search, explore });
        }
      };
      await probeRoutes('before');
      const wave = Array.from({ length: 20 }, (_, i) => client.query('overlapSymbol', i % 2 ? roots[1] : undefined));
      // Catch all pending outcomes from the moment they are created.
      const resultsPromise = Promise.all(wave);
      void resultsPromise.catch(() => {}); // retained for the awaited assertion/cleanup, never an unobserved rejection
      await until(() => events().filter(event => event.event === 'call-start' && event.query === 'overlapSymbol').length === 20, 30000,
        () => `${activeWorkers()} workers; ${client.stderr}`);
      const overlapping = events().filter(event => event.event === 'call-start' && event.query === 'overlapSymbol');
      expect(new Set(overlapping.map(event => `${event.pid}:${event.threadId}`)).size).toBe(20);
      fs.writeFileSync(release, 'release controlled delivery to ordinary SQLite handler');
      const results = await resultsPromise;
      for (const result of results) resultText(result, 'overlapSymbol');
      expect(client.replies.filter(reply => reply.result?.content)).toHaveLength(26);
      if (mode === 'shared-daemon') {
        daemonPid = JSON.parse(fs.readFileSync(path.join(roots[0], '.codegraph/daemon.pid'), 'utf8')).pid;
        const peer = new Client(roots[0], mode, observations, release); clients.push(peer);
        await peer.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'peer', version: '1' } });
        resultText(await peer.query('overlapSymbol'), 'overlapSymbol');
        expect(new Set(events().filter(event => event.event === 'spawn').map(event => event.pid)).size).toBe(1);
        await peer.close();
        resultText(await client.query('overlapSymbol'), 'overlapSymbol');
      }
      const firstWave = events();
      const workerPids = new Set(firstWave.filter(event => event.event === 'spawn').map(event => event.pid));
      expect(workerPids.size).toBe(1);
      expect(firstWave.filter(event => event.event === 'spawn')).toHaveLength(20);
      for (const event of firstWave.filter(event => event.event === 'spawn')) {
        expect(event.execution.policy).toMatchObject({ executionMode: mode, preset: 'default',
          concurrency: { kind: 'unlimited' }, deadline: { kind: 'finite', milliseconds: 45000 }, retention: 'retire-idle', classification: 'custom' });
        if (mode === 'read-only-fallback') expect(event.execution).toMatchObject({ capability: 'read-only', ownership: { writer: false, synchronization: false } });
      }
      let starting = 0, peakStarting = 0;
      for (const event of firstWave) {
        if (event.event === 'spawn') peakStarting = Math.max(peakStarting, ++starting);
        if (event.event === 'ready') starting--;
      }
      expect(peakStarting).toBeLessThanOrEqual(2);
      if (mode === 'writable-fallback') {
        expect(firstWave.some(event => event.event === 'unavailable-candidate')).toBe(true);
        expect(firstWave.filter(event => event.event === 'unavailable-candidate-exit').every(event => event.code === 78)).toBe(true);
        expect(client.stderr).toContain('Shared daemon unavailable');
        expect(client.stderr).not.toMatch(/version.*differs|Attached to shared daemon/);
      }
      if (mode === 'read-only-fallback') expect(client.stderr).toContain('local read-only fallback');
      const idleStarted = process.hrtime.bigint();
      let polls = 0;
      while (activeWorkers() && Number(process.hrtime.bigint() - idleStarted) / 1e6 < 45000) {
        await client.request('ping');
        const status = await client.request('tools/call', { name: 'codegraph_status', arguments: {} });
        expect(status.isError).toBeFalsy(); polls++;
        await wait(500);
      }
      expect(activeWorkers()).toBe(0);
      expect(polls).toBeGreaterThan(0);
      const idleObservedMs = Number(process.hrtime.bigint() - idleStarted) / 1e6;
      const idleEvents = events();
      for (const exit of idleEvents.filter(event => event.event === 'exit')) {
        const lastResult = idleEvents.filter(event => event.event === 'result' && event.pid === exit.pid && event.threadId === exit.threadId).at(-1);
        expect(Number(BigInt(exit.monotonicNs) - BigInt(lastResult.monotonicNs)) / 1e6).toBeGreaterThanOrEqual(30000);
      }
      expect(idleEvents.filter(event => event.event === 'exit')).toHaveLength(20);
      // Status/heartbeats did not keep workers alive, and remain available at zero.
      await client.request('ping');
      if (observer) {
        expect({ version: observer.pragma('data_version', { simple: true }), db: roots.map(root => digest(getDatabasePath(root))),
          wal: roots.map(root => digest(getDatabasePath(root) + '-wal')), metadata: roots.map(root => digest(path.join(root, '.codegraph/.gitignore'))),
          schema: observer.pragma('schema_version', { simple: true }) }).toEqual(readonlyBefore);
        for (const [i, owner] of owners.entries()) { expect(owner.isWatching()).toBe(true); expect(fs.readFileSync(getWriterPidPath(roots[i]), 'utf8')).toBe(locks[i]); }
      }
      if (mode === 'shared-daemon') {
        // Recreate from zero with concurrent genuine demand. A departed chat's
        // occupied worker must not terminate or suppress the surviving chat.
        fs.rmSync(release);
        const beforeCalls = events().filter(event => event.event === 'call-start').length;
        const departed = new Client(roots[0], mode, observations, release); clients.push(departed);
        await departed.request('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'departing-peer', version: '1' } });
        const victim = departed.query('overlapSymbol').then(value => ({ value, error: undefined }), error => ({ value: undefined, error: String(error) }));
        const survivors = Promise.all(Array.from({ length: 20 }, (_, i) => client.query('overlapSymbol', i % 2 ? roots[1] : undefined)));
        void survivors.catch(() => {});
        await until(() => events().filter(event => event.event === 'call-start').length === beforeCalls + 21, 30000, () => client.stderr);
        await departed.close();
        fs.writeFileSync(release, 'survivor demand continues after the other socket closes');
        const survivorResults = await survivors;
        for (const result of survivorResults) resultText(result, 'overlapSymbol');
        const abandoned = await victim;
        expect(abandoned.value).toBeUndefined(); expect(abandoned.error).toContain('owned client exited');
        expect(departed.replies.filter(reply => reply.result?.content)).toHaveLength(0);
        disappearingSession = { departedPid: departed.child.pid, departedQueryReplies: 0, survivorResults, overlapAfterZero: 21 };
      }
      await probeRoutes('after');
      fs.writeFileSync(path.join(roots[0], 'live.ts'), 'export function afterIdleNativeSymbol() { return 3; }\n');
      if (owners.length) await until(() => owners[0].searchNodes('afterIdleNativeSymbol').length === 1, 10000);
      let fresh: any;
      const nativeUntil = Date.now() + 10000;
      do { fresh = await client.query('afterIdleNativeSymbol'); if (fresh.content[0].text.includes('live.ts:1')) break; await wait(50); } while (Date.now() < nativeUntil);
      expect(fresh.content[0].text).toContain('live.ts:1');
      resultText(await client.query('overlapSymbol', roots[1]), 'overlapSymbol');
      const recreated = events().filter(event => event.event === 'spawn').slice(20);
      expect(recreated).toHaveLength(mode === 'shared-daemon' ? 21 : 1);
      expect(events().filter(event => event.event === 'result').length).toBeGreaterThan(20);
      await client.close();
      if (daemonPid) await until(() => !alive(daemonPid!), 10000);
      await settleLosingCandidates(roots[0], () => process.pid);
      expect(activeWorkers()).toBe(0);
      const final = events();
      let startingAll = 0, peakStartingAll = 0;
      for (const event of final) {
        if (event.event === 'spawn') peakStartingAll = Math.max(peakStartingAll, ++startingAll);
        if (event.event === 'ready') startingAll--;
      }
      expect(peakStartingAll).toBeLessThanOrEqual(2); expect(startingAll).toBe(0);
      const history = final.filter(event => event.event === 'session-history');
      expect(history).toHaveLength(6);
      expect(new Set(history.map(event => event.historyId)).size).toBe(1);
      expect(history.map(event => event.projectRoot)).toEqual([roots[0], roots[1], roots[0], roots[0], roots[1], roots[0]]);
      expect(history.map(event => event.index)).toEqual([1, 1, 2, 3, 2, 4]);
      const routedStarts = final.filter(event => event.event === 'call-start' && /^(alpha|beta)RouteSymbol$/.test(event.query));
      expect(routedStarts).toHaveLength(12);
      for (const event of routedStarts) {
        const index = event.query === 'betaRouteSymbol' ? 1 : 0;
        expect(event.context.project).toEqual({ route: index ? 'explicit' : 'default', root: roots[index], index: getDatabasePath(roots[index]) });
      }
      expect(final.filter(event => event.event === 'exit')).toHaveLength(final.filter(event => event.event === 'spawn').length);
      if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, `unlimited-${mode}.json`), JSON.stringify({
        mode, roots, releaseControl: 'owned file gates actual worker input; real built SQLite handler produces output', demand: 20,
        peakOverlap: 20, peakStarting, peakStartingAll, routeProbes, idleGraceMs: 30000, idleObservationBoundMs: 45000, polls,
        idleObservedMs, readonlyBefore, disappearingSession,
        events: final, results, replies: clients.map(client => client.replies), stderr: clients.map(client => client.stderr),
      }, null, 2));
    } finally {
      if (!fs.existsSync(release)) fs.writeFileSync(release, 'cleanup release');
      await Promise.all(clients.map(client => client.close()));
      if (daemonPid && alive(daemonPid)) await until(() => !alive(daemonPid!), 10000);
      await settleLosingCandidates(roots[0], () => process.pid);
      if (process.env.VALIDATION_OUT) fs.writeFileSync(path.join(process.env.VALIDATION_OUT, `unlimited-${mode}-cleanup.json`), JSON.stringify({
        mode, roots, events: events(), replies: clients.map(client => client.replies), stderr: clients.map(client => client.stderr),
        clients: clients.map(client => ({ pid: client.child.pid, exitCode: client.child.exitCode, signalCode: client.child.signalCode })),
        daemonPid, daemonAlive: daemonPid ? alive(daemonPid) : false,
      }, null, 2));
      removeSpawnLog(roots[0]); observer?.close();
      for (const [i, owner] of owners.entries()) { owner.close(); releaseWriterLock(roots[i]); }
      await fs.promises.rm(fixture, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
    }
  }, 120000);
});
