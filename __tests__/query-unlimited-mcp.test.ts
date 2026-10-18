import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import * as net from 'net';
import { CodeGraph } from '../src';
import { getWriterPidPath } from '../src/mcp/writer-lock';
import { parseQueryDiagnostics as diagnostic } from './query-diagnostics-helper';
import { removeSpawnLog, settleLosingCandidates, spawnLogFor } from './daemon-candidates';
import { CompatibilityClient, alive as compatibilityAlive, saveCompatibilityEvidence } from './query-compatibility-host';
import { queryUnlimitedCases, runUnlimitedMCPCase } from './query-unlimited-mcp-fixture';

const wait = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(check: () => boolean, ms: number, detail: () => string = () => '') {
  const end = Date.now() + ms;
  while (!check()) { if (Date.now() > end) throw new Error(`Owned unlimited proof deadline: ${detail()}`); await wait(20); }
}

describe('independent unlimited concurrency through actual MCP', () => {
  it.runIf(process.platform === 'win32')('keeps Windows case-alias queries usable across two live MCP engines and the first engine exit', async () => {
    const { MCPEngine }: typeof import('../src/mcp/engine') = require('../dist/mcp/engine');
    const { MCPSession }: typeof import('../src/mcp/session') = require('../dist/mcp/session');
    const { SocketTransport }: typeof import('../src/mcp/transport') = require('../dist/mcp/transport');
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-query-case-alias-')));
    const root = path.join(fixture, 'Repo'), alias = path.join(fixture, 'REPO');
    const engines: Array<InstanceType<typeof MCPEngine>> = [];
    const sessions: Array<InstanceType<typeof MCPSession>> = [];
    const endpoints: Array<{ name: string; server: net.Server; replies: any[]; issued: number[];
      buffer: string; seenBytes: number; serverClosed: boolean }> = [];
    const sockets: Array<{ socket: net.Socket; name: string; closed: boolean; ended: boolean; hadError: boolean | null }> = [];
    const errors: string[] = [];
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    const evidence: Record<string, unknown> = { fixture, root, alias,
      construction: { rootless: true, readOnly: true, watch: false, queryPool: false, hostPid: process.pid } };
    let primary: unknown, phase = 'fixture setup', fixtureRemoved = false;
    const bounded = async <T,>(work: Promise<T>, label: string): Promise<T> => {
      let timer: NodeJS.Timeout | undefined;
      try { return await Promise.race([work, new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`Owned case-alias ${label} deadline`)), 10000);
      })]); } finally { if (timer) clearTimeout(timer); }
    };
    const ownSocket = (socket: net.Socket, name: string) => {
      const owned = { socket, name, closed: false, ended: false, hadError: null as boolean | null };
      sockets.push(owned);
      socket.on('error', error => errors.push(`${name}: ${String(error)}`));
      socket.once('end', () => { owned.ended = true; });
      socket.once('close', hadError => { owned.closed = true; owned.hadError = hadError; });
    };
    const endpoint = async (name: string) => {
      // Both built engines resolve through the same production lifecycle module.
      // No default project is selected: each real MCP call supplies its own path.
      const engine = new MCPEngine({ readOnly: true, watch: false, queryPool: false });
      engines.push(engine);
      let session: InstanceType<typeof MCPSession> | undefined;
      const server = net.createServer(socket => {
        ownSocket(socket, `${name}:server`);
        session = new MCPSession(new SocketTransport(socket), engine);
        sessions.push(session); session.start();
      });
      const row = { name, server, replies: [] as any[], issued: [] as number[], buffer: '', seenBytes: 0, serverClosed: false };
      endpoints.push(row);
      server.on('error', error => errors.push(`${name}:server: ${String(error)}`));
      server.once('close', () => { row.serverClosed = true; });
      await bounded(new Promise<void>((resolve, reject) => {
        server.once('error', reject); server.listen(0, '127.0.0.1', () => { server.removeListener('error', reject); resolve(); });
      }), `${name} listen`);
      const client = net.createConnection((server.address() as net.AddressInfo).port, '127.0.0.1');
      ownSocket(client, `${name}:client`);
      client.setEncoding('utf8');
      client.on('data', (chunk: string) => {
        row.seenBytes += Buffer.byteLength(chunk);
        if (row.seenBytes > 1024 * 1024) { errors.push(`${name}: reply capture overflow`); client.destroy(); return; }
        row.buffer += chunk;
        let end: number;
        while ((end = row.buffer.indexOf('\n')) >= 0) {
          const line = row.buffer.slice(0, end); row.buffer = row.buffer.slice(end + 1);
          try {
            const reply = JSON.parse(line);
            if (reply.jsonrpc !== '2.0' || 'method' in reply || !row.issued.includes(reply.id)
              || ('result' in reply) === ('error' in reply) || row.replies.length >= 16) throw new Error('Invalid owned MCP reply');
            row.replies.push(reply);
          } catch (error) { errors.push(`${name}: ${String(error)}`); }
        }
      });
      const request = async (method: string, params: unknown) => {
        const id = row.issued.length + 1; row.issued.push(id);
        client.write(JSON.stringify({ jsonrpc: '2.0', id, method, params }) + '\n');
        await until(() => errors.length > 0 || row.replies.some(reply => reply.id === id), 10000,
          () => JSON.stringify({ name, method, errors, replies: row.replies }));
        expect(errors).toEqual([]);
        return row.replies.find(reply => reply.id === id)!;
      };
      const initialized = await request('initialize', { protocolVersion: '2024-11-05', capabilities: {},
        clientInfo: { name, version: '1' } });
      expect(initialized.error).toBeUndefined();
      expect(initialized.result.serverInfo).toBeDefined();
      client.write(JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) + '\n');
      return { engine, request, stopSession: () => session?.stop() };
    };
    const expectedSearch = (reply: any, symbol: string, line: number) => {
      expect(reply.error, JSON.stringify(reply)).toBeUndefined();
      expect(reply.result.isError).toBeFalsy();
      expect(reply.result.content[0].text).toContain(symbol);
      expect(reply.result.content[0].text).toContain(`app.ts:${line}`);
    };
    try {
      fs.mkdirSync(root);
      fs.writeFileSync(path.join(root, 'app.ts'),
        'export function aliasFirst() { return 701; }\nexport function aliasSurvivor() { return 907; }\n');
      const graph = await CodeGraph.init(root);
      try { await graph.indexAll(); } finally { graph.close(); }
      const first = await endpoint('case-alias-first'), second = await endpoint('case-alias-second');
      phase = 'first session holds the indexed project';
      const original = await first.request('tools/call', { name: 'codegraph_search', arguments: { query: 'aliasFirst', projectPath: root } });
      evidence.original = original; expectedSearch(original, 'aliasFirst', 1);
      phase = 'second live session queries the same Windows directory under different case';
      const joined = await second.request('tools/call', { name: 'codegraph_search', arguments: { query: 'aliasFirst', projectPath: alias } });
      evidence.joined = joined; expectedSearch(joined, 'aliasFirst', 1);
      phase = 'second session remains useful after the first engine exits';
      first.stopSession(); await bounded(first.engine.stop(), 'first engine stop');
      evidence.firstEngineStopped = true;
      const surviving = await second.request('tools/call', { name: 'codegraph_search', arguments: { query: 'aliasSurvivor', projectPath: alias } });
      evidence.surviving = surviving; expectedSearch(surviving, 'aliasSurvivor', 2);
      phase = 'owned native cleanup';
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (name: string, action: () => unknown | Promise<unknown>) => {
        try { await action(); cleanup.push({ phase: name, error: null }); }
        catch (error) { cleanup.push({ phase: name, error: error instanceof Error ? error.stack ?? error.message : String(error) }); }
      };
      for (const [index, session] of sessions.entries()) await attempt(`session ${index} stop`, () => session.stop());
      await Promise.all(engines.map((engine, index) => attempt(`engine ${index} drain`, () => bounded(engine.stop(), `engine ${index} drain`))));
      await Promise.all(endpoints.map(row => attempt(`${row.name} server close`, async () => {
        if (row.server.listening) await bounded(new Promise<void>((resolve, reject) => row.server.close(error => error ? reject(error) : resolve())), `${row.name} close`);
      })));
      await attempt('all native sockets closed and complete MCP replies retained', async () => {
        for (const owned of sockets) owned.socket.destroy();
        await until(() => sockets.every(owned => owned.closed), 10000);
        expect(errors).toEqual([]);
        expect(sockets.every(owned => owned.hadError === false)).toBe(true);
        expect(endpoints.every(row => row.serverClosed && !row.buffer.length)).toBe(true);
        for (const row of endpoints) for (const id of row.issued) expect(row.replies.filter(reply => reply.id === id)).toHaveLength(1);
      });
      if (cleanup.every(row => row.error === null)) await attempt('owned fixture removal after both engine drains', async () => {
        await fs.promises.rm(fixture, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
        expect(fs.existsSync(fixture)).toBe(false); fixtureRemoved = true;
      });
      saveCompatibilityEvidence('case-alias-shared-project', { ...evidence, phase,
        primary: primary instanceof Error ? primary.stack : String(primary ?? ''),
        outcome: primary || cleanup.some(row => row.error) ? 'fail' : 'pass', cleanup, fixtureRemoved, errors,
        sockets: sockets.map(({ socket: _socket, ...owned }) => owned),
        endpoints: endpoints.map(({ server: _server, ...row }) => row) });
      const failures = cleanup.filter(row => row.error);
      if (failures.length && !primary) throw new Error('Owned case-alias cleanup incomplete: ' + JSON.stringify(failures));
      if (failures.length && primary) process.stderr.write('Owned case-alias cleanup also failed: ' + JSON.stringify(failures) + '\n');
    }
  }, 90000);

  it.runIf(process.platform === 'win32')('serves three compatible cold callers while their shared service is still starting', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-query-concurrent-startup-')));
    const root = path.join(fixture, 'project');
    const observations = path.join(fixture, 'workers.jsonl');
    const startupObservations = path.join(fixture, 'startup.jsonl');
    const release = path.join(fixture, 'release');
    const clients: CompatibilityClient[] = [];
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    const evidence: Record<string, unknown> = { fixture, root };
    const daemonPids = new Set<number>();
    let candidatePids: number[] = [];
    let primary: unknown;
    let phase = 'fixture setup';
    let fixtureRemoved = false;
    const readEvents = (file: string): any[] => fs.existsSync(file)
      ? fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)) : [];
    const launch = (name: string, policy: NodeJS.ProcessEnv) => {
      const client = new CompatibilityClient(root, name, {
        QUERY_UNLIMITED_OBSERVATIONS: observations, QUERY_UNLIMITED_RELEASE: release, QUERY_UNLIMITED_FAIL_DAEMON: '0',
        QUERY_STARTUP_OBSERVATIONS: startupObservations, QUERY_STARTUP_RELEASE: release,
        CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited', ...policy,
      }, 30000, [path.join(__dirname, 'fixtures/query-unlimited-preload.cjs'),
        path.join(__dirname, 'fixtures/query-startup-listen-gate.cjs')]);
      clients.push(client); return client;
    };
    const query = async (client: CompatibilityClient, symbol: string) => {
      await client.initialize();
      return client.tool('codegraph_search', { query: symbol });
    };
    const queries: Array<Promise<PromiseSettledResult<any>>> = [];
    // Retain each outcome immediately: the intended RED is a caller-visible
    // refusal, not an unhandled rejection or a failed native-timing assertion.
    const capture = (request: Promise<any>) => {
      const result = request.then(value => ({ status: 'fulfilled' as const, value }),
        reason => ({ status: 'rejected' as const, reason: String(reason) }));
      queries.push(result);
    };
    try {
      fs.mkdirSync(root);
      fs.writeFileSync(path.join(root, 'app.ts'), [
        'export function coldCallerAlpha() { return 701; }',
        'export function coldCallerBeta() { return 907; }',
        'export function coldCallerGamma() { return 1103; }', '',
      ].join('\n'));
      const graph = await CodeGraph.init(root);
      try { await graph.indexAll(); } finally { graph.close(); }

      phase = 'first cold caller starts the ordinary service';
      const owner = launch('cold-owner', { CODEGRAPH_QUERY_MODE: 'default', CODEGRAPH_QUERY_POOL_SIZE: '1' });
      capture(query(owner, 'coldCallerAlpha'));
      await until(() => readEvents(startupObservations).some(event => event.event === 'listen-held'), 15000,
        () => JSON.stringify(clients.map(client => client.facts())));
      // Hold at the native listen boundary, after real owner publication. This
      // deterministically exercises the election-to-listening window rather
      // than hoping three process startups happen to overlap on a busy host.
      phase = 'two compatible cold peers reach the not-yet-listening service';
      const peers = [launch('cold-peer-zero-padded', { CODEGRAPH_QUERY_POOL_SIZE: '01' }),
        launch('cold-peer-trimmed', { CODEGRAPH_QUERY_POOL_SIZE: ' 1 ', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: ' unlimited ' })];
      capture(query(peers[0], 'coldCallerBeta'));
      capture(query(peers[1], 'coldCallerGamma'));
      await until(() => peers.every(client => readEvents(startupObservations)
        .some(event => event.event === 'connect-unavailable' && event.pid === client.child.pid)), 10000,
      () => JSON.stringify({ startup: readEvents(startupObservations), clients: clients.map(client => client.facts()) }));
      fs.writeFileSync(release, 'allow the genuine service to listen');

      phase = 'all three callers receive independently expected SQLite results';
      const results = await Promise.all(queries);
      evidence.results = results;
      for (const [index, symbol] of ['coldCallerAlpha', 'coldCallerBeta', 'coldCallerGamma'].entries()) {
        const outcome = results[index];
        expect(outcome.status, JSON.stringify(outcome)).toBe('fulfilled');
        if (outcome.status !== 'fulfilled') throw new Error('Cold startup caller did not complete');
        const reply = outcome.value;
        expect(reply.error, JSON.stringify({ symbol, reply, stderr: clients[index].stderr })).toBeUndefined();
        expect(reply.result.isError).toBeFalsy();
        expect(reply.result.content[0].text).toContain(symbol);
        expect(reply.result.content[0].text).toContain(`app.ts:${index + 1}`);
      }
      const statuses = await Promise.all(clients.map(async client => {
        const reply = await client.tool('codegraph_status');
        expect(reply.error, client.stderr).toBeUndefined();
        return diagnostic(reply.result);
      }));
      evidence.statuses = statuses;
      for (const status of statuses) {
        expect(status.effective).toMatchObject({ executionMode: 'shared-daemon',
          concurrency: { kind: 'finite', maximum: 1 }, deadline: { kind: 'absent' } });
        expect(status.ownership).toEqual(statuses[0].ownership);
        expect(status.ownership).toMatchObject({ resourceScope: 'shared-project', capability: 'read-write' });
        expect(status.lifecycle).toMatchObject({ state: 'active', dispatch: 'worker', ready: true, workers: 1 });
        daemonPids.add(status.ownership.pid);
      }
      const workers = readEvents(observations).filter(event => event.event === 'spawn');
      expect(workers).toHaveLength(1);
      expect(workers[0].pid).toBe(statuses[0].ownership.pid);
      expect(clients.every(client => client.child.pid !== statuses[0].ownership.pid)).toBe(true);
      phase = 'owned native cleanup';
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (name: string, action: () => unknown | Promise<unknown>) => {
        try { await action(); cleanup.push({ phase: name, error: null }); }
        catch (error) { cleanup.push({ phase: name, error: error instanceof Error ? error.stack ?? error.message : String(error) }); }
      };
      await attempt('release owned native-listen gate', () => fs.writeFileSync(release, 'cleanup release'));
      await attempt('capture daemon and candidate ownership', () => {
        for (const event of readEvents(startupObservations)) if (event.event === 'listen-held') daemonPids.add(event.pid);
        const pidFile = path.join(root, '.codegraph/daemon.pid');
        if (fs.existsSync(pidFile)) daemonPids.add(JSON.parse(fs.readFileSync(pidFile, 'utf8')).pid);
        if (fs.existsSync(spawnLogFor(root))) candidatePids = fs.readFileSync(spawnLogFor(root), 'utf8').split(/\r?\n/)
          .map(Number).filter(pid => Number.isSafeInteger(pid) && pid > 0);
      });
      await attempt('settle caller requests', async () => { evidence.settledResults = await Promise.all(queries); });
      await attempt('losing candidates exit before closing the winner', () => settleLosingCandidates(root,
        () => [...daemonPids].find(compatibilityAlive), 15000));
      await Promise.all(clients.map(client => attempt(`client ${client.name} exit`, () => client.close())));
      await attempt('shared daemon natural exit', () => until(() => [...daemonPids].every(pid => !compatibilityAlive(pid)), 20000));
      await attempt('all recorded candidates exit', () => settleLosingCandidates(root, () => process.pid, 15000));
      let observed: any[] = [];
      let startup: any[] = [];
      await attempt('verify all owned native processes and workers exited', () => {
        observed = readEvents(observations); startup = readEvents(startupObservations);
        expect(clients.every(client => client.child.exitCode === 0 && client.child.signalCode === null)).toBe(true);
        expect([...daemonPids, ...candidatePids].filter(compatibilityAlive)).toEqual([]);
        const workerKey = (event: any) => `${event.pid}:${event.threadId}`;
        expect(observed.filter(event => event.event === 'exit').map(workerKey).sort())
          .toEqual(observed.filter(event => event.event === 'spawn').map(workerKey).sort());
        expect(startup.some(event => event.event === 'listen-gate-deadline')).toBe(false);
      });
      if (cleanup.every(row => row.error === null)) await attempt('owned fixture and spawn-log removal', async () => {
        removeSpawnLog(root);
        await fs.promises.rm(fixture, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
        expect(fs.existsSync(fixture)).toBe(false); fixtureRemoved = true;
      });
      saveCompatibilityEvidence('concurrent-startup-join', { ...evidence, phase,
        primary: primary instanceof Error ? primary.stack : String(primary ?? ''),
        outcome: primary || cleanup.some(row => row.error) ? 'fail' : 'pass', cleanup, fixtureRemoved,
        events: observed, startup, clients: clients.map(client => client.facts()), daemonPids: [...daemonPids], candidatePids });
      const failures = cleanup.filter(row => row.error);
      if (failures.length && !primary) throw new Error('Owned concurrent-startup cleanup incomplete: ' + JSON.stringify(failures));
      if (failures.length && primary) process.stderr.write('Owned concurrent-startup cleanup also failed: ' + JSON.stringify(failures) + '\n');
    }
  }, 120000);

  it.runIf(process.platform === 'win32')('starts a shared worker service, reuses equivalent sessions, refuses conflicts and executes the strict direct preset', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-query-startup-join-')));
    const roots = [path.join(fixture, 'shared'), path.join(fixture, 'direct')];
    const observations = path.join(fixture, 'workers.jsonl');
    const release = path.join(fixture, 'release');
    const clients: CompatibilityClient[] = [];
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    const evidence: Record<string, unknown> = { fixture, roots };
    const daemonPids = new Set<number>();
    let candidatePids: number[] = [];
    let primary: unknown;
    let fixtureRemoved = false;
    let phase = 'fixture setup';
    const events = (): any[] => fs.existsSync(observations)
      ? fs.readFileSync(observations, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)) : [];
    const launch = (root: string, name: string, env: NodeJS.ProcessEnv) => {
      const client = new CompatibilityClient(root, name, {
        QUERY_UNLIMITED_OBSERVATIONS: observations, QUERY_UNLIMITED_RELEASE: release,
        QUERY_UNLIMITED_FAIL_DAEMON: '0', ...env,
      }, 15000, [path.join(__dirname, 'fixtures/query-unlimited-preload.cjs')]);
      clients.push(client); return client;
    };
    const search = async (client: CompatibilityClient, symbol: string) => {
      const reply = await client.tool('codegraph_search', { query: symbol });
      expect(reply.error, client.stderr).toBeUndefined();
      expect(reply.result.isError).toBeFalsy();
      expect(reply.result.content[0].text).toContain(symbol);
      expect(reply.result.content[0].text).toContain('app.ts:1');
      return reply;
    };
    const status = async (client: CompatibilityClient) => {
      const reply = await client.tool('codegraph_status');
      expect(reply.error, client.stderr).toBeUndefined();
      return diagnostic(reply.result);
    };
    try {
      for (const [index, root] of roots.entries()) {
        fs.mkdirSync(root);
        fs.writeFileSync(path.join(root, 'app.ts'), `export function ${index ? 'directJoinSymbol' : 'sharedJoinSymbol'}() { return ${index ? 907 : 701}; }\n`);
        const graph = await CodeGraph.init(root);
        try { await graph.indexAll(); } finally { graph.close(); }
      }
      // No timing/synthetic-result shim: the ordinary worker's SQLite handler
      // receives the request immediately and must return the indexed symbol.
      fs.writeFileSync(release, 'startup join does not gate query delivery');
      phase = 'ordinary shared startup and SQLite response';
      const owner = launch(roots[0], 'join-owner', {
        CODEGRAPH_QUERY_MODE: 'default', CODEGRAPH_QUERY_POOL_SIZE: '1', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
      });
      await owner.initialize();
      evidence.initial = await search(owner, 'sharedJoinSymbol');
      const before = await status(owner);
      evidence.before = before;
      daemonPids.add(before.ownership.pid);
      expect(before.effective).toMatchObject({ executionMode: 'shared-daemon', concurrency: { kind: 'finite', maximum: 1 },
        deadline: { kind: 'absent' }, retention: 'ordinary' });
      expect(before.ownership).toMatchObject({ resourceScope: 'shared-project', capability: 'read-write', writerAuthority: true, synchronizationAuthority: true });
      expect(before.ownership.pid).not.toBe(owner.child.pid);
      expect(before.lifecycle).toMatchObject({ state: 'active', dispatch: 'worker', ready: true, workers: 1 });
      const daemonLock = fs.readFileSync(path.join(roots[0], '.codegraph/daemon.pid'), 'utf8');
      const writerLock = fs.readFileSync(getWriterPidPath(roots[0]), 'utf8');
      const candidatesBefore = fs.readFileSync(spawnLogFor(roots[0]), 'utf8');
      const sharedWorkersBefore = events().filter(event => event.event === 'spawn');
      expect(sharedWorkersBefore).toHaveLength(1);
      expect(sharedWorkersBefore[0].pid).toBe(before.ownership.pid);

      phase = 'behavior-equivalent session reuses the same worker service';
      const peer = launch(roots[0], 'join-peer', {
        CODEGRAPH_QUERY_POOL_SIZE: '01', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: ' unlimited ',
      });
      await peer.initialize();
      evidence.peerResult = await search(peer, 'sharedJoinSymbol');
      const matching = await status(peer);
      evidence.matching = matching;
      expect(matching.ownership).toEqual(before.ownership);
      expect(matching.effective.concurrency).toEqual(before.effective.concurrency);
      expect(matching.effective.deadline).toEqual(before.effective.deadline);
      expect(matching.effective.retention).toBe(before.effective.retention);
      expect(matching.lifecycle).toMatchObject({ state: 'active', workers: 1 });
      expect(events().filter(event => event.event === 'spawn')).toEqual(sharedWorkersBefore);

      phase = 'incompatible acquisition refuses without replacement or fallback';
      const conflict = launch(roots[0], 'join-conflict', {
        CODEGRAPH_QUERY_POOL_SIZE: '2', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
      });
      await conflict.initialize();
      const refusal = await conflict.tool('codegraph_search', { query: 'sharedJoinSymbol' });
      evidence.refusal = refusal;
      expect(refusal.error).toMatchObject({ code: -32603, data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason: 'incompatible', projectRoot: roots[0] } });
      expect(refusal.error.data.guidance).toContain(roots[0]);
      expect(refusal.result).toBeUndefined();
      expect(conflict.stderr).not.toMatch(/serving this session in-process|local read-only fallback|Shared daemon unavailable/);
      expect(fs.readFileSync(path.join(roots[0], '.codegraph/daemon.pid'), 'utf8')).toBe(daemonLock);
      expect(fs.readFileSync(getWriterPidPath(roots[0]), 'utf8')).toBe(writerLock);
      expect(fs.readFileSync(spawnLogFor(roots[0]), 'utf8')).toBe(candidatesBefore);
      expect(events().filter(event => event.event === 'spawn')).toEqual(sharedWorkersBefore);
      await conflict.close();

      phase = 'surviving session remains usable after the original session exits';
      await owner.close();
      evidence.survivingResult = await search(peer, 'sharedJoinSymbol');
      const surviving = await status(peer);
      evidence.surviving = surviving;
      expect(surviving.ownership).toEqual(before.ownership);
      expect(surviving.effective).toEqual(matching.effective);
      expect(surviving.lifecycle).toMatchObject({ state: 'active', workers: 1 });

      phase = 'ordinary direct startup uses the strict combined preset';
      const direct = launch(roots[1], 'join-direct-strict', {
        CODEGRAPH_NO_DAEMON: '1', CODEGRAPH_QUERY_MODE: 'unlimited',
      });
      await direct.initialize();
      evidence.directResult = await search(direct, 'directJoinSymbol');
      const strict = await status(direct);
      evidence.strict = strict;
      expect(strict.effective).toMatchObject({ source: 'strict', executionMode: 'direct', preset: 'unlimited', classification: 'unlimited',
        concurrency: { kind: 'unlimited' }, deadline: { kind: 'absent' }, retention: 'retire-idle' });
      expect(strict.implementation.support.combinedPreset).toBe(true);
      expect(strict.ownership).toMatchObject({ pid: direct.child.pid, resourceScope: 'local-engine', capability: 'read-write' });
      expect(strict.lifecycle).toMatchObject({ state: 'active', dispatch: 'worker', workers: 1 });
      const observed = events();
      const workers = observed.filter(event => event.event === 'spawn');
      expect(workers).toHaveLength(2);
      expect(new Set(workers.map(event => event.pid))).toEqual(new Set([before.ownership.pid, direct.child.pid]));
      expect(workers.find(event => event.pid === direct.child.pid).execution.policy).toMatchObject({ source: 'strict', preset: 'unlimited', deadline: { kind: 'absent' } });
      for (const worker of workers) {
        expect(observed.some(event => event.event === 'call-start' && event.pid === worker.pid && event.threadId === worker.threadId)).toBe(true);
        expect(observed.some(event => event.event === 'result' && event.pid === worker.pid && event.threadId === worker.threadId)).toBe(true);
      }
      phase = 'owned native cleanup';
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (name: string, action: () => unknown | Promise<unknown>) => {
        try { await action(); cleanup.push({ phase: name, error: null }); }
        catch (error) { cleanup.push({ phase: name, error: error instanceof Error ? error.stack ?? error.message : String(error) }); }
      };
      await attempt('capture daemon and candidate ownership', () => {
        for (const root of roots) {
          const pidFile = path.join(root, '.codegraph/daemon.pid');
          if (fs.existsSync(pidFile)) daemonPids.add(JSON.parse(fs.readFileSync(pidFile, 'utf8')).pid);
          if (fs.existsSync(spawnLogFor(root))) candidatePids.push(...fs.readFileSync(spawnLogFor(root), 'utf8').split(/\r?\n/)
            .map(Number).filter(pid => Number.isSafeInteger(pid) && pid > 0));
        }
      });
      await Promise.all(clients.map(client => attempt(`client ${client.name} exit`, () => client.close())));
      await attempt('shared daemon natural exit', () => until(() => [...daemonPids].every(pid => !compatibilityAlive(pid)), 20000));
      for (const root of roots) await attempt(`candidate exits ${root}`, () => settleLosingCandidates(root, () => process.pid, 15000));
      let observed: any[] = [];
      await attempt('verify all owned native processes and workers exited', () => {
        observed = events();
        expect(clients.every(client => client.child.exitCode === 0 && client.child.signalCode === null)).toBe(true);
        expect([...daemonPids, ...candidatePids].filter(compatibilityAlive)).toEqual([]);
        const workerKeys = (event: any) => `${event.pid}:${event.threadId}`;
        expect(observed.filter(event => event.event === 'exit').map(workerKeys).sort())
          .toEqual(observed.filter(event => event.event === 'spawn').map(workerKeys).sort());
      });
      if (cleanup.every(row => row.error === null)) await attempt('owned fixture and spawn-log removal', async () => {
        for (const root of roots) removeSpawnLog(root);
        await fs.promises.rm(fixture, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
        expect(fs.existsSync(fixture)).toBe(false); fixtureRemoved = true;
      });
      saveCompatibilityEvidence('startup-join', { ...evidence, phase, primary: primary instanceof Error ? primary.stack : String(primary ?? ''),
        outcome: primary || cleanup.some(row => row.error) ? 'fail' : 'pass', cleanup, fixtureRemoved, events: observed,
        clients: clients.map(client => client.facts()), daemonPids: [...daemonPids], candidatePids });
      const failures = cleanup.filter(row => row.error);
      if (failures.length && !primary) throw new Error('Owned startup-join cleanup incomplete: ' + JSON.stringify(failures));
      if (failures.length && primary) process.stderr.write('Owned startup-join cleanup also failed: ' + JSON.stringify(failures) + '\n');
    }
  }, 120000);

  it.each(queryUnlimitedCases)
  ('reports real requested/effective execution and routes safely in %s with %s',
    async (mode, profile) => runUnlimitedMCPCase(mode, profile), 120000);
});
