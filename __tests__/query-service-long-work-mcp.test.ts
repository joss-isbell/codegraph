import { describe, expect, it } from 'vitest';
import * as fs from 'fs';
import * as os from 'os';
import * as path from 'path';
import { CodeGraph } from '../src';
import { parseQueryDiagnostics } from './query-diagnostics-helper';
import { removeSpawnLog, settleLosingCandidates, spawnLogFor } from './daemon-candidates';
import { CompatibilityClient, alive, until, wait, saveCompatibilityEvidence } from './query-compatibility-host';

describe('shared service preserves healthy work through refused acquisitions', () => {
  it.runIf(process.platform === 'win32')('survives a healthy 47-second shared query through repeated concurrent incompatible acquisitions and unknown reconnects', async () => {
    const fixture = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), 'cg-policy-long-work-')));
    const ordinaryEvidence = `${fixture}-evidence.json`;
    const root = path.join(fixture, 'project'); fs.mkdirSync(root);
    const workers = path.join(fixture, 'workers.jsonl');
    const timing = path.join(fixture, 'timing.jsonl');
    const network = path.join(fixture, 'network.jsonl');
    const unknownHello = path.join(fixture, 'unknown-hello');
    const clients: CompatibilityClient[] = [];
    const cleanup: Array<{ phase: string; error: string | null }> = [];
    const evidence: Record<string, unknown> = { root, releaseMs: 47000, clientDeadlineMs: 80000, caseBoundMs: 120000 };
    const waves: unknown[] = [];
    const issuedTools: Array<{ client: CompatibilityClient; id: number; verify: (reply: any) => void }> = [];
    let nextToolId = 1000;
    let completedScenario = false;
    let finalReplyVerification: Record<string, unknown> = { outcome: 'not-run', reason: 'scenario incomplete or native cleanup failed' };
    let daemonPid: number | undefined;
    let candidatePids: number[] = [];
    let primary: unknown;
    let phase = 'fixture setup';
    let fixtureRemoved = false;
    let longSettled = false;
    let longResult: Promise<{ reply?: any; error?: unknown }> | undefined;
    const events = (file: string): any[] => fs.existsSync(file)
      ? fs.readFileSync(file, 'utf8').split(/\r?\n/).filter(Boolean).map(line => JSON.parse(line)) : [];
    const preloads = ['query-unlimited-preload.cjs', 'query-waiting-preload.cjs', 'query-compatibility-network.cjs']
      .map(name => path.join(__dirname, 'fixtures', name));
    const launch = (name: string, overrides: NodeJS.ProcessEnv = {}) => {
      const client = new CompatibilityClient(root, name, {
        CODEGRAPH_QUERY_MODE: 'default', CODEGRAPH_QUERY_POOL_SIZE: '1', CODEGRAPH_QUERY_BUSY_TIMEOUT_MS: 'unlimited',
        CODEGRAPH_DAEMON_IDLE_TIMEOUT_MS: '1000',
        QUERY_UNLIMITED_OBSERVATIONS: workers, QUERY_UNLIMITED_RELEASE: path.join(fixture, 'unused-release'),
        QUERY_WAITING_OBSERVATIONS: timing,
        QUERY_COMPAT_NETWORK_OBSERVATIONS: network, QUERY_COMPAT_UNKNOWN_HELLO: unknownHello, ...overrides,
      }, 80000, preloads);
      clients.push(client); return client;
    };
    const locks = () => ['daemon.pid', 'writer.pid'].map(name => {
      const file = path.join(root, '.codegraph', name);
      const stat = fs.statSync(file, { bigint: true });
      return { name, text: fs.readFileSync(file, 'utf8'), birthtimeNs: String(stat.birthtimeNs),
        mtimeNs: String(stat.mtimeNs), ctimeNs: String(stat.ctimeNs) };
    });
    const candidateLog = () => fs.existsSync(spawnLogFor(root)) ? fs.readFileSync(spawnLogFor(root), 'utf8') : '';
    const resultText = (reply: any, symbol: string, line: number) => {
      expect(reply.error).toBeUndefined(); expect(reply.result.isError).toBeFalsy();
      expect(reply.result.content[0].text).toContain(symbol);
      expect(reply.result.content[0].text).toContain(`app.ts:${line}`);
    };
    const refused = (reply: any, reason: 'incompatible' | 'unknown') => {
      expect(reply.error).toMatchObject({ code: -32603,
        data: { code: 'CODEGRAPH_QUERY_POLICY_REFUSED', reason, projectRoot: root } });
      expect(reply.error.data.guidance).toContain(root); expect(reply.result).toBeUndefined();
    };
    const issue = (client: CompatibilityClient, name: string, args: Record<string, unknown>,
      verify: (reply: any) => void, id = ++nextToolId) => {
      issuedTools.push({ client, id, verify });
      return client.tool(name, args, id);
    };
    const search = (client: CompatibilityClient, symbol: string, line: number, id: number) =>
      issue(client, 'codegraph_search', { query: symbol }, reply => resultText(reply, symbol, line), id);
    const status = (client: CompatibilityClient) => issue(client, 'codegraph_status', {}, reply => {
      expect(reply.error).toBeUndefined(); parseQueryDiagnostics(reply.result);
    });
    const challenge = (client: CompatibilityClient, query: string, reason: 'incompatible' | 'unknown', id: number, projectPath?: string) =>
      issue(client, 'codegraph_search', { query, ...(projectPath ? { projectPath } : {}) }, reply => refused(reply, reason), id);
    try {
      fs.writeFileSync(path.join(root, 'app.ts'), 'export function healthyInitialSymbol() { return 701; }\n'
        + 'export function delayedTimingSymbol() { return 47; }\n'
        + 'export function healthyAfterSymbol() { return 907; }\n');
      const graph = await CodeGraph.init(root); try { await graph.indexAll(); } finally { graph.close(); }
      const owner = launch('healthy-long-owner'); await owner.initialize();
      evidence.initial = await search(owner, 'healthyInitialSymbol', 1, 100);
      resultText(evidence.initial, 'healthyInitialSymbol', 1);
      const before = parseQueryDiagnostics((await status(owner)).result);
      expect(before.effective).toMatchObject({ executionMode: 'shared-daemon',
        concurrency: { kind: 'finite', maximum: 1 }, deadline: { kind: 'absent' }, retention: 'ordinary' });
      expect(before.lifecycle).toMatchObject({ state: 'active', dispatch: 'worker', workers: 1 });
      daemonPid = before.ownership.pid;
      const lockBefore = locks(), candidatesBefore = candidateLog();
      const spawnedBefore = events(workers).filter(event => event.event === 'spawn');
      expect(spawnedBefore).toHaveLength(1);
      expect(spawnedBefore[0]).toMatchObject({ pid: daemonPid, execution: { generation: before.ownership.generation } });
      Object.assign(evidence, { before, lockBefore, candidatesBefore, spawnedBefore });

      phase = 'healthy worker request admitted';
      // Attach rejection handling immediately; the real worker, SQLite handler
      // and result stay unchanged. Existing timing preload delays delivery only.
      const requested = process.hrtime.bigint();
      longResult = search(owner, 'delayedTimingSymbol', 2, 47000)
        .then(reply => { longSettled = true; return { reply }; }, error => { longSettled = true; return { error }; });
      await until(() => events(timing).some(event => event.event === 'controlled-start'), 10000, () => owner.stderr);
      const controlledStart = events(timing).find(event => event.event === 'controlled-start');
      expect(controlledStart).toMatchObject({ pid: daemonPid, threadId: spawnedBefore[0].threadId,
        context: { requestId: 47000 } });

      for (const [wave, offsetMs] of [0, 15000, 30000].entries()) {
        await wait(Math.max(0, offsetMs - Number(process.hrtime.bigint() - BigInt(controlledStart.monotonicNs)) / 1e6));
        phase = `refusal and actual reconnect wave ${wave + 1}`;
        const waveStarted = process.hrtime.bigint();
        const dropRequest = path.join(fixture, `drop-${wave}`);
        const retry = launch(`reconnecting-${wave}`, { QUERY_COMPAT_DROP_REQUEST: dropRequest,
          CODEGRAPH_DAEMON_RETRY_MS: '100', CODEGRAPH_DAEMON_RETRY_MAX_MS: '100' });
        const wrong = [launch(`conflict-${wave}-a`, { CODEGRAPH_QUERY_POOL_SIZE: '2' }),
          launch(`conflict-${wave}-b`, { CODEGRAPH_QUERY_POOL_SIZE: '2' })];
        await Promise.all([retry, ...wrong].map(client => client.initialize()));
        const attached = parseQueryDiagnostics((await status(retry)).result);
        expect(attached.ownership).toEqual(before.ownership); expect(attached.effective).toEqual(before.effective);
        const incompatible = await Promise.all(wrong.map(client => challenge(client, 'mustNeverExecuteConflict', 'incompatible', 200)));
        incompatible.forEach(reply => refused(reply, 'incompatible'));
        await Promise.all(wrong.map(client => client.close()));

        // Global owned hello gate is deliberately enabled only after the retry
        // peer really attached, so request200 must traverse reconnect/refusal.
        fs.writeFileSync(unknownHello, 'subsequent hello omits the required effective policy\n');
        fs.writeFileSync(dropRequest, 'drop this peer request200 before forwarding\n');
        const unknown = launch(`unknown-acquisition-${wave}`);
        const unknownAcquisition = (async () => {
          await unknown.initialize();
          return challenge(unknown, 'mustNeverExecuteUnknown', 'unknown', 200);
        })();
        const unknownResults = await Promise.all([unknownAcquisition,
          challenge(retry, 'mustNeverReplayOrphan', 'unknown', 200)]);
        unknownResults.forEach(reply => refused(reply, 'unknown'));
        const explicit = await challenge(retry, 'mustNeverExecuteExplicit', 'unknown', 201, root);
        refused(explicit, 'unknown');
        for (const [client, ids] of [[retry, [200, 201]], [unknown, [200]]] as const)
          for (const id of ids) expect(client.replies.filter(reply => reply.id === id)).toHaveLength(1);
        await Promise.all([retry, unknown].map(client => client.close()));
        fs.rmSync(unknownHello);

        const current = parseQueryDiagnostics((await status(owner)).result);
        expect(longSettled).toBe(false); expect(alive(daemonPid)).toBe(true);
        expect(current.ownership).toEqual(before.ownership); expect(current.effective).toEqual(before.effective);
        expect(locks()).toEqual(lockBefore); expect(candidateLog()).toBe(candidatesBefore);
        expect(events(workers).filter(event => event.event === 'spawn')).toEqual(spawnedBefore);
        expect(events(workers).filter(event => event.event === 'exit')).toEqual([]);
        expect(events(workers).filter(event => event.event === 'call-start').map(event => event.query))
          .toEqual(['healthyInitialSymbol', 'delayedTimingSymbol']);
        const linkEvents = events(network).filter(event => event.pid === retry.child.pid);
        expect(linkEvents.filter(event => event.event === 'owned-link-drop'))
          .toEqual([expect.objectContaining({ id: 200, forwarded: false })]);
        expect(linkEvents.filter(event => event.event === 'owned-link-closed')).toHaveLength(1);
        expect(events(network).filter(event => event.event === 'unknown-hello' && BigInt(event.monotonicNs) >= waveStarted).length)
          .toBeGreaterThanOrEqual(2);
        waves.push({ wave: wave + 1, offsetMs, startedNs: String(waveStarted), completedNs: String(process.hrtime.bigint()),
          attached, incompatible, unknownResults, explicit, current, retryPid: retry.child.pid, linkEvents });
      }

      phase = 'real delayed SQLite result and surviving service';
      const completed = await longResult;
      expect(completed.error).toBeUndefined(); resultText(completed.reply, 'delayedTimingSymbol', 2);
      const elapsedMs = Number(process.hrtime.bigint() - requested) / 1e6;
      expect(elapsedMs).toBeGreaterThanOrEqual(47000); expect(elapsedMs).toBeLessThan(80000);
      expect(owner.replies.filter(reply => reply.id === 47000)).toHaveLength(1);
      const controlled = events(timing).filter(event => event.event.startsWith('controlled-'));
      expect(controlled.map(event => event.event)).toEqual(['controlled-start', 'controlled-release']);
      expect(controlled[1]).toMatchObject({ pid: daemonPid, threadId: controlledStart.threadId, id: controlledStart.id });
      expect(BigInt(controlled[1].monotonicNs) - BigInt(controlledStart.monotonicNs)).toBeGreaterThanOrEqual(47000000000n);
      const controlledResults = events(timing).filter(event => event.event === 'result' && event.pid === daemonPid
        && event.threadId === controlledStart.threadId && event.id === controlledStart.id);
      expect(controlledResults).toHaveLength(1);
      expect(controlledResults[0].context).toMatchObject({ requestId: 47000 });
      expect(BigInt(controlledResults[0].monotonicNs)).toBeGreaterThanOrEqual(BigInt(controlled[1].monotonicNs));
      const healthy = await search(owner, 'healthyAfterSymbol', 3, 101);
      resultText(healthy, 'healthyAfterSymbol', 3);
      const after = parseQueryDiagnostics((await status(owner)).result);
      expect(after.ownership).toEqual(before.ownership); expect(after.effective).toEqual(before.effective);
      expect(locks()).toEqual(lockBefore); expect(candidateLog()).toBe(candidatesBefore);
      expect(events(workers).filter(event => event.event === 'spawn')).toEqual(spawnedBefore);
      expect(events(workers).filter(event => event.event === 'call-start').map(event => event.query))
        .toEqual(['healthyInitialSymbol', 'delayedTimingSymbol', 'healthyAfterSymbol']);
      for (const client of clients) expect(client.stderr)
        .not.toMatch(/serving this session in-process|local read-only fallback|Shared daemon unavailable/);
      Object.assign(evidence, { elapsedMs, completed, controlled, healthy, after });
      phase = 'owned native cleanup';
      completedScenario = true;
    } catch (error) { primary = error; throw error; }
    finally {
      const attempt = async (name: string, action: () => unknown | Promise<unknown>) => {
        try { await action(); cleanup.push({ phase: name, error: null }); }
        catch (error) { cleanup.push({ phase: name, error: error instanceof Error ? error.stack ?? error.message : String(error) }); }
      };
      await attempt('capture daemon and candidate ownership', () => {
        const pidFile = path.join(root, '.codegraph/daemon.pid');
        if (!daemonPid && fs.existsSync(pidFile)) daemonPid = JSON.parse(fs.readFileSync(pidFile, 'utf8')).pid;
        candidatePids = candidateLog().split(/\r?\n/).map(Number).filter(pid => Number.isSafeInteger(pid) && pid > 0);
      });
      // On an earlier failed challenge, let the bounded controlled healthy job
      // finish naturally before closing its session; do not signal the daemon.
      await attempt('settle controlled request before shutdown', async () => { if (longResult) await longResult; });
      await Promise.all(clients.map(client => attempt(`client ${client.name} exit`, () => client.close())));
      await attempt('shared daemon natural exit', async () => { if (daemonPid) await until(() => !alive(daemonPid!), 20000); });
      await attempt('recorded candidates exit', () => settleLosingCandidates(root, () => process.pid, 15000));
      let workerEvents: any[] = [], timingEvents: any[] = [], networkEvents: any[] = [];
      await attempt('capture final worker, timing and network observations', () => {
        workerEvents = events(workers); timingEvents = events(timing); networkEvents = events(network);
      });
      await attempt('verify all owned native processes and worker identities exited', () => {
        expect(clients.every(client => client.child.exitCode === 0 && client.child.signalCode === null)).toBe(true);
        expect([...candidatePids, ...(daemonPid ? [daemonPid] : [])].filter(alive)).toEqual([]);
        const key = (event: any) => `${event.pid}:${event.threadId}`;
        expect(workerEvents.filter(event => event.event === 'exit').map(key).sort())
          .toEqual(workerEvents.filter(event => event.event === 'spawn').map(key).sort());
        expect(timingEvents.filter(event => event.event === 'exit').map(key).sort())
          .toEqual(timingEvents.filter(event => event.event === 'spawn').map(key).sort());
      });
      if (completedScenario && cleanup.every(row => row.error === null)) await attempt('verify final replies and controlled result after EOF', () => {
        finalReplyVerification = { outcome: 'fail', issuedTools: issuedTools.length };
        for (const client of clients) {
          expect(client.child.stdout.readableEnded, `${client.name} stdout EOF`).toBe(true);
          expect(client.child.stderr.readableEnded, `${client.name} stderr EOF`).toBe(true);
        }
        for (const { client, id, verify } of issuedTools) {
          const replies = client.replies.filter(reply => reply.id === id);
          expect(replies, `${client.name} request ${id} after EOF`).toHaveLength(1);
          verify(replies[0]);
        }
        const controlled = timingEvents.filter(event => event.event.startsWith('controlled-'));
        expect(controlled.map(event => event.event)).toEqual(['controlled-start', 'controlled-release']);
        const [start, release] = controlled;
        expect(start).toMatchObject({ pid: daemonPid, context: { requestId: 47000 } });
        expect(release).toMatchObject({ pid: start.pid, threadId: start.threadId, id: start.id });
        const results = timingEvents.filter(event => event.event === 'result' && event.pid === start.pid
          && event.threadId === start.threadId && event.id === start.id);
        expect(results).toHaveLength(1);
        expect(results[0].context).toMatchObject({ requestId: 47000 });
        expect(timingEvents.filter(event => event.event === 'result' && event.context?.requestId === 47000)).toEqual(results);
        expect(BigInt(results[0].monotonicNs)).toBeGreaterThanOrEqual(BigInt(release.monotonicNs));
        finalReplyVerification = { outcome: 'pass', issuedTools: issuedTools.length,
          controlledResult: { pid: start.pid, threadId: start.threadId, internalId: start.id, requestId: 47000 } };
      });
      if (!primary && cleanup.every(row => row.error === null)) await attempt('owned fixture and spawn-log removal', async () => {
        removeSpawnLog(root);
        await fs.promises.rm(fixture, { recursive: true, force: true, maxRetries: 10, retryDelay: 100 });
        expect(fs.existsSync(fixture)).toBe(false); fixtureRemoved = true;
      });
      await attempt('persist complete compatibility evidence', () => saveCompatibilityEvidence('compatibility-long-work', {
        ...evidence, waves, phase, completedScenario, finalReplyVerification,
        issuedTools: issuedTools.map(({ client, id }) => ({ client: client.name, id })),
        outcome: primary || cleanup.some(row => row.error) ? 'fail' : 'pass',
        primary: primary instanceof Error ? primary.stack : String(primary ?? ''), cleanup, fixtureRemoved,
        workerEvents, timingEvents, networkEvents, clients: clients.map(client => client.facts()), daemonPid, candidatePids }, ordinaryEvidence));
      if (!process.env.VALIDATION_OUT) process.stderr.write(`Long-work evidence: ${ordinaryEvidence}\n`);
      const failures = cleanup.filter(row => row.error);
      if (failures.length && !primary) throw new Error('Owned long-work cleanup incomplete: ' + JSON.stringify(failures));
      if (failures.length && primary) process.stderr.write('Owned long-work cleanup also failed: ' + JSON.stringify(failures) + '\n');
    }
  }, 120000);
});
