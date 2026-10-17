'use strict';
// Real native owner and worker hosts. EOF drains owned resources; no forced
// success exit or graph/admission/query stub is used.
const fs = require('node:fs');
const { MCPEngine } = require('../../dist/mcp/engine');
const { captureMcpIndexRoute } = require('../../dist/directory');
const { resolveQueryPolicy } = require('../../dist/mcp/query-policy');
const { tryAcquireWriterLock, releaseWriterLock } = require('../../dist/mcp/writer-lock');
const CodeGraph = require('../../dist').default;
const [role, root, beta] = process.argv.slice(2);
const send = value => process.stdout.write(JSON.stringify(value) + '\n');
let graph, engine, owns = false, stopping;
async function stop() {
  if (stopping) return stopping;
  stopping = (async () => {
    const failures = [];
    try { await engine?.stop(); } catch (error) { failures.push(String(error).slice(0, 2048)); }
    let graphClosed = !graph;
    try { graph?.close(); graphClosed = true; } catch (error) { failures.push(String(error).slice(0, 2048)); }
    try { if (owns && graphClosed) releaseWriterLock(root); } catch (error) { failures.push(String(error).slice(0, 2048)); }
    if (failures.length) { process.exitCode = 1; send({ event: 'cleanup-failed', failures }); }
    else send({ event: 'drained', pid: process.pid });
  })();
  return stopping;
}

// Intentionally incomplete cleanup is isolated in a genuine native child. The
// fault only changes one real close/unlink result; actual SQLite opens, writer
// acquisition, route checks, admission, sync and native host exit still run.
async function cleanupFault(scenario) {
  const assert = require('node:assert/strict');
  const lifecycle = require('../../dist/mcp/project-lifecycle');
  const locks = require('../../dist/mcp/writer-lock');
  const { getDaemonPidPath } = require('../../dist/mcp/daemon-paths');
  const observer = require('./mcp-project-ownership-observer.cjs');
  const held = captureMcpIndexRoute(root);
  assert.ok(held, 'real indexed own route'); observer.begin([root]);
  const actual = resolveQueryPolicy({ poolSize: '0' }, 'direct', 3);
  const prospective = resolveQueryPolicy(actual.requested, 'shared-daemon', 3);
  const fault = new Error('Injected one-shot real native close failure');
  const writerPath = locks.getWriterPidPath(root), originalOpen = CodeGraph.openSync, originalUnlink = fs.unlinkSync, originalLink = fs.linkSync;
  let nativeCloseAttempts = 0, unlinkAttempts = 0, progressRefs = 0, shutdown, renamed = false;
  const checks = {}, lifetimes = [], outcomes = [];
  const progress = { acquireIndexProgress: async () => {
    progressRefs++; let done = false;
    return { release: async () => { if (!done) { done = true; progressRefs--; } } };
  } };
  const nativeCloseFault = scenario.includes('unpublished') || scenario.endsWith('-acquire')
    || scenario === 'registration-undefined' || scenario === 'reader-reservation-poison';
  CodeGraph.openSync = function(selected, options) {
    if (scenario === 'unlink-rollback' && !options?.readOnly && !renamed) {
      // A real held-route filesystem change between the final assertion and
      // the actual facade open forces acquisition rollback; no opener stub.
      fs.renameSync(held.index, held.index + '-retained'); renamed = true;
    }
    const opened = originalOpen.call(this, selected, options);
    if (!options?.readOnly) {
      const close = opened.close.bind(opened);
      opened.close = () => {
        nativeCloseAttempts++;
        if (nativeCloseFault && nativeCloseAttempts === 1) {
          if (scenario === 'reader-reservation-poison') close(); // later real close would be idempotent
          throw fault;
        }
        return close();
      };
      if (scenario === 'default-acquire' || scenario === 'explicit-acquire') queueMicrotask(() => {
        shutdown = engine.stop(); void shutdown.catch(() => {});
      });
    } else if (scenario === 'default-unpublished') {
      fs.writeFileSync(getDaemonPidPath(root), '{'); // genuine typed final native-owner refusal
    } else if (scenario === 'explicit-unpublished') queueMicrotask(() => {
      shutdown = engine.stop(); void shutdown.catch(() => {});
    });
    return opened;
  };
  if (scenario === 'unlink-retire' || scenario === 'unlink-rollback' || scenario === 'replacement-record' || scenario === 'unreadable-record' || scenario === 'constructor-unlink') {
    fs.unlinkSync = function(file) {
      if (String(file) === writerPath) {
        unlinkAttempts++;
        if (scenario === 'unlink-retire' || scenario === 'unlink-rollback' || scenario === 'constructor-unlink') {
          const error = new Error('Injected exact writer-record unlink EACCES'); error.code = 'EACCES'; throw error;
        }
      }
      return originalUnlink.apply(this, arguments);
    };
  }
  const settle = async pending => { try { return { fulfilled: true, value: await pending }; } catch (error) { return { fulfilled: false, error }; } };
  const request = (intent, lifetime, resourceObserver = progress) => ({ route: held, intent, lifetime: lifetime.signal,
    watchOptions: {}, resourceObserver, admit: (selected, signal) => lifecycle.admitQueryProject(selected.root, prospective, signal) });
  let primary, cleanupError, owner, retained = true;
  try {
    if (scenario === 'constructor-unlink') {
      fs.linkSync = function(from, to) {
        const result = originalLink.apply(this, arguments);
        if (String(to) === writerPath) {
          // Observe the actual atomic acquisition, then create a genuine live
          // foreign rebuild condition before the synchronous fallible retain.
          fs.writeFileSync(locks.getWriterPidPath(root, 'rebuild.pid'), JSON.stringify({ pid: process.ppid, mode: 'rebuild', startedAt: Date.now() }));
        }
        return result;
      };
      try { new MCPEngine({ executionMode: 'direct', queryPolicy: actual, queryAdmissionPolicy: prospective,
        projectRoute: held, writerLockRoot: root, watch: false, queryPool: false }); }
      catch (error) { primary = error; }
      owner = locks.readWriterLock(root);
      checks.constructorPrimaryPreserved = primary?.name === 'RebuildInProgressError';
      checks.realNewWriterRetained = owner?.pid === process.pid && unlinkAttempts === 1;
      const lifetime = new AbortController(); lifetimes.push(lifetime);
      const next = await settle(lifecycle.acquireProject(request('read', lifetime))); cleanupError = next.error;
      checks.failedConstructorCoordinatorPoisoned = !next.fulfilled && next.error !== primary;
      checks.failedConstructorPoisonSticky = (await settle(lifecycle.acquireProject(request('read', lifetime)))).error === cleanupError;
      checks.failedConstructorNeverRetriesUnlink = unlinkAttempts === 1;
      checks.inertConstructorDidNotOpenSQLite = observer.facts().total === 0;
    } else if (scenario === 'reader-reservation-poison' || scenario === 'registration-undefined') {
      const lifetime = new AbortController(); lifetimes.push(lifetime);
      let reservation;
      if (scenario === 'reader-reservation-poison') {
        const acquired = locks.tryAcquireWriterLock(root, 'direct'); assert.equal(acquired.kind, 'acquired');
        reservation = lifecycle.retainStartupProjectWriter(held, acquired.info);
      }
      const writer = await lifecycle.acquireProject({ ...request('watch', lifetime), startupOwner: reservation }); await writer.ready();
      owner = locks.readWriterLock(root); assert.equal(owner?.pid, process.pid);
      if (scenario === 'reader-reservation-poison') {
        const reader = await lifecycle.acquireProject(request('read', lifetime));
        const nativeBytes = fs.readFileSync(writerPath, 'utf8');
        checks.moduleBorrowerRollbackDoesNotUnlink = lifecycle.rollbackStartupProjectWriter(held,
          { kind: 'acquired', info: owner, pidPath: writerPath },
          { pidPath: writerPath, locator: require('node:path').join(fs.realpathSync(require('node:path').dirname(writerPath)), require('node:path').basename(writerPath)), previous: owner }) === 'borrowed'
          && fs.readFileSync(writerPath, 'utf8') === nativeBytes;
        const first = await settle(writer.release()); cleanupError = first.error;
        const second = await settle(reader.release()), third = await settle(reservation.release());
        checks.firstNativeFailure = !first.fulfilled && first.error.errors?.includes(fault);
        checks.readerRetainsSameFailure = !second.fulfilled && second.error === first.error;
        checks.lastReservationRetainsSameFailure = !third.fulfilled && third.error === first.error;
        checks.cachedReservationFailure = (await settle(reservation.release())).error === first.error;
        checks.readerStillActuallyReadable = reader.cg.searchNodes('alphaOriginal').length > 0;
      } else {
        let callbackHolder, observerEntered = false;
        const beforeRegistration = observer.facts().total;
        const rejectedRegistration = await settle(lifecycle.acquireProject({ ...request('read', lifetime, {
          acquireIndexProgress: async () => { observerEntered = true; throw new Error('Observer must follow ownership registration'); },
        }), retain: acquisition => { callbackHolder = acquisition; throw undefined; } }));
        checks.callbackUndefinedPrimaryPreserved = !rejectedRegistration.fulfilled && rejectedRegistration.error === undefined;
        checks.registrationPrecedesEveryAwait = !observerEntered && observer.facts().total === beforeRegistration;
        checks.failedRegistrationHolderDisposed = (await settle(callbackHolder.release())).fulfilled;
        checks.existingNativeReaderSurvives = writer.cg.searchNodes('alphaOriginal').length > 0;
        let entered, resume, retainedAcquisition;
        const entry = new Promise(resolve => { entered = resolve; }), gate = new Promise(resolve => { resume = resolve; });
        const pending = lifecycle.acquireProject({ ...request('read', lifetime, { acquireIndexProgress: async () => {
          entered(); await gate; throw undefined; // real outer registration failure, including a non-Error primary
        } }), retain: acquisition => { retainedAcquisition = acquisition; } });
        const disposition = settle(pending); await entry;
        const first = await settle(writer.release()); cleanupError = first.error; resume();
        const failed = await disposition; primary = failed.error;
        checks.undefinedPrimaryPreserved = !failed.fulfilled && failed.error === undefined;
        checks.earlyHolderWasRetained = !!retainedAcquisition;
        checks.sameHolderHasStickyCleanup = (await settle(retainedAcquisition.release())).error === first.error;
        checks.originalHolderStillFails = (await settle(writer.release())).error === first.error;
      }
      const before = nativeCloseAttempts;
      checks.noCoordinatorResurrection = (await settle(lifecycle.acquireProject(request('read', lifetime)))).error === cleanupError;
      checks.noAutomaticNativeCloseRetry = nativeCloseAttempts === before && before === 1;
      checks.lifetimeRetained = lifetimes.every(lifetime => !lifetime.signal.aborted);
    } else {
      let reservation;
      if (scenario === 'replacement-record') {
        const acquired = locks.tryAcquireWriterLock(root, 'direct'); assert.equal(acquired.kind, 'acquired');
        reservation = lifecycle.retainStartupProjectWriter(held, acquired.info);
      }
      engine = new MCPEngine({ executionMode: 'direct', queryPolicy: actual, queryAdmissionPolicy: prospective,
        ...(scenario.startsWith('explicit-') ? {} : { projectRoute: held }), ...(reservation ? { projectWriter: reservation } : {}),
        watch: scenario.startsWith('explicit-'), queryPool: false, resourceObserver: progress, onInitialDefaultOutcome: outcome => outcomes.push(outcome) });
      const attempt = scenario.startsWith('explicit-')
        ? engine.getToolHandler().execute('codegraph_search', { query: 'alphaOriginal', projectPath: root })
        : engine.ensureInitialized(root);
      const disposition = await settle(attempt); primary = disposition.error;
      if (!scenario.includes('unpublished') && !scenario.endsWith('-acquire') && scenario !== 'unlink-rollback') {
        // Join the real catch-up before deliberately changing only its cleanup boundary.
        const answer = await engine.getToolHandler().execute('codegraph_search', { query: 'alphaOriginal' });
        assert.ok(answer.content[0]?.text.includes('alphaOriginal'));
      }
      owner = locks.readWriterLock(root); assert.equal(owner?.pid, process.pid);
      if (scenario === 'unreadable-record') fs.writeFileSync(writerPath, '{');
      const firstStop = shutdown ?? engine.stop(), stopped = await settle(firstStop); cleanupError = stopped.error;
      if (scenario === 'replacement-record') {
        assert.equal(stopped.fulfilled, true);
        const replacement = { ...owner, startedAt: owner.startedAt + 1 }; fs.writeFileSync(writerPath, JSON.stringify(replacement));
        const bytes = fs.readFileSync(writerPath, 'utf8'); await reservation.release();
        checks.replacementNotUnlinked = fs.readFileSync(writerPath, 'utf8') === bytes && unlinkAttempts === 0;
        checks.progressReleasedAfterProvedDrain = progressRefs === 0; retained = false;
      } else {
        checks.stopRejectsIncompleteNativeCleanup = !stopped.fulfilled;
        checks.stopPromiseIsSticky = engine.stop() === firstStop;
        checks.stopFailureIsSticky = (await settle(engine.stop())).error === cleanupError;
        checks.noPublishedDefaultAfterFailedAttempt = !engine.hasDefaultCodeGraph() || !scenario.includes('default-');
        if (scenario === 'default-unpublished') {
          checks.typedRequestPrimaryPreserved = !disposition.fulfilled && disposition.error.reason === 'unknown';
          checks.typedDefaultOutcomePreserved = outcomes.length === 1 && outcomes[0].kind === 'refused';
        }
        if (scenario.includes('unpublished') || scenario.endsWith('-acquire')) checks.closeFailureWasRetained = nativeCloseAttempts === 1;
        if (renamed) { fs.renameSync(held.index + '-retained', held.index); renamed = false; }
        const lifetime = new AbortController(); lifetimes.push(lifetime);
        const before = nativeCloseAttempts, unlinks = unlinkAttempts, nativeBefore = observer.facts().total;
        const next = await settle(lifecycle.acquireProject(request('read', lifetime)));
        checks.noCoordinatorResurrection = !next.fulfilled && cleanupError.errors?.includes(next.error);
        checks.poisonRefusalPrecedesNativeOpen = observer.facts().total === nativeBefore;
        checks.noAutomaticCleanupRetry = nativeCloseAttempts === before && unlinkAttempts === unlinks;
        checks.recordRetained = fs.existsSync(writerPath);
        if (scenario === 'unlink-retire' || scenario === 'unlink-rollback') checks.remainingExactIdentity =
          JSON.stringify(locks.readWriterLock(root)) === JSON.stringify(owner) && unlinkAttempts === 1;
        if (scenario === 'unreadable-record') checks.unreadableRecordNotClaimedReleased = fs.readFileSync(writerPath, 'utf8') === '{' && unlinkAttempts === 0;
      }
    }
    if (retained) { if (scenario !== 'constructor-unlink') checks.progressRetained = progressRefs > 0;
      checks.cleanupFailurePresent = cleanupError !== undefined; }
    for (const [name, value] of Object.entries(checks)) assert.equal(value, true, name);
    send({ event: 'cleanup-fault-observed', scenario, checks, retained, progressRetained: progressRefs > 0,
      owner, primary: String(primary), nativeCloseAttempts, unlinkAttempts, facts: observer.facts() });
    // These expected failures intentionally leave the real holder/owner/progress
    // retained until genuine host exit. Exit is not a claim of successful drain.
  } finally {
    CodeGraph.openSync = originalOpen; fs.unlinkSync = originalUnlink; fs.linkSync = originalLink;
    if (renamed) fs.renameSync(held.index + '-retained', held.index);
  }
}

async function main() {
  if (role === 'cleanup-fault') {
    await cleanupFault(beta);
  } else if (role === 'writer') {
    const writer = tryAcquireWriterLock(root, 'direct');
    if (writer.kind !== 'acquired') throw new Error('Disposable native writer could not acquire its writer slot');
    owns = true;
    graph = CodeGraph.openSync(root);
    send({ event: 'ready', pid: process.pid, writer: writer.info });
    process.stdin.resume();
    process.stdin.once('end', () => { void stop().catch(error => { process.exitCode = 1; send({ event: 'stop-error', error: String(error) }); }); });
  } else if (role === 'workers') {
    const actual = resolveQueryPolicy({ poolSize: '1', busyTimeoutMs: '10000' }, 'direct', 3);
    const prospective = resolveQueryPolicy(actual.requested, 'shared-daemon', 3);
    engine = new MCPEngine({ executionMode: 'direct', queryPolicy: actual, queryAdmissionPolicy: prospective,
      projectRoute: captureMcpIndexRoute(root), watch: false, queryPool: true });
    await engine.ensureInitialized(root);
    const handler = engine.getToolHandler();
    const first = await handler.execute('codegraph_search', { query: 'alphaOriginal' });
    const second = await handler.execute('codegraph_search', { query: 'betaOriginal', projectPath: beta });
    send({ event: 'answers', first, second, diagnostics: engine.getQueryDiagnostics() });
    await stop();
  } else if (role === 'measurements' || role === 'measurement-drift') {
    const { measurePendingChanges, endFreshnessMeasurements } = require('../../dist/mcp/index-freshness');
    const observer = require('./mcp-project-ownership-observer.cjs');
    const held = captureMcpIndexRoute(root);
    if (!held) throw new Error('Measurement fixture requires its exact own index');
    if (role === 'measurement-drift') {
      if (beta === 'selector') {
        const alternate = require('node:path').join(root, '.alternate');
        fs.mkdirSync(alternate); fs.copyFileSync(held.index, require('node:path').join(alternate, 'codegraph.db'));
        process.env.CODEGRAPH_DIR = '.alternate';
      } else if (beta === 'missing') fs.renameSync(held.index, held.index + '-retained');
      else throw new Error('Unknown measurement drift');
    }
    const counts = await measurePendingChanges(root, undefined, held);
    await endFreshnessMeasurements();
    send({ event: 'measurement-drained', counts, held, workers: observer.workerFacts() });
    await stop();
  } else throw new Error('Unknown ownership host role');
}
main().catch(async error => {
  process.exitCode = 1;
  send({ event: 'failure', error: String(error).slice(0, 2048) });
  try { await stop(); } catch (cleanup) { send({ event: 'secondary', error: String(cleanup).slice(0, 2048) }); }
});
