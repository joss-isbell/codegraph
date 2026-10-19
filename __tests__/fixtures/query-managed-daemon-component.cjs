// Explicitly managed component seam, NOT an ordinary MCP permission adapter.
// The parent owns an exact native reservation; a true outwait result alone
// never authorizes transfer. All production operations below are unchanged.
const fs = require('node:fs');
const path = require('node:path');
const { randomUUID } = require('node:crypto');
const { Daemon, tryAcquireDaemonLock } = require('../../dist/mcp/daemon');
const { outwaitCandidate } = require('../../dist/mcp/index');
const { getDaemonPidPath } = require('../../dist/mcp/daemon-paths');
const { getWriterPidPath } = require('../../dist/mcp/writer-lock');
const { captureMcpIndexRoute, assertMcpIndexRoute } = require('../../dist/directory');

const config = JSON.parse(fs.readFileSync(process.argv[2], 'utf8'));
const emit = value => process.stdout.write(JSON.stringify({ pid: process.pid, ...value }) + '\n');
const locator = file => path.join(fs.realpathSync(path.dirname(file)), path.basename(file));
const snapshot = file => {
  const stat = fs.statSync(file);
  return { locator: locator(file), raw: fs.readFileSync(file).toString('base64'), birthtimeMs: stat.birthtimeMs, ino: stat.ino };
};
const same = (actual, expected) => JSON.stringify(actual) === JSON.stringify(expected);
let daemon;
let watch;
let transferring = false;
let changed = false;
let stopping;
const stop = () => stopping ??= (async () => {
  watch?.close();
  if (daemon) { emit({ event: 'draining' }); await daemon.stop('owned management component completed'); }
  else process.exit(0);
})();
process.stdin.resume();
process.stdin.once('end', () => { void stop().catch(fail); });
function fail(error) { emit({ event: 'failed', error: error.stack || String(error) }); process.exit(1); }

(async () => {
  if (config.mode === 'candidate') {
    const lock = tryAcquireDaemonLock(config.root);
    if (lock.kind !== 'acquired') throw new Error('candidate did not acquire its own native daemon lock');
    const file = getDaemonPidPath(config.root);
    const identity = { pid: process.pid, startedAt: JSON.parse(fs.readFileSync(file, 'utf8')).startedAt, token: randomUUID() };
    const identityFile = path.join(config.root, '.managed-candidate-identity.json');
    fs.writeFileSync(identityFile, JSON.stringify(identity) + '\n', { flag: 'wx' });
    const record = snapshot(file);
    process.stdin.removeAllListeners('end');
    process.stdin.once('end', () => {
      try {
        if (!same(snapshot(file), record)) throw new Error('candidate cannot release a replaced daemon record');
        fs.unlinkSync(file);
        emit({ event: 'candidate-released', identity, record });
        process.exit(0);
      } catch (error) { fail(error); }
    });
    emit({ event: 'candidate-ready', identity, record, identityFile, creation: snapshot(identityFile) });
    return;
  }
  const file = getWriterPidPath(config.root);
  const route = captureMcpIndexRoute(config.root);
  if (!route || route.root !== config.root) throw new Error('management component has no exact canonical own route');
  const assertReservation = () => {
    assertMcpIndexRoute(route);
    if (changed || !same(snapshot(file), config.reservation)) throw new Error('exact managed writer reservation changed');
    const owner = JSON.parse(Buffer.from(config.reservation.raw, 'base64').toString('utf8'));
    if (owner.pid !== config.owner || owner.mode !== 'handover' || owner.startedAt !== config.startedAt) throw new Error('unproved managed reservation identity');
    process.kill(owner.pid, 0);
    return owner;
  };
  assertReservation();
  watch = fs.watch(path.dirname(file), (_event, name) => {
    if (!transferring && String(name) === path.basename(file)) {
      try { if (!same(snapshot(file), config.reservation)) changed = true; } catch { changed = true; }
    }
  });
  emit({ event: 'reservation-held', reservation: config.reservation });
  if (config.candidate) {
    const candidate = config.candidate;
    if (!same(snapshot(getDaemonPidPath(config.root)), candidate.record)
      || !same(snapshot(candidate.identityFile), candidate.creation)) throw new Error('candidate creation or native record changed');
    const identity = JSON.parse(fs.readFileSync(candidate.identityFile, 'utf8'));
    if (!same(identity, candidate.identity) || identity.pid !== JSON.parse(Buffer.from(candidate.record.raw, 'base64')).pid
      || identity.startedAt !== JSON.parse(Buffer.from(candidate.record.raw, 'base64')).startedAt) throw new Error('candidate identity is not the created native holder');
    process.kill(identity.pid, 0);
    const waited = await outwaitCandidate(config.root, config.owner, identity.pid);
    emit({ event: 'outwait-result', waited, candidate: identity });
    // Even true does not grant authority when the exact reservation changed.
    try { assertReservation(); } catch (error) {
      emit({ event: 'reservation-refused', message: error.message }); watch.close(); process.exit(0); return;
    }
    if (!waited) throw new Error('candidate wait did not finish under the held reservation');
  }
  assertReservation();
  const lock = tryAcquireDaemonLock(config.root);
  if (lock.kind !== 'acquired') throw new Error('management component did not acquire its own daemon lock');
  assertReservation();
  transferring = true;
  daemon = new Daemon(config.root, { handoverFrom: config.mode === 'stranger' ? null : config.owner, projectRoute: route });
  try {
    const listening = await daemon.start();
    watch.close();
    const writer = JSON.parse(fs.readFileSync(file, 'utf8'));
    if (writer.pid !== process.pid || writer.mode !== 'daemon' || listening.lock.pid !== process.pid) throw new Error('native handover did not produce the listening owner');
    emit({ event: 'listening', listening, writer, reservation: config.reservation });
  } catch (error) {
    if (config.mode !== 'stranger') throw error;
    daemon = null;
    if (!same(snapshot(file), config.reservation)) throw new Error('stranger changed managed reservation');
    watch.close();
    emit({ event: 'stranger-refused', message: error.message, reservation: snapshot(file) });
    process.exit(0);
  }
})().catch(fail);
