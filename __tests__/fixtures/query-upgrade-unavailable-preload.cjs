// Timing only at the approved completion/acquisition boundaries. Production
// stop/getDaemonSocket are called unchanged; refusals are never caught/reset.
const fs = require('node:fs');
const path = require('node:path');
const threads = require('node:worker_threads');
const root = process.env.QUERY_UPGRADE_ROOT;
const role = process.env.QUERY_UPGRADE_ROLE;
const observations = process.env.QUERY_UPGRADE_OBSERVATIONS;
const emit = value => fs.appendFileSync(observations, JSON.stringify({ pid: process.pid, role, at: process.hrtime.bigint().toString(), ...value }) + '\n');
const owners = () => Object.fromEntries(['writer.pid', 'daemon.pid'].map(name => {
  const file = path.join(root, '.codegraph', name);
  try {
    const stat = fs.statSync(file);
    return [name, { locator: path.join(fs.realpathSync(path.dirname(file)), name), raw: fs.readFileSync(file).toString('base64'), birthtimeMs: stat.birthtimeMs, ino: stat.ino }];
  } catch (error) { if (error.code === 'ENOENT') return [name, null]; throw error; }
}));
const waitFile = (file, signal) => new Promise((resolve, reject) => {
  let watcher;
  const finish = (error, value) => { watcher?.close(); signal?.removeEventListener('abort', aborted); error ? reject(error) : resolve(value); };
  const aborted = () => finish(Object.assign(new Error('upgrade fixture boundary abandoned'), { name: 'AbortError' }));
  const inspect = () => {
    if (signal?.aborted) { aborted(); return; }
    try { if (fs.existsSync(file)) finish(null, JSON.parse(fs.readFileSync(file, 'utf8'))); } catch (error) { finish(error); }
  };
  watcher = fs.watch(path.dirname(file), inspect);
  signal?.addEventListener('abort', aborted, { once: true });
  inspect();
});

if (threads.isMainThread && root && role) {
  if (process.env.CODEGRAPH_DAEMON_INTERNAL === '1' && role === 'controlled') {
    // Actual native candidate, original CLI argv/env/stdio, before CLI imports.
    emit({ event: 'candidate-before-cli-exit', code: 78, argv: process.argv, execArgv: process.execArgv, owners: owners() });
    process.exit(78);
  }
  const mcpDir = path.resolve(path.dirname(process.argv[1]), '../mcp');
  if (process.env.CODEGRAPH_DAEMON_INTERNAL === '1' && role === 'owner') {
    const OriginalWorker = threads.Worker;
    threads.Worker = class extends OriginalWorker {
      constructor(...args) {
        super(...args);
        if (!String(args[0]).replaceAll('\\', '/').endsWith('/mcp/query-worker.js')) return;
        const threadId = this.threadId;
        emit({ event: 'worker-created', threadId });
        this.on('exit', code => emit({ event: 'worker-exit', threadId, code }));
      }
    };
    const { MCPEngine } = require(path.join(mcpDir, 'engine'));
    const original = MCPEngine.prototype.stop;
    MCPEngine.prototype.stop = function(...args) {
      // Invoke immediately. Rejection propagates unchanged and is NEVER held.
      const actual = original.apply(this, args);
      return actual.then(async value => {
        emit({ event: 'native-drain-complete-caller-notification-held', owners: owners() });
        const release = await waitFile(process.env.QUERY_UPGRADE_DRAIN_RELEASE);
        // abort releases ONLY the notification of an already-successful native
        // stop for failed-fixture cleanup; it cannot reject/undo that real stop.
        // Acquisition's separate abort, below, always rejects permission work.
        emit({ event: 'native-drain-completion-notification-released', release, cleanupOnly: release.abort === true, owners: owners() });
        return value;
      });
    };
  }
  if (role === 'controlled') {
    const childProcess = require('node:child_process');
    const spawn = childProcess.spawn;
    childProcess.spawn = function(...args) {
      const child = spawn.apply(this, args);
      const options = args[2];
      if (options?.detached && options.env?.CODEGRAPH_DAEMON_INTERNAL === '1') {
        emit({ event: 'candidate-created', candidatePid: child.pid, argv: args[1], stdio: options.stdio,
          stdoutPipe: child.stdout !== null, stderrPipe: child.stderr !== null, owners: owners() });
        child.on('error', error => emit({ event: 'candidate-error', candidatePid: child.pid, error: String(error) }));
        child.on('exit', (code, signal) => emit({ event: 'candidate-exit', candidatePid: child.pid, code, signal }));
        child.on('close', (code, signal) => emit({ event: 'candidate-close', candidatePid: child.pid, code, signal,
          connected: child.connected, channelReleased: child.channel == null, owners: owners() }));
      }
      return child;
    };
    const { MCPServer } = require(path.join(mcpDir, 'index'));
    const prepare = MCPServer.prototype.prepareHandshake;
    MCPServer.prototype.prepareHandshake = async function(signal, ...args) {
      const descriptor = await prepare.call(this, signal, ...args);
      if (descriptor.kind !== 'proxy') return descriptor;
      const original = descriptor.deps.getDaemonSocket;
      let calls = 0, initiallyAttached = false, held = false;
      descriptor.deps.getDaemonSocket = async function(...socketArgs) {
        const ordinal = ++calls;
        emit({ event: 'acquisition-enter', ordinal, initiallyAttached, owners: owners() });
        if (fs.existsSync(process.env.QUERY_UPGRADE_ARM)) {
          if (!initiallyAttached || held) throw new Error('upgrade gate is not the first post-loss acquisition');
          held = true;
          const arm = JSON.parse(fs.readFileSync(process.env.QUERY_UPGRADE_ARM, 'utf8'));
          if (arm.clientPid !== process.pid || arm.root !== root) throw new Error('upgrade gate belongs to another actual session');
          emit({ event: 'first-postloss-acquisition-held', ordinal, arm, owners: owners() });
          const release = await waitFile(process.env.QUERY_UPGRADE_ACQUISITION_RELEASE, signal);
          if (release.abort) throw new Error('parent abandoned upgrade acquisition proof');
          if (release.clientPid !== process.pid || release.ownerPid !== arm.ownerPid) throw new Error('unproved upgrade release identity');
          try { process.kill(arm.ownerPid, 0); throw new Error('old owner still alive at genuine-unavailability release'); }
          catch (error) { if (error.code !== 'ESRCH') throw error; }
          const observed = owners();
          if (Object.values(observed).some(value => value !== null)) throw new Error('old owner records remain at acquisition release');
          emit({ event: 'first-postloss-acquisition-delegated-after-absence', ordinal, release, owners: observed });
        }
        const socket = await original.apply(this, socketArgs);
        if (socket) initiallyAttached = true;
        emit({ event: 'actual-acquisition-returned', ordinal, socket: !!socket, owners: owners() });
        return socket;
      };
      return descriptor;
    };
  }
}
