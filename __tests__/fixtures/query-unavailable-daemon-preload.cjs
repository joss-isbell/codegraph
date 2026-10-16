// Reuse one genuine native candidate-unavailability boundary across public
// tests. The process exits before service locks, construction or listening.
module.exports = record => {
  if (process.env.QUERY_UNLIMITED_FAIL_DAEMON !== '1') return;
  const childProcess = require('node:child_process');
  const spawn = childProcess.spawn;
  childProcess.spawn = function(command, args, options) {
    if (!options?.detached || options.env?.CODEGRAPH_DAEMON_INTERNAL !== '1') return spawn.call(this, command, args, options);
    const child = spawn.call(this, command, ['-e', 'process.exit(78)'], options);
    record({ event: 'unavailable-candidate', candidatePid: child.pid, reason: 'controlled exit 78 before bind' });
    child.on('exit', code => record({ event: 'unavailable-candidate-exit', candidatePid: child.pid, code }));
    return child;
  };
};
