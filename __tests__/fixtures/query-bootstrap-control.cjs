'use strict';

// Keep the real child inert until its native creation identity has been acquired.
// This fixture channel is separate from the bootstrap IPC protocol under test.
module.exports = function bootstrapControl() {
  let beginResolve, releaseResolve;
  const begin = new Promise(resolve => { beginResolve = resolve; });
  const release = new Promise(resolve => { releaseResolve = resolve; });
  let pending = '', seenBytes = 0, begun = false;
  const fail = error => process.stderr.write(Buffer.from(String(error), 'utf8').subarray(0, 4096), () => process.exit(1));
  process.stdin.on('error', fail);
  process.stdin.on('data', bytes => {
    seenBytes += bytes.length;
    if (seenBytes > 64) { fail('Native fixture control exceeded 64 bytes'); return; }
    pending += bytes.toString('utf8');
    for (let newline; (newline = pending.indexOf('\n')) !== -1;) {
      const token = pending.slice(0, newline);
      pending = pending.slice(newline + 1);
      if (token === 'begin' && !begun) { begun = true; beginResolve(true); }
      else if (token === 'cancel' && !begun) beginResolve(false);
      else if (token === 'release' && begun) releaseResolve();
      else fail('Unexpected native fixture control token');
    }
  });
  return { begin, release };
};
