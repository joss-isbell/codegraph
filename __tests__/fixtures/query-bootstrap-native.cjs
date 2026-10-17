'use strict';

// A real native candidate with no service authority: promotion is an observable
// fixture event, and is impossible until a protocol1 acknowledgment arrives.
let promoted = false;
let finishing = false;
const mode = process.argv[2] || 'permitted';
const control = require('./query-bootstrap-control.cjs')();
const writeAndExit = (event, exitCode = 0) => {
  if (finishing) return;
  finishing = true;
  process.stdout.write(JSON.stringify({ event, pid: process.pid }) + '\n', (error) => {
    process.exit(error ? 1 : exitCode);
  });
};
(async () => {
  if (!await control.begin) { writeAndExit('cancelled'); return; }
  process.on('message', (message) => {
    if (mode !== 'permitted-exit' && !promoted && message && message.protocol === 1 && message.kind === 'acknowledged') {
      promoted = true;
      writeAndExit('promoted');
      if (process.connected) process.disconnect();
    }
  });
  process.on('disconnect', () => {
    if (mode !== 'live-disconnect' && !promoted) writeAndExit('promotion-denied');
  });
  if (mode === 'pre-service-exit') writeAndExit('pre-service-exit', 78);
  else if (mode === 'live-disconnect') {
    control.release.then(() => writeAndExit('controlled-exit'));
    process.disconnect();
  } else if (mode !== 'silent') {
    let status = { protocol: 1, kind: 'permitted' };
    if (mode === 'malformed') status = { protocol: 2, kind: 'permitted' };
    if (mode === 'malformed-refusal') status = { protocol: 1, kind: 'refused', refusal: { reason: 'unknown' } };
    if (mode === 'malformed-compact') status.detailsOmitted = true;
    if (mode === 'oversized') status.padding = '€'.repeat(30_000);
    if (mode === 'exact-bound') {
      status.padding = '';
      status.padding = 'x'.repeat(65_536 - Buffer.byteLength(JSON.stringify(status), 'utf8'));
    }
    const send = () => process.send(status, (error) => {
      if (error) process.stderr.write(String(error).slice(0, 4096) + '\n', () => process.exit(1));
      else if (mode === 'permitted-exit') writeAndExit('pre-service-exit', 78);
    });
    send();
    if (mode === 'duplicate') { send(); send(); }
  }
})().catch(error => process.stderr.write(Buffer.from(String(error), 'utf8').subarray(0, 4096), () => process.exit(1)));
