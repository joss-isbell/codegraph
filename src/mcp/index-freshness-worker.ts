/** Exact CLI-parity change count, isolated from the MCP transport event loop. */
import { parentPort, workerData } from 'worker_threads';
import { collectBeforeExit } from '../worker-teardown';
import { assertMcpIndexRoute, type McpIndexRoute } from '../directory';

if (parentPort) {
  const port = parentPort;
  let cg: import('../index').default | null = null;
  let counts: { added: number; modified: number; removed: number } | null = null;
  const input: { root: string; projectRoute?: McpIndexRoute } = workerData;
  try {
    const CodeGraph = (require('../index') as typeof import('../index')).default;
    // Loaded: from here on the owner may terminate this worker. Ending it while
    // it loads those modules can crash the process on Windows (worker-teardown.ts).
    port.postMessage({ type: 'loaded' });
    if (input.projectRoute) {
      if (input.projectRoute.root !== input.root) throw new Error('Measurement worker route differs from its root.');
      assertMcpIndexRoute(input.projectRoute);
    }
    cg = CodeGraph.openSync(input.root, { readOnly: true });
    const changes = cg.getChangedFiles();
    counts = {
      added: changes.added.length,
      modified: changes.modified.length,
      removed: changes.removed.length,
    };
  } catch {
    // A failed or timed-out measurement must never masquerade as zero changes.
  } finally {
    try { cg?.close(); }
    catch (error) {
      counts = null;
      process.stderr.write(`[CodeGraph MCP] Measurement graph cleanup failed: ${String(error).slice(0, 2048)}\n`);
    }
  }
  // The owner terminates this worker on the answer: no GC marking in flight then.
  collectBeforeExit();
  port.postMessage({ type: 'counts', counts });
}
