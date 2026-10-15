/**
 * Query worker thread — issue: concurrent MCP tool calls starve the daemon.
 *
 * The shared daemon serves every session on ONE event loop with synchronous
 * `node:sqlite`. `codegraph_explore` is CPU-heavy (FTS + RWR/personalized-
 * PageRank + impact + output building) stitched together by microtask `await`s,
 * so N concurrent explores keep the microtask queue continuously full and
 * starve the macrotask phases — timers AND socket I/O. The transport freezes:
 * no response flushes, no request is read, until the whole batch drains. With
 * ~10 subagents that routinely exceeds the MCP client's request timeout.
 *
 * This worker moves the heavy read-tool dispatch OFF the daemon's main loop.
 * Each worker owns its OWN read connection (node:sqlite WAL allows N concurrent
 * readers across connections — verified: a worker reader sees the main writer's
 * committed catch-up/watcher writes), so {@link QueryPool} runs N tool calls in
 * true parallel up to core count while the main loop stays free for the MCP
 * transport. The worker runs {@link ToolHandler.executeReadTool} — validation +
 * dispatch + error classification — and returns the raw {@link ToolResult}; the
 * MAIN thread keeps the catch-up gate, the watcher-state notices (staleness /
 * worktree), `codegraph_status`, and telemetry, none of which a watcher-less
 * read connection can answer.
 */

import { parentPort, workerData } from 'worker_threads';
import type { ToolResult } from './tools';
import type { QueryExecutionContext, QueryRequestContext } from './query-context';
import { immutable } from './query-policy';
import { collectBeforeExit } from '../worker-teardown';
import type CodeGraph from '../index';

interface WorkerInit {
  root: string | null;
  execution?: QueryExecutionContext;
}

interface CallMessage {
  type: 'call';
  id: number;
  toolName: string;
  args: Record<string, unknown>;
  context?: QueryRequestContext;
}

// Mirror the engine's lazy-require of the heavy CodeGraph + tools chain. This
// module is only ever loaded as a Worker, so the require runs once on spawn.
const loadCodeGraph = (): typeof import('../index').default =>
  (require('../index') as typeof import('../index')).default;
const loadToolHandler = (): typeof import('./tools').ToolHandler =>
  (require('./tools') as typeof import('./tools')).ToolHandler;

if (parentPort) {
  const port = parentPort;
  const { root, execution } = immutable(workerData as WorkerInit);

  // Open the default project's READ connection once, if present. Other repos are
  // opened lazily on first cross-project (projectPath) call by the ToolHandler's
  // own per-handler cache. openSync does not start a watcher — workers are pure
  // readers; the single watcher/writer stays on the daemon's main thread.
  let handler: InstanceType<typeof import('./tools').ToolHandler> | null = null;
  let cg: CodeGraph | null = null;
  let initError: string | null = null;
  try {
    cg = root ? loadCodeGraph().openSync(root, { readOnly: execution?.capability === 'read-only' }) : null;
    handler = new (loadToolHandler())(cg, execution);
  } catch (err) {
    cg?.close();
    initError = err instanceof Error ? err.message : String(err);
  }

  // Tell the pool we're up. `ok:false` lets the pool count a hard open failure
  // against its recovery budget without hanging or changing execution paths.
  port.postMessage({ type: 'ready', ok: initError === null, error: initError });

  port.on('message', (msg: CallMessage) => {
    if (!msg || msg.type !== 'call') return;
    void serve(msg);
  });

  const serve = async (msg: CallMessage): Promise<void> => {
    const context = msg.context ? immutable(msg.context) : undefined;
    // Test-only crash hook so the pool's worker-recovery path is exercisable
    // deterministically. Gated behind an env flag only the suite sets — inert in
    // normal operation (and `__test_crash__` isn't a real tool name anyway).
    if (msg.toolName === '__test_crash__' && process.env.CODEGRAPH_QUERY_WORKER_ALLOW_TEST_CRASH === '1') {
      collectBeforeExit();
      process.exit(13);
    }
    if (!handler) {
      port.postMessage({
        type: 'result',
        id: msg.id,
        context,
        result: errorResult(`codegraph worker could not open the project: ${initError}`),
      });
      return;
    }
    try {
      // executeReadTool already classifies NotIndexed/PathRefusal/internal errors
      // into a ToolResult and never throws — the catch is belt-and-suspenders.
      const result: ToolResult = await handler.executeReadTool(msg.toolName, msg.args, context);
      port.postMessage({ type: 'result', id: msg.id, result, context });
    } catch (err) {
      port.postMessage({
        type: 'result',
        id: msg.id,
        context,
        result: errorResult(err instanceof Error ? err.message : String(err)),
      });
    }
  };
}

function errorResult(text: string): ToolResult {
  return { isError: true, content: [{ type: 'text', text }] };
}
