/**
 * MCP per-connection session — speaks the JSON-RPC protocol (initialize,
 * tools/list, tools/call) over a single {@link JsonRpcTransport}. It owns
 * per-client state only (which protocol version the client asked for, whether
 * it advertised `roots`, the one-shot roots/list latch); the heavyweight
 * resources (CodeGraph, watcher, ToolHandler) live in the shared
 * {@link MCPEngine} so daemon mode can collapse N inotify sets / DB handles
 * to one.
 *
 * The state-machine itself mirrors what `MCPServer` used to do inline before
 * issue #411 split it out — the same regression tests in
 * `__tests__/mcp-initialize.test.ts` still drive this code path.
 */

import * as path from 'path';
import { randomUUID } from 'crypto';
import { createQueryRequestContext } from './query-context';
import { JsonRpcRequest, JsonRpcNotification, JsonRpcTransport, ErrorCodes } from './transport';
import { MCPEngine } from './engine';
import { tools } from './tools';
import { SERVER_INSTRUCTIONS, SERVER_INSTRUCTIONS_PENDING } from './server-instructions';
import { CodeGraphPackageVersion } from './version';
import { getTelemetry, ClientInfo } from '../telemetry';
import { getUpdateNotice } from '../upgrade/update-check';
import { ExploreSessionState } from './explore-session-state';
import { isServicePolicyRefusal, type ServicePolicyRefusal } from './query-service-compatibility';

/**
 * MCP Server Info — kept on the session because some clients log it. The
 * version tracks the real package version (was a hard-coded '0.1.0').
 */
// Exported so the proxy can answer `initialize` locally with the IDENTICAL
// payload the daemon would send — no drift between the two handshake paths.
export const SERVER_INFO = {
  name: 'codegraph',
  version: CodeGraphPackageVersion,
};

/**
 * Instructions for the `initialize` response, with the update-availability
 * notice appended when one is known (#1243). Exported so the proxy's local
 * handshake sends the IDENTICAL payload — same convention as SERVER_INFO.
 * `getUpdateNotice` is a memoized synchronous cache read, so the #172
 * respond-fast contract holds; when no notice exists the instructions are
 * byte-identical to the bare constants.
 *
 * Test-authoring note: on a machine whose real `~/.codegraph` cache knows a
 * newer release, spawned servers append the notice — a test asserting exact
 * instructions equality must set `CODEGRAPH_NO_UPDATE_CHECK=1` in the spawn
 * env or it will fail only in the weeks after a release ships.
 */
export function initializeInstructions(base: string, notice: string | null = getUpdateNotice()): string {
  if (!notice) return base;
  return (
    `${base}\n\n---\n${notice} This server keeps running the old version until ` +
    `the user upgrades — mention it when convenient; do not run the upgrade yourself.`
  );
}

/** MCP Protocol Version (latest the server claims). */
export const PROTOCOL_VERSION = '2024-11-05';

/**
 * How long to wait for the client's `roots/list` response before giving up
 * and falling back to the process cwd.
 */
const ROOTS_LIST_TIMEOUT_MS = 5000;

/**
 * Convert a file:// URI to a filesystem path. Handles URL encoding and
 * Windows drive letter paths.
 */
function fileUriToPath(uri: string): string {
  try {
    const url = new URL(uri);
    let filePath = decodeURIComponent(url.pathname);
    if (process.platform === 'win32' && /^\/[a-zA-Z]:/.test(filePath)) {
      filePath = filePath.slice(1);
    }
    return path.resolve(filePath);
  } catch {
    return uri.replace(/^file:\/\/\/?/, '');
  }
}

/** First usable filesystem path from a `roots/list` result, or null. */
function firstRootPath(result: unknown): string | null {
  if (!result || typeof result !== 'object') return null;
  const roots = (result as { roots?: unknown }).roots;
  if (!Array.isArray(roots) || roots.length === 0) return null;
  const first = roots[0] as { uri?: unknown };
  if (typeof first?.uri !== 'string') return null;
  return fileUriToPath(first.uri);
}

export interface MCPSessionOptions {
  /**
   * Explicit project path from the `--path` CLI flag. When set, the session
   * will not bother asking the client for `roots/list` — we already know
   * where the project lives.
   */
  explicitProjectPath?: string | null;
}

/**
 * One MCP client's view of the server. Created fresh per stdio launch
 * (direct mode) or per socket connection (daemon mode).
 */
export class MCPSession {
  private clientSupportsRoots = false;
  /** From the initialize handshake — attributes usage rollups to the agent host. */
  private clientInfo: ClientInfo | undefined;
  private rootsAttempted = false;
  private resolvePromise: Promise<void> | null = null;
  private explicitProjectPath: string | null;
  // Retain client selection even when its first attempt finds no index. A
  // later retry must not silently substitute the launcher's CLI/cwd hint.
  private selectedProjectHint: string | null = null;
  private initialDefaultRefusal: ServicePolicyRefusal | null = null;
  /**
   * What `codegraph_explore` has already returned to THIS client, per project
   * (CG-17). Owned by the session, not the engine: the daemon shares one engine
   * (and one ToolHandler, and a pool of worker threads) across every connected
   * client, so state kept over there would blend two agents' histories and let
   * one session's calls suppress source the other has never seen. It dies with
   * the session — a reconnecting client starts clean.
   */
  private readonly exploreSession = new ExploreSessionState();
  private readonly sessionId = randomUUID();
  private closed = false;
  private readonly requests = new Map<string | number, { controller: AbortController; commit?: () => void }>();

  constructor(
    private transport: JsonRpcTransport,
    private engine: MCPEngine,
    opts: MCPSessionOptions = {},
  ) {
    this.explicitProjectPath = opts.explicitProjectPath ?? null;
    this.transport.onClose?.(() => this.endSession());
  }

  /**
   * Start handling messages from the transport. Returns immediately — the
   * session lives for as long as the transport is open.
   */
  start(): void {
    this.transport.start(this.handleMessage.bind(this));
  }

  /**
   * Tear down the session. Does NOT touch the engine (the engine may serve
   * other sessions) or call `process.exit` (the daemon decides when to exit).
   */
  stop(): void {
    this.endSession();
    this.transport.stop();
  }

  private endSession(): void {
    if (this.closed) return;
    this.closed = true;
    for (const request of this.requests.values()) request.controller.abort();
    this.requests.clear();
    this.engine.cancelQuerySession(this.sessionId);
  }

  /** Underlying transport — exposed for daemon-side close hooks. */
  getTransport(): JsonRpcTransport {
    return this.transport;
  }

  /**
   * This session's explore call history (CG-17). Exposed so tests can assert
   * that two sessions on one daemon keep separate state; nothing in the server
   * reaches for another session's copy.
   */
  getExploreSessionState(): ExploreSessionState {
    return this.exploreSession;
  }

  /** Terminal only for the actual engine's initial-default outcome. Static
   * discovery stays usable; this session retains sole tool-response ownership. */
  refuseInitialDefault(refusal: ServicePolicyRefusal): void {
    if (this.closed || this.initialDefaultRefusal) return;
    this.initialDefaultRefusal = refusal;
    for (const [id, lifetime] of this.requests) {
      this.requests.delete(id);
      lifetime.controller.abort();
      this.sendInitialDefaultRefusal(id);
    }
    this.engine.cancelQuerySession(this.sessionId);
  }

  private sendInitialDefaultRefusal(id: string | number): void {
    const refusal = this.initialDefaultRefusal;
    if (!refusal) return;
    // A broken outbound stream must not prevent aborting the other owned
    // requests or replace the real permission refusal in the engine callback.
    try {
      this.transport.sendError(id, ErrorCodes.InternalError, refusal.message,
        { code: refusal.code, reason: refusal.reason, projectRoot: refusal.projectRoot, guidance: refusal.guidance });
    } catch (error) {
      try { process.stderr.write(`[CodeGraph MCP] Terminal refusal delivery failed: ${String(error).slice(0, 512)}.\n`); }
      catch { /* outbound streams are already unavailable */ }
    }
  }

  private assertDefaultResolutionLive(): void {
    if (this.initialDefaultRefusal) throw this.initialDefaultRefusal;
    if (this.closed) throw new Error('MCP session closed during project resolution');
  }

  private async handleMessage(message: JsonRpcRequest | JsonRpcNotification): Promise<void> {
    if (this.closed) return;
    const isRequest = 'id' in message;
    switch (message.method) {
      case 'notifications/cancelled': {
        if (isRequest) break;
        const id = (message.params as { requestId?: unknown } | undefined)?.requestId;
        if (typeof id === 'string' || typeof id === 'number') {
          this.requests.get(id)?.controller.abort();
          this.requests.delete(id);
        }
        break;
      }
      case 'initialize':
        if (isRequest) await this.handleInitialize(message as JsonRpcRequest);
        break;
      case 'initialized':
        // Notification that client has finished initialization — no action needed.
        break;
      case 'tools/list':
        if (isRequest) await this.handleToolsList(message as JsonRpcRequest);
        break;
      case 'tools/call':
        if (isRequest) await this.handleToolsCall(message as JsonRpcRequest);
        break;
      case 'ping':
        if (isRequest) this.transport.sendResult((message as JsonRpcRequest).id, {});
        break;
      case 'resources/list':
        // We expose no MCP resources, but some clients (opencode, Codex) probe
        // for them on connect; reply with an empty list instead of a
        // MethodNotFound error that surfaces as a scary `-32601` log line. (#621)
        if (isRequest) this.transport.sendResult((message as JsonRpcRequest).id, { resources: [] });
        break;
      case 'resources/templates/list':
        if (isRequest) this.transport.sendResult((message as JsonRpcRequest).id, { resourceTemplates: [] });
        break;
      case 'prompts/list':
        // Likewise — no prompts exposed, but answer the probe cleanly. (#621)
        if (isRequest) this.transport.sendResult((message as JsonRpcRequest).id, { prompts: [] });
        break;
      default:
        if (isRequest) {
          this.transport.sendError(
            (message as JsonRpcRequest).id,
            ErrorCodes.MethodNotFound,
            `Method not found: ${message.method}`,
          );
        }
    }
  }

  private async handleInitialize(request: JsonRpcRequest): Promise<void> {
    const params = request.params as {
      rootUri?: string;
      workspaceFolders?: Array<{ uri: string; name: string }>;
      capabilities?: { roots?: unknown };
      clientInfo?: { name?: unknown; version?: unknown };
    } | undefined;

    this.clientSupportsRoots = !!params?.capabilities?.roots;
    if (params?.clientInfo) {
      this.clientInfo = {
        name: typeof params.clientInfo.name === 'string' ? params.clientInfo.name : undefined,
        version: typeof params.clientInfo.version === 'string' ? params.clientInfo.version : undefined,
      };
    }

    // Explicit project signal, strongest first: client-provided rootUri /
    // workspaceFolders (LSP-style), else the --path the server was launched
    // with. cwd is NOT used here — we defer it so a roots/list answer can
    // win over it. See issue #196.
    let explicitPath: string | null = null;
    if (params?.rootUri) {
      explicitPath = fileUriToPath(params.rootUri);
    } else if (params?.workspaceFolders?.[0]?.uri) {
      explicitPath = fileUriToPath(params.workspaceFolders[0].uri);
    } else if (this.explicitProjectPath) {
      explicitPath = this.explicitProjectPath;
    }
    if (params?.rootUri || params?.workspaceFolders?.[0]?.uri) this.selectedProjectHint = explicitPath;
    else this.selectedProjectHint ??= explicitPath;

    // No schema probe chooses instructions before native caller permission.
    // Only an already verified own index supports claiming a ready default.
    const indexed = this.engine.hasVerifiedDefaultRoute();

    // Respond to the handshake BEFORE doing any heavy init — see issue #172.
    this.transport.sendResult(request.id, {
      protocolVersion: PROTOCOL_VERSION,
      capabilities: { tools: {} },
      serverInfo: SERVER_INFO,
      instructions: initializeInstructions(indexed ? SERVER_INSTRUCTIONS : SERVER_INSTRUCTIONS_PENDING),
    });

    const selected = this.selectedProjectHint ?? explicitPath;
    if (selected && !this.initialDefaultRefusal) {
      // Kick off engine init in the background. If another session in the
      // same daemon already opened the project, `ensureInitialized` is a
      // ~free no-op — N concurrent clients pay exactly one open.
      this.resolvePromise = this.engine.ensureInitialized(selected);
      void this.resolvePromise.catch(() => { /* default calls retain the original typed refusal */ });
    }
  }

  private async handleToolsList(request: JsonRpcRequest): Promise<void> {
    // Always expose the tools — even when the server root has no index. Gating
    // availability on whether `./` is indexed (the old behavior) breaks the
    // monorepo case where only sub-projects carry a `.codegraph/` (the agent
    // saw zero tools and couldn't even reach an indexed sub-project by
    // `projectPath`), and it hides the tools from a session that started before
    // the user ran `codegraph init` (most hosts request the list once, so the
    // freshly-built index never surfaces). #964. The not-indexed case is still
    // safe: a call against an un-indexed path returns SUCCESS-shaped guidance
    // ("pass projectPath / run codegraph init"), never `isError`, so it can't
    // teach the agent to abandon codegraph. `getTools()` returns the default
    // surface even before a project is open.
    this.transport.sendResult(request.id, {
      tools: this.engine.getToolHandler().getTools(),
    });
  }

  private async handleToolsCall(request: JsonRpcRequest): Promise<void> {
    if (this.initialDefaultRefusal) { this.sendInitialDefaultRefusal(request.id); return; }
    if (this.requests.has(request.id)) return; // duplicate live ids cannot produce two answers
    const lifetime = { controller: new AbortController(), commit: undefined as (() => void) | undefined };
    this.requests.set(request.id, lifetime);
    try {
      const params = request.params as {
        name: string;
        arguments?: Record<string, unknown>;
      };

      if (!params || !params.name) {
        this.transport.sendError(request.id, ErrorCodes.InvalidParams, 'Missing tool name');
        return;
      }

      const toolName = params.name;
      const toolArgs = params.arguments || {};
      const context = createQueryRequestContext(this.engine.getQueryExecutionContext(), this.sessionId, request.id);

      const tool = tools.find((t) => t.name === toolName);
      if (!tool) {
        this.transport.sendError(
          request.id,
          ErrorCodes.InvalidParams,
          `Unknown tool: ${toolName}`,
        );
        return;
      }

      if (process.env.CODEGRAPH_MCP_DEBUG) process.stderr.write(`[mcp-debug] toolsCall ${toolName} id=${String(request.id)} pre-init\n`);
      // Speculative default failure cannot poison an independently admitted
      // explicit target; ToolHandler performs that target's own permission.
      if (typeof toolArgs.projectPath !== 'string') await this.retryInitIfNeeded();
      if (lifetime.controller.signal.aborted || this.closed) return;

      if (process.env.CODEGRAPH_MCP_DEBUG) process.stderr.write(`[mcp-debug] toolsCall ${toolName} id=${String(request.id)} dispatch\n`);
      const result = await this.engine.getToolHandler().execute(toolName, toolArgs, this.exploreSession, context, {
        signal: lifetime.controller.signal, deferDelivery: commit => { lifetime.commit = commit; },
      });
      if (process.env.CODEGRAPH_MCP_DEBUG) process.stderr.write(`[mcp-debug] toolsCall ${toolName} id=${String(request.id)} done\n`);
      if (this.closed || lifetime.controller.signal.aborted || this.requests.get(request.id) !== lifetime) return;
      const delivered = this.transport.sendResult(request.id, result);
      if (delivered === false) return;
      lifetime.commit?.();
      // After the reply is on the wire — telemetry must never delay a tool
      // response (in-memory increment only; see src/telemetry).
      getTelemetry().recordUsage('mcp_tool', toolName, !result.isError, this.clientInfo);
    } catch (error) {
      if (!this.closed && !lifetime.controller.signal.aborted && this.requests.get(request.id) === lifetime) {
        this.transport.sendError(request.id, ErrorCodes.InternalError, error instanceof Error ? error.message : String(error),
          isServicePolicyRefusal(error) ? { code: error.code, reason: error.reason, projectRoot: error.projectRoot, guidance: error.guidance } : undefined);
      }
    } finally {
      if (this.requests.get(request.id) === lifetime) this.requests.delete(request.id);
    }
  }

  /**
   * Lazy default-project resolution. Three layers:
   *   1. await the in-flight init kicked off from `handleInitialize` (if any);
   *   2. if still uninitialized and we never asked the client for its roots,
   *      do so now (one-shot); fall back to cwd if the client lacks roots;
   *   3. last-resort: re-walk from the best candidate — picks up projects
   *      that were `codegraph init`'d *after* the server started.
   */
  private async retryInitIfNeeded(): Promise<void> {
    this.assertDefaultResolutionLive();
    if (this.resolvePromise) {
      const pending = this.resolvePromise;
      try { await pending; }
      catch (error) { if (isServicePolicyRefusal(error)) throw error; }
      finally { if (this.resolvePromise === pending) this.resolvePromise = null; }
    }
    this.assertDefaultResolutionLive();

    if (this.engine.hasDefaultCodeGraph()) return;

    const hint = this.selectedProjectHint ?? this.explicitProjectPath ?? this.engine.getProjectPath();
    if (!hint && !this.rootsAttempted) {
      this.rootsAttempted = true;
      this.resolvePromise = this.clientSupportsRoots
        ? this.initFromRoots()
        : this.engine.ensureInitialized(process.cwd());
      const pending = this.resolvePromise;
      try { await pending; }
      catch (error) { if (isServicePolicyRefusal(error)) throw error; }
      finally { if (this.resolvePromise === pending) this.resolvePromise = null; }
      this.assertDefaultResolutionLive();
      if (this.engine.hasDefaultCodeGraph()) return;
    }

    // Last resort: await permission from the best candidate. Picks up
    // projects that appeared after the server started.
    // roots/list (or another initialize) can select a stronger hint while the
    // preceding await is pending; read the retained selection AFTER that wait.
    const candidate = this.selectedProjectHint ?? this.explicitProjectPath ?? this.engine.getProjectPath() ?? process.cwd();
    this.assertDefaultResolutionLive();
    await this.engine.retryInitialize(candidate);
  }

  /**
   * Ask the client for its workspace root via `roots/list` and open the
   * first one. Falls back to `process.cwd()` on timeout or empty answer.
   */
  private async initFromRoots(): Promise<void> {
    let target = process.cwd();
    try {
      const result = await this.transport.request('roots/list', undefined, ROOTS_LIST_TIMEOUT_MS);
      const rootPath = firstRootPath(result);
      if (rootPath) {
        target = rootPath;
        this.selectedProjectHint ??= rootPath;
      } else {
        process.stderr.write('[CodeGraph MCP] Client returned no workspace roots; falling back to process cwd.\n');
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      process.stderr.write(`[CodeGraph MCP] roots/list request failed (${msg}); falling back to process cwd.\n`);
    }
    this.assertDefaultResolutionLive();
    target = this.selectedProjectHint ?? target;
    await this.engine.ensureInitialized(target);
  }
}
