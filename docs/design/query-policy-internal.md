# Internal query policy and request context (Issue #9)

This is an internal preparation seam. Public startup still uses the original
permissive parser, finite concurrency, 45-second pooled busy timer and ordinary
retention. `CODEGRAPH_QUERY_MODE` does not activate a preset in this change.
Read-only fallback remains unpooled. Service compatibility and replacement
behavior are unchanged. No installation or live service is changed.

## Policy boundary

- `query-policy.ts` keeps raw `RequestedQueryPolicy` separate from immutable
  `ResolvedQueryPolicy`. `resolveQueryPolicy` implements the settled strict
  contract. It is not the public environment adapter until Issue #15.
- `resolveLegacyQueryPolicy` snapshots existing settings at engine construction.
  It preserves numeric coercion and flooring,
  ignored malformed input and legacy timeout behavior. It deliberately ignores
  the new mode input. Its direct/writable defaults are bounded to two workers;
  shared-daemon defaults use cores minus one bounded to 1–16. Issue #10 removes
  both explicit finite override clamps; supported finite maxima are exact lazy
  demand ceilings. Read-only defaults
  disable pooling. `resolvePoolSize` remains the old parser's compatibility seam.
- Strict resolution uses decimal digits only, trimmed case-sensitive sentinels,
  exact safe pool integers 0–9,007,199,254,740,991 and finite deadlines
  1,000–2,147,483,647 ms. An explicitly disabled pool has an inapplicable deadline;
  its still-validated timeout input is retained separately. Overrides are
  independent. The unlimited preset or unlimited pool override requests idle
  retirement; disabling only the timer does not. Classification is
  `default`/`custom`/`unlimited`, separate from deadline applicability.
- `MCPEngineOptions.queryPolicy` accepts an already-resolved internal policy.
  The engine, dispatcher, pool and worker receive the same immutable policy and
  authority. A supplied policy bypasses legacy pool/env reconstruction and caps.
  `QueryPool` represents an absent timer with `null`, never a huge finite timer.
  Startup pacing remains distinct from the maximum. Unlimited lifecycle proof
  belongs to the later lifecycle tickets; this interface alone is not acceptance.
- `getQueryExecutionState` separates resolution from current construction and
  dispatch: read-only/disabled execution reports main-thread inapplicability.
  A constructed cold/recovering pool reports worker dispatch with `ready:false`;
  permanent failure reports unavailable dispatch and a failed lifecycle, never a
  main-thread escape. Actual retention remains ordinary until Issue #14. This is an
  internal diagnostics input, not an additional MCP tool or public claim.
- `connectWithHello` resolves a prospective **shared-daemon** policy, independently
  of a direct/fallback engine, and sends it in the internal client hello. Issue
  #16 must compare it with actual service facts. This change does not compare,
  reject, mutate or replace services based on policy.

## Execution and ownership boundary

- `QueryExecutionContext` is clone-safe and recursively frozen. Its generation
  identifies an engine, its capability distinguishes read-only authority, and
  its ownership identifies whether that engine owns writer/synchronization work.
  Ownership describes responsibility; it does not assert that a project writer
  lock has already been acquired before lazy initialization.
- `MCPSession` allocates a session id once and a fresh request/admission context
  before initialization waits. Proxy fallback keeps its session id across local
  engine retirement; each new engine has its own generation. Explore history
  remains in the existing session-owned `ExploreSessionState` and clone-safe args.
  Session disconnect/cancellation settlement and replay accounting follow in #12.
- The dispatcher resolves the actual project after catch-up and captures its
  canonical root/index plus `default`/`explicit` origin, including default roots
  reached through directory aliases. Worker-wire `projectPath` does not
  change that origin. Queued jobs retain the context across retries. The worker
  freezes its cloned context, verifies engine generation, and executes against
  the admitted project rather than reinterpreting a mutable wire path. Every
  read handler receives the verified own-root connection directly, without
  another nearest-index walk. Missing/invalid own indexes or a changed canonical
  root/index fail closed with recoverable guidance, including linked worktrees
  beneath an indexed ancestor. A valid replacement at the same own index keeps
  the existing reopen behavior. Responses carry context only on the internal worker
  envelope; MCP results expose no request/session/ownership side channel.
- Workers receive read-only capability for Issue #11's enforced read-only opener.
  This ticket does not enable public read-only pooling or change existing worker
  opening behavior. Main-thread read-only default and explicit routes continue
  through the engine's existing read-only project lifecycle.

## Worker readiness and capacity boundary (Issue #10)

- A pool's presence owns every validated read. Cold starts, a recovering empty
  ready set and terminal failure never select owner-thread execution. Status,
  catch-up, watcher notices and session emission handling stay on the main
  thread. Explicit pool zero remains the ordinary unpooled path.
- Failed default initialization starts a rootless pool, including the synchronous
  retry path, so a valid explicit project still receives real worker execution.
  The unavailable default keeps its existing owner-side guidance; workers do not
  initialize or repair it as a side effect of serving another project.
- Finite overrides above 16 are exact safe-integer demand ceilings. Defaults
  remain direct/writable maximum two and daemon cores-minus-one bounded to
  1–16. At most two workers start concurrently; ready workers continue growing
  on demand, so this pacing does not limit actual execution to two. A very large
  maximum never eagerly allocates that many workers.
- Readiness has its own 15-second startup allowance. It does not constrain
  healthy execution and does not depend on the optional busy timer. Factory
  failures, failed/never-ready opens, dispatch exceptions, errors and every exit
  code consume the bounded recovery budget. A running call retries once with
  the same admission identity and timer. Exhaustion settles outstanding callers
  truthfully, retires owned workers and rejects later admissions. No automatic
  replacement revives a failed or destroyed pool.
- `state` (`accepting`/`failed`/`destroyed`) and immutable `activity` observations
  are the internal lifecycle seams for #12 and #14. `activity.executing` includes
  work whose caller's finite timer expired; it stays occupied until actual
  completion or worker teardown. `activity.queued` counts only unsettled callers,
  `starting` counts readiness waits and `retiring` counts awaited owned teardown.
  Jobs already retain immutable per-request/session/generation context, and all
  caller outcomes share one idempotent settlement path. Issue #12 extends these
  observations with `draining`, cancellation and natural drain; idle retirement
  remains Issue #14.
- Results must match both the owned Worker object and its current job id.
  Duplicate readiness, stale/duplicate results and error/exit races cannot free
  another job's slot or deliver a second outcome. Expired jobs are never retried.
  Destroy is idempotent and awaits failed as well as active owned workers. It
  retains the Windows startup handshake/grace before termination. The real
  worker crash fixture collects before self-exit; failed initialization closes
  any connection it already opened.

## Request abandonment and handover (Issue #12)

- The immutable worker envelope retains engine generation, session, request and
  admission identities. Owner-only `QueryRequestLifetime` carries an AbortSignal
  separately; it is never cloned into a worker or stored in session-global state.
  Queued cancellation removes work; executing cancellation settles its caller
  once while the actual synchronous execution continues occupying its worker.
  Retry never replays a settled request. Late or duplicate results cannot release
  another job's slot or produce another response.
- Built-in stdio and socket transports notify session end exactly once on EOF,
  error, close or explicit stop. `MCPSession` handles the actual wire notification
  `notifications/cancelled` with `params.requestId`; a notification gets no reply.
  Session end also abandons that session's already-answered but still-executing
  work. A shared peer's requests, workers and explore history remain independent.
  After a shared explicit-project activation gate finishes, the owner checks the
  request lifetime again before status measurement, watcher recovery or dispatch;
  cancellation does not cancel the shared project activation itself.
  The tests inject that protocol notification at the socket/stdio seam and observe
  real disconnects. They do not establish which signal any particular host's Stop
  button sends. Unpooled synchronous work cannot receive a new event-loop signal
  until the synchronous call yields; this adds no host cancellation guarantee.
- Source emission is stripped from results on the owner, and its session-history
  commit is deferred until a live transport accepts the response for writing.
  Cancellation, a rejected write or session end discards that commit. There is no
  delivery acknowledgement from the MCP client; acceptance means handed to the
  connected transport, not proof the remote host consumed the bytes.
- The local-handshake proxy owns lifetimes for buffered, local and forwarded
  requests. Cancellation removes pending/inflight replay entries and aborts local
  dispatch. Closed and superseded socket attachments cannot deliver responses.
  Late results from a cancelled forwarded call are discarded, and daemon loss
  replays only still-live requests. Shutdown aborts these lifetimes before stopping
  its owned engines, so neither work nor an engine can resurrect after shutdown.
- Routine writable/read-only fallback handover stops new admission to the old
  engine and waits for admitted local calls and actual worker execution to finish
  naturally. `QueryPool.drainNaturally()` has no execution timer or promised drain
  duration. Database connections and writer ownership remain until execution and
  owned worker exit complete. An incomplete stop remains owned and is reported as
  incomplete; healthy work needing an interruption requires a new decision.
- `QUERY_CLEANUP_BOUNDS` version 1 declares provisional engineering bounds:
  **15 s drain after abandonment or intentional targeted shutdown**, a **distinct
  15 s startup-handshake allowance**, and **5 s final worker-exit cleanup**. These
  are not query deadlines. Abandoned synchronous execution gets a targeted worker
  retirement after its drain; another session's worker is not terminated. Capacity
  stays accounted for through actual completion or verified owned-worker exit.
  If targeted abandonment teardown rejects, the unexited worker and its finite
  slot remain owned. Healthy peers continue; the known incomplete teardown is
  surfaced by natural handover and final intentional destroy. Final destroy waits
  for every other owned termination before rejecting, and the engine retains its
  pool ownership reference when cleanup is incomplete.
  Intentional engine shutdown awaits all active/failed owned workers before owner
  database release. Worker identity is the exact owned Worker object, not a process
  name or project registry. Existing PID/creation checks and native process-tree
  ownership remain with the qualified Windows/Linux supervisor. No global service
  stop, new cancellation setting or public cancellation tool is added.
- Real SQLite workers prove cancellation, late completion, stuck targeted cleanup
  and healthy writable/read-only handover beyond the provisional drain interval
  with an internally absent busy timer. Host deadlines remain independent. Native
  Windows and Linux receipts, original failures, final worker exits and cleanup
  tails are retained; source or bound artifact changes invalidate an attempt.

## Validation boundary

The policy matrix, real SQLite/worker protocol, all four host-facing modes and
default/explicit project routes are checked beneath the already agreed seams.
Existing writer locks, catch-up, session emission history, fallback handover and
replacement suites remain regression evidence. Run all checks with the proved
`scripts/validation/runner.py`, using a separate complete build and bound current
`dist`, runtime and both dependency trees. `query-tests.py` uses Vitest 2's Node
API with an explicit empty workspace, no implicit build and an attempt-local Vite
cache. Its JSON result must contain every test tail; only predeclared exact
platform-gated skips are accepted. No held project or installed registry is used.
