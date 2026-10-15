# Internal query policy and request context (Issue #9)

This is an internal preparation seam. Public startup still uses the original
permissive parser, finite concurrency, 45-second pooled busy timer and ordinary
retention. `CODEGRAPH_QUERY_MODE` does not activate a preset in this change.
Read-only fallback remains unpooled by default; Issue #11 permits an explicitly
selected positive finite pool. Service compatibility and replacement
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
  disable pooling, unless a valid positive finite override explicitly selects
  local read-only workers. `resolvePoolSize` remains the old parser's compatibility seam.
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
- Workers receive read-only capability. Default worker opens and every lazy
  explicit-project open enforce it; replacement opens preserve the connection's
  capability. Main-thread read-only default and explicit routes continue through
  the engine's read-only project lifecycle.

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
  caller outcomes share one idempotent settlement path. Cancellation, natural
  drain and idle retirement are not implemented here.
- Results must match both the owned Worker object and its current job id.
  Duplicate readiness, stale/duplicate results and error/exit races cannot free
  another job's slot or deliver a second outcome. Expired jobs are never retried.
  Destroy is idempotent and awaits failed as well as active owned workers. It
  retains the Windows startup handshake/grace before termination. The real
  worker crash fixture collects before self-exit; failed initialization closes
  any connection it already opened.

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

## Read-only fallback boundary (Issue #11)

- The existing finite pool-size setting now selects read-only workers explicitly.
  Unset/malformed/nonapplicable selections stay unpooled; zero keeps main-thread
  reads and an inapplicable pool deadline. Legacy permissive numeric parsing
  remains until #15. A constructed finite pool remains authoritative while cold
  or recovering, using #10's timer-independent failure contract.
- A read-only engine's default, explicit, lazy worker and replacement connections
  stay read-only. It acquires no writer/synchronization lease, starts no watcher,
  performs no catch-up, migration, bulk-load repair or WAL maintenance, and does
  not repair a missing or outdated `.codegraph/.gitignore`. Ordinary writable
  validation still repairs that metadata. Rejected native database opens close
  their opened handle and do not retry with writable authority.
- Internal execution facts identify `read-only-fallback`, read-only capability,
  absent writer/synchronization ownership and `local-engine` resource scope.
  These fallback resources belong to each local engine; they are exceptional
  resources rather than a service shared across chats. Existing shared-daemon
  facts identify `shared-project` scope. No additional MCP diagnostic or knob was
  added.
- Read-only activation remains finite-only in `maybeStartPool`. A supplied
  internal unlimited policy remains represented as requested/resolved, while
  actual execution stays unpooled. #14/#15 own lifting this guard once the
  unlimited lifecycle/preset is complete. Public unlimited preset uptake,
  cancellation/drain and service compatibility remain downstream.
- Real worker/SQLite tests cover cold default, same/other explicit and rootless
  routes, directory aliases, session emission stripping/isolation, genuine CLI
  service-unavailable fallback beside a live native-watching writer, unchanged
  schema/content/WAL/ancillary metadata during reader-only intervals and owner
  changes before and after reader cleanup. POSIX replacement tests exercise live
  open inode replacement; Windows cannot unlink an open database and uses the
  ordinary platform guard. All checks use the proved supervisor and receipt
  consumer; no live held index or installed service is touched.
