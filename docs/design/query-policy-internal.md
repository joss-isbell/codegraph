# Internal query policy and request context (Issue #9)

This is an internal preparation seam. Public startup still uses the original
permissive numeric parser, finite concurrency, default 45-second pooled busy timer and ordinary
retention. `CODEGRAPH_QUERY_MODE` does not activate a preset in this change.
Read-only fallback remains unpooled. Service compatibility and replacement
behavior are unchanged. No installation or live service is changed.

## Policy boundary

- `query-policy.ts` keeps raw `RequestedQueryPolicy` separate from immutable
  `ResolvedQueryPolicy`. `resolveQueryPolicy` implements the settled strict
  contract. It is not the public environment adapter until Issue #15.
- `resolveLegacyQueryPolicy` snapshots existing settings at engine construction.
  It preserves numeric coercion and flooring,
  ignored malformed input and numeric timeout flooring. Issue #13 adds the
  trimmed, case-sensitive independent `CODEGRAPH_QUERY_BUSY_TIMEOUT_MS=unlimited`
  sentinel and prevents timer overflow; malformed numeric input still falls back
  to 45 seconds until strict public validation in #15. It deliberately ignores
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
  caller outcomes share one idempotent settlement path. Cancellation, natural
  drain and idle retirement are not implemented here.
- Results must match both the owned Worker object and its current job id.
  Duplicate readiness, stale/duplicate results and error/exit races cannot free
  another job's slot or deliver a second outcome. Expired jobs are never retried.
  Destroy is idempotent and awaits failed as well as active owned workers. It
  retains the Windows startup handshake/grace before termination. The real
  worker crash fixture collects before self-exit; failed initialization closes
  any connection it already opened.

## Independent waiting boundary (Issue #13)

- The existing busy-timeout override can omit the pool admission timer entirely.
  It preserves mode capacity and ordinary retention; it does not activate the
  unlimited preset. Disabled pools, including ordinary read-only fallback, retain
  inapplicable deadlines. Unset waiting keeps the 45-second finite default.
- Finite waiting starts on admission and spans queue plus execution without
  resetting on dispatch or recovery. Busy guidance reports actual total elapsed
  time, is success-shaped and asks the caller to retry. It makes no worker-health
  or synchronous interruption promise. An expired queued call never executes;
  an executing call keeps its slot until completion or safe teardown, and late
  results cannot deliver a second answer or release another call's slot.
- Startup readiness, bounded recovery, transport/catch-up and shutdown safeguards
  remain separate. The absent busy timer is no permanent-failure settlement
  mechanism. Calling applications retain independent deadlines; a long-enough
  controlled client proves CodeGraph waiting only. #15 integrates cancellation
  and the complete strict preset/input contract; #16 completes attachment safety.
- Controlled host-facing proofs release a real built SQLite worker call at
  47 seconds, with a 90-second experiment bound and an independent 80-second
  client deadline, in direct, writable-fallback and shared-daemon modes. The test
  preload delays delivery to the unchanged worker handler; it never fabricates
  graph output. Ordinary default/explicit project queries run beside this proof.

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
