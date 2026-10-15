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
  It preserves numeric coercion, flooring, the 16-worker explicit override cap,
  ignored malformed input and legacy timeout behavior. It deliberately ignores
  the new mode input. Its direct/writable defaults are bounded to two workers;
  shared-daemon defaults use cores minus one bounded to 1–16. Read-only defaults
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
  dispatch: read-only, cold, disabled or unhealthy execution reports main-thread
  inapplicability. Actual retention remains ordinary until Issue #14. This is an
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
  root/index plus `default`/`explicit` origin. Worker-wire `projectPath` does not
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
