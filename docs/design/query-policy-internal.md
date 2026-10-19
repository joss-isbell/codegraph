# Query configuration and request context

## Configure query limits

This guide describes the revised Issue #29 **source candidate**, not an accepted
release or an installed-runtime change. Its controls apply to direct stdio MCP,
writable fallback, read-only fallback and shared project-daemon execution.
Source inspection alone is not proof of runtime uptake; focused Ticket #29
results and combined verification in #21 establish that separately.

Set these environment variables in the configuration that launches CodeGraph:

| Setting | Meaning |
| --- | --- |
| `CODEGRAPH_QUERY_MODE=unlimited` | Convenience preset: no configured worker/admission maximum and no CodeGraph busy timer. |
| `CODEGRAPH_QUERY_POOL_SIZE=unlimited` | Remove only the configured worker/admission maximum. |
| `CODEGRAPH_QUERY_POOL_SIZE=<count>` | Use a finite worker ceiling; `0` deliberately disables the pool. |
| `CODEGRAPH_QUERY_BUSY_TIMEOUT_MS=unlimited` | Omit only the CodeGraph pooled busy timer; this is not a huge finite timeout. |
| `CODEGRAPH_QUERY_BUSY_TIMEOUT_MS=<milliseconds>` | Bound pooled waiting from admission through queueing and execution. |

Each explicit per-control value overrides **only its own** preset selection.
For example, the following are independent combinations, not new modes:

| Selected values | Effective behavior |
| --- | --- |
| Mode `unlimited`, no independent overrides | Unlimited pool and absent busy timer. |
| Pool `4`, busy timeout `unlimited` | At most four workers; no CodeGraph busy timer. |
| Pool `unlimited`, busy timeout `60000` | No configured worker ceiling; a 60-second CodeGraph busy timer. |
| Mode `unlimited`, pool `4` | Finite pool of four; busy timer still absent. |
| Mode `unlimited`, busy timeout `60000` | Unlimited pool; finite 60-second busy timer. |
| Pool `0`, with any busy setting or preset | Main-thread reads; the pool busy timer is inapplicable. |

Remove a variable to stop overriding it. Mode is unset or `default` for ordinary
defaults, or `unlimited` for the preset. Mode values and the independent
`unlimited` sentinels are trimmed and case-sensitive. An unsupported mode,
including an empty mode, is rejected with the setting and original value.

### Defaults and compatible numeric settings

With no applicable selection, direct and writable fallback use cores minus one
bounded to 1–2 workers; the shared daemon uses cores minus one bounded to 1–16.
Pooled busy waiting defaults to **45,000 ms**. Read-only fallback is **unpooled
by default**: a positive resolved finite count, independent unlimited pool, or
the preset opts it into workers. Setting only its busy timeout does not create
a pool. Read-only owner and worker connections remain read-only, without writer
or synchronization authority.

Existing independent-setting parsing remains compatible; no numeric syntax
migration is required:

- **Pool numbers:** JavaScript numeric conversion followed by flooring accepts
  spellings such as `+2`, `0x10`, `1e3` and `2.9` (respectively 2, 16, 1000 and 2).
  Nonnegative safe whole counts through `9007199254740991` are supported after
  flooring, including finite ceilings above 16. Workers grow on demand rather
  than allocating the maximum eagerly. Zero disables pooling.
- **Pool fallback:** an unset or empty value selects ordinary mode defaults.
  Whitespace-only numeric input converts to zero. Malformed, negative,
  non-finite or unsafe numeric input retains the legacy cores-minus-one fallback
  bounded to 1–16, including on direct/writable routes. Such input does not opt
  read-only fallback into a pool. Prefer an explicit count or remove the value
  when choosing a policy deliberately.
- **Busy numbers:** numeric values from `1000` through `2147483647` ms are
  floored. Unset, empty, malformed, non-finite, too-small or oversized values
  fall back to 45,000 ms; zero does not disable the timer. Oversized finite
  input uses that fallback rather than scheduling an overflowing timer.
- **Explicit overrides:** even an empty or malformed independent value overrides
  that side of the preset using the fallback rules above. `default` is a mode
  value, not an independent-control sentinel. When the pool is disabled, its
  busy timer is inapplicable regardless of the parsed timeout.

### Shared service uptake and use

Changing a client's environment does not change an already-running service.
Attachment checks the service's reported implementation, canonical project and
actual effective policy; compatible sessions reuse the project's engine/pool
while retaining separate request and exploration history. A service that cannot
prove compatibility, or has different effective controls, is refused rather
than silently serving with old limits or interrupting unknown/healthy work.
Use controls compatible with its actual policy, or allow its existing sessions
and service to drain naturally before reconnecting with the selected policy.
A refusal is not successful uptake. Existing fenced stale-record recovery must
still re-admit and verify the replacement; it is not permission to stop a live
service or create a competing writer. Fallback engines remain exceptional local
resources, not another pool per session on a compatible shared service.

### Limits outside these settings

Unlimited means absence of the selected **CodeGraph configured ceiling**, not
infinite hardware capacity. Active demand can grow worker count, CPU and memory
use; no RAM savings or idle-to-zero product guarantee follows. Worker startup,
failure recovery, cancellation and safe cleanup remain separate from busy
waiting. A finite busy response does not interrupt synchronous work already
executing. Calling applications retain independent deadlines: a host can stop
waiting before CodeGraph returns, even when CodeGraph's own busy timer is absent.

## Current implementation boundary

`query-policy.ts` captures raw settings once and resolves an immutable policy.
The preset supplies omitted independent values to `resolveLegacyQueryPolicy`;
the retained `source: 'strict'` diagnostic tag identifies captured startup, not
a stricter numeric grammar. Mode validation is the new syntax boundary.
`MCPEngineOptions.queryPolicy` carries the selected policy to the dispatcher,
pool and workers rather than rebuilding it from a later environment snapshot.
`QueryPool` represents the absent timer as `null`. Internal execution diagnostics
distinguish resolution, actual construction/dispatch and effective service
policy; they are not an extra MCP tool or evidence of installed uptake.

## Historical implementation notes (Issues #9–#14)

The following records preserve earlier implementation and validation context.
Stage-specific future work, numerical experiment bounds and historical platform
receipts are not new acceptance requirements or current runtime claims. Revised
#29 requires focused Windows checks of the requested controls and retained
protections; ordinary combined build/regression verification belongs to #21.

### Execution and ownership boundary

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

### Worker readiness and capacity boundary (Issue #10)

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

### Request abandonment and handover (Issue #12)

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
  Expiry of the last queued caller wakes a natural-drain waiter; expiry of an
  executing caller still leaves that waiter pending until execution or owned exit.
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
  If every finite slot is reserved by failed owned cleanup, there is no usable or
  recovering capacity: queued and new calls get terminal capacity-unavailable
  outcomes even with no busy timer. This keeps reserved ownership without
  oversubscribing the finite ceiling or stranding another session indefinitely.
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

### Independent waiting boundary (Issue #13)

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
  Expired queued entries are pruned before demand growth; a late ready worker
  cannot allocate extra capacity for callers whose deadlines already answered.
- Startup readiness, bounded recovery, transport/catch-up and shutdown safeguards
  remain separate. The absent busy timer is no permanent-failure settlement
  mechanism. Calling applications retain independent deadlines; a long-enough
  controlled client proves CodeGraph waiting only. At this stage the public
  preset and attachment integration were downstream; revised #29 consolidates
  that work while preserving compatible independent-setting parsing.
- Controlled host-facing proofs release a real built SQLite worker call at
  47 seconds, with a 90-second experiment bound and an independent 80-second
  client deadline, in direct, writable-fallback and shared-daemon modes. The test
  preload delays delivery to the unchanged worker handler; it never fabricates
  graph output. Release and elapsed/result identity use monotonic
  `process.hrtime.bigint` timestamps; epoch timestamps remain context only.
  Ordinary default/explicit project queries run beside this proof.

### Historical validation boundary

Earlier checks used `scripts/validation/runner.py` and `query-tests.py` with
bound builds/runtime/dependencies, isolated state and complete test/shutdown
tails. Their failed or incomplete results remain evidence, not passes. Reuse
valid unchanged evidence with explicit applicability to the current candidate;
the earlier broad matrix and evidence-platform work are not a new campaign.

### Read-only fallback boundary (Issue #11)

- The existing finite pool-size setting now selects read-only workers explicitly.
  Unset/malformed/nonapplicable selections stay unpooled; zero keeps main-thread
  reads and an inapplicable pool deadline. Permissive numeric parsing remains
  compatible in revised #29. A constructed finite pool remains authoritative
  while cold or recovering, using #10's timer-independent failure contract.
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
- Read-only activation supports explicitly selected finite and unlimited pools.
  Unset/default and explicit zero remain unpooled. At this stage public preset
  uptake, cancellation/drain and service compatibility were downstream work.
- Real worker/SQLite tests cover cold default, same/other explicit and rootless
  routes, directory aliases, session emission stripping/isolation, genuine CLI
  service-unavailable fallback beside a live native-watching writer, unchanged
  schema/content/WAL/ancillary metadata during reader-only intervals and owner
  changes before and after reader cleanup. POSIX replacement tests exercise live
  open inode replacement; Windows cannot unlink an open database and uses the
  ordinary platform guard. All checks use the proved supervisor and receipt
  consumer; no live held index or installed service is touched.

### Independent unlimited pool and idle retirement (Issue #14)

- `CODEGRAPH_QUERY_POOL_SIZE=unlimited` independently selects pooled execution
  without a configured worker or admission maximum in direct, writable fallback,
  read-only fallback and shared-daemon modes. The existing finite busy timer
  remains unless its independent override removes it. Finite counts, deliberate
  zero and ordinary defaults retain their meanings; no huge eager allocation or
  replacement fixed concurrency ceiling is introduced. At most two cold workers
  start simultaneously, and each readiness event permits further demand growth.
- The internal idle grace version 1 is 30 seconds per ready unused worker,
  observed by 45 seconds in real-worker acceptance. Each worker's actual last
  completion/readiness starts its own grace. An unused peer can retire while
  another healthy query continues indefinitely. Open chats, status polling and
  protocol heartbeats neither cancel nor reset an unused worker's grace.
- Only dispatchable idle workers retire. Queued live demand reuses needed idle
  workers before retiring them; starting, executing and retrying work stays owned.
  A caller's finite expiry does not make synchronous execution idle. Cancellation
  retains the separate targeted abandonment contract from #12. Routine handover
  drains without imposing the idle grace as a query deadline.
- Each retiring worker leaves the dispatchable set before asynchronous teardown,
  but remains counted and owned through verified exit. Finite reserved slots
  cannot be oversubscribed. New demand queues for readiness or safely grows other
  available capacity; retired workers cannot run duplicate jobs or resurrect.
  Cleanup rejection stays visible and reserves that worker's slot while unrelated
  healthy work and session history continue. If every finite slot is reserved by
  failed cleanup, queued/new demand settles unavailable rather than waiting
  forever or escaping the pool; known cleanup failure rejects natural handover
  and final destruction truthfully. Windows startup protection remains in force.
- Retirement follows the unlimited pool override or the internal unlimited
  preset, including a finite override that makes that preset custom. Removing
  only the busy timer preserves ordinary retention. No retirement knob is added.
  Real worker counts establish release of query workers, not RAM savings or a
  fixed active-resource bound. Demand can grow CPU and memory use, and a later
  query pays the observed cold-worker startup cost.
- Reader retirement/recreation preserves read-only authority and leaves the
  owner connection, writer lock, native watching and synchronization available.
  Normal shared chats reuse one project engine/pool; exceptional fallback engines
  retain local ownership and are not represented as shared across chats.
