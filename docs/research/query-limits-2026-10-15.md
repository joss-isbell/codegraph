# Query execution limits and settings propagation

- Task: [Find every query limit and how settings reach existing servers](https://github.com/joss-isbell/codegraph/issues/2).
- Role and author: exploration agent `query_limits`.
- Report version: `query-limits-public-v3`.
- Examined source: `joss-isbell/codegraph` commit `8f1f842b5b828175992d2ce2cdf673b955b2cce9`, package version 1.6.2; research branch `research/query-limits-20261015`.
- Date: 2026-10-15.
- State: static research complete; configuration choices remain open. No implementation, executable validation, installation, or service activation was performed.

## Main finding

Two existing environment variables already select worker capacity and the busy-response delay. Supporting an explicit unlimited mode requires removing two independent worker ceilings, adding a genuine disabled-timer state, and making read-only fallback eligible for safe worker execution. Configuration propagation also needs attention: a client can currently request different settings and attach to an already-running same-version daemon without learning that its settings did not take effect. These are source findings, not measurements of a deployed service.

**Confirmed product constraints:** preserve existing defaults and provide an easy opt-in to uncapping, as recorded in [Remove CodeGraph query limits on demand and include selected repositories under non-Git folders](https://github.com/joss-isbell/codegraph/issues/1). Configuration syntax, precedence, and effective-setting propagation remain to be decided.

## Current execution paths

| Path | Current worker maximum | Existing override | Important boundary |
| --- | --- | --- | --- |
| Direct stdio MCP | `max(1, min(cores - 1, 2))` by default | `CODEGRAPH_QUERY_POOL_SIZE` | A numeric override bypasses the direct default of 2 but remains clamped to 16. |
| Writable proxy fallback | Same default as direct mode | Same setting | The fallback first acquires writer ownership. |
| Shared project daemon | `max(1, min(cores - 1, 16))` by default | Same setting | Its engine and pool serve all connected sessions, including explicit-project reads. |
| Read-only proxy fallback | No worker pool | The size override cannot enable it | It serves on the main event loop while another owner retains synchronization. |

The direct constant and both writable engine construction sites establish the first two rows. The daemon enables pooling without the direct override, and gives every connection the same engine. The read-only fallback does not request pooling, and the engine separately refuses to create a pool for any read-only instance. Sources: [direct limit][direct-limit], [writable fallback][writable-fallback], [direct construction][direct-engine], [daemon construction][daemon-engine], [shared sessions][daemon-sessions], [read-only fallback][readonly-fallback], [engine pool selection][engine-pool].

- **No separate fixed request-admission count appears in the inspected path.** Stdio and socket transports dispatch requests without waiting for earlier tool calls to finish. `QueryPool` admits calls into an ordinary array without a configured length bound. The fixed count governs executing worker threads; excess admitted calls wait. Sources: [stdio dispatch][stdio-dispatch], [socket dispatch][socket-dispatch], [queue and job admission][pool-run].
- **The pool size has two independent ceilings.** `resolvePoolSize()` clamps numeric input to `MAX_POOL_SIZE = 16`; the `QueryPool` constructor clamps its supplied size again. Updating only one does not lift the ceiling. A size of 0 disables the pool through engine selection. Negative values restore the default, although an adjacent comment incorrectly describes them as disabling it. Sources: [pool sizing][pool-sizing], [constructor][pool-constructor], [engine pool selection][engine-pool].
- **The pool serves aggregate engine demand.** It is not a per-session limit within a daemon. Separate direct processes or project daemons create separate pools. Workers open explicit projects lazily; this is not a single global pool across all daemon processes. Sources: [engine-owned pool][engine-pool], [shared sessions][daemon-sessions], [worker project opening][worker-open].

## Startup pacing and failure controls

- `MAX_CONCURRENT_SPAWN = 2` limits workers that are still starting. It does not limit the number of ready workers that may execute. When one worker becomes ready, growth may start another; a burst can eventually reach the configured pool maximum. Keeping this pacing is compatible with removing a fixed execution ceiling, although a cold burst will still wait for worker initialization. Sources: [startup throttle][spawn-throttle], [ready handling and growth][pool-growth].
- The pool starts one eager worker and grows lazily. Workers stay resident until pool destruction; this class has no idle-worker retirement path. More permitted workers can therefore increase both active and retained memory. This research establishes no memory-saving result. Sources: [constructor][pool-constructor], [pool growth][pool-growth], [destruction][pool-destroy].
- Twelve accumulated worker failures trip a pool circuit breaker; an in-flight call is normally retried once after worker death. These are recovery controls, not query-admission counts. When the pool is unavailable, unhealthy, or not yet warm, the handler uses main-thread dispatch. Sources: [failure budget][pool-failure-budget], [crash recovery][pool-recovery], [dispatch selection][dispatch-selection].
- **A disabled timer changes the circuit-breaker failure path.** Once the crash budget is exhausted, the pool stops spawning workers, but already-queued jobs are left for the busy timer to settle. The existing repeated-startup-failure test relies on a 30 ms busy timer to finish those jobs. With the timer disabled, those admitted jobs could remain pending until pool destruction. An unlimited-wait contract therefore needs explicit settlement or controlled rerouting when pool capacity becomes permanently unavailable. Sources: [crash recovery and drain][pool-growth], [failure settlement test][failure-tests].
- Worker teardown waits for the startup handshake, bounded by a separate 15-second settle interval, because terminating a worker during module loading can crash the process on Windows. An unlimited query mode should preserve this protection. Sources: [startup settle protection][spawn-throttle], [destruction][pool-destroy].

## Busy-response delay

- `CODEGRAPH_QUERY_BUSY_TIMEOUT_MS` defaults to 45,000 ms. The parser accepts finite numbers of at least 1,000 ms and floors fractions. Zero, negative, nonnumeric, blank, and infinite values do not disable the timer; they restore the default. Source: [busy timeout parser][busy-parser].
- The timer starts when `run()` admits a job. It includes both time waiting for a worker and time executing on a worker. A running query can therefore receive the busy response even though the message says it waited in the queue. The timer does not cancel synchronous work: an executing worker continues, and its later result is discarded for that already-settled client promise. Queued settled jobs are skipped before dispatch. Sources: [busy wording][busy-wording], [job admission and timer][pool-run], [growth and result settlement][pool-growth].
- This is not a complete incoming-request deadline: initialization and catch-up gates occur before pool admission, and `codegraph_status` stays on the main thread rather than using the query pool. Therefore changing the pool timer does not bound or remove every earlier wait. Source: [main-thread preflight and status][main-preflight].
- The response intentionally lacks `isError: true`; ordinary load produces success-shaped retry guidance. Keeping that response shape is an existing compatibility requirement for finite waiting modes. Source: [busy wording and response][busy-wording].
- **A very large timer is not an unlimited timer.** Node's versioned documentation states that `setTimeout()` delays greater than 2,147,483,647 ms become 1 ms. The current parser accepts larger finite values and passes them directly to `setTimeout()`, so such an override can cause an almost immediate busy response. An unlimited state needs to omit timer creation; long finite deadlines need validation or segmented scheduling. Sources: [Node 22 timer behavior](https://nodejs.org/download/release/v22.4.1/docs/api/timers.html#settimeoutcallback-delay-args), [busy timeout parser][busy-parser], [timer creation][pool-run].

## Read-only fallback requires explicit worker opening policy

The existing read-only fallback protects a live synchronization owner by opening indexes read-only and avoiding watcher, sync, and writer-lock acquisition. Merely enabling the current query-worker code in this mode would weaken that protection: its default graph is opened with `CodeGraph.openSync(root)` without `readOnly: true`, and explicit-project opens fall back to the same writable default. Writable database opening may run migrations, bulk-load repair, and WAL maintenance. Sources: [read-only fallback][readonly-fallback], [engine read-only project opening][engine-readonly-open], [worker opening][worker-open], [explicit-project opening][explicit-open], [database open behavior][db-open].

- A coherent candidate enables the read-only fallback pool and passes an explicit database-opening policy through both default and explicit-project worker opens. Since synchronization belongs to the main engine, making query-worker connections read-only in every mode is another candidate. Neither candidate has been implemented or validated.
- Acceptance must prove absence of watcher/sync activity, writer-lock mutation, migrations, repair, and database writes in read-only fallback; successful tool output alone is insufficient. Existing tests already assert database `data_version` and owner-lock preservation across async, retry, and explicit initialization. Sources: [read-only ownership test][readonly-tests], [database read-only branch][db-open].

## Client deadlines, cancellation, and shutdown

The source describes its 45-second busy timer as shorter than an approximately 60-second client deadline. That comment is not evidence of any particular current client's timeout. Disabling this server timer cannot disable a host's independent request deadline. The transport's own five-second default timeout applies to server-initiated requests such as `roots/list`, not to incoming tool-call execution. Sources: [busy default][busy-default], [transport request timeout][transport-timeout].

CodeGraph advertises MCP protocol version `2024-11-05`, so this note uses that version's primary specifications rather than assuming later protocol changes. Its lifecycle guidance recommends appropriate request timeouts; its cancellation utility defines `notifications/cancelled`, recommends stopping work and freeing resources where possible, permits ignoring requests that cannot be cancelled, and requires graceful handling of completion races. No universal timeout duration is prescribed there. Sources: [advertised version][protocol-version], [MCP lifecycle](https://modelcontextprotocol.io/specification/2024-11-05/basic/lifecycle#error-handling), [MCP cancellation](https://modelcontextprotocol.io/specification/2024-11-05/basic/utilities/cancellation).

- The inspected session switch has no cancellation handler. Pool jobs have no request cancellation signal, and stopping a session closes its transport without cancelling its tool jobs. Local fallback also ignores cancellation notifications. Removing the busy timer can consequently leave abandoned jobs waiting or executing unless a lifecycle policy is added. Sources: [session stop and request dispatch][session-dispatch], [pool job shape][pool-jobs], [local fallback dispatch][local-dispatch].
- A candidate policy removes cancelled queued work immediately, suppresses delivery after cancellation, and retains an executing worker's lease until its synchronous query finishes. Whether hard termination of running work is desired is a separate choice; it must account for startup protection and worker/database cleanup. This is a proposed contract, not established behavior.
- The main-thread liveness watchdog is a different mechanism: its default is 60 seconds without event-loop heartbeats. It should not be silently repurposed as the busy timer. Main-thread degraded dispatch can still block transport when pooling is unavailable. Sources: [watchdog purpose and default][watchdog], [dispatch selection][dispatch-selection].
- Proxy fallback retirement waits for its local calls before stopping the engine and handing writer ownership back. With indefinite waiting, a stuck local call could delay that handover indefinitely unless the new cancellation/drain contract addresses it. Source: [fallback retirement][local-retire].

## How settings reach a shared daemon

- A spawned daemon inherits a copy of its launcher's environment. Whichever launcher wins daemon election supplies the initial pool and timeout settings. The engine resolves pool size when creating the pool, and the pool resolves busy delay in its constructor. No per-client settings update is carried in the inspected attach path. Sources: [spawn environment][daemon-spawn], [engine pool selection][engine-pool], [pool constructor][pool-constructor].
- The Windows pipe rendezvous hashes only the canonical project root; its name does not contain the pool settings. The daemon lock holds PID, version, socket path, and start time. Its hello contains package version, PID, socket path, and protocol. Connection acceptance checks package-version compatibility without comparing pool size or busy delay. Sources: [rendezvous and lock identity][daemon-identity], [hello fields][daemon-hello], [attach acceptance][daemon-attach].
- **Inference from those concrete interfaces:** a newly configured client can silently attach to an existing bounded same-version daemon, leaving the requested no-limit settings ineffective. Updating saved launch configuration is therefore not proof of effective daemon uptake.

Candidate contracts for settings uptake, all still open:

1. Report normalized requested settings, effective settings, implementation identity, and compatibility in hello/status. Unknown fields from an older daemon must remain explicitly unknown rather than being interpreted as unlimited.
2. On a mismatch, continue using the existing daemon with a clear pending-uptake notice, or route the requesting client through a compatible read-only local pool until the existing service naturally drains. The latter preserves the caller's capacity request but requires the read-only worker changes above.
3. Change settings inside an existing compatible daemon through an explicit runtime update protocol. This is a larger change: shared-client authority, conflicting preferences, reductions with active jobs, and timeout changes for already-admitted jobs would need contracts.
4. Use configuration-sensitive daemon identities while maintaining exactly one writer per project. Distinct socket names alone do not make simultaneous writer ownership safe.

Natural-drain behavior should be explicit in whichever candidate is chosen. No candidate here requires or authorizes a forced global restart.

## Candidate configuration surface

A small extension of existing settings could be:

| Input | Candidate interpretation |
| --- | --- |
| `CODEGRAPH_QUERY_POOL_SIZE=unlimited` | Grow workers with demand without a configured execution-count ceiling; retain documented startup pacing and recovery behavior. |
| Positive finite pool size | Honor the requested count, including values above 16. |
| `CODEGRAPH_QUERY_POOL_SIZE=0` | Preserve existing pool-disabled behavior. |
| `CODEGRAPH_QUERY_BUSY_TIMEOUT_MS=0` | Do not create the busy backstop timer. |
| Positive finite busy delay | Preserve elapsed queue-plus-execution semantics, with explicit timer-range handling. |

This syntax is illustrative and is not an accepted decision. A convenience preset that selects both unlimited execution and no busy backstop could provide one easy setting, but its precedence over explicit numeric values must be decided. Current project configuration has no query-capacity fields; adding settings to `codegraph.json` would also require deciding how project-specific preferences interact with an engine serving multiple projects. Sources: [current project config][project-config], [worker explicit-project routing][worker-open].

## Choices still requiring a decision

- Should execution count and busy delay remain independently configurable? If there is a preset, do explicit numeric settings override it or conflict with it?
- Should explicit uncapping use one effective contract across direct, writable fallback, read-only fallback, and shared daemon paths, or allow distinct configured limits? Existing defaults remain unchanged. Read-only fallback parallelism is within the requested capability, not a silent exclusion.
- Should finite busy delay preserve today's total elapsed semantics, or become queue-only? If unchanged, the response wording should accurately describe both states.
- Which same-version daemon mismatch behavior is acceptable: visible pending uptake, compatible read-only fallback, or a runtime update mechanism?
- Which request/session cancellation and drain behavior is required before offering indefinite waiting? How are irreducibly synchronous in-flight queries accounted for?
- Should idle workers be retained indefinitely after a burst or retired? This affects resource cost but does not establish a need for an active-query cap.
- Which client deadlines and platform behaviors must be measured before claiming end-to-end long-running requests work?

## Meaningful acceptance evidence

These are proposed future checks, not passing results. Executable validation should use a deterministic wrapper with deadlines, process cleanup, bounded logs, snapshot identity, and short receipts.

1. **Sizing and admission:** hold more than 16 jobs behind a controlled gate and observe more than 16 simultaneously executing in numeric-above-16 and unlimited modes. Prove a finite configured limit still holds and no extra queue-admission ceiling appears. Preserve size 0 and define invalid/fractional/negative input behavior. Existing tests currently pin the old hard ceiling and already provide a fake-worker concurrency gate. Source: [sizing/concurrency tests][sizing-tests].
2. **Busy semantics:** prove disabled mode returns a late real result beyond the former backstop; finite mode remains success-shaped; expired queued work is not dispatched; late worker completion cannot double-deliver. Cover timer-range boundaries and demonstrate that an extreme value never becomes an immediate timeout. Exhaust the crash budget with the timer disabled and prove every admitted job still reaches the defined failure/fallback disposition. Source: [busy and destruction tests][busy-tests].
3. **All execution paths:** exercise direct stdio, several daemon sessions, writable fallback, and read-only fallback with real workers. Logs alone do not prove concurrent execution. Cover missing default index plus explicit-project calls. Sources: [direct/fallback tests][direct-tests], [multi-project pool tests][project-tests].
4. **Ownership and output:** preserve tool-result parity, per-session explore history, exact project routing, single-writer ownership, and read-only database/lock invariants under concurrent load. Sources: [multi-project pool tests][project-tests], [read-only ownership tests][readonly-tests].
5. **Existing-server uptake:** configure a client differently from a live same-version daemon and verify the effective settings are reported accurately. Exercise unknown legacy hello fields, mismatch disposition, natural drain, reconnection, conflicting clients, and absence of duplicate writers or forced global replacement.
6. **Cancellation and lifecycle:** cancel queued and executing requests; disconnect one session without harming other sessions; verify abandoned jobs do not remain unaccounted for and shutdown settles admitted work even with no busy timer. Include fallback-to-daemon handover under a long-running local call.
7. **Windows teardown and responsiveness:** preserve the worker-start handshake before termination, process cleanup, and bounded shutdown; demonstrate that transport pings/replies remain responsive under the selected load. Existing real-worker teardown tests provide a starting point. Source: [real-worker lifecycle tests][lifecycle-tests].

## Evidence limits

- Findings are static source observations against the stated commit, plus primary Node and MCP documentation verified on 2026-10-15. No live service, client-specific deadline, CPU throughput, memory usage, or Windows worker lifecycle was measured.
- The existing tests cited above establish intended coverage from their source; they were not run for this research and do not constitute current passing receipts.
- The note does not select configuration precedence, mismatch behavior, or cancellation policy. It identifies the decisions and the source seams a later implementation must cover.
- Repository source links are pinned to the examined commit. Node timer documentation is versioned; MCP references match CodeGraph's advertised protocol version.

## Pinned repository sources

[direct-limit]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/index.ts#L77-L78
[writable-fallback]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/index.ts#L125-L162
[direct-engine]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/index.ts#L529-L553
[daemon-engine]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/daemon.ts#L228-L243
[daemon-sessions]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/daemon.ts#L435-L467
[readonly-fallback]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/index.ts#L110-L152
[engine-pool]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/engine.ts#L132-L157
[stdio-dispatch]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/transport.ts#L284-L295
[socket-dispatch]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/transport.ts#L360-L380
[pool-sizing]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L48-L130
[pool-constructor]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L180-L187
[worker-open]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-worker.ts#L45-L68
[spawn-throttle]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L62-L81
[pool-growth]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L241-L317
[pool-destroy]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L346-L381
[pool-failure-budget]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L54-L60
[pool-recovery]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L266-L287
[dispatch-selection]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/tools.ts#L2818-L2856
[busy-parser]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L133-L139
[busy-wording]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L141-L153
[pool-run]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L313-L336
[engine-readonly-open]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/engine.ts#L96-L128
[explicit-open]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/tools.ts#L2329-L2335
[db-open]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/db/index.ts#L165-L203
[main-preflight]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/tools.ts#L2759-L2825
[busy-default]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L48-L49
[transport-timeout]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/transport.ts#L106-L128
[protocol-version]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/session.ts#L58-L59
[session-dispatch]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/session.ts#L140-L201
[pool-jobs]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/query-pool.ts#L91-L100
[local-dispatch]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/proxy.ts#L350-L370
[watchdog]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/liveness-watchdog.ts#L24-L58
[local-retire]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/proxy.ts#L328-L340
[daemon-spawn]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/index.ts#L289-L305
[daemon-identity]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/daemon-paths.ts#L40-L109
[daemon-hello]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/daemon.ts#L167-L187
[daemon-attach]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/mcp/proxy.ts#L158-L194
[project-config]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/project-config.ts#L34-L94
[sizing-tests]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/__tests__/query-pool.test.ts#L71-L117
[busy-tests]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/__tests__/query-pool.test.ts#L165-L182
[direct-tests]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/__tests__/mcp-daemon.test.ts#L389-L483
[project-tests]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/__tests__/query-pool.test.ts#L391-L437
[readonly-tests]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/__tests__/mcp-writer-lock.test.ts#L224-L252
[failure-tests]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/__tests__/query-pool.test.ts#L319-L347
[lifecycle-tests]: https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/__tests__/query-pool.test.ts#L468-L541
