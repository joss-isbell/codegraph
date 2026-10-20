# Installed CodeGraph RAM: memory ownership and duplication

**Version:** 1.0, 2026-10-20. **Question:** [Map memory ownership and duplication in installed CodeGraph](https://github.com/joss-isbell/codegraph/issues/42), within [Reduce installed CodeGraph RAM without sacrificing results or uncapped concurrency](https://github.com/joss-isbell/codegraph/issues/41). **Source:** `3e2305c811e89b9f7a4048134d245eaa38b3a96e`, package `1.6.2`, the installation selected by [After approval, switch the affected installation](https://github.com/joss-isbell/codegraph/issues/40). Repository links are pinned to that source, not older main or a historical capacity candidate.

## Conclusion and evidence status

The useful unit is **the installed execution topology under equal workload**, not a Node process count or database disk size. This source supports shared-daemon, direct and fallback engines. Only the shared-daemon route shares its engine between client sessions. Within one engine process, the project coordinator can hold a read-only graph and a separate writer graph; each query worker opens additional read-only graph connections. These are separate ownership boundaries, not proof that each connection contains the entire database. [Routing][route] [Daemon][daemon] [Project reader][resources] [Project writer][resource-writer] [Query worker][worker]

The most useful next step is attribution: identify which owners actually participate, then distinguish private commitment, private residency, mapped/shared pages and transient request state. The promising questions are unnecessary engine replication, reader-only construction of indexing machinery, duplicated native/per-project cache state, and overlapping request/message representations. These are **hypotheses**, not selected optimizations or measured savings.

- **Established source fact:** traced in the pinned source. A declared cache capacity is not its populated byte size or resident footprint.
- **Local observation, kept private:** installation binding and launcher were inspected read-only. One existing session reported direct/local-engine execution, unlimited query concurrency, no busy deadline and idle retirement. That observation does not establish fleet-wide policy, route, workload or lifecycle. Raw launcher/configuration details, machine inventory, paths, PIDs and memory snapshots are deliberately absent here.
- **Missing measurements:** allocation attribution, reclaimed bytes, speed effects and repeated-cycle retention. A large working set alone does not establish a leak; working set and private commitment measure different things. [Windows memory counters][win-counters] [Working set][win-ws]
- **Work performed:** source/document review only. No runtime experiments, production index initialization/rebuild, restart, installation/configuration change or load campaign. No savings percentage or leak claim is supported.

Here, **worker** means a Node worker thread unless explicitly labelled a child process; **graph connection** means a `CodeGraph` facade plus its database/query/layer objects, not a fully loaded graph. The query pool uses `Worker`; the watchdog is a separate child process. `openSync` opens a database and constructs the facade; traversals allocate result maps/sets during execution. [Pool][pool] [Watchdog][watchdog] [Facade][facade] [Traversal][traversal]

## 1. Execution topology

| Owner / route | Established sharing and lifetime | Consequence for RAM attribution |
| --- | --- | --- |
| MCP forwarding/session layer | Preparation selects proxy attachment or a local direct session. Direct construction passes `queryPool: true`. [Routing][route] [Direct construction][direct] | Classify every measured client by its effective route. Include installation-specific forwarding/bootstrap processes privately; do not infer their role from the Node executable name. |
| Shared daemon | One daemon constructs one engine; accepted sockets get separate sessions sharing it. [Daemon][daemon] [Session attachment][daemon-session] | Same verified daemon can reuse engine/pool state across clients. Different primary projects or compatibility cohorts are not automatically one shared owner. |
| Direct and fallback engines | Direct constructs its own engine/session. Writable fallback acquires a writer reservation; read-only fallback constructs a local read-only engine without auto-sync ownership. [Direct][direct] [Fallback][fallback] | Separate local engines replicate process-local state. A direct route is not inherently finite concurrency. Include any foreign writer needed for freshness in the comparison rather than attributing only its reader's process. |
| Cross-project resource coordinator | Module-local map keyed by canonical project root holds leases, reader/writer references and gates. It opens one shared RO graph per entry, then a separate writer/watch graph if needed. [Resource identity][resource-entry] [Reader open][resources] [Writer open][resource-writer] | This reuse is within the module instance/process, not all engines or worker isolates. Count actual reader and writer handles; one engine need not mean one SQLite connection. |
| Transition cohort | Retry drains/stops a writable local engine before spawning the replacement daemon. [Handover][handover] | Separate natural startup/retirement overlap from steady state. Source/artifact and generation matter; do not label an older process a leak or force global restarts for a cleaner chart. |

**Conflicting assumptions:** the daemon constructor's nearby comment says direct mode leaves its pool off, but actual direct construction passes `queryPool: true`. The pool's general description mentions eager warming, but the engine passes `warmWorker: false`. Effective diagnostics and executable construction outrank those comments. [Daemon][daemon] [Direct][direct] [Engine pool adapter][engine-pool]

## 2. Ownership, duplication and retention

| Memory owner | Contents / duplication boundary | Lifetime and missing evidence |
| --- | --- | --- |
| Main-thread project resource | Coordinator RO graph, optional distinct writer graph, watcher/catch-up/sync state. Each facade wires extraction, resolver, graph, traversal and context layers, including RO opens. [Reader open][resources] [Writer open][resource-writer] [Layer wiring][layers] | Retirement waits for gates/indexing, closes eligible reader/writer and releases exact ownership; facade close unwatches and closes DB. Measure what each layer actually populates. [Retirement][resource-retire] [Close][facade-close] |
| Query pool | Queued arguments/contexts/promises and idle/starting/executing/retiring worker records. Unlimited policy gives `Infinity` maximum; growth follows outstanding demand with cold-start pacing, not a fixed active ceiling. [Pool construction][pool] [Growth][pool-growth] | Engine creates no eager worker. Under retire-idle policy, eligible idle workers retire after 30 seconds even while another worker is occupied. Count real termination completion, not only logical removal. [Adapter][engine-pool] [Retirement][pool-idle] [Grace constant][pool-grace] [Exit accounting][pool-exit] |
| Each query worker | Own tools/graph chain, default RO graph when present, and a handler that opens cross-project graphs lazily; results go through `postMessage`. [Worker][worker] | Independent native readers enable parallel reads. Measure duplicated modules/layers/statements/node objects/native caches across these readers before proposing reuse. Retirement and handler eviction are different boundaries. |
| Each handler's explicit-project cache | Map of canonical roots to graphs plus usage times/gates; eight retained projects, default ten-minute idle timeout, overrideable. Worker handlers do not have the engine's watcher hook. [Handler][handler] | Trimming returns while that handler has active calls, skips live gates and later closes/releases excess or idle graphs. Eight is **idle retention**, not an active-project admission cap or a fleet-wide memory bound. Default graph is outside this explicit-project cache. [Lookup][project-open] [Trim][trim] |
| Ordinary facade SQLite connection | Requests `cache_size=-64000`, `temp_store=MEMORY`, `mmap_size=268435456`; writable opens select WAL. Prepared statements and a 1,000-entry node LRU belong to each QueryBuilder. [Pragmas][pragmas] [Node cache][node-cache] [Statements][statements] | Cache suggestion is approximately 62.5 MiB, allocated as needed, not an up-front reservation. A 256 MiB mapping request is not 256 MiB of private physical RAM. Measure occupancy, native temp/sort/statement allocations and mapped residency. Entry bounds are not byte bounds. [SQLite cache semantics][sqlite-cache] [Mapped I/O][sqlite-mmap] |
| Resolver state | Per-facade LRUs for nodes, file strings, split lines, imports, names, qualified names and methods; metadata limit normally 5,000, content normally 1,000, node-ID limit at least 20,000. Warm-up builds file/name sets. [Default limit][resolver-default] [Cache construction][resolver-cache] [Warm-up][resolver-warm] | `clearCaches()` clears these and companion memos. Construction does not mean maximum capacity is populated. Measure duplicate string/row representations and actual retention after catch-up/indexing. [Clear][resolver-clear] |
| Grammar/parser and branch-label state | Module caches hold WASM languages/parsers; branch labels retain up to eight source/tree pairs and explicitly delete replaced/evicted native trees. Parser reset/clear operations exist. [Grammar cache][grammars] [Reset][grammar-reset] [Branch cache][branch-cache] | Cost depends on languages and participating isolates, not grammar files' disk sizes. Worker retirement is a separate lifetime boundary. |
| Per-session exploration metadata | Bounded project/call/file/range detail with a smaller view cloned to workers; not an unbounded archive of full response bodies. [Session bounds][session-state] | Preserve session isolation. Measure actual metadata and transient clones rather than sharing source-suppression history between clients. |
| Request-local working state | Traversal maps, edges, visited/enqueued sets; materialized row arrays; source/output strings; worker messages. Adapter also supports lazy `iterate`. [Traversal][traversal] [Row interface][row-interface] [Worker result][worker-result] | Node messages normally clone data; transfer/shared storage needs explicit design. Profile simultaneous copies and remaining references through result delivery. Preserve ranking, order, source and output budgets if representations change. [Node message contract][node-workers] |
| Support work | Status can start freshness-measurement workers; WAL healing can create a checkpoint worker with its own connection; watchdog is a child process. [Status][status] [Freshness][freshness] [Checkpoint][checkpoint] [Watchdog][watchdog] | Freshness ownership remains until native exit even after returning unknown on deadline. Include short-lived peaks and observer overhead. Full status polling is not a neutral high-frequency memory sampler. |

**Indexing is a separate phase.** Extraction reads grammar bytes in the parent, prewarms parse workers, optionally uses a store-writer thread, and retains a rolling ordered window of source/content/results. Grammar messages have no transfer list: reusing disk reads is not evidence of shared grammar heaps. Parse/store lifetimes end separately. [Extraction/window][extraction] [Parse message][parse-message] [Store cleanup][extraction-close] [Parse cleanup][parse-close] [Node cloning][node-workers]

Resolution workers open their own raw RO SQLite connection, QueryBuilder and resolver with `cache_size=-32000` (approximately 31.25 MiB suggested capacity), not the ordinary facade's full pragma set. Connection recycling deliberately preserves higher-layer caches; the enclosing `finally` destroys the pool. Do not apply the ordinary facade's mmap assumption to resolver workers. Measure extraction, resolution/synthesis and post-index idle separately. [Resolver worker][resolver-worker] [Pool cleanup][resolution-close] [SQLite cache semantics][sqlite-cache]

## 3. Candidate questions: established mechanisms, unmeasured value

1. **Topology first:** if equal-demand clients unnecessarily retain separate engines where compatible sharing is intended, investigate attachment/routing before small caches. Preserve project authority, compatibility, direct/fallback correctness and isolation. One inspected session does not establish fleet uptake. [Routes][route] [Effective diagnostics][diagnostics]
2. **Reader-only construction:** can reader facades avoid constructing extraction/resolver/framework machinery until actually required, without postponing required query work? This is an allocation/lifetime hypothesis, not proof that today's empty LRUs consume maximum budgets. [Layers][layers] [Resolver factory][resolver-factory]
3. **Native reader duplication:** attribute coordinator RO, writer and worker RO occupancy before considering reuse/distinct reader configuration. Independent readers must retain parallelism. Merely shrinking caches to exchange RAM for extra I/O/slower work is not sufficient; private-byte changes may shift memory into mapped/system-cache residency. [Pragmas][pragmas] [SQLite allocations][sqlite-memory] [Mapped pages][sqlite-mmap]
4. **Representation/peak overlap:** profile row arrays, traversal state, source formatting and message cloning. Streaming/lazy materialization remains a candidate if complete answers, ordering and simultaneous demand are preserved; truncation or skipped work does not. [Traversal][traversal] [Iterator][row-interface] [Worker result][worker-result]
5. **Phase retention:** determine whether warmed indexing caches remain useful after catch-up and whether support workers actually exit. Idle query retirement already exists; slowing work so fewer workers become active is not a new memory optimization. [Clear][resolver-clear] [Retirement][pool-idle] [Grace constant][pool-grace]

Do not introduce a concurrency ceiling, admission cap or busy deadline. Preserve the accepted [unlimited contract](https://github.com/joss-isbell/codegraph/issues/4#issuecomment-6193666914) and [safe changeover agreement](https://github.com/joss-isbell/codegraph/issues/6#issuecomment-6194652040). Report incidental speed effects of a demonstrated allocation/representation/lifetime optimization; saving RAM solely by slowing or postponing the same work fails this map's objective.

## 4. Minimal meaningful installed-Windows baseline

This is a **proposed design**, not an executed benchmark or authority to mutate production.

### Identity and fixed work

Keep a private manifest: package/archive and relevant executable/module hashes; Node version/flags; effective policy; process creation identity/ancestry; engine generation; verified project/index file identity; corpus/config/index state/revision; workload. Publish pseudonymous client/project/role labels, not raw inventory. Diagnostics expose execution mode, PID/generation, resource scope, policy and lifecycle counts; collect actual values rather than infer them from environment intent. [Diagnostics][diagnostics]

Use isolated disposable task-owned fixtures. Their indexes may be prepared within the baseline task authorization using installed supervised tooling; only production index builds remain human-led. Never initialize or rebuild the held production index as part of the baseline. Include a small and a representative large corpus, source-body/overload/flow/impact queries, and cross-project/nested-included-source cases where applicable. Use identical corpus and query parameters in baseline/candidate. Include a controlled fixture edit/reconcile step to check freshness, not only stable reads.

Use finite **fixed offered arrivals** and equal client/project mix; record sent/admitted/started/completed/cancelled/failed work, queue and concurrent execution. Include serial and simultaneous demand. A completion-paced driver could silently offer less work to a slower candidate and manufacture lower RAM. Uncapped policy does not require infinite test demand: hold finite representative demand constant and verify no new configured ceiling/deadline.

### Separate memory ledgers

| Ledger | Required interpretation |
| --- | --- |
| Private commitment | Per-process private committed bytes; synchronized group totals/peak. `PrivateUsage` is private commit, not residency. Do not add individual lifetime peaks as though simultaneous. [Counters][win-counters] |
| Residency | Total/private resident working sets separately. Working set counts resident pages; shared pages can appear in multiple processes. Summed working sets are **not unique physical RAM**. [Working set][win-ws] [Share metadata][win-share] |
| Shared/mapped/system cache | Selected VMMap process breakdowns plus RAMMap physical/file views or equivalent supported WPT capture. Prove unique-page accounting before publishing a unique physical total; otherwise report private residency and unresolved shared overlap. Never add whole system cache to process working sets. VMMap separates commitment/types/residency; RAMMap exposes physical/file pages. [VMMap][vmmap] [RAMMap][rammap] |
| JS/native attribution | Optional instrumented attribution runs: per-isolate heap fields, external/array buffers, plus native/map evidence. Array buffers are already in external; worker RSS is process-wide. Do not add overlapping fields to OS totals. Node heap fields do not completely attribute SQLite/WASM/OS memory. [Node metrics][node-memory] [SQLite allocation objects][sqlite-memory] |
| Speed/work | Latency including cold waits, throughput, offered/completed demand, errors/cancellation, CPU/disk/faults. Correlate thread/connection counts. Report memory peak, idle plateau, phase duration and memory-over-time, not one convenient instant. |

Use installed Windows tools through documented interfaces: finite counter sampling for timelines and targeted VMMap/RAMMap/WPT attribution. Record observer overhead. Owned validation invocations require installed deadline/cleanup tooling, bounded logs and real completion evidence; do not build a custom supervisor. If the installed capture path cannot resolve shared physical pages, state that capability gap instead of claiming unique physical savings. No such tools were launched here.

### Required cells

Proposed minimum: three repetitions per relevant cell and arm; alternate run order where feasible, retain every run/range. Do not clear global caches or trim production working sets to manufacture cold conditions.

| Cell | Fixed workload and transition markers |
| --- | --- |
| Connected unused | One then multiple connected clients, before any query and after prior use; isolate forwarding/session cost from graph opening/worker demand. |
| Process-cold first use | Fresh owned launch, first fixed query; startup/module/DB/readiness/response markers. Call it process-cold; separately label OS/file cache warm or unknown. |
| Warm serial / burst | Same query mix, then simultaneous multi-client arrivals; observe worker growth and equal demand across arms. Existing readiness pacing is not an active ceiling. [Growth][pool-growth] |
| Same project / multiple clients | Same index through actual selected routes; establish engine/writer/reader sharing or duplication. Compare whole relevant process group, not one proxy. |
| Multiple projects | Two then more than eight existing fixture projects, including simultaneous calls. Observe retention/drain; do not demand eviction while calls/gates still own graphs. [Trimming][trim] |
| Burst to idle | End identical burst, keep clients connected, then disconnect only owned clients. Observe before/after 30-second query retirement and beyond effective project/daemon idle policies. Worker retirement, project eviction and no-client daemon exit are different events. [Query retirement][pool-idle] [Project timeout][handler] [Daemon timeout][daemon-idle] |
| Fixture edit/catch-up/watch | Identical edit and subsequent query expectations; demonstrate fresh, complete, isolated answers and unchanged source selection. |
| Indexing and return | Task-owned fixture extraction, resolution/synthesis and post-index idle within baseline authorization using installed supervised tooling; fixed overlapping query demand if supported. Include writer, parse/store/resolver and support work; preserve deterministic graph input/order. [Extraction][extraction] [Resolution][resolver-worker] |

Avoid continuous full `codegraph_status` polling: it starts freshness measurements. Capture policy/lifecycle at controlled markers, use OS sampling for the timeline and record probe-induced work. Unknown freshness remains unknown, not zero pending changes. [Status][status] [Freshness ownership][freshness]

### What would count as evidence

Future comparisons need matching identities/corpus/demand; complete semantic/source output equivalence with only declared nondeterministic fields normalized; freshness/isolation checks; every offered request dispositioned; unchanged unlimited contract; synchronized group memory curves with correct shared/private accounting; and latency/throughput beside the demonstrated allocation mechanism. Graph row counts alone are insufficient. Keep incomplete/failed captures and observer effects beside the finding.

A candidate can reduce private allocation without reducing unique physical residency, or move cost into indexing/system cache: publish that distinction. Leak diagnosis requires repeated comparable cycles and unexpected retained allocations/ownership, not a process count or one post-burst working set.

## 5. Limits and disposition

Static review establishes construction/ownership/retention and diagnostic boundaries, not byte attribution, reclaim timing, savings or speed. Private runtime observations are uncontrolled snapshots. Installation binding relies on the accepted installation/critical-artifact checks, not a fresh full-archive attestation; package version alone is insufficient. Installation-specific launcher details remain private and must be classified in the measurement.

Official Node memory guidance is v22 documentation; message-cloning guidance is the longstanding API contract in current official docs, not evidence that a newer Node is installed. Effective native SQLite pragmas/compile options still need runtime readback. No Linux qualification, production supervisor change, tests/builds, prototype, activation or release is implied.

**Research disposition:** source ownership map and installed-Windows baseline design ready for review. Next: [Measure installed CodeGraph memory across active and idle workloads](https://github.com/joss-isbell/codegraph/issues/44) attribution, then narrow follow-up questions. Do not select an optimization or promise savings from this report alone.

## Primary-source references

[route]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/index.ts#L624-L678
[direct]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/index.ts#L691-L722
[fallback]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/index.ts#L171-L234
[daemon]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/daemon.ts#L250-L261
[daemon-session]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/daemon.ts#L522-L546
[daemon-idle]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/daemon.ts#L549-L566
[resource-entry]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/project-lifecycle.ts#L60-L114
[resources]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/project-lifecycle.ts#L305-L334
[resource-writer]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/project-lifecycle.ts#L493-L538
[resource-retire]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/project-lifecycle.ts#L353-L429
[handover]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/proxy.ts#L1071-L1095
[engine-pool]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/engine.ts#L242-L255
[diagnostics]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/engine.ts#L301-L330
[pool]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/query-pool.ts#L154-L209
[pool-growth]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/query-pool.ts#L366-L397
[pool-idle]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/query-pool.ts#L399-L435
[pool-grace]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/query-pool.ts#L69-L70
[pool-exit]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/query-pool.ts#L525-L564
[worker]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/query-worker.ts#L46-L82
[worker-result]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/query-worker.ts#L89-L118
[handler]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/tools.ts#L1869-L1980
[project-open]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/tools.ts#L2350-L2415
[trim]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/tools.ts#L2464-L2516
[facade]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/index.ts#L469-L487
[layers]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/index.ts#L177-L247
[facade-close]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/index.ts#L497-L505
[pragmas]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/index.ts#L30-L45
[node-cache]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/queries.ts#L244-L252
[statements]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/queries.ts#L317-L330
[resolver-default]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L90-L103
[resolver-cache]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L325-L355
[resolver-warm]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L487-L525
[resolver-clear]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L540-L572
[resolver-factory]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L3504-L3507
[grammars]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/grammars.ts#L337-L353
[grammar-reset]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/grammars.ts#L841-L869
[branch-cache]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/graph/branch-guards.ts#L156-L205
[session-state]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/explore-session-state.ts#L103-L144
[traversal]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/graph/traversal.ts#L33-L74
[row-interface]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/sqlite-adapter.ts#L15-L25
[status]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/tools.ts#L8138-L8155
[freshness]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/index-freshness.ts#L13-L94
[checkpoint]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/index.ts#L674-L718
[watchdog]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/liveness-watchdog.ts#L187-L248
[extraction]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/index.ts#L2231-L2327
[parse-message]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/parse-pool.ts#L288-L308
[extraction-close]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/index.ts#L2581-L2605
[parse-close]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/index.ts#L2760-L2770
[resolver-worker]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/resolver-worker.ts#L53-L84
[resolution-close]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L2589-L2603
[win-counters]: https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-process_memory_counters_ex
[win-ws]: https://learn.microsoft.com/en-us/windows/win32/memory/working-set
[win-share]: https://learn.microsoft.com/en-us/windows/win32/api/psapi/ns-psapi-psapi_working_set_ex_block
[vmmap]: https://learn.microsoft.com/en-us/sysinternals/downloads/vmmap
[rammap]: https://learn.microsoft.com/en-us/sysinternals/downloads/rammap
[node-memory]: https://nodejs.org/docs/latest-v22.x/api/process.html#processmemoryusage
[node-workers]: https://nodejs.org/api/worker_threads.html#portpostmessagevalue-transferlist
[sqlite-cache]: https://www.sqlite.org/pragma.html#pragma_cache_size
[sqlite-mmap]: https://www.sqlite.org/mmap.html
[sqlite-memory]: https://www.sqlite.org/malloc.html
