# Installed CodeGraph RAM: allocation hypotheses

Research date: 2026-10-20
Report version: installed-ram-allocations-v1
Question: [Find allocation reductions that preserve CodeGraph answers and concurrency (#43)](https://github.com/joss-isbell/codegraph/issues/43)
Map: [Reduce installed CodeGraph RAM without sacrificing results or uncapped concurrency (#41)](https://github.com/joss-isbell/codegraph/issues/41)

## Disposition

**Prioritize removing temporary representations in active query rendering and database decoding; investigate ownership transfer on the indexing buffer path separately.** These are source-supported hypotheses, not demonstrated savings. None requires fewer admitted queries, smaller answers, stale indexes, or deliberately slower scheduling. No candidate is selected for implementation by this report.

The installed-source baseline supplied for this investigation is commit `3e2305c811e89b9f7a4048134d245eaa38b3a96e`, whose [package version is 1.6.2][version]. The inspected checkout matched that exact commit; this report does not substitute the repository's older main branch or a historical capacity candidate. Node v24.16.0 documentation is used for the APIs below because that is the [bundle script's default][bundle]; the actual installed executable, native kernel availability, SQLite build and runtime configuration still belong in a future artifact-bound baseline.

This was static research only: no new indexing, query workload, benchmark, executable validation, heap snapshot, runtime reconfiguration or process control. There are **no measured byte savings, percentages, speed comparisons, leak diagnoses or claims that these sites dominate installed RAM**. Source locations establish allocations and potential overlap, not actual retained size.

### Non-negotiable comparison contract

- Preserve the same eligible source, extraction/resolution decisions and graph edges; complete/current answers, source bodies, ordering, output budgets, truncation notices, security refusals and recoverable failure behavior.
- Keep uncapped query admission, existing direct/fallback behavior, freshness barriers, isolation and idle retirement. Do not replace concurrent readers with a serial worker or add busy deadlines.
- An incidental speed cost of a real reduction in copies or representation is acceptable to investigate. A reduction caused only by stretching time, admitting less simultaneous demand, reducing active service capacity merely to slow required work, removing query concurrency, doing less work or postponing required work is not. Sharing duplicate state and safely retiring idle workers remain valid when admitted demand is preserved.
- Internal streaming means consuming the **same required data** with less overlap, not returning a partial answer or deferring correctness work to a later query.

## Ranked hypothesis set

Rank reflects an investigation sequence (relevance to active queries, evidence specificity and change surface), **not measured return on investment**.

| Rank | Hypothesis and phase | Actual representation to remove | Key prerequisite / disproof |
| --- | --- | --- | --- |
| 1 | Range-based source rendering; active queries | Whole-file split-string arrays plus temporary slice/join strings when only some source ranges are emitted | Exact text and budget parity, including newline edge cases; reject if offsets allocate more or change dedup/trimming decisions |
| 2 | Decode SQL rows directly into final results; active queries and indexing lookups | Simultaneously retained raw row array/objects plus mapped Node/Edge wrappers | Consume each cursor synchronously with independent statement ownership and equivalent error handling; reject if overlap is immaterial |
| 3 | Transfer exclusive kernel table buffers; conditional indexing path | Copies of raw tables at parse-worker → parent and parent → store-worker messages | Verify native buffers' transferable ownership and absence of later sender use; reject if an ownership-repair copy cancels the gain |
| 4 | Feed SQL batches from a row producer; indexing | Full-file binding-row arrays layered on decoded Nodes/Edges, then another flat parameters array | Preserve existing batching, transaction and insertion order; direct kernel binding is a later, higher-risk extension |
| 5 | Canonical Nodes within one resolver generation; indexing/resolution | Independently hydrated Node wrappers and parsed metadata for the same ID in different lookup caches | Audit mutation and eviction/retention first; reject if a canonical store grows the live set |

### 1. Render source ranges without splitting every line into strings

**Observed.** Explore reads an admitted file into a string, splits the entire file on LF, then repeatedly slices/joins ranges. It also builds strings solely to count the length of already-covered spans. The complete text remains available for fingerprinting and source rendering. These operations occur **after** file admission/budget decisions; their costs need not be proportional to the emitted source. See [explore source acquisition, range rendering and covered-character accounting][explore-text]. The files tool similarly splits all lines before emitting its offset/limit window and character budget, with explicit trailing-empty-line parity. See [files source/window handling][files-text].

**Change hypothesis.** Retain the one decoded source string, scan its newline positions once, and represent ranges with compact offsets. Build output strings only for ranges actually used by the existing renderer. Compute covered-source length from offsets rather than allocating `slice(...).join(...)` solely to obtain `.length`. Start with the files renderer or explore's range helpers, not a rewrite of selection/ranking.

This removes arrays of line references/string views and intermediate joined text; it does **not** establish that every split line currently owns a separate copy of its characters. V8 representation and retained backing strings must be measured. A dense typed offset table can itself cost more than a split array on unusual files, so include both long-line and many-short-line cases.

**Equivalence obligations.** Preserve UTF-16 character accounting, LF versus CRLF handling, empty files, missing/final newline, trailing empty line, 1-based line numbers, gap markers, numbered output and every existing budget/overshoot rule. Keep the current validated path and file read, hash the same full text, and preserve read-failure handling. Do not turn range rendering into repeated disk reads or a cross-query source cache: either could change freshness, error behavior or latency.

**Possible effects.** Less allocation and GC; one full scan remains necessary for exact line counts and fingerprinting. Offset construction and repeated range lookup may change CPU use. No disk increase is required and no query queue/concurrency change is involved.

**Minimal falsification probe, later.** On identical admitted files, compare full serialized files/explore responses byte-for-byte for every rendering mode, dedup/no-dedup and budget tier; include CRLF/LF, non-ASCII/surrogate characters, edits between calls and unreadable files. At the same overlapping request arrivals and achieved concurrency, compare allocation counts, peak live heap, private commit, wall time and throughput. A lower peak caused merely by longer request spacing fails the contract.

**Related but separate.** Resolution retains both source and split-line caches: [content cache][resolver-content], [split-line cache construction][resolver-lines]. Replacing those arrays with offsets would require updating the context consumers that expect `string[]`, preserving its **CRLF-normalizing** split semantics. It is not a drop-in reuse of the query renderer's LF-only logic.

### 2. Remove raw-row-array overlap, without changing final result shape

**Observed.** Several DB methods call `.all()` and then map every row into a second object representation. Examples include [file-node lookups][file-node-rows], [name lookups][name-node-rows], [kind lookups][kind-node-rows], and [ordered incoming/outgoing edges][edge-rows]. [Row converters][row-converters] create the public Node/Edge wrappers and parse metadata/decorators/type parameters. The adapter forwards `all` and `iterate`; Node documents that the former returns a row-object array and the latter a row-object iterator. See [adapter forwarding][adapter-iteration] and [Node StatementSync documentation][node-sqlite].

**Change hypothesis.** For a measured hot method, consume an iterator and append each converted row directly to the **same final result array**. The final answer, graph working set and metadata remain intact; only the raw row array and its period of overlap disappear. This does not by itself avoid all per-row allocations, and copying string references is not proof that all string bytes were duplicated.

Where a consumer provably only needs IDs/names/counts, a dedicated projection may remove more decoding, but do not narrow a general Node-returning API and silently remove fields. The code already has examples of dedicated lightweight queries and streaming scans; reuse that design discipline rather than inventing a whole-graph replacement.

**Equivalence obligations.** Keep SQL predicates, bound parameters, total ordering and all decoded fields. Fully consume before returning; no cursor may escape across awaits or collide with another use of a cached statement. Existing [kind iterators deliberately prepare independently][kind-streams] to avoid overlapping-cursor conflicts. Also preserve failure translation: [the adapter currently rewrites failures for get/all but intentionally leaves iterate raw][adapter-errors]. A mechanical substitution can change observable error guidance; this is part of the candidate, not an accepted regression.

**Possible effects.** More JS/C++ transitions can cost CPU compared with native `.all()`; that cost is incidental to removing a real representation, not a reason to space out requests. Longer-held cursors can affect WAL checkpoint progress if consumption yields or leaks, so the first probe must keep synchronous consumption and guaranteed closure. No disk-format or connection-count change is required.

**Minimal probe, later.** Choose one high-fanout callers/callees lookup and one dense-file node lookup identified by a baseline. Compare ordered deep result equality and complete tool-response equality, including corrupt/missing metadata and error paths. Compare peak live wrappers/arrays and DB cursor lifetime under identical overlapping requests; reject if WAL retention or latency offsets the benefit.

### 3. Transfer owned raw kernel tables instead of cloning them twice

**Observed.** On the optional native-kernel path, [parse workers already defer per-node decoding][parse-buffers] and return five raw table buffers in an ordinary `postMessage` without a transfer list. The parent [forwards those buffers to the store worker][forward-buffers], whose [send method also has no transfer list][store-send]. The [store worker then decodes them][store-decode]. This is already a substantial deferred-materialization optimization; the residual candidate is buffer ownership, not rebuilding it.

Node's [worker-message contract][node-workers] says unlisted ArrayBuffer memory is copied; transferred buffers invalidate sender views. Pooled/non-owned Buffer storage needs special care and may be untransferable. The [native result shape][native-buffers] alone does not prove exclusive, transferable backing storage.

**Change hypothesis.** Where backing-store ownership is verified, transfer each unique buffer backing store at both hops, with explicit ownership handoff. Keep the same parse-worker result, store ordering, acknowledgement protocol and deferred-decode boundary. Do not assume adding `[buffer.buffer]` indiscriminately is safe. If backing buffers alias, deduplicate the transfer list and account for every view.

**Equivalence obligations.** Preserve metadata/counts/errors needed after posting, extra CommonJS refs, fallback extraction, framework extraction hooks and kernel-disabled behavior. The main thread currently uses result counts/errors beyond store dispatch; transfer only raw tables after proving there is no subsequent access. Preserve retry/recovery behavior: after transfer, the sender cannot retransmit the original backing memory. The present source is not evidence that a zero-copy failure/retry route already exists.

**Possible effects.** Potentially fewer native/external-memory copies and less copy CPU; benefit only when the kernel/deferred-store path is actually used. Ownership-normalization copies or a more elaborate retry envelope can reduce or erase the advantage. No new admission limit, store-window reduction, or lower parse concurrency is part of this hypothesis.

**Minimal probe, later.** First inspect/test transferable ownership on the **exact bundled native addon** and runtime; exercise aliasing, pooled buffers, worker failure before/after handoff, kernel fallback and framework hooks. Then compare raw table bytes, final database content/edge order/errors and peak external memory at the same parse and store overlap. Do not count a detached sender view alone as whole-process RAM savings.

### 4. Avoid full-file SQL binding-row arrays before considering kernel-to-bind

**Observed.** [insertNodes][node-bindings] builds all node binding rows and name-segment rows before issuing batched inserts. [storeFileBundle][bundle-store] builds another full edge-row array within the file transaction. [runBatched][batch-bindings] then creates a flat parameter array for each existing multi-row statement. On the kernel store route, this follows [decode into full Nodes, Edges and refs][kernel-decode] and [validation/filter/denormalization][bundle-finalize]. These are distinct representations whose lifetimes can overlap; the claim is not that every referenced string is copied at each stage.

**Change hypothesis.** First make the existing batch writer accept a row producer with bounded temporary binding storage while retaining its current batch-size sequence. Consume all required rows within the same transaction. This removes a full-file binding layer without throttling parsing or changing the store window. Only if this is valuable, investigate decoding kernel rows directly into the established binding path, retaining just the ID/validity information needed for endpoint filtering.

**Equivalence obligations.** Preserve file and row insertion order, duplicate/replace/ignore behavior, required-field rejection, cache invalidation, name-segment vocabulary, null/default conversions, JSON representation, reference denormalization and one-file transaction atomicity. A direct decoder must not bypass the shared finalize contract or change `extraJson`/metadata semantics. [Ordered store admission][store-order] is correctness-relevant; writing whichever file or resolver chunk finishes first is not an optimization compatible with this baseline.

**Possible effects.** Less per-file temporary allocation; incremental decoding may add CPU or impede batching if implemented carelessly. Retain current SQL batches before contemplating changes to batch shape or native interfaces. No extra disk traffic is inherently required.

**Minimal probe, later.** Compare logical database tables and ordered query results for duplicate IDs, invalid nodes, foreign-key filtering, extra refs, optional fields, JSON metadata, segment vocabulary and store failures/rollback. Compare phase-specific native/heap peak memory and indexing throughput at unchanged concurrency. Raw database file hashes are not an equality oracle where timestamps/page layout legitimately differ.

### 5. Share resolver Node representations only within a proven stable generation

**Observed.** Resolver caches separately retain per-file, name, lower-name, qualified-name, ID and kind lookups; [constructor and field declarations][resolver-caches] establish these lifetimes. [File and name context lookups][resolver-lookup-file-name] call separate SQL hydration methods; [qualified-name/kind lookups][resolver-lookup-qualified] do the same. Thus the same logical ID can be independently decoded into multiple wrappers and JSON arrays. This is not a claim that every cache reference is a separate object: [resolver ID lookup already reuses QueryBuilder's returned object][resolver-id].

**Change hypothesis.** Use one canonical representation per ID within the existing stable node-generation window and let lookup caches hold references or IDs. Preserve lookup-array order and multiple distinct overloads. Keep retention tied to cache reachability/eviction, not an additional strong map of every seen Node: an unbounded canonical map could consume **more** RAM than the existing independent LRUs.

**Equivalence obligations.** Audit consumer mutation before sharing objects. Preserve the [current cache invalidation boundary][resolver-clear-all] and [generation-sensitive edge/supertype semantics][resolver-clear]; never intern across projects, database replacement or an incomplete freshness boundary. Do not simply shrink the LRUs: that exchanges retention for repeated reads without establishing a better representation.

**Possible effects.** Fewer duplicate wrappers/JSON decodes versus extra indirection and bookkeeping. A lower-rank candidate because mutation and retention risks are broader than the explicit temporary arrays above.

**Minimal probe, later.** Record duplicate live Node IDs and retained bytes by cache, then attempt generation-scoped reuse only if duplication is material. Compare complete extraction/resolution tables and overload ordering across full index, incremental edit, deletion and database replacement. Check stale-object reuse and total cache reachability under eviction. A new intern table exceeding removed duplicates disproves this implementation.

## Findings that narrow the search

### Whole-graph materialization: already partially solved

The source already streams kind/language/decorator scans and documents deterministic order and cursor ownership: [existing streaming APIs][kind-streams]. Resolver warm-up uses string name/file sets rather than full Nodes, and [its yielding variant streams names into the same final Set][warm-caches]. The source explicitly notes that yielding alone keeps the same final memory. A search of the inspected query/index/resolution paths found no production call to `getAllNodes()`; its existence is not evidence that every query loads the graph.

Some contexts intentionally retain the full list for a requested kind because repeated per-reference scans previously dominated work: [kind cache versus uncached scans][resolver-lookup-qualified]. Removing that cache or rematerializing it on each use is not automatically an optimization. Candidate 5 asks whether the **same live information** can have fewer wrappers.

Resolver chunk reassembly also retains chunk results and combined arrays: [ordered Promise.all/concatenation][resolve-chunks]. These are mostly duplicate references in outer arrays, not proven duplicate payloads. Changing completion-order admission would alter semantics; incremental ordered consumption needs a separate demonstrated benefit and careful failure/atomicity analysis. It is not a required next step.

### Parser lifetimes: do not diagnose a missing free from process RAM alone

The ordinary tree-sitter extraction path already [deletes its tree and clears source in finally][tree-cleanup]. Parse workers [periodically reset parsers][parse-buffers]; [parser reset deletes the parser, while language caches persist][parser-reset]. This does not prove WASM/native heap release to Windows, nor that retained grammar state is unnecessary. No new forced GC, more aggressive parser recycling, grammar reload policy or worker reduction is recommended here. First distinguish live objects, allocator high-water marks and genuinely reusable language state.

Grammar pre-read bytes are [posted to each parse worker without transfer][grammar-post]. Shared immutable bytes could be a future representation question, but a single transferable ownership handoff cannot fan the same buffer to several workers, and removing raw byte copies does not remove per-worker parser/runtime state. Do not fold this into candidate 3 without separate attribution.

### SQLite: settings are not measured savings

Normal connections configure `cache_size=-64000`, `temp_store=MEMORY` and `mmap_size=268435456`: [connection configuration][db-config]. Resolver workers separately request [a 32000-KiB cache][resolver-db]; store workers request [64000 KiB and in-memory temp storage][store-db]. These are connection/phase facts, not evidence that every process commits all those bytes.

SQLite documents that [page-cache allocation is demand-driven and the setting is a suggested maximum][sqlite-cache]. Its [mmap documentation][sqlite-mmap] explains that mapped reads can avoid a private page copy; mapped address space is not all private RAM, and Windows has mapping/truncation considerations. Therefore do not add configured maxima or summed working sets and call that unique physical usage.

**Reject as shortcuts:** lowering heap/cache/mmap limits without attribution; disabling mmap solely to shrink displayed address space; moving temp storage to disk solely to relocate RAM; switching to [SQLite shared-cache, which SQLite discourages and which changes locking][sqlite-shared]; enabling read-uncommitted; sharing one synchronous connection by serializing otherwise concurrent queries. None is established as a result-preserving allocation improvement here. A later measured cache-policy question can be graduated if page-cache duplication is actually material, but it must compare useful cache/I/O behavior and whole-system memory.

## Follow-up boundary and minimal common evidence

Use an installed-artifact baseline, then start the next experiment with **one bounded candidate** so its effects can be attributed. The final route may combine multiple justified optimizations; this report does not make the whole list an implementation obligation.

1. Pin executable/package/native-addon identities, runtime/SQLite versions, source commit, index identity and workload input. Public findings need sanitized methods and summaries, not local configuration or paths.
2. Measure active queries, indexing overlap and quiescence separately. Include all relevant processes and native/external allocations; keep private committed bytes, resident working set, shared/mapped pages and address space distinct. Attribute row wrappers, source lines, cache Nodes and raw table buffers before ranking by expected benefit.
3. Compare baseline/candidate at the same arrival schedule, overlapping demand, selected files, answer budgets and freshness requirements. Report latency and completed throughput alongside memory. Keep result-equivalence and uncapped-admission checks independent of the memory claim.
4. Probe cold/warm requests, burst growth, concurrent cross-project work, source edits/deletions, DB replacement and failure/fallback paths relevant to the selected change. Production index construction remains human-led; research here did not authorize it.
5. Reject a candidate that saves memory only by slowing/spacing required work, returns less source, changes graph selection/order, leaks cursors/WAL marks, retains stale shared objects or moves the cost to another unmeasured process/cache.

Process topology/shared-service lifecycle is intentionally a separate investigation. This report proposes no installation switch, active-service restart, runtime activation, public release, schema migration or production implementation. All five candidates remain **source-only / unmeasured / unselected**.

## Exact-source references

All repository links below point to the installed-source commit, not a moving branch.

[version]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/package.json#L1-L5
[bundle]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/scripts/build-bundle.sh#L20-L26
[explore-text]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/tools.ts#L5729-L5794
[files-text]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/mcp/tools.ts#L7922-L7964
[resolver-content]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L624-L647
[resolver-lines]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L836-L846
[file-node-rows]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/queries.ts#L1030-L1042
[name-node-rows]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/queries.ts#L1403-L1417
[kind-node-rows]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/queries.ts#L1301-L1309
[edge-rows]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/queries.ts#L2195-L2238
[row-converters]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/queries.ts#L170-L208
[adapter-iteration]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/sqlite-adapter.ts#L110-L126
[adapter-errors]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/sqlite-adapter.ts#L66-L76
[kind-streams]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/queries.ts#L1312-L1377
[parse-buffers]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/parse-worker.ts#L87-L125
[forward-buffers]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/index.ts#L2342-L2375
[store-send]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/store-writer.ts#L154-L167
[store-decode]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/store-worker.ts#L60-L75
[native-buffers]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/codegraph-kernel/src/lib.rs#L59-L68
[node-bindings]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/queries.ts#L536-L596
[bundle-store]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/queries.ts#L610-L651
[batch-bindings]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/queries.ts#L336-L358
[kernel-decode]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/kernel/decode.ts#L63-L173
[bundle-finalize]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/store-writer.ts#L38-L65
[store-order]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/index.ts#L2425-L2448
[resolver-caches]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L244-L343
[resolver-lookup-file-name]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L685-L713
[resolver-lookup-qualified]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L776-L796
[resolver-id]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L528-L535
[resolver-clear]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L268-L289
[resolver-clear-all]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L540-L578
[warm-caches]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/index.ts#L487-L525
[resolve-chunks]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/resolver-pool.ts#L263-L294
[tree-cleanup]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/tree-sitter.ts#L766-L775
[parser-reset]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/grammars.ts#L841-L865
[grammar-post]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/parse-pool.ts#L302-L308
[db-config]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/db/index.ts#L30-L44
[resolver-db]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/resolution/resolver-worker.ts#L53-L64
[store-db]: https://github.com/joss-isbell/codegraph/blob/3e2305c811e89b9f7a4048134d245eaa38b3a96e/src/extraction/store-worker.ts#L81-L97

## Runtime/database primary references

[node-sqlite]: https://nodejs.org/download/release/v24.16.0/docs/api/sqlite.html
[node-workers]: https://nodejs.org/download/release/v24.16.0/docs/api/worker_threads.html#portpostmessagevalue-transferlist
[sqlite-cache]: https://www.sqlite.org/pragma.html#pragma_cache_size
[sqlite-mmap]: https://www.sqlite.org/mmap.html
[sqlite-shared]: https://www.sqlite.org/sharedcache.html#use_of_shared_cache_is_discouraged
