# Embedded repositories below a non-Git indexing root

- Task: [Determine how the reviewed non-Git scanner approach fits current scanning and watching](https://github.com/joss-isbell/codegraph/issues/3).
- Role: exploration; author: research agent; report version: `embedded-scope-research-v1`; date: 2026-10-15.
- Examined snapshot: public repository commit `8f1f842b5b828175992d2ce2cdf673b955b2cce9`, package version `1.6.2`.
- State: research complete; implementation and acceptance remain pending. This note records source findings and proposed decisions, not approval to change scope or activate a runtime.
- Validation: static source and primary documentation were read. No build, engine, index, target test, or runtime activation was run for this research. Historical fixture evidence described below applies only to its retained candidate.

## Finding

The existing `includeIgnored` configuration can express the desired selection. The source repair should make it work when the indexing root is a plain directory, then keep scanner, scoped sync, and watcher scope consistent. A new scanner configuration field is unnecessary for this behavior.

The current failure has two connected causes:

- `discoverEmbeddedRepoRoots()` returns an empty list when Git cannot resolve a repository from the root. The comment assumes the filesystem walk handles child repositories already. [Discovery source](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L959-L977)
- The filesystem fallback honors parent ignore rules and unions ordinary `include` files, but never reads `includeIgnored`. A parent ignore rule can therefore prune a selected embedded repository before its own rules are reached. Both synchronous and asynchronous scans reach this fallback. [Fallback selection](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L1540-L1567), [asynchronous scan](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L1595-L1623), [filesystem walk](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L1626-L1772)

Existing coverage does not contradict this finding. The non-Git workspace test checks visible child repositories and child-local ignore rules; it has no parent ignore rule and no explicit opt-in. [Existing non-Git test](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/__tests__/multi-repo-workspace.test.ts#L354-L367)

## Shared contract to preserve

The two inclusion fields have different purposes:

- `includeIgnored` selects embedded Git repositories whose discovery would otherwise be hidden by parent ignore rules.
- `include` force-adds ordinary first-party source despite ignore rules. Replacing the former with the latter would bypass child ignore rules and change the contract. [Configuration definitions](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/project-config.ts#L34-L69)

`ScopeIgnore` and `buildScopeIgnore()` are the existing shared seam. Git enumeration filters collected files through that matcher, scoped sync applies a cached instance, and the watcher constructs and refreshes the same matcher. The filesystem fallback separately layers per-directory matchers; the repair must avoid introducing another independent interpretation. [Scope builder](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L838-L922), [Git enumeration](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L1288-L1304), [scoped-sync cache](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L1853-L1901), [scoped/full sync](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L3320-L3343), [watcher construction](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/sync/watcher.ts#L395-L415)

Preserve these existing priorities and distinctions:

1. Project `exclude` wins across the full workspace, including tracked files, selected repositories, and their ancestors.
2. Ordinary paths use parent scope. A selected child repository uses its own rules; making the route to that child traversable must not admit unrelated files beside it.
3. Built-in dependency/build exclusions apply to the full embedded path. Selection does not automatically revive `node_modules`, `dist`, or `target`.
4. Git internals and CodeGraph data directories are separately excluded from ordinary walking and watching.
5. General `include` continues to override ignore rules only within its existing limits.

[Matcher priority](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L866-L897), [walk exclusions](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L1698-L1701), [watcher exclusions](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/sync/watcher.ts#L757-L775), [data-directory definition](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/directory.ts#L75-L87), [existing child-scope test](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/__tests__/multi-repo-workspace.test.ts#L193-L212)

## Lessons from the reviewed approach

A privately supplied v3 scanner review (isolated-fixture snapshot, 2026-10-10) describes a candidate that addresses the core failure:

- Derive static prefixes from explicit `includeIgnored` patterns.
- Reuse bounded embedded-repository discovery under those prefixes.
- Match each discovered repository against the complete allowlist and deduplicate results, so a shared prefix cannot admit unselected siblings.
- Enumerate selected children through their own Git commands, apply shared scope, and union their supported source paths with ordinary visible files.
- Feed the same selected roots into watcher/scoped-sync scope.
- Compile child-local nested ignore patterns for existing and future filenames rather than depend only on snapshots of already ignored directories.
- Apply nested negations in hierarchy order, suppress rule files below ignored ancestors, and prevent a file negation from crossing an ignored parent directory.

The review reports isolated checks for wildcard/sibling isolation, existing and future ignored filenames, negations, inactive nested rule files, same-instance configuration removal/restoration, incremental add/modify/deletion, database identity, and preserved unrelated dependency rows. Those private artifacts are not independently public-verifiable from this note. They support the approach as a source-port starting point; they do not establish acceptance for this repository snapshot, installed behavior, production-sized cost, or live native watcher delivery.

Port the behavior into TypeScript and preserve current call contracts. In particular, current source passes project-root context to `isSourceFile()`, which supports contextual file recognition. Copying an older generated module could discard such intervening details. [Current source-file calls](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L1595-L1618)

## Discovery and boundary constraints

- Literal directory patterns ending in `/` can start directly at the selected repository. Wildcards start at their literal prefix; a leading wildcard yields a project-root search. The existing helper also drops the last segment of a wholly literal pattern without a trailing slash. Preserve documented matching semantics and test each spelling. [Static-prefix derivation](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L527-L567)
- Existing discovery searches at most four directory levels and 2,000 directories per search, stops descending at a found repository, and skips built-in ignored directories. Broad wildcard selection can still miss repositories beyond those bounds. Removing query admission limits does not require removing these scanner bounds. [Discovery limits and traversal](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L699-L785)
- Explicit-start discovery needs project-relative boundary checks. The pattern loader validates strings, not traversal. Reusing static prefixes as filesystem starting points must not let `../` escape the project, or let directly named starts bypass Git/CodeGraph-data exclusions that ordinary walking enforces. This is a required source-port review point; no exploit or executable probe was run. [Pattern validation](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/project-config.ts#L208-L229), [ordinary forced-include containment guard](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L627-L646)
- Linked worktrees and submodule worktrees are skipped as duplicate working views; ordinary submodule checkouts remain eligible. Preserve that distinction. [Classifier](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L704-L739), [existing real-worktree tests](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/__tests__/multi-repo-workspace.test.ts#L290-L348)
- Normal scanning and watcher topology follow directory links with realpath cycle deduplication; the discovery BFS descends directory entries rather than symbolic-link entries. A directly selected link and a wildcard-discovered link can therefore behave differently. Retain the existing general link policy unless a separate decision changes it, and test direct directory links and cycles. [Scanner link handling](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L1669-L1727), [watcher topology](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/sync/watcher.ts#L486-L520)

## Watcher and ignore-precedence gaps

Cached patterns can classify a future source filename correctly while the mechanism for changing those patterns remains incomplete:

- Root `codegraph.json` and root `.gitignore` changes bypass ordinary scope filtering and request full reconciliation.
- Nested `.gitignore` changes are considered only after scope filtering. An active child rule file that ignores its own filename can suppress its own edit.
- Only the root `.git/info/exclude` receives the special refresh exception. Selected-child metadata changes are otherwise suppressed with Git internals; an external `core.excludesFile` is not watched here.
- Directory topology refresh rebuilds the watch tree and requests full sync, but does not rebuild `ignoreMatcher`. A selected repository created after startup may therefore require a scope refresh or restart before joining the cached child set; the source explicitly describes that lifecycle.
- Full sync clears the extraction scoped-matcher cache, but that is distinct from refreshing the watcher's matcher.

[Event filtering and refresh](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/sync/watcher.ts#L624-L710), [topology refresh](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/sync/watcher.ts#L523-L534), [documented matcher lifecycle](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/sync/watcher.ts#L348-L356), [extraction cache reset](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L3338-L3343)

Git documents deeper `.gitignore` files overriding higher ones, with the last matching pattern winning at the same precedence. Negating a file cannot revive it beneath an ignored parent. Git also ranks `.gitignore` above `info/exclude`, which ranks above `core.excludesFile`; tracked files are not affected by Git ignore rules. [Official Git ignore documentation](https://git-scm.com/docs/gitignore)

Current `buildDefaultIgnore()` appends info/exclude and core.excludesFile after root `.gitignore`, giving conflicting later entries priority in the flattened matcher. CodeGraph also deliberately filters collected tracked files through its scope matcher. The historical approach retains those pre-existing choices. Do not describe the resulting behavior as exact Git equivalence without tests and an explicit decision about these distinctions. [Extra-exclude loading](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L390-L423), [matcher construction](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L456-L472), [tracked-file filtering](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/src/extraction/index.ts#L1288-L1298)

## Decisions for the implementation handoff

| Decision | Recommendation | Product consequence |
| --- | --- | --- |
| Configuration surface | Reuse `includeIgnored`; preserve `include` separately. | Existing selections work without a new scanner setting or a broader ignore bypass. |
| Selection and exclusion priorities | Preserve explicit exclusions, full-path built-in exclusions, and Git/CodeGraph-data boundaries. | Selecting a repository does not sweep in unrelated dependencies, build output, or index internals. |
| Discovery limits | Retain existing bounds and require full-pattern filtering. | Literal selections stay narrow; broad patterns still have bounded discovery rather than exhaustive unlimited search. |
| Live refresh | Include active child `.gitignore` edits and creation/removal of selected repositories in acceptance. Decide separately whether child metadata/global-ignore edits require automatic observation. | Avoid a watcher that keeps serving or restoring files under obsolete rules; any retained refresh limitation must be visible to users. |
| Cross-file Git precedence | Add conflicting-rule tests and correct ordering if full child ignore semantics are part of the accepted scope. | A child's explicit negation should not lose to a lower-priority global exclusion accidentally. |
| Tracked-file ignore treatment | State and test the intended CodeGraph behavior rather than infer it from Git. | Maintainers can distinguish compatibility with existing CodeGraph filtering from Git's tracked-file semantics. |
| Worktrees and links | Preserve current general policies; test new explicit-start paths. | The repair avoids duplicate worktree graphs and accidental expansion through an unchecked start path. |

These are recommendations for the map. Research does not stand in for the maintainer's decisions about consequential exclusions or refresh limitations.

## Proposed implementation and acceptance

The principal source changes belong in `src/extraction/index.ts`; `src/sync/watcher.ts` may need changes for the accepted live-refresh contract. Extend `__tests__/multi-repo-workspace.test.ts`, `__tests__/watcher.test.ts`, and relevant inclusion/exclusion tests. Existing watcher refresh coverage uses injected events and does not cover explicit hidden child repositories under a non-Git root. [Existing refresh tests](https://github.com/joss-isbell/codegraph/blob/8f1f842b5b828175992d2ce2cdf673b955b2cce9/__tests__/watcher.test.ts#L625-L753)

Required acceptance should cover:

1. A non-Git parent with a parent ignore rule, selected and unselected child repositories, ordinary visible parent source, and unrelated ignored siblings. Compare synchronous scan, asynchronous scan, shared scope, scoped sync, and full sync.
2. Literal roots, a shared-prefix wildcard, leading wildcards, overlapping selections, duplicate roots, and discovery-bound behavior. Require complete matcher filtering.
3. Child root/deeper directory rules, ignored filename patterns, allowed negations, inactive nested rule files, and existing/future files created after matcher construction. Compare appropriate untracked-path expectations with Git.
4. Built-in exclusions even when tracked, explicit project excludes, directly selected roots under forbidden ancestors, project traversal rejection, linked/submodule worktrees, directory links, and cycle handling.
5. Same-instance configuration removal/restoration; root and active child ignore edits; creation/removal of selected repositories; and whichever metadata/global-ignore refresh behavior is accepted. Verify full reconciliation removes newly excluded rows and admits newly selected rows.
6. Add, modify, parsed-symbol replacement, and physical deletion while selection remains enabled. Compare database identity and unrelated file/node/edge rows; scope removal alone is insufficient evidence of deletion catch-up.
7. Two independent projects with different selections/rules, plus regressions for a Git parent and an indexing root that is an ignored subdirectory of an enclosing repository.
8. Native Windows watcher delivery as well as deterministic injected-event tests. Exercise the Linux per-directory strategy where its pruning behavior matters; a matcher probe alone does not establish live watching.
9. Production-representative discovery/ignore-collection cost before making latency claims. Existing Git collection calls have per-command deadlines and buffer bounds; retaining those does not establish total startup cost across many repositories.
10. A deterministic verification wrapper with deadlines, process cleanup, bounded logs, short results, and pinned source/build inputs. Keep source acceptance, installed-runtime uptake, and production index preservation as separate evidence gates.

No current-snapshot executable acceptance is available from this research. The next step is to resolve the scope decisions above, then implement and verify against the selected contract.