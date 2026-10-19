# Isolated validation for Issue #7

These adapters establish Issue #8's protected baseline and supervising runner.
They do not activate unlimited query mode, change included-repository scope,
modify the installed CodeGraph configuration, or establish candidate acceptance.
The complete regression, per-execution-mode candidate proof, packaging, rollback
rehearsal and installed acceptance belong to subsequent tickets.

## Run contract

Invoke `python scripts/validation/runner.py <plan.json>` with a saved plan that
declares these fields before launch:

```json
{
  "sourceRoot": "/absolute/owned/checkout",
  "cwd": "/absolute/owned/checkout",
  "output": "/absolute/new/attempt",
  "command": ["/absolute/runtime", "/absolute/adapter", "argument"],
  "deadlineSeconds": 900,
  "scenarios": ["declared-scenario"],
  "artifacts": ["/absolute/built/artifact", "/absolute/dependencies"],
  "environment": {"BASELINE_ROOT": "/absolute/protected/baseline"},
  "executionMode": "declared-mode"
}
```

Paths are literal arguments, never shell command strings. The output directory
must not already exist. Keep every failed/incomplete attempt and use a new name
for a corrected attempt. A Git source root binds its commit, binary dirty patch,
tracked and relevant untracked files. An archived baseline instead carries
`baseline-source-commit.txt`. Generated directories are excluded from the source
inventory; declare built output and dependencies as artifacts when they are
inputs. The runner automatically binds its interpreter and validation scripts.
Do not edit bound files while a run is active.

`declarations-v1.json` records the provisional 120-second worker case, 90-second
late-result case (release at 47 seconds, client deadline at least 75 seconds),
10-minute build, 15-minute selected integration and 30-minute complete regression
bounds; 30-second idle grace observed for 45 seconds; separate 15-second startup
and 15-second drain allowances followed by 5-second cleanup; 10 MiB per log stream
and 8 KiB excerpts. Self-tests deliberately override their synthetic probe bounds
in saved plans. These are experiment and cleanup bounds, not healthy-query
deadlines or product latency promises. The 0.25-second sampling value is the
minimum pause between polls; actual observed sample gaps include file/OS work.

## Ownership and evidence

- Windows gates the launcher until it joins a kill-on-close Job Object without
  breakaway. Linux uses kernel ancestry and a child subreaper, creation identities
  and PID-bound signaling. Adopted natural exits are reaped during monitoring;
  the gated child's exit status remains owned by `Popen`. Run Linux lifecycle
  work under `docker run --rm --init`.
- Cleanup uses raw kernel membership, independently of optional resource reads.
  It never selects a process from a CodeGraph service registry or a process name.
  Whole-process cleanup preserves the existing engine worker-start protection;
  these tools do not call `worker.terminate()` or change engine teardown.
- Native file-change witnesses and before/after hashes invalidate transient or
  lasting source/artifact changes. Watch failures, lost logs, overflow, a missing
  scenario tail, nonzero exit or incomplete cleanup fail the attempt.
- `receipt.json` binds source/artifacts, declarations, settings, execution mode,
  runtime/OS, events, resource samples, exit and cleanup. Evidence inventories
  recursively bind nested proofs. `summary.json` is produced by the actual
  `report.py` consumer, which verifies required evidence, collection shapes,
  event/scenario equality and produced project/session inventory equality.
- Adapters emit start/assert/complete/final events through `events.py`. Every
  declared scenario must complete and the final assertion must be the tail.
  Optional project/session arrays can be empty; required evidence cannot.

`selftest.py` exercises real producers and the same consumer: success, empty,
singleton and multiple inventories, deadlines, surviving children/grandchildren,
natural root exit, actual OS interruption, saturated output, missing completion
or final evidence, transient mutations and malformed/tampered receipts. Its
unrelated sentinel must survive. Linux additionally proves that an adopted
descendant's natural exit becomes observable while the producer remains live.

## Windows input-integrity lifecycle

New runner receipts use `codegraph-validation-v2` and an explicit platform
integrity profile; historical v1 receipts retain their original meaning.
The `windows-guard-v2` profile uses one aggregate `windows_witness.Guard` through
`integrity.WindowsInputs`: actual native arming precedes a repeated full
input baseline and target release. The observer stays active through process
cleanup and the final input read. Its frozen closed ledger must establish
timely startup, final native drain, joined readers, checked namespace identity
and checked handle closure, exactly matching the Guard's selected watch and
retained namespace domains; advisory snapshots cannot pass the reporting
consumer. The complete terminal integrity file is compact JSON bounded to
64 MiB, with bounded readback. Every new v2 receipt uses this finite integrity
reader, including the Linux profile; historical v1 is not upgraded. Schema,
profile, runtime and saved-plan mismatches refuse before integrity hashing or
decoding, not after an unbounded read. Oversize is a whole failure, not an accepted
row prefix. Startup has one 30-second maximum and close one 8-second maximum,
not fresh budgets per directory. Keep an enclosing bounded owner for stalled
native calls; incomplete observation never becomes acceptance.

Current-runner artifact bindings use the host's literal `Path` identity rules:
Windows case/separator aliases name the same binding, and equivalent keys with
conflicting digests refuse before integrity evidence I/O. Ordinary route checks
precede current-runner byte reads; resolution does not erase a linked route.
Removing every new receipt/plan marker cannot reinterpret a binding to today's
exact runner bytes as genuine historical-v1 evidence.

Supported Windows inputs use literal ordinary local fixed-drive DOS paths,
with native case-insensitive directories and actual same-object/path identity
joins. UNC, device/extended syntax, subst/mapped/removable/virtual drives,
reparse declared-root/ancestor routes, case-sensitive directories and unavailable native identity
queries fail closed rather than inheriting weaker legacy guarantees. Retained
directory handles require `FILE_LIST_DIRECTORY|FILE_READ_ATTRIBUTES`
on every retained ancestor and leaf, with read/write sharing but no delete
sharing. Denied or unknown access vetoes startup before target release;
attributes-only fallback is not a supported mode.
The source-only policy suppresses existing-directory modified timestamps caused
by allowed child output; it does not promise transient directory-timestamp
assurance. Strict artifact events retain their higher precedence.

Source exclusions still permit generated outputs. Explicit artifacts are
strict, including nested `__pycache__` content, and override source exclusions.
On Windows, the input inventory visits members without following reparse
points. It binds exact bounded junction/symlink reparse bytes and membership,
not just the final resolved target. A nested link in an artifact or nonskipped
source path is admitted only when its literal target route is ordinary on
every component and is independently covered by an explicit strict artifact
declaration. That actual target is also declared to the Guard for retained
route identity. Dangling, unsupported, parent-traversing or linked-chain targets
and undeclared targets refuse before target release. Do not resolve a linked
declaration first or exempt all junctions/caches. The inventory caps members
at 262144, link records at 4096, each native reparse record at 16 KiB and its
conservative metadata domain at 64 MiB; overflow fails, never drops members.
For example, a workspace dependency junction to ordinary `ui` requires `ui`
as a strict artifact. A build writing `ui/dist` cannot use an unchanged strict
whole-`ui` declaration; separate its mutable output rather than weakening the
input policy. Ordinary viewer output outside `ui` is a different plan.
The runner actually sets and records `PYTHONDONTWRITEBYTECODE=1` for children
so importing validation helpers does not mutate those immutable inputs.
The copied-B recovery caller uses the same Windows implementation and keeps
its final component read under observation. Linux retains its existing inotify
implementation and is labelled separately, not promoted to Windows guarantees.

`integrity-tests.py` is the focused normal-caller check: real runner/receipt/
report outcomes for generated writes, source write/restore, strict cached
artifact mutation and separately declared ordinary junction targets; actual
undeclared/linked-chain target preflight vetoes; rejection probes derived from a real completed receipt
and the recovery-compatible single-Witness interface. Run it only through the
same bounded outer supervision contract with a new output directory. It is not
a substitute for the reusable native module's qualification or product suites.
Its real clean positive enters the current runner CLI through a Windows case
alias. Derived negatives strip every new marker and the closed lifecycle, or
introduce conflicting equivalent artifact keys. Oversize checks share one
readonly 64MiB+1 file; their v2 and stripped-plan phases retain exact raw plans
and explicit replay contexts, then restore the active plan in `finally`.
Phase-specific receipts are not all reconsumable under one simultaneous plan.

The focused and existing selftest plan builders explicitly redeclare an actual
configured `PYTHONPYCACHEPREFIX` when it matches `sys.pycache_prefix`, so nested
runner gates/targets preserve an enclosing source-bound cache route despite the
runner's inherited-`PYTHON*` scrub. No prefix is invented for standalone callers
without one; the normal environment isolation policy and scenario counts stay
unchanged. A wrapper using an immutable absent cache prefix must protect that
absence for the complete observation; `-B` alone does not disable cache reads.

### Source-bound normal Windows caller

Use `launch-normal.ps1 -Plan <saved-plan.json> -LaunchOutput <new-launch-dir>`
for normal source-bound builds and selected query tests. The plan must
prospectively declare a fresh absent ordinary local path with an existing
ordinary parent:

```json
{
  "sourceBoundCache": {"prefix": "C:\\owned\\evidence\\absent-cache-attempt-1"},
  "environment": {
    "PYTHONPYCACHEPREFIX": "C:\\owned\\evidence\\absent-cache-attempt-1",
    "PYTHONDONTWRITEBYTECODE": "1"
  }
}
```

Keep the existing plan's other required fields. Build plans retain their
ordinary command `[python, stage.py, "build"]`; selected-test plans retain
`[python, query-tests.py, suite]` and their declared exact test selection. The
launcher uses the declared prefix and selected Python runtime, launching with
`-B -X pycache_prefix=<prefix>` and the matching environment, and adds an
enclosing hard deadline/log bound; it does not replace the runner's owned Job
cleanup. `normal.py` refuses a missing, existing, mismatched or undeclared cache
route before loading helpers, executes the runner/report/integrity/Windows
witness from captured raw `.py` bytes, and invokes the original `runner.run`.

The runner reconciles every executed raw-helper digest against its complete
post-native-arm immutable binding before releasing the target. Its existing
single `WindowsInputs` Guard additionally protects the prefix's absence, with
the same native watch/retained namespace closure and close deadline. The saved
plan environment carries that exact prefix through the existing gate and child
adapters. Terminal evidence records the selected absent domain and the final
absence read under observation; the actual report consumer checks those joins,
the complete closed ledger and the bound raw helper sources. A transient prefix
creation/removal therefore fails even if the terminal path is absent. No cache
directory is created or removed by this route.

For read-only receipt replay, launch the same interpreter with the saved prefix,
`-B`, matching environment and `normal.py <attempt/plan.json> --report
<attempt/receipt.json>`. The prefix must still be absent and the bound helper
bytes unchanged. Legacy plans without `sourceBoundCache` still use `runner.py`
and retain their original policy; they are not upgraded to this source-bound
assurance by `-B` alone. Neither route constitutes product acceptance until the
complete selected producer and consumer tail passes.

### Describe and launch reviewed Windows inputs

`normal.py PLAN --binding OUTPUT.json` uses the same raw-source loader and the
runner's authoritative source/artifact selection, including its interpreter
and complete helper directory. The JSON shape is the existing `source-before`
shape: commit, dirty delta, file digests, root-inclusive membership and reparse
records. It creates only the requested new file, outside source/artifacts,
target output and the absent cache route. It starts no target or native input
observation and creates no home/cache/acceptance receipt. Malformed runnable
plans, invalid supervision bounds and an existing output refuse.

Use the existing bounded launcher for this read-only command as well:

```powershell
$pythonExe = 'C:\Users\atramenta\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe'
$planFile = 'C:\owned\final-plan.json'
$bindingFile = 'C:\owned\frozen-binding.json'
pwsh -NoProfile -File scripts/validation/launch-normal.ps1 `
  -Plan $planFile -Python $pythonExe -LaunchOutput C:\owned\describe-launch `
  -BindingOutput $bindingFile -DeadlineSeconds 60
if ($LASTEXITCODE -ne 0) { throw 'Input description failed.' }
```

Finalize the selected plan and its applicable controlling inputs **before**
this description. Declare the plan itself and those control files as strict
artifacts. Reconcile the candidate source/build/dependency maps and existing
release/proof references against their reviewed values before freezing the
expectation; reject conflicting inherited digests instead of choosing the
latest value. The description is current observation, not proof of that
provenance. An old saved map is not a new observation, and a changed plan or
control set needs a newly prepared expectation. No extra review/resource
documents are required by these commands.

The prospective caller is `launch-reviewed.py`. Its one frozen JSON input has
the fields below; `binding` is the entire parsed description, not counts or a
subset. `controls` holds the applicable existing `{path, sha256}` reference
records with their previously reviewed digests (it may be empty). The selected
plan is checked separately and is always a strict artifact. For example, after
the provenance reconciliation above, prepare it in PowerShell:

```powershell
$selected = Get-Content -LiteralPath $planFile -Raw | ConvertFrom-Json -AsHashtable
$frozen = Get-Content -LiteralPath $bindingFile -Raw | ConvertFrom-Json -AsHashtable
$reviewedControls = @() # Populate from the selected run's existing reviewed references.
function Read-GitFacts([string[]]$Arguments) {
  $rows = @(& git -C $selected.sourceRoot @Arguments)
  if ($LASTEXITCODE -ne 0) { throw 'Git identity read failed.' }
  return $rows
}
$review = [ordered]@{
  plan = @{path=$planFile; sha256=(Get-FileHash -LiteralPath $planFile -Algorithm SHA256).Hash.ToLowerInvariant()}
  binding = $frozen
  controls = $reviewedControls
  git = @{tree=(Read-GitFacts @('rev-parse','HEAD^{tree}')); branch=(Read-GitFacts @('branch','--show-current')); status=@(Read-GitFacts @('status','--porcelain=v1'))}
  python = $pythonExe
  powershell = (Get-Command pwsh).Source
  inheritedTEMP = $env:TEMP
  inheritedTMP = $env:TMP
  launchOutput = 'C:\owned\selected-launch'
  deadlineSeconds = 900
}
$reviewFile = 'C:\owned\reviewed-launch.json' # Outside the observed input roots.
$review | ConvertTo-Json -Depth 32 | Out-File -LiteralPath $reviewFile -Encoding utf8 -NoClobber
$reviewHash = (Get-FileHash -LiteralPath $reviewFile -Algorithm SHA256).Hash.ToLowerInvariant()
& $pythonExe -I -S -B scripts/validation/launch-reviewed.py $reviewFile `
  --sha256 $reviewHash --output C:\owned\immediate-preflight
if ($LASTEXITCODE -ne 0) { throw 'Reviewed launch did not pass.' }
```

Retain the frozen review-file digest with the selected launch instruction;
do not replace it with a new digest after an unexpected change. The caller
checks exact parsed maps, control/interpreter/caller hashes, Git commit/tree/
branch/status, inherited TEMP/TMP, absent process/plan name filters and fresh
endpoints. It invokes `normal.py --binding` through `launch-normal.ps1` before
comparing, then delegates dispatch and native exit/cleanup checks to that same
ordinary launcher. Directory/member totals in `preflight.json` are display
only. Add `--check-only` with its own fresh `--output` for static feedback
without dispatch; neither that success nor a matching description is target
acceptance. A normal run and its complete consumer tail still decide that.
This full-selection caller deliberately rejects `QUERY_TEST_NAME_PATTERN`;
ordinary explicitly filtered focused plans retain their existing launcher.

Keep inputs quiet between the comparison and native arming. This extension
does not add an armed expected-binding protocol or close that existing
interval. Expectation and preflight outputs stay outside observed roots to
avoid circular control hashes. Historical callers/results stay unchanged.

For focused helper feedback, run `integrity-tests.py --binding-only` in a saved
normal bounded plan with scenario `windows-inputs` and environment
`BINDING_POWERSHELL` set to the absolute `pwsh` executable. This selection
uses disposable source/helper copies and a real normal run/report, and checks
added/missing/changed strict helpers, root-inclusive directories, same-count
membership changes, refusal paths and real reviewed dispatch. The existing
`query-adapter-proof.py` synthetic fixture also preserves exact case-sensitive
`(file, fullName)` identities; place its fresh output beneath the checkout's
`.validation` so copied adapters can resolve the checkout's unchanged Vitest
dependencies. A workspace dependency junction's ordinary `ui` target remains
an independently declared strict input.

## Protected baseline and observation protocol

`prepare.py` preserves the exact baseline Git archive and initial dirty-delta
evidence before candidate changes. `runtime-copy.py` protects the verified
supported Windows runtime. `stage.py` supervises dependency bootstrap, the
complete engine/viewer/grammar build, artifact freezing and Linux archival.
The final freeze covers root and workspace-local dependency trees. With those
trees bound as read-only inputs, the build adapter uses Vite's documented
[`--configLoader runner`](https://v7.vite.dev/config/) option to avoid its default
temporary configuration writes inside `ui/node_modules/.vite-temp`. It performs
all ordinary build steps and compares every shipped file with the retained
ordinary-build manifest; the default build's failed integrity attempt is retained.
Local `.validation/` data contains private operational evidence and large
artifacts; stage only the validation source/docs, never that directory.

`baseline.cjs` uses the original public library and real CLI/MCP sessions with
SQLite and native watching. Two separate disposable Git projects contain the
same independently asserted three-file app-to-service-to-leaf flow. Two alpha
sessions share one daemon while beta uses another. Closing alpha, changing its
source and reopening proves catch-up without losing beta. Quiet live sessions
remain usable after 45 seconds, then direct execution and a no-op reconciliation
complete the five-scenario tail. This small protocol measures baseline behavior;
it is not the later all-mode policy or included-repository acceptance suite.
Query concurrency and busy-response settings remain unset. Test-owned daemon
idle timeout is explicitly shortened to one second after clients close, and the
installation check is disabled; live client connections remain open during the
45-second observation. Effective settings record these operational controls,
session modes, tool selection, sandbox and runtime flags.

Fixture evidence records initial/final source, project configuration,
`.gitignore`, `.git/info/exclude` (including default comments), `.git/config`,
the selected sandbox global Git configuration/default ignore files (including
absence), and configured `core.excludesFile` if present. Host system Git config
is disabled for the fixtures. Project/index identities and normalized initial
and final file/node/edge manifests are retained with complete RPC observations.

`observe-workers.cjs` observes Node's public Worker interface without changing
policy. `measurements.py` first consumes the completed baseline receipt, then
interprets resources using explicit phase/sample indices and a separate 45-second
idle window plus its final five seconds. It distinguishes query workers from
other Node workers and OS threads. RSS sums can double-count shared pages;
sampled peaks can miss short peaks; CPU deltas exclude work before a process's
first sample. Native inotify watch counts apply on Linux, not Windows. Compare
baseline and candidate within the same platform, runtime, fixtures and protocol.

`linux.py` prepares a uniquely named image/container, preserves the resolved
image, and supervises the complete Linux bootstrap/build/proof/baseline/
measurement/freeze/archive ledger. Source, fixtures and live receipts stay on
the native Linux filesystem. Only completed evidence is copied to the Windows
export mount: Docker documents that Windows-origin bind mounts do not provide
[Linux inotify events](https://docs.docker.com/desktop/features/wsl/best-practices/).
Container cleanup checks the owned label before removal and verifies absence
with a successful Docker inventory query. The adapters do not stop Docker
Desktop or unrelated containers. Both the OS/runtime/source image and complete
built source/dependency/runtime archive remain available for matched comparisons
and rollback.

The initial accepted source is `bcd1586a48ad65ee2aeda3d562eaafc0aae74e87`.
Durable local attempt paths, hashes, measurements, failed-attempt history and the
independent review version are recorded in the ticket's implementation report.
Public delivery should contain project facts, not local installation inventories.

## Exact previous-installation recovery (Issue #28)

`recovery.py` preserves the actual selected installation B instead of building a
checkout or installing a current npm version. A private input declaration names
the complete installed engine/runtime tree, the selected launcher's dependency
closure, the complete selected Git distribution and the saved MCP configuration.
Protection hashes originals before/after copying and retains the original launch
recipe and every component byte. Inactive launchers and historical logs remain
untouched; they are not silently substituted for the selected B closure. Private
settings, process inventories and artifacts belong under `.validation/`, never in
the public tracker or a commit.

Restoration creates a new path beneath an explicitly declared disposable owner.
It verifies the protected inventory before copying and records the original and
relocated recipes separately. Only the external command/argument paths relocate
to copied components: no launcher, runtime, worker, dependency or saved-config
bytes are patched. The saved full configuration is evidence; it is never written
back to the live configuration. Saved settings do not prove the inherited
environment of an older installed process; unknown values stay unknown.

`capture_snapshot` uses SQLite's online backup API through a read-only source
connection, validates integrity and schema/project metadata in the same read
transaction, and records the original main/WAL/SHM provenance. It produces a
self-contained database in DELETE journal mode. `restore_snapshot` verifies that
snapshot and writes only a new, absent disposable database with absent sidecars.
It cannot overwrite an existing database. A live main-file-only copy is never a
recovery snapshot.

The focused `recovery-tests.py` scenarios exercise real live WAL state and exact
component preservation, path relocation and changed-byte refusal. The actual
`recovery-rehearsal.py`/`recovery-probe.cjs` protocol restores B, uses B's copied
Git/runtime/library to create compatible disposable indexes, and sends ordinary
MCP requests through its exact copied launcher. It records runtime/worker module
identities, keeps copied B under a native integrity witness, snapshots a committed
WAL while disposable owners remain live, restores protected compatible state only
after closing the target owner, then explicitly catches up legitimate later
source changes. The later closed index remains preserved, an unrelated newer
project remains available, and a second reconciliation must be a no-op. Complete
RPC prefixes, source/semantic/index/snapshot/identity and cleanup evidence is
retained even on failure. `recovery-client.cjs` caps cumulative nested stdout at
10 MiB and stderr at 1 MiB per session, retains only those byte prefixes and
fails the whole protocol on overflow. It persists counts and failure diagnostics
on nonzero exit, signal and graceful-close timeout. `recovery-client-tests.cjs`
exercises those paths with real owned children, including repeated short RPC
lines that must not evade cumulative limits. Test-only direct-session, sandbox, temporary-directory and observer
controls are separately identified from saved B settings.

Run every operation through `runner.py` with predeclared scenarios, artifacts,
deadlines and a fresh output directory. `recovery-ledger.py` consumes the actual
receipts, checks current relevant adapter bytes and the protected-to-original B
inventory, and retains all attempt outcomes. This Windows installed-B rehearsal
does not prove B can open a candidate-updated schema, qualify a later candidate
package, change the installation, stop any installed owner, or access a held
index. Those remain later acceptance and explicit installation-decision gates.
