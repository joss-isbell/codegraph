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
RPC, source/semantic/index/snapshot/identity and cleanup evidence is retained even
on failure. Test-only direct-session, sandbox, temporary-directory and observer
controls are separately identified from saved B settings.

Run every operation through `runner.py` with predeclared scenarios, artifacts,
deadlines and a fresh output directory. `recovery-ledger.py` consumes the actual
receipts, checks current relevant adapter bytes and the protected-to-original B
inventory, and retains all attempt outcomes. This Windows installed-B rehearsal
does not prove B can open a candidate-updated schema, qualify a later candidate
package, change the installation, stop any installed owner, or access a held
index. Those remain later acceptance and explicit installation-decision gates.
