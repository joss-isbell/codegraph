"""Exercise preserved B only; immutable inputs and disposable output are separate."""
import json
import os
from pathlib import Path
import subprocess
import sys

from events import event, assertion
from integrity import Witness
from recovery import restore_bundle, file_manifest
from report import digest
from runner import write

out = Path(os.environ["VALIDATION_OUT"]).resolve()
protected = Path(os.environ["PROTECTED_B"]).resolve()
owner = Path(os.environ["RECOVERY_OWNER"]).resolve()
scenarios = ["restore-exact-B", "B-compatible-index", "B-ordinary-MCP", "B-online-snapshot",
             "B-restore-catchup", "B-owned-cleanup", "B-runtime-worker-identity"]
event("start", scenario=scenarios[0])
restored = out / "restored-B"
recipe = restore_bundle(protected, restored, owner)
manifest = json.loads((protected / "manifest.json").read_text())
assertion(scenarios[0], "restored B retains every protected component byte",
          file_manifest(restored / "components") == manifest["files"], manifestSha256=digest(protected / "manifest.json"))
assertion(scenarios[0], "restored launch command selects its copied runtime",
          Path(recipe["launch"]["command"]).resolve() == restored / "components/engine/node.exe")
event("complete", scenario=scenarios[0])

components = restored / "components"
witness = Witness(components, list(components.rglob("*")), artifact=True)
identity_path = out / "B-identities.jsonl"
env = {**os.environ, **recipe["launch"]["env"], "RECOVERY_IDENTITIES": str(identity_path),
       "RECOVERY_RECIPE": str(restored / "restored-launch.json"), "RECOVERY_PYTHON": sys.executable,
       "NODE_OPTIONS": '--require="' + Path(__file__).with_name("recovery-observe.cjs").as_posix() + '"',
       "GIT_CONFIG_NOSYSTEM": "1"}
# Rendezvous, registry and temporary paths are test-owned. The B launcher bytes
# and their original recipe remain retained separately and unchanged.
temp = out / "home/temp"
temp.mkdir(parents=True)
env.update(TEMP=str(temp), TMP=str(temp), TMPDIR=str(temp))
command = [recipe["launch"]["command"], "--liftoff-only", "--disable-warning=ExperimentalWarning",
           str(Path(__file__).with_name("recovery-probe.cjs"))]
write(out / "effective-rehearsal-settings.json", {
    "saved": recipe["launch"]["env"], "protocolOverrides": {"temporaryDirectory": str(temp),
    "homeRegistry": os.environ["HOME"], "NODE_OPTIONS": env["NODE_OPTIONS"], "GIT_CONFIG_NOSYSTEM": "1",
    "CODEGRAPH_NO_DAEMON": "1 (disposable direct sessions only)"},
    "launchRecipe": str(restored / "restored-launch.json"), "originalSettingsBytesUnchanged": True,
    "liveInheritedEnvironment": "unknown; no installed owner environment is inferred"})
result = None
final_files_unchanged = False
final_component_manifest = {}
try:
    if os.name == "nt":
        witness.check()
    # Restoration's earlier hash is not a baseline protected by this observer.
    if file_manifest(components) != manifest["files"]:
        raise ValueError("restored B changed before native integrity baseline")
    result = subprocess.run(command, env={**env, "PYTHONDONTWRITEBYTECODE": "1"}, cwd=out)
finally:
    try:
        final_component_manifest = file_manifest(components)
        final_files_unchanged = final_component_manifest == manifest["files"]
    finally:
        closed = witness.close()
    write(out / "restored-B-integrity.json", {"changes": witness.changes, "errors": witness.errors,
          "filesUnchanged": final_files_unchanged,
          **({"windowsWitness": closed} if os.name == "nt" else {})})


scenario = scenarios[-1]
event("start", scenario=scenario)
assertion(scenario, "whole B recovery probe exited successfully", result is not None and result.returncode == 0, exitCode=result.returncode if result is not None else None)
assertion(scenario, "restored B files remained immutable throughout launch and recovery",
          not witness.changes and not witness.errors and final_files_unchanged)
identities = [json.loads(line) for line in identity_path.read_text().splitlines()]
runtimes = [row for row in identities if row["type"] == "runtime"]
workers = [row for row in identities if row["type"] == "worker-spawn"]
expected_runtime = str((components / "engine/node.exe").resolve())
assertion(scenario, "all observed Node processes and threads use B's copied runtime",
          bool(runtimes) and all(row["executable"] == expected_runtime for row in runtimes)
          and final_component_manifest["engine/node.exe"]["sha256"] == manifest["files"]["engine/node.exe"]["sha256"], observations=len(runtimes))
assertion(scenario, "exact copied launcher and upstream CLI actually executed",
          any(str(components / "launcher/proxy.mjs") in row["argv"] for row in runtimes) and
          any(str(components / "engine/lib/dist/bin/codegraph.js") in row["argv"] for row in runtimes))
assertion(scenario, "ordinary MCP exercised B's own ready query worker",
          any(row["file"].endswith("query-worker.js") for row in workers) and
          any(row["type"] == "worker-ready" and row["file"].endswith("query-worker.js") for row in identities))
assertion(scenario, "all launched worker modules belong to and match copied B",
          bool(workers) and all(Path(row["file"]).resolve().is_relative_to(components / "engine") and
          row["sha256"] == manifest["files"][Path(row["file"]).relative_to(components).as_posix()]["sha256"] for row in workers))
assertion(scenario, "all spawned B workers completed teardown",
          all(any(end["type"] == "worker-exit" and end["pid"] == row["pid"] and end["threadId"] == row["threadId"]
                  for end in identities) for row in workers), workers=len(workers))
write(out / "B-runtime-summary.json", {"runtimeSha256": final_component_manifest["engine/node.exe"]["sha256"], "versions": runtimes[0]["versions"],
      "runtimeObservations": len(runtimes), "workerSpawns": len(workers),
      "queryWorkerSpawns": sum(row["file"].endswith("query-worker.js") for row in workers),
      "protectedManifestSha256": digest(protected / "manifest.json")})
event("complete", scenario=scenario)
event("final", passed=True, scenarios=scenarios)

