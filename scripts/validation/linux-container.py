"""Complete Linux ledger inside an owned docker run --rm --init container."""
import json
import os
from pathlib import Path
import subprocess
import sys
import shutil

from runner import write

root = Path("/work")
here = root / "scripts/validation"
out = Path("/evidence")
out.mkdir()
export = Path("/export")
runtime = ["/usr/local/bin/node", "/usr/local/lib/node_modules/npm"]
scenarios = ["discovery-index", "shared-sessions-projects", "catchup-query", "idle-resources", "direct-session"]
ledger = []
for stage, seconds in [("bootstrap", 600), ("build", 600), ("runner-proof", 180), ("baseline", 900), ("measurements", 120), ("freeze", 600), ("archive", 600)]:
    dependencies = ([] if stage == "bootstrap" else
                    [str(directory) for directory in sorted(root.rglob("node_modules"))
                     if directory.is_dir() and "node_modules" not in directory.relative_to(root).parts[:-1]])
    plan = {"output": str(out / (stage + "-linux-v1")), "sourceRoot": str(root), "cwd": str(root),
            "deadlineSeconds": seconds, "artifacts": runtime + dependencies,
            "environment": {"BASELINE_ROOT": str(root), "BASELINE_RUNTIME": "/usr/local", "BASELINE_NODE": "/usr/local/bin/node"},
            "scenarios": [stage], "command": [sys.executable, str(here / "stage.py"), stage]}
    if stage not in ["bootstrap", "build"]:
        plan["artifacts"].append(str(root / "dist"))
    if stage == "build":
        plan["environment"]["BASELINE_BUILD_CONFIG_LOADER"] = "runner"
        expected = Path("/expected-linux-artifact.json")
        if expected.is_file():
            plan["environment"]["BASELINE_EXPECTED_ARTIFACT"] = str(expected)
            plan["artifacts"].append(str(expected))
    if stage == "runner-proof":
        plan["command"] = [sys.executable, str(here / "selftest.py")]
        plan["bounds"] = {"startupHandshakeSeconds": 1, "drainSeconds": 1, "cleanupSeconds": 5}
    if stage == "baseline":
        plan["command"] = ["/usr/local/bin/node", "--liftoff-only", "--disable-warning=ExperimentalWarning", "--require", str(here / "observe-workers.cjs"), str(here / "baseline.cjs")]
        plan["environment"]["VALIDATION_PROJECT_ROOT"] = "/tmp/codegraph-validation-projects"
        plan["scenarios"] = scenarios
        plan["executionMode"] = "shared-daemon-and-direct"
    if stage == "measurements":
        plan["command"] = [sys.executable, str(here / "measurements.py"), str(out / "baseline-linux-v1")]
        plan["scenarios"] = ["baseline-measurements"]
        plan["artifacts"].append(str(out / "baseline-linux-v1"))
    path = out / (stage + ".plan.json")
    write(path, plan)
    result = subprocess.run([sys.executable, str(here / "runner.py"), str(path)], timeout=seconds + 120)
    receipt = json.loads((Path(plan["output"]) / "receipt.json").read_text())
    ledger.append({"stage": stage, "outcome": receipt["outcome"], "receipt": str(Path(plan["output"]) / "receipt.json")})
    write(out / "container-results.json", ledger)
    # Native Linux filesystems are required for inotify proof and project state.
    # Export immutable completed attempts only; the Windows mount is storage.
    shutil.copytree(Path(plan["output"]), export / Path(plan["output"]).name)
    shutil.copyfile(path, export / path.name)
    shutil.copyfile(out / "container-results.json", export / "container-results.json")
    if result.returncode or receipt["outcome"] != "pass":
        sys.exit(1)
