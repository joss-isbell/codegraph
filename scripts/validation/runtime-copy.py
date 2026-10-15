"""Retain a supported existing runtime beside the failed prerequisite identity."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from events import event, assertion
from runner import write
from report import digest

scenario = "supported-runtime"
event("start", scenario=scenario)
root = Path(__file__).resolve().parent.parent.parent
destination = root / ".validation" / "baseline-runtime-v2"
destination.mkdir(exist_ok=False)
source = Path(sys.argv[1]).resolve()
shutil.copy2(source, destination / "node.exe")
shutil.copytree(root / ".validation" / "baseline-runtime" / "npm", destination / "node_modules" / "npm")
for name in ["npm.cmd", "npm.ps1", "npx.cmd", "npx.ps1"]:
    shutil.copy2(destination / "node_modules" / "npm" / "bin" / name, destination / name)
version = subprocess.check_output([str(destination / "node.exe"), "-p", "JSON.stringify(process.versions)"]).decode()
versions = json.loads(version)
assertion(scenario, "supported runtime copied byte for byte", digest(source) == digest(destination / "node.exe"))
major, minor, *_ = [int(p) for p in versions["node"].split(".")]
assertion(scenario, "runtime supports node:sqlite and project engine range", 22 <= major < 25 and (major > 22 or minor >= 5))
write(Path(os.environ["VALIDATION_OUT"]) / "runtime.json", {"versions": versions, "sha256": digest(destination / "node.exe"),
                                                          "retainedRuntime": str(destination)})
event("complete", scenario=scenario)
event("final", passed=True, scenarios=[scenario])
