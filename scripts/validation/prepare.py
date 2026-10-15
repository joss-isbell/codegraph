"""Protect the exact baseline source/runtime without opening any held index."""
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from events import event, assertion
from runner import write
from report import digest

root = HERE.parent.parent
scenario = "protect-baseline"
event("start", scenario=scenario)
baseline = root / ".validation" / "baseline"
baseline.mkdir(parents=True, exist_ok=False)
commit = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"]).decode().strip()
archive = subprocess.check_output(["git", "-C", str(root), "archive", commit])
(baseline.parent / "baseline-source.tar").write_bytes(archive)
with tarfile.open(fileobj=io.BytesIO(archive)) as source:
    source.extractall(baseline, filter="data")
(baseline / "baseline-source-commit.txt").write_text(commit + "\n")
runtime = baseline.parent / "baseline-runtime"
runtime.mkdir()
node = Path(shutil.which("node")).resolve()
shutil.copy2(node, runtime / node.name)
npm_root = node.parent / "node_modules" / "npm"
shutil.copytree(npm_root, runtime / "npm")
initial_root = root.parent
private = baseline.parent / "initial-delta"
private.mkdir()
for path in initial_root.glob("initial-*"):
    if path.is_file():
        shutil.copy2(path, private / path.name)
metadata = {"sourceCommit": commit, "sourceArchiveSha256": digest(baseline.parent / "baseline-source.tar"),
            "runtimeSha256": digest(runtime / node.name), "packageLockSha256": digest(baseline / "package-lock.json"),
            "baseline": str(baseline), "runtime": str(runtime), "initialDirtyDelta":
            [{"name": p.name, "sha256": digest(p), "bytes": p.stat().st_size} for p in sorted(private.iterdir())],
            "heldInstallationAccess": "none", "heldIndexAccess": "none", "serviceChanges": "none"}
write(baseline.parent / "baseline-protection-initial.json", metadata)
write(Path(os.environ["VALIDATION_OUT"]) / "protection.json", metadata)
assertion(scenario, "exact requested baseline source", commit == "bcd1586a48ad65ee2aeda3d562eaafc0aae74e87")
assertion(scenario, "complete initial delta evidence copied", len(metadata["initialDirtyDelta"]) >= 3)
assertion(scenario, "runtime copy matches executable", digest(node) == digest(runtime / node.name))
event("complete", scenario=scenario)
event("final", passed=True, scenarios=[scenario])
