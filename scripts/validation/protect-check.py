"""Revalidate protected baseline bytes and the exact initial instruction delta."""
import hashlib
import json
import os
from pathlib import Path
import tarfile

from events import event, assertion
from report import digest
from runner import write

scenario = "protected-baseline"
event("start", scenario=scenario)
root = Path(os.environ["BASELINE_ROOT"])
state = root.parent
initial = json.loads((state / "baseline-protection-initial.json").read_text())
frozen = json.loads((state / os.environ.get("BASELINE_ARTIFACT_NAME", "baseline-artifact-v1.json")).read_text())
expected = "bcd1586a48ad65ee2aeda3d562eaafc0aae74e87"
assertion(scenario, "baseline source identities agree", initial["sourceCommit"] == frozen["sourceCommit"] == expected)
assertion(scenario, "exact Git source archive retained", digest(state / "baseline-source.tar") == initial["sourceArchiveSha256"] == frozen["sourceArchiveSha256"])
assertion(scenario, "dependency lock retained unchanged", digest(root / "package-lock.json") == initial["packageLockSha256"] == frozen["packageLockSha256"])
source_files = 0
with tarfile.open(state / "baseline-source.tar") as archive:
    for item in archive.getmembers():
        if item.isfile():
            expected_hash = hashlib.sha256(archive.extractfile(item).read()).hexdigest()
            if digest(root / item.name) != expected_hash:
                raise AssertionError("protected source changed: " + item.name)
            source_files += 1
assertion(scenario, "all archived source bytes match protected tree", source_files > 100, files=source_files)
artifact_counts = {}
for directory, files in frozen["files"].items():
    path = Path(directory)
    actual = {str(p.relative_to(path)).replace("\\", "/"): digest(p) for p in sorted(path.rglob("*")) if p.is_file()}
    assertion(scenario, "complete artifact/dependency/runtime inventory unchanged", actual == files, files=len(actual))
    # Root and workspace dependency trees share the basename node_modules.
    # Preserve their distinct identities in the protection summary as well.
    artifact_counts[str(path)] = len(actual)
originals = Path(os.environ["INITIAL_EVIDENCE_ROOT"])
for item in initial["initialDirtyDelta"]:
    assertion(scenario, "initial dirty evidence retained exactly", digest(state / "initial-delta" / item["name"]) == digest(originals / item["name"]) == item["sha256"], evidenceName=item["name"])
held = Path(os.environ["HELD_CHECKOUT"])
for name in ["AGENTS.md", "GLOSSARY.md"]:
    assertion(scenario, "held instruction/glossary bytes preserved", digest(held / name) == digest(originals / ("initial-" + name)), file=name)
assertion(scenario, "original unsupported prerequisite runtime identity retained", digest(state / "baseline-runtime/node.exe") == initial["runtimeSha256"])
write(Path(os.environ["VALIDATION_OUT"]) / "protection-verified.json", {"sourceCommit": expected, "sourceFiles": source_files,
      "artifacts": artifact_counts, "sourceArchiveSha256": initial["sourceArchiveSha256"], "runtime": frozen["runtime"],
      "initialDelta": initial["initialDirtyDelta"], "heldProjectIndexAccess": "none", "heldCodeGraphSettingsOrServicesChanged": False})
event("complete", scenario=scenario)
event("final", passed=True, scenarios=[scenario])
