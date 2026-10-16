"""Actual receipt consumer and exact-B identity join for the recovery slice."""
import json
import os
from pathlib import Path
import sys

from events import event, assertion
from recovery import file_manifest, original_inventory
from report import consume, digest
from runner import write

scenario = "recovery-final-ledger"
event("start", scenario=scenario)
declaration = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8-sig"))
out = Path(os.environ["VALIDATION_OUT"])
root = Path(declaration["sourceRoot"])
accepted = []
for item in declaration["accepted"]:
    receipt = Path(item["path"])
    summary = consume(receipt)
    assertion(scenario, "required complete receipt passes its actual consumer", summary["outcome"] == "pass", role=item["role"], summary=summary)
    binding = json.loads((receipt.parent / "source-before.json").read_text())
    for file in item["currentFiles"]:
        assertion(scenario, "accepted proof binds current relevant recovery adapter bytes",
                  binding["sources"].get(file) == digest(root / file), role=item["role"], file=file)
    for file, expected in declaration["coreSha256"].items():
        assertion(scenario, "accepted proof retains qualified runner and consumer bytes",
                  binding["sources"].get("scripts/validation/" + file) == expected, role=item["role"], file=file)
    accepted.append({**item, "sha256": digest(receipt), "summary": summary})
assertion(scenario, "all five required protection/current focused/rehearsal roles joined",
          [item["role"] for item in accepted] == ["actual-B-protection", "current-WAL-recovery", "current-exact-bundle",
                                               "current-nested-client", "actual-B-rehearsal"])

protected = Path(declaration["protectedB"])
assertion(scenario, "protected B manifest remains at its declared identity",
          digest(protected / "manifest.json") == declaration["protectedManifestSha256"])
manifest = json.loads((protected / "manifest.json").read_text())
assertion(scenario, "all protected B component bytes remain unchanged", file_manifest(protected / "components") == manifest["files"])
original = original_inventory(manifest["sources"], manifest["launcherClosure"])
assertion(scenario, "original installed B and saved settings still equal the exact protection point", original == manifest["files"])

history = []
paths = sorted(receipt for root_path in declaration["stateRoots"] for receipt in Path(root_path).glob("*/receipt.json"))
for receipt in paths:
    if receipt.parent == out:
        continue
    value = json.loads(receipt.read_text())
    history.append({"path": str(receipt), "sha256": digest(receipt), "outcome": value["outcome"],
                    "failures": value.get("failures", []), "cleanup": value.get("cleanup"),
                    "exitCode": value.get("exitCode"), "scenarios": value.get("scenarios")})
write(out / "recovery-final-ledger.json", {"accepted": accepted, "history": history,
      "protectedManifestSha256": declaration["protectedManifestSha256"], "protectedFiles": len(manifest["files"]),
      "newConclusions": ["exact installed B preserved and independently restored", "consistent compatible DB snapshot restored",
                         "later legitimate source/index work preserved and explicitly caught up", "owned cleanup completed"],
      "limits": ["Windows actual B only", "no candidate-updated DB compatibility claim", "no installation activation",
                 "older live inherited environment remains unknown"]})
assertion(scenario, "failed red attempts and complete history remain retained",
          any(item["outcome"] == "fail" for item in history) and len(history) >= 7)
event("complete", scenario=scenario)
event("final", passed=True, scenarios=[scenario])
