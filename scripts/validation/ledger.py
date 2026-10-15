"""Require every declared final receipt; retain historical failed outcomes."""
import json
import os
from pathlib import Path
import sys

from events import event, assertion
from report import consume, digest
from runner import write

scenario = "final-ledger"
event("start", scenario=scenario)
manifest = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8-sig"))
out = Path(os.environ["VALIDATION_OUT"])
accepted = []
for row in manifest["accepted"]:
    path = Path(row["path"])
    result = consume(path)
    assertion(scenario, "declared final receipt passed its actual reporting consumer", result["outcome"] == "pass", role=row["role"], summary=result)
    source = json.loads((path.parent / "source-before.json").read_text())
    core = {file: next((value for name, value in source["artifacts"].items() if name.replace("\\", "/").endswith("/scripts/validation/" + file)), None)
            for file in manifest["coreSha256"]}
    assertion(scenario, "final receipt binds qualified supervision/consumer bytes", core == manifest["coreSha256"], role=row["role"], hashes=core)
    accepted.append({**row, "sha256": digest(path), "summary": result})
history = []
state = Path(manifest["stateRoot"])
paths = list(state.glob("*/receipt.json")) + list(state.glob("linux-validation-*/inner/*/receipt.json"))
for path in sorted(paths):
    if path.parent == out:
        continue
    value = json.loads(path.read_text())
    history.append({"path": str(path.relative_to(state)), "sha256": digest(path), "outcome": value["outcome"],
                    "elapsedSeconds": value.get("elapsedSeconds"), "reasons": value.get("failures", [])})
write(out / "final-ledger.json", {"coreSha256": manifest["coreSha256"], "accepted": accepted, "history": history})
assertion(scenario, "complete final ledger and retained attempt history recorded", len(accepted) >= 10 and bool(history), accepted=len(accepted), history=len(history))
event("complete", scenario=scenario)
event("final", passed=True, scenarios=[scenario])
