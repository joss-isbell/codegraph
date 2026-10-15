"""The reporting consumer. Never converts missing/incomplete evidence to pass."""
import hashlib
import json
from pathlib import Path
import sys


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _consume(path):
    path = Path(path).resolve()
    receipt = json.loads(path.read_text(encoding="utf-8"))
    reasons = list(receipt.get("failures", []))
    if receipt.get("schema") != "codegraph-validation-v1":
        reasons.append("unknown receipt schema")
    if receipt.get("outcome") != "pass":
        reasons.append("producer did not pass")
    if not receipt.get("cleanup", {}).get("complete"):
        reasons.append("owned process cleanup is incomplete")
    if receipt.get("exitCode") != 0:
        reasons.append("child did not exit successfully")
    if not receipt.get("bindingStable"):
        reasons.append("source/artifact binding changed")
    expected = receipt.get("scenarios", [])
    events = receipt.get("events", [])
    if not expected or not isinstance(expected, list) or any(not isinstance(s, str) for s in expected) or len(set(expected)) != len(expected):
        reasons.append("scenario inventory must be a nonempty unique array")
        expected = []
    if not isinstance(events, list) or any(not isinstance(e, dict) for e in events):
        reasons.append("event inventory must be an array")
        events = []
    for scenario in expected:
        kinds = [e.get("type") for e in events if e.get("scenario") == scenario]
        if kinds.count("start") != 1 or kinds.count("complete") != 1:
            reasons.append(f"missing/duplicate scenario completion: {scenario}")
        elif kinds.index("start") > kinds.index("complete"):
            reasons.append(f"scenario completed before start: {scenario}")
        assertions = [e for e in events if e.get("scenario") == scenario and e.get("type") == "assert"]
        if not assertions or any(e.get("passed") is not True for e in assertions):
            reasons.append(f"missing/failed assertion: {scenario}")
    final = [e for e in events if e.get("type") == "final"]
    if len(final) != 1 or final[0].get("passed") is not True or final[0].get("scenarios") != expected:
        reasons.append("missing/mismatched final acceptance assertion")
    if events and events[-1].get("type") != "final":
        reasons.append("final acceptance assertion was not the tail")
    if any(e.get("scenario") not in expected for e in events if e.get("type") != "final"):
        reasons.append("undeclared scenario")
    for name in ["projects", "sessions"]:
        if not isinstance(receipt.get(name), list) or any(not isinstance(e, dict) for e in receipt.get(name, [])):
            reasons.append(f"{name} inventory must be an array of objects (empty is allowed)")
    required = receipt.get("evidence", [])
    if not isinstance(required, list) or not required:
        reasons.append("evidence inventory must be a nonempty array")
        required = []
    mandatory = {"plan.json", "declarations.json", "source-before.json", "source-after.json", "dirty-before.patch",
                 "events.jsonl", "resources.jsonl", "integrity.json", "stdout.log", "stderr.log"}
    paths = [entry.get("path") for entry in required if isinstance(entry, dict)]
    if not mandatory.issubset(paths):
        reasons.append("required evidence inventory is incomplete")
    for entry in required:
        if not isinstance(entry, dict) or not isinstance(entry.get("path"), str) or not isinstance(entry.get("sha256"), str):
            reasons.append("evidence entry is malformed")
            continue
        evidence = path.parent / entry["path"]
        if not evidence.resolve().is_relative_to(path.parent):
            reasons.append("evidence path escapes attempt")
            continue
        if not evidence.is_file() or digest(evidence) != entry["sha256"]:
            reasons.append(f"missing/changed evidence: {entry['path']}")
    if mandatory.issubset(paths) and not any(reason.startswith("missing/changed evidence") for reason in reasons):
        produced_events = [json.loads(line) for line in (path.parent / "events.jsonl").read_text().splitlines()]
        if produced_events != events:
            reasons.append("receipt events differ from actual producer stream")
        plan = json.loads((path.parent / "plan.json").read_text())
        if plan.get("scenarios") != expected:
            reasons.append("receipt scenarios differ from predeclared plan")
        if json.loads((path.parent / "source-before.json").read_text()) != json.loads((path.parent / "source-after.json").read_text()):
            reasons.append("source/artifact manifests differ")
        integrity = json.loads((path.parent / "integrity.json").read_text())
        if not isinstance(integrity, list) or any(w.get("changes") or w.get("errors") for w in integrity):
            reasons.append("source/artifact integrity witness rejected attempt")
        resources = (path.parent / "resources.jsonl").read_text().splitlines()
        if not resources or any(not isinstance(json.loads(line).get("processes"), list) for line in resources):
            reasons.append("resource sample collection is missing or malformed")
        inventory = path.parent / "inventory.json"
        if inventory.exists():
            produced = json.loads(inventory.read_text())
            if any(receipt.get(name) != produced.get(name) for name in ["projects", "sessions"]):
                reasons.append("receipt inventory differs from actual producer collections")
    return {"outcome": "fail" if reasons else "pass", "reasons": list(dict.fromkeys(reasons)),
            "attempt": receipt.get("attempt"), "receipt": str(path),
            "excerpt": receipt.get("failureExcerpt", ""), "logs": receipt.get("logs", {})}


def consume(path):
    try:
        return _consume(path)
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        return {"outcome": "fail", "reasons": [f"malformed/missing receipt evidence: {type(exc).__name__}: {exc}"],
                "receipt": str(Path(path).resolve()), "excerpt": "", "logs": {}}


if __name__ == "__main__":
    result = consume(sys.argv[1])
    print(json.dumps(result, indent=2))
    sys.exit(0 if result["outcome"] == "pass" else 1)
