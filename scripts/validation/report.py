"""The reporting consumer. Never converts missing/incomplete evidence to pass."""
import hashlib
import json
import os
from pathlib import Path
import sys

WINDOWS_INTEGRITY_BYTES = 64 * 1024 * 1024


def digest(path, limit=None):
    result, total = hashlib.sha256(), 0
    with Path(path).open("rb") as stream:
        while data := stream.read(1024 * 1024 if limit is None else min(1024 * 1024, limit + 1 - total)):
            total += len(data)
            if limit is not None and total > limit:
                raise ValueError("integrity.json exceeds64MiB before hash/decode")
            result.update(data)
    return result.hexdigest()


def current_runner_bound(before):
    """Compare literal artifact routes with the host's Path identity rules.

    Windows Path equality folds case/separators, but never follows a reparse
    point or erases '..'. Equivalent keys must carry one unambiguous digest.
    Truly historical bindings need not name today's installed runner bytes.
    """
    artifacts = before.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ValueError("integrity classification rejected before evidence I/O: malformed artifact bindings")
    identities = {}
    current = Path(__file__).absolute().with_name("runner.py")
    for spelling, value in artifacts.items():
        if not isinstance(spelling, str):
            raise ValueError("integrity classification rejected before evidence I/O: malformed artifact path")
        identity = Path(spelling)
        if identity in identities and identities[identity] != value:
            raise ValueError("integrity classification rejected before evidence I/O: conflicting equivalent artifact bindings")
        identities[identity] = value
        if os.name == "nt" and identity == current:
            # Inspect every original route component before any byte read;
            # Path equality alone does not authorize a declaration route.
            from windows_witness import ordinary
            ordinary(identity, False)
    if os.name == "nt":
        from windows_witness import ordinary
        ordinary(current, False)
    return identities.get(current) == digest(current)


def integrity_profile(receipt, plan, before):
    """Classify/refuse before integrity evidence hashing or JSON decoding.

    Metadata is not acceptance evidence until the ordinary inventory checks.
    Conservative new-source/marker detection cannot promote historical proof.
    """
    schema, runtime = receipt.get("schema"), receipt.get("runtime")
    if schema not in {"codegraph-validation-v1", "codegraph-validation-v2"}:
        raise ValueError("integrity classification rejected before evidence I/O: unknown receipt schema")
    bound_current = current_runner_bound(before)
    markers = (bound_current or "integrityProfile" in receipt or "integrityPolicy" in receipt
               or "integrityProfile" in plan or (isinstance(runtime, dict) and "system" in runtime))
    if schema == "codegraph-validation-v1":
        if markers:
            raise ValueError("integrity classification rejected before evidence I/O: versioned runner cannot downgrade")
        return False
    profile = receipt.get("integrityProfile")
    expected = {"Windows": "windows-guard-v2", "Linux": "linux-inotify-v1"}.get(
        runtime.get("system") if isinstance(runtime, dict) else None)
    if profile not in {"windows-guard-v2", "linux-inotify-v1"} or profile != expected or profile != plan.get("integrityProfile"):
        raise ValueError("integrity classification rejected before evidence I/O: missing/mismatched schema/profile/runtime/plan")
    policy = ({"childDontWriteBytecode": "1", "artifactPycacheIncluded": True}
              if profile == "windows-guard-v2" else {"artifactPycacheIncluded": False})
    if receipt.get("integrityPolicy") != policy:
        raise ValueError("integrity classification rejected before evidence I/O: missing/mismatched policy")
    source_bound_cache(receipt, plan, before, profile)
    return profile == "windows-guard-v2"


def source_bound_cache(receipt, plan, before, profile):
    """Optional stricter normal caller; legacy no-prefix plans keep their meaning."""
    declaration, actual = plan.get("sourceBoundCache"), receipt.get("sourceBoundCache")
    if declaration is None and actual is None:
        return None
    if (profile != "windows-guard-v2" or not isinstance(declaration, dict)
            or set(declaration) != {"prefix"} or not isinstance(actual, dict)
            or set(actual) != {"prefix", "helpers", "interpreterPrefix", "dontWriteBytecode"}):
        raise ValueError("source-bound cache declaration/evidence is missing or malformed")
    prefix = declaration["prefix"]
    if (not isinstance(prefix, str) or actual["prefix"] != prefix or actual["interpreterPrefix"] != prefix
            or actual["dontWriteBytecode"] is not True or not sys.dont_write_bytecode
            or sys.pycache_prefix != prefix or os.environ.get("PYTHONPYCACHEPREFIX") != prefix
            or os.environ.get("PYTHONDONTWRITEBYTECODE") != "1"):
        raise ValueError("source-bound reporting interpreter/cache route differs")
    from windows_witness import absolute, ordinary
    ordinary(absolute(prefix).parent, True)
    environment = plan.get("environment", {})
    for name, value in (("PYTHONPYCACHEPREFIX", prefix), ("PYTHONDONTWRITEBYTECODE", "1")):
        if environment.get(name) != value or any(key.upper() == name and key != name for key in environment):
            raise ValueError("source-bound saved child cache environment differs")
    here = Path(__file__).absolute().parent
    expected = {here / name for name in
                ("normal.py", "runner.py", "report.py", "integrity.py", "windows_witness.py")}
    helpers = actual["helpers"]
    if (not isinstance(helpers, dict) or len(helpers) != len(expected)
            or any(not isinstance(p, str) or not isinstance(value, str) for p, value in helpers.items())
            or {Path(p) for p in helpers} != expected):
        raise ValueError("source-bound raw helper inventory differs")
    # The active bootstrap captured normal.py too. Join its complete current
    # raw-source inventory, including that entrypoint, on immediate consumption
    # and replay; checking only imported module markers would miss its change.
    bootstrap = getattr(sys.modules.get("runner"), "SOURCE_BOUND_BOOTSTRAP", None)
    active = bootstrap.get("helpers") if isinstance(bootstrap, dict) else None
    if (not isinstance(active, dict) or len(active) != len(expected)
            or any(not isinstance(p, str) or not isinstance(value, str) for p, value in active.items())
            or {Path(p): value for p, value in active.items()} != {Path(p): value for p, value in helpers.items()}):
        raise ValueError("active normal bootstrap/raw helper sources differ from attempt")
    bound = {Path(p): value for p, value in before["artifacts"].items()}
    for spelling, value in helpers.items():
        source = Path(spelling)
        if bound.get(source) != value:
            raise ValueError("executed raw helpers differ from immutable source binding")
        if source.name != "normal.py":
            if getattr(sys.modules.get(source.stem), "__validation_source_sha256__", None) != value:
                raise ValueError("report consumer did not execute the bound raw helpers")
    return prefix


def windows_closed(value, selection, absent_roots=(), absent_final=None):
    """Consume the shared v2 namespace/lifecycle verifier, not an advisory prefix."""
    if (not isinstance(value, dict) or value.get("errors") != []
            or value.get("qualificationOnly") is not False
            or value.get("qualificationMode", "missing") is not None):
        return False
    try:
        from windows_witness import validate_closed
        validate_closed(value, require_clean=True, allow_qualification=False)
        keys = {"watchRoots", "namespaceWatchRoots", "namespacePaths"}
        if absent_roots:
            keys.add("absentRoots")
        if not isinstance(selection, dict) or set(selection) != keys:
            return False
        for selected, closed, field in [("watchRoots", "watches", "root"),
                                         ("namespaceWatchRoots", "namespaceWatches", "root"),
                                         ("namespacePaths", "namespaceHandles", "path")]:
            roots = selection[selected]
            if (not isinstance(roots, list) or any(not isinstance(p, str) for p in roots)
                    or roots != [row[field] for row in value[closed]]):
                return False
        if absent_roots:
            expected = {Path(p) for p in absent_roots}
            roots = selection["absentRoots"]
            if (not isinstance(roots, list) or any(not isinstance(p, str) for p in roots)
                    or len(roots) != len(expected) or {Path(p) for p in roots} != expected
                    or not isinstance(absent_final, dict) or len(absent_final) != len(expected)
                    or {Path(p) for p in absent_final} != expected
                    or any(value is not True for value in absent_final.values())):
                return False
            retained = {Path(p) for p in selection["namespacePaths"]}
            watches = [Path(p) for p in selection["watchRoots"]]
            if any(p.parent not in retained or not any(p.is_relative_to(root) for root in watches)
                   for p in expected):
                return False
    except (ImportError, ValueError, TypeError, KeyError, AttributeError):
        return False
    return True



def _consume(path):
    path = Path(path).resolve()
    receipt = json.loads(path.read_text(encoding="utf-8"))
    # These metadata inputs select only a stricter reader/refusal. They still
    # need their exact evidence hashes below before any acceptance is possible.
    plan = json.loads((path.parent / "plan.json").read_text())
    before = json.loads((path.parent / "source-before.json").read_text())
    windows_profile = integrity_profile(receipt, plan, before)
    # Any new receipt has the finite reader, including a consistently claimed
    # Linux profile. Platform metadata cannot select an unbounded new reader.
    bounded_integrity = receipt["schema"] == "codegraph-validation-v2"
    reasons = list(receipt.get("failures", []))
    if receipt.get("schema") not in {"codegraph-validation-v1", "codegraph-validation-v2"}:
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
        is_integrity = evidence.resolve() == (path.parent / "integrity.json").resolve()
        if (is_integrity and bounded_integrity and evidence.is_file()
                and evidence.stat().st_size > WINDOWS_INTEGRITY_BYTES):
            reasons.append("missing/changed evidence: integrity.json exceeds64MiB")
            continue
        if not evidence.is_file() or digest(evidence, WINDOWS_INTEGRITY_BYTES
                                            if bounded_integrity and is_integrity else None) != entry["sha256"]:
            reasons.append(f"missing/changed evidence: {entry['path']}")
    if mandatory.issubset(paths) and not any(reason.startswith("missing/changed evidence") for reason in reasons):
        produced_events = [json.loads(line) for line in (path.parent / "events.jsonl").read_text().splitlines()]
        if produced_events != events:
            reasons.append("receipt events differ from actual producer stream")
        if plan.get("scenarios") != expected:
            reasons.append("receipt scenarios differ from predeclared plan")
        if json.loads((path.parent / "source-before.json").read_text()) != json.loads((path.parent / "source-after.json").read_text()):
            reasons.append("source/artifact manifests differ")
        if bounded_integrity:
            with (path.parent / "integrity.json").open("rb") as stream:
                raw_integrity = stream.read(WINDOWS_INTEGRITY_BYTES + 1)
            if len(raw_integrity) > WINDOWS_INTEGRITY_BYTES:
                raise ValueError("integrity.json exceeds64MiB before decode")
            integrity = json.loads(raw_integrity)
        else:
            integrity = json.loads((path.parent / "integrity.json").read_text())
        if not isinstance(integrity, list) or any(w.get("changes") or w.get("errors") for w in integrity):
            reasons.append("source/artifact integrity witness rejected attempt")
        if receipt.get("schema") == "codegraph-validation-v2":
            if windows_profile:
                if not isinstance(integrity, list) or len(integrity) != 1:
                    reasons.append("one aggregate Windows integrity ledger required")
                elif not windows_closed(integrity[0].get("windowsWitness"), integrity[0].get("windowsSelection"),
                                        [plan["sourceBoundCache"]["prefix"]] if "sourceBoundCache" in plan else [],
                                        integrity[0].get("absentFinal")):
                    reasons.append("Windows integrity lifecycle is missing or incomplete")
            elif not integrity:
                reasons.append("Linux legacy integrity observations/policy are missing")
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
