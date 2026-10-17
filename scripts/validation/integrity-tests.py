"""Focused Windows input-integrity checks through normal callers.

Run under runner.py with a fresh VALIDATION_OUT and scenario windows-inputs.
This source is not its own resource grant or native qualification receipt.
"""
import json
import ctypes
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import struct
import time

sys.dont_write_bytecode = True
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from events import event, assertion
from integrity import Witness
from report import consume, digest, WINDOWS_INTEGRITY_BYTES
from runner import Log, run, write

SCENARIO = "windows-inputs"


def check(name, passed, **details):
    assertion(SCENARIO, name, passed, **details)


def fixture(root):
    root.mkdir()
    (root / "existing-empty").mkdir()
    (root / "baseline-source-commit.txt").write_text("windows-inputs-fixture-v1")
    (root / "app.ts").write_text("export const original = 1;\n")
    return root


def junction(path, target):
    """Create an actual fixture-local mount-point link, without a shell/skip."""
    path.mkdir()
    substitute, printable = ("\\??\\" + str(target)).encode("utf-16-le"), str(target).encode("utf-16-le")
    names = substitute + b"\0\0" + printable + b"\0\0"
    payload = struct.pack("<HHHH", 0, len(substitute), len(substitute) + 2, len(printable)) + names
    raw = struct.pack("<IHH", 0xA0000003, len(payload), 0) + payload
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32,
                                  ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p]
    kernel.CreateFileW.restype = ctypes.c_void_p
    kernel.DeviceIoControl.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_void_p,
                                      ctypes.c_uint32, ctypes.c_void_p, ctypes.c_uint32,
                                      ctypes.c_void_p, ctypes.c_void_p]
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    handle = kernel.CreateFileW(str(path), 0x40000000, 7, None, 3, 0x02200000, None)
    if handle in (None, ctypes.c_void_p(-1).value):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        count, data = ctypes.c_uint32(), ctypes.create_string_buffer(raw)
        if not kernel.DeviceIoControl(handle, 0x900A4, data, len(raw), None, 0, ctypes.byref(count), None):
            raise ctypes.WinError(ctypes.get_last_error())
    finally:
        if not kernel.CloseHandle(handle):
            raise ctypes.WinError(ctypes.get_last_error())
    return raw.hex()


def run_case(proof, name, mode, expected, *, source_mutation=False, linked=None, aliased=False):
    source = fixture(proof / (name + "-source"))
    artifacts = proof / (name + "-artifacts")
    (artifacts / "__pycache__").mkdir(parents=True)
    cached = artifacts / "__pycache__" / "pinned.pyc"
    cached.write_bytes(b"ordinary immutable cached artifact")
    mutable = source / "app.ts" if source_mutation else cached
    declared = [str(artifacts)]
    target = link = raw_link = None
    if linked:
        target = proof / (name + "-ordinary-target")
        target.mkdir()
        mutable = target / "actual-input.bin"
        mutable.write_bytes(b"ordinary separately declared target")
        link = (source if linked == "source-undeclared" else artifacts) / "dependency"
        raw_target = target
        if linked == "chain":
            intermediate = proof / (name + "-intermediate")
            intermediate.mkdir()
            raw_target = intermediate / "link2"
            junction(raw_target, target)
        raw_link = junction(link, raw_target)
        if linked in {"declared", "chain"}:
            declared.append(str(target))
    out = proof / name
    plan = {"sourceRoot": str(source), "cwd": str(HERE.parent.parent), "output": str(out),
            "command": [sys.executable, str(HERE / "selftest.py"), "produce", mode],
            "deadlineSeconds": 8, "scenarios": ["probe"], "artifacts": declared,
            "executionMode": "windows-inputs-focused",
            "environment": {"PROBE_SOURCE": str(source), "MUTABLE_ARTIFACT": str(mutable)},
            "bounds": {"logBytesPerStream": 65536, "excerptBytes": 8192,
                       "drainSeconds": 0.1, "startupHandshakeSeconds": 0.1,
                       "cleanupSeconds": 3, "sampleSeconds": 0.02}}
    # Redeclare the actual configured outer prefix across the runner's normal
    # inherited-PYTHON* scrub; do not invent a prefix for standalone callers.
    cache_prefix = os.environ.get("PYTHONPYCACHEPREFIX")
    if cache_prefix is not None and cache_prefix == sys.pycache_prefix:
        plan["environment"]["PYTHONPYCACHEPREFIX"] = cache_prefix
    plan_path = proof / (name + ".plan.json")
    write(plan_path, plan)
    if linked in {"undeclared", "source-undeclared", "chain"}:
        failure = None
        try:
            run(plan_path)  # Actual caller must veto before releasing its target.
        except ValueError as error:
            failure = str(error)
        expected_reason = "Reparse route refused" if linked == "chain" else "Undeclared reparse target"
        check(name + ": actual caller rejects unsupported closure before target release",
              failure is not None and expected_reason in failure
              and not (out / "events.jsonl").exists() and not (out / "receipt.json").exists(),
              failure=failure, partialAttempt=str(out), targetResultClaim=False)
        write(proof / (name + ".preflight.json"), {"failure": failure, "targetReleased": False,
                                                  "partialAttempt": str(out)})
        return out
    if aliased:
        # The real clean run enters the actual normal CLI through an ordinary
        # case/separator alias; this is not a fabricated artifact-map spelling.
        caller = HERE.with_name(HERE.name.swapcase()) / "RuNnEr.Py"
        native_code = subprocess.run([sys.executable, "-B", str(caller), str(plan_path)],
                                     cwd=HERE.parent.parent, timeout=20).returncode
    else:
        native_code = run(plan_path)  # Exact public caller, not a reimplementation.
    receipt = json.loads((out / "receipt.json").read_text())
    summary = consume(out / "receipt.json")  # Exact reporting consumer.
    check(name + ": actual normal runner/report outcome", summary["outcome"] == expected
          and native_code == (0 if expected == "pass" else 1),
          receipt=str(out / "receipt.json"), summary=summary)
    check(name + ": native target and owned cleanup finished",
          receipt["exitCode"] == 0 and receipt["cleanup"]["complete"] is True)
    rows = json.loads((out / "integrity.json").read_text())
    if aliased:
        before = json.loads((out / "source-before.json").read_text())
        actual_keys = [key for key, value in before["artifacts"].items()
                       if Path(key) == HERE / "runner.py" and value == digest(HERE / "runner.py")]
        check(name + ": actual current-runner alias preserved in producer binding",
              len(actual_keys) == 1 and actual_keys[0] != str((HERE / "runner.py").resolve()),
              actualCaller=str(caller), artifactKeys=actual_keys)
    if linked:
        before = json.loads((out / "source-before.json").read_text())
        record = before["reparse"][str(link)]
        check(name + ": actual nofollow metadata and independently guarded target",
              record["dataHex"] == raw_link and record["target"] == str(target)
              and record["coveredBy"] == str(target)
              and before["membership"]["artifact"][str(link)] == "reparse"
              and str(target) in rows[0]["windowsSelection"]["namespacePaths"]
              and str(mutable) in before["artifacts"])
    check(name + ": exactly one aggregate closed observation",
          len(rows) == 1 and rows[0]["windowsWitness"]["observationComplete"] is True
          and rows[0]["windowsWitness"]["nativeHandlesClosed"] is True)
    if expected == "fail":
        before = json.loads((out / "source-before.json").read_text())
        after = json.loads((out / "source-after.json").read_text())
        check(name + ": restored endpoint bytes cannot hide transient mutation", before == after
              and receipt["bindingStable"] is False
              and any(row["path"] == str(mutable) for row in rows[0]["windowsWitness"]["events"]))
    else:
        check(name + ": checked closed lifecycle accepted", rows[0]["windowsWitness"]["complete"] is True)
    return out


def rejection(proof, accepted, name, mutate):
    # Copies are explicit consumer rejection probes, never fresh target results.
    out = proof / ("rejection-" + name)
    shutil.copytree(accepted, out)
    receipt = json.loads((out / "receipt.json").read_text())
    plan = json.loads((out / "plan.json").read_text())
    rows = json.loads((out / "integrity.json").read_text())
    mutate(receipt, plan, rows)
    write(out / "plan.json", plan)
    write(out / "integrity.json", rows)
    for entry in receipt["evidence"]:
        if entry["path"] in {"plan.json", "integrity.json"}:
            entry["sha256"] = digest(out / entry["path"])
    write(out / "receipt.json", receipt)
    summary = consume(out / "receipt.json")
    check(name + ": actual report rejects altered lifecycle/version evidence",
          summary["outcome"] == "fail", summary=summary)
    if name == "case-alias-all-markers-stripped-lifecycle":
        check("actual aliased current-source binding prevents compound legacy downgrade",
              any("versioned runner cannot downgrade" in reason for reason in summary["reasons"]), summary=summary)
    return {"name": name, "summary": summary}


def strip_all_markers(receipt, plan, rows):
    receipt["schema"] = "codegraph-validation-v1"
    receipt.pop("integrityProfile", None)
    receipt.pop("integrityPolicy", None)
    receipt["runtime"].pop("system", None)
    plan.pop("integrityProfile", None)
    for row in rows:
        row.pop("windowsWitness", None)
        row.pop("windowsSelection", None)


def conflicting_binding_rejection(proof, accepted):
    out = proof / "rejection-conflicting-equivalent-artifact"
    shutil.copytree(accepted, out)
    receipt = json.loads((out / "receipt.json").read_text())
    for leaf in ("source-before.json", "source-after.json"):
        binding = json.loads((out / leaf).read_text())
        original = next(key for key in binding["artifacts"] if Path(key) == HERE / "runner.py")
        alias = original.swapcase().replace("\\", "/")
        check("conflicting fixture has distinct equivalent Windows spelling",
              original != alias and Path(original) == Path(alias))
        binding["artifacts"][alias] = "0" * 64
        write(out / leaf, binding)
        for entry in receipt["evidence"]:
            if entry["path"] == leaf:
                entry["sha256"] = digest(out / leaf)
    write(out / "receipt.json", receipt)
    summary = consume(out / "receipt.json")
    check("actual consumer refuses conflicting equivalent artifact bindings",
          summary["outcome"] == "fail"
          and any("conflicting equivalent artifact bindings" in reason for reason in summary["reasons"]),
          summary=summary)
    return {"name": "conflicting-equivalent-artifact", "summary": summary}


def oversized_rejections(proof, accepted, mutations):
    # One immutable oversized file, shared by separate receipt metadata probes.
    # Metadata probes have a common v2 plan; the compound downgrade has its
    # own exact stripped-plan phase. Save both raw plans and restore the active
    # route in finally, so replay is explicit rather than simultaneous fiction.
    out = proof / "rejection-shared-oversized-integrity"
    shutil.copytree(accepted, out)
    original_ledger = {"path": str(accepted / "integrity.json"),
                       "sha256": digest(accepted / "integrity.json")}
    stripped_rows = json.loads((out / "integrity.json").read_text())
    for row in stripped_rows:
        row.pop("windowsWitness", None)
        row.pop("windowsSelection", None)
    write(out / "integrity.json", stripped_rows)
    stripped_payload = {"bytes": (out / "integrity.json").stat().st_size,
                        "sha256": digest(out / "integrity.json")}
    with (out / "integrity.json").open("r+b") as stream:
        stream.truncate(WINDOWS_INTEGRITY_BYTES + 1)
    os.chmod(out / "integrity.json", stat.S_IREAD)
    check("one actual oversized derivative has lifecycle removed before readonly padding",
          len(stripped_rows) > 0 and all("windowsWitness" not in row and "windowsSelection" not in row
                                         for row in stripped_rows)
          and (out / "integrity.json").stat().st_size == WINDOWS_INTEGRITY_BYTES + 1
          and bool((out / "integrity.json").stat().st_file_attributes & 1))
    write(out / "oversize-fixture-derivation.json", {"sourceLedger": original_ledger,
          "strippedPayloadBeforePadding": stripped_payload, "lifecycleRowsRemoved": len(stripped_rows),
          "actualFixture": str(out / "integrity.json"), "fixtureBytes": WINDOWS_INTEGRITY_BYTES + 1,
          "readonlyAfterPreparation": True, "separateOversizeCopy": False})
    fixture_derivation = {"path": str(out / "oversize-fixture-derivation.json"),
                          "sha256": digest(out / "oversize-fixture-derivation.json")}
    original = json.loads((out / "receipt.json").read_text())
    plan = json.loads((out / "plan.json").read_text())
    original_plan = (out / "plan.json").read_bytes()
    (out / "v2.plan.raw").write_bytes(original_plan)
    results = []
    for name, mutate in [("intact-profile", lambda _r, _p, _w: None), *mutations]:
        receipt = json.loads(json.dumps(original))
        mutate(receipt, dict(plan), [])  # These mutations change receipt only.
        receipt_path = out / (name + ".receipt.json")
        write(receipt_path, receipt)
        summary = consume(receipt_path)
        expected = ("integrity.json exceeds64MiB" if name == "intact-profile"
                    else "integrity classification rejected before evidence I/O")
        check(name + ": oversized ledger refused before unbounded hash/decode",
              summary["outcome"] == "fail" and any(expected in r for r in summary["reasons"]),
              summary=summary, sharedFixture=str(out / "integrity.json"))
        results.append({"name": name, "receipt": str(receipt_path), "summary": summary,
                        "sharedFixture": str(out / "integrity.json"), "derivedOnly": True,
                        "sharedFixtureDerivation": fixture_derivation,
                        "planPhase": {"path": str(out / "v2.plan.raw"), "sha256": digest(out / "v2.plan.raw")}})
    receipt = json.loads(json.dumps(original))
    stripped_plan = json.loads(original_plan)
    strip_all_markers(receipt, stripped_plan, [])
    write(out / "stripped.plan.raw", stripped_plan)
    receipt_path = out / "all-markers-stripped.receipt.json"
    phase = {"path": str(out / "stripped.plan.raw"), "sha256": digest(out / "stripped.plan.raw")}
    for entry in receipt["evidence"]:
        if entry["path"] == "plan.json":
            entry["sha256"] = phase["sha256"]
    write(receipt_path, receipt)
    restoration = {"attempted": False, "complete": False}
    try:
        (out / "plan.json").write_bytes((out / "stripped.plan.raw").read_bytes())
        summary = consume(receipt_path)
        write(out / "all-markers-stripped.summary.json", summary)
        check("case-aliased/all-markers-stripped oversized derivative refuses before evidence I/O",
              summary["outcome"] == "fail" and any("versioned runner cannot downgrade" in reason
                                                    for reason in summary["reasons"]), summary=summary)
        results.append({"name": "all-markers-stripped", "receipt": str(receipt_path), "summary": summary,
                        "sharedFixture": str(out / "integrity.json"), "derivedOnly": True,
                        "sharedFixtureDerivation": fixture_derivation,
                        "planPhase": phase, "simultaneouslyReconsumable": False})
    finally:
        restoration["attempted"] = True
        try:
            (out / "plan.json").write_bytes(original_plan)
            restoration["complete"] = (out / "plan.json").read_bytes() == original_plan
        finally:
            write(out / "plan-phase-restoration.json", restoration)
        check("oversize compound phase restored exact original active plan", restoration["complete"])
    write(out / "replay-contexts.json", {"restoreNamedRawPlanTo": str(out / "plan.json"),
          "contexts": [{"receipt": row["receipt"], "planPhase": row["planPhase"]} for row in results],
          "simultaneousPlanClaim": False, "restoration": restoration})
    return results


def compatibility(proof, mutate):
    root = proof / ("recovery-compatible-" + ("mutation" if mutate else "clean"))
    root.mkdir()
    target = root / "component.bin"
    target.write_bytes(b"retained copied component")
    witness = Witness(root, [], artifact=True)
    try:
        witness.check()
        original, metadata = target.read_bytes(), target.stat()
        if mutate:
            target.write_bytes(b"changed")
            target.write_bytes(original)
            os.utime(target, ns=(metadata.st_atime_ns, metadata.st_mtime_ns))
        final = target.read_bytes()  # Last protected read precedes close.
    finally:
        closed = witness.close()
    check("recovery-compatible single Witness closes actual observation",
          closed["observationComplete"] is True and closed["nativeHandlesClosed"] is True
          and final == original)
    check("recovery-compatible single Witness retains immutable veto",
          bool(witness.changes or witness.errors) is mutate and closed["complete"] is (not mutate))
    return closed


def log_close_rejections(proof):
    """Call the actual drainer synchronously; no extra process or reader."""
    root = proof / "log-close-failure-probes"
    root.mkdir()

    class InjectedCloseFailure(BaseException):
        pass

    class FaultPipe:
        def __init__(self, path, fail_read):
            self.raw = path.open("rb")
            self.fail_read = fail_read
            self.close_calls = 0

        def read(self, count):
            if self.fail_read:
                raise OSError("injected prior read failure")
            return self.raw.read(count)

        def close(self):
            self.close_calls += 1
            self.raw.close()
            raise InjectedCloseFailure("injected close-after-closed failure")

    results = []
    marker = b"actual normal Log close fixture\n"
    for mode, fail_read in (("close-only", False), ("read-and-close", True)):
        source = root / (mode + ".pipe-source")
        source.write_bytes(marker)
        pipe = FaultPipe(source, fail_read)
        # The production drain body is called directly: no constructor/thread,
        # replacement reader, subprocess, monkeypatch or second close attempt.
        log = Log.__new__(Log)
        log.total, log.overflow, log.tail = 0, False, b""
        log.path, log.errors = root / (mode + ".raw"), []
        escaped = None
        try:
            log.drain(pipe, 4096, 4096)
        except BaseException as error:
            escaped = type(error).__name__ + ": " + str(error)
        expected = (["log capture failed: OSError: injected prior read failure"] if fail_read else [])
        expected.append("log pipe close failed: InjectedCloseFailure: injected close-after-closed failure")
        expected_raw = b"" if fail_read else marker
        facts = {"mode": mode, "directActualDrain": True, "constructorCalled": False,
                 "additionalProcessOrReaderThread": False, "underlyingPipeClosed": pipe.raw.closed,
                 "closeCalls": pipe.close_calls, "escaped": escaped, "errors": list(log.errors),
                 "expectedErrors": expected, "raw": str(log.path), "rawSha256": digest(log.path),
                 "seenBytes": log.total, "overflow": log.overflow,
                 "rawAndTailExact": log.path.read_bytes() == expected_raw and log.tail == expected_raw}
        results.append(facts)
        write(root / "results.json", results)  # Preserve facts before the rejecting assertion.
        check(mode + ": actual Log retains close-after-closed BaseException without losing earlier errors",
              escaped is None and pipe.raw.closed and pipe.close_calls == 1
              and log.errors == expected and facts["rawAndTailExact"]
              and log.total == len(expected_raw) and log.overflow is False, facts=facts)
    return results


def main():
    if os.name != "nt":
        raise RuntimeError("Windows focused caller checks require actual Windows")
    event("start", scenario=SCENARIO)
    proof = Path(os.environ["VALIDATION_OUT"]) / "windows-inputs"
    proof.mkdir()
    log_close = log_close_rejections(proof)
    accepted = run_case(proof, "clean", "success", "pass", aliased=True)
    run_case(proof, "ignored-generated", "generated-directory", "pass")
    run_case(proof, "source-write-restore", "artifact-transient", "fail", source_mutation=True)
    run_case(proof, "strict-artifact-pycache", "artifact-transient", "fail")
    run_case(proof, "undeclared-junction", "success", "fail", linked="undeclared")
    run_case(proof, "source-undeclared-junction", "success", "fail", linked="source-undeclared")
    run_case(proof, "intermediate-junction-chain", "success", "fail", linked="chain")
    run_case(proof, "declared-junction", "success", "pass", linked="declared")
    run_case(proof, "declared-junction-target-write-restore", "artifact-transient", "fail", linked="declared")
    def downgrade(receipt, plan, _rows):
        receipt["schema"] = "codegraph-validation-v1"
        receipt.pop("integrityProfile")
        plan.pop("integrityProfile")
    def wrong_platform(receipt, plan, _rows):
        receipt["integrityProfile"] = plan["integrityProfile"] = "linux-inotify-v1"
        receipt["integrityPolicy"] = {"artifactPycacheIncluded": False}
    mutations = [
        ("wrong-platform-profile", wrong_platform),
        ("missing-schema", lambda r, _p, _w: r.pop("schema")),
        ("missing-profile", lambda r, _p, _w: r.pop("integrityProfile")),
        ("unknown-profile", lambda r, _p, _w: r.update(integrityProfile="unknown")),
        ("version-downgrade", downgrade),
        ("missing-selection", lambda _r, _p, w: w[0].pop("windowsSelection")),
        ("mismatched-selection", lambda _r, _p, w: w[0]["windowsSelection"].update(watchRoots=[])),
        ("missing-lifecycle", lambda _r, _p, w: w[0].pop("windowsWitness")),
        ("unclosed-handle", lambda _r, _p, w: w[0]["windowsWitness"].update(nativeHandlesClosed=False)),
        ("missing-observation", lambda _r, _p, w: w[0]["windowsWitness"].update(observationComplete=False)),
        ("empty-watches", lambda _r, _p, w: w[0]["windowsWitness"].update(watches=[])),
        ("unbound-namespace", lambda _r, _p, w: w[0]["windowsWitness"].update(namespaceBound=False)),
        ("unclosed-namespace", lambda _r, _p, w: w[0]["windowsWitness"].update(namespaceHandlesClosed=False)),
        ("missing-namespace-handles", lambda _r, _p, w: w[0]["windowsWitness"].pop("namespaceHandles")),
        ("changed-namespace-identity", lambda _r, _p, w:
            w[0]["windowsWitness"]["namespaceHandles"][0].update(identityStable=False)),
    ]
    rejected = [rejection(proof, accepted, name, mutate) for name, mutate in mutations]
    rejected.append(rejection(proof, accepted, "case-alias-all-markers-stripped-lifecycle", strip_all_markers))
    rejected.append(conflicting_binding_rejection(proof, accepted))
    oversized = oversized_rejections(proof, accepted, [
        ("missing-schema", lambda r, _p, _w: r.pop("schema")),
        ("unknown-schema", lambda r, _p, _w: r.update(schema="unknown")),
        ("missing-profile", lambda r, _p, _w: r.pop("integrityProfile")),
        ("unknown-profile", lambda r, _p, _w: r.update(integrityProfile="unknown")),
        ("linux-profile", lambda r, _p, _w: r.update(integrityProfile="linux-inotify-v1")),
        ("missing-runtime", lambda r, _p, _w: r.pop("runtime")),
        ("mismatched-runtime", lambda r, _p, _w: r["runtime"].update(system="Linux")),
    ])
    single = [compatibility(proof, False), compatibility(proof, True)]
    write(proof / "results.json", {"normalCompletedCases": 6, "normalPreReleaseVetoes": 3,
                                  "consumerRejectionProbes": rejected, "oversizeRejectionProbes": oversized,
                                  "recoveryCompatibleClosed": single,
                                  "logCloseFailureProbes": log_close,
                                  "moduleQualificationClaim": False})
    event("complete", scenario=SCENARIO)
    event("final", passed=True, scenarios=[SCENARIO])


if __name__ == "__main__":
    main()
