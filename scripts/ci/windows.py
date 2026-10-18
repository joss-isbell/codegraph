"""Run the ordinary Windows install/build/test commands with bounded supervision."""
import ctypes
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid


ROOT = Path(__file__).resolve().parents[2]
STAGES = (("install", ("ci",), 600), ("build", ("run", "build"), 600),
          ("test", ("test", "--", "--reporter=default", "--reporter=json"), 1800))
TOTAL_SECONDS = 3300
LOG_BYTES = 10 * 1024 * 1024
EXCERPT_BYTES = 8192
CLEANUP = {"startupHandshakeSeconds": 15, "drainSeconds": 15,
           "cleanupSeconds": 5}


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def load_supervisor():
    # Reuse containment and bounded drains, not the Issue #8 scenario consumer
    # or its source Witness. The normal CI job has no private acceptance claim.
    path = ROOT / "scripts" / "validation" / "runner.py"
    spec = importlib.util.spec_from_file_location("codegraph_ci_supervisor", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # The shared primitive historically discards CloseHandle's BOOL. Keep its
    # behavior unchanged elsewhere; this caller may only accept checked closure.
    class CheckedKernel:
        def __getattr__(self, name):
            return getattr(native_kernel, name)

        def CloseHandle(self, handle):
            if not native_kernel.CloseHandle(handle):
                raise ctypes.WinError(ctypes.get_last_error())
            return True

    native_kernel = module.kernel
    module.kernel = CheckedKernel()
    return module


class ObservedPipe:
    """Retain the real Log reader's close result; never retry/race-close it."""
    def __init__(self, pipe):
        self.pipe = pipe
        self.closeAttempted, self.closeCompleted, self.closeError = False, False, None

    @property
    def closed(self):
        return self.pipe.closed

    def read(self, size):
        return self.pipe.read(size)

    def close(self):
        self.closeAttempted = True
        try:
            self.pipe.close()
            self.closeCompleted = True
        except BaseException as error:
            # Frozen Log's finally does not catch close errors; caller observes this.
            self.closeError = (type(error).__name__ + ": " + str(error))[:4096]


def run_stage(core, name, command, seconds, output, whole_end, interrupted):
    output.mkdir()
    started = time.monotonic()
    end = min(started + seconds, whole_end - 70)
    result = {"stage": name, "command": command, "deadlineSeconds": seconds,
              "outcome": "incomplete", "exitCode": None,
              "launchState": "not-started", "failures": [], "logs": {},
              "cleanup": {"complete": False}}
    save(output / "result.json", result)
    owner = None
    child = None
    logs = []
    try:
        if interrupted.is_set() or time.monotonic() >= end:
            raise RuntimeError("interrupted or no command budget remains")
        # Retain a partially initialized owner for independent handle cleanup.
        owner = core.Ownership.__new__(core.Ownership)
        core.Ownership.__init__(owner)
        result["ownerUuid"] = owner.token
        if interrupted.is_set() or time.monotonic() >= end:
            raise RuntimeError("interrupted or command deadline expired before launch")
        result["launchState"] = "gate-constructor-started"
        child = subprocess.Popen([sys.executable, str(core.HERE / "runner.py"), "_gate"],
            cwd=ROOT, env={**os.environ, "CODEGRAPH_DIR": ".codegraph",
                           "VALIDATION_OWNER_UUID": owner.token,
                           "NPM_CONFIG_CACHE": str(output.parent / "npm-cache")},
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP)
        owner.gate = child
        result["gatePid"] = child.pid
        result["launchState"] = "gate-created"
        if not core.kernel.AssignProcessToJobObject(owner.handle, int(child._handle)):
            assignment_error = ctypes.get_last_error()
            # The gate has not received an argv, so no npm payload can start.
            child.kill()
            child.wait(timeout=5)
            raise ctypes.WinError(assignment_error)
        result["launchState"] = "job-owned"
        for stream, pipe in (("stdout", child.stdout), ("stderr", child.stderr)):
            observed = ObservedPipe(pipe)
            log = core.Log.__new__(core.Log)
            entry = {"pipe": observed, "log": log, "path": output / (stream + ".log"),
                     "constructorReturned": False}
            logs.append(entry)  # retain a partial constructor/possibly starting reader.
            core.Log.__init__(log, observed, entry["path"], LOG_BYTES, EXCERPT_BYTES)
            entry["constructorReturned"] = True
        if interrupted.is_set() or time.monotonic() >= end:
            raise RuntimeError("interrupted or command deadline expired before payload release")
        # Both bounded drains exist before the only payload release.
        result["launchState"] = "payload-write-started"
        payload = (json.dumps(command) + "\n").encode("utf-8")
        if child.stdin.write(payload) != len(payload):
            raise RuntimeError("short payload write; command launch is uncertain")
        child.stdin.flush()
        child.stdin.close()
        result["launchState"] = "payload-released"
        while child.poll() is None:
            if interrupted.is_set():
                raise RuntimeError("interrupted")
            if time.monotonic() >= end:
                raise RuntimeError("command deadline expired")
            if any(entry["log"].overflow or entry["log"].errors for entry in logs):
                raise RuntimeError("output overflow or log capture failure")
            time.sleep(0.1)
        result["exitCode"] = child.returncode
        if time.monotonic() >= end:
            result["failures"].append("command deadline expired")
        if child.returncode != 0:
            result["failures"].append("command returned a nonzero exit code")
    except BaseException as exc:
        result["failures"].append(f"{type(exc).__name__}: {exc}")
    finally:
        if child is not None and child.stdin is not None and not child.stdin.closed:
            try:
                child.stdin.close()
            except BaseException as exc:
                result["failures"].append(f"stdin close failed: {exc}")
        if owner is not None:
            try:
                result["cleanup"] = owner.cleanup(CLEANUP)
                if result["cleanup"].get("intentionallyStoppedOwnedPids"):
                    result["failures"].append("owned processes required forced cleanup")
            except BaseException as exc:
                result["failures"].append(f"cleanup failed: {type(exc).__name__}: {exc}")
            finally:
                # cleanup() can raise during native membership queries before
                # closing the Job. Close that exact retained handle BEFORE
                # waiting for readers; kill-on-close then releases inherited
                # pipes. The original cleanup error remains a failure.
                handle = getattr(owner, "handle", None)
                if handle:
                    try:
                        if not core.kernel.CloseHandle(handle):
                            raise ctypes.WinError(ctypes.get_last_error())
                        owner.handle = None
                        result["emergencyJobCloseChecked"] = True
                    except BaseException as exc:
                        result["failures"].append(f"independent Job close failed: {exc}")
        if child is not None:
            try:
                child.wait(timeout=5)
                result["exitCode"] = child.returncode
            except BaseException as exc:
                result["failures"].append(f"gate exit wait failed: {exc}")
        try:
            drain_end = time.monotonic() + 5
            for entry in logs:
                log, observed, path = entry["log"], entry["pipe"], entry["path"]
                row = result["logs"][path.name] = {"constructorReturned": entry["constructorReturned"],
                    "joined": False, "pipeClosed": False, "retainedBytes": None}
                try:
                    thread = getattr(log, "thread", None)
                    if thread is None:
                        observed.close()  # fixed Log has not constructed/started a reader.
                        raise RuntimeError("partial Log constructor without reader thread")
                    thread.join(timeout=max(0, drain_end - time.monotonic()))
                    row["joined"] = not thread.is_alive()
                except BaseException as exc:
                    # Unknown/alive reader is never race-closed or retried here.
                    result["failures"].append(f"reader join failed ({path.name}): {type(exc).__name__}: {exc}")
                try:
                    row.update(pipeCloseAttempted=observed.closeAttempted,
                        pipeCloseCompleted=observed.closeCompleted, pipeCloseError=observed.closeError,
                        seenBytes=getattr(log, "total", None), overflow=getattr(log, "overflow", None),
                        errors=list(getattr(log, "errors", [])))
                    row["pipeClosed"] = observed.closed
                    if (not row["constructorReturned"] or not row["joined"] or not row["pipeClosed"]
                            or not row["pipeCloseAttempted"] or not row["pipeCloseCompleted"]
                            or row["pipeCloseError"] or row["overflow"] or row["errors"]):
                        result["failures"].append(f"log EOF/drain/checked pipe close incomplete ({path.name})")
                    result["failures"].extend(row["errors"])
                except BaseException as exc:
                    result["failures"].append(f"reader observation failed ({path.name}): {type(exc).__name__}: {exc}")
                try:
                    row["retainedBytes"] = path.stat().st_size
                    if row["retainedBytes"] != row.get("seenBytes"):
                        result["failures"].append(f"log retained size mismatch ({path.name})")
                except BaseException as exc:
                    result["failures"].append(f"log stat failed ({path.name}): {type(exc).__name__}: {exc}")
        finally:
            # Reader join/state/stat failures cannot suppress this exact retained close.
            if child is not None:
                try:
                    child._handle.Close()
                    result["gateHandleCloseChecked"] = True
                except BaseException as exc:
                    result["failures"].append(f"gate handle close failed: {exc}")
    result["elapsedSeconds"] = time.monotonic() - started
    result["failureExcerpt"] = b"\n".join(getattr(entry["log"], "tail", b"") for entry in logs)[-EXCERPT_BYTES:].decode("utf-8", errors="replace")
    if time.monotonic() >= whole_end:
        result["failures"].append("whole CI deadline expired")
    passed = (result["exitCode"] == 0 and result["cleanup"].get("complete") is True
              and not result["failures"])
    result["outcome"] = "pass" if passed else "fail"
    if passed:
        result["failureExcerpt"] = ""
    save(output / "result.json", result)
    if time.monotonic() >= whole_end and result["outcome"] == "pass":
        result["failures"].append("whole CI deadline expired while saving result")
        result["outcome"] = "fail"
        save(output / "result.json", result)
    print(f"{name}: {result['outcome']} ({result['elapsedSeconds']:.1f}s); logs: {output}", flush=True)
    if not passed:
        print("\n".join(result["failures"]), flush=True)
        print(result["failureExcerpt"], flush=True)
    return result


def main():
    if os.name != "nt" or sys.version_info < (3, 9):
        raise RuntimeError("ordinary Windows CI requires Windows and Python >= 3.9")
    node = shutil.which("node")
    npm = shutil.which("npm.cmd")
    if not node or not npm:
        raise RuntimeError("Node and npm.cmd must be on PATH")
    npm_cli = Path(npm).resolve().parent / "node_modules" / "npm" / "bin" / "npm-cli.js"
    if not npm_cli.is_file():
        raise RuntimeError("cannot find npm's installed CLI beside npm.cmd")
    output_base = (Path(os.environ["RUNNER_TEMP"]) / "codegraph-windows-ci"
                   if os.environ.get("RUNNER_TEMP") else ROOT / ".validation" / "windows-ci")
    output = output_base / str(uuid.uuid4())
    output.mkdir(parents=True)
    started = time.monotonic()
    whole_end = started + TOTAL_SECONDS
    interrupted = threading.Event()
    handlers = {}
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGBREAK):
        handlers[sig] = signal.signal(sig, lambda _sig, _frame: interrupted.set())
    receipt = {"schema": "codegraph-ordinary-windows-ci-v1", "outcome": "incomplete",
               "sourceSha": os.environ.get("GITHUB_SHA"), "python": sys.version,
               "node": str(Path(node).resolve()), "npmCli": str(npm_cli),
               "deadlineSeconds": TOTAL_SECONDS, "stages": [],
               "plannedStages": [name for name, _, _ in STAGES], "failures": []}
    save(output / "receipt.json", receipt)
    try:
        core = load_supervisor()
        for name, args, seconds in STAGES:
            result = run_stage(core, name, [str(Path(node).resolve()), str(npm_cli), *args],
                               seconds, output / name, whole_end, interrupted)
            receipt["stages"].append(result)
            save(output / "receipt.json", receipt)
            if result["outcome"] != "pass":
                break
    except BaseException as exc:
        receipt["supervisorError"] = f"{type(exc).__name__}: {exc}"
        receipt["failures"].append("supervisor error: " + receipt["supervisorError"])

    commands_passed = (len(receipt["stages"]) == len(STAGES)
                       and all(stage["outcome"] == "pass" for stage in receipt["stages"]))
    receipt["commandSequenceOutcome"] = "pass" if commands_passed else "fail"
    # Report command status, not a final pass: every fallible report operation
    # and signal-handler restoration belongs to final receipt reconciliation.
    try:
        print(f"Windows CI commands: {receipt['commandSequenceOutcome']}; "
              f"final receipt pending: {output / 'receipt.json'}", flush=True)
        if "supervisorError" in receipt:
            print(receipt["supervisorError"], flush=True)
    except BaseException as exc:
        receipt["failures"].append(f"stdout reporting failed: {type(exc).__name__}: {exc}")
    try:
        if os.environ.get("GITHUB_STEP_SUMMARY"):
            with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
                summary.write(f"Windows install/build/test commands: **{receipt['commandSequenceOutcome']}**. "
                              "Final status, bounded logs and receipts are in the windows-ci artifact.\n")
    except BaseException as exc:
        receipt["failures"].append(f"step-summary reporting failed: {type(exc).__name__}: {exc}")
    if receipt["failures"]:
        try:
            print("\n".join(receipt["failures"])[-4096:], file=sys.stderr, flush=True)
        except BaseException as exc:
            receipt["failures"].append(f"stderr reporting failed: {type(exc).__name__}: {exc}")
    for sig, handler in handlers.items():
        try:
            signal.signal(sig, handler)
        except BaseException as exc:
            receipt["failures"].append(f"signal restoration failed: {type(exc).__name__}: {exc}")

    def observe_tail():
        receipt["elapsedSeconds"] = time.monotonic() - started
        receipt["interrupted"] = interrupted.is_set()
        for observed, reason in ((receipt["interrupted"], "CI interrupted"),
                (time.monotonic() >= whole_end, "whole CI deadline expired during finalization")):
            if observed and reason not in receipt["failures"]:
                receipt["failures"].append(reason)
        receipt["outcome"] = "pass" if commands_passed and not receipt["failures"] else "fail"

    observe_tail()
    saved_outcome = None
    try:
        save(output / "receipt.json", receipt)
        saved_outcome = receipt["outcome"]
    except BaseException as exc:
        receipt["failures"].append(f"final receipt write failed: {type(exc).__name__}: {exc}")
    observe_tail()
    if saved_outcome != receipt["outcome"]:
        # At most one failure reconciliation write; no command is retried.
        try:
            save(output / "receipt.json", receipt)
        except BaseException as exc:
            receipt["failures"].append(f"failure receipt write failed: {type(exc).__name__}: {exc}")
            receipt["outcome"] = "fail"
            try:
                os.write(2, ("Windows CI could not persist its failed final receipt: "
                             + "; ".join(receipt["failures"])).encode("utf-8", errors="replace")[:4096])
            except BaseException:
                pass  # A lost diagnostic stream cannot turn native failure into pass.
    return 0 if receipt["outcome"] == "pass" else 1


if __name__ == "__main__":
    sys.dont_write_bytecode = True
    sys.exit(main())
