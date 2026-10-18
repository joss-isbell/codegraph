"""Run the existing Windows CI helper regressions with bounded supervision."""
import os
from pathlib import Path
import signal
import sys
import threading
import time
import uuid

sys.dont_write_bytecode = True
import windows as ci


HERE = Path(__file__).resolve().parent
STAGES = (("windows", "windows.test.py"), ("verify-windows", "verify-windows.test.py"))
SUITE_SECONDS = 120
TOTAL_SECONDS = 360


def main():
    if os.name != "nt" or sys.version_info < (3, 9):
        raise RuntimeError("Windows CI helper tests require Windows and Python >= 3.9")
    output_base = (Path(os.environ["RUNNER_TEMP"]) / "codegraph-windows-ci-helpers"
                   if os.environ.get("RUNNER_TEMP") else ci.ROOT / ".validation" / "windows-ci-helpers")
    output = output_base / str(uuid.uuid4())
    output.mkdir(parents=True)
    started = time.monotonic()
    whole_end = started + TOTAL_SECONDS
    interrupted = threading.Event()
    handlers = {}
    receipt = {"schema": "codegraph-windows-ci-helpers-v1", "outcome": "incomplete",
               "sourceSha": os.environ.get("GITHUB_SHA"), "python": sys.version,
               "interpreter": sys.executable, "deadlineSeconds": TOTAL_SECONDS,
               "plannedStages": [name for name, _ in STAGES], "stages": [], "failures": []}
    try:
        ci.save(output / "receipt.json", receipt)
        for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGBREAK):
            handlers[sig] = signal.signal(sig, lambda _sig, _frame: interrupted.set())
        core = ci.load_supervisor()
        for name, script in STAGES:
            result = ci.run_stage(core, name, [sys.executable, "-B", str(HERE / script)],
                                  SUITE_SECONDS, output / name, whole_end, interrupted)
            receipt["stages"].append(result)
            ci.save(output / "receipt.json", receipt)
    except BaseException as exc:
        receipt["failures"].append(f"helper supervisor error: {type(exc).__name__}: {exc}")
    finally:
        for sig, handler in handlers.items():
            try:
                signal.signal(sig, handler)
            except BaseException as exc:
                receipt["failures"].append(f"signal restoration failed: {type(exc).__name__}: {exc}")

    commands_passed = (len(receipt["stages"]) == len(STAGES)
                       and all(stage["outcome"] == "pass" for stage in receipt["stages"]))
    receipt["commandSequenceOutcome"] = "pass" if commands_passed else "fail"
    try:
        print(f"Windows CI helper commands: {receipt['commandSequenceOutcome']}; "
              f"final receipt pending: {output / 'receipt.json'}", flush=True)
        if receipt["failures"]:
            print("\n".join(receipt["failures"])[-4096:], file=sys.stderr, flush=True)
    except BaseException as exc:
        receipt["failures"].append(f"helper reporting failed: {type(exc).__name__}: {exc}")

    def observe_tail():
        receipt["elapsedSeconds"] = time.monotonic() - started
        receipt["interrupted"] = interrupted.is_set()
        for observed, reason in ((receipt["interrupted"], "helper tests interrupted"),
                (time.monotonic() >= whole_end, "whole helper deadline expired during finalization")):
            if observed and reason not in receipt["failures"]:
                receipt["failures"].append(reason)
        receipt["outcome"] = "pass" if commands_passed and not receipt["failures"] else "fail"

    observe_tail()
    saved_outcome = None
    try:
        ci.save(output / "receipt.json", receipt)
        saved_outcome = receipt["outcome"]
    except BaseException as exc:
        receipt["failures"].append(f"final helper receipt write failed: {type(exc).__name__}: {exc}")
    observe_tail()
    if saved_outcome != receipt["outcome"]:
        try:
            ci.save(output / "receipt.json", receipt)
        except BaseException as exc:
            receipt["outcome"] = "fail"
            try:
                os.write(2, f"Cannot persist failed helper receipt: {exc}".encode(
                    "utf-8", errors="replace")[:4096])
            except BaseException:
                pass
    return 0 if receipt["outcome"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
