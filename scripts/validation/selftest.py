"""Exercise the runner and its actual reporting consumer through process seams."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from events import event, assertion
from runner import process_sample, write


def producer(mode):
    scenario = "probe"
    out = Path(os.environ["VALIDATION_OUT"])
    if mode not in ["child", "grandchild", "short-parent", "short-descendant"]:
        event("start", scenario=scenario)
    if mode == "grandchild":
        (out / "grandchild.pid").write_text(str(os.getpid()))
        time.sleep(60)
    elif mode == "short-descendant":
        (out / "short-descendant.pid").write_text(str(os.getpid()))
        time.sleep(0.25)
    elif mode == "short-parent":
        subprocess.Popen([sys.executable, __file__, "produce", "short-descendant"])
        limit = time.monotonic() + 5
        while not (out / "short-descendant.pid").exists() and time.monotonic() < limit:
            time.sleep(0.01)
    elif mode == "reaped-exit":
        child = subprocess.Popen([sys.executable, __file__, "produce", "short-parent"])
        child.wait(timeout=5)
        pid = int((out / "short-descendant.pid").read_text())
        limit = time.monotonic() + 5
        gone = False
        while time.monotonic() < limit:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                gone = True
                break
            time.sleep(0.05)
        assertion(scenario, "adopted natural descendant exit is reaped while target is live", gone)
        event("complete", scenario=scenario)
        event("final", passed=True, scenarios=[scenario])
    elif mode == "child":
        child = subprocess.Popen([sys.executable, __file__, "produce", "grandchild"])
        (out / "child.pid").write_text(str(os.getpid()))
        time.sleep(60)
    elif mode in ["timeout", "interrupt", "os-interrupt", "natural-exit"]:
        child = subprocess.Popen([sys.executable, __file__, "produce", "child"])
        (out / "producer.pid").write_text(str(os.getpid()))
        if mode != "natural-exit":
            time.sleep(60)
        else:
            deadline = time.monotonic() + 5
            while not (out / "grandchild.pid").exists() and time.monotonic() < deadline:
                time.sleep(0.05)
            assertion(scenario, "tree reached readiness before natural parent exit", (out / "grandchild.pid").exists())
            event("complete", scenario=scenario)
            event("final", passed=True, scenarios=[scenario])
    elif mode == "overflow":
        for _ in range(64):
            os.write(1, b"x" * 4096)
        assertion(scenario, "late acceptance tail exists", True)
        event("complete", scenario=scenario)
        event("final", passed=True, scenarios=[scenario])
    elif mode == "generated-directory":
        generated = Path(os.environ["PROBE_SOURCE"]) / "existing-empty" / "node_modules" / "package"
        generated.mkdir(parents=True)
        (generated / "generated.js").write_text("generated output")
        assertion(scenario, "ignored generated directory created without changing source bytes", True)
        event("complete", scenario=scenario)
        event("final", passed=True, scenarios=[scenario])
    elif mode in ["mutation", "artifact-transient", "source-transient"]:
        path = Path(os.environ["MUTABLE_ARTIFACT"])
        if mode == "source-transient":
            path = Path(os.environ["PROBE_SOURCE"]) / "existing-empty" / "transient.ts"
            path.write_text("export const transient = 1;")
            path.unlink()
        elif mode == "artifact-transient":
            before = path.stat()
            original = path.read_bytes()
            path.write_text("changed")
            path.write_bytes(original)
            os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns))
        else:
            path.write_text("changed")
        assertion(scenario, "producer finished", True)
        event("complete", scenario=scenario)
        event("final", passed=True, scenarios=[scenario])
    else:
        # Empty optional collection is a real produced value; required inventories remain nonempty.
        count = {"singleton": 1, "multiple": 2}.get(mode, 0)
        write(out / "inventory.json", {"projects": [{"name": str(n)} for n in range(count)],
                                      "sessions": [{"name": str(n)} for n in range(count)]})
        assertion(scenario, "producer finished", True, projects=[], sessions=[])
        if mode != "missing-completion":
            event("complete", scenario=scenario)
        if mode != "missing-final":
            event("final", passed=True, scenarios=[scenario])


def main():
    scenario = "runner-proof"
    event("start", scenario=scenario)
    out = Path(os.environ["VALIDATION_OUT"])
    root = HERE.parent.parent
    proof = out / "probes"
    proof.mkdir()
    sentinel = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(180)"])
    results = []
    try:
        cases = [("success", "pass"), ("singleton", "pass"), ("multiple", "pass"),
                               ("missing-final", "fail"), ("missing-completion", "fail"),
                               ("overflow", "fail"), ("timeout", "fail"), ("interrupt", "fail"), ("mutation", "fail"),
                               ("artifact-transient", "fail"), ("source-transient", "fail"),
                               ("os-interrupt", "fail"), ("natural-exit", "pass"), ("generated-directory", "pass")]
        if os.name != "nt":
            cases.append(("reaped-exit", "pass"))
        for mode, expected in cases:
            attempt = proof / mode
            mutable = proof / (mode + ".artifact")
            mutable.write_text("original")
            fixture = proof / (mode + "-source")
            fixture.mkdir()
            (fixture / "existing-empty").mkdir()
            (fixture / "baseline-source-commit.txt").write_text("selftest-fixture-v1")
            (fixture / "app.ts").write_text("export const original = 1;\n")
            plan = {"sourceRoot": str(fixture), "cwd": str(root), "output": str(attempt),
                    "command": [sys.executable, str(HERE / "selftest.py"), "produce", mode],
                    "deadlineSeconds": 2 if mode == "timeout" else 15, "scenarios": ["probe"],
                    "artifacts": [str(mutable)], "executionMode": "runner-selftest",
                    "environment": {"MUTABLE_ARTIFACT": str(mutable), "PROBE_SOURCE": str(fixture)},
                    "bounds": {"logBytesPerStream": 32768, "drainSeconds": 0.1,
                               "startupHandshakeSeconds": 0.1, "cleanupSeconds": 3, "sampleSeconds": 0.05}}
            plan_path = proof / (mode + ".plan.json")
            write(plan_path, plan)
            options = {}
            if mode == "os-interrupt" and os.name == "nt":
                info = subprocess.STARTUPINFO()
                info.dwFlags = subprocess.STARTF_USESHOWWINDOW
                info.wShowWindow = 0
                options = {"creationflags": subprocess.CREATE_NEW_CONSOLE, "startupinfo": info}
            child = subprocess.Popen([sys.executable, str(HERE / "runner.py"), str(plan_path)],
                                     stdout=subprocess.PIPE, stderr=subprocess.PIPE, **options)
            if mode in ["interrupt", "os-interrupt"]:
                deadline = time.monotonic() + 8
                while not (attempt / "grandchild.pid").exists() and time.monotonic() < deadline:
                    time.sleep(0.05)
                assertion(scenario, "interrupted tree reached grandchild readiness", (attempt / "grandchild.pid").exists())
                if mode == "interrupt":
                    (attempt / "interrupt.request").touch()
                elif os.name == "nt":
                    sent = subprocess.run([sys.executable, str(HERE / "signal-child.py"), str(child.pid)], capture_output=True, timeout=5)
                    assertion(scenario, "CTRL_BREAK sent to owned private hidden console", sent.returncode == 0,
                              stderr=sent.stderr.decode(errors="replace"))
                else:
                    import signal
                    os.kill(child.pid, signal.SIGINT)
            stdout, stderr = child.communicate(timeout=25)
            (proof / (mode + ".controller.stdout")).write_bytes(stdout)
            (proof / (mode + ".controller.stderr")).write_bytes(stderr)
            # Execute the exact reporting consumer used for engineering results.
            consumer = subprocess.run([sys.executable, str(HERE / "report.py"), str(attempt / "receipt.json")],
                                      capture_output=True, timeout=5)
            (proof / (mode + ".consumer.json")).write_bytes(consumer.stdout)
            actual = json.loads(consumer.stdout)
            assertion(scenario, f"{mode}: real reporting consumer yields {expected}", actual["outcome"] == expected,
                      receipt=str(attempt / "receipt.json"), consumerExit=consumer.returncode)
            receipt = json.loads((attempt / "receipt.json").read_text())
            assertion(scenario, f"{mode}: cleanup complete", receipt["cleanup"]["complete"])
            assertion(scenario, f"{mode}: unrelated sentinel survives", sentinel.poll() is None)
            if mode in ["timeout", "interrupt", "os-interrupt", "natural-exit"]:
                known = receipt["cleanup"]["observedIdentities"]
                assertion(scenario, f"{mode}: child and grandchild creation identities captured", len(known) >= 4,
                          ownedCount=len(known))
                for pid_file in ["producer.pid", "child.pid", "grandchild.pid"]:
                    pid = int((attempt / pid_file).read_text())
                    assertion(scenario, f"{mode}: {pid_file} stopped", process_sample(pid) is None)
            if mode == "os-interrupt":
                assertion(scenario, "OS signal reached registered supervisor handler", "interrupted" in receipt["failures"])
            results.append({"mode": mode, "outcome": actual["outcome"], "receipt": str(attempt / "receipt.json")})
        # Real receipt mutations validate that the consumer rejects damaged evidence,
        # cleanup failure and malformed/empty required inventories. These are clearly
        # labelled rejection probes, never represented as target attempts.
        good = proof / "success" / "receipt.json"
        original = json.loads(good.read_text())
        for name, change in [
            ("cleanup-failure", lambda r: r.update(cleanup={"complete": False})),
            ("empty-scenarios", lambda r: r.update(scenarios=[])),
            ("empty-evidence", lambda r: r.update(evidence=[])),
            ("scalar-events", lambda r: r.update(events={})),
            ("changed-log", lambda r: r["evidence"][0].update(sha256="0" * 64)),
            ("pruned-evidence", lambda r: r.update(evidence=r["evidence"][:1])),
            ("missing-one-core", lambda r: r.update(evidence=[e for e in r["evidence"] if e["path"] != "source-before.json"])),
            ("event-member-null", lambda r: r.update(events=[None])),
            ("scenario-member-object", lambda r: r.update(scenarios=[{}])),
            ("evidence-member-null", lambda r: r.update(evidence=[None])),
            ("cleanup-array", lambda r: r.update(cleanup=[])),
            ("receipt-event-substitution", lambda r: r["events"][1].update(name="substituted")),
        ]:
            mutated = json.loads(json.dumps(original))
            change(mutated)
            path = good.parent / (name + ".rejection-probe.json")
            write(path, mutated)
            consumer = subprocess.run([sys.executable, str(HERE / "report.py"), str(path)], capture_output=True, timeout=5)
            assertion(scenario, f"consumer rejects {name}", consumer.returncode == 1,
                      consumer=json.loads(consumer.stdout))
        write(out / "proof-results.json", results)
        singleton = proof / "singleton" / "receipt.json"
        altered = json.loads(singleton.read_text())
        altered["projects"] = []
        altered_path = singleton.parent / "inventory-substitution.rejection-probe.json"
        write(altered_path, altered)
        rejected = subprocess.run([sys.executable, str(HERE / "report.py"), str(altered_path)], capture_output=True, timeout=5)
        assertion(scenario, "actual singleton inventory cannot become empty in receipt", rejected.returncode == 1)
        # Non-finite declarations must reject before creating an attempt or target.
        for invalid in [float("nan"), float("inf"), 0, -1, True]:
            invalid_plan = {**plan, "deadlineSeconds": invalid, "output": str(proof / ("invalid-" + str(invalid)))}
            invalid_path = proof / ("invalid-" + str(invalid) + ".plan.json")
            write(invalid_path, invalid_plan)
            rejected = subprocess.run([sys.executable, str(HERE / "runner.py"), str(invalid_path)], capture_output=True, timeout=5)
            assertion(scenario, f"invalid deadline {invalid} rejected before launch", rejected.returncode != 0 and not Path(invalid_plan["output"]).exists())
        event("complete", scenario=scenario)
        event("final", passed=True, scenarios=[scenario])
    finally:
        sentinel.terminate()
        sentinel.wait(timeout=5)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "produce":
        producer(sys.argv[2])
    else:
        main()
