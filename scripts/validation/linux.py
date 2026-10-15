"""Docker prerequisites and isolated Linux validation; never stop other containers."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import shutil
import tarfile
import uuid

from events import event, assertion
from runner import write, Log

out = Path(os.environ["VALIDATION_OUT"])
mode = sys.argv[1]
call_number = 0


def docker(*args, timeout=20):
    global call_number
    call_number += 1
    command = ["docker", *args]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logs = [Log(pipe, out / f"docker-{call_number:03d}.{stream}.log", 10 * 1024 * 1024, 8192)
            for stream, pipe in [("stdout", process.stdout), ("stderr", process.stderr)]]
    limit = time.monotonic() + timeout
    failed = None
    while process.poll() is None:
        if any(log.overflow or log.errors for log in logs):
            failed = "Docker CLI log overflow/capture failure"
        elif time.monotonic() >= limit:
            failed = "Docker CLI deadline expired"
        if failed:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            break
        time.sleep(0.1)
    for log in logs:
        log.thread.join(timeout=5)
    if failed or any(log.thread.is_alive() or log.errors or log.overflow for log in logs):
        raise RuntimeError(failed or "Docker CLI logs did not close cleanly")
    result = subprocess.CompletedProcess(command, process.returncode,
                                        Path(logs[0].path).read_text(errors="replace"), Path(logs[1].path).read_text(errors="replace"))
    print(result.stdout)
    print(result.stderr, file=sys.stderr)
    return result


if mode == "prerequisite":
    scenario = "linux-prerequisite"
    event("start", scenario=scenario)
    attempts = []
    limit = time.monotonic() + 90
    while True:
        try:
            result = docker("info", "--format", "{{json .}}", timeout=10)
            attempts.append({"exitCode": result.returncode, "stdout": result.stdout, "stderr": result.stderr})
            ready = result.returncode == 0 and json.loads(result.stdout).get("OSType") == "linux"
        except (subprocess.TimeoutExpired, json.JSONDecodeError) as exc:
            attempts.append({"error": str(exc)})
            ready = False
        if ready or time.monotonic() >= limit:
            break
        time.sleep(2)
    write(out / "docker-readiness.json", attempts)
    assertion(scenario, "existing Linux engine became ready within declared 90 seconds", ready)
    inventory = docker("ps", "-a", "--no-trunc", "--format", "{{json .}}")
    write(out / "existing-containers.json", {"exitCode": inventory.returncode, "rows": inventory.stdout.splitlines()})
    assertion(scenario, "pre-existing containers recorded without mutation", inventory.returncode == 0)
    event("complete", scenario=scenario)
    event("final", passed=True, scenarios=[scenario])
elif mode == "validate":
    scenarios = ["linux-image", "linux-container"]
    event("start", scenario=scenarios[0])
    source = Path(os.environ["BASELINE_ARCHIVE"])
    context = out / "context"
    work = context / "work"
    work.mkdir(parents=True)
    with tarfile.open(source) as archive:
        archive.extractall(work, filter="data")
    (work / "baseline-source-commit.txt").write_text(os.environ["BASELINE_COMMIT"])
    shutil.copytree(Path(__file__).parent, work / "scripts" / "validation", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copyfile(source, context / "baseline-source.tar")
    (context / ".dockerignore").write_text("**/node_modules\n**/dist\n**/.git\n**/.codegraph\n**/.validation\n**/__pycache__\n")
    dockerfile = "FROM node:22-bookworm\nWORKDIR /work\nCOPY work /work\nCOPY baseline-source.tar /baseline-source.tar\n"
    expected = os.environ.get("BASELINE_EXPECTED_LINUX_ARTIFACT")
    if expected:
        shutil.copyfile(expected, context / "expected-linux-artifact.json")
        dockerfile += "COPY expected-linux-artifact.json /expected-linux-artifact.json\n"
    (context / "Dockerfile").write_text(dockerfile)
    identity = str(uuid.uuid4())
    image = "codegraph-validation-8:" + identity
    name = "codegraph-validation-8-" + identity
    write(out / "docker-owned-identities.json", {"name": name, "image": image, "label": identity})
    result = docker("build", "--tag", image, str(context), timeout=580)
    assertion(scenarios[0], "isolated Linux image prepared without host dependencies", result.returncode == 0)
    inspected = docker("image", "inspect", image)
    write(out / "linux-image.json", json.loads(inspected.stdout))
    assertion(scenarios[0], "resolved image identity retained", inspected.returncode == 0)
    saved = docker("image", "save", "--output", str(out / "baseline-linux-image.tar"), image, timeout=180)
    assertion(scenarios[0], "complete Linux OS/runtime/source image retained", saved.returncode == 0 and (out / "baseline-linux-image.tar").stat().st_size > 0)
    event("complete", scenario=scenarios[0])
    event("start", scenario=scenarios[1])
    evidence = out / "inner"
    evidence.mkdir()
    try:
        result = docker("run", "--rm", "--init", "--name", name, "--label", "codegraph.validation.owner=" + identity,
                        "--stop-timeout", "5", "--mount", f"type=bind,source={evidence},target=/export",
                        image, "python3", "/work/scripts/validation/linux-container.py", timeout=3600)
        assertion(scenarios[1], "complete Linux bootstrap/build/proof/baseline/freeze/archive ledger passed", result.returncode == 0)
        ledger = json.loads((evidence / "container-results.json").read_text())
        assertion(scenarios[1], "all seven inner receipts passed their real consumer", len(ledger) == 7 and all(row["outcome"] == "pass" for row in ledger))
    finally:
        found = docker("inspect", name)
        if found.returncode == 0:
            details = json.loads(found.stdout)[0]
            if details["Config"]["Labels"].get("codegraph.validation.owner") != identity:
                raise RuntimeError("refuse cleanup: container identity does not match owned label")
            removed = docker("rm", "--force", details["Id"])
            if removed.returncode != 0:
                raise RuntimeError("owned container cleanup failed")
        final = docker("ps", "-a", "--filter", "name=^/" + name + "$", "--format", "{{.ID}}")
        absent = final.returncode == 0 and not final.stdout.strip()
        write(out / "container-cleanup.json", {"name": name, "absent": absent, "queryExitCode": final.returncode})
    assertion(scenarios[1], "owned --rm --init container exited and was removed", absent)
    event("complete", scenario=scenarios[1])
    event("final", passed=True, scenarios=scenarios)
elif mode == "attest":
    scenario = "owned-containers-absent"
    event("start", scenario=scenario)
    names = json.loads(os.environ["VALIDATION_OWNED_CONTAINERS"])
    results = []
    for name in names:
        result = docker("ps", "-a", "--filter", "name=^/" + name + "$", "--format", "{{.ID}}")
        absent = result.returncode == 0 and not result.stdout.strip()
        results.append({"name": name, "absent": absent, "queryExitCode": result.returncode})
        assertion(scenario, "completed attempt's uniquely owned container is absent", absent, containerName=name)
    write(out / "container-cleanup-attestation.json", results)
    event("complete", scenario=scenario)
    event("final", passed=True, scenarios=[scenario])
else:
    raise ValueError(mode)
