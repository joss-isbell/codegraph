"""Consume one complete ordinary-CI attempt; unreadable discovery is never absence."""
import json
import math
import os
from pathlib import Path
import stat
import sys


def regular_file(path):
    info = path.lstat()  # Only a genuine missing leaf may be ignored by discovery.
    if not stat.S_ISREG(info.st_mode) or getattr(info, "st_file_attributes", 0) & 0x400:
        raise ValueError(f"receipt is not a regular file: {path}")


def read_json(path):
    regular_file(path)
    with path.open("rb") as source:
        raw = source.read(2 * 1024 * 1024 + 1)
    if len(raw) > 2 * 1024 * 1024:
        raise ValueError("receipt exceeds 2 MiB")
    return json.loads(raw)


def verify(root):
    candidates = []
    with os.scandir(root) as entries:
        for entry in entries:
            info = entry.stat(follow_symlinks=False)
            if getattr(info, "st_file_attributes", 0) & 0x400:
                raise ValueError(f"attempt directory is a reparse point: {entry.path}")
            if not stat.S_ISDIR(info.st_mode):
                continue
            path = Path(entry.path) / "receipt.json"
            try:
                regular_file(path)
            except FileNotFoundError:
                continue
            candidates.append(path)
            if len(candidates) == 2:
                break
    if len(candidates) != 1:
        raise ValueError("expected exactly one ordinary-CI receipt")
    path = candidates[0]
    receipt = read_json(path)
    stages = receipt.get("stages")
    if (receipt.get("schema") != "codegraph-ordinary-windows-ci-v1"
            or receipt.get("plannedStages") != ["install", "build", "test"]
            or not isinstance(stages, list)
            or [stage.get("stage") for stage in stages] != ["install", "build", "test"]):
        raise ValueError("expected complete install/build/test receipt")
    if (receipt.get("outcome") != "pass" or receipt.get("commandSequenceOutcome") != "pass"
            or receipt.get("failures") != [] or receipt.get("interrupted") is not False):
        raise ValueError("ordinary-CI final receipt did not pass")
    elapsed = receipt.get("elapsedSeconds")
    if (receipt.get("deadlineSeconds") != 3300 or type(elapsed) not in (int, float)
            or not math.isfinite(elapsed) or not 0 <= elapsed < 3300):
        raise ValueError("ordinary-CI final receipt exceeded its deadline")
    for stage in stages:
        if (stage.get("outcome") != "pass" or type(stage.get("exitCode")) is not int
                or stage["exitCode"] != 0 or stage.get("failures") != []
                or stage.get("cleanup", {}).get("complete") is not True
                or stage["cleanup"].get("remainingPids") != []
                or stage["cleanup"].get("intentionallyStoppedOwnedPids") != []):
            raise ValueError(f"ordinary-CI stage did not pass: {stage.get('stage')}")
        directory = path.parent / stage["stage"]
        if read_json(directory / "result.json") != stage:
            raise ValueError(f"stage receipt disagrees with final receipt: {stage['stage']}")
        logs = stage.get("logs")
        if not isinstance(logs, dict) or set(logs) != {"stdout.log", "stderr.log"}:
            raise ValueError("incomplete ordinary-CI capture declarations")
        for stream, capture in logs.items():
            if (any(capture.get(key) is not True for key in ("constructorReturned", "joined",
                    "pipeClosed", "pipeCloseAttempted", "pipeCloseCompleted"))
                    or capture.get("pipeCloseError") is not None
                    or capture.get("overflow") is not False or capture.get("errors") != []):
                raise ValueError(f"incomplete ordinary-CI capture: {stage['stage']}/{stream}")
            retained = capture.get("retainedBytes")
            if (type(retained) is not int or not 0 <= retained <= 10 * 1024 * 1024
                    or capture.get("seenBytes") != retained):
                raise ValueError("invalid ordinary-CI capture size")
            log_path = directory / stream
            regular_file(log_path)
            with log_path.open("rb") as source:
                observed = 0
                while observed <= 10 * 1024 * 1024:
                    block = source.read(65536)
                    if not block:
                        break
                    observed += len(block)
            if observed != retained:
                raise ValueError("ordinary-CI raw capture size mismatch")
    return {"outcome": "pass", "receipt": str(path)}


def main():
    try:
        result = verify(Path(sys.argv[1]))
    except BaseException as error:
        print(json.dumps({"outcome": "fail", "error": f"{type(error).__name__}: {error}"[:4096]}))
        return 1
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
