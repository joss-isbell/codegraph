"""Small evidence protocol shared by real adapters and supervisor probes."""
import json
import os
import time


def event(kind, **fields):
    with open(os.environ["VALIDATION_EVENTS"], "a", encoding="utf-8") as output:
        output.write(json.dumps({"type": kind, "atEpochSeconds": time.time(), **fields}) + "\n")


def assertion(scenario, name, passed, **evidence):
    event("assert", scenario=scenario, name=name, passed=bool(passed), **evidence)
    if not passed:
        raise AssertionError(name)
