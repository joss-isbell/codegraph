"""Interpret bound baseline resource samples by explicit adapter phase windows."""
import json
import os
from pathlib import Path
import statistics
import sys

from events import event, assertion
from report import consume
from runner import write

source = Path(sys.argv[1])
out = Path(os.environ["VALIDATION_OUT"])
scenario = "baseline-measurements"
event("start", scenario=scenario)
checked = consume(source / "receipt.json")
assertion(scenario, "actual baseline receipt passes reporting consumer before interpretation", checked["outcome"] == "pass", summary=checked)
read = lambda name: json.loads((source / name).read_text())
lines = lambda name: [json.loads(line) for line in (source / name).read_text().splitlines()]
samples = lines("resources.jsonl")
markers = lines("phases.jsonl")
workers = lines("workers.jsonl")


def resource_summary(rows):
    rss = [sum(p["rssBytes"] or 0 for p in row["processes"]) for row in rows]
    cpu = {}
    for row in rows:
        for p in row["processes"]:
            key = (p["pid"], p["creationIdentity"])
            cpu.setdefault(key, []).append(p["cpuSeconds"])
    return {"sampleCount": len(rows), "peakAggregateRssBytes": max(rss, default=0),
            "maxSampleGapSeconds": max((b["elapsedSeconds"]-a["elapsedSeconds"] for a,b in zip(rows,rows[1:])), default=0),
            "medianAggregateRssBytes": statistics.median(rss) if rss else None,
            "cpuSecondsObservedDelta": sum(max(values) - min(values) for values in cpu.values()),
            "peakProcesses": max((len(row["processes"]) for row in rows), default=0),
            "peakOsThreads": max((sum(p.get("threads") or 0 for p in row["processes"]) for row in rows), default=0),
            "peakNativeWatches": max((sum(p["nativeWatches"] or 0 for p in row["processes"]) for row in rows), default=0)
            if any(p.get("nativeWatches") is not None for row in rows for p in row["processes"]) else None}


def worker_summary(start, end, rows):
    # PID membership from resource samples excludes workers whose process has
    # already exited without emitting a per-thread exit event.
    pids = {p["pid"] for row in rows for p in row["processes"]}
    active = set()
    query = set()
    peak = 0
    at_start = 0
    query_peak = 0
    query_start = 0
    for record in workers:
        if record["atEpochMs"] > end:
            break
        if record["pid"] not in pids:
            continue
        key = (record["pid"], record["threadId"])
        if record["type"] == "spawn":
            active.add(key)
            if "query-worker.js" in record["file"]:
                query.add(key)
        elif record["type"] == "exit":
            active.discard(key)
            query.discard(key)
        if record["atEpochMs"] < start:
            at_start = len(active)
            query_start = len(query)
        else:
            peak = max(peak, len(active))
            query_peak = max(query_peak, len(query))
    return {"workersAtStart": at_start, "peakWorkers": max(peak, at_start), "workersAtEnd": len(active),
            "queryWorkersAtStart": query_start, "peakQueryWorkers": max(query_peak, query_start), "queryWorkersAtEnd": len(query)}


phases = {}
for start in [row for row in markers if row["type"] == "start"]:
    end = next(row for row in markers if row["phase"] == start["phase"] and row["type"] == "complete")
    rows = samples[start["resourceSampleIndex"]:end["resourceSampleIndex"]]
    assert rows, "declared phase has no samples"
    phases[start["phase"]] = {"seconds": (end["atEpochMs"] - start["atEpochMs"]) / 1000,
                              **resource_summary(rows), **worker_summary(start["atEpochMs"], end["atEpochMs"], rows)}
idle_start, idle_end = read("idle-start.json"), read("idle-end.json")
idle = samples[idle_start["resourceSampleIndex"]:idle_end["resourceSampleIndex"]]
assertion(scenario, "declared 45 second idle window has complete resource observation", len(idle) >= 10 and idle_end["atEpochMs"] - idle_start["atEpochMs"] >= 45000 and resource_summary(idle)["maxSampleGapSeconds"] <= 5)
tail = [row for row in idle if row["elapsedSeconds"] >= idle[-1]["elapsedSeconds"] - 5]
summary = {"protocol": "two-project-flow-v1", "baselineAttempt": checked["attempt"], "timings": read("timings.json"),
           "wholeAttempt": resource_summary(samples), "phases": phases,
           "idle45Seconds": {**resource_summary(idle), **worker_summary(idle_start["atEpochMs"], idle_end["atEpochMs"], idle)},
           "idleFinal5Seconds": resource_summary(tail),
           "limits": ["RSS sums sampled owned process working sets and can double-count shared pages.",
                      "CPU is cumulative per-process delta within each sample window, excluding work before the first sample.",
                      "Worker counts observe public Node Worker construction, not OS threads; process membership limits stale exit records.",
                      "Windows native inotify watch counts are not applicable.",
                      "The 0.25 second declaration is the minimum poll pause; file binding and OS sampling add overhead. Actual maximum sample gaps are reported.",
                      "This small fixture baseline establishes no candidate or installed acceptance."]}
write(out / "measurements.json", summary)
assertion(scenario, "all five declared phases interpreted", len(phases) == 5)
event("complete", scenario=scenario)
event("final", passed=True, scenarios=[scenario])
