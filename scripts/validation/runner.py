"""Dependency-free, fail-closed supervisor. Run: python runner.py PLAN.json.

Windows descendants inherit a kill-on-close Job Object. A gated launcher cannot
start the target until assignment succeeds. No PID from a registry/service scan
is ever selected for termination. Linux uses kernel ancestry, child subreaping
and PID-bound signaling; lifecycle containers must also run with --init.
"""
import ctypes
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import signal
import shutil
import subprocess
import sys
import threading
import time
import traceback
import uuid

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from report import consume, digest
from integrity import Witness, relevant


def write(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], stderr=subprocess.DEVNULL)


def binding(root, artifacts):
    root = Path(root).resolve()
    paths = set()
    try:
        if Path(git(root, "rev-parse", "--show-toplevel").decode().strip()).resolve() != root:
            raise subprocess.CalledProcessError(1, "git root identity")
        tracked = git(root, "ls-files", "-z").decode().split("\0")
        untracked = git(root, "ls-files", "--others", "--exclude-standard", "-z").decode().split("\0")
        paths.update(root / p for p in tracked + untracked if p and relevant(p))
        commit = git(root, "rev-parse", "HEAD").decode().strip()
        delta = git(root, "diff", "--binary", "HEAD")
    except subprocess.CalledProcessError:
        commit = (root / "baseline-source-commit.txt").read_text().strip()
        delta = b""
        paths.update(p for p in root.rglob("*") if p.is_file() and relevant(p.relative_to(root)))
    for directory in ["src", "ui/src", "scripts/validation"]:
        base = root / directory
        if base.exists():
            paths.update(p for p in base.rglob("*") if p.is_file() and "__pycache__" not in p.parts)
    paths.update(root / p for p in ["package.json", "package-lock.json", "ui/package.json"])
    sources = {str(p.relative_to(root)).replace("\\", "/"): digest(p) if p.is_file() else None
               for p in sorted(paths)}
    artifact_files = {}
    for item in artifacts:
        p = Path(item).resolve()
        if p.is_dir():
            artifact_files.update({str(f): digest(f) for f in sorted(p.rglob("*"))
                                   if f.is_file() and "__pycache__" not in f.parts})
        else:
            artifact_files[str(p)] = digest(p) if p.is_file() else None
    return {"commit": commit, "dirtyDeltaSha256": hashlib.sha256(delta).hexdigest(),
            "sources": sources, "artifacts": artifact_files}


if os.name == "nt":
    from ctypes import wintypes as w
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)

    class Limits(ctypes.Structure):
        _fields_ = [("PerProcessUserTimeLimit", ctypes.c_int64), ("PerJobUserTimeLimit", ctypes.c_int64),
                    ("LimitFlags", w.DWORD), ("MinimumWorkingSetSize", ctypes.c_size_t),
                    ("MaximumWorkingSetSize", ctypes.c_size_t), ("ActiveProcessLimit", w.DWORD),
                    ("Affinity", ctypes.c_size_t), ("PriorityClass", w.DWORD), ("SchedulingClass", w.DWORD)]

    class Extended(ctypes.Structure):
        _fields_ = [("BasicLimitInformation", Limits), ("IoInfo", ctypes.c_uint64 * 6),
                    ("ProcessMemoryLimit", ctypes.c_size_t), ("JobMemoryLimit", ctypes.c_size_t),
                    ("PeakProcessMemoryUsed", ctypes.c_size_t), ("PeakJobMemoryUsed", ctypes.c_size_t)]

    class Memory(ctypes.Structure):
        _fields_ = [("cb", w.DWORD), ("PageFaultCount", w.DWORD)] + [
            (name, ctypes.c_size_t) for name in ["PeakWorkingSetSize", "WorkingSetSize", "QuotaPeakPagedPoolUsage",
            "QuotaPagedPoolUsage", "QuotaPeakNonPagedPoolUsage", "QuotaNonPagedPoolUsage", "PagefileUsage", "PeakPagefileUsage"]]

    class ThreadEntry(ctypes.Structure):
        _fields_ = [("dwSize", w.DWORD), ("cntUsage", w.DWORD), ("th32ThreadID", w.DWORD),
                    ("th32OwnerProcessID", w.DWORD), ("tpBasePri", w.LONG), ("tpDeltaPri", w.LONG), ("dwFlags", w.DWORD)]

    kernel.CreateJobObjectW.argtypes = [ctypes.c_void_p, w.LPCWSTR]
    kernel.CreateJobObjectW.restype = w.HANDLE
    kernel.SetInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD]
    kernel.AssignProcessToJobObject.argtypes = [w.HANDLE, w.HANDLE]
    kernel.QueryInformationJobObject.argtypes = [w.HANDLE, ctypes.c_int, ctypes.c_void_p, w.DWORD, ctypes.c_void_p]
    kernel.TerminateJobObject.argtypes = [w.HANDLE, w.UINT]
    kernel.CloseHandle.argtypes = [w.HANDLE]
    kernel.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
    kernel.OpenProcess.restype = w.HANDLE
    kernel.GetProcessTimes.argtypes = [w.HANDLE] + [ctypes.c_void_p] * 4
    psapi.GetProcessMemoryInfo.argtypes = [w.HANDLE, ctypes.c_void_p, w.DWORD]
    kernel.CreateToolhelp32Snapshot.argtypes = [w.DWORD, w.DWORD]
    kernel.CreateToolhelp32Snapshot.restype = w.HANDLE
    kernel.Thread32First.argtypes = [w.HANDLE, ctypes.c_void_p]
    kernel.Thread32Next.argtypes = [w.HANDLE, ctypes.c_void_p]


def process_sample(pid):
    if os.name == "nt":
        handle = kernel.OpenProcess(0x1000 | 0x10, False, pid)
        if not handle:
            return None
        try:
            creation, exited, kt, ut = (ctypes.c_uint64() for _ in range(4))
            if not kernel.GetProcessTimes(handle, ctypes.byref(creation), ctypes.byref(exited), ctypes.byref(kt), ctypes.byref(ut)):
                return None
            memory = Memory()
            memory.cb = ctypes.sizeof(memory)
            ok = psapi.GetProcessMemoryInfo(handle, ctypes.byref(memory), memory.cb)
            return {"pid": pid, "creationIdentity": str(creation.value), "cpuSeconds": (kt.value + ut.value) / 1e7,
                    "rssBytes": memory.WorkingSetSize if ok else None,
                    "peakRssBytes": memory.PeakWorkingSetSize if ok else None, "nativeWatches": None}
        finally:
            kernel.CloseHandle(handle)
    try:
        stat = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        if stat[0] == "Z":
            return None
        watches = 0
        try:
            for f in Path(f"/proc/{pid}/fdinfo").iterdir():
                try:
                    watches += sum(line.startswith("inotify ") for line in f.read_text().splitlines())
                except OSError:
                    watches = None
                    break
        except OSError:
            watches = None
        return {"pid": pid, "creationIdentity": stat[19], "cpuSeconds": (int(stat[11]) + int(stat[12])) / os.sysconf("SC_CLK_TCK"),
                "rssBytes": int(stat[21]) * os.sysconf("SC_PAGE_SIZE"), "threads": int(stat[17]), "nativeWatches": watches}
    except (FileNotFoundError, ProcessLookupError):
        return None


def thread_counts():
    if os.name != "nt":
        return {}
    snapshot = kernel.CreateToolhelp32Snapshot(4, 0)
    counts = {}
    entry = ThreadEntry()
    entry.dwSize = ctypes.sizeof(entry)
    try:
        ok = kernel.Thread32First(snapshot, ctypes.byref(entry))
        while ok:
            pid = entry.th32OwnerProcessID
            counts[pid] = counts.get(pid, 0) + 1
            ok = kernel.Thread32Next(snapshot, ctypes.byref(entry))
    finally:
        kernel.CloseHandle(snapshot)
    return counts


class Ownership:
    def __init__(self):
        self.handle = None
        self.gate = None
        self.identities = {}
        self.token = str(uuid.uuid4())
        if os.name == "nt":
            self.handle = kernel.CreateJobObjectW(None, None)
            if not self.handle:
                raise ctypes.WinError(ctypes.get_last_error())
            limits = Extended()
            limits.BasicLimitInformation.LimitFlags = 0x2000  # KILL_ON_JOB_CLOSE; no breakaway.
            if not kernel.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)):
                raise ctypes.WinError(ctypes.get_last_error())
        elif sys.platform == "linux":
            self.libc = ctypes.CDLL(None, use_errno=True)
            if self.libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
                raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")
        else:
            raise RuntimeError("process containment requires Windows Job Objects or Linux subreaper")

    def launch(self, argv, cwd, env):
        # Gate starts no descendants before the supervisor owns it.
        env = {**env, "VALIDATION_OWNER_UUID": self.token}
        self.gate = subprocess.Popen([sys.executable, str(HERE / "runner.py"), "_gate"], cwd=cwd, env=env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
            start_new_session=os.name != "nt")
        if os.name == "nt" and not kernel.AssignProcessToJobObject(self.handle, int(self.gate._handle)):
            self.gate.kill()
            self.gate.wait()
            raise ctypes.WinError(ctypes.get_last_error())
        self.gate.stdin.write((json.dumps(argv) + "\n").encode())
        self.gate.stdin.close()
        return self.gate

    def pids(self):
        if self.gate is None:
            return []
        if os.name == "nt":
            data = ctypes.create_string_buffer(65536)
            if not kernel.QueryInformationJobObject(self.handle, 3, data, len(data), None):
                raise ctypes.WinError(ctypes.get_last_error())
            count = ctypes.c_uint32.from_buffer(data, 4).value
            if count > (len(data) - 8) // ctypes.sizeof(ctypes.c_size_t):
                raise RuntimeError("owned process inventory overflow")
            return list((ctypes.c_size_t * count).from_buffer(data, 8))
        parents = {}
        zombies = set()
        for path in Path("/proc").iterdir():
            if path.name.isdigit():
                try:
                    stat = (path / "stat").read_text().rsplit(")", 1)[1].split()
                    parents[int(path.name)] = int(stat[1])
                    if stat[0] == "Z":
                        zombies.add(int(path.name))
                except (FileNotFoundError, ProcessLookupError):
                    pass
        # Subreaping takes orphaned daemon exit ownership away from container
        # PID 1. Reap those adopted zombies promptly so a live target can observe
        # daemon exit before reopening. Popen alone owns the gated child's status.
        for pid in zombies:
            if parents.get(pid) == os.getpid() and pid != self.gate.pid:
                try:
                    os.waitpid(pid, os.WNOHANG)
                except (ChildProcessError, ProcessLookupError):
                    pass
        owned = {os.getpid()}
        while True:
            children = {pid for pid, parent in parents.items() if parent in owned}
            added = children - owned
            if not added:
                break
            owned.update(added)
        # Kernel subreaping keeps double-forked/setsid/env-clearing children in
        # this supervisor's ancestry even after their launchers have exited.
        return [pid for pid in owned if pid != os.getpid() and pid not in zombies]

    def sample(self):
        threads = thread_counts()
        result = []
        for pid in self.pids():
            row = process_sample(pid)
            if row:
                if os.name == "nt":
                    row["threads"] = threads.get(pid)
                self.identities[(pid, row["creationIdentity"])] = row
                result.append(row)
        return result

    def cleanup(self, bounds):
        start = time.monotonic()
        survivors = self.pids()
        self.sample()
        # Allow workers to finish startup before intentional whole-process cleanup.
        # Never invokes worker.terminate(); existing worker handshake code is unchanged.
        wait = bounds["startupHandshakeSeconds"] + bounds["drainSeconds"]
        while survivors and time.monotonic() - start < wait:
            time.sleep(0.1)
            self.sample()
            survivors = self.pids()
        killed = survivors
        if killed:
            if os.name == "nt":
                if not kernel.TerminateJobObject(self.handle, 124):
                    raise ctypes.WinError(ctypes.get_last_error())
            else:
                for pid in survivors:
                    try:
                        fd = os.pidfd_open(pid)
                    except ProcessLookupError:
                        continue
                    try:
                        current = process_sample(pid)
                        if current and (pid, current["creationIdentity"]) not in self.identities:
                            raise RuntimeError(f"owned PID creation identity changed before cleanup: {pid}")
                        if current:
                            signal.pidfd_send_signal(fd, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    finally:
                        os.close(fd)
        if self.gate:
            self.gate.wait(timeout=bounds["cleanupSeconds"])
        if os.name != "nt":
            while True:
                try:
                    pid, _ = os.waitpid(-1, os.WNOHANG)
                    if pid == 0:
                        break
                except ChildProcessError:
                    break
        deadline = time.monotonic() + bounds["cleanupSeconds"]
        while self.pids() and time.monotonic() < deadline:
            if os.name != "nt":
                try:
                    while os.waitpid(-1, os.WNOHANG)[0]:
                        pass
                except ChildProcessError:
                    pass
            time.sleep(0.05)
        remaining_pids = self.pids()
        remaining = self.sample()
        complete = not remaining_pids
        if self.handle:
            kernel.CloseHandle(self.handle)
            self.handle = None
        return {"complete": complete, "remaining": remaining, "remainingPids": remaining_pids, "intentionallyStoppedOwnedPids": killed,
                "observedIdentities": list(self.identities.values()), "elapsedSeconds": time.monotonic() - start}


class Log:
    def __init__(self, pipe, path, limit, excerpt):
        self.total = 0
        self.overflow = False
        self.tail = b""
        self.path = path
        self.errors = []
        self.thread = threading.Thread(target=self.drain, args=(pipe, limit, excerpt), daemon=True)
        self.thread.start()

    def drain(self, pipe, limit, excerpt):
        try:
            with open(self.path, "wb") as output:
                while True:
                    data = pipe.read(4096)
                    if not data:
                        break
                    remaining = max(0, limit - self.total)
                    output.write(data[:remaining])
                    output.flush()
                    self.total += len(data)
                    self.overflow = self.total > limit
                    self.tail = (self.tail + data)[-excerpt:]
        except BaseException as exc:
            self.errors.append(f"log capture failed: {type(exc).__name__}: {exc}")
        finally:
            pipe.close()


def run(plan_path):
    plan_path = Path(plan_path).resolve()
    plan = json.loads(plan_path.read_text(encoding="utf-8-sig"))
    declarations = json.loads((HERE / "declarations-v1.json").read_text())
    bounds = {**declarations, **plan.get("bounds", {})}
    seconds = plan["deadlineSeconds"]
    if isinstance(seconds, bool) or not isinstance(seconds, (int, float)) or not math.isfinite(seconds) or seconds <= 0 or seconds > 86400:
        raise ValueError("declare a positive suite deadline of at most 24h before launch")
    for name in ["startupHandshakeSeconds", "drainSeconds", "cleanupSeconds", "sampleSeconds", "logBytesPerStream", "excerptBytes"]:
        value = bounds[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise ValueError(f"declare positive finite {name} before launch")
    if bounds["sampleSeconds"] > 5 or bounds["cleanupSeconds"] > 60 or bounds["startupHandshakeSeconds"] + bounds["drainSeconds"] > 120:
        raise ValueError("supervision sampling/cleanup bounds exceed supported maximum")
    if any(not isinstance(bounds[name], int) or bounds[name] > 100 * 1024 * 1024 for name in ["logBytesPerStream", "excerptBytes"]):
        raise ValueError("log and excerpt bounds must be integer bytes no larger than 100 MiB")
    scenarios = plan["scenarios"]
    if not isinstance(scenarios, list) or not scenarios or len(set(scenarios)) != len(scenarios):
        raise ValueError("declare nonempty unique scenario array before launch")
    out = Path(plan["output"]).resolve()
    out.mkdir(parents=True, exist_ok=False)
    write(out / "plan.json", plan)
    write(out / "declarations.json", bounds)
    home = out / "home"
    home.mkdir()
    allowed = {"SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP", "PATHEXT", "NUMBER_OF_PROCESSORS",
               "PROCESSOR_ARCHITECTURE", "OS", "LANG", "LC_ALL", "LANGUAGE", "TZ", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"}
    env = {k: v for k, v in os.environ.items() if k.upper() in allowed}
    # Keep nested npm/cmd invocations below Windows' 8191-character command
    # environment limit and exclude installed CodeGraph launchers from lookup.
    command_dirs = [str(Path(sys.executable).parent)]
    command_dirs += [str(Path(found).parent) for name in ["git", "docker", "node", "sh"] if (found := shutil.which(name))]
    if os.name == "nt":
        command_dirs += [str(Path(os.environ["SystemRoot"]) / "System32"), os.environ["SystemRoot"]]
    else:
        command_dirs += ["/usr/bin", "/bin"]
    env["PATH"] = os.pathsep.join(dict.fromkeys(command_dirs))
    env.update({"HOME": str(home), "USERPROFILE": str(home), "XDG_CONFIG_HOME": str(home / "config"),
                "GIT_CONFIG_GLOBAL": str(home / "gitconfig"), "APPDATA": str(home / "appdata"),
                "LOCALAPPDATA": str(home / "local"), "CODEGRAPH_DIR": ".codegraph",
                "VALIDATION_OUT": str(out), "VALIDATION_EVENTS": str(out / "events.jsonl")})
    if os.name == "nt":
        env.update(HOMEDRIVE=home.drive, HOMEPATH=str(home)[len(home.drive):])
    env.update(plan.get("environment", {}))
    source = binding(plan["sourceRoot"], plan.get("artifacts", []) + [sys.executable, str(HERE)])
    write(out / "source-before.json", source)
    try:
        delta = git(plan["sourceRoot"], "diff", "--binary", "HEAD")
    except subprocess.CalledProcessError:
        delta = b""
    (out / "dirty-before.patch").write_bytes(delta)
    started = time.monotonic()
    bound_paths = [Path(plan["sourceRoot"]) / p for p in source["sources"]] + [Path(p) for p in source["artifacts"]]
    def signatures():
        return {str(p): (p.stat().st_size, p.stat().st_mtime_ns) if p.is_file() else None for p in bound_paths}
    initial_signatures = signatures()
    changed_during_run = False
    witness = Witness(plan["sourceRoot"], bound_paths)
    witnesses = [witness]
    for item in plan.get("artifacts", []) + [sys.executable, str(HERE)]:
        p = Path(item).resolve()
        if p.is_dir():
            witnesses.append(Witness(p, [], artifact=True))
        elif p.is_file():
            witnesses.append(Witness(p.parent, [], artifact=True, only=p.name))
        else:
            witness.errors.append(f"declared artifact is missing: {p}")
    receipt = {"schema": "codegraph-validation-v1", "attempt": out.name, "outcome": "incomplete",
               "runtime": {"python": sys.version, "executableSha256": digest(sys.executable),
                           "os": platform.platform(), "machine": platform.machine()},
               "settings": {k: v for k, v in env.items() if k.startswith("CODEGRAPH_")},
               "executionMode": plan.get("executionMode", "supervised"), "scenarios": scenarios,
               "binding": {"commit": source["commit"], "dirtyDeltaSha256": source["dirtyDeltaSha256"]},
               "exitCode": None, "cleanup": {"complete": False}, "failures": [], "events": [], "evidence": [],
               "projects": [], "sessions": []}
    write(out / "receipt.json", receipt)  # Durable interrupted attempt exists before spawn.
    owner = Ownership()
    logs = []
    interrupted = threading.Event()
    def interrupt(_sig, _frame):
        interrupted.set()
    handlers = {}
    for sig in [signal.SIGINT, signal.SIGTERM] + ([signal.SIGBREAK] if os.name == "nt" else []):
        handlers[sig] = signal.signal(sig, interrupt)
    try:
        child = owner.launch(plan["command"], plan["cwd"], env)
        receipt["rootPid"] = child.pid
        logs = [Log(pipe, out / (name + ".log"), bounds["logBytesPerStream"], bounds["excerptBytes"])
                for name, pipe in [("stdout", child.stdout), ("stderr", child.stderr)]]
        with (out / "resources.jsonl").open("w", encoding="utf-8") as resources:
            while True:
                rows = owner.sample()
                resources.write(json.dumps({"elapsedSeconds": time.monotonic() - started, "processes": rows}) + "\n")
                resources.flush()
                if signatures() != initial_signatures:
                    changed_during_run = True
                reason = ("interrupted" if interrupted.is_set() or (out / "interrupt.request").exists() else
                          "log capture failed" if any(log.errors for log in logs) else
                          "source witness failed" if any(w.errors for w in witnesses) else
                          "output overflow" if any(log.overflow for log in logs) else
                          "suite deadline expired" if time.monotonic() - started >= seconds else None)
                if reason:
                    receipt["failures"].append(reason)
                    break
                if child.poll() is not None:
                    break
                time.sleep(bounds["sampleSeconds"])
        receipt["exitCode"] = child.poll()
    except BaseException as exc:
        receipt["failures"].append(f"supervisor error: {type(exc).__name__}: {exc}")
        (out / "supervisor-error.txt").write_text(traceback.format_exc())
    finally:
        try:
            receipt["cleanup"] = owner.cleanup(bounds)
        except BaseException as exc:
            receipt["failures"].append(f"cleanup failure: {exc}")
        for sig, handler in handlers.items():
            signal.signal(sig, handler)
        for log in logs:
            log.thread.join(timeout=bounds["cleanupSeconds"])
            if log.thread.is_alive():
                receipt["failures"].append("log handle did not close")
            if log.overflow:
                receipt["failures"].append("output overflow")
            receipt["failures"].extend(log.errors)
    after = binding(plan["sourceRoot"], plan.get("artifacts", []) + [sys.executable, str(HERE)])
    write(out / "source-after.json", after)
    for w in witnesses:
        w.close()
    changes = [{"root": str(w.root), "changes": w.changes, "errors": w.errors} for w in witnesses]
    write(out / "integrity.json", changes)
    for w in witnesses:
        receipt["failures"].extend(w.errors)
    receipt["bindingStable"] = source == after and not changed_during_run and not any(w.changes or w.errors for w in witnesses)
    if not receipt["bindingStable"]:
        receipt["failures"].append("source/artifact binding changed during attempt")
    events = out / "events.jsonl"
    try:
        receipt["events"] = [json.loads(line) for line in events.read_text().splitlines()]
    except (OSError, ValueError) as exc:
        receipt["failures"].append(f"missing/malformed event evidence: {exc}")
    inventory = out / "inventory.json"
    if inventory.exists():
        try:
            produced = json.loads(inventory.read_text())
            receipt["projects"] = produced["projects"]
            receipt["sessions"] = produced["sessions"]
        except (OSError, ValueError, KeyError, TypeError) as exc:
            receipt["failures"].append(f"malformed produced inventory: {exc}")
    receipt["elapsedSeconds"] = time.monotonic() - started
    receipt["logs"] = {Path(log.path).name: {"path": str(log.path), "seenBytes": log.total,
                       "retainedBytes": Path(log.path).stat().st_size} for log in logs}
    receipt["failureExcerpt"] = "\n".join(log.tail.decode("utf-8", errors="replace") for log in logs)[-bounds["excerptBytes"]:]
    receipt["evidence"] = [{"path": str(p.relative_to(out)).replace("\\", "/"), "sha256": digest(p)}
                           for p in sorted(out.rglob("*")) if p.is_file()
                           and p not in [out / name for name in ["receipt.json", "summary.json", "interrupt.request"]]
                           and p.relative_to(out).parts[0] not in ["home", "npm-cache"] and "__pycache__" not in p.parts]
    receipt["outcome"] = "pass" if not receipt["failures"] and receipt["exitCode"] == 0 and receipt["cleanup"]["complete"] else "fail"
    write(out / "receipt.json", receipt)
    summary = consume(out / "receipt.json")
    receipt["outcome"] = summary["outcome"]
    receipt["failures"] = summary["reasons"]
    write(out / "receipt.json", receipt)
    summary = consume(out / "receipt.json")
    if summary["outcome"] == "pass":
        summary["excerpt"] = ""
    write(out / "summary.json", summary)
    print(json.dumps(summary, indent=2))
    return 0 if summary["outcome"] == "pass" else 1


if __name__ == "__main__":
    if sys.argv[1] == "_gate":
        argv = json.loads(sys.stdin.readline())
        sys.exit(subprocess.call(argv))
    sys.exit(run(sys.argv[1]))
