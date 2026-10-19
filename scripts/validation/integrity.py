"""Native source-change witnesses; a transient write still invalidates evidence."""
import ctypes
import os
from pathlib import Path
import struct
import stat
import json
import threading
import time

if os.name == "nt":
    from windows_witness import Guard, ordinary

IGNORED = {".git", ".validation", ".tmp", "node_modules", "dist", ".svelte-kit", "__pycache__"}


def relevant(relative):
    return not any(part in IGNORED for part in Path(relative).parts)


def reparse_record(path, info):
    """Read the actual link object, never its target, into one bounded record."""
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    kernel.CreateFileW.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32,
                                  ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p]
    kernel.CreateFileW.restype = ctypes.c_void_p
    kernel.DeviceIoControl.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_uint32,
                                      ctypes.c_void_p, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_void_p]
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    handle = kernel.CreateFileW(str(path), 0x80, 7, None, 3, 0x02200000, None)
    if handle in (None, ctypes.c_void_p(-1).value):
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        buffer, count = ctypes.create_string_buffer(16384), ctypes.c_uint32()
        if not kernel.DeviceIoControl(handle, 0x900A8, None, 0, buffer, len(buffer), ctypes.byref(count), None):
            raise ctypes.WinError(ctypes.get_last_error())
        raw = buffer.raw[:count.value]
        if not 16 <= len(raw) <= 16384:
            raise ValueError("Unsupported reparse record length: " + str(path))
        tag, length, reserved = struct.unpack_from("<IHH", raw)
        if tag not in (0xA0000003, 0xA000000C) or tag != info.st_reparse_tag or length + 8 != len(raw) or reserved:
            raise ValueError("Unknown/inconsistent reparse tag or data: " + str(path))
        base = 20 if tag == 0xA000000C else 16
        if len(raw) < base:
            raise ValueError("Truncated reparse record: " + str(path))
        suboff, sublen, printoff, printlen = struct.unpack_from("<HHHH", raw, 8)
        flags = struct.unpack_from("<I", raw, 16)[0] if tag == 0xA000000C else 0
        if flags not in (0, 1):
            raise ValueError("Unknown symlink flags: " + str(path))
        names = []
        for offset, size in ((suboff, sublen), (printoff, printlen)):
            if offset % 2 or size % 2 or base + offset + size > len(raw):
                raise ValueError("Invalid reparse name range: " + str(path))
            names.append(raw[base + offset:base + offset + size].decode("utf-16-le", errors="strict"))
        literal = names[0]
        if not literal or "\0" in literal:
            raise ValueError("Invalid reparse target name: " + str(path))
        if literal.startswith(("\\??\\", "\\\\?\\")):
            literal = literal[4:]
        target = Path(literal)
        if flags == 1:
            if target.drive or target.is_absolute():
                raise ValueError("Inconsistent relative symlink: " + str(path))
            target = path.parent / target
        # Do not resolve/collapse a route through an intermediate link. The
        # ordinary predicate checks every literal component and refuses '..'.
        target = ordinary(target)
        return {"tag": tag, "attributes": info.st_file_attributes, "dataHex": raw.hex(),
                "substituteName": names[0], "printName": names[1], "flags": flags,
                "target": str(target)}
    finally:
        if not kernel.CloseHandle(handle):
            raise ctypes.WinError(ctypes.get_last_error())


def windows_input_closure(root, artifacts, *, source=True):
    """Nofollow membership/link map; explicit strict declarations admit targets.

    No implicit target traversal or source exclusion can weaken an artifact.
    Returned targets are also actual Guard declarations, retaining their route.
    """
    root = ordinary(Path(root).absolute(), True)
    declared = set()
    for index, item in enumerate(artifacts):
        if index >= 4096:
            raise ValueError("Windows input declarations exceed4096")
        declared.add(ordinary(Path(item).absolute()))
    declared = tuple(sorted(declared))
    files = {"source": set(), "artifact": set()}
    members = {"source": {}, "artifact": {}}
    links, targets, total, budget = {}, set(), 0, 0
    for domain, roots in (("source", [root] if source else []), ("artifact", declared)):
        for base in roots:
            stack = []
            def push(path):
                nonlocal total, budget
                if domain == "source" and path != root and not relevant(path.relative_to(root)):
                    return
                total += 1
                budget += len(json.dumps(str(path), ensure_ascii=True)) + 256
                if total > 262144 or budget > 64 * 1024 * 1024 or len(str(path)) > 4096:
                    raise ValueError("Windows nofollow input inventory exceeds finite member/path/64MiB bounds")
                stack.append(path)
            push(base)
            while stack:
                path = stack.pop()
                if str(path) in members[domain]:
                    continue
                info = path.lstat()  # Every child is classified before any descent/read.
                if info.st_file_attributes & 0x400:
                    if len(links) >= 4096:
                        raise ValueError("Windows reparse input count exceeds4096")
                    row = reparse_record(path, info)
                    covered = [p for p in declared if Path(row["target"]) == p
                               or (p.is_dir() and Path(row["target"]).is_relative_to(p))]
                    if not covered:
                        raise ValueError("Undeclared reparse target requires explicit strict artifact: " + str(path))
                    row["coveredBy"] = str(max(covered, key=lambda p: len(p.parts)))
                    encoded = json.dumps(row, ensure_ascii=True, separators=(",", ":"))
                    budget += len(encoded)
                    if budget > 64 * 1024 * 1024:
                        raise ValueError("Windows nofollow input record budget exceeds64MiB")
                    links[str(path)] = row
                    targets.add(Path(row["target"]))
                    members[domain][str(path)] = "reparse"
                elif stat.S_ISDIR(info.st_mode):
                    members[domain][str(path)] = "directory"
                    with os.scandir(path) as children:
                        for child in children:
                            push(Path(child.path))
                elif stat.S_ISREG(info.st_mode):
                    members[domain][str(path)] = "file"
                    files[domain].add(path)
                else:
                    raise ValueError("Unsupported nofollow input member: " + str(path))
    return {"files": files, "membership": members, "reparse": links, "targets": tuple(sorted(targets))}


class _LegacyWitness:
    """Previous Linux inotify behavior; Windows uses the shared Guard only."""
    def __init__(self, root, paths, artifact=False, only=None):
        self.root = Path(root).resolve()
        self.artifact = artifact
        self.only = only
        self.changes = []
        self.errors = []
        self.stopping = False
        self.handles = []
        self.threads = []
        directories = {self.root: bool(artifact and only is None)}
        if not artifact:
            directories.update({p: True for p in self.root.iterdir() if p.is_dir() and relevant(p.name)})
        for path in paths:
            path = Path(path).resolve()
            if path.is_relative_to(self.root):
                relative = path.relative_to(self.root)
                if len(relative.parts) > 1:
                    top = self.root / relative.parts[0]
                    if top.is_dir():
                        directories[top] = True
        if os.sys.platform == "linux":
            self.libc = ctypes.CDLL(None, use_errno=True)
            self.libc.inotify_init1.argtypes = [ctypes.c_int]
            self.libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
            self.fd = self.libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
            if self.fd < 0:
                raise OSError(ctypes.get_errno(), "inotify_init1")
            self.watches = {}
            linux_directories = set(directories)
            for directory, recursive in directories.items():
                if recursive:
                    linux_directories.update(p for p in directory.rglob("*") if p.is_dir()
                                             and (artifact or relevant(p.relative_to(self.root))))
            for directory in linux_directories:
                wd = self.libc.inotify_add_watch(self.fd, os.fsencode(directory), 0x00000FCE)
                if wd < 0:
                    self.errors.append(f"cannot witness source directory: {directory}")
                self.watches[wd] = directory
            thread = threading.Thread(target=self.watch_linux, daemon=True)
            self.threads.append(thread)
            thread.start()
        else:
            raise RuntimeError("native source integrity witness requires Windows or Linux")

    def note(self, directory, name):
        relative = (directory / name).relative_to(self.root)
        allowed = ("__pycache__" not in relative.parts) if self.artifact else relevant(relative)
        if allowed and (self.only is None or str(relative) == self.only):
            self.changes.append(str(relative))

    def watch_linux(self):
        import time
        try:
            while not self.stopping:
                try:
                    data = os.read(self.fd, 65536)
                except BlockingIOError:
                    time.sleep(0.01)
                    continue
                offset = 0
                while offset < len(data):
                    wd, mask, cookie, length = struct.unpack_from("iIII", data, offset)
                    if mask & 0x4000:
                        self.errors.append("source witness overflow")
                    elif wd in self.watches:
                        name = os.fsdecode(data[offset + 16:offset + 16 + length].split(b"\0", 1)[0])
                        self.note(self.watches[wd], name)
                    offset += 16 + length
        except BaseException as exc:
            if not self.stopping:
                self.errors.append(f"source witness error: {exc}")

    def close(self):
        self.stopping = True

        for thread in self.threads:
            thread.join(timeout=2)
            if thread.is_alive():
                self.errors.append("source witness did not release handle")
        os.close(self.fd)


class WindowsInputs:
    """One bounded Windows Guard for a source policy and strict artifact union.

    Construction only selects inputs. Call start/check before rereading a full
    baseline and releasing a target; close only after the last protected read.
    """

    def __init__(self, root, artifacts, *, source=True, absent_roots=()):
        self.root = Path(root).absolute()
        artifacts = tuple(artifacts)
        closure = windows_input_closure(self.root, artifacts, source=source)
        self.guard = Guard([*artifacts, *closure["targets"]], source_root=self.root if source else None,
                           source_skip=IGNORED if source else (), absent_roots=absent_roots)
        self._closed = False
        self._absent_final = None

    def start(self, deadline):
        return self.guard.start(deadline)

    def check(self):
        return self.guard.check()

    def snapshot(self):
        return self.guard.snapshot()

    @property
    def changes(self):
        return [str(Path(row["path"]).relative_to(self.root)) if Path(row["path"]).is_relative_to(self.root)
                else row["path"] for row in self.snapshot()["events"]]

    @property
    def errors(self):
        state = self.snapshot()
        errors = [row["code"] + ": " + row["message"] for row in state["errors"]]
        if self._closed and state.get("observationComplete") is not True:
            errors.append("Windows immutable-input observation did not close cleanly")
        return errors

    def close(self, deadline=None):
        if deadline is None:
            deadline = time.monotonic() + 8
        # Keep the terminal absence read under the same native observation as
        # all final source/artifact reads. Creation+removal is still an event.
        self._absent_final = {str(p): not os.path.lexists(p) for p in sorted(self.guard.absent)}
        ledger = self.guard.close(deadline)
        self._closed = True
        return ledger.as_dict()

    def row(self):
        row = {"root": str(self.root), "changes": self.changes, "errors": self.errors,
                "windowsSelection": {
                    "watchRoots": [str(p) for p in self.guard.watch_roots],
                    "namespaceWatchRoots": [str(p) for p in self.guard.namespace_watch_roots],
                    "namespacePaths": [str(p) for p in self.guard.namespace_paths]},
                "windowsWitness": self.snapshot()}
        if self.guard.absent:
            row["windowsSelection"]["absentRoots"] = [str(p) for p in sorted(self.guard.absent)]
            row["absentFinal"] = self._absent_final
        return row


def Witness(root, paths, artifact=False, only=None):
    """Compatibility seam for a single copied-artifact/source observer.

    The runner uses WindowsInputs directly so all its inputs share one startup
    and close budget. Existing single-Witness callers retain their old surface.
    """
    if os.name != "nt":
        return _LegacyWitness(root, paths, artifact=artifact, only=only)
    base = Path(root).absolute()
    immutable = [base / only if only is not None else base] if artifact else []
    witness = WindowsInputs(base, immutable, source=not artifact)
    witness.start(time.monotonic() + 30)
    return witness
