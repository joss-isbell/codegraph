"""Native source-change witnesses; a transient write still invalidates evidence."""
import ctypes
import os
from pathlib import Path
import struct
import threading

IGNORED = {".git", ".validation", ".tmp", "node_modules", "dist", ".svelte-kit", "__pycache__"}


def relevant(relative):
    return not any(part in IGNORED for part in Path(relative).parts)


class Witness:
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
        if os.name == "nt":
            from ctypes import wintypes as w
            self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            self.kernel.CreateFileW.argtypes = [w.LPCWSTR, w.DWORD, w.DWORD, ctypes.c_void_p, w.DWORD, w.DWORD, w.HANDLE]
            self.kernel.CreateFileW.restype = w.HANDLE
            self.kernel.ReadDirectoryChangesW.argtypes = [w.HANDLE, ctypes.c_void_p, w.DWORD, w.BOOL, w.DWORD,
                                                         ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
            self.kernel.CancelIoEx.argtypes = [w.HANDLE, ctypes.c_void_p]
            self.kernel.CloseHandle.argtypes = [w.HANDLE]
            for directory, recursive in sorted(directories.items()):
                handle = self.kernel.CreateFileW(str(directory), 1, 7, None, 3, 0x02000000, None)
                if handle == ctypes.c_void_p(-1).value:
                    self.errors.append(f"cannot witness source directory: {directory}")
                    continue
                ready = threading.Event()
                thread = threading.Thread(target=self.watch_windows, args=(handle, directory, recursive, ready), daemon=True)
                self.handles.append(handle)
                self.threads.append(thread)
                thread.start()
                ready.wait(2)
        elif os.sys.platform == "linux":
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

    def watch_windows(self, handle, directory, recursive, ready):
        try:
            while not self.stopping:
                buffer = ctypes.create_string_buffer(65536)
                count = ctypes.c_uint32()
                ready.set()
                ok = self.kernel.ReadDirectoryChangesW(handle, buffer, len(buffer), recursive, 0x1F,
                                                      ctypes.byref(count), None, None)
                if not ok:
                    if not self.stopping:
                        self.errors.append(f"source witness failed: {ctypes.get_last_error()}")
                    break
                if count.value == 0:
                    self.errors.append("source witness overflow")
                    continue
                offset = 0
                while offset < count.value:
                    following, action, length = struct.unpack_from("III", buffer, offset)
                    name = buffer.raw[offset + 12:offset + 12 + length].decode("utf-16-le")
                    # Windows can report a parent's LastWriteTime when an
                    # ignored generated child directory changes. Existing
                    # directories have their own recursive child witnesses;
                    # keep additions/deletions/renames and all file writes.
                    if action != 3 or not (directory / name).is_dir():
                        self.note(directory, name)
                    if not following:
                        break
                    offset += following
        except BaseException as exc:
            self.errors.append(f"source witness error: {exc}")

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
        if os.name == "nt":
            for handle in self.handles:
                self.kernel.CancelIoEx(handle, None)
        for thread in self.threads:
            thread.join(timeout=2)
            if thread.is_alive():
                self.errors.append("source witness did not release handle")
        if os.name == "nt":
            for handle in self.handles:
                self.kernel.CloseHandle(handle)
        else:
            os.close(self.fd)
