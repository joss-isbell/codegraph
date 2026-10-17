"""Versioned Windows immutable-input witness; source-only, not qualified.

Derived proportionally from the frozen version-three witness. This module never
validates a consumer's mutable output poststate. Only close(deadline)'s frozen
ledger can close the observation domain; live snapshots are advisory copies.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict, replace
import ctypes
import hashlib
import json
import math
import os
from pathlib import Path
import struct
import threading
import time


SCHEMA = "codegraph-windows-witness-closed-v2"
MAX_WATCHES, MAX_EVENTS, MAX_ERRORS = 256, 4096, 128
MAX_OWNED_EVENTS = 32768
MAX_LEDGER_BYTES = 64 * 1024 * 1024
MAX_INPUT_ROOTS = 4096
MAX_NAMESPACE_HANDLES = 1024
MAX_NATIVE_READERS = MAX_WATCHES + MAX_NAMESPACE_HANDLES
MAX_PATH_CHARS, MAX_CATEGORY_CHARS = 4096, 128
BUFFER_BYTES = 65536
NOTIFY_FILTER = 0x5F  # names, attributes, size, last-write AND creation; not access/security
NAMESPACE_FILTER = 0x06  # nonrecursive directory names + attributes, NOT timestamps
START_MAX_SECONDS, CLOSE_MAX_SECONDS = 30, 8
ACTIONS = {1: "added", 2: "removed", 3: "modified", 4: "renamed-old", 5: "renamed-new"}


def inside(path, root):
    return path == root or path.is_relative_to(root)


def absolute(path):
    path = Path(path)
    if (not path.is_absolute() or ".." in path.parts or len(str(path)) > MAX_PATH_CHARS
            or any(':' in part for part in path.parts[1:])
            or not (len(path.drive) == 2 and path.drive[1] == ':' and path.anchor == path.drive + '\\')):
        raise ValueError("Literal absolute non-parent path required")
    return path.absolute()


def bounded_roots(values, cap=MAX_INPUT_ROOTS):
    result = set()
    for index, value in enumerate(values):
        if index >= cap:
            raise ValueError("Declared witness input count exceeds finite capacity")
        result.add(absolute(value))
    return result


def ordinary(path, directory=None):
    path = absolute(path)
    current = path
    while True:
        if os.path.lexists(current) and current.lstat().st_file_attributes & 0x400:
            raise ValueError("Reparse route refused: " + str(current))
        if current == current.parent:
            break
        current = current.parent
    if not os.path.lexists(path) or (directory is True and not path.is_dir()) or (directory is False and not path.is_file()):
        raise ValueError("Ordinary existing input required: " + str(path))
    return path


@dataclass(frozen=True)
class EventRow:
    seq: int
    watchRoot: str
    path: str
    action: int
    actionName: str
    disposition: str
    category: str


@dataclass(frozen=True)
class ErrorRow:
    code: str
    message: str


@dataclass(frozen=True)
class WatchRow:
    root: str
    armed: bool
    joined: bool
    drained: bool
    handlesClosed: bool
    channel: str
    initialIdentity: object
    finalIdentity: object
    namespaceBound: bool


@dataclass(frozen=True)
class NativeIdentity:
    volumeSerial: str
    fileId: str
    finalPath: str
    attributes: int
    reparseTag: int
    caseSensitiveFlags: int


@dataclass(frozen=True)
class NamespaceRow:
    path: str
    initialIdentity: object
    finalIdentity: object
    identityStable: bool
    retainedWithoutDeleteShare: bool
    handleClosed: bool


@dataclass(frozen=True)
class ClosedLedger:
    schema: str
    state: str
    observationComplete: bool
    complete: bool
    startupDeadlineSatisfied: bool
    closeDeadlineSatisfied: bool
    nativeArmedStartup: bool
    joined: bool
    drained: bool
    nativeHandlesClosed: bool
    namespaceBound: bool
    namespaceHandlesClosed: bool
    events: tuple
    ownedEvents: tuple
    errors: tuple
    watches: tuple
    namespaceWatches: tuple
    namespaceHandles: tuple
    omitted: int
    ignoredOutOfClosureEvents: int
    qualificationOnly: bool
    qualificationMode: str | None

    def as_dict(self):
        # asdict recursively clones frozen tuple members; JSON callers may edit
        # their own copy without changing the authoritative closed ledger.
        return asdict(self)


class Guard:
    def __init__(self, immutable_roots, *, source_root=None, source_skip=(),
                 mutable_policy=None, absent_roots=()):
        if os.name != "nt":
            raise ValueError("Windows native witness required")
        self.immutable = tuple(sorted(bounded_roots(immutable_roots), key=str))
        self.absent = frozenset(bounded_roots(absent_roots))
        self.immutable = tuple(sorted(set(self.immutable) | self.absent, key=str))
        self.source = ordinary(source_root, True) if source_root is not None else None
        skip = set()
        for index, part in enumerate(source_skip):
            if index >= 256:
                raise ValueError("Source exclusion component count exceeds256")
            skip.add(part)
        self.skip = frozenset(skip)
        if any(not isinstance(part, str) or not part or Path(part).name != part for part in self.skip):
            raise ValueError("Source exclusions must be literal path components")
        self.policy = mutable_policy
        self._lock = threading.RLock()
        self._events, self._owned, self._errors = [], [], []
        self._omitted, self._ignored, self._seq = 0, 0, 0
        self._state, self._closed = "new", None
        self._startup_ok = False
        self._watches = []
        self._namespace_watches, self._retained = [], []
        self._native, self._start_deadline = None, None
        self._after_open = None  # set only by separately pinned private fixture adapter
        self._fault, self._event_cap, self._owned_cap = None, MAX_EVENTS, MAX_OWNED_EVENTS
        self._ledger_bytes = 1048576  # bounded header/error/small formatting reserve
        roots = list(self.immutable) + ([self.source] if self.source is not None else [])
        if mutable_policy is not None:
            roots += list(bounded_roots(mutable_policy.watch_roots, MAX_WATCHES))
        directories, declared_directories = set(), set()
        for path in roots:
            if path in self.absent:
                if os.path.lexists(path):
                    raise ValueError("Expected absent immutable input appeared")
                directories.add(ordinary(path.parent, True))
                declared_directories.add(path.parent)
                continue
            ordinary(path)
            declared_directories.add(path if path.is_dir() else path.parent)
            directories.add(path if path.is_dir() else path.parent)
            if path.is_dir():
                directories.add(ordinary(path.parent, True))
        selected = []
        for directory in sorted(directories, key=lambda p: (len(p.parts), str(p))):
            if not any(inside(directory, root) for root in selected):
                selected.append(directory)
        if not selected or len(selected) > MAX_WATCHES:
            raise ValueError("Native watch closure empty or exceeds256 directories")
        self.watch_roots = tuple(selected)
        # Keep every ordinary route component, not only the coalesced readers.
        # Open ascending: a retained parent without delete sharing cannot detach
        # while its child is opened and checked. No namespace allocation here.
        namespace = set()
        for path in declared_directories | directories:
            if not (len(path.drive) == 2 and path.drive[1] == ':' and path.anchor == path.drive + '\\'):
                raise ValueError('Only ordinary local DOS drive routes are supported')
            namespace.update([path, *path.parents])
            if len(namespace) > MAX_NAMESPACE_HANDLES:
                raise ValueError('Namespace retention exceeds1024 ordinary directories')
        self.namespace_paths = tuple(sorted(namespace, key=lambda p: (len(p.parts), str(p))))
        self.namespace_watch_roots = tuple(sorted({p.parent for p in namespace if p != p.parent}, key=str))
        if len(self.watch_roots) + len(self.namespace_watch_roots) > MAX_NATIVE_READERS:
            raise ValueError('Total reader count exceeds finite native capacity')

    def _all_watches(self):
        return self._namespace_watches + self._watches

    def _budget(self, deadline, code):
        if time.monotonic() >= deadline:
            raise TimeoutError(code + ': inclusive native operation deadline expired')

    def _retain_namespace(self, deadline):
        self._native = _Native()
        for path in self.namespace_paths:
            self._budget(deadline, 'START_DEADLINE')
            handle = self._native.open_directory(path, overlapped=False)
            row = {'path': path, 'handle': handle, 'initial': None, 'final': None,
                   'stable': False, 'closed': False}
            self._retained.append(row)  # ownership precedes every fallible query
            row['initial'] = self._native.identity(handle, path)
            self._budget(deadline, 'START_DEADLINE')

    def _retained_identity(self, path):
        return next(row['initial'] for row in self._retained if row['path'] == path)

    def _reserve_native_records(self):
        # Reserve both endpoint identities before releasing any target. Actual
        # final same-object/path rows cannot grow outside this conservative
        # escaped compact JSON +512 bytes/row envelope; unknowns fail closed.
        for row in self._retained:
            value = NamespaceRow(str(row['path']), row['initial'], row['initial'], True, True, True)
            self._ledger_bytes += record_bytes(value)
        for roots, channel in ((self.watch_roots, 'protected'), (self.namespace_watch_roots, 'namespace')):
            for path in roots:
                native = self._retained_identity(path)
                self._ledger_bytes += record_bytes(WatchRow(str(path), True, True, True, True,
                                                            channel, native, native, True))
        if self._ledger_bytes > MAX_LEDGER_BYTES:
            raise ValueError('Reserved native record domain exceeds64MiB ledger budget')

    def _verify_namespace(self, deadline):
        for row in self._retained:
            self._budget(deadline, 'NAMESPACE_DEADLINE')
            current = self._native.identity(row['handle'], row['path'])
            if not same_object(current, row['initial']):
                raise ValueError('Retained namespace native identity changed: ' + str(row['path']))
            row['final'], row['stable'] = current, True
            self._budget(deadline, 'NAMESPACE_DEADLINE')

    def _namespace_observe(self, watch_root, path, action):
        # This dedicated ATTRIBUTES-only modification channel distinguishes a
        # transient reparse/attribute transition from source directory timestamps.
        if path not in self.namespace_paths:
            with self._lock:
                self._ignored += 1
            return
        self._append(watch_root, path, action, 'namespace', 'namespace-route')

    def _error(self, code, value):
        literal = str(value)
        escaped = literal.encode("unicode_escape", errors="backslashreplace").decode("ascii")
        digest = hashlib.sha256(literal.encode("utf-8", errors="backslashreplace")).hexdigest()
        with self._lock:
            if len(escaped) > 512:
                self._omitted += 1
            if len(self._errors) < MAX_ERRORS:
                self._errors.append(ErrorRow(code, escaped[:512] + " [sha256=" + digest + "]"))
            else:
                self._omitted += 1

    def _observe(self, watch_root, path, action):
        path = absolute(path)
        if action not in ACTIONS or len(str(path)) > MAX_PATH_CHARS:
            self._error("DECODE_ERROR", "Unknown action or oversized exact event path")
            return
        try:
            category = self.policy(path, action) if self.policy is not None else None
            if category is not None and (not isinstance(category, str) or not category
                                         or len(category) > MAX_CATEGORY_CHARS):
                raise ValueError("Owned event category must be an exact bounded string")
        except BaseException as error:
            self._error("OWNED_POLICY_ERROR", error)
            return
        relevant = any(inside(path, root) or (action != 3 and inside(root, path)) for root in self.immutable)
        if self.source is not None and inside(path, self.source):
            source_relevant = not any(part in self.skip for part in path.relative_to(self.source).parts)
            # Legacy source policy deliberately ignores existing-directory
            # timestamp notifications caused by allowed generated children.
            # Explicit immutable union above ALWAYS wins. Directory attributes
            # on retained route nodes use the separate native0x6 channel.
            if action == 3 and os.path.lexists(path):
                attributes = path.lstat().st_file_attributes
                if attributes & 0x10 and not attributes & 0x400:
                    source_relevant = False
            relevant = relevant or source_relevant
        # An explicit immutable root wins over a claimed owned-output category.
        # Consumers must avoid declaring mutable roots as immutable wholesale.
        if not relevant and category is None:
            with self._lock:
                self._ignored += 1
            return
        self._append(watch_root, path, action, 'immutable' if relevant else 'owned',
                     'immutable-input' if relevant else category)

    def _append(self, watch_root, path, action, disposition, category):
        with self._lock:
            rows = self._owned if disposition == 'owned' else self._events
            cap = self._owned_cap if disposition == 'owned' else self._event_cap
            if len(rows) >= cap:
                self._omitted += 1
                self._error("EVENT_LIMIT", "Exact event ledger capacity exceeded")
                return
            row = EventRow(self._seq + 1, str(watch_root), str(path), action, ACTIONS[action], disposition, category)
            size = record_bytes(row)
            if self._ledger_bytes + size > MAX_LEDGER_BYTES:
                self._omitted += 1
                self._error('EVENT_BYTES_LIMIT', 'Total exact retained ledger exceeds64MiB record budget')
                return
            self._seq += 1
            self._ledger_bytes += size
            rows.append(row)

    def _decode(self, directory, buffer, count, namespace=False):
        if count == 0:
            self._error("NATIVE_OVERFLOW", "Successful ReadDirectoryChangesW completion returned zero bytes")
            return
        if count < 0 or count > BUFFER_BYTES:
            raise ValueError("Unknown native completion byte count")
        cursor = 0
        while True:
            if cursor + 12 > count:
                raise ValueError("Truncated native event header")
            offset, action, size = struct.unpack_from("III", buffer.raw, cursor)
            if action not in ACTIONS or size == 0 or size % 2 or cursor + 12 + size > count:
                raise ValueError("Unknown/truncated native action or name")
            name = buffer.raw[cursor + 12:cursor + 12 + size].decode("utf-16-le", errors="strict")
            relative = Path(name)
            if (not name or "\x00" in name or relative.drive or relative.is_absolute()
                    or ".." in relative.parts or not inside(directory / relative, directory)):
                raise ValueError("Escaping native event path")
            if namespace:
                self._namespace_observe(directory, directory / relative, action)
            else:
                self._observe(directory, directory / relative, action)
            if offset == 0:
                if count - (cursor + 12 + size) > 3:
                    raise ValueError("Unknown unparsed native completion suffix")
                return
            if offset < 12 + size or offset % 4 or cursor + offset >= count:
                raise ValueError("Unknown native event offset")
            cursor += offset

    def start(self, deadline):
        if self._state != "new":
            raise ValueError("Witness start is one-use")
        self._state = "starting"
        now = time.monotonic()
        if not isinstance(deadline, (int, float)) or isinstance(deadline, bool) or not math.isfinite(deadline):
            self._error("START_DEADLINE", "Finite absolute monotonic deadline required")
            return self.snapshot()
        deadline = min(deadline, now + START_MAX_SECONDS)
        if now >= deadline:
            self._error("START_DEADLINE", "Inclusive startup deadline already expired; no handles allocated")
            return self.snapshot()
        self._start_deadline = deadline
        try:
            self._retain_namespace(deadline)
            self._reserve_native_records()
            self._budget(deadline, 'START_DEADLINE')
        except BaseException as error:
            self._error('NAMESPACE_BIND_ERROR', error)
            self._state = 'failed-start'
            return self.snapshot()
        # Namespace self/parent channel arms first; no target may be released
        # until ALL readers and the post-arm native binding check succeed.
        selections = [(root, True) for root in self.namespace_watch_roots]
        selections += [(root, False) for root in self.watch_roots]
        for root, namespace in selections:
            if time.monotonic() >= deadline:
                self._error("START_DEADLINE", "Inclusive aggregate startup deadline expired")
                break
            watch = _Watch(self, root, namespace)
            (self._namespace_watches if namespace else self._watches).append(watch)
            try:
                watch.thread.start()
                watch.thread_started = True
                ready = watch.started.wait(max(0, deadline - time.monotonic()))
            except BaseException as error:
                self._error("START_FAILURE", error)
                break
            if not ready or time.monotonic() >= deadline:
                self._error("START_DEADLINE", "Actual native arm missed aggregate deadline")
                break
            if not watch.armed or not watch.thread.is_alive():
                self._error("START_UNARMED", "Native read not armed/live: " + str(root))
                break
        try:
            self._verify_namespace(deadline)
        except BaseException as error:
            self._error('NAMESPACE_BIND_ERROR', error)
        self._startup_ok = (len(self._all_watches()) == len(selections)
                            and all(w.armed and w.bound and w.thread.is_alive() for w in self._all_watches())
                            and time.monotonic() < deadline and not self._events
                            and not self._errors and not self._omitted)
        self._state = "active" if self._startup_ok else "failed-start"
        state = self.snapshot()
        if time.monotonic() >= deadline:
            self._startup_ok, self._state = False, 'failed-start'
            self._error('START_DEADLINE', 'Inclusive startup receipt construction exceeded deadline')
            return self.snapshot()
        return state

    def snapshot(self):
        if self._closed is not None:
            return self._closed.as_dict()
        with self._lock:
            return {"schema": SCHEMA, "state": self._state, "complete": False,
                    "observationComplete": False, "advisoryOnly": True,
                    "startupDeadlineSatisfied": self._startup_ok,
                    "nativeArmedStartup": len(self._watches) == len(self.watch_roots)
                        and len(self._namespace_watches) == len(self.namespace_watch_roots)
                        and all(w.armed and w.bound for w in self._all_watches()),
                    'namespaceBound': self._startup_ok,
                    "events": [asdict(row) for row in self._events],
                    "ownedEvents": [asdict(row) for row in self._owned],
                    "errors": [asdict(row) for row in self._errors], "omitted": self._omitted,
                    "ignoredOutOfClosureEvents": self._ignored,
                    "qualificationOnly": self._fault is not None, "qualificationMode": self._fault,
                    "qualificationPendingCompletionReady": bool(self._watches)
                        and all(w.completion_ready.is_set() for w in self._watches)}

    def check(self):
        state = self.snapshot()
        if (self._state != "active" or not self._startup_ok or state["events"] or state["errors"]
                or state["omitted"] or any(not w.thread.is_alive() for w in self._all_watches())):
            raise ValueError("Sticky native immutable-input witness veto")
        return state

    def _close_readers(self, group, deadline):
        for watch in group:
            watch.close_deadline = deadline
            watch.stop.set()
        for watch in group:
            if watch.thread_started:
                try:
                    watch.thread.join(max(0, deadline - time.monotonic()))
                except BaseException as error:
                    self._error("CLOSE_DEADLINE", error)
        joined = all(not w.thread.is_alive() for w in group)
        drained = all(w.drained for w in group)
        if not joined or time.monotonic() >= deadline:
            self._error("CLOSE_DEADLINE", "Native threads/drain missed one aggregate close deadline")
        handles_closed = True
        for watch in group:
            if watch.thread.is_alive() or not watch.drained:
                handles_closed = False
                # Live target retains buffer/OVERLAPPED until actual drain. The
                # caller must fail and terminate its contained process if stuck.
                continue
            if watch.handles:
                try:
                    self._budget(deadline, 'CLOSE_DEADLINE')
                    watch.final_identity = self._native.identity(watch.handles[0], watch.root)
                    watch.bound = watch.bound and same_object(watch.final_identity, watch.initial_identity)
                    if not watch.bound:
                        raise ValueError('Watch handle no longer matches retained namespace')
                    self._budget(deadline, 'CLOSE_DEADLINE')
                except BaseException as error:
                    watch.bound = False
                    self._error('NAMESPACE_BIND_ERROR', error)
            for handle in watch.handles:
                if time.monotonic() >= deadline:
                    handles_closed = False
                    self._error("CLOSE_DEADLINE", "Handle closure missed aggregate deadline; retain handles")
                    break
                try:
                    ctypes.set_last_error(0)
                    if not watch.kernel.CloseHandle(handle):
                        handles_closed = False
                        self._error("HANDLE_CLOSE_ERROR", ctypes.get_last_error())
                    else:
                        watch.closed_handles += 1
                except BaseException as error:
                    handles_closed = False
                    self._error("HANDLE_CLOSE_ERROR", error)
            watch.handles_closed = watch.closed_handles == len(watch.handles)
        return joined, drained, handles_closed

    def close(self, deadline):
        if self._closed is not None:
            raise ValueError("Witness close is one-use; retain the original immutable ledger")
        now = time.monotonic()
        if not isinstance(deadline, (int, float)) or isinstance(deadline, bool) or not math.isfinite(deadline):
            deadline = now
            self._error("CLOSE_DEADLINE", "Finite absolute close deadline required")
        deadline = min(deadline, now + CLOSE_MAX_SECONDS)
        self._state = "closing"
        # Namespace self/route readers dominate the full protected-reader drain,
        # post-drain identity query and checked watch-handle closes.
        protected = self._close_readers(self._watches, deadline)
        namespace = self._close_readers(self._namespace_watches, deadline)
        joined, drained, handles_closed = (all(values) for values in zip(protected, namespace))
        # Keep namespace retention until BOTH channel domains have drained and
        # their watch handles are closed. Never release a live reader's route.
        namespace_closed = not self._retained
        namespace_bound = False
        if joined and drained and handles_closed:
            try:
                self._verify_namespace(deadline)
                namespace_bound = len(self._retained) == len(self.namespace_paths)
            except BaseException as error:
                self._error('NAMESPACE_BIND_ERROR', error)
            for row in reversed(self._retained):
                try:
                    self._budget(deadline, 'CLOSE_DEADLINE')
                    if not self._native.kernel.CloseHandle(row['handle']):
                        raise ctypes.WinError(ctypes.get_last_error())
                    row['closed'] = True
                    self._budget(deadline, 'CLOSE_DEADLINE')
                except BaseException as error:
                    self._error('NAMESPACE_HANDLE_CLOSE_ERROR', error)
            namespace_closed = all(row['closed'] for row in self._retained)
        elif self._retained:
            self._error('NAMESPACE_RETAINED', 'Unknown live native domain retains namespace handles')
        if self._fault == "close":
            handles_closed = False
            self._error("QUALIFICATION_CLOSE", "Injected checked-close result refusal after actual closes")
        handles_closed = handles_closed and namespace_closed
        close_ok = time.monotonic() < deadline and joined and drained and handles_closed
        with self._lock:
            def watch_row(w):
                return WatchRow(str(w.root), w.armed, not w.thread.is_alive(), w.drained,
                                w.handles_closed, 'namespace' if w.namespace else 'protected',
                                w.initial_identity, w.final_identity, w.bound)
            watches = tuple(watch_row(w) for w in self._watches)
            namespace_watches = tuple(watch_row(w) for w in self._namespace_watches)
            namespace_rows = tuple(NamespaceRow(str(r['path']), r['initial'], r['final'], r['stable'],
                                                True, r['closed']) for r in self._retained)
            observation_ok = (self._startup_ok and close_ok and not self._errors and not self._omitted
                              and namespace_bound and all(w.bound for w in self._all_watches())
                              and len(watches) == len(self.watch_roots)
                              and len(namespace_watches) == len(self.namespace_watch_roots))
            self._closed = ClosedLedger(SCHEMA, "closed" if joined and drained and handles_closed else "incomplete",
                bool(observation_ok),
                bool(observation_ok and not self._events), self._startup_ok, close_ok,
                len(watches) == len(self.watch_roots)
                    and len(namespace_watches) == len(self.namespace_watch_roots)
                    and all(w.armed for w in watches + namespace_watches),
                joined, drained, handles_closed, namespace_bound, namespace_closed,
                tuple(self._events), tuple(self._owned), tuple(self._errors),
                watches, namespace_watches, namespace_rows, self._omitted, self._ignored,
                self._fault is not None, self._fault)
            self._state = "closed"
            if time.monotonic() >= deadline and close_ok:
                self._error('CLOSE_DEADLINE', 'Inclusive frozen-ledger construction exceeded deadline')
                self._closed = replace(self._closed, observationComplete=False, complete=False,
                                       closeDeadlineSatisfied=False, errors=tuple(self._errors),
                                       omitted=self._omitted)
        return self._closed


class _Watch:
    def __init__(self, guard, root, namespace=False):
        self.guard, self.root = guard, root
        self.namespace = namespace
        self.bound = False
        self.initial_identity = self.final_identity = None
        self.started, self.stop = threading.Event(), threading.Event()
        self.completion_ready = threading.Event()
        self.armed, self.drained, self.handles_closed = False, False, False
        self.handles, self.closed_handles, self.close_deadline = [], 0, None
        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread_started = False
        self.kernel = None
        self.pending_storage = None

    def _completion(self, handle, overlap, buffer, final=False):
        count = ctypes.c_uint32()
        ctypes.set_last_error(0)
        ok = self.kernel.GetOverlappedResult(handle, ctypes.byref(overlap), ctypes.byref(count), False)
        error = ctypes.get_last_error()
        if ok:
            # Do NOT predicate decoding on a nonzero count. A successful zero
            # final completion is overflow, not a clean cancellation.
            try:
                self.guard._decode(self.root, buffer, count.value, self.namespace)
            except BaseException as error:
                # Native I/O completed even if its observation is unusable.
                # Decode failure is sticky; it does not require leaking safe IO.
                self.guard._error("DECODE_ERROR", error)
            return True
        if final and error == 995:  # ERROR_OPERATION_ABORTED: explicit cancellation
            return True
        self.guard._error("DRAIN_ERROR" if final else "NATIVE_ERROR", error)
        return False

    def run(self):
        handle = event = overlap = buffer = None
        pending = False
        known_drained = True
        try:
            if not self.namespace and self.guard._fault == "startup":
                raise OSError("Injected native-start boundary refusal")
            kernel = self.kernel = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel.CreateFileW.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32,
                ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p]
            kernel.CreateFileW.restype = ctypes.c_void_p
            kernel.CreateEventW.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_wchar_p]
            kernel.CreateEventW.restype = ctypes.c_void_p
            kernel.ReadDirectoryChangesW.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_uint32,
                ctypes.c_int, ctypes.c_uint32, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
            kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
            kernel.GetOverlappedResult.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int]
            kernel.ResetEvent.argtypes = [ctypes.c_void_p]
            kernel.CancelIoEx.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
            kernel.CloseHandle.argtypes = [ctypes.c_void_p]
            class Overlapped(ctypes.Structure):
                _fields_ = [("Internal", ctypes.c_size_t), ("InternalHigh", ctypes.c_size_t),
                    ("Offset", ctypes.c_uint32), ("OffsetHigh", ctypes.c_uint32), ("hEvent", ctypes.c_void_p)]
            self.guard._budget(self.guard._start_deadline, 'START_DEADLINE')
            handle = kernel.CreateFileW(str(self.root), 0x81, 3, None, 3, 0x42200000, None)
            if handle == ctypes.c_void_p(-1).value:
                handle = None
                raise ctypes.WinError(ctypes.get_last_error())
            self.handles.append(handle)
            if not self.namespace and self.guard._after_open is not None:
                self.guard._after_open(self.root)
            self.initial_identity = self.guard._native.identity(handle, self.root)
            self.bound = same_object(self.initial_identity, self.guard._retained_identity(self.root))
            if not self.bound:
                raise ValueError('Actual watch handle differs from retained ordinary root')
            self.guard._budget(self.guard._start_deadline, 'START_DEADLINE')
            event = kernel.CreateEventW(None, True, False, None)
            if not event:
                raise ctypes.WinError(ctypes.get_last_error())
            self.handles.append(event)
            buffer, overlap = ctypes.create_string_buffer(BUFFER_BYTES), Overlapped()
            overlap.hEvent = event
            self.pending_storage = (buffer, overlap)
            first = True
            while not self.stop.is_set():
                if not kernel.ResetEvent(event):
                    raise ctypes.WinError(ctypes.get_last_error())
                ctypes.set_last_error(0)
                ok = kernel.ReadDirectoryChangesW(handle, buffer, BUFFER_BYTES, not self.namespace,
                                                  NAMESPACE_FILTER if self.namespace else NOTIFY_FILTER,
                                                  None, ctypes.byref(overlap), None)
                if not ok and ctypes.get_last_error() != 997:
                    raise ctypes.WinError(ctypes.get_last_error())
                pending, known_drained, self.armed = True, False, True
                self.started.set()  # actual API success or ERROR_IO_PENDING, not thread progress
                if first and not self.namespace and self.guard._fault == "error":
                    self.guard._error("QUALIFICATION_ERROR", "Injected native-result boundary refusal")
                if first and not self.namespace and self.guard._fault == "overflow":
                    self.guard._decode(self.root, buffer, 0)
                hold = first and not self.namespace and self.guard._fault in ("final-event", "final-zero")
                first = False
                while not self.stop.is_set():
                    if hold:
                        state = kernel.WaitForSingleObject(event, 20)
                        if state == 0:
                            self.completion_ready.set()  # actual OS completion, not decoded yet
                            self.stop.wait(0.02)
                        elif state != 258:
                            raise OSError("Unknown held native wait result: " + str(state))
                        continue
                    state = kernel.WaitForSingleObject(event, 20)
                    if state == 0:
                        break
                    if state != 258:
                        raise OSError("Unknown native wait result: " + str(state))
                if self.stop.is_set():
                    break
                known_drained = self._completion(handle, overlap, buffer)
                pending = not known_drained
                if not known_drained:
                    raise OSError("Native completion uncertain")
        except BaseException as error:
            self.guard._error("START_FAILURE" if not self.armed else "NATIVE_ERROR", error)
        finally:
            self.started.set()
            if pending and handle and event and overlap is not None:
                try:
                    ctypes.set_last_error(0)
                    if not self.kernel.CancelIoEx(handle, ctypes.byref(overlap)) and ctypes.get_last_error() != 1168:
                        self.guard._error("CANCEL_ERROR", ctypes.get_last_error())
                    warned = False
                    while True:
                        state = self.kernel.WaitForSingleObject(event, 20)
                        if state == 0:
                            known_drained = self._completion(handle, overlap, buffer, final=True)
                            if not self.namespace and self.guard._fault == "final-zero":
                                # Explicitly injected successful-final count0,
                                # same decoder; actual result was drained above.
                                self.guard._decode(self.root, buffer, 0)
                            break
                        if state != 258:
                            self.guard._error("DRAIN_ERROR", "Unknown final wait result: " + str(state))
                            break
                        if self.close_deadline is not None and time.monotonic() >= self.close_deadline and not warned:
                            warned = True
                            self.guard._error("CLOSE_DEADLINE", "Pending native read retained beyond close deadline")
                except BaseException as error:
                    self.guard._error("DRAIN_ERROR", error)
                    known_drained = False
                if not known_drained:
                    # Never release pending storage on uncertainty. This live
                    # retained thread is a failure requiring contained exit.
                    while True:
                        time.sleep(0.05)
            self.drained = known_drained


def same_object(left, right):
    return (isinstance(left, NativeIdentity) and isinstance(right, NativeIdentity)
            and (left.volumeSerial, left.fileId, left.finalPath) ==
                (right.volumeSerial, right.fileId, right.finalPath))


def record_bytes(row):
    return len(json.dumps(asdict(row), ensure_ascii=True, separators=(',', ':')).encode('ascii')) + 512


class _Native:
    """Finite ordinary local-DOS namespace binding; no allocation on import."""
    def __init__(self):
        kernel = self.kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.CreateFileW.argtypes = [ctypes.c_wchar_p, ctypes.c_uint32, ctypes.c_uint32,
            ctypes.c_void_p, ctypes.c_uint32, ctypes.c_uint32, ctypes.c_void_p]
        kernel.CreateFileW.restype = ctypes.c_void_p
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel.CloseHandle.restype = ctypes.c_int
        kernel.GetFileInformationByHandleEx.argtypes = [ctypes.c_void_p, ctypes.c_int,
                                                       ctypes.c_void_p, ctypes.c_uint32]
        kernel.GetFileInformationByHandleEx.restype = ctypes.c_int
        kernel.GetFinalPathNameByHandleW.argtypes = [ctypes.c_void_p, ctypes.c_wchar_p,
                                                    ctypes.c_uint32, ctypes.c_uint32]
        kernel.GetFinalPathNameByHandleW.restype = ctypes.c_uint32
        kernel.GetDriveTypeW.argtypes = [ctypes.c_wchar_p]
        kernel.GetDriveTypeW.restype = ctypes.c_uint32
        kernel.QueryDosDeviceW.argtypes = [ctypes.c_wchar_p, ctypes.c_wchar_p, ctypes.c_uint32]
        kernel.QueryDosDeviceW.restype = ctypes.c_uint32

    def local_drive(self, path):
        if self.kernel.GetDriveTypeW(path.anchor) != 3:
            raise ValueError('Only local fixed DOS drive namespace is supported')
        target = ctypes.create_unicode_buffer(MAX_PATH_CHARS + 1)
        count = self.kernel.QueryDosDeviceW(path.drive, target, len(target))
        if not count or count >= len(target):
            raise ValueError('Actual DOS drive mapping unknown or oversized')
        suffix = target.value.removeprefix('\\Device\\HarddiskVolume')
        if (not target.value.startswith('\\Device\\HarddiskVolume') or not suffix
                or not suffix.isascii() or not suffix.isdigit()):
            raise ValueError('Subst, mapped, virtual or unknown drive alias unsupported')

    def open_directory(self, path, overlapped):
        self.local_drive(path)
        flags = 0x02200000 | (0x40000000 if overlapped else 0)
        # LIST_DIRECTORY must participate in sharing for EVERY retained directory.
        # An access refusal is sticky startup failure; never fall back to attributes-only.
        handle = self.kernel.CreateFileW(str(path), 0x81,
                                         3, None, 3, flags, None)
        if handle == ctypes.c_void_p(-1).value or not handle:
            raise ctypes.WinError(ctypes.get_last_error())
        return handle

    def identity(self, handle, expected):
        self.local_drive(expected)
        class AttributeTagInfo(ctypes.Structure):
            _fields_ = [('attributes', ctypes.c_uint32), ('reparseTag', ctypes.c_uint32)]
        class FileIdInfo(ctypes.Structure):
            _fields_ = [('volumeSerial', ctypes.c_uint64), ('fileId', ctypes.c_ubyte * 16)]
        attributes, file_id = AttributeTagInfo(), FileIdInfo()
        case_sensitive = ctypes.c_uint32()
        for selector, value in ((9, attributes), (18, file_id), (23, case_sensitive)):
            if not self.kernel.GetFileInformationByHandleEx(handle, selector, ctypes.byref(value), ctypes.sizeof(value)):
                raise ctypes.WinError(ctypes.get_last_error())
        if not attributes.attributes & 0x10 or attributes.attributes & 0x400 or attributes.reparseTag:
            raise ValueError('Actual native handle is not an ordinary nonreparse directory')
        if case_sensitive.value:
            raise ValueError('Case-sensitive or unknown directory name semantics are unsupported')
        result = ctypes.create_unicode_buffer(MAX_PATH_CHARS + 8)
        count = self.kernel.GetFinalPathNameByHandleW(handle, result, len(result), 0)
        if not count or count >= len(result):
            raise ValueError('Actual final normalized DOS path unknown or oversized')
        literal = result.value
        if not literal.startswith('\\\\?\\') or literal.startswith('\\\\?\\UNC\\'):
            raise ValueError('Unsupported native namespace path form')
        final = absolute(literal[4:])
        if final != expected:
            raise ValueError('Native final path differs from declared ordinary path: ' + literal)
        return NativeIdentity(format(file_id.volumeSerial, '016x'), bytes(file_id.fileId).hex(),
                              str(final), int(attributes.attributes), int(attributes.reparseTag),
                              int(case_sensitive.value))


def validate_closed(record, *, require_clean=True, allow_qualification=False):
    """Pure structural/lifecycle verifier, NOT native evidence or output legality.

    Consumers still bind exact source/proof and reconcile every final owned row.
    Never accepts an advisory snapshot, unsupported literal route, omitted suffix
    or boolean prefix. Pure route syntax is not native route/object proof.
    Returns a detached record only; no native API or filesystem check is called.
    """
    if isinstance(record, ClosedLedger):
        record = record.as_dict()
    if (not isinstance(record, dict) or set(record) != set(ClosedLedger.__dataclass_fields__)
            or record.get('schema') != SCHEMA or record.get('state') != 'closed'):
        raise ValueError('Exact fully closed v2 ledger required')
    for field in ('observationComplete', 'startupDeadlineSatisfied', 'closeDeadlineSatisfied',
                  'nativeArmedStartup', 'joined', 'drained', 'nativeHandlesClosed',
                  'namespaceBound', 'namespaceHandlesClosed'):
        if record.get(field) is not True:
            raise ValueError('Incomplete witness lifecycle: ' + field)
    if (not isinstance(record.get('errors'), (tuple, list)) or record['errors']
            or type(record.get('omitted')) is not int or record['omitted'] != 0
            or type(record.get('qualificationOnly')) is not bool
            or (not record['qualificationOnly'] and record.get('qualificationMode') is not None)
            or (record['qualificationOnly'] and not isinstance(record.get('qualificationMode'), str))
            or type(record.get('ignoredOutOfClosureEvents')) is not int or record['ignoredOutOfClosureEvents'] < 0
            or (record['qualificationOnly'] and not allow_qualification)):
        raise ValueError('Sticky witness errors/omissions or qualification-only record')
    for field, cap in (('watches', MAX_WATCHES), ('namespaceWatches', MAX_NAMESPACE_HANDLES),
                       ('namespaceHandles', MAX_NAMESPACE_HANDLES), ('events', MAX_EVENTS),
                       ('ownedEvents', MAX_OWNED_EVENTS)):
        if not isinstance(record.get(field), (tuple, list)) or len(record[field]) > cap:
            raise ValueError('Missing or oversized frozen ledger domain: ' + field)
    if not record['watches'] or not record['namespaceHandles']:
        raise ValueError('Empty native closure')
    retained = {}
    def route(value):
        if not isinstance(value, str) or len(value) > MAX_PATH_CHARS:
            raise ValueError('Exact bounded literal route string required')
        # Same bounded absolute/local-DOS predicate as production selection.
        # Windows Path keys unify case/separator aliases before every domain
        # join, duplicate check and parent derivation; no filesystem lookup.
        return absolute(value)

    def identity(value, path):
        if (not isinstance(value, dict) or set(value) != set(NativeIdentity.__dataclass_fields__)
                or route(value.get('finalPath')) != path
                or type(value.get('attributes')) is not int or not value['attributes'] & 0x10
                or value['attributes'] & 0x400 or value.get('reparseTag') != 0
                or type(value.get('caseSensitiveFlags')) is not int or value['caseSensitiveFlags'] != 0
                or not isinstance(value.get('volumeSerial'), str) or len(value['volumeSerial']) != 16
                or not isinstance(value.get('fileId'), str) or len(value['fileId']) != 32
                or any(c not in '0123456789abcdef' for c in value['volumeSerial'] + value['fileId'])):
            raise ValueError('Invalid actual namespace identity row')
        return (value['volumeSerial'], value['fileId'], route(value['finalPath']))
    for row in record['namespaceHandles']:
        if not isinstance(row, dict) or set(row) != set(NamespaceRow.__dataclass_fields__):
            raise ValueError('Exact typed namespace row required')
        path = route(row.get('path'))
        if (path in retained
                or row.get('identityStable') is not True or row.get('handleClosed') is not True
                or row.get('retainedWithoutDeleteShare') is not True):
            raise ValueError('Invalid retained native namespace row')
        before, after = identity(row.get('initialIdentity'), path), identity(row.get('finalIdentity'), path)
        if before != after:
            raise ValueError('Changed native namespace identity')
        retained[path] = before
    watch_sets = []
    for field, channel in (('watches', 'protected'), ('namespaceWatches', 'namespace')):
        seen = set()
        for row in record[field]:
            if not isinstance(row, dict) or set(row) != set(WatchRow.__dataclass_fields__):
                raise ValueError('Exact typed watch row required')
            path = route(row.get('root'))
            if path in seen or path not in retained or row.get('channel') != channel:
                raise ValueError('Unbound or duplicate watch root')
            seen.add(path)
            if any(row.get(key) is not True for key in ('armed', 'joined', 'drained', 'handlesClosed', 'namespaceBound')):
                raise ValueError('Incomplete actual native reader')
            if (identity(row.get('initialIdentity'), path) != retained[path]
                    or identity(row.get('finalIdentity'), path) != retained[path]):
                raise ValueError('Native reader and retained root differ')
        watch_sets.append(seen)
    expected_parents = {path.parent for path in retained if path != path.parent}
    if watch_sets[1] != expected_parents or not expected_parents.issubset(retained):
        raise ValueError('Incomplete namespace parent observation closure')
    sequence = set()
    for field, dispositions in (('events', {'immutable', 'namespace'}), ('ownedEvents', {'owned'})):
        for row in record[field]:
            if not isinstance(row, dict) or set(row) != set(EventRow.__dataclass_fields__):
                raise ValueError('Exact typed event row required')
            seq = row.get('seq')
            if (type(seq) is not int or seq < 1 or seq in sequence or type(row.get('action')) is not int
                    or row['action'] not in ACTIONS
                    or row.get('actionName') != ACTIONS[row['action']] or row.get('disposition') not in dispositions
                    or not isinstance(row.get('path'), str) or len(row['path']) > MAX_PATH_CHARS
                    or not isinstance(row.get('category'), str) or not row['category']
                    or len(row['category']) > MAX_CATEGORY_CHARS):
                raise ValueError('Malformed typed terminal event')
            sequence.add(seq)
            roots = watch_sets[1] if row['disposition'] == 'namespace' else watch_sets[0]
            event_path, watch_path = route(row['path']), route(row.get('watchRoot'))
            if watch_path not in roots:
                raise ValueError('Typed event has unknown native reader')
            if not inside(event_path, watch_path):
                raise ValueError('Typed event escapes actual reader closure')
            if row['disposition'] == 'namespace' and (event_path not in retained
                    or event_path.parent != watch_path or row['category'] != 'namespace-route'):
                raise ValueError('Invalid exact namespace transition row')
            if row['disposition'] == 'immutable' and row['category'] != 'immutable-input':
                raise ValueError('Invalid immutable typed event category')
    if sequence != set(range(1, len(sequence) + 1)):
        raise ValueError('Incomplete terminal event sequence')
    if record.get('complete') is not (not record['events']):
        raise ValueError('Immutable acceptance does not match full ledger')
    if require_clean and (record['events'] or record['ownedEvents'] or record.get('complete') is not True):
        raise ValueError('Read-only witness requires no immutable or owned rows')
    if len(json.dumps(record, ensure_ascii=True, separators=(',', ':')).encode('ascii')) > MAX_LEDGER_BYTES:
        raise ValueError('Complete compact frozen ledger exceeds64MiB')
    # asdict already cloned dataclasses; this bounded generic copy also handles
    # externally decoded JSON without retaining caller-owned aliases.
    def clone(value):
        if isinstance(value, dict):
            return {key: clone(item) for key, item in value.items()}
        if isinstance(value, (tuple, list)):
            return [clone(item) for item in value]
        return value
    return clone(record)
