"""Real Windows command/receipt seam, with one native CloseHandle fault."""
import ctypes
from ctypes import wintypes
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch


HERE = Path(__file__).resolve().parent


def adapter():
    spec = importlib.util.spec_from_file_location("ordinary_ci_test", HERE / "windows.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless(os.name == "nt", "real Windows native boundary")
class NativeReceiptChecks(unittest.TestCase):
    def test_native_job_close_error_cannot_be_reported_as_success(self):
        ci = adapter()
        native_factory = ctypes.WinDLL
        library = native_factory("kernel32", use_last_error=True)
        native_create = library.CreateJobObjectW
        native_create.argtypes = [ctypes.c_void_p, wintypes.LPCWSTR]
        native_create.restype = wintypes.HANDLE
        native_close = library.CloseHandle
        native_close.argtypes = [wintypes.HANDLE]
        native_close.restype = wintypes.BOOL
        jobs, denied = [], []

        def create(*args):
            handle = native_create(*args)
            jobs.append(handle)
            return handle

        def close(handle):
            if handle in jobs and not denied:
                denied.append(handle)
                ctypes.set_last_error(5)
                return 0  # Native boundary fault; genuine handle remains open.
            result = native_close(handle)
            if result and handle in jobs:
                jobs.remove(handle)
            return result

        library.CreateJobObjectW = create
        library.CloseHandle = close

        def factory(name, **kwargs):
            return library if name == "kernel32" else native_factory(name, **kwargs)

        with patch("ctypes.WinDLL", side_effect=factory):
            core = ci.load_supervisor()
        with tempfile.TemporaryDirectory(prefix="codegraph-ci-native-close-") as temporary:
            try:
                result = ci.run_stage(core, "native-close", [sys.executable, "-B", "-c", "print('real child')"],
                                      10, Path(temporary) / "stage", time.monotonic() + 80, threading.Event())
                self.assertEqual(result["exitCode"], 0)
                self.assertEqual(result["outcome"], "fail")
                self.assertTrue(any("cleanup failed" in reason for reason in result["failures"]))
            finally:
                # On old code the failed close was ignored; release that exact
                # retained test-created handle, never a PID or scanned handle.
                for handle in jobs:
                    native_close(handle)


if __name__ == "__main__":
    unittest.main()
