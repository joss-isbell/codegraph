"""Real Windows command/receipt seam, with one native CloseHandle fault."""
import ctypes
from ctypes import wintypes
import importlib.util
import io
import json
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
    def test_unicode_failed_command_retains_complete_final_receipt_on_cp1252_stdout(self):
        ci = adapter()
        with tempfile.TemporaryDirectory(prefix="codegraph-ci-unicode-report-") as temporary:
            root = Path(temporary)
            npm_cli = root / "node_modules" / "npm" / "bin" / "npm-cli.js"
            npm_cli.parent.mkdir(parents=True)
            # External-command fixture only: real Python processes, native Jobs,
            # raw UTF-8 output, unchanged CI stages and actual receipt persistence.
            npm_cli.write_text("import sys\n"
                "if sys.argv[1] == 'test':\n"
                "    sys.stdout.buffer.write((chr(0x276f) + ' test failed\\n').encode('utf-8'))\n"
                "    sys.stdout.buffer.flush()\n"
                "    sys.exit(23)\n"
                "print('fixture command passed')\n", encoding="utf-8")
            encoded = io.BytesIO()
            console = io.TextIOWrapper(encoded, encoding="cp1252", errors="strict", write_through=True)
            def which(command):
                return sys.executable if command == "node" else str(root / "npm.cmd")
            with patch.object(ci.shutil, "which", side_effect=which), \
                    patch.dict(os.environ, {"RUNNER_TEMP": str(root / "attempt")}), \
                    patch.object(sys, "stdout", console):
                exit_code = ci.main()
            paths = list((root / "attempt" / "codegraph-windows-ci").glob("*/receipt.json"))
            self.assertEqual(len(paths), 1)
            receipt = json.loads(paths[0].read_text(encoding="utf-8"))
            stage_path = paths[0].parent / "test" / "result.json"
            stage = json.loads(stage_path.read_text(encoding="utf-8"))
            if [entry["stage"] for entry in receipt["stages"]] != ["install", "build", "test"]:
                print("CI_REPORTING_REPRO=" + json.dumps({"receipt": receipt, "testStage": stage}))
            self.assertEqual(exit_code, 1)
            self.assertEqual([entry["stage"] for entry in receipt["stages"]], ["install", "build", "test"])
            self.assertEqual(receipt["stages"][-1], stage)
            self.assertEqual(receipt["outcome"], "fail")
            self.assertEqual(receipt["commandSequenceOutcome"], "fail")
            self.assertEqual(stage["exitCode"], 23)
            self.assertEqual(stage["outcome"], "fail")
            self.assertIn("\u276f test failed", stage["failureExcerpt"])
            self.assertIn(b"\xe2\x9d\xaf test failed", (stage_path.parent / "stdout.log").read_bytes())
            self.assertTrue(stage["cleanup"]["complete"])
            self.assertEqual(stage["cleanup"]["remainingPids"], [])
            self.assertEqual(stage["cleanup"]["intentionallyStoppedOwnedPids"], [])
            self.assertTrue(stage["gateHandleCloseChecked"])
            self.assertNotIn("supervisorError", receipt)
            for capture in stage["logs"].values():
                self.assertTrue(capture["joined"])
                self.assertTrue(capture["pipeCloseCompleted"])
                self.assertIsNone(capture["pipeCloseError"])
            console.close()

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
