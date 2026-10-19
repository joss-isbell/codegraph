"""Public command/receipt checks for ordinary Windows CI (no product fixture skips)."""
import json
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


HERE = Path(__file__).resolve().parent


def successful_attempt(root):
    attempt = root / "attempt"
    attempt.mkdir()
    receipt = {"schema": "codegraph-ordinary-windows-ci-v1", "outcome": "pass",
               "commandSequenceOutcome": "pass", "plannedStages": ["install", "build", "test"],
               "failures": [], "interrupted": False, "elapsedSeconds": 1, "deadlineSeconds": 3300,
               "node": "node.exe", "npmCli": "npm-cli.js", "stages": []}
    for name, args in (("install", ["ci"]), ("build", ["run", "build"]), ("test", ["test"])):
        directory = attempt / name
        directory.mkdir()
        logs = {}
        for stream in ("stdout.log", "stderr.log"):
            (directory / stream).write_bytes(b"")
            logs[stream] = {"constructorReturned": True, "joined": True, "pipeClosed": True,
                           "pipeCloseAttempted": True, "pipeCloseCompleted": True,
                           "pipeCloseError": None, "seenBytes": 0, "retainedBytes": 0,
                           "overflow": False, "errors": []}
        stage = {"stage": name, "command": ["node.exe", "npm-cli.js", *args], "outcome": "pass",
                 "exitCode": 0, "failures": [], "launchState": "payload-released",
                 "gateHandleCloseChecked": True, "logs": logs,
                 "cleanup": {"complete": True, "remainingPids": [], "intentionallyStoppedOwnedPids": []}}
        (directory / "result.json").write_text(json.dumps(stage), encoding="utf-8")
        receipt["stages"].append(stage)
    (attempt / "receipt.json").write_text(json.dumps(receipt), encoding="utf-8")
    return attempt, receipt


class ReceiptChecks(unittest.TestCase):
    def test_discovery_access_error_cannot_be_hidden_as_a_missing_attempt(self):
        with tempfile.TemporaryDirectory(prefix="codegraph-ci-access-") as temporary:
            root = Path(temporary)
            successful_attempt(root)
            blocked = root / "blocked"
            blocked.mkdir()
            spec = importlib.util.spec_from_file_location("ordinary_ci_consumer", HERE / "verify-windows.py")
            consumer = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(consumer)
            native_lstat = Path.lstat

            def denied(path, *args, **kwargs):
                if path == blocked / "receipt.json":
                    raise PermissionError("injected OS metadata access error")
                return native_lstat(path, *args, **kwargs)

            with patch.object(Path, "lstat", denied):
                with self.assertRaises(PermissionError):
                    consumer.verify(root)

    def test_late_finalization_is_not_a_passing_receipt(self):
        with tempfile.TemporaryDirectory(prefix="codegraph-ci-late-") as temporary:
            root = Path(temporary)
            attempt, receipt = successful_attempt(root)
            receipt["elapsedSeconds"] = 3301
            (attempt / "receipt.json").write_text(json.dumps(receipt), encoding="utf-8")
            result = subprocess.run([sys.executable, "-B", str(HERE / "verify-windows.py"), str(root)],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 1)
            self.assertIn("deadline", result.stdout)

    def test_complete_receipt_is_consumed_but_missing_raw_tail_is_not(self):
        with tempfile.TemporaryDirectory(prefix="codegraph-ci-log-") as temporary:
            root = Path(temporary)
            attempt, _ = successful_attempt(root)
            command = [sys.executable, "-B", str(HERE / "verify-windows.py"), str(root)]
            complete = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(complete.returncode, 0, complete.stdout)
            (attempt / "test/stderr.log").unlink()
            incomplete = subprocess.run(command, capture_output=True, text=True, timeout=10)
            self.assertEqual(incomplete.returncode, 1)

    def test_unclosed_capture_cannot_be_overridden_by_a_pass_label(self):
        with tempfile.TemporaryDirectory(prefix="codegraph-ci-close-") as temporary:
            root = Path(temporary)
            attempt, receipt = successful_attempt(root)
            receipt["stages"][2]["logs"]["stderr.log"]["pipeCloseCompleted"] = False
            (attempt / "receipt.json").write_text(json.dumps(receipt), encoding="utf-8")
            (attempt / "test/result.json").write_text(json.dumps(receipt["stages"][2]), encoding="utf-8")
            result = subprocess.run([sys.executable, "-B", str(HERE / "verify-windows.py"), str(root)],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 1)
            self.assertIn("capture", result.stdout)

    def test_a_passing_prefix_is_not_a_complete_ci_result(self):
        with tempfile.TemporaryDirectory(prefix="codegraph-ci-prefix-") as temporary:
            root = Path(temporary)
            (root / "attempt").mkdir()
            (root / "attempt/receipt.json").write_text(json.dumps({
                "schema": "codegraph-ordinary-windows-ci-v1", "outcome": "pass",
                "commandSequenceOutcome": "pass", "failures": [],
                "stages": [{"stage": "install", "outcome": "pass", "exitCode": 0}],
            }), encoding="utf-8")
            result = subprocess.run([sys.executable, "-B", str(HERE / "verify-windows.py"), str(root)],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 1)
            self.assertIn("complete install/build/test", result.stdout)

    def test_an_invalid_second_receipt_cannot_be_hidden_by_discovery(self):
        with tempfile.TemporaryDirectory(prefix="codegraph-ci-receipt-") as temporary:
            root = Path(temporary)
            (root / "first").mkdir()
            (root / "first" / "receipt.json").write_text('{"outcome":"pass"}', encoding="utf-8")
            (root / "second" / "receipt.json").mkdir(parents=True)
            result = subprocess.run([sys.executable, "-B", str(HERE / "verify-windows.py"), str(root)],
                                    capture_output=True, text=True, timeout=10)
            self.assertEqual(result.returncode, 1)
            self.assertIn("receipt is not a regular file", result.stdout)


if __name__ == "__main__":
    unittest.main()
