"""Compare a frozen reviewed input description, then use the normal launcher.

No target runs in --check-only mode. This static check does not arm a witness or
close the existing quiet-input interval before normal.py's native observation.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).absolute().parent


def sha256(path):
    with Path(path).open("rb") as source:
        result = hashlib.file_digest(source, "sha256")
    return result.hexdigest()


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key: " + key)
        result[key] = value
    return result


def read(path):
    return json.loads(Path(path).read_text(encoding="utf-8-sig"), object_pairs_hook=unique_object)


def literal(value):
    if not isinstance(value, str) or not Path(value).is_absolute() or ".." in Path(value).parts:
        raise ValueError("literal absolute path required")
    return Path(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("review", type=Path)
    parser.add_argument("--sha256", required=True, help="previously frozen review-file digest")
    parser.add_argument("--output", required=True, type=Path, help="fresh static-preflight directory")
    parser.add_argument("--check-only", action="store_true")
    args = parser.parse_args()
    if os.name != "nt" or sha256(args.review) != args.sha256:
        raise ValueError("Windows and the exact frozen review file are required")
    review = read(args.review)
    plan_path = literal(review["plan"]["path"])
    if sha256(plan_path) != review["plan"]["sha256"]:
        raise ValueError("selected plan differs from reviewed plan")
    plan = read(plan_path)
    expected = review["binding"]
    artifacts = {}
    for name, digest in expected["artifacts"].items():
        path = literal(name)
        if path in artifacts and artifacts[path] != digest:
            raise ValueError("conflicting expected artifact hashes")
        artifacts[path] = digest
    # Plan/control references must already be in the final strict input map;
    # duplicate proof declarations cannot silently replace a conflicting hash.
    controls = [review["plan"], *review["controls"]]
    known = dict(artifacts)
    for control in controls:
        path, digest = literal(control["path"]), control["sha256"]
        if path not in artifacts or known[path] != digest or sha256(path) != digest:
            raise ValueError("missing, changed or conflicting reviewed control: " + str(path))
        known[path] = digest
    python, powershell = literal(review["python"]), literal(review["powershell"])
    if (python != Path(sys.executable) or artifacts.get(python) != sha256(python)
            or artifacts.get(HERE / "launch-normal.ps1") != sha256(HERE / "launch-normal.ps1")
            or artifacts.get(Path(__file__).absolute()) != sha256(__file__)):
        raise ValueError("reviewed interpreter/caller route differs")
    if (os.environ.get("TEMP") != review["inheritedTEMP"]
            or os.environ.get("TMP") != review["inheritedTMP"]
            or any(key.upper() == "QUERY_TEST_NAME_PATTERN" for key in os.environ)
            or any(key.upper() == "QUERY_TEST_NAME_PATTERN" for key in plan["environment"])):
        raise ValueError("inherited TEMP/TMP or unfiltered selection policy differs")
    output = literal(str(args.output))
    launch_output = literal(review["launchOutput"])
    roots = [literal(plan["sourceRoot"])]
    for domain in expected["membership"].values():
        roots.extend(literal(name) for name, kind in domain.items() if kind == "directory")
    roots.extend(literal(row["target"]) for row in expected["reparse"].values())
    for path in (output, launch_output):
        if (any(path.is_relative_to(root) for root in roots) or path in artifacts
                or path.parent.resolve(strict=True) != path.parent):
            raise ValueError("preflight/launch output must be ordinary and outside observed inputs")
    fresh = (output, launch_output, literal(plan["output"]), literal(plan["sourceBoundCache"]["prefix"]))
    if len(set(fresh)) != len(fresh) or any(os.path.lexists(path) for path in fresh):
        raise ValueError("preflight, launch, target and cache endpoints must be fresh and distinct")
    seconds = review["deadlineSeconds"]
    if isinstance(seconds, bool) or not isinstance(seconds, int) or not 1 <= seconds <= 7200:
        raise ValueError("launcher deadline must be an integer from 1 to 7200 seconds")
    output.mkdir()
    observed_path = output / "binding.json"
    launcher = [str(powershell), "-NoProfile", "-File", str(HERE / "launch-normal.ps1"),
                "-Plan", str(plan_path), "-Python", str(python)]
    subprocess.run(launcher + ["-LaunchOutput", str(output / "description-launch"),
                   "-BindingOutput", str(observed_path), "-DeadlineSeconds", "60"],
                   check=True, timeout=90)
    observed = read(observed_path)
    # Sorted serialization compares exact parsed map values (including JSON
    # types); counts are display-only, never an acceptance substitute.
    if json.dumps(observed, sort_keys=True) != json.dumps(expected, sort_keys=True):
        raise ValueError("current canonical input map differs from frozen reviewed inputs")
    def git(*arguments):
        return subprocess.check_output(["git", "-C", plan["sourceRoot"], *arguments], timeout=15).decode().rstrip("\r\n")
    if (git("rev-parse", "HEAD") != expected["commit"]
            or git("rev-parse", "HEAD^{tree}") != review["git"]["tree"]
            or git("branch", "--show-current") != review["git"]["branch"]
            or git("status", "--porcelain=v1").splitlines() != review["git"]["status"]):
        raise ValueError("reviewed Git identity/branch/status differs")
    result = {"inputsMatch": True, "binding": str(observed_path), "targetStarted": False,
              "sourceMembers": len(observed["membership"]["source"]),
              "artifactMembers": len(observed["membership"]["artifact"]),
              "staticPreflightOnly": True}
    (output / "preflight.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result), flush=True)
    if args.check_only:
        return 0
    # Retain the ordinary launcher's native exit, deadline, bounded-log and
    # owned-cleanup checks. A matched static map is not a passing target result.
    return subprocess.run(launcher + ["-LaunchOutput", str(launch_output),
                          "-DeadlineSeconds", str(seconds)], timeout=seconds + 60).returncode


if __name__ == "__main__":
    sys.exit(main())
