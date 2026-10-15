"""Bootstrap/build/freeze adapters: success requires explicit final assertions."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import tarfile

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from events import event, assertion
from runner import write
from report import digest

stage = sys.argv[1]
root = Path(os.environ["BASELINE_ROOT"]).resolve()
runtime = Path(os.environ.get("BASELINE_RUNTIME", str(root.parent / "baseline-runtime")))
default_node = runtime / ("node.exe" if os.name == "nt" else "bin/node")
node = Path(os.environ.get("BASELINE_NODE", str(default_node)))
npm_cli = runtime / "node_modules" / "npm" / "bin" / "npm-cli.js"
if not npm_cli.is_file():
    npm_cli = runtime / ("npm" if os.name == "nt" else "lib/node_modules/npm") / "bin" / "npm-cli.js"
out = Path(os.environ["VALIDATION_OUT"])
event("start", scenario=stage)
if stage == "clone":
    root.mkdir(exist_ok=False)
    with tarfile.open(root.parent / "baseline-source.tar") as archive:
        archive.extractall(root, filter="data")
    commit = os.environ["BASELINE_SOURCE_COMMIT"]
    (root / "baseline-source-commit.txt").write_text(commit + "\n")
    assertion(stage, "fresh exact source archive cloned without modifying protected baseline", (root / "package-lock.json").is_file())
elif stage == "bootstrap":
    env = {**os.environ, "npm_config_cache": str(out / "npm-cache"), "npm_config_update_notifier": "false"}
    command = [str(node), str(npm_cli), "ci", "--no-audit", "--no-fund"]
    result = subprocess.run(command, cwd=root, env=env)
    assertion(stage, "npm ci completed", result.returncode == 0, exitCode=result.returncode)
    lock = json.loads((root / "package-lock.json").read_text())
    installed = [p for p in lock["packages"] if p and (root / p / "package.json").is_file()]
    write(out / "dependencies.json", {"lockSha256": digest(root / "package-lock.json"), "installed": installed,
                                      "packages": lock["packages"]})
    assertion(stage, "compiler and both test-project dependencies installed",
              all((root / "node_modules" / package / "package.json").is_file()
                  for package in ["typescript", "vitest", "jsdom", "@sveltejs/vite-plugin-svelte"]))
elif stage == "build":
    env = {k: v for k, v in os.environ.items() if k.upper() != "PATH"}
    env["PATH"] = str(runtime) + os.pathsep + os.environ["PATH"]
    loader = os.environ.get("BASELINE_BUILD_CONFIG_LOADER", "bundle")
    if loader not in ["bundle", "runner"]:
        raise ValueError("unsupported baseline build config loader")
    # Vite's default loader creates temporary configuration files inside
    # ui/node_modules. Its supported runner loader keeps the dependency input
    # trees read-only while executing the same complete build steps.
    commands = ([[str(node), str(npm_cli), "run", "build"]] if loader == "bundle" else [
        [str(node), str(root / "node_modules/typescript/bin/tsc")],
        [str(node), str(npm_cli), "run", "copy-assets"],
        [str(node), str(npm_cli), "run", "build", "--workspace", "ui", "--", "--configLoader", "runner"],
        [str(node), str(root / "scripts/check-ui-build.mjs")],
        [str(node), "-e", "require('fs').chmodSync('dist/bin/codegraph.js', 0o755)"]])
    write(out / "build-input-mode.json", {"viteConfigLoader": loader, "commands": commands})
    for command in commands:
        result = subprocess.run(command, cwd=root, env=env)
        assertion(stage, "complete build step finished", result.returncode == 0, command=command, exitCode=result.returncode)
    required = [root / "dist" / "db" / "schema.sql", root / "dist" / "viewer" / "index.html"]
    required += [root / "dist" / "extraction" / "wasm" / p.name for p in (root / "src" / "extraction" / "wasm").glob("*.wasm")]
    assertion(stage, "schema, every source grammar and viewer shipped", len(required) > 2 and all(p.is_file() for p in required),
              assets=[str(p.relative_to(root)) for p in required])
    write(out / "required-assets.json", [{"path": str(p.relative_to(root)), "sha256": digest(p)} for p in required])
    expected_path = os.environ.get("BASELINE_EXPECTED_ARTIFACT")
    if expected_path:
        expected = json.loads(Path(expected_path).read_text())["files"][str(root / "dist")]
        actual = {str(p.relative_to(root / "dist")).replace("\\", "/"): digest(p)
                  for p in sorted((root / "dist").rglob("*")) if p.is_file()}
        assertion(stage, "read-only dependency build matches every ordinary-build shipped file", actual == expected, files=len(actual))
elif stage == "freeze":
    identities = {}
    # npm workspaces can retain dependencies below their own node_modules even
    # when most packages are hoisted. Keep each outer dependency tree once;
    # nested node_modules are already covered by that tree's recursive hashes.
    dependencies = sorted(directory for directory in root.rglob("node_modules")
                          if directory.is_dir() and "node_modules" not in directory.relative_to(root).parts[:-1])
    for directory in [root / "dist", *dependencies, runtime]:
        identities[str(directory)] = {str(p.relative_to(directory)).replace("\\", "/"): digest(p)
                                     for p in sorted(directory.rglob("*")) if p.is_file()}
    versions = subprocess.check_output([str(node), "-p", "JSON.stringify({versions:process.versions,platform:process.platform,arch:process.arch})"])
    artifact_name = os.environ.get("BASELINE_ARTIFACT_NAME", "baseline-artifact-v1.json")
    metadata = {"version": artifact_name.removesuffix(".json"), "sourceCommit": (root / "baseline-source-commit.txt").read_text().strip(),
                "files": identities, "runtime": json.loads(versions), "sourceArchiveSha256": digest(root.parent / "baseline-source.tar"),
                "packageLockSha256": digest(root / "package-lock.json")}
    write(root.parent / artifact_name, metadata)
    write(out / "artifact.json", metadata)
    assertion(stage, "complete built baseline and dependencies retained", root / "node_modules" in dependencies and all(identities.values()),
              dependencyRoots=[str(directory.relative_to(root)).replace("\\", "/") for directory in dependencies])
elif stage == "linux-prerequisite":
    result = subprocess.run(["docker", "info", "--format", "{{json .}}"], capture_output=True, text=True)
    print(result.stdout)
    print(result.stderr, file=sys.stderr)
    write(out / "docker-info.json", {"exitCode": result.returncode, "stdout": result.stdout, "stderr": result.stderr})
    assertion(stage, "Linux Docker engine available for --init lifecycle proof", result.returncode == 0)
elif stage == "archive":
    # Source, dependencies, built assets and the complete Node distribution are
    # retained independently of disposable container/project state.
    destination = out / "baseline-linux-artifact.tar"
    with tarfile.open(destination, "w") as archive:
        archive.add(root, arcname="work", filter=lambda item: None if "__pycache__" in Path(item.name).parts else item)
        archive.add(runtime, arcname="runtime", filter=lambda item: None if "__pycache__" in Path(item.name).parts else item)
    write(out / "archive.json", {"path": destination.name, "sha256": digest(destination), "bytes": destination.stat().st_size})
    assertion(stage, "complete Linux built baseline and dependencies retained", destination.stat().st_size > 0)
elif stage == "build-environment":
    env = {k: v for k, v in os.environ.items() if k.upper() != "PATH"}
    env["PATH"] = str(runtime) + os.pathsep + os.environ["PATH"]
    result = subprocess.run([str(node), str(npm_cli), "run", "env", "--workspace", "ui"], cwd=root,
                            env=env, capture_output=True, text=True)
    selected = [line for line in result.stdout.splitlines() if line.partition("=")[0].upper() in ["PATH", "PATHEXT"]]
    write(out / "selected-build-environment.json", selected)
    for line in selected:
        print(line)
    assertion(stage, "workspace environment command succeeded", result.returncode == 0)
else:
    raise ValueError(stage)
event("complete", scenario=stage)
event("final", passed=True, scenarios=[stage])
