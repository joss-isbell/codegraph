"""Source-bound Windows caller: python -B -X pycache_prefix=ABSENT normal.py PLAN.

The saved plan must declare the same literal sourceBoundCache.prefix and child
environment. This bootstrap executes helper source bytes, never helper pyc.
The original runner retains process ownership, deadlines and its one Guard.
"""
import hashlib
import json
import os
from pathlib import Path
import sys
import types


HERE = Path(__file__).absolute().parent
HELPERS = ("windows_witness", "report", "integrity", "runner")


def main():
    if len(sys.argv) not in (2, 4) or (len(sys.argv) == 4 and sys.argv[2] != "--report"):
        raise ValueError("usage: normal.py PLAN.json [--report RECEIPT.json]")
    if os.name != "nt":
        raise ValueError("source-bound normal caller is Windows only")
    plan_path = Path(sys.argv[1]).absolute()
    plan = json.loads(plan_path.read_text(encoding="utf-8-sig"))
    declaration = plan.get("sourceBoundCache")
    if not isinstance(declaration, dict) or set(declaration) != {"prefix"}:
        raise ValueError("declare exact sourceBoundCache.prefix before interpreter launch")
    prefix = declaration["prefix"]
    if (not isinstance(prefix, str) or not Path(prefix).is_absolute()
            or ".." in Path(prefix).parts or os.path.lexists(prefix)
            or not Path(prefix).parent.is_dir()
            or sys.pycache_prefix != prefix or os.environ.get("PYTHONPYCACHEPREFIX") != prefix
            or not sys.dont_write_bytecode or os.environ.get("PYTHONDONTWRITEBYTECODE") != "1"):
        raise ValueError("launch -B with the declared literal fresh absent PYTHONPYCACHEPREFIX")
    environment = plan.get("environment", {})
    for name, value in (("PYTHONPYCACHEPREFIX", prefix), ("PYTHONDONTWRITEBYTECODE", "1")):
        if (environment.get(name) != value
                or any(key.upper() == name and key != name for key in environment)):
            raise ValueError("saved plan must preserve exact child " + name)
    # Capture all executed helper bytes before importing any helper. The runner
    # reconciles these digests against its full post-arm immutable baseline.
    sources = {name: (HERE / (name + ".py")).read_bytes() for name in HELPERS}
    hashes = {str(HERE / (name + ".py")): hashlib.sha256(raw).hexdigest()
              for name, raw in sources.items()}
    hashes[str(HERE / "normal.py")] = hashlib.sha256((HERE / "normal.py").read_bytes()).hexdigest()
    sys.path.insert(0, str(HERE))
    for name, raw in sources.items():
        module = types.ModuleType(name)
        module.__file__ = str(HERE / (name + ".py"))
        module.__validation_source_sha256__ = hashes[module.__file__]
        sys.modules[name] = module
        exec(compile(raw, module.__file__, "exec"), module.__dict__)
    runner = sys.modules["runner"]
    runner.SOURCE_BOUND_BOOTSTRAP = {"planPath": str(plan_path), "prefix": prefix, "helpers": hashes}
    if len(sys.argv) == 4:
        receipt = Path(sys.argv[3]).absolute()
        if receipt.parent / "plan.json" != plan_path:
            raise ValueError("report must use the receipt's saved plan.json")
        result = sys.modules["report"].consume(receipt)
        print(json.dumps(result, indent=2))
        return 0 if result["outcome"] == "pass" else 1
    return runner.run(plan_path)


if __name__ == "__main__":
    sys.exit(main())
