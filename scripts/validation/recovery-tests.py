"""Focused recovery seam checks, invoked only through runner.py."""
import json
import os
from pathlib import Path
import sqlite3
import sys

from events import event, assertion

scenario = sys.argv[1]
out = Path(os.environ["VALIDATION_OUT"])
event("start", scenario=scenario)

if scenario == "recovery-wal-snapshot":
    # A real uncheckpointed WAL is deliberately required: copying only the main
    # file must be unable to satisfy the independently known committed facts.
    import recovery
    owner = out / "owned"
    owner.mkdir()
    source = owner / "live.db"
    db = sqlite3.connect(source)
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA wal_autocheckpoint=0")
        db.execute("CREATE TABLE facts (name TEXT PRIMARY KEY, value TEXT)")
        db.execute("INSERT INTO facts VALUES ('before', 'protected')")
        db.commit()
        assertion(scenario, "committed facts remain in a nonempty live WAL", Path(str(source) + "-wal").stat().st_size > 0)
        snapshot = recovery.capture_snapshot(source, owner / "snapshot", owner)
        db.execute("INSERT INTO facts VALUES ('later', 'legitimate')")
        db.commit()
        restored = recovery.restore_snapshot(owner / "snapshot", owner / "restored.db", owner)
        with sqlite3.connect(restored) as read:
            assertion(scenario, "restoration contains the consistent protected transaction only",
                      read.execute("SELECT * FROM facts ORDER BY name").fetchall() == [("before", "protected")])
            assertion(scenario, "restored snapshot passes SQLite integrity", read.execute("PRAGMA integrity_check").fetchall() == [("ok",)])
        assertion(scenario, "snapshot identifies online SQLite backup rather than main-file copying",
                  snapshot["method"] == "sqlite-online-backup" and snapshot["integrity"] == ["ok"])
        blocked = False
        try:
            recovery.restore_snapshot(owner / "snapshot", out / "outside.db", owner)
        except ValueError:
            blocked = True
        assertion(scenario, "restore refuses a path outside the declared disposable owner", blocked and not (out / "outside.db").exists())
        # A later corruption cannot silently become a different recovery point.
        with open(owner / "snapshot" / "codegraph.db", "ab") as corrupt:
            corrupt.write(b"tampered")
        blocked = False
        try:
            recovery.restore_snapshot(owner / "snapshot", owner / "bad.db", owner)
        except ValueError:
            blocked = True
        assertion(scenario, "restore refuses changed protected bytes", blocked and not (owner / "bad.db").exists())
    finally:
        db.close()
elif scenario == "recovery-exact-bundle":
    import recovery
    owner = out / "owned"
    owner.mkdir()
    fixture = owner / "original"
    for name in ["engine/lib", "launcher", "git/cmd"]:
        (fixture / name).mkdir(parents=True)
    for name, data in {"engine/node.exe": b"old-runtime", "engine/lib/worker.js": b"old-worker",
                       "launcher/proxy.mjs": b"import './bootstrap.mjs';", "launcher/bootstrap.mjs": b"old-launcher",
                       "git/cmd/git.exe": b"old-git"}.items():
        (fixture / name).write_bytes(data)
    selected = {"command": str(fixture / "engine/node.exe"),
                "args": ["--liftoff-only", str(fixture / "launcher/proxy.mjs"), "--git", str(fixture / "git/cmd/git.exe"),
                         "--engine-root", str(fixture / "engine"), "--", "serve", "--mcp"],
                "env": {"CODEGRAPH_DIR": ".codegraph", "CODEGRAPH_TELEMETRY": "0"}}
    settings = fixture / "config.toml"
    settings.write_text("[mcp_servers.codegraph]\ncommand = " + json.dumps(selected["command"]) +
                        "\nargs = " + json.dumps(selected["args"]) +
                        '\n[mcp_servers.codegraph.env]\nCODEGRAPH_DIR = ".codegraph"\nCODEGRAPH_TELEMETRY = "0"\n', encoding="utf-8")
    specification = {"engine": str(fixture / "engine"), "launcherRoot": str(fixture / "launcher"),
                     "launcherFiles": ["proxy.mjs", "bootstrap.mjs"], "gitRoot": str(fixture / "git"),
                     "settings": str(settings), "protected": str(owner / "protected"), "ownerRoot": str(owner)}
    before = recovery.file_manifest(fixture)
    manifest = recovery.protect_bundle(specification)
    restored = recovery.restore_bundle(owner / "protected", owner / "restored", owner)
    assertion(scenario, "complete old artifact/runtime/launcher/Git/settings bytes restored",
              recovery.file_manifest(owner / "restored" / "components") == manifest["files"])
    assertion(scenario, "protection and restoration leave original bytes unchanged", recovery.file_manifest(fixture) == before)
    launch = restored["launch"]
    assertion(scenario, "only external launch paths relocate while exact B files remain unchanged",
              launch["command"] == str(owner / "restored/components/engine/node.exe") and
              launch["args"][1] == str(owner / "restored/components/launcher/proxy.mjs") and
              launch["args"][3] == str(owner / "restored/components/git/cmd/git.exe") and
              launch["args"][5] == str(owner / "restored/components/engine") and launch["env"] == selected["env"])
    second_owner = out / "second-owner"
    second_owner.mkdir()
    relocated = recovery.restore_bundle(owner / "protected", second_owner / "restored", second_owner)
    assertion(scenario, "separately retained readonly protection restores into a distinct disposable owner",
              Path(relocated["launch"]["command"]).is_relative_to(second_owner))
    blocked = False
    try:
        recovery.restore_bundle(owner / "protected", out / "outside-owner", second_owner)
    except ValueError:
        blocked = True
    assertion(scenario, "bundle restore refuses an unowned destination", blocked and not (out / "outside-owner").exists())
    blocked = False
    try:
        recovery.restore_bundle(owner / "protected", owner / "protected/nested", owner)
    except ValueError:
        blocked = True
    assertion(scenario, "bundle restore never writes inside its protected input", blocked and not (owner / "protected/nested").exists())
    (owner / "protected/components/engine/lib/worker.js").write_bytes(b"new-worker")
    blocked = False
    try:
        recovery.restore_bundle(owner / "protected", owner / "bad", owner)
    except ValueError:
        blocked = True
    assertion(scenario, "changed B worker bytes reject recovery before launch", blocked and not (owner / "bad").exists())
else:
    raise ValueError("unknown recovery test scenario")

event("complete", scenario=scenario)
event("final", passed=True, scenarios=[scenario])
