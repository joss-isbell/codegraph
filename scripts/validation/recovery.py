"""Local, disposable-only exact-artifact recovery tools. Never activate an install.

Snapshot creation uses SQLite's online backup API, including committed WAL state.
Restoration always creates a new owned path; it cannot overwrite a live database.
Invoke executable operations only through the qualified validation supervisor.
"""
import json
from contextlib import closing
from pathlib import Path
import shutil
import sqlite3
import sys
import tomllib

from report import digest


def owned(path, owner):
    path, owner = Path(path).resolve(), Path(owner).resolve()
    if path == owner or not path.is_relative_to(owner):
        raise ValueError("recovery path must be beneath its declared disposable owner")
    return path


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")


def read_database(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=5)


def database_identity(db):
    tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    return {
        "applicationId": db.execute("PRAGMA application_id").fetchone()[0],
        "userVersion": db.execute("PRAGMA user_version").fetchone()[0],
        "schema": db.execute("SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name").fetchall(),
        "schemaVersions": db.execute("SELECT * FROM schema_versions ORDER BY version").fetchall() if "schema_versions" in tables else [],
        "projectMetadata": db.execute("SELECT * FROM project_metadata ORDER BY key").fetchall() if "project_metadata" in tables else [],
    }


def capture_snapshot(source, destination, owner):
    source, destination = owned(source, owner), owned(destination, owner)
    if not source.is_file():
        raise ValueError("snapshot source must be an existing disposable database")
    destination.mkdir(parents=True, exist_ok=False)
    protected = destination / "codegraph.db"
    provenance = {suffix or "main": {"sha256": digest(file), "bytes": file.stat().st_size}
                  for suffix in ["", "-wal", "-shm"] if (file := Path(str(source) + suffix)).is_file()}
    with closing(read_database(source)) as original:
        original.execute("BEGIN")
        identity = database_identity(original)
        with closing(sqlite3.connect(protected)) as copied:
            original.backup(copied, pages=128, sleep=0.01)
            integrity = [row[0] for row in copied.execute("PRAGMA integrity_check")]
            if integrity != ["ok"] or database_identity(copied) != identity:
                raise ValueError("snapshot integrity or transaction identity failed")
            # The snapshot has no live owners. Make it self-contained so no
            # copied WAL/SHM or service lock can be mistaken for required state.
            copied.execute("PRAGMA journal_mode=DELETE")
        original.rollback()
    result = {"schema": "codegraph-consistent-snapshot-v1", "method": "sqlite-online-backup",
              "source": str(source), "sourceSidecarsAtCapture": provenance,
              "databaseSha256": digest(protected), "bytes": protected.stat().st_size,
              "identity": identity, "integrity": integrity, "snapshotJournalMode": "delete"}
    save(destination / "snapshot.json", result)
    return result


def restore_snapshot(snapshot, destination, owner):
    snapshot, destination = owned(snapshot, owner), owned(destination, owner)
    metadata = json.loads((snapshot / "snapshot.json").read_text(encoding="utf-8"))
    protected = snapshot / "codegraph.db"
    if metadata.get("schema") != "codegraph-consistent-snapshot-v1" or metadata.get("method") != "sqlite-online-backup":
        raise ValueError("unsupported snapshot provenance")
    if digest(protected) != metadata["databaseSha256"]:
        raise ValueError("protected database bytes changed")
    with closing(read_database(protected)) as db:
        if [row[0] for row in db.execute("PRAGMA integrity_check")] != ["ok"]:
            raise ValueError("protected database integrity failed")
        if json.loads(json.dumps(database_identity(db))) != metadata["identity"]:
            raise ValueError("protected database identity changed")
    if any(Path(str(destination) + suffix).exists() for suffix in ["", "-wal", "-shm"]):
        raise ValueError("restore requires an absent database and absent sidecars")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with open(protected, "rb") as source, open(destination, "xb") as target:
        shutil.copyfileobj(source, target)
    if digest(destination) != metadata["databaseSha256"]:
        raise ValueError("restored database bytes differ")
    return destination


def file_manifest(root):
    root = Path(root).resolve()
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
            raise ValueError("recovery components cannot contain reparse/link entries")
        if path.is_file():
            result[path.relative_to(root).as_posix()] = {"sha256": digest(path), "bytes": path.stat().st_size}
    return result


def directory_manifest(root):
    return sorted(path.relative_to(root).as_posix() for path in Path(root).rglob("*") if path.is_dir())


def original_inventory(sources, launcher_files):
    inventory = {}
    for component in ["engine", "git"]:
        inventory.update({component + "/" + key: value for key, value in file_manifest(sources[component]).items()})
    launcher = Path(sources["launcher"])
    for relative in launcher_files:
        path = owned(launcher / relative, launcher)
        if path.is_symlink() or not path.is_file():
            raise ValueError("launcher closure entry must be a regular file")
        inventory["launcher/" + relative] = {"sha256": digest(path), "bytes": path.stat().st_size}
    settings = Path(sources["settings"])
    inventory["settings/config.toml"] = {"sha256": digest(settings), "bytes": settings.stat().st_size}
    return inventory


def relocated_launch(selected, sources, destination):
    def relocate(value):
        path = Path(value)
        if not path.is_absolute():
            return value
        resolved = path.resolve()
        for component, source in sources.items():
            source = Path(source).resolve()
            if component == "settings":
                continue
            if resolved == source or resolved.is_relative_to(source):
                return str(destination / component / resolved.relative_to(source))
        raise ValueError("absolute launch path is outside the preserved B closure")
    return {"command": relocate(selected["command"]), "args": [relocate(value) for value in selected["args"]],
            "env": selected.get("env", {})}


def protect_bundle(specification):
    owner = Path(specification["ownerRoot"]).resolve()
    destination = owned(specification["protected"], owner)
    sources = {"engine": str(Path(specification["engine"]).resolve()),
               "launcher": str(Path(specification["launcherRoot"]).resolve()),
               "git": str(Path(specification["gitRoot"]).resolve()),
               "settings": str(Path(specification["settings"]).resolve())}
    for source in sources.values():
        if destination == Path(source) or destination.is_relative_to(source):
            raise ValueError("protection destination cannot be inside an original B component")
    selected = tomllib.loads(Path(sources["settings"]).read_text(encoding="utf-8"))["mcp_servers"]["codegraph"]
    if not isinstance(selected.get("command"), str) or not isinstance(selected.get("args"), list):
        raise ValueError("saved CodeGraph launch recipe is missing")
    # Resolve the external rendezvous only. All copied artifact bytes stay exact.
    relocated_launch(selected, sources, destination / "components")
    original = original_inventory(sources, specification["launcherFiles"])
    launcher = Path(sources["launcher"])
    settings = Path(sources["settings"])
    destination.mkdir(parents=True, exist_ok=False)
    components = destination / "components"
    for component in ["engine", "git"]:
        shutil.copytree(sources[component], components / component)
    for relative in specification["launcherFiles"]:
        target = components / "launcher" / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(launcher / relative, target)
    (components / "settings").mkdir()
    shutil.copy2(settings, components / "settings/config.toml")
    if file_manifest(components) != original:
        raise ValueError("protected B differs from original component inventory")
    # Re-read the originals after copying so source mutation cannot qualify B.
    after = original_inventory(sources, specification["launcherFiles"])
    if after != original:
        raise ValueError("original B changed during preservation")
    manifest = {"schema": "codegraph-exact-B-v1", "sources": sources, "selectedSettings": selected,
                "files": original, "directories": directory_manifest(components),
                "launcherClosure": specification["launcherFiles"],
                "liveInheritedEnvironment": "unknown; saved settings do not prove older owner settings",
                "heldIndexAccess": "none", "serviceChanges": "none", "settingsChanges": "none"}
    save(destination / "manifest.json", manifest)
    return manifest


def restore_bundle(protected, destination, owner):
    # The immutable protected input may be retained in another owned checkout;
    # only the new writable destination belongs to this operation's owner.
    protected, destination = Path(protected).resolve(), owned(destination, owner)
    if destination.is_relative_to(protected):
        raise ValueError("restoration cannot write inside the protected B input")
    manifest_path = protected / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema") != "codegraph-exact-B-v1":
        raise ValueError("unsupported B artifact manifest")
    components = protected / "components"
    if file_manifest(components) != manifest["files"] or directory_manifest(components) != manifest["directories"]:
        raise ValueError("protected B artifact bytes or directory inventory changed")
    launch = relocated_launch(manifest["selectedSettings"], manifest["sources"], destination / "components")
    destination.mkdir(parents=True, exist_ok=False)
    shutil.copytree(components, destination / "components")
    if file_manifest(destination / "components") != manifest["files"]:
        raise ValueError("restored B artifact bytes differ")
    result = {"schema": "codegraph-restored-B-v1", "protectedManifestSha256": digest(manifest_path),
              "originalLaunch": manifest["selectedSettings"], "launch": launch,
              "relocation": "external command/argument paths only; B artifact/config bytes unchanged"}
    save(destination / "restored-launch.json", result)
    return result


if __name__ == "__main__":
    if sys.argv[1] == "protect":
        import os
        from events import event, assertion
        scenario = "protect-exact-B"
        event("start", scenario=scenario)
        specification = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8-sig"))
        manifest = protect_bundle(specification)
        save(Path(os.environ["VALIDATION_OUT"]) / "protection.json",
             {"protectedManifest": str(Path(specification["protected"]) / "manifest.json"),
              "manifestSha256": digest(Path(specification["protected"]) / "manifest.json"),
              "files": len(manifest["files"]), "heldIndexAccess": "none", "serviceChanges": "none"})
        assertion(scenario, "complete exact installed B closure and private saved settings preserved", bool(manifest["files"]))
        event("complete", scenario=scenario)
        event("final", passed=True, scenarios=[scenario])
    elif sys.argv[1] == "snapshot":
        capture_snapshot(*sys.argv[2:5])
    elif sys.argv[1] == "restore-snapshot":
        restore_snapshot(*sys.argv[2:5])
    else:
        raise ValueError("unknown recovery operation")
