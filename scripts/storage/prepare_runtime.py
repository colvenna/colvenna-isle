#!/usr/bin/env python3
"""Build a fixed ARM64 storage artifact and an offline-installing runtime image."""

import argparse
import gzip
import hashlib
import json
import os
import platform
import re
import shutil
import subprocess
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]


def run(*args, cwd=None, env=None):
    subprocess.run(args, cwd=cwd, env=env, check=True)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def download(url, expected):
    with urlopen(Request(url, headers={"User-Agent": "DuckDB/v1.5.6(linux_arm64) Python"}), timeout=60) as response:
        data = response.read()
    if hashlib.sha256(data).hexdigest() != expected:
        raise RuntimeError(f"Downloaded content differs from the lock: {url}")
    return data


def git_identity(path):
    return subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--image", default="colvenna-isle-storage:local")
    parser.add_argument("--jobs", type=int, default=8)
    args = parser.parse_args()
    workspace = args.workspace.resolve()
    if workspace == ROOT or ROOT in workspace.parents:
        parser.error("use an external workspace for source/build/downloaded artifacts")
    lock = json.loads((ROOT / "runtime/storage-lock.json").read_text())
    if platform.machine() != lock["architecture"]:
        parser.error("this lock qualifies aarch64 only")
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    tool_versions = {}
    for tool, expected in lock["build_tools"].items():
        output = subprocess.check_output([tool, "--version"], text=True).splitlines()[0]
        if not re.search(r"(?<!\d)" + re.escape(expected) + r"(?!\d)", output):
            raise RuntimeError(f"Build tool {tool} differs from the lock: {output}")
        tool_versions[tool] = output
    workspace.mkdir(parents=True, exist_ok=True)
    upstream = workspace / "upstream"
    source = lock["object_storage"]
    if not upstream.exists():
        run("git", "clone", "--no-checkout", source["source_repository"], str(upstream))
        run("git", "checkout", "--detach", source["source_commit"], cwd=upstream)
        run("git", "submodule", "update", "--init", "--depth", "1", cwd=upstream)
    if git_identity(upstream) != source["source_commit"]:
        raise RuntimeError("Existing workspace has a different source identity; it was preserved")
    dirty = subprocess.check_output(["git", "status", "--porcelain"], cwd=upstream, text=True)
    if dirty:
        raise RuntimeError("Existing source has changes; it was preserved")
    if git_identity(upstream / "duckdb") != lock["duckdb"]["source_commit"]:
        raise RuntimeError("DuckDB submodule identity differs from the lock")
    if git_identity(upstream / "extension-ci-tools") != source["extension_ci_tools_commit"]:
        raise RuntimeError("Extension CI tools identity differs from the lock")
    if sha256(upstream / "rust/Cargo.lock") != source["cargo_lock_sha256"]:
        raise RuntimeError("Rust lock differs from the source matrix")
    run("git", "fetch", "--depth", "1", "origin", "tag", "v" + lock["duckdb"]["version"], cwd=upstream / "duckdb")
    tag = subprocess.check_output(
        ["git", "rev-parse", "v" + lock["duckdb"]["version"] + "^{commit}"],
        cwd=upstream / "duckdb", text=True,
    ).strip()
    if tag != lock["duckdb"]["source_commit"]:
        raise RuntimeError("Release tag does not name the locked DuckDB commit")
    build = upstream / "build/colvenna"
    run(
        "cmake", "-S", str(upstream / "duckdb"), "-B", str(build), "-G", "Ninja",
        "-DCMAKE_BUILD_TYPE=Release", "-DDUCKDB_EXTENSION_CONFIGS=" + str(upstream / "extension_config.cmake"),
        "-DEXTENSION_STATIC_BUILD=1", "-DBUILD_EXTENSIONS_ONLY=0", "-DBUILD_MAIN_DUCKDB_LIBRARY=1",
        "-DBUILD_UNITTESTS=0", "-DBUILD_SHELL=0", "-DENABLE_EXTENSION_AUTOLOADING=0",
        "-DENABLE_EXTENSION_AUTOINSTALL=0",
    )
    env = os.environ.copy()
    env["CARGO_BUILD_JOBS"] = str(args.jobs)
    run("cmake", "--build", str(build), "--target", "duckdb_object_storage_loadable_extension",
        "--parallel", str(args.jobs), env=env)
    if sha256(upstream / "rust/Cargo.lock") != source["cargo_lock_sha256"]:
        raise RuntimeError("Build changed the Rust dependency lock")
    artifacts = workspace / "artifacts"
    artifacts.mkdir(exist_ok=True)
    shutil.copy2(build / "extension/duckdb_object_storage/duckdb_object_storage.duckdb_extension",
                 artifacts / "duckdb_object_storage.duckdb_extension")
    for extension in lock["extensions"]:
        data = gzip.decompress(download(extension["url"], extension["archive_sha256"]))
        target = artifacts / f"{extension['name']}.duckdb_extension"
        target.write_bytes(data)
        if sha256(target) != extension["sha256"]:
            raise RuntimeError(f"Extension checksum mismatch: {extension['name']}")
    wheel = artifacts / lock["duckdb"]["wheel_url"].rsplit("/", 1)[1]
    wheel.write_bytes(download(lock["duckdb"]["wheel_url"], lock["duckdb"]["wheel_sha256"]))
    manifest = {
        "schema_version": 1,
        "duckdb_version": lock["duckdb"]["version"],
        "platform": lock["platform"],
        "source_commit": source["source_commit"],
        "lock_sha256": sha256(ROOT / "runtime/storage-lock.json"),
        "link_mode": "static",
        "build_tools": tool_versions,
        "files": {p.name: sha256(p) for p in sorted(artifacts.glob("*.duckdb_extension"))},
    }
    (artifacts / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    context = workspace / "image-context"
    (context / "scripts/storage").mkdir(parents=True, exist_ok=True)
    (context / "artifacts").mkdir(exist_ok=True)
    for file in list(artifacts.glob("*.duckdb_extension")) + [wheel, artifacts / "manifest.json"]:
        shutil.copy2(file, context / "artifacts" / file.name)
    shutil.copy2(ROOT / "scripts/storage/worker.py", context / "scripts/storage/worker.py")
    shutil.copy2(ROOT / "src/colvenna_isle/region.py", context / "scripts/storage/region.py")
    shutil.copy2(ROOT / "runtime/Dockerfile.storage", context / "Dockerfile")
    run("docker", "build", "--build-arg", "RUNTIME_BASE=" + lock["runtime_base"],
        "--label", "org.opencontainers.image.source=" + source["source_repository"],
        "--label", "org.opencontainers.image.revision=" + source["source_commit"],
        "--tag", args.image, str(context))
    image = json.loads(subprocess.check_output(["docker", "image", "inspect", args.image], text=True))[0]
    manifest["image"] = {"reference": args.image, "id": image["Id"], "architecture": image["Architecture"],
                         "base": lock["runtime_base"], "user": image["Config"]["User"]}
    manifest["worker_sha256"] = sha256(ROOT / "scripts/storage/worker.py")
    manifest["colvenna_sources"] = {
        name: sha256(ROOT / name) for name in
        ["runtime/storage-lock.json", "runtime/Dockerfile.storage", "scripts/storage/prepare_runtime.py",
         "scripts/storage/worker.py", "src/colvenna_isle/region.py"]
    }
    (workspace / "build-receipt.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
