#!/usr/bin/env python3
"""Build and check a development artifact bundle from one clean committed source."""

import argparse
import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import subprocess
import sys
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_REGION = json.loads((ROOT / "tests/fixture-region.json").read_text())["region"]


def run(command, *, cwd=ROOT, env=None):
    return subprocess.run(command, cwd=cwd, env=env, check=True, text=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--storage-receipt", type=Path)
    parser.add_argument("--storage-image")
    args = parser.parse_args()
    output = args.output.resolve()
    if output == ROOT or ROOT in output.parents or output.exists():
        parser.error("use a new external output directory; original artifacts are never overwritten")
    if bool(args.storage_receipt) != bool(args.storage_image):
        parser.error("storage image and build receipt must be provided together")
    if run(["git", "status", "--porcelain"]).strip():
        parser.error("commit source changes before building")
    if sys.version_info[:2] != (3, 12) or platform.machine() != "aarch64":
        parser.error("current locks require Python 3.12 on aarch64")
    head = run(["git", "rev-parse", "HEAD"]).strip()
    for package, version in {"setuptools": "80.9.0", "wheel": "0.45.1"}.items():
        if importlib.metadata.version(package) != version:
            raise RuntimeError(f"Build package differs from the lock: {package}")
    output.mkdir(parents=True)
    archive = output / f"colvenna-isle-{head}.tar"
    subprocess.run(["git", "archive", "--format=tar", "-o", str(archive), head], cwd=ROOT, check=True)
    source = output / "source"
    source.mkdir()
    with tarfile.open(archive) as tar:
        tar.extractall(source, filter="data")
    version = "0.0.0.dev0+g" + head
    (source / "src/colvenna_isle/_version.py").write_text(f'__version__ = "{version}"\n')
    log = output / "validation.log"
    checks = [
        [sys.executable, "-m", "pip", "check"],
        [sys.executable, "scripts/check_docs.py"],
        [sys.executable, "scripts/check_contracts.py", "--source-commit", head],
        [sys.executable, "-m", "unittest", "discover", "-s", "tests/control", "-v"],
        [sys.executable, "-m", "unittest", "discover", "-s", "tests/intake", "-v"],
        [sys.executable, "-m", "unittest", "discover", "-s", "tests/writer", "-v"],
        [sys.executable, "examples/orders/run_local.py"],
    ]
    with log.open("x") as stream:
        for command in checks:
            try:
                stream.write(run(command, cwd=source, env=dict(os.environ, PYTHONPATH=str(source / "src"),
                    COLVENNA_REGION=FIXTURE_REGION, AWS_REGION=FIXTURE_REGION, AWS_DEFAULT_REGION=FIXTURE_REGION)))
            except subprocess.CalledProcessError as error:
                stream.write(error.stdout or "")
                (output / "failure.json").write_text(json.dumps({
                    "status": "FAIL", "source_commit": head, "command": command,
                    "returncode": error.returncode, "log": log.name,
                }, indent=2) + "\n")
                raise
            stream.flush()
    wheelhouse = output / "wheel"
    wheelhouse.mkdir()
    wheel_log = run([sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-build-isolation",
                     "--no-index", "--wheel-dir", str(wheelhouse), str(source)], cwd=output)
    (output / "wheel-build.log").write_text(wheel_log)
    wheel = next(wheelhouse.glob("*.whl"))
    installed = output / "installed"
    (output / "install.log").write_text(run([
        sys.executable, "-m", "pip", "install", "--no-index", "--no-deps", "--target",
        str(installed), str(wheel)], cwd=output))
    env = dict(os.environ, PYTHONPATH=str(installed), COLVENNA_REGION=FIXTURE_REGION,
               AWS_REGION=FIXTURE_REGION, AWS_DEFAULT_REGION=FIXTURE_REGION)
    package_version = run([sys.executable, "-c",
                          "from colvenna_isle._version import __version__; print(__version__)"],
                          cwd=output, env=env).strip()
    assert package_version == version
    (output / "installed-check.log").write_text(run([
        sys.executable, "-m", "colvenna_isle.contracts", "generate", "--check"], cwd=output, env=env))
    licenses = []
    for distribution in sorted(importlib.metadata.distributions(), key=lambda d: d.metadata["Name"].lower()):
        files = distribution.files or []
        notices = []
        for file in files:
            if any(name in str(file).lower() for name in ["license", "copying", "notice"]):
                path = distribution.locate_file(file)
                if path.is_file():
                    target = output / "third-party" / distribution.metadata["Name"] / str(file)
                    # RECORD paths can escape site-packages; copy notices only inside dist-info/package.
                    if ".." in Path(file).parts:
                        continue
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(path, target)
                    notices.append({"path": str(target.relative_to(output)), "sha256": digest(target)})
        licenses.append({"name": distribution.metadata["Name"], "version": distribution.version,
                         "license": distribution.metadata.get("License-Expression")
                                    or distribution.metadata.get("License", "See bundled notices"),
                         "notices": notices})
    image = None
    if args.storage_receipt:
        storage = json.loads(args.storage_receipt.read_text())
        lock = json.loads((source / "runtime/storage-lock.json").read_text())
        for name, expected in storage["colvenna_sources"].items():
            if digest(source / name) != expected:
                raise RuntimeError(f"Storage source differs from the build receipt: {name}")
        inspected = json.loads(run(["docker", "image", "inspect", args.storage_image]))[0]
        if inspected["Id"] != storage["image"]["id"] or inspected["Architecture"] != "arm64":
            raise RuntimeError("Storage image differs from its receipt")
        # Verify the installed extension files rather than trusting a copied receipt.
        code = ("import hashlib,json,pathlib,duckdb; "
                "p=pathlib.Path('/opt/colvenna/artifacts'); "
                "print(json.dumps({'version':duckdb.__version__,"
                "'files':{f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in p.glob('*.duckdb_extension')}}))")
        actual = json.loads(run(["docker", "run", "--rm", "--network", "none", "--read-only",
                                 "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                                 "--entrypoint", "python", args.storage_image, "-c", code]))
        if actual["version"] != lock["duckdb"]["version"] or actual["files"] != storage["files"]:
            raise RuntimeError("Installed storage binaries differ")
        load_code = ("import duckdb; c=duckdb.connect(config={'allow_unsigned_extensions':True,"
                     "'autoinstall_known_extensions':False,'autoload_known_extensions':False}); "
                     "[c.execute(\"LOAD '/opt/colvenna/artifacts/\"+x+\".duckdb_extension'\") "
                     "for x in ['httpfs','aws','duckdb_object_storage']]; print(c.execute('SELECT 42').fetchall())")
        (output / "storage-import.log").write_text(run([
            "docker", "run", "--rm", "--network", "none", "--read-only", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--entrypoint", "python", args.storage_image, "-c", load_code]))
        shutil.copy2(args.storage_receipt, output / "storage-build-receipt.json")
        image = {"id": inspected["Id"], "base": lock["runtime_base"], "architecture": "arm64",
                 "user": inspected["Config"]["User"], "installed_binaries": actual,
                 "provenance": "storage-build-receipt.json",
                 "registry_digest": None}
    manifest = {
        "schema_version": 1, "status": "PASS", "scope": "DEVELOPMENT_ARTIFACT_NOT_PRODUCT_RELEASE",
        "source_commit": head, "source_tree": run(["git", "rev-parse", "HEAD^{tree}"]).strip(),
        "version": version, "python": platform.python_version(), "architecture": platform.machine(),
        "source_archive": archive.name, "wheel": str(wheel.relative_to(output)),
        "checks": ["pip check", "docs/receipts", "contract groups", "control HTTP/metadata contracts",
                   "intake admission and uncertainty boundaries", "writer identity and termination boundaries",
                   "orders full/incremental",
                   "wheel installation and generated schema import"] + (["offline native extension import"] if image else []),
        "sources": {name: digest(ROOT / name) for name in run(["git", "ls-files"]).splitlines()},
        "licenses": licenses, "storage_image": image,
        "artifacts": {str(p.relative_to(output)): digest(p) for p in sorted(output.rglob("*"))
                      if p.is_file() and "source" not in p.relative_to(output).parts
                      and "installed" not in p.relative_to(output).parts},
        "not_run": ["GitHub Actions (reported separately)", "AWS deployment", "business services",
                    "product release acceptance", "byte-identical independent native rebuild"],
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"status": "PASS", "source_commit": head, "version": version,
                      "manifest": str(output / "manifest.json"), "storage_image": image and image["id"]}))


if __name__ == "__main__":
    main()
