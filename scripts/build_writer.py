#!/usr/bin/env python3
"""Build a writer or query image from the locked ZIP and qualified native image."""

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
# Suites run against the AWS documentation example account, never a real one.
FIXTURE_ACCOUNT = "123456789012"
FIXTURE_REGION = json.loads((ROOT / "tests/fixture-region.json").read_text())["region"]


def run(command):
    return subprocess.check_output(command, text=True, cwd=ROOT).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--service", choices=["writer", "query"], default="writer")
    parser.add_argument("--storage-image", help="local image reference; must match the locked immutable image ID")
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists() or output == ROOT or ROOT in output.parents:
        parser.error("use a new external output directory")
    if run(["git", "status", "--porcelain"]):
        parser.error("commit source before building")
    head = run(["git", "rev-parse", "HEAD"])
    manifest = json.loads((args.bundle / "manifest.json").read_text())
    archive = args.bundle / manifest["zip"]
    if (manifest["status"] != "PASS" or manifest["source_commit"] != head
            or hashlib.sha256(archive.read_bytes()).hexdigest() != manifest["zip_sha256"]):
        raise RuntimeError("Application ZIP/source mismatch")
    lock = json.loads((ROOT / "runtime/writer-lock.json").read_text())
    account = os.environ.get("COLVENNA_ACCOUNT", "")
    if not re.fullmatch(r"[0-9]{12}", account):
        parser.error("set COLVENNA_ACCOUNT: the native storage image is pulled from that account's ECR")
    from colvenna_isle.region import region

    storage = args.storage_image or (f"{account}.dkr.ecr.{region()}.amazonaws.com/{lock['storage_repository']}"
                                    f"@{lock['storage_digest']}")
    base = json.loads(run(["docker", "image", "inspect", storage]))[0]
    if base["Id"] != lock["storage_digest"] or base["Architecture"] != "arm64":
        raise RuntimeError("Native base identity mismatch")
    output.mkdir(parents=True)
    context = output / "context"
    context.mkdir()
    # Extract the verified ZIP, rather than trusting a mutable staging directory.
    import zipfile
    with zipfile.ZipFile(archive) as value:
        if any(Path(name).is_absolute() or ".." in Path(name).parts for name in value.namelist()):
            raise RuntimeError("Unsafe ZIP path")
        value.extractall(context / "package")
    shutil.copy2(args.bundle / "manifest.json", context / "manifest.json")
    shutil.copy2(ROOT / "runtime/Dockerfile.writer", context / "Dockerfile")
    shutil.copy2(ROOT / "runtime/columnar-requirements.lock", context / "columnar-requirements.lock")
    tag = "colvenna-isle-" + args.service + ":" + head
    with (output / "build.log").open("x") as stream:
        subprocess.run(["docker", "build", "--platform", "linux/arm64",
            "--build-arg", "STORAGE_IMAGE=" + storage,
            "--build-arg", "COLUMNAR_EXPORT=true",
            "--label", "org.opencontainers.image.revision=" + head, "-t", tag, str(context)],
            cwd=ROOT, check=True, stdout=stream, stderr=subprocess.STDOUT)
    image = json.loads(run(["docker", "image", "inspect", tag]))[0]
    if image["Architecture"] != "arm64" or image["Config"]["User"] != "10001:10001":
        raise RuntimeError("Writer image platform/user mismatch")
    module = "writer.runtime" if args.service == "writer" else "query.runtime"
    code = (
        "import hashlib,json,pathlib,duckdb,boto3; "
        "p=pathlib.Path('/opt/colvenna/artifacts'); m=json.loads((p/'manifest.json').read_text()); "
        "assert duckdb.__version__==m['duckdb_version']; "
        "assert all(hashlib.sha256((p/n).read_bytes()).hexdigest()==h for n,h in m['files'].items()); "
        "c=duckdb.connect(config={'allow_unsigned_extensions':True,'autoinstall_known_extensions':False,"
        "'autoload_known_extensions':False}); "
        "[c.execute(\"LOAD '/opt/colvenna/artifacts/\"+n+\".duckdb_extension'\") for n in "
        "['httpfs','aws','duckdb_object_storage']]; "
        "import colvenna_isle." + module + "; "
        + "import pyarrow; assert pyarrow.__version__=='24.0.0'; "
        + "print(json.dumps({'native':m,'aws':boto3.__version__"
        + ",'pyarrow':pyarrow.__version__}))")
    native = json.loads(run(["docker", "run", "--rm", "--network", "none", "--read-only",
        "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--entrypoint", "python", tag, "-c", code]))
    with (output / "ingestion-tests.log").open("x") as stream:
        subprocess.run(["docker", "run", "--rm", "--network", "none", "--read-only",
            "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--tmpfs", "/tmp:rw,size=128m",
            "--mount", "type=bind,src=" + str(ROOT / "tests/ingestion") + ",dst=/source/tests/ingestion,readonly",
            "--mount", "type=bind,src=" + str(ROOT / "docs/history") + ",dst=/source/docs/history,readonly",
            "-e", "COLVENNA_TIMING=off", "-e", "COLVENNA_REGION=" + FIXTURE_REGION, "-e", "AWS_REGION=" + FIXTURE_REGION, "-e", "AWS_DEFAULT_REGION=" + FIXTURE_REGION, "-e", "COLVENNA_ACCOUNT=" + FIXTURE_ACCOUNT, "--entrypoint", "python", tag, "-m", "unittest", "discover", "-s", "/source/tests/ingestion", "-v"],
            check=True, stdout=stream, stderr=subprocess.STDOUT)
    # Socket and SQL checks use the actual hardened image without AWS. Keep
    # native-engine tests separate from the Lambda ZIP's transport checks.
    if args.service == "query":
        for name, tests in [("query", "query"), ("query-native", "query_native")]:
            with (output / (name + "-tests.log")).open("x") as stream:
                target = "/source/tests/" + tests
                subprocess.run(["docker", "run", "--rm", "--network", "none", "--read-only",
                    "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--tmpfs", "/tmp:rw,size=256m",
                    "--mount", "type=bind,src=" + str(ROOT / "tests" / tests) + ",dst=" + target + ",readonly",
                    "--mount", "type=bind,src=" + str(ROOT / "tests/query") + ",dst=/query-fixtures,readonly",
                    "--mount", "type=bind,src=" + str(ROOT / "tests/ingestion") + ",dst=/ingestion-fixtures,readonly",
                    "--mount", "type=bind,src=" + str(ROOT / "infra") + ",dst=/source/infra,readonly",
                    "-e", "PYTHONPATH=/query-fixtures:/ingestion-fixtures:/opt/colvenna/application",
                    "-e", "COLVENNA_TIMING=off", "-e", "COLVENNA_REGION=" + FIXTURE_REGION, "-e", "AWS_REGION=" + FIXTURE_REGION, "-e", "AWS_DEFAULT_REGION=" + FIXTURE_REGION, "-e", "COLVENNA_ACCOUNT=" + FIXTURE_ACCOUNT, "--entrypoint", "python", tag, "-m", "unittest", "discover", "-s", target, "-v"],
                    check=True, stdout=stream, stderr=subprocess.STDOUT)
    else:
        with (output / "pipeline-tests.log").open("x") as stream:
            # Writer validates Parquet and Native publication. User task/pandas
            # subprocess tests run in the locked compute environment via the
            # control artifact checks and actual environment qualification.
            for pattern in ["test_input_objects.py", "test_writer_inputs.py", "test_native_inputs.py",
                            "test_parquet.py", "test_workspace.py", "test_output_validation.py",
                            "test_publications.py", "test_writer_publications.py", "test_candidates.py",
                            "test_publication_qualification.py", "test_incremental_publications.py",
                            "test_sql_tasks.py", "test_writer_tasks.py", "test_native_governance.py"]:
                subprocess.run(["docker", "run", "--rm", "--network", "none", "--read-only",
                    "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--tmpfs", "/tmp:rw,size=128m",
                    "--mount", "type=bind,src=" + str(ROOT / "tests/pipeline") +
                        ",dst=/source/tests/pipeline,readonly",
                    "-e", "COLVENNA_TIMING=off", "-e", "COLVENNA_REGION=" + FIXTURE_REGION, "-e", "AWS_REGION=" + FIXTURE_REGION, "-e", "AWS_DEFAULT_REGION=" + FIXTURE_REGION, "-e", "COLVENNA_ACCOUNT=" + FIXTURE_ACCOUNT, "--entrypoint", "python", tag, "-m", "unittest", "discover",
                    "-s", "/source/tests/pipeline", "-p", pattern, "-v"],
                    check=True, stdout=stream, stderr=subprocess.STDOUT)
    receipt = {"status": "PASS", "service": args.service,
               "source_commit": head, "source_tree": run(["git", "rev-parse", "HEAD^{tree}"]),
               "application_zip_sha256": manifest["zip_sha256"],
               "storage_image": {"repository": lock["storage_repository"], "digest": lock["storage_digest"]},
               "image_id": image["Id"], "tag": tag, "native": native,
               "dockerfile_sha256": hashlib.sha256((context / "Dockerfile").read_bytes()).hexdigest(),
               "columnar_lock_sha256": hashlib.sha256(
                   (context / "columnar-requirements.lock").read_bytes()).hexdigest()}
    (output / "manifest.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps(receipt))


if __name__ == "__main__":
    main()
