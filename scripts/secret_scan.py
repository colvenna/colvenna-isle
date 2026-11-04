#!/usr/bin/env python3
"""Publication secret scan with ggshield (GitGuardian's CLI), required before the repository or a release
is made public (CONTRIBUTING.md#secret-scanning).

It scans what the public can fetch, with the .gitguardian.yaml of the scanned main branch:
  - history: an anonymous `git clone --mirror` (every branch, tag and refs/pull/*), plus each pull-request
    head a force push replaced that GitHub still serves by its SHA;
  - release assets (--releases) and workflow artifacts (--workflow-artifacts) as published, and local
    paths (--path), each archive opened recursively and each distinct file scanned once.
Reports with matched values stay in --output (keep it private); the printed summary holds none.
Exit 0 only when every scan finds 0 incidents.

usage: scripts/secret_scan.py --output DIR [--repository OWNER/NAME] [--releases] [--workflow-artifacts]
                              [--path PATH ...]
"""

import argparse
import datetime
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import zipfile
from dataclasses import dataclass
from pathlib import Path

ZIP_SUFFIXES = (".zip", ".whl", ".jar")
TAR_SUFFIXES = (".tar", ".tar.gz", ".tgz")
MAX_DEPTH = 6


@dataclass
class Unpacked:
    files: int = 0
    distinct: int = 0


def unpack(sources, out):
    """Open each source and every archive nested in it; store each distinct file content once under out."""
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    result = Unpacked()
    seen = set()

    def visit(name, data, depth):
        if depth < MAX_DEPTH and name.lower().endswith(ZIP_SUFFIXES) and zipfile.is_zipfile(io.BytesIO(data)):
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                for member in archive.infolist():
                    if not member.is_dir():
                        visit(member.filename, archive.read(member), depth + 1)
            return
        if depth < MAX_DEPTH and name.lower().endswith(TAR_SUFFIXES):
            try:
                with tarfile.open(fileobj=io.BytesIO(data)) as archive:
                    for member in archive.getmembers():
                        if member.isfile():
                            visit(member.name, archive.extractfile(member).read(), depth + 1)
                return
            except tarfile.TarError:
                pass
        result.files += 1
        digest = hashlib.sha256(data).hexdigest()
        if digest in seen:
            return
        seen.add(digest)
        result.distinct += 1
        folder = out / digest[:16]
        folder.mkdir()
        (folder / Path(name).name).write_bytes(data)

    for source in map(Path, sources):
        for path in sorted(source.rglob("*")) if source.is_dir() else [source]:
            if path.is_file():
                visit(path.name, path.read_bytes(), 0)
    return result


def force_pushed_heads(pages):
    """(pull request, replaced head SHA) of each force push in the GraphQL pages, each SHA once.

    GitHub keeps serving such a commit by its SHA once the repository is public, though no ref reaches it."""
    heads, seen = [], set()
    for page in pages:
        for pull in page["data"]["repository"]["pullRequests"]["nodes"]:
            for event in pull["timelineItems"]["nodes"]:
                sha = (event.get("beforeCommit") or {}).get("oid")
                if sha and sha not in seen:
                    seen.add(sha)
                    heads.append((pull["number"], sha))
    return heads


@dataclass
class Scan:
    scope: str
    returncode: int
    report: dict


def summarize(scans):
    """A report of each scan that locates every incident without its value, and whether all are clean."""
    lines, clean = [], True
    for scan in scans:
        if scan.returncode not in (0, 1) or "total_incidents" not in scan.report:
            lines.append(f"{scan.scope}: ggshield exited {scan.returncode}")
            clean = False
            continue
        total = scan.report["total_incidents"]
        lines.append(f"{scan.scope}: {total} incidents")
        clean = clean and scan.returncode == 0 and total == 0
        commits = scan.report.get("scans")
        for commit in commits if commits is not None else [scan.report]:
            where = f"commit {commit['id'][:12]}, " if commits is not None else ""
            for entity in commit.get("entities_with_incidents") or []:
                filename = Path(entity["filename"])
                if filename.is_absolute():
                    filename = Path(*filename.parts[-2:])
                for found in entity["incidents"]:
                    line = found["occurrences"][0]["line_start"] if found["occurrences"] else "?"
                    lines.append(f"  - {found['type']}, {where}{filename}:{line}, ignore_sha {found['ignore_sha']}")
    lines.append("VERDICT: CLEAN" if clean else "VERDICT: FINDINGS")
    return "\n".join(lines) + "\n", clean


FORCE_PUSHES = """query($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(first: 50, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes { number timelineItems(first: 100, itemTypes: [HEAD_REF_FORCE_PUSHED_EVENT]) {
        nodes { ... on HeadRefForcePushedEvent { beforeCommit { oid } afterCommit { oid } } } } }
    }
  }
}"""
ANONYMOUS_GIT = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}


def run(command, **options):
    return subprocess.run(command, check=True, capture_output=True, text=True, **options).stdout


def gh_json(*args):
    return json.loads(run(["gh", "api", *args]))


def mirror(repository, out):
    """An anonymous mirror clone plus the force-pushed-away heads GitHub serves; a line about the refs."""
    url = f"https://github.com/{repository}.git"
    git = ["git", "-c", "credential.helper=", "-C", str(out)]
    run(["git", "-c", "credential.helper=", "clone", "--quiet", "--mirror", url, str(out)], env=ANONYMOUS_GIT)
    run([*git, "remote", "set-url", "--push", "origin", "no-push://read-only"])
    owner, name = repository.split("/", 1)
    pages, cursor = [], None
    while True:
        args = ["graphql", "-f", "query=" + FORCE_PUSHES, "-F", "owner=" + owner, "-F", "name=" + name]
        pages.append(gh_json(*args, *(["-F", "cursor=" + cursor] if cursor else [])))
        info = pages[-1]["data"]["repository"]["pullRequests"]["pageInfo"]
        if not info["hasNextPage"]:
            break
        cursor = info["endCursor"]
    status = {"reachable": 0, "fetched": 0, "not served": 0}
    for _, sha in force_pushed_heads(pages):
        if subprocess.run([*git, "cat-file", "-e", sha + "^{commit}"], capture_output=True).returncode == 0:
            status["reachable"] += 1
        elif subprocess.run([*git, "fetch", "--quiet", "origin", f"{sha}:refs/force-pushed/{sha}"],
                            capture_output=True, env=ANONYMOUS_GIT).returncode == 0:
            status["fetched"] += 1
        else:
            status["not served"] += 1
    refs = run([*git, "for-each-ref", "--format=%(refname)"]).split()
    count = lambda prefix: sum(ref.startswith(prefix) for ref in refs)  # noqa: E731
    commits = run([*git, "rev-list", "--all", "--count"]).strip()
    return (f"refs: {count('refs/heads/')} branches, {count('refs/tags/')} tags, {count('refs/pull/')} refs/pull; "
            f"force-pushed heads: {status['fetched']} fetched, {status['reachable']} still reachable, "
            f"{status['not served']} not served; {commits} commits")


def download_releases(repository, out):
    tags = run(["gh", "api", "--paginate", f"repos/{repository}/releases?per_page=100", "--jq", ".[].tag_name"])
    for tag in tags.split():
        (out / tag).mkdir(parents=True)
        run(["gh", "release", "download", tag, "-R", repository, "-D", str(out / tag)])


def download_workflow_artifacts(repository, out):
    out.mkdir(parents=True)
    pages = run(["gh", "api", "--paginate", f"repos/{repository}/actions/artifacts?per_page=100",
                 "--jq", ".artifacts[] | select(.expired | not) | .id"])
    for artifact in pages.split():
        with open(out / f"{artifact}.zip", "wb") as stream:
            subprocess.run(["gh", "api", f"repos/{repository}/actions/artifacts/{artifact}/zip"],
                           check=True, stdout=stream)


def ggshield(binary, config, out, scope, *command):
    report = out / (scope.replace(" ", "-") + ".json")
    with open(report, "w") as stdout, open(report.with_suffix(".stderr"), "w") as stderr:
        returncode = subprocess.run([binary, "--config-path", str(config), "secret", "scan", *command, "--json"],
                                    stdout=stdout, stderr=stderr).returncode
    try:
        data = json.loads(report.read_text())
    except json.JSONDecodeError:
        data = {}
    return Scan(scope, returncode, data)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--output", type=Path, required=True, help="new private directory for the reports")
    parser.add_argument("--repository", default="colvenna/colvenna-isle")
    parser.add_argument("--branch", default="main", help="the branch whose .gitguardian.yaml applies")
    parser.add_argument("--releases", action="store_true", help="scan every published release's assets")
    parser.add_argument("--workflow-artifacts", action="store_true", help="scan every unexpired workflow artifact")
    parser.add_argument("--path", type=Path, action="append", default=[], help="a local file or directory")
    parser.add_argument("--ggshield", default=shutil.which("ggshield") or "ggshield")
    args = parser.parse_args(argv)

    out = args.output
    out.mkdir(mode=0o700, parents=True)
    version = run([args.ggshield, "--version"]).strip()
    header = [f"date: {datetime.datetime.now(datetime.timezone.utc):%Y-%m-%dT%H:%M:%SZ}",
              f"repository: {args.repository}", f"ggshield: {version}"]

    clone = out / "mirror.git"
    header.append(mirror(args.repository, clone))
    branch = run(["git", "-C", str(clone), "rev-parse", f"refs/heads/{args.branch}"]).strip()
    config = out / "gitguardian.yaml"
    try:
        config.write_text(run(["git", "-C", str(clone), "show", f"refs/heads/{args.branch}:.gitguardian.yaml"]))
    except subprocess.CalledProcessError:
        sys.exit(f"{args.branch} has no .gitguardian.yaml: the scan uses the repository's own configuration")
    header.append(f"configuration: .gitguardian.yaml of {args.branch} {branch}")

    scans = [ggshield(args.ggshield, config, out, "history", "repo", str(clone))]
    groups = []
    if args.releases:
        download_releases(args.repository, out / "releases")
        groups.append(("release assets", [out / "releases"]))
    if args.workflow_artifacts:
        download_workflow_artifacts(args.repository, out / "workflow-artifacts")
        groups.append(("workflow artifacts", [out / "workflow-artifacts"]))
    if args.path:
        groups.append(("paths", args.path))
    for scope, sources in groups:
        unpacked = out / "unpacked" / scope.replace(" ", "-")
        stats = unpack(sources, unpacked)
        header.append(f"{scope}: {stats.files} files, {stats.distinct} distinct contents scanned")
        scans.append(ggshield(args.ggshield, config, out, scope, "path", "--recursive", "--yes", str(unpacked)))

    text, clean = summarize(scans)
    (out / "summary.txt").write_text("\n".join(header) + "\n" + text)
    print("\n".join(header) + "\n" + text + f"reports: {out}")
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
