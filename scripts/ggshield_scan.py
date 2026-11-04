#!/usr/bin/env python3
"""Publication secret scan with ggshield (GitGuardian's CLI), required before the repository or a release
is made public (CONTRIBUTING.md#secret-scanning).

It scans everything the public can fetch from the repository, with the .gitguardian.yaml of the scanned
branch (main by default):
  - history: an anonymous `git clone --mirror` (every branch, tag and refs/pull/*), plus each pull-request
    head a force push replaced that GitHub still serves by its SHA;
  - release assets and workflow artifacts as published, and any local --path, each archive opened
    recursively (zip, wheel, jar, tar, gzip) and each distinct file scanned once;
  - GitHub text: issues and pull requests with their comments, reviews and review comments, release notes
    and commit comments.
Reports with matched values stay in --output (keep it private); the summary holds none. Exit 0 only when
every scan is complete and finds 0 incidents.

usage: scripts/ggshield_scan.py --output DIR [--repository OWNER/NAME] [--branch BRANCH] [--path PATH ...]
                              [--ggshield BINARY]
"""

import argparse
import datetime
import gzip
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
# ggshield skips a document over 1 MiB (its API's limit) and a request body over 2.5 MiB, in which JSON
# escaping can grow text up to 6 times; parts of 200,000 characters (at most 800 kB of UTF-8) stay under
# both. Parts overlap, so a value that crosses a cut is whole in one of them.
PART_CHARS = 200_000
OVERLAP_CHARS = 4_096


@dataclass
class Unpacked:
    files: int = 0
    distinct: int = 0


def unpack(sources, out, part_chars=PART_CHARS, overlap_chars=OVERLAP_CHARS):
    """Open each source and every archive nested in it (zip, wheel, jar, tar, gzip); store each distinct file
    content once under out, a UTF-8 text longer than part_chars as overlapping parts."""
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
        if depth < MAX_DEPTH and name.lower().endswith(".gz"):
            try:
                visit(name[:-3], gzip.decompress(data), depth + 1)
                return
            except (OSError, EOFError):
                pass
        result.files += 1
        digest = hashlib.sha256(data).hexdigest()
        if digest in seen:
            return
        seen.add(digest)
        result.distinct += 1
        for index, part in enumerate(parts(data, part_chars, overlap_chars)):
            folder = out / (digest[:16] if index == 0 else f"{digest[:16]}.{index}")
            folder.mkdir()
            (folder / Path(name).name).write_bytes(part)

    for source in map(Path, sources):
        for path in sorted(source.rglob("*")) if source.is_dir() else [source]:
            if path.is_file():
                visit(path.name, path.read_bytes(), 0)
    return result


def parts(data, part_chars, overlap_chars):
    """data itself, or a UTF-8 text longer than part_chars as windows that overlap by overlap_chars."""
    if len(data) <= part_chars:
        return [data]
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return [data]
    if len(text) <= part_chars:
        return [data]
    step = part_chars - overlap_chars
    return [text[start:start + part_chars].encode() for start in range(0, len(text) - overlap_chars, step)]


def force_pushed_heads(pages):
    """The head SHA each force push in the GraphQL pages replaced, each once.

    GitHub keeps serving such a commit by its SHA once the repository is public, though no ref reaches it."""
    heads = []
    for page in pages:
        for pull in page["data"]["repository"]["pullRequests"]["nodes"]:
            for event in pull["timelineItems"]["nodes"]:
                sha = (event.get("beforeCommit") or {}).get("oid")
                if sha and sha not in heads:
                    heads.append(sha)
    return heads


@dataclass
class Scan:
    scope: str
    returncode: int
    report: dict
    stderr: str = ""


BINARY_SKIP = "can't detect encoding"


def skipped(stderr):
    """(file, reason) of each item ggshield skipped, from its "Skipped <url>: <reason>" lines."""
    for line in stderr.splitlines():
        if line.startswith("Skipped "):
            url, _, reason = line[len("Skipped "):].rpartition(": ")
            yield url.split("://", 1)[-1], reason


def has_errors(report):
    return bool(report.get("errors")) or any(has_errors(inner) for inner in report.get("scans") or [])


def located(filename):
    path = Path(filename)
    return Path(*path.parts[-2:]) if path.is_absolute() else path


def summarize(scans):
    """A report of each scan that locates every incident without its value, and whether all are clean.

    A scan is clean when ggshield exits 0, reports no incident and no error, says nothing failed, and skips
    nothing but binary files (which it cannot decode)."""
    lines, clean = [], True
    for scan in scans:
        if scan.returncode not in (0, 1) or "total_incidents" not in scan.report:
            lines.append(f"{scan.scope}: ggshield exited {scan.returncode}")
            clean = False
            continue
        if has_errors(scan.report) or "Scanning failed" in scan.stderr:
            lines.append(f"{scan.scope}: incomplete, ggshield reported a failed scan (see its report)")
            clean = False
        total = scan.report["total_incidents"]
        skips = list(skipped(scan.stderr))
        binary = sum(reason == BINARY_SKIP for _, reason in skips)
        lines.append(f"{scan.scope}: {total} incidents"
                     + (f"; {binary} binary files not scanned ({BINARY_SKIP})" if binary else ""))
        clean = clean and scan.returncode == 0 and total == 0
        for filename, reason in skips:
            if reason != BINARY_SKIP:
                lines.append(f"  - not scanned: {located(filename)} ({reason})")
                clean = False
        commits = scan.report.get("scans")
        for commit in commits if commits is not None else [scan.report]:
            where = f"commit {commit['id'][:12]}, " if commits is not None else ""
            for entity in commit.get("entities_with_incidents") or []:
                for found in entity["incidents"]:
                    line = found["occurrences"][0]["line_start"] if found["occurrences"] else "?"
                    lines.append(f"  - {found['type']}, {where}{located(entity['filename'])}:{line}, "
                                 f"ignore_sha {found['ignore_sha']}")
    lines.append("VERDICT: CLEAN" if clean else "VERDICT: FINDINGS")
    return "\n".join(lines) + "\n", clean


FORCE_PUSHES = """query($owner: String!, $name: String!, $cursor: String) {
  repository(owner: $owner, name: $name) {
    pullRequests(first: 50, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes { timelineItems(first: 100, itemTypes: [HEAD_REF_FORCE_PUSHED_EVENT]) {
        nodes { ... on HeadRefForcePushedEvent { beforeCommit { oid } } } } }
    }
  }
}"""
ANONYMOUS_GIT = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}  # never prompt; no credential helper either
GIT = ["git", "-c", "credential.helper="]


def run(command, **options):
    done = subprocess.run(command, capture_output=True, text=True, **options)
    if done.returncode:
        sys.exit(f"{' '.join(command[:4])} ... failed ({done.returncode}): {done.stderr.strip()}")
    return done.stdout


def slug(scope):
    return scope.replace(" ", "-")


def mirror(repository, clone):
    run([*GIT, "clone", "--quiet", "--mirror", f"https://github.com/{repository}.git", str(clone)],
        env=ANONYMOUS_GIT)
    run([*GIT, "-C", str(clone), "remote", "set-url", "--push", "origin", "no-push://read-only"])


def fetch_force_pushed(repository, clone):
    """Fetch into the clone each replaced pull-request head GitHub still serves; count them by outcome."""
    owner, name = repository.split("/", 1)
    pages, cursor = [], None
    while True:
        args = ["gh", "api", "graphql", "-f", "query=" + FORCE_PUSHES, "-F", "owner=" + owner, "-F", "name=" + name]
        pages.append(json.loads(run(args + (["-F", "cursor=" + cursor] if cursor else []))))
        info = pages[-1]["data"]["repository"]["pullRequests"]["pageInfo"]
        if not info["hasNextPage"]:
            break
        cursor = info["endCursor"]
    status = {"fetched": 0, "still reachable": 0, "not served": 0}
    git = [*GIT, "-C", str(clone)]
    for sha in force_pushed_heads(pages):
        if subprocess.run([*git, "cat-file", "-e", sha + "^{commit}"], capture_output=True).returncode == 0:
            status["still reachable"] += 1
        elif subprocess.run([*git, "fetch", "--quiet", "origin", f"{sha}:refs/force-pushed/{sha}"],
                            capture_output=True, env=ANONYMOUS_GIT).returncode == 0:
            status["fetched"] += 1
        else:
            status["not served"] += 1
    return status


def describe_refs(clone):
    refs = run([*GIT, "-C", str(clone), "for-each-ref", "--format=%(refname)"]).split()
    count = {kind: sum(ref.startswith(f"refs/{kind}/") for ref in refs) for kind in ("heads", "tags", "pull")}
    commits = run([*GIT, "-C", str(clone), "rev-list", "--all", "--count"]).strip()
    return f"{count['heads']} branches, {count['tags']} tags, {count['pull']} refs/pull; {commits} commits"


def download_releases(repository, out):
    out.mkdir(parents=True)
    tags = run(["gh", "api", "--paginate", f"repos/{repository}/releases?per_page=100",
                "--jq", ".[] | select(.assets | length > 0) | .tag_name"])
    for tag in tags.split():
        (out / tag).mkdir()
        run(["gh", "release", "download", tag, "-R", repository, "-D", str(out / tag)])


def download_workflow_artifacts(repository, out):
    out.mkdir(parents=True)
    artifact_ids = run(["gh", "api", "--paginate", f"repos/{repository}/actions/artifacts?per_page=100",
                        "--jq", ".artifacts[] | select(.expired | not) | .id"])
    for artifact_id in artifact_ids.split():
        with open(out / f"{artifact_id}.zip", "wb") as stream:
            subprocess.run(["gh", "api", f"repos/{repository}/actions/artifacts/{artifact_id}/zip"],
                           check=True, stdout=stream)


GITHUB_TEXT = [  # (API path, jq that emits one {name, text} object a line)
    ("issues?state=all&per_page=100",
     '.[] | {name: "\\(if .pull_request then "pull" else "issue" end)-\\(.number).md", '
     'text: "\\(.title)\\n\\n\\(.body // "")"}'),
    ("issues/comments?per_page=100", '.[] | {name: "comment-\\(.id).md", text: (.body // "")}'),
    ("pulls/comments?per_page=100",
     '.[] | {name: "review-comment-\\(.id).md", text: "\\(.diff_hunk // "")\\n\\n\\(.body // "")"}'),
    ("releases?per_page=100", '.[] | {name: "release-\\(.tag_name).md", text: "\\(.name // "")\\n\\n\\(.body // "")"}'),
    ("comments?per_page=100", '.[] | {name: "commit-comment-\\(.id).md", text: (.body // "")}'),
]


def download_github_text(repository, out):
    """Write each issue, pull request, comment, review, release note and commit comment to its own file."""
    out.mkdir(parents=True)
    documents = []
    for path, jq in GITHUB_TEXT:
        documents += run(["gh", "api", "--paginate", f"repos/{repository}/{path}", "--jq", jq]).splitlines()
    pulls = run(["gh", "api", "--paginate", f"repos/{repository}/pulls?state=all&per_page=100",
                 "--jq", ".[].number"]).split()
    for number in pulls:
        documents += run(["gh", "api", "--paginate", f"repos/{repository}/pulls/{number}/reviews?per_page=100",
                          "--jq", f'.[] | select(.body != "") | {{name: "pull-{number}-review-\\(.id).md", '
                                  'text: .body}']).splitlines()
    for line in documents:
        document = json.loads(line)
        (out / document["name"]).write_text(document["text"])
    return len(documents)


def ggshield(binary, config, out, scope, *command):
    report = out / f"{slug(scope)}.json"
    with open(report, "w") as stdout, open(report.with_suffix(".stderr"), "w") as stderr:
        returncode = subprocess.run([binary, "--config-path", str(config), "secret", "scan", *command, "--json"],
                                    stdout=stdout, stderr=stderr).returncode
    try:
        data = json.loads(report.read_text())
    except json.JSONDecodeError:
        data = {}
    return Scan(scope, returncode, data, report.with_suffix(".stderr").read_text())


def quota(binary):
    done = subprocess.run([binary, "quota"], capture_output=True, text=True)
    return done.stdout.splitlines()[0] if done.returncode == 0 and done.stdout else "unavailable"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--output", type=Path, required=True, help="new private directory for the reports")
    parser.add_argument("--repository", default="colvenna/colvenna-isle")
    parser.add_argument("--branch", default="main", help="the branch whose .gitguardian.yaml applies")
    parser.add_argument("--path", type=Path, action="append", default=[], help="a local file or directory")
    parser.add_argument("--ggshield", default=shutil.which("ggshield") or "ggshield")
    args = parser.parse_args(argv)

    out = args.output
    out.mkdir(mode=0o700, parents=True)
    header = [f"date: {datetime.datetime.now(datetime.timezone.utc):%Y-%m-%dT%H:%M:%SZ}",
              f"repository: {args.repository}", f"ggshield: {run([args.ggshield, '--version']).strip()}",
              f"quota before: {quota(args.ggshield)}"]

    clone = out / "mirror.git"
    mirror(args.repository, clone)
    pushed = fetch_force_pushed(args.repository, clone)
    header.append(f"refs: {describe_refs(clone)}; force-pushed heads: "
                  + ", ".join(f"{number} {outcome}" for outcome, number in pushed.items()))
    found = subprocess.run([*GIT, "-C", str(clone), "rev-parse", "--verify", f"refs/heads/{args.branch}"],
                           capture_output=True, text=True)
    if found.returncode:
        sys.exit(f"the repository has no branch {args.branch}")
    branch_sha = found.stdout.strip()
    config = out / "gitguardian.yaml"
    shown = subprocess.run([*GIT, "-C", str(clone), "show", f"{branch_sha}:.gitguardian.yaml"],
                           capture_output=True, text=True)
    if shown.returncode:
        sys.exit(f"{args.branch} has no .gitguardian.yaml: the scan uses the repository's own configuration")
    config.write_text(shown.stdout)
    header.append(f"configuration: .gitguardian.yaml of {args.branch} {branch_sha}")

    scans = [ggshield(args.ggshield, config, out, "history", "repo", str(clone))]
    download_releases(args.repository, out / "releases")
    download_workflow_artifacts(args.repository, out / "workflow-artifacts")
    texts = download_github_text(args.repository, out / "github-text")
    groups = [("release assets", [out / "releases"]), ("workflow artifacts", [out / "workflow-artifacts"]),
              ("GitHub text", [out / "github-text"])] + ([("paths", args.path)] if args.path else [])
    for scope, sources in groups:
        unpacked = out / "unpacked" / slug(scope)
        stats = unpack(sources, unpacked)
        header.append(f"{scope}: {stats.files} files, {stats.distinct} distinct contents scanned"
                      + (f" ({texts} documents)" if scope == "GitHub text" else ""))
        if stats.files:
            scans.append(ggshield(args.ggshield, config, out, scope, "path", "--recursive", "--yes", str(unpacked)))

    text, clean = summarize(scans)
    header.append(f"quota after: {quota(args.ggshield)}")
    report = "\n".join(header) + "\n" + text
    (out / "summary.txt").write_text(report)
    print(report + f"reports: {out}")
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
