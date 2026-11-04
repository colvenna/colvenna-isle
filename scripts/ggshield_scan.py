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
    and commit comments; and the wiki, when it is enabled.
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
import re
import shutil
import subprocess
import sys
import tarfile
import zipfile
import zlib
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from ggshield_pin import installed_version, pinned_version

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
    unread: int = 0  # binary files under an extension ggshield's path scan drops without reading


ARCHIVE_ERRORS = (OSError, EOFError, zlib.error, zipfile.BadZipFile, tarfile.TarError)


def unpack(sources, out, part_chars=PART_CHARS, overlap_chars=OVERLAP_CHARS, skip_extensions=frozenset(),
           excluded=lambda path: False):
    """Open each source and every archive nested in it (zip, wheel, jar, tar, gzip); store each distinct file
    content once under out. A UTF-8 text longer than part_chars is stored as overlapping parts, each in a
    folder that records its number and the line it starts in. ggshield's path scan drops files by extension (skip_extensions)
    without reading them, so a UTF-8 text under such an extension gets ".txt" appended, and a binary one is
    counted as unread. A file ggshield would exclude by name (excluded, given the stored path) is stored
    under a neutral name instead. A damaged archive is stored as a file."""
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    result = Unpacked()
    seen = set()

    def opened(name, data, depth):
        lower = name.lower()
        if depth >= MAX_DEPTH:
            return False
        try:
            if lower.endswith(ZIP_SUFFIXES) and zipfile.is_zipfile(io.BytesIO(data)):
                with zipfile.ZipFile(io.BytesIO(data)) as archive:
                    members = [(m.filename, archive.read(m)) for m in archive.infolist() if not m.is_dir()]
            elif lower.endswith(TAR_SUFFIXES):
                with tarfile.open(fileobj=io.BytesIO(data)) as archive:
                    members = [(m.name, archive.extractfile(m).read()) for m in archive.getmembers() if m.isfile()]
            elif lower.endswith(".gz"):
                members = [(name[:-3], gzip.decompress(data))]
            else:
                return False
        except ARCHIVE_ERRORS:
            return False
        for member, content in members:
            visit(member, content, depth + 1)
        return True

    def visit(name, data, depth):
        if opened(name, data, depth):
            return
        result.files += 1
        digest = hashlib.sha256(data).hexdigest()
        if digest in seen:
            return
        seen.add(digest)
        result.distinct += 1
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError:
            text = None
        name = Path(name).name or "file"
        if Path(name).suffix.lower()[1:] in skip_extensions:
            if text is None:
                result.unread += 1
            else:
                name += ".txt"
        for index, (first_line, part) in enumerate(parts(data, text, part_chars, overlap_chars)):
            folder = out / (digest[:16] if index == 0 else f"{digest[:16]}.{index}.L{first_line}")
            folder.mkdir()
            target = folder / name
            if excluded(target):
                target = folder / (name.replace(".", "_") + ".txt")
            target.write_bytes(part)

    for source in map(Path, sources):
        for path in sorted(source.rglob("*")) if source.is_dir() else [source]:
            if path.is_file():
                visit(path.name, path.read_bytes(), 0)
    return result


def parts(data, text, part_chars, overlap_chars):
    """(first line, bytes) of data itself, or of windows of a UTF-8 text longer than part_chars that overlap
    by overlap_chars."""
    if text is None or len(text) <= part_chars:
        return [(1, data)]
    step = part_chars - overlap_chars
    starts = range(0, len(text) - overlap_chars, step)
    return [(text.count("\n", 0, start) + 1, text[start:start + part_chars].encode()) for start in starts]


def file_line(path, line):
    """The stored file's place in its original: the content's folder and name, and the line there."""
    path = Path(path)
    digest, _, first = path.parent.name.partition(".L")
    return f"{digest}/{path.name}", line + int(first or 1) - 1


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
    unscanned: list = field(default_factory=list)  # paths ggshield's default exclusions leave out


BINARY_SKIP = "can't detect encoding"


def skipped(stderr):
    """(file, reason) of each item ggshield skipped, from its "Skipped <url>: <reason>" lines."""
    for line in stderr.splitlines():
        if line.startswith("Skipped "):
            url, _, reason = line[len("Skipped "):].rpartition(": ")
            yield url.split("://", 1)[-1], reason


def has_errors(report):
    return bool(report.get("errors")) or any(has_errors(inner) for inner in report.get("scans") or [])


def located(filename, line):
    """A finding's file and line: as in the repository for a commit, else as file_line places it."""
    return (filename, line) if not Path(filename).is_absolute() else file_line(filename, line)


def problems(scan):
    """Why a scan is not complete: ggshield failed, reported errors, or skipped a file that is not binary."""
    if scan.returncode not in (0, 1) or "total_incidents" not in scan.report:
        return [f"{scan.scope}: ggshield exited {scan.returncode}"]
    found = []
    if has_errors(scan.report) or "Scanning failed" in scan.stderr:
        found.append(f"{scan.scope}: incomplete, ggshield reported a failed scan (see its report)")
    found += [f"  - not scanned: {file_line(filename, 1)[0]} ({reason})"
              for filename, reason in skipped(scan.stderr) if reason != BINARY_SKIP]
    found += [f"  - not scanned: {path} (ggshield's default exclusions)" for path in scan.unscanned]
    return found


def findings(scan):
    """Each incident's detector, commit (for a history), file, line and ignore SHA; never its value."""
    commits = scan.report.get("scans")
    for commit in commits if commits is not None else [scan.report]:
        where = f"commit {commit['id'][:12]}, " if commits is not None else ""
        for entity in commit.get("entities_with_incidents") or []:
            for found in entity["incidents"]:
                line = found["occurrences"][0]["line_start"] if found["occurrences"] else 1
                place, line = located(entity["filename"], line)
                yield f"{found['type']}, {where}{place}:{line}, ignore_sha {found['ignore_sha']}"


def summarize(scans):
    """A report of each scan that locates every incident without its value, and whether all are clean.

    A scan is clean when ggshield exits 0, reports no incident and no error, says nothing failed, and skips
    nothing but binary files (which it cannot decode)."""
    lines, clean = [], True
    for scan in scans:
        trouble = problems(scan)
        if "total_incidents" in scan.report:
            binary = sum(reason == BINARY_SKIP for _, reason in skipped(scan.stderr))
            lines.append(f"{scan.scope}: {scan.report['total_incidents']} incidents"
                         + (f"; {binary} binary files not scanned ({BINARY_SKIP})" if binary else ""))
            clean = clean and scan.returncode == 0 and scan.report["total_incidents"] == 0
        lines += trouble + [f"  - {item}" for item in findings(scan)]
        clean = clean and not trouble
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


def run(command, failure=None, **options):
    """The command's output; on failure, exit with failure (or the command and its stderr)."""
    done = subprocess.run(command, capture_output=True, text=True, **options)
    if done.returncode:
        sys.exit(failure or f"{' '.join(command[:4])} ... failed ({done.returncode}): {done.stderr.strip()}")
    return done.stdout


def ggshield_rules(binary):
    """What ggshield 1.56.1 leaves out without saying so, read from the Python of ggshield's environment: the
    extensions its path scan drops unread, and a predicate for the paths its default exclusions skip (in
    every scan, history included). --ignore-default-excludes is accepted but has no effect in 1.56.1."""
    folder = Path(shutil.which(binary) or binary).resolve().parent
    code = ("import json\n"
            "from ggshield.cmd.secret.scan.secret_scan_common_options import IGNORED_DEFAULT_WILDCARDS as W\n"
            "from ggshield.core.filter import init_exclusion_regexes\n"
            "from ggshield.utils._binary_extensions import BINARY_EXTENSIONS as B\n"
            "print(json.dumps([sorted(B), sorted(r.pattern for r in init_exclusion_regexes(set(W)))]))")
    for python in (folder / "python3", folder / "python"):
        done = subprocess.run([str(python), "-c", code], capture_output=True, text=True) if python.exists() else None
        if done and done.returncode == 0:
            extensions, patterns = json.loads(done.stdout)
            regexes = [re.compile(pattern) for pattern in patterns]
            return set(extensions), lambda path: any(r.search(PurePosixPath(path).as_posix()) for r in regexes)
    sys.exit(f"cannot read what ggshield leaves out: no Python beside {folder / Path(binary).name} imports ggshield")


def excluded_history_paths(clone, excluded):
    """The paths any commit of the clone touches that ggshield's default exclusions skip."""
    paths = set(run([*GIT, "-C", str(clone), "log", "--all", "--format=", "--name-only"]).split("\n")) - {""}
    return sorted(path for path in paths if excluded(path))


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

    want, have = pinned_version(), installed_version(args.ggshield)
    if have != want:  # the completeness checks read this version's messages
        sys.exit(f"the scan needs ggshield {want}, installed from runtime/ggshield-requirements.lock (found: {have})")
    skip_extensions, excluded = ggshield_rules(args.ggshield)
    out = args.output.resolve()  # ggshield reports the paths it was given; file_line() maps absolute ones
    out.mkdir(mode=0o700, parents=True)
    header = [f"date: {datetime.datetime.now(datetime.timezone.utc):%Y-%m-%dT%H:%M:%SZ}",
              f"repository: {args.repository}", f"ggshield: {have}", f"quota before: {quota(args.ggshield)}"]

    clone = out / "mirror.git"
    mirror(args.repository, clone)
    pushed = fetch_force_pushed(args.repository, clone)
    header.append(f"refs: {describe_refs(clone)}; force-pushed heads: "
                  + ", ".join(f"{number} {outcome}" for outcome, number in pushed.items()))
    branch_sha = run([*GIT, "-C", str(clone), "rev-parse", "--verify", f"refs/heads/{args.branch}"],
                     failure=f"the repository has no branch {args.branch}").strip()
    config = out / "gitguardian.yaml"
    config.write_text(run([*GIT, "-C", str(clone), "show", f"{branch_sha}:.gitguardian.yaml"],
                          failure=f"{args.branch} has no .gitguardian.yaml: the scan uses the repository's own "
                                  "configuration"))
    header.append(f"configuration: .gitguardian.yaml of {args.branch} {branch_sha}")

    scans = [ggshield(args.ggshield, config, out, "history", "repo", str(clone))]
    scans[-1].unscanned = excluded_history_paths(clone, excluded)
    if json.loads(run(["gh", "api", f"repos/{args.repository}"]))["has_wiki"]:
        wiki = out / "wiki.git"
        run([*GIT, "clone", "--quiet", "--mirror", f"https://github.com/{args.repository}.wiki.git", str(wiki)],
            failure="the wiki is enabled but has no clone to scan: disable it, or add a page and scan again",
            env=ANONYMOUS_GIT)
        header.append(f"wiki: {describe_refs(wiki)}")
        scans.append(ggshield(args.ggshield, config, out, "wiki", "repo", str(wiki)))
        scans[-1].unscanned = excluded_history_paths(wiki, excluded)
    download_releases(args.repository, out / "releases")
    download_workflow_artifacts(args.repository, out / "workflow-artifacts")
    texts = download_github_text(args.repository, out / "github-text")
    groups = [("release assets", [out / "releases"]), ("workflow artifacts", [out / "workflow-artifacts"]),
              ("GitHub text", [out / "github-text"])] + ([("paths", args.path)] if args.path else [])
    for scope, sources in groups:
        unpacked = out / "unpacked" / slug(scope)
        stats = unpack(sources, unpacked, skip_extensions=skip_extensions, excluded=excluded)
        header.append(f"{scope}: {stats.files} files, {stats.distinct} distinct contents scanned"
                      + (f" ({texts} documents)" if scope == "GitHub text" else "")
                      + (f"; {stats.unread} binary files ggshield drops by extension" if stats.unread else ""))
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
