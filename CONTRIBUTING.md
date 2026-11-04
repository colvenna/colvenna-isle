# Contributing

This repository contains Colvenna Isle: the product services (API, intake, writer, query, control plane, built-in pipelines), the Query Editor and Admin Console, the deployment tools, the contracts and the design documents.

1. Create an independent branch for your change and open a focused pull request that explains the change and its validation.
2. Keep the API contract, design documents, user guide, installation guide, release notes and README aligned with the code.
3. Cite primary sources for upstream capabilities. Separate implemented behavior, proposed behavior and validation results.
4. Do not include credentials, API Keys, customer data, AWS account IDs, role names or deployment identifiers.

Releases make no backward-compatibility commitment: a change may alter contracts or storage formats when the release notes and the [installation guide](docs/installation.md#10-破坏性升级) describe the upgrade.

## Local validation

```bash
python3 scripts/check_docs.py
```

checks local links, SVG/XML and JSON syntax and the architecture receipts.

```bash
python3 -m unittest discover -s tests/ggshield_scan
```

checks the [publication secret scan](#secret-scanning)'s archive unpacking and report without ggshield or the network.

The service suites run without AWS against the documentation example account:

```bash
python3 -m venv /tmp/colvenna-build
/tmp/colvenna-build/bin/python -m pip install --require-hashes --only-binary=:all: \
  -r runtime/contracts-requirements.lock -r runtime/aws-requirements.lock -r runtime/build-requirements.lock \
  -r examples/orders/requirements.lock
export COLVENNA_ACCOUNT=123456789012 COLVENNA_TIMING=off PYTHONPATH=src
/tmp/colvenna-build/bin/python -m colvenna_isle.contracts generate --check
for suite in control intake writer query; do /tmp/colvenna-build/bin/python -m unittest discover -s tests/$suite; done
```

The native suites (DuckDB 1.5.6 with the object-storage extension) run inside the writer and query images; `scripts/build_product.py` builds the images and runs them (see the [installation guide](docs/installation.md#5-构建-release)). When changing SQL/Python example behavior or dependencies, run the [local orders check](examples/orders/README.md) in the locked environment. The [development artifact workflow](.github/workflows/build.yml) runs the package, contract and example checks on an ARM64 runner; AWS behavior needs a deployed environment and the [UAT test cases](docs/uat/test-cases.md).

## Secret scanning

Nothing is made public before [ggshield](https://docs.gitguardian.com/ggshield-docs/getting-started), GitGuardian's command-line scanner, finds 0 incidents in it with this repository's [`.gitguardian.yaml`](.gitguardian.yaml). This holds for the repository's history, its published artifacts and its GitHub text. The repository is public, so a push publishes its commits. The local hooks below are a best-effort check before that, and CI scans each push again afterwards. The publication scan is the gate for a release and for making anything public. ggshield is installed from a hash-locked file:

```bash
python3 -m venv ~/.venvs/colvenna-ggshield
~/.venvs/colvenna-ggshield/bin/python -m pip install --require-hashes --only-binary=:all: \
  -r runtime/ggshield-requirements.lock
export PATH="$HOME/.venvs/colvenna-ggshield/bin:$PATH" GITGUARDIAN_API_KEY=...  # or: ggshield auth login
```

- **Before a release, or before the repository is made public**, a maintainer runs the publication scan, then attaches its summary to the release's verification record. Check the remaining API quota first with `ggshield quota`, because a full scan uses part of it.

  ```bash
  python3 scripts/ggshield_scan.py --output <private directory> [--path <release build output>]
  ```

  It scans everything the public can fetch:
  - an anonymous mirror clone (every branch, tag and `refs/pull/*`), plus each pull-request head that a force push replaced and GitHub still serves;
  - the release assets and workflow artifacts as published, plus any `--path`, with every archive opened (zip, wheel, jar, tar, gzip);
  - the GitHub text: issues, pull requests, comments, reviews, release notes and commit comments, and the wiki when it is enabled. An enabled wiki that cannot be cloned makes the scan fail.

  It uses the `.gitguardian.yaml` of `main`; `--branch` selects another branch. It refuses any ggshield version other than the one in the lock file, because its checks read that version's messages.

  Some files would be left out by ggshield without notice, so they are prepared first:
  - A text over ggshield's 1 MiB limit is scanned in overlapping parts.
  - ggshield drops files by extension (for example `.svg` and `.pdf`), so a text under such an extension is scanned under a `.txt` name.
  - ggshield's default exclusions (for example `vendor/`, `node_modules/`, `.venv/`, `*.mdx` and `*.sops`) apply to every scan, and its `--ignore-default-excludes` has no effect in 1.56.1. Unpacked files that they match are scanned under a neutral name. Any path in the history that they match makes the scan fail.

  It exits 0 only when every scan is complete and finds 0 incidents. The summary counts the binary files that ggshield cannot decode or drops by extension. Any other file that ggshield skips makes the scan fail.

  The reports in `--output` contain the matched values, so keep that directory private. The summary has no values; it records the date, the scanned commit, the ggshield version, the quota and each finding's location.
- **Each commit and each push (best effort):** `pre-commit install` enables the [ggshield hooks](.pre-commit-config.yaml). They use the `ggshield` on `PATH` and refuse any version other than the one in the lock file.
  - The pre-commit hook scans the staged changes.
  - The pre-push hook scans the commits about to be published. It covers at most `max_commits_for_hook` commits of a push (1000, set in `.gitguardian.yaml`), and ggshield lets a push through with only a warning when it cannot work out the commit range. When one push updates several refs (`git push --all`, or several branches or tags), pre-commit hands ggshield only the first, so push them one at a time.
  - Without an API key, they stop with ggshield's error.
  - `SKIP=ggshield,ggshield-push` or `git push --no-verify` skips them. The CI job and the publication scan still apply.
- **Each push and pull request:** the [secret scan workflow](.github/workflows/ggshield.yml) scans the new commits with the `GITGUARDIAN_API_KEY` Actions secret, which the repository owner sets. A manual run scans the whole history.
  - Pull requests from forks get no secrets, so for them the job only notes this. A maintainer scans such a change before merging it.
- **A finding:**
  - A real credential is revoked or rotated first, then the history is reviewed. It is never ignored.
  - Any other finding (a test value, a public example, a false positive) is added to `.gitguardian.yaml` by its ignore SHA, with a name that gives the file and the reason. The addition is reviewed in its pull request.
  - No path or detector is ignored.
  - A tracked file's name should avoid credential words such as "secret", "token" or "password". The development artifact's `manifest.json` pairs each tracked path with its SHA-256, and ggshield reported such a pair as a secret. An ignore entry would be needed again each time the file changed, so rename the file instead (#25).
- **Deployment values:** ggshield does not look for AWS account IDs, ARNs, endpoints or internal addresses, so a maintainer also searches what the scan fetched:
  - Each account that a deployment of yours used must appear 0 times anywhere.
  - In the first-party content, list the accounts named in ARNs, the AWS endpoint hosts and the private IPv4 addresses, and review each one. The first-party content is the histories, the release assets, the GitHub text, and the project's own logs, manifests and documents inside the workflow artifacts.
  - The rest of the workflow artifacts bundle third-party packages, which name documentation and AWS-owned accounts and endpoints, so only your account is searched there.

  Expected hits are documentation examples (such as `123456789012` and `example.com`), accounts and endpoints that AWS owns (such as a service's image registry), and example addresses (such as a VPC CIDR):

  ```bash
  ACCOUNT=<your deployment account ID>; DIR=<private directory>
  histories() { for git in "$DIR"/mirror.git "$DIR"/wiki.git; do if [ -d "$git" ]; then git -C "$git" log --all -p; fi; done; }
  first_party() { histories; grep -rhI '' "$DIR/unpacked/release-assets" "$DIR/unpacked/GitHub-text"
                  grep -rhI '' --include='*.log' --include='manifest.json' --include='*.md' "$DIR/unpacked/workflow-artifacts"; }
  { first_party; grep -rhI '' "$DIR/unpacked/workflow-artifacts"; } | grep -c "$ACCOUNT"  # must be 0
  first_party | grep -oE 'arn:aws[a-z-]*:[a-z0-9-]*:[a-z0-9-]*:[0-9]{12}:' | grep -oE '[0-9]{12}' | sort | uniq -c
  first_party | grep -oE '[a-z0-9.-]+\.(amazonaws\.com|cloudfront\.net|awsapps\.com)' | sort | uniq -c
  first_party | grep -oE '\b(10\.[0-9]+|172\.(1[6-9]|2[0-9]|3[01])|192\.168)\.[0-9]+\.[0-9]+\b' | sort | uniq -c
  ```

## Architecture diagram

When modifying the architecture, update its JSON source, regenerate the standalone HTML and README SVG, and refresh the delivery and browser receipts. The diagram source is [docs/design/architecture.json](docs/design/architecture.json). Record unavailable checks explicitly; visual validation does not establish storage or cloud acceptance. Tooling provenance is recorded in [architecture validation](docs/design/architecture-validation.md).

Use the [design feedback template](.github/ISSUE_TEMPLATE/design-feedback.yml) for feature or design questions. For confidential security concerns, follow [SECURITY.md](SECURITY.md).
