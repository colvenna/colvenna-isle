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
python3 -m unittest discover -s tests/secret_scan
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

Nothing is made public before [ggshield](https://docs.gitguardian.com/ggshield-docs/getting-started), GitGuardian's command-line scanner, finds 0 incidents in it with this repository's [`.gitguardian.yaml`](.gitguardian.yaml). This holds for the repository's history and for every published artifact.

- **Before a release, or before the repository is made public**, a maintainer runs the publication scan and attaches its summary to the release's verification record:

  ```bash
  python3 -m venv /tmp/colvenna-ggshield
  /tmp/colvenna-ggshield/bin/python -m pip install --require-hashes --only-binary=:all: -r runtime/secret-scan-requirements.lock
  GITGUARDIAN_API_KEY=... python3 scripts/secret_scan.py --ggshield /tmp/colvenna-ggshield/bin/ggshield \
    --output <private directory> --releases --workflow-artifacts --path <release build output>
  ```

  It scans what the public can fetch:
  - an anonymous mirror clone (every branch, tag and `refs/pull/*`), plus each pull-request head that a force push replaced and GitHub still serves;
  - the release assets and workflow artifacts as published, plus local paths, with every archive opened.

  It uses the `.gitguardian.yaml` of `main` (`--branch` selects another branch) and exits 0 only when every scan is clean. The reports in `--output` contain the matched values, so keep that directory private. The printed summary has no values; it lists the date, the scanned commit, the ggshield version and each finding's location.
- **Each commit:** `pre-commit install` enables the [ggshield hook](.pre-commit-config.yaml), which scans the staged changes.
  - It needs a GitGuardian API key: `GITGUARDIAN_API_KEY`, or `ggshield auth login`.
  - Without one, it stops with ggshield's error. `SKIP=ggshield git commit ...` skips it, but the CI job and the publication scan still apply.
- **Each push and pull request:** the [secret scan workflow](.github/workflows/secret-scan.yml) scans the new commits with the `GITGUARDIAN_API_KEY` Actions secret. A manual run scans the whole history.
  - Pull requests from forks get no secrets, so for them the job only notes this. A maintainer scans such a change before merging it.
- **A finding:**
  - A real credential is revoked or rotated first, then the history is reviewed. It is never ignored.
  - Any other finding (a test value, a public example, a false positive) is added to `.gitguardian.yaml` by its ignore SHA, with a name that gives the file and the reason. The addition is reviewed in its pull request.
  - No path or detector is ignored.

## Architecture diagram

When modifying the architecture, update its JSON source, regenerate the standalone HTML and README SVG, and refresh the delivery and browser receipts. The diagram source is [docs/design/architecture.json](docs/design/architecture.json). Record unavailable checks explicitly; visual validation does not establish storage or cloud acceptance. Tooling provenance is recorded in [architecture validation](docs/design/architecture-validation.md).

Use the [design feedback template](.github/ISSUE_TEMPLATE/design-feedback.yml) for feature or design questions. For confidential security concerns, follow [SECURITY.md](SECURITY.md).
