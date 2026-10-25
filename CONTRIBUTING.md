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

## Architecture diagram

When modifying the architecture, update its JSON source, regenerate the standalone HTML and README SVG, and refresh the delivery and browser receipts. The diagram source is [docs/design/architecture.json](docs/design/architecture.json). Record unavailable checks explicitly; visual validation does not establish storage or cloud acceptance. Tooling provenance is recorded in [architecture validation](docs/design/architecture-validation.md).

Use the [design feedback template](.github/ISSUE_TEMPLATE/design-feedback.yml) for feature or design questions. For confidential security concerns, follow [SECURITY.md](SECURITY.md).
