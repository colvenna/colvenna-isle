<h1 align="center" id="colvenna-isle">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/logo-dark.svg">
    <img src="docs/assets/logo-light.svg" alt="Colvenna Isle — Durable analytics databases on S3" width="480">
  </picture>
</h1>

<p align="center">
  <a href="LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/License-Apache--2.0-0b6b69"></a>
  <a href="docs/release-notes.md"><img alt="Release: 1.1.0" src="https://img.shields.io/badge/Release-1.1.0-475569"></a>
</p>

<p align="center">
  <a href="docs/assets/promo.mp4"><img src="docs/assets/promo.gif" alt="Colvenna Isle in one minute: API ingestion, durable databases on Amazon S3, SQL and pandas pipelines, the Query Editor and the Admin Console" width="960"></a>
</p>

<p align="center"><a href="docs/assets/promo.mp4">▶ Watch with narration (MP4, 1 min)</a></p>

[中文说明](README.zh-CN.md) · [Release notes](docs/release-notes.md) · [Installation guide](docs/installation.md) · [User guide](docs/user-guide.md) · [Quickstart](examples/quickstart/README.md) · [Documentation](docs/README.md) · [Contributing](CONTRIBUTING.md)

Colvenna Isle is a database and analytics product for AWS. Applications write through an API, data teams transform with SQL and Python pipelines, and analysts query durable analytics databases stored in Amazon S3 from a Query Editor, with an Admin Console for keys, pipelines, runtime environments and operations.

## Release 1.1.0

The next release completes the Colvenna identifiers across code, configuration, protocols and data; `main` is `2.0.0.dev0` (unreleased). This is a breaking upgrade: see the [migration design](docs/design/identifier-migration.md). Candidate AWS acceptance is recorded for each exact source commit and artifact digest in [PR #23](https://github.com/colvenna/colvenna-isle/pull/23).

Published version 1.1.0 ([GitHub Release `v1.1.0`](https://github.com/colvenna/colvenna-isle/releases/tag/v1.1.0)) provides the historical 1.x interfaces and capabilities accepted on AWS:

- a single AWS account of your choice (the account ID is taken from the deployment, never from the source), ARM64;
- a standard commercial AWS region chosen at installation (`--region`): `us-east-2` passed fresh installation, all 56 functional UAT cases, backup and isolated restore, upgrade, interrupted redeploy and uninstall; `us-west-2` passed regression; other commercial regions are accepted by configuration but not qualified;
- DuckDB 1.5.6 on the S3 object filesystem;
- the writer and query services at 2 vCPU / 4 GiB.

1.1.0 also loads `after_write` / `after_ingestion` snapshots in typed Arrow batches and checks source row and byte quotas before copying. Upgrading from 1.0.0 requires uninstalling with the 1.0.0 tools and installing 1.1.0 fresh. The [release notes](docs/release-notes.md) list the changes, the acceptance evidence, the support matrix, sizes, limits and measured baselines, and the items that are planned but not implemented. Measured baselines are in the [performance report](docs/performance.md); no SLA target is claimed. User acceptance is defined by the [UAT test cases](docs/uat/test-cases.md).

## Documentation

| Guide | For |
| --- | --- |
| [Installation guide](docs/installation.md) | Operators: deployment host, native storage image, release build, fresh install, space provisioning, open/close, breaking upgrade, backup and isolated restore, uninstall |
| [User guide](docs/user-guide.md) | Developers, data engineers and analysts: API Keys, datasets, ingestion, synchronous writes, queries, pipelines, Query Editor and Admin Console |
| [API contract](docs/design/api-contract.md) | Every route, scope, limit and error code |
| [Performance tests](docs/performance-tests.md) | How performance and capacity are measured; run separately, not part of UAT ([results](docs/performance.md)) |
| [Cost estimate](docs/cost-estimate.md) | Rough monthly AWS cost of one environment at three data tiers |
| [Design documents](docs/README.md) | Architecture, synchronous writes, pipelines, Python task contract, runtime environments |

## Get started

1. For the current candidate, use a clean checkout of the reviewed commit and run `./install.sh fresh --environment <name> --account <account-id> --region <region>`. It checks the deployment host (an ARM64 EC2 instance with an instance role, Python 3.12, Docker for `linux/arm64`, Node.js), builds the release and installs it as a new environment. See the [installation guide](docs/installation.md).
2. Provision a space and its first administrator key, then run the [quickstart](examples/quickstart/README.md). It covers API ingestion, a synchronous table write with `after_write` read-back, and a published SQL + pandas pipeline queried through SQL.
3. Continue with the [user guide](docs/user-guide.md) and the [API contract](docs/design/api-contract.md).

## Product capabilities

| Area | Release 1.1.0 |
| --- | --- |
| Ingestion | Batch API, schema validation, idempotency, FIFO buffering, and traceable ingestion receipts |
| Direct writes | Registered direct tables, native constraints, atomic insert/upsert/update/delete and durable original receipts ([synchronous writes](docs/design/synchronous-writes.md)) |
| Storage | Native DuckDB database semantics over an S3-backed object filesystem |
| Data modeling | ODS, DWD, DWS, and ADS schemas, with explicit processing watermarks |
| Pipelines | Versioned DAGs, per-task incremental/full/partition processing, built-in scheduling, optional Airflow, writer/standard Lambda/ECS executors and locked Python environments |
| Long jobs | Fixed input versions, independent compute, staged results, chunked imports, and version publication |
| SQL analysis | Query Editor, authorized catalog, asynchronous execution, cancellation, results, and export |
| Operations | Writer status, queue backlog, pipeline progress, data freshness, query status, audit, retention, quotas and costs |
| Lifecycle | Clean install, breaking upgrade, open/close admission, uninstall, consistent backup and isolated same-version restore |
| Access | `Authorization: Bearer <API_KEY>`, space-bound scopes, expiry, revocation, and rotation |

## Architecture

![Colvenna Isle AWS architecture](docs/assets/architecture.svg)

The write path is:

```text
API Gateway → intake Lambda → SQS FIFO → ECS Fargate writer → S3-backed DuckDB
API Gateway → independent VPC sync Lambda → approved writer → native commit receipt
```

API Gateway uses a Lambda Authorizer for opaque Bearer API Keys. Each database has one active writer; query and long-job compute run independently. ODS/DWD/DWS/ADS start as schemas in one database. Sustained write contention can justify separating the ingestion database from the serving database.

DynamoDB uses one control-plane table for API Keys, space routing, pipeline/environment metadata and asynchronous status projections. Intake sends directly to SQS and returns `202` after S3 archival and a confirmed SQS send; the DuckDB ingestion ledger handles durable deduplication.

Synchronous table writes use `data:write` and explicit direct ownership. The separate proxy waits for the sole writer's native transaction; `200 COMMITTED` binds data, receipt, changes and watermark. A timeout remains an original-identity reconciliation problem. See the [synchronous write contract](docs/design/synchronous-writes.md).

Authorizer and management Lambda functions access DynamoDB through the AWS SDK. SQL requests reach the ECS query Service through API Gateway's VPC Link and an internal ALB. The writer Service polls SQS without an ALB; the scheduling Lambda invokes approved Lambda versions or starts independent ECS RunTask compute.

The database's physical S3 objects are managed by SlateDB, including SSTs, WAL, and manifests. ODS/DWD/DWS/ADS are native DuckDB tables. Parquet snapshots and staged compute results use separate S3 prefixes.

Long jobs bind to immutable input files, stage their output in S3, and let the writer import chunks before publishing a new result version. Storage manifest polling does not establish freshness of a persistent DuckDB attachment; ingestion, commit, and query visibility are separate states. See the [engineering design](docs/design/aws-product-design.md).

Python tasks read named Parquet inputs through a context file and output Parquet plus a manifest. Dependencies such as pandas are resolved before publication, locked with hashes and packaged into immutable environments. Standard Lambda supports bounded tasks up to 15 minutes; large jobs use ECS. Lambda Managed Instances and Durable Functions profiles are planned and not part of 1.1.0.

[Interactive architecture](docs/design/architecture.html) · [Engineering design](docs/design/aws-product-design.md) · [API contract](docs/design/api-contract.md) · [Pipeline design](docs/design/pipeline-design.md)

## Read and preview

No AWS account, credentials, or dependencies are required to read the documents.

```bash
python3 -m http.server 8025 --bind 127.0.0.1
```

Open `http://127.0.0.1:8025/docs/design/architecture.html` for the standalone architecture viewer. A local HTTP server is for documentation preview; it is not the product API.

## Validate documentation

```bash
python3 scripts/check_docs.py
```

The check validates local links, SVG/XML and JSON syntax, and the architecture source/artifact hashes. The GitHub Actions [documentation workflow](.github/workflows/docs.yml) runs the same check. Browser inspection evidence is recorded in [architecture validation](docs/design/architecture-validation.md); it does not establish AWS runtime acceptance.

The [orders example](examples/orders/README.md) includes a runnable local SQL/pandas file-contract check and a dependency lock for Python 3.12/Linux arm64. The [quickstart](examples/quickstart/README.md) publishes the same pipeline in an installed environment.

The [score_orders walkthrough](examples/orders/score-orders-guide.md) explains its Python/Lambda settings, temporary files and output manifest.

The [development builder](scripts/build_release.py) packages a clean source commit into a versioned wheel and records locks, notices, validation logs and the qualified storage image; the [installation guide](docs/installation.md#4-原生存储镜像每个账号一次) shows how it fits into building the native storage image.

## Contribute

Use an independent branch and submit a pull request. Keep release copy, API contracts, and engineering design consistent, and distinguish current upstream capabilities from product requirements. See [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

Releases make no backward-compatibility or data-migration commitment. Upgrades are breaking: close the environment, deploy the new release and rebuild affected spaces (see the [installation guide](docs/installation.md#10-破坏性升级)). How later releases handle breaking changes is stated in their release notes.

## License and attribution

Project source code, documentation, design specifications, and original brand assets are licensed under [Apache-2.0](LICENSE). Third-party components (DuckDB and its extensions, locked Python distributions, base images and the Archify viewer runtime) keep their own licenses; see [third-party notices](THIRD_PARTY_NOTICES.md).

DuckDB and AWS are third-party technologies. This project is not an official DuckDB or AWS product.

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
