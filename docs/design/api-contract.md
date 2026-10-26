# Colvenna Isle API contract

Colvenna Isle `2.0.0` (tag `v2.0.0`) uses `colvenna_isle`, `COLVENNA_*`, `x-colvenna-*` headers and `_colvenna_*` system columns. Contract revision is 2 and native space format is 6; old revisions and old native identities are rejected. Public route paths remain `/v1/`. See the [migration design](identifier-migration.md); AWS acceptance is recorded for each source commit and artifact digest in the [2.0.0 UAT report](../uat/v2.0.0-report.md).

Applies to 2.0.0; published 1.x interfaces are historical. The route behavior originated in 1.1.0; the [user guide](../user-guide.md) describes how to use them. Operator-only actions (space provisioning, quotas, backups and restores, installation) use IAM tools, not public routes; see the [installation guide](../installation.md).

The executable contract is [OpenAPI 3.1](../../src/colvenna_isle/contracts/v2/openapi.json) plus [JSON Schema](../../src/colvenna_isle/contracts/v2/contracts.schema.json) and [semantic validation](../../src/colvenna_isle/contracts/validation.py). These are offline contract checks: they do not authenticate a key or prove that a referenced resource exists, which the services check independently. Unknown request fields and caller-supplied execution resources are rejected.

Releases provide no backward compatibility, automatic in-place migration of older API/configuration/storage formats, or zero-downtime upgrade; see [destructive upgrades](../installation.md#10-破坏性升级) and the [release limits](../release-notes.md#限制).

## Authentication and authorization

All requests use an opaque API Key:

```http
Authorization: Bearer <API_KEY>
```

API Gateway HTTP API invokes a Lambda Authorizer. The token is the API Key itself. Authorizer context identifies the principal, authorized spaces, scopes, expiry, and key_id. Services enforce the requested space and operation independently of caller-supplied body fields.

| Scope | Access |
| --- | --- |
| `datasets:manage` | Read space configuration and register dataset definitions |
| `ingest:write` | Submit ingestions |
| `ingest:replay` | Request original-identity replay together with `ingest:write` |
| `data:write` | Mutate explicitly registered direct-write targets and reconcile owned write receipts |
| `query:read` | Read authorized catalog, submit queries, and access owned query results |
| `query:manage` | Together with `query:read`, inspect job metadata and cancel another owner's query in the authorized space; does not grant their SQL or results |
| `pipeline:run` | Run authorized transformations |
| `pipeline:manage` | Create/edit/publish definitions and manage schedule ownership |
| `runtime:manage` | Create and build approved dependency environments |
| `monitor:read` | Inspect authorized space status |
| `keys:manage` | Manage keys in authorized spaces within delegation limits |
| `audit:read` | Read the space audit trail and original SQL in traces |
| `retention:manage` | Change retention policy and holds and request collections |
| `backup:manage` | Authorize the writer's native snapshot for an operator backup (`scripts/aws/backup.py`) |

Key expiry and revocation are checked during authorization. Authorizer caching is disabled. Keys are stored as hashes; full credentials are not returned by list endpoints or written into request logs.

Common errors include `401` for missing authentication, `403` for rejected identity or insufficient space/scope access, `409` for conflicting state/idempotency-key reuse, and `429` for quota/ingestion backpressure.

JSON request bodies use `Content-Type: application/json`. Required Idempotency-Key values are 1–128 ASCII letters/digits/underscore/dot/colon/hyphen. If-Match contains a specific quoted revision, for example `"rev_12"`; wildcard replacement is not supported. The API body is bounded to 1 MiB and an ingestion to 10,000 records. A dataset's registered schema and keys must additionally validate each record. Decimal is a string; JSON BIGINT values outside the JavaScript safe integer range use decimal strings.

## AWS request routing

Authorization runs before the route integration: API Gateway invokes the Authorizer Lambda, which reads the DynamoDB key record using its IAM execution role and AWS SDK. The authorizer returns identity and scope context.

| Route purpose | Integration |
| --- | --- |
| Ingestion submission | Lambda proxy integration to the intake function; S3 archive, then direct SQS send |
| Direct table writes and original receipts | Independent VPC Lambda proxy; fixed private 8082 RPC to the approved writer |
| Ingestion status and administrative APIs | Lambda proxy integration to the control API; DynamoDB status projections and CloudWatch metrics |
| Catalog, SQL submission, query status/results, and cancellation | Private HTTP proxy integration through VPC Link to an internal ALB listener; query target group contains ECS query Service Tasks |
| Pipeline definitions, environments and runs | Lambda proxy integration to the control API; scheduling invokes approved Lambda versions or ECS RunTask templates |

The ECS writer is a Service with one active Task that polls SQS. The query Service maintains long-lived Tasks behind the internal ALB. Independent compute uses Lambda Invoke or ECS RunTask. An ECS Service does not inherently require an ALB.

The [writer ownership protocol](aws-product-design.md#7-aws-组件选择) binds private control requests and replies to a task, process birth, immutable source/image, address and permit epoch. A replacement waits for explicit approval after the original task's terminal receipt. The private control service accepts no arbitrary SQL or URL and has no public route; public query and status endpoints are served by their own services.

For private HTTP integrations, API Gateway overwrites the internal identity headers from authorizer context. Query services check the requested space and operation against that context and are reachable only through the approved private ingress path.

## Space and dataset configuration

Space creation/rebuild uses an IAM-authenticated deployment bootstrap. It is not a public API. Each space generation has one database route and the five internal schemas ODS/DWD/DWS/ADS/CTL.

| Endpoint | Contract |
| --- | --- |
| `GET /v1/admin/spaces/{space_id}/configuration` | `datasets:manage`; return the atomic route/version and registered dataset definitions |
| `POST /v1/admin/spaces/{space_id}/datasets` | `datasets:manage`; required `If-Match: "<generation>"`; register immutable dataset/schema_version/schema/key_columns/partition_columns and optional ODS ingestion metadata; return `201` |

A same-definition retry returns the same registered metadata. A different definition at the same qualified dataset name returns `409`. Current-generation schema changes require the controlled stop/invalidate/rebuild process ([rebuild](../installation.md#10-破坏性升级)). The [machine contract](../../src/colvenna_isle/contracts/v2/contracts.schema.json) defines registration bounds and the event/version columns; the [user guide](../user-guide.md) describes registration. The installer enables public Bearer authentication only after the authorizer and control functions run the release code and reject missing or invalid credentials (see [fresh installation](../installation.md#6-全新安装)).

Pipeline targets may register `publication_state` with `protocol: complete_image_v1`, an explicit typed `schema`, the same nonnullable business `key_columns`, and `allow_empty`. This binds an internal complete state image to the target definition; it does not approve an incremental computation or a user task state-output interface. In 2.0.0, full Python task admission rejects such a target with `TASK_STATE_AUTHORITY_NOT_READY`. The writer validates and publishes data and state images through the native publication ledger described in the [pipeline design](pipeline-design.md#5-运行重试与发布).

## Synchronous table writes

Direct targets explicitly declare `write_owner: direct`, structured native `constraints` and optional `indexes`. Missing ownership never grants direct access. Native PRIMARY KEY or UNIQUE must actually support the nonnullable registered keys used by upsert/update/delete. Foreign keys reference previously registered direct targets with matching scalar types and native unique columns. No public DDL or arbitrary SQL is accepted.

`POST /v1/spaces/{space_id}/writes` requires `data:write` and `Idempotency-Key`. One request selects one current registered direct dataset and exactly one form:

| operation | Required form |
| --- | --- |
| `insert` / `upsert` | `records`: complete typed target rows |
| `update` | `updates`: complete `key` plus nonempty non-key `set` |
| `delete` | `keys`: complete registered keys |

The limit is 1 MiB / 1,000 items. `timeout_ms` defaults to 10,000 and accepts 1–20,000, covering admission, queueing and waiting. These are waiting budgets, not latency guarantees. The independent proxy owns no S3, FIFO or native database privileges.

```json
{
  "dataset": "dwd.orders",
  "schema_version": 1,
  "operation": "update",
  "updates": [{"key": {"id": 123}, "set": {"amount": "42.00"}}],
  "timeout_ms": 10000
}
```

`200 COMMITTED` follows one durable native transaction containing actual target changes, the original `ctl.writes` result, typed-key INSERT/UPDATE/DELETE changes with before/after partitions, and the dataset watermark. Zero-row updates/deletes still receive a unique result and commit sequence. Native constraint violations roll back the entire batch and persist an original `REJECTED` result, returned with `422`.

The ID binds current space generation, dataset and idempotency key; principal and normalized business content are immutable bindings. Waiting budgets do not alter the business hash. Concurrent duplicates share execution; changed content returns `409`, another principal cannot claim the original. A writer-only immutable S3 intent precedes queue admission, and native registration precedes business mutation. Neither registration means COMMITTED.

Only confirmed unexecuted originals receive `429 NOT_SUBMITTED`. `503/504 UNKNOWN` and lost connections require reconciliation of the original ID/key; an already-started transaction may still commit. The API neither promises timeout rollback nor substitutes asynchronous `202` ingestion. Original identities are retained across the 30-day retry window; expired and unresolved identities cannot become new operations.

`GET /v1/spaces/{space_id}/writes/{write_id}` reads the approved writer's authoritative native ledger. The original principal with `data:write`, or a space administrator with `datasets:manage`, may inspect it. Missing projections or an unavailable native ledger cannot decide success or rejection. Unknown results include the original `write_id` and `status_url` when identity is available.

See [synchronous writes](synchronous-writes.md) for the design. The write receipt supplies the commit sequence and watermark that a query with `consistency: after_write` uses as its native-read precondition (see [Catalog and SQL queries](#catalog-and-sql-queries)).

## Ingestion

### `POST /v1/spaces/{space_id}/ingestions`

Required scope: `ingest:write`. Required header: `Idempotency-Key`.

```json
{
  "dataset": "orders",
  "schema_version": 1,
  "operation": "upsert",
  "records": [
    {
      "event_id": "order-event-123",
      "source_version": 7,
      "event_time": "2026-10-15T08:00:00",
      "order_id": 123,
      "amount": "42.00"
    }
  ]
}
```

The dataset must be registered for ingestion. Operations are `append`, `upsert`, and `delete`; schema and required business identifiers determine the accepted record shape. A `TIMESTAMP` value carries no offset (`2026-10-15T08:00:00`); a `TIMESTAMPTZ` value requires one (`Z` or `+08:00`).

ODS retains unique event history, including old business versions and sparse tombstones. The same event ID with different typed content rejects the batch. A distinct event at the same business key/version also rejects it; lower versions are traceable `STALE` events and cannot regress the key's version/tombstone. A repeated batch returns its original committed receipt and commit sequence. The reserved `_colvenna_*` columns record event provenance.

Response: `202 Accepted`, only after the immutable source object is stored in S3 and SQS confirms `SendMessage`. The intake Lambda sends directly to SQS FIFO. DynamoDB contains one control-plane table for API Keys, routing, and asynchronous status projections; it does not dispatch ingestion messages.

```json
{
  "ingestion_id": "ing_123",
  "status": "ACCEPTED",
  "status_url": "/v1/spaces/space_123/ingestions/ing_123"
}
```

ACCEPTED means the source object is stored and SQS has confirmed the send. It does not mean the database has committed or an analytical result is visible. Reusing an idempotency key with the same payload returns the existing identity; a different payload returns `409`. The intake Lambda does not write DynamoDB; status projections are updated asynchronously from writer observations and database ledgers.

The intake protocol binds the ID to space generation, ODS dataset and Idempotency-Key, retains the first raw JSON bytes/VersionId, and sends a fixed object/hash contract. An uncertain send returns `503` with `status=UNKNOWN`, the original ingestion_id and status_url; clients reconcile or retry the same Idempotency-Key. A different principal cannot claim an archived identity; key rotation preserves principal ownership.

### `GET /v1/spaces/{space_id}/ingestions/{ingestion_id}`

Control-plane status can lag ingestion and database commits. Responses include `observed_at` and the projection version. A missing or unavailable projection returns an explicit `UNKNOWN`/reconciling state; it is not proof that the batch was never accepted. Committed data and publication state are authoritative in the DuckDB ledgers. Retries retain the same idempotency key and ingestion ID.

The status API allows the ingestion owner with `ingest:write`, or an administrator explicitly granted `monitor:read` in that space. It can rebuild observations through the approved writer; it reports the original native version/time and separates projection availability from the business result. In 2.0.0 the receipt reports ODS commits with `publication.status=NOT_PUBLISHED` and the `dwd`/`dws`/`ads` layers as `NOT_IMPLEMENTED`; downstream progress is reported by [pipeline runs](#get-v1spacesspace_idpipelinespipeline_idrunsrun_id) and the [space status](#get-v1adminspacesspace_idstatus). The response below is abridged.

An authorized ingestion owner or space administrator can read the receipt.

```json
{
  "ingestion_id": "ing_123",
  "status": "COMMITTED",
  "projection_version": 2,
  "observed_at": "2026-10-15T08:00:04Z",
  "publication": {"status": "NOT_PUBLISHED"},
  "layers": {
    "ods": {"status": "COMMITTED", "watermark": "ing_123"},
    "dwd": {"status": "NOT_IMPLEMENTED"},
    "dws": {"status": "NOT_IMPLEMENTED"},
    "ads": {"status": "NOT_IMPLEMENTED"}
  }
}
```

The `ods` watermark of a committed batch is its ingestion ID. Queue message delivery and FIFO deduplication do not replace the database ingestion ledger.

### Ingestion governance

Intake applies per-space batch and conservative shared-queue thresholds before archival. Overload returns `429` with `retry_after_ms` and `Retry-After`; unavailable age observations return `503`. Confirmed work retains its original identity and queue/archive bindings.

`GET /v1/spaces/{space_id}/ingestions/{ingestion_id}/diagnostics` requires `monitor:read` and reports bounded native attempts, DLQ reconciliation receipts and time-stamped backlog/age/capacity/retention observations.

`POST /v1/spaces/{space_id}/ingestions/{ingestion_id}/replay` accepts an empty body and requires both `ingest:write` and `ingest:replay` from the original owner. It successfully reconciles the current native authority and fixed original before audited dispatch. Existing commits return `200 COMMITTED` without sending; confirmed original sends return `202 ACCEPTED` with an audit ID. Native unavailability cannot authorize replay. Concurrent retries retain the original ID/payload. Replay expires at 30 days; archives remain at least 45 days and fourteen days after the last replay intent. Cleanup requires explicit reference and native/queue reconciliation.

## Catalog and SQL queries

### `GET /v1/spaces/{space_id}/catalog`

Required scope: `query:read`. Return only authorized schemas, tables, columns, and types. The catalog contains ODS/DWD/DWS/ADS objects the caller may query; internal ledgers and unpublished generations are excluded from the product catalog.

### `POST /v1/spaces/{space_id}/queries`

Required scope: `query:read`.

```json
{
  "sql": "SELECT order_date, sum(amount) AS revenue FROM ads.orders GROUP BY order_date",
  "client_request_id": "editor_request_123",
  "max_rows": 1000,
  "timeout_ms": 30000,
  "consistency": "published",
  "release_id": "ads_r8"
}
```

The service enforces server-defined bounds on result size, execution time, memory, and concurrent queries. The Query Editor executes authorized read-only SQL; authorization and external-access restrictions are server responsibilities.

`client_request_id` is required. It binds the immutable original request to its owner, space and generation. A retry returns the same query ID and its actual current state; changing the request under that ID returns `409`. Disconnecting HTTP does not cancel or rerun the query. `max_rows` is at most 10,000; `timeout_ms` is at most 300,000.

`consistency` is `published` by default. `after_ingestion` additionally requires an `ingestion_id` and reads through a connection in the writer instance after confirming that commit. It establishes the corresponding committed database state, not completion of every downstream transformation.

An explicit release_id must be honored. A reader that has not observed it cannot substitute an older release.

`current` reads registered native ingestion/direct tables and published pipeline targets, with actual generation, dataset commit sequence and snapshot sequence. Direct/ODS versions use `release_id: null`. `published` copies only analytical releases; an explicit `release_id` resolves that exact historical release within the same native read transaction. If the reader has not observed it, the original query fails with `PUBLICATION_RELEASE_NOT_READY`, retaining its `query_id`; it never substitutes another release. Reading an unpublished/direct dataset in published mode returns `PUBLICATION_NOT_AVAILABLE`. `after_write` / `after_ingestion` bind the original query execution to one bounded snapshot copied by the approved active writer after checking the Native commit and receipt visibility. User SQL runs on the sealed local copy. Limits are 100,000 total source rows, 2 MiB JSON and a 5-second Native copy budget; unknown capture replies are not retried. The current source loads the copy in exactly typed Arrow batches. Row counts and a conservative ASCII-JSON byte estimate are checked in the same native read transaction before fetching any rows; exceeding either returns `413 QUERY_SOURCE_QUOTA`. The byte estimate includes JSON escaping, schema/version metadata and formatting allowances for BIGINT and timestamps; the exact encoded-size check remains in place during capture. Capture still has a five-second budget (`QUERY_SOURCE_TIMEOUT`), and the original query retains its own execution deadline. 1.0.0 loaded these snapshots row by row; see [changes in 1.1.0](../release-notes.md#110-的变更). The [user guide](../user-guide.md) describes how to choose a consistency mode.

Each query stages only the registered datasets its SQL names. DuckDB's own parser finds them without running the SQL: base tables, the inner statement of `EXPLAIN`, and tables bound by functions such as `query_table`; names match case-insensitively, as the binder resolves them. `data_versions` covers exactly the staged datasets, plus the dataset of an exact `release_id` and the dataset of an `after_write` / `after_ingestion` receipt. Those two are versioned and confirmed in the same native read transaction even when the SQL names none of their rows. A query that names no dataset, such as `SELECT 1`, copies no rows and succeeds in every consistency; for `current` and `published` its `data_versions` is `[]`. In `published` mode only the named datasets matter: naming a direct, ODS or unpublished dataset returns `503 PUBLICATION_NOT_AVAILABLE`. Before copying, the reader counts the rows to stage in the same transaction. More than 2,000,000 rows fails at once with `413 QUERY_STAGING_LIMIT`, and the after-receipt 100,000-row limit fails before copying with `413 QUERY_SOURCE_QUOTA`. The private copy stays bounded by 1 GiB (`413 QUERY_SNAPSHOT_TOO_LARGE`). `current` / `published` move each dataset as one private Parquet file, written inside the native read transaction and loaded by server code before the session is sealed. After-receipt captures transport typed JSON rows. Measured staging times are in the [performance baseline](../performance.md#查询).

Response: `202 Accepted`.

```json
{
  "query_id": "qry_123",
  "status": "QUEUED",
  "status_url": "/v1/spaces/space_123/queries/qry_123"
}
```

### `GET /v1/spaces/{space_id}/queries/{query_id}`

Query owners can inspect the job and their results. An administrator needs both `query:read` and `query:manage` to inspect another owner's job metadata or cancel it; this does not grant the owner's SQL or result rows.

```json
{
  "query_id": "qry_123",
  "status": "SUCCEEDED",
  "elapsed_ms": 820,
  "data_versions": [{"dataset": "ads.orders", "release_id": "ads_r8"}],
  "columns": [
    {"name": "order_date", "type": "DATE", "encoding": "native_json"},
    {"name": "revenue", "type": "DECIMAL(18,2)", "encoding": "native_json"}
  ],
  "rows": [["2026-10-14", "1250.00"]],
  "next_page_token": null,
  "export_url": null
}
```

States are `QUEUED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLING`, and `CANCELLED`. Large results use paged retrieval or an authorized, short-lived export URL. Logs and general monitoring do not expose unauthorized query text or results.

Unavailable/unreconciled observations return `UNKNOWN`, not a successful job. Optional `page_token` and `page_size` query parameters request subsequent pages (`page_size` ≤ 10,000 and within the admitted query limit). Default read freshness requires actual release/watermark verification; see [external reader refresh](aws-product-design.md#2-存储扩展的能力与边界).

`view=metadata` returns lifecycle metadata. `view=result` returns the owner's rows; `view=sql` returns the owner's original SQL without rows. An explicitly authorized administrator can inspect metadata and cancel another owner's job, but explicit SQL/result requests are refused. Every response rechecks the current key and generation. Native column types and arbitrary or duplicate aliases remain positional; `native_text` retains values such as nanosecond timestamps and intervals without lossy adapters.

Results expire ten minutes after the original immutable S3 publication. Paging reads the exact result version/hash and never executes SQL again. The service limits retained results to 64 MiB globally, 32 MiB per space and 64 references. Expiry never becomes a new successful execution.

### `GET /v1/spaces/{space_id}/queries/{query_id}/export`

The result response supplies this endpoint with an owner-bound `token`, valid for at most two minutes and no later than result expiry. The authenticated endpoint returns the same typed JSON result from S3. A copied URL cannot give another principal access or bypass key revocation; it contains no AWS credentials. Page and export tokens have different purposes.

### `GET /v1/spaces/{space_id}/queries`

Return up to fifty recent jobs belonging to the authenticated principal in the current generation, newest first. Terminal query history and original SQL remain accessible for twenty-four hours after completion; active or unreconciled execution custody is retained. Exact-version garbage collection removes expired result/SQL objects and expired history references only after verifying that no live retention reference remains. A minimal query identity/status record remains to prevent replay of an expired original client request.

### `POST /v1/spaces/{space_id}/queries/{query_id}/cancel`

Cancel an owned query or one the caller is authorized to administer. Cancellation targets that query/task, not the database writer process. Terminal jobs retain their terminal status.

An atomically cancelled queued job can become `CANCELLED` only when it never acquired an execution. Running cancellation remains `CANCELLING` until the original process has been reaped. Completion and cancellation use conditional transitions; a completion that commits first retains its success, while a cancellation that commits first prevents later result publication.

## Pipeline execution

Definitions and scheduling are built into Colvenna Isle. The [pipeline design](pipeline-design.md), [Python file contract](python-task-contract.md) and [dependency environments](runtime-environments.md) specify executor and publication behavior. Query Editor access alone does not permit publishing code.

### Definition and version APIs

Required scope: `pipeline:manage`, with independent checks of source/target dataset access and approved profiles.

| Endpoint | Contract |
| --- | --- |
| `POST /v1/spaces/{space_id}/pipelines` | Create a DRAFT definition and return pipeline_id/revision |
| `GET /v1/spaces/{space_id}/pipelines/{pipeline_id}` | Return authorized metadata, draft revision, active version and schedule owner |
| `PUT /v1/spaces/{space_id}/pipelines/{pipeline_id}/draft` | Replace the draft using required `If-Match` revision; stale revisions return `412` |
| `POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/validate` | Validate the draft DAG, schema, code references, parameters, environments and profiles; return diagnostics |
| `POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/versions` | Publish a validated immutable version with Idempotency-Key; return `201` and pipeline_version |
| `GET /v1/spaces/{space_id}/pipelines/{pipeline_id}/versions/{pipeline_version}` | Return immutable version metadata and authorized artifact references |
| `POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/activate` | Activate a published version using required `If-Match` revision |
| `PUT /v1/spaces/{space_id}/pipelines/{pipeline_id}/schedule` | Update owner/expression/timezone and ENABLED/PAUSED state using required `If-Match` revision |

Draft requests contain `definition` (the parsed [YAML](../../examples/orders/pipeline.yaml)) and an artifact list with relative paths, upload IDs, SHA-256 and byte counts. Single artifacts are bounded to 10 MiB and the package to 50 MiB. The validate/version requests name the exact `draft_revision`; activation names `pipeline_version`. A control API upload endpoint, `POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/artifact-uploads`, issues bounded, short-lived uploads for authorized draft paths. The service verifies uploaded content before publishing; callers cannot reference arbitrary S3 buckets or executable AWS resources.

External tasks name `runtime_environment: {environment_id, version}`. Metadata exposes both `revision` for conditional changes and `draft_revision` for the exact code draft. Upload receipts contain `upload_url`, `upload_headers` and `expires_at`; send every returned header with the PUT, including the checksum, byte length and conditional creation. Treat the URL as a temporary credential. Create the draft with placeholder upload IDs, upload its files, then conditionally replace the draft with actual IDs before validation/publication.

Version publication binds the complete immutable code package, environment versions, resolved executor configuration and schema. A subsequent draft edit does not mutate existing versions. Active schedule configuration is separately revisioned; enabling or changing its owner never modifies a running job's bindings.

Schedule owner is `colvenna`, `external` or `none`. Colvenna Isle-owned schedules use supported EventBridge Scheduler rate/cron expressions. External ownership disables Colvenna Isle's automatic run creation; internal dependency progression continues. PAUSED stops new runs but does not cancel existing runs. Clients cannot override the schedule owner in a run request.

### `POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/runs`

Required scope: `pipeline:run`. Required header: `Idempotency-Key`.

```json
{
  "pipeline_version": "pv_<hash>",
  "inputs": {
    "<task_id>.<input_alias>": {"release_id": "<release of a pipeline dataset>"}
  },
  "parameters": {"high_value_threshold": "500.00"},
  "mode": "snapshot"
}
```

Root input keys are `<task_id>.<input_alias>`. Only root inputs that read pipeline datasets can be pinned to an authorized immutable release; pinning an ingestion (ODS) input returns `422 RUN_INPUT_RELEASE_REQUIRES_PIPELINE_DATASET`. Otherwise the server binds current committed watermarks during admission. Omitted inputs are fixed before dispatch; they do not remain "latest" during execution. Task-to-task inputs bind published outputs from this run and cannot be overridden.

Each task's published `processing.mode` independently selects full, incremental or partition_recompute. Admission binds delta intervals, previous state releases and affected partition sets as required. The request's mode specifies run intent, not a replacement for these task-level processing rules. A full downstream task receives a complete upstream release snapshot, even when the upstream computation was incremental; see [processing modes](processing-modes.md).

`mode` is `snapshot` or `backfill`. Backfill additionally names authorized partitions and requires a compatible partition-aware definition. Dataset schemas, parameter types and profiles come from the published version. Arbitrary scripts, AWS roles, targets or executors cannot be supplied in a run request.

Return `202` only after durable admission:

```json
{
  "run_id": "run_123",
  "pipeline_version": "v7",
  "status": "QUEUED",
  "status_url": "/v1/spaces/space_123/pipelines/orders/runs/run_123"
}
```

The same key and canonical request return the same run identity, including after an HTTP timeout; incompatible reuse returns `409`. Runtime request identity is retained for at least the run and published replay window, independently of FIFO deduplication.

### `GET /v1/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}`

Run owners with `pipeline:run`, or authorized administrators with `monitor:read`, may inspect the run. Return bound inputs, parameters, environment/code identities, task/attempt states, import progress, actual releases and `observed_at`/projection version:

```json
{
  "run_id": "run_123",
  "pipeline_version": "v7",
  "status": "RUNNING",
  "observed_at": "2026-10-15T08:05:00Z",
  "projection_version": 8,
  "tasks": [
    {"task_id": "clean_orders", "job_id": "job_1", "status": "PUBLISHED", "release_id": "dwd_r13"},
    {"task_id": "score_orders", "job_id": "job_2", "status": "VALIDATING", "attempt_id": "attempt_1"},
    {"task_id": "daily_sales", "job_id": "job_3", "status": "BLOCKED"},
    {"task_id": "dashboard", "job_id": "job_4", "status": "BLOCKED"}
  ]
}
```

Run states are QUEUED, RUNNING, PUBLISHED, FAILED, CANCELLING, CANCELLED and UNKNOWN; trial runs end VALIDATED. A QUEUED run that has not started reports `waiting: {code, since}` (for example `PIPELINE_RUN_ACTIVE`, `SPACE_RUN_QUOTA` or `WRITER_AUTHORITY_UNAVAILABLE`). PUBLISHED means every required task has published; it does not promise one global transaction across the DAG. Task states are defined in the pipeline design. Missing/conflicting projections report UNKNOWN and reconcile authoritative execution/publication records.

### Cancellation and retry

`POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}/cancel` requires run ownership or administrative authorization and returns the cancellation/publication outcome. Already published task releases remain published. Cancellation revokes publication eligibility before acknowledging a cancelled task; it cannot merely set a DDB flag that races a writer commit.

`POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}/retry` requires `pipeline:run` and Idempotency-Key. It resumes an eligible FAILED run with new attempts for failed/unpublished tasks, preserving definition, environments, inputs and already published upstream releases. UNKNOWN must be reconciled first. Changed inputs/code or a rerun of published tasks require a new run and publication conflict checks.

### Dependency environment APIs

Required scope: `runtime:manage`; read access follows the authorized space and management/monitoring scopes.

| Endpoint | Contract |
| --- | --- |
| `POST /v1/spaces/{space_id}/runtime-environments` | Create a versioned DRAFT from dependency source, approved Python/platform and build profile |
| `POST /v1/spaces/{space_id}/runtime-environments/{environment_id}/versions/{version}/build` | Idempotently admit a build and return `202`/build_id |
| `GET /v1/spaces/{space_id}/runtime-environments/{environment_id}/versions/{version}` | Return build state, locked dependencies/hash, image digest and approved executor bindings |

The build service resolves full dependencies and verifies packages before READY. Published pipelines reference READY versions. Private package credentials use server-configured secret references, not request parameters or plaintext requirements. The server checks delegation and approved indexes/build profiles; READY versions cannot be overwritten.

Builds run from fixed source on ARM64, verify their artifacts and bind numeric Lambda versions or ECS task definition revisions; a dependency upgrade creates a new environment version (see [dependency environments](runtime-environments.md)). A build observation retains its original `build_id` and, when confirmed, `codebuild_id`. READY metadata also exposes exact `dependency_lock` and `provenance` S3 version/hash/byte references and `runtime` Python/ABI/platform/base-image/adapter/source identities. UNKNOWN does not dispatch another build.

## Monitoring and key administration

### `GET /v1/admin/spaces/{space_id}/status`

Required scope: `monitor:read`.

Return collection timestamps, writer task/instance identity, heartbeat and last commit, queue backlog/oldest-message age/DLQ count, layer watermarks, and job/query states. Missing observations are UNKNOWN rather than healthy. Scope monitoring uses control projections and metrics, not repeated full-table scans.

Each section is an `observation` with `status` (`OK`, `DEGRADED`, `FAILED`, `STOPPED`, `UNKNOWN`), `observed_at`, `age_seconds`, `source`, `reason` and metrics; `datasets` lists every registered dataset's native version and current release. The Admin Console overview renders these sections; see the [user guide](../user-guide.md).

### Governance routes

| Route | Scope | Contract |
| --- | --- | --- |
| `GET /v1/admin/spaces/{space_id}/metrics?window_seconds=` | `monitor:read` | Time-stamped series; missing data is `UNKNOWN` with a reason |
| `GET /v1/admin/spaces/{space_id}/trace?kind=&id=&pipeline_id=` | `monitor:read` | Run/ingestion/release/query nodes with authority and conflicts; SQL text only with `audit:read`, never results |
| `GET /v1/admin/spaces/{space_id}/audit?limit=&action=&page_token=` | `audit:read` | Immutable audit records written in the audited change's own transaction |
| `GET /v1/admin/spaces/{space_id}/quotas` | `monitor:read` | Operator-set space quotas and observed usage |
| `GET /v1/admin/spaces/{space_id}/costs?since=&until=` | `monitor:read` | Measured usage × list price per workload; not an invoice |
| `GET`/`PUT /v1/admin/spaces/{space_id}/retention` | `retention:manage` (`monitor:read` reads) | Policy with product minimums; `If-Match` revision |
| `POST .../retention/holds`, `POST .../holds/{hold_id}/release` | `retention:manage` | Holds for backfill, audit and same-version backups |
| `POST .../retention/collections`, `GET .../collections/{collection_id}` | `retention:manage` (`monitor:read` reads) | Idempotent collection with exact-version receipts |
| `GET /v1/spaces/{space_id}/pipelines`, `.../versions`, `.../runs`, `.../draft/artifacts?path=` | `pipeline:manage` / `pipeline:run` / `monitor:read` | Console lists and draft source readback |
| `POST /v1/spaces/{space_id}/pipelines/{pipeline_id}/artifacts` | `pipeline:manage` | Inline UTF-8 upload with the same admission, quota and audit as presigned uploads |
| `POST .../pipelines/{pipeline_id}/trial-runs`, `GET .../runs/{run_id}/preview` | `pipeline:run` (+ `query:read` for rows) | Sample trial runs that are validated like an import but never granted or published |
| `GET /v1/spaces/{space_id}/runtime-environments` | `runtime:manage` / `monitor:read` | Environment list; version metadata adds `failure` and `dependency_source` |

The [user guide](../user-guide.md) describes audit, diagnostics, quotas, costs and retention.

### `GET /v1/admin/spaces/{space_id}/api-keys`

Required scope: `keys:manage`.

Return key_id, name, fingerprint, scopes, created_at, expires_at, status, and last_used_at. Never return token or key hash.

### `POST /v1/admin/spaces/{space_id}/api-keys`

Required scope: `keys:manage`.

```json
{
  "name": "orders-ingestion",
  "scopes": ["ingest:write"],
  "expires_at": "2027-01-15T00:00:00Z"
}
```

Response: `201 Created`.

```json
{
  "key": {
    "key_id": "key_123",
    "name": "orders-ingestion",
    "scopes": ["ingest:write"],
    "expires_at": "2027-01-15T00:00:00Z",
    "status": "ACTIVE"
  },
  "token": "<one-time-api-key>"
}
```

The server enforces space ownership and delegation limits: scopes must be a subset of the issuer's and expiry must not exceed the issuer's. A supplied scope does not grant permission by itself. The full token is available only in this creation response. Metadata includes the trusted creation_request_id for uncertain-response reconciliation; token recovery is unavailable.

### `POST /v1/admin/spaces/{space_id}/api-keys/{key_id}/rotate`

Required scope: `keys:manage`. The body uses the same name/scopes/expires_at fields as creation. Return `201` with replacement metadata and one-time token. Rotation preserves principal ownership, records replaces_key_id/replacement_key_id and admits one replacement per original credential. The caller transitions to the replacement, then explicitly revokes the previous credential. Rotation cannot expand the previous key's scopes or the caller's delegation ceiling.

### `POST /v1/admin/spaces/{space_id}/api-keys/{key_id}/revoke`

Required scope: `keys:manage`. Revoke the key and retain its audit identity. Rotation creates a replacement key, lets the caller transition, then revokes the previous key.

The first management key is issued by a controlled deployment bootstrap. Public APIs do not provide an unauthenticated administrator/key-creation path.

## Deployment region and cost observations

The deployment selects one commercial AWS region (partition `aws`) through `COLVENNA_REGION` / `AWS_REGION`; conflicting settings fail before AWS calls. Stack resources use `AWS::Region`, services use the injected runtime region, and executor approval binds account, exact deployed region and runtime namespace. A contract-valid ARN from another region still cannot pass runtime approval. Chinese, GovCloud and isolated partitions are outside this contract.

Cost reports use only a price snapshot for the deployed region. With none, `status` is `UNKNOWN`, `prices.retrieved` is null, `prices.sources`, `lines` and `totals` are empty, and `unmeasured` names `REGIONAL_PRICES_UNAVAILABLE`. Existing `COMPLETE` / `PARTIAL` observations retain their meanings when regional prices are available. Price snapshot keys and provenance identify the region actually priced; they do not configure deployment.

DuckDB is a trademark of the DuckDB Foundation. Colvenna Isle is independent of the DuckDB Foundation and is not endorsed by it.
