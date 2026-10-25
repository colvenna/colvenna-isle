"""Single source for generated JSON Schema, OpenAPI and bounded profile contracts."""

SCOPES = ["datasets:manage", "ingest:write", "ingest:replay", "data:write", "query:read", "query:manage",
          "pipeline:run", "pipeline:manage", "runtime:manage", "monitor:read", "keys:manage",
          "audit:read", "retention:manage", "backup:manage"]
STATES = {
    "task": {
        "BLOCKED": ["READY", "FAILED", "CANCELLING"],
        "READY": ["DISPATCHING", "FAILED", "CANCELLING"],
        "DISPATCHING": ["RUNNING", "RETRY_WAIT", "FAILED", "CANCELLING"],
        "RUNNING": ["STAGED", "VALIDATING", "RETRY_WAIT", "FAILED", "CANCELLING"],
        "STAGED": ["VALIDATING", "VALIDATED", "FAILED", "CANCELLING"],
        "VALIDATING": ["IMPORTING", "FAILED", "CANCELLING"],
        "IMPORTING": ["PUBLISHED", "FAILED", "CANCELLING"],
        "RETRY_WAIT": ["READY", "FAILED", "CANCELLING"],
        "FAILED": ["RETRY_WAIT"],
        "CANCELLING": ["CANCELLED", "PUBLISHED"],
        # A trial attempt is validated like a candidate but never granted or imported.
        "CANCELLED": [], "PUBLISHED": [], "VALIDATED": [],
    },
    "run": {
        "QUEUED": ["RUNNING", "FAILED", "CANCELLING"],
        "RUNNING": ["PUBLISHED", "VALIDATED", "FAILED", "CANCELLING"],
        "FAILED": ["QUEUED"],
        "CANCELLING": ["CANCELLED", "PUBLISHED"],
        "CANCELLED": [], "PUBLISHED": [], "VALIDATED": [],
    },
    "query": {
        "QUEUED": ["RUNNING", "FAILED", "CANCELLING", "CANCELLED"],
        "RUNNING": ["SUCCEEDED", "FAILED", "CANCELLING"],
        "CANCELLING": ["CANCELLED"],
        "FAILED": [], "CANCELLED": [], "SUCCEEDED": [],
    },
    "environment": {
        "DRAFT": ["RESOLVING", "FAILED"], "RESOLVING": ["BUILDING", "FAILED"],
        "BUILDING": ["VERIFYING", "FAILED"], "VERIFYING": ["READY", "FAILED"],
        "READY": [], "FAILED": [],
    },
    "ingestion": {
        "ACCEPTED": ["COMMITTED", "FAILED"], "COMMITTED": ["PUBLISHED"],
        "PUBLISHED": [], "FAILED": ["COMMITTED"],
    },
}
PROFILES = {
    "writer_short": {"executor": "writer", "languages": ["sql"], "max_timeout_seconds": 30},
    "python_lambda_standard": {"executor": "lambda", "languages": ["python"], "max_timeout_seconds": 900},
    "sql_lambda_standard": {"executor": "lambda", "languages": ["sql"], "max_timeout_seconds": 900},
    "sql_ecs_medium": {"executor": "ecs_task", "languages": ["sql"], "max_timeout_seconds": 86400},
    "python_ecs_medium": {"executor": "ecs_task", "languages": ["python"], "max_timeout_seconds": 86400},
}
LIMITS = {
    "api_body_bytes": 1048576, "ingestion_records": 10000, "write_records": 1000, "sql_bytes": 65536,
    "query_max_rows": 10000, "query_timeout_ms": 300000, "pipeline_tasks": 128,
    "manifest_files": 10000, "max_attempts": 10, "artifact_bytes": 10485760,
    "artifact_package_bytes": 52428800, "parameters": 128,
    "supported_lambda_modes": ["standard"], "max_active_runs": 1,
}

# Server-owned adapter budgets. A task cannot supply a filesystem/output override.
TASK_BUDGETS = {
    "python_lambda_standard": {"workspace_bytes": 536870912, "output_bytes": 134217728,
                               "output_rows": 1000000, "file_nodes": 20000, "log_bytes": 65536},
    "python_ecs_medium": {"workspace_bytes": 1073741824, "output_bytes": 268435456,
                         "output_rows": 2000000, "file_nodes": 20000, "log_bytes": 65536},
    "sql_lambda_standard": {"workspace_bytes": 536870912, "output_bytes": 134217728,
                            "output_rows": 1000000, "file_nodes": 20000, "log_bytes": 65536,
                            "memory_bytes": 1073741824},
    "sql_ecs_medium": {"workspace_bytes": 1073741824, "output_bytes": 268435456,
                       "output_rows": 2000000, "file_nodes": 20000, "log_bytes": 65536,
                       "memory_bytes": 2147483648},
    # Short SQL shares the sole writer task; its separate process is bounded
    # well below the native connection's own memory and disk allowance.
    "writer_short": {"workspace_bytes": 268435456, "output_bytes": 33554432,
                     "output_rows": 100000, "file_nodes": 2000, "log_bytes": 65536,
                     "memory_bytes": 268435456},
}
AUDIT_ACTIONS = [
    "KEY_BOOTSTRAPPED", "KEY_CREATED", "KEY_ROTATED", "KEY_REVOKED", "DATASET_REGISTERED",
    "PIPELINE_CREATED", "PIPELINE_DRAFT_REPLACED", "PIPELINE_ARTIFACT_ADMITTED", "PIPELINE_VERSION_PUBLISHED",
    "PIPELINE_ACTIVATED", "PIPELINE_SCHEDULE_CHANGED", "ENVIRONMENT_CREATED", "ENVIRONMENT_BUILD_ADMITTED",
    "RUN_CREATED", "RUN_CANCEL_REQUESTED", "RUN_RETRIED", "TRIAL_CREATED", "INGESTION_REPLAY_INTENT",
    "RETENTION_POLICY_CHANGED", "RETENTION_HOLD_PLACED", "RETENTION_HOLD_RELEASED",
    "RETENTION_COLLECTION_REQUESTED", "RETENTION_COLLECTION_COMPLETED", "QUOTA_POLICY_CHANGED",
    "GENERATION_PURGED", "BACKUP_CREATED", "SPACE_RESTORED",
]
# Current-version retention: (default, product minimum, maximum) in seconds or counts.
# Spaces named probe_* are qualification-only and may use the probe minimum.
RETENTION_POLICY = {
    "raw_seconds": (3888000, 3888000, 31536000), "replay_grace_seconds": (1209600, 1209600, 31536000),
    "upload_seconds": (86400, 3600, 2592000), "staging_seconds": (604800, 3600, 31536000),
    "pipeline_version_seconds": (2592000, 86400, 31536000), "release_keep": (3, 1, 1000),
    "release_seconds": (604800, 3600, 31536000), "failed_build_seconds": (604800, 3600, 31536000),
}
RETENTION_PROBE_MINIMUM = 60
# Per-space admission quotas set by the IAM operator (never self-delegated).
QUOTA_POLICY = {
    "max_active_runs": (4, 1, 64), "max_dispatched_jobs": (8, 1, 256), "max_concurrent_builds": (2, 1, 4),
    "max_pipelines": (64, 1, 512), "max_pipeline_versions": (200, 1, 2000),
    "max_stored_code_bytes": (536870912, 1048576, 10737418240), "max_trial_sample_rows": (1000, 1, 10000),
    "max_task_memory_bytes": (2147483648, 268435456, 2147483648),
    "max_task_workspace_bytes": (1073741824, 268435456, 1073741824),
}
COST_CATEGORIES = ["lambda_compute", "ecs_compute", "writer_compute", "codebuild", "s3_storage", "s3_requests",
                   "ecr_storage", "query_compute"]

# Platform-maintained key state for merge_keys targets: one row per key ever
# published, including DELETE tombstones and the highest accepted version.
KEY_STATE_METADATA = [
    {"name": "_colvenna_version", "type": "BIGINT", "nullable": False},
    {"name": "_colvenna_sequence", "type": "BIGINT", "nullable": False},
    {"name": "_colvenna_deleted", "type": "BOOLEAN", "nullable": False},
]


def text(maximum=256, pattern=None):
    value = {"type": "string", "minLength": 1, "maxLength": maximum}
    if pattern:
        value["pattern"] = pattern
    return value


def integer(minimum=0, maximum=9007199254740991):
    return {"type": "integer", "minimum": minimum, "maximum": maximum}


def enum(*values):
    return {"enum": list(values)}


def ref(name):
    return {"$ref": f"#/$defs/{name}"}


def array(item, maximum=10000, minimum=0, unique=False):
    value = {"type": "array", "items": item, "minItems": minimum, "maxItems": maximum}
    if unique:
        value["uniqueItems"] = True
    return value


def obj(properties, required=None):
    return {"type": "object", "properties": properties,
            "required": list(properties) if required is None else required,
            "additionalProperties": False}


def mapping(value, maximum=128, minimum=0, keys="identifier"):
    return {"type": "object", "additionalProperties": value, "maxProperties": maximum,
            "minProperties": minimum, "propertyNames": ref(keys)}


def nullable(value):
    return {"anyOf": [value, {"type": "null"}]}


def schemas():
    identifier = text(128, r"^[A-Za-z_][A-Za-z0-9_.-]*$")
    sha = text(64, r"^[0-9a-f]{64}$")
    relative = text(512, r"^(?!/)(?!.*(?:^|/)\.\.(?:/|$))(?!.*\\)[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*$")
    dataset = text(256, r"^(ods|dwd|dws|ads)\.[A-Za-z_][A-Za-z0-9_]*$")
    column = text(128, r"^[A-Za-z_][A-Za-z0-9_]*$")
    stamp = {**text(40, r"^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|[+-]\d\d:\d\d)$"),
             "format": "colvenna-timestamp"}
    s3 = text(2048, r"^s3://[a-z0-9][a-z0-9.-]+/[^\s*?]+$")
    boolean = {"type": "boolean"}
    scalar = {"type": ["string", "integer", "number", "boolean", "null"]}
    d = {
        "identifier": identifier, "sha256": sha, "relative_path": relative,
        "task_identifier": text(128, r"^[A-Za-z_][A-Za-z0-9_-]*$"),
        "input_alias": text(128, r"^[A-Za-z_][A-Za-z0-9_]*$"),
        "dataset": dataset, "timestamp": stamp, "s3_uri": s3,
        "type": text(40, r"^(BIGINT|BOOLEAN|VARCHAR|VARCHAR\[\]|DATE|TIMESTAMP|TIMESTAMPTZ|JSON|BLOB|DECIMAL\([1-9][0-9]?,[0-9]{1,2}\))$"),
        "column": obj({"name": column, "type": ref("type"), "nullable": boolean}),
        "schema": array(ref("column"), 256, 1),
        "parameters": mapping(scalar),
        "watermark": obj({"ods_sequence": integer()}),
        "partition": mapping(scalar, 16, 1),
        "selection": obj({"mode": enum("snapshot", "changes", "affected_keys", "affected_partitions"),
                          "partitions": array(ref("partition"), 10000)}, ["mode"]),
        "input_binding": {
            "oneOf": [
                obj({"dataset": dataset, "selection": enum("snapshot", "changes", "affected_partitions")}),
                obj({"task": ref("task_identifier"), "selection": enum("snapshot", "changes", "affected_partitions")}),
                obj({"state": enum("target_key_state"), "selection": enum("affected_keys")}),
            ]
        },
        "processing": obj({
            "mode": enum("full", "incremental", "partition_recompute"),
            "bootstrap": enum("full"), "on_missing_state": enum("fail", "full"),
            "partition_columns": array(column, 16, 1, True),
            "on_unbounded_impact": enum("fail", "full"),
        }, ["mode"]),
        "output": obj({
            "dataset": dataset, "write_mode": enum("versioned_replace", "merge_keys", "replace_partitions"),
            "key_columns": array(column, 16, 1, True), "allow_empty": boolean,
            "schema": ref("schema"), "change_metadata": ref("schema"),
            "partition_columns": array(column, 16, 1, True),
        }, ["dataset", "write_mode", "key_columns", "allow_empty", "schema"]),
        "parameter_definition": obj({
            "type": enum("decimal_string", "date", "timestamp", "timestamptz", "integer", "boolean", "string"),
            "default": scalar, "minimum": scalar, "maximum": scalar,
        }, ["type"]),
        "schedule": obj({"owner": enum("colvenna", "external", "none"), "expression": text(256),
                         "timezone": text(128)}, ["owner"]),
        "environment_reference": obj({"environment_id": identifier, "version": identifier}),
        "task": obj({
            "task_id": ref("task_identifier"), "language": enum("sql", "python"), "processing": ref("processing"),
            "executor": enum("writer", "lambda", "ecs_task"), "entrypoint": relative,
            "runtime_environment": ref("environment_reference"), "resource_profile": {"enum": list(PROFILES)},
            "input_mode": enum("local_files", "s3_files"), "timeout_seconds": integer(1, 86400),
            "max_attempts": integer(1, 10), "depends_on": array(ref("task_identifier"), 128, unique=True),
            "inputs": mapping(ref("input_binding"), 128, 1, keys="input_alias"), "output": ref("output"),
        }, ["task_id", "language", "processing", "executor", "entrypoint", "resource_profile",
            "timeout_seconds", "max_attempts", "inputs", "output"]),
        "pipeline": obj({
            "spec_version": {"const": 1}, "pipeline_id": identifier, "name": text(256),
            "schedule": ref("schedule"), "max_active_runs": {"const": 1}, "coalesce_pending": {"const": True},
            "parameters": mapping(ref("parameter_definition")), "tasks": array(ref("task"), 128, 1),
        }, ["spec_version", "pipeline_id", "name", "schedule", "max_active_runs", "coalesce_pending", "tasks"]),
        "input_file": obj({
            "s3_uri": s3, "object_version": text(1024), "sha256": sha,
            "rows": integer(), "bytes": integer(), "local_path": text(2048, r"^/"),
        }, ["s3_uri", "object_version", "sha256", "rows", "bytes"]),
        "context_input": obj({
            "dataset": dataset, "release_id": identifier, "manifest_sha256": sha,
            "watermark": ref("watermark"), "selection": ref("selection"),
            "schema": ref("schema"), "files": array(ref("input_file")),
        }),
        "input_manifest": obj({
            "contract_version": {"const": 2}, "dataset": dataset, "release_id": identifier,
            "watermark": ref("watermark"), "schema": ref("schema"), "files": array(ref("input_file")),
        }),
        "context_processing": obj({
            "mode": enum("full", "incremental", "partition_recompute"),
            "from_watermark": ref("watermark"), "to_watermark": ref("watermark"),
            "state_release_id": nullable(identifier), "bootstrap": boolean,
            "affected_partitions": array(ref("partition")),
        }, ["mode"]),
        "context_output": obj({
            "name": {"const": "result"}, "dataset": dataset, "directory": text(2048, r"^/"),
            "write_mode": enum("versioned_replace", "merge_keys", "replace_partitions"),
            "expected_previous_release": nullable(identifier), "key_columns": array(column, 16, 1, True),
            "allow_empty": boolean, "replace_partitions": array(ref("partition")),
            "schema": ref("schema"), "target_schema": ref("schema"),
            "partition_columns": array(column, 16, 1, True),
        }, ["name", "dataset", "directory", "write_mode", "expected_previous_release",
            "key_columns", "allow_empty", "replace_partitions", "schema"]),
        "context": obj({
            "contract_version": {"const": 2}, "space_id": identifier, "pipeline_id": identifier,
            "pipeline_version": identifier, "run_id": identifier, "task_id": ref("task_identifier"),
            "job_id": identifier, "attempt_id": identifier, "execution_id": identifier,
            "input_mode": enum("local_files", "s3_files"), "parameters": ref("parameters"),
            "inputs": mapping(ref("context_input"), 128, 1, keys="input_alias"), "output": ref("context_output"),
            "processing": ref("context_processing"),
        }, ["contract_version", "space_id", "pipeline_id", "pipeline_version", "run_id", "task_id",
            "job_id", "attempt_id", "execution_id", "input_mode", "parameters", "inputs", "output"]),
        "output_file": obj({"path": relative, "rows": integer(), "bytes": integer(), "sha256": sha,
                            "partition_values": mapping(scalar, 16)}),
        "output_manifest": obj({
            "contract_version": {"const": 2}, "job_id": identifier, "attempt_id": identifier,
            "execution_id": identifier, "output_name": {"const": "result"}, "format": {"const": "parquet"},
            "schema": ref("schema"), "row_count": integer(), "files": array(ref("output_file")),
            "replace_partitions": array(ref("partition")),
            "metrics": mapping({"type": ["integer", "number", "string", "boolean"]}),
        }, ["contract_version", "job_id", "attempt_id", "execution_id", "output_name", "format",
            "schema", "row_count", "files"]),
        "ingestion_request": obj({
            "dataset": text(128, r"^[A-Za-z_][A-Za-z0-9_]*$"), "schema_version": integer(1),
            "operation": enum("append", "upsert", "delete"),
            "records": array({"type": "object", "minProperties": 1}, 10000, 1),
        }),
        "write_request": obj({
            "dataset": dataset, "schema_version": integer(1),
            "operation": enum("insert", "upsert", "update", "delete"),
            "records": array({"type": "object", "minProperties": 1}, 1000, 1),
            "updates": array(obj({"key": {"type": "object", "minProperties": 1},
                                  "set": {"type": "object", "minProperties": 1}}), 1000, 1),
            "keys": array({"type": "object", "minProperties": 1}, 1000, 1),
            "timeout_ms": integer(1, 20000),
        }, ["dataset", "schema_version", "operation"]),
        "write_receipt": obj({
            "write_id": text(70, r"^write_[0-9a-f]{64}$"), "space_id": identifier,
            "generation": identifier, "dataset": dataset, "schema_version": integer(1),
            "status": enum("COMMITTED", "REJECTED"), "commit_sequence": integer(1),
            "affected_rows": integer(), "committed_at": stamp, "rejected_at": stamp,
            "code": text(128), "retry_until": stamp, "dataset_watermark": integer(1),
        }, ["write_id", "space_id", "generation", "dataset", "schema_version", "status", "retry_until"]),
        "write_unknown": obj({
            "write_id": text(70, r"^write_[0-9a-f]{64}$"), "status": {"const": "UNKNOWN"},
            "status_url": text(2048, r"^/v1/spaces/"), "code": text(128),
            "message": text(512), "request_id": text(256),
        }, ["write_id", "status", "status_url", "code"]),
        "write_not_submitted": obj({
            "write_id": text(70, r"^write_[0-9a-f]{64}$"), "status": {"const": "NOT_SUBMITTED"},
            "code": text(128), "retry_after_ms": integer(1),
        }),
        "ingestion_message": obj({
            "contract_version": {"const": 2}, "ingestion_id": identifier, "space_id": identifier,
            "generation": text(36, r"^gen_[0-9a-f]{32}$"), "route_version": integer(1),
            "dataset": text(132, r"^ods\.[a-z_][a-z0-9_]*$"), "schema_version": integer(1),
            "dataset_definition_sha256": sha, "principal_id": identifier, "payload_sha256": sha,
            "source": obj({"bucket": text(63, r"^[a-z0-9][a-z0-9.-]+$"),
                           "key": text(1024, r"^spaces/[A-Za-z_][A-Za-z0-9_.-]*/ingest/"),
                           "version_id": text(1024), "sha256": sha,
                           "bytes": integer(1, LIMITS["api_body_bytes"])}),
            "producer_source_commit": text(40, r"^[0-9a-f]{40}$"),
        }),
        "dataset_registration": obj({
            "dataset": dataset, "schema_version": integer(1),
            "schema": ref("schema"), "key_columns": array(column, 16, unique=True),
            "partition_columns": array(column, 16, unique=True),
            "write_owner": enum("direct", "ingestion", "pipeline"),
            "publication_state": obj({
                "protocol": enum("complete_image_v1", "key_state_v1"), "schema": ref("schema"),
                "key_columns": array(column, 16, 1, True), "allow_empty": boolean,
            }),
            "constraints": ref("table_constraints"),
            "indexes": array(array(column, 16, 1, True), 16, unique=True),
            "ingestion": obj({
                "operations": array(enum("append", "upsert", "delete"), 3, 1, True),
                "event_id_column": column, "source_version_column": column,
            }, ["operations", "event_id_column"]),
        }, ["dataset", "schema_version", "schema", "key_columns", "partition_columns"]),
        "table_constraints": obj({
            "primary_key": array(column, 16, 1, True),
            "unique": array(array(column, 16, 1, True), 16, unique=True),
            "foreign_keys": array(obj({
                "columns": array(column, 16, 1, True), "references": dataset,
                "referenced_columns": array(column, 16, 1, True),
            }), 16, unique=True),
            "checks": array(obj({
                "column": column, "operator": enum("=", "<>", "<", "<=", ">", ">="),
                "value": scalar,
            }), 32, unique=True),
        }, []),
        "registered_dataset": obj({
            "definition": ref("dataset_registration"), "definition_sha256": sha,
            "registered_revision": integer(1),
        }),
        "space_configuration": obj({
            "contract_version": {"const": 2}, "space_id": identifier,
            "generation": identifier, "revision": integer(1), "route_version": integer(1),
            "database_uri": s3, "schemas": {"const": ["ods", "dwd", "dws", "ads", "ctl"]},
            "datasets": mapping(ref("registered_dataset"), 64, keys="dataset"),
            "ingestion_policy": ref("ingestion_policy"),
        }, ["contract_version", "space_id", "generation", "revision", "route_version",
            "database_uri", "schemas", "datasets"]),
        "ingestion_policy": obj({
            "max_pending_messages": integer(1, 100000), "max_oldest_seconds": integer(1, 604800),
            "max_drain_seconds": integer(1, 604800), "unavailable_writer_buffer": integer(1, 1000),
            "max_batch_records": integer(1, LIMITS["ingestion_records"]),
            "max_batch_bytes": integer(1, LIMITS["api_body_bytes"]),
        }),
        "query_request": obj({
            "sql": text(65536), "client_request_id": identifier,
            "max_rows": integer(1, 10000), "timeout_ms": integer(1, 300000),
            "consistency": enum("published", "current", "after_ingestion", "after_write"),
            "release_id": identifier, "ingestion_id": identifier, "write_id": identifier,
        }, ["sql", "client_request_id"]),
        "key_request": obj({"name": text(256), "scopes": array({"enum": SCOPES}, len(SCOPES), 1, True),
                            "expires_at": stamp}),
        "artifact": obj({"path": relative, "upload_id": identifier, "sha256": sha, "bytes": integer(1, 10485760)}),
        "draft_request": obj({"definition": ref("pipeline"), "artifacts": array(ref("artifact"), 128, 1)}),
        "upload_request": obj({"path": relative, "sha256": sha, "bytes": integer(1, 10485760)}),
        "validate_request": obj({"draft_revision": identifier}),
        "version_request": obj({"draft_revision": identifier}),
        "activate_request": obj({"pipeline_version": identifier}),
        "schedule_request": obj({
            "owner": enum("colvenna", "external", "none"), "expression": text(256),
            "timezone": text(128), "state": enum("ENABLED", "PAUSED"),
        }, ["owner", "state"]),
        "run_input": obj({"release_id": identifier}),
        "run_request": obj({
            "pipeline_version": identifier,
            "inputs": {"type": "object", "additionalProperties": ref("run_input"), "maxProperties": 128,
                       "propertyNames": text(256, r"^[A-Za-z_][A-Za-z0-9_-]*\.[A-Za-z_][A-Za-z0-9_-]*$")},
            "parameters": ref("parameters"), "mode": enum("snapshot", "backfill"),
            "partitions": array(ref("partition"), 10000, 1),
        }, ["pipeline_version", "mode"]),
        "environment_request": obj({
            "environment_id": identifier, "version": identifier, "python_version": {"const": "3.12"},
            "platform": {"const": "linux_arm64"}, "build_profile": enum("binary_wheels"),
            "dependency_source": obj({"format": {"const": "requirements"}, "content": text(65536)}),
        }),
        "empty_request": obj({}),
        "http_request": obj({
            "method": enum("GET", "POST", "PUT"), "path": text(2048, r"^/v1/"),
            "headers": {"type": "object", "additionalProperties": text(8192),
                        "propertyNames": text(128, r"^[A-Za-z0-9-]+$")},
            "query": {"type": "object", "maxProperties": 8,
                      "additionalProperties": {"type": ["string", "integer"]},
                      "propertyNames": text(64, r"^[a-z_]+$")},
            "body": {},
        }, ["method", "path", "headers"]),
        "error": obj({"code": text(128), "message": text(1024), "request_id": text(256),
                      "retry_after_ms": integer(), "ingestion_id": identifier, "status": {"const": "UNKNOWN"},
                      "status_url": text(2048, r"^/v1/spaces/")}, ["code", "message", "request_id"]),
        "ingestion_receipt": obj({"ingestion_id": identifier, "status": {"const": "ACCEPTED"},
                                 "status_url": text(2048, r"^/v1/spaces/")}),
        "replay_receipt": obj({
            "ingestion_id": identifier, "status": enum("ACCEPTED", "COMMITTED"),
            "status_url": text(2048, r"^/v1/spaces/"), "commit_sequence": integer(1), "audit_id": sha,
        }, ["ingestion_id", "status", "audit_id"]),
        "ingestion_capacity": obj({
            "state": enum("MEASURED", "UNKNOWN"), "batches_per_second": nullable({"type": "number", "minimum": 0}),
            "samples": integer(), "sample_window_seconds": integer(), "consumer_paused": boolean,
            "observed_unix": {"type": "number"},
        }, ["state", "batches_per_second"]),
        "ingestion_queue": obj({
            "observed_at": stamp, "pending_messages": integer(), "oldest_seconds": nullable({"type": "number"}),
            "oldest_observed_at": nullable(stamp), "dlq_messages": integer(), "capacity": ref("ingestion_capacity"),
            "queue_scope": {"const": "SHARED_QUEUE_CONSERVATIVE_BOUND"},
        }),
        "ingestion_diagnostics": obj({
            "ingestion_id": identifier, "space_id": identifier,
            "native_status": enum("COMMITTED", "FAILED", "UNKNOWN"),
            "attempts": array(obj({"message_id": text(256), "error_code": text(128), "observed_at": stamp}), 32),
            "dlq_reconciliations": array(obj({
                "message_id": text(256), "ingestion_id": identifier, "envelope_sha256": sha,
                "commit_sequence": integer(1),
            }), 32),
            "queue": ref("ingestion_queue"),
            "retention": obj({
                "retain_until": stamp, "cleanup_eligible": boolean, "native_identity_must_remain": boolean,
                "reason": enum("ELIGIBLE_FOR_OPERATOR_REVIEW", "RETAIN_OR_RECONCILE"),
            }),
            "replay_window_seconds": integer(),
        }),
        "query_receipt": obj({"query_id": identifier, "status": enum(*STATES["query"], "UNKNOWN"),
                             "status_url": text(2048, r"^/v1/spaces/")}),
        "run_receipt": obj({"run_id": identifier, "pipeline_version": identifier,
                           "status": {"const": "QUEUED"}, "status_url": text(2048, r"^/v1/spaces/")}),
        "pipeline_receipt": obj({"pipeline_id": identifier, "revision": identifier, "status": {"const": "DRAFT"}}),
        "version_receipt": obj({"pipeline_id": identifier, "pipeline_version": identifier, "status": {"const": "PUBLISHED"}}),
        "pipeline_metadata": obj({
            "pipeline_id": identifier, "revision": identifier, "status": enum("ENABLED", "PAUSED", "ARCHIVED"),
            "draft_revision": identifier,
            "active_version": nullable(identifier), "schedule_owner": enum("colvenna", "external", "none"),
            "definition": ref("pipeline"), "artifacts": array(ref("artifact"), 128),
            "schedule": ref("schedule_request"),
        }, ["pipeline_id", "revision", "draft_revision", "status", "active_version", "schedule_owner"]),
        "version_metadata": obj({
            "pipeline_id": identifier, "pipeline_version": identifier, "definition": ref("pipeline"),
            "package_uri": s3, "package_object_version": text(1024), "package_sha256": sha,
            "environment_versions": mapping(ref("environment_reference"), keys="task_identifier"),
            "definition_epoch": sha, "code_hashes": mapping(sha, keys="relative_path"),
        }),
        "validation_result": obj({"valid": boolean, "diagnostics": array(
            obj({"path": text(2048), "code": text(128), "message": text(1024)}), 1000)}),
        "upload_receipt": obj({"upload_id": identifier, "upload_url": text(4096, r"^https://"),
                              "upload_headers": mapping(text(1024), keys="header_name"),
                              "expires_at": stamp}),
        "header_name": text(128, r"^[A-Za-z0-9-]+$"),
        "schedule_metadata": obj({"revision": identifier, "schedule": ref("schedule_request")}),
        "catalog": obj({"datasets": array(obj({"dataset": dataset, "schema": ref("schema")}))}),
        "dataset_metadata": obj({"dataset": dataset, "release_id": nullable(identifier),
                                 "watermark": ref("watermark"), "observed_at": stamp}),
        "layer_status": obj({
            "status": enum("ACCEPTED", "COMMITTED", "RUNNING", "PUBLISHED", "FAILED", "UNKNOWN", "NOT_IMPLEMENTED"),
            "watermark": {"anyOf": [text(256), ref("watermark")]},
            "release_id": identifier, "input_release_id": identifier,
        }, ["status"]),
        "ingestion_status": obj({
            "ingestion_id": identifier, "status": enum(*STATES["ingestion"], "UNKNOWN"),
            "projection_version": integer(), "observed_at": stamp, "accepted_at": stamp,
            "committed_at": stamp, "published_at": stamp,
            "space_id": identifier, "dataset": dataset, "commit_sequence": integer(1),
            "error_code": text(128), "reconciling": boolean,
            "authority": enum("NATIVE_LEDGER", "PROJECTION", "UNAVAILABLE"),
            "projection_state": enum("CURRENT", "REBUILT", "STORED", "RECONCILING", "UNAVAILABLE",
                                     "IGNORED_OLD", "CONFLICT"),
            "publication": obj({"status": enum("NOT_PUBLISHED", "PUBLISHED"), "release_id": identifier}, ["status"]),
            "layers": obj({name: ref("layer_status") for name in ["ods", "dwd", "dws", "ads"]}, []),
        }, ["ingestion_id", "status", "projection_version", "observed_at"]),
        "query_column": obj({"name": {"type": "string", "maxLength": 65536}, "type": text(4096),
                             "encoding": enum("native_json", "native_text")}),
        "query_data_version": obj({
            "dataset": dataset, "release_id": nullable(identifier), "generation": identifier,
            "commit_sequence": integer(), "snapshot_sequence": integer(),
        }, ["dataset", "release_id"]),
        "query_result": obj({
            "columns": array(ref("query_column"), 256),
            "rows": array(array({"type": ["string", "number", "boolean", "null", "array", "object"]}, 256), 10000),
            "data_versions": array(ref("query_data_version")), "truncated": boolean,
        }),
        "query_status": obj({
            "query_id": identifier, "status": enum(*STATES["query"], "UNKNOWN"),
            "elapsed_ms": integer(), "data_versions": array(ref("query_data_version")),
            "columns": array(ref("query_column")),
            "rows": array(array({"type": ["string", "number", "boolean", "null", "array", "object"]}, 256), 10000),
            "next_page_token": nullable(text(4096)),
            "export_url": nullable(text(4096, r"^https://")),
            "error_code": text(128), "source_commit": text(40, r"^[0-9a-f]{40}$"),
            "execution_id": identifier, "row_count": integer(), "truncated": boolean,
            "view": enum("metadata", "result", "sql"), "sql": text(65536),
        }, ["query_id", "status"]),
        "query_history": obj({"queries": array(ref("query_status"), 100)}),
        "task_status": obj({
            "task_id": identifier, "job_id": identifier, "status": enum(*STATES["task"], "UNKNOWN"),
            "attempt_id": identifier, "execution_id": identifier, "release_id": identifier,
            "imported_chunks": integer(), "total_chunks": integer(),
            "executor": enum("writer", "lambda", "ecs_task"), "attempt_number": integer(0, 10),
            "dispatch_id": text(1024), "error_code": text(128), "input_export_id": identifier,
            "processing": ref("context_processing"), "input_versions": mapping(mapping(scalar, 16), 128,
                                                                                keys="input_alias"),
            "eligibility_epoch": integer(), "observed_at": stamp,
            "attempts": array(ref("attempt_status"), 10), "progress": ref("import_progress"),
            "transient_code": text(128), "skipped": text(128),
        }, ["task_id", "job_id", "status"]),
        "attempt_status": obj({
            "attempt_id": identifier, "status": text(32, r"^[A-Z_]+$"), "created_at": nullable(stamp),
            "dispatched_at": nullable(stamp), "ended_at": nullable(stamp), "deadline_at": nullable(stamp),
            "dispatch_id": nullable(text(1024)), "execution_id": nullable(identifier),
            "eligibility_epoch": nullable(integer()), "code": nullable(text(128)),
            "usage": mapping(nullable(scalar), 16, keys="metric_name"),
        }, ["attempt_id", "status"]),
        "import_progress": obj({
            "state": text(32, r"^[A-Z_]+$"), "imported_chunks": integer(), "total_chunks": nullable(integer()),
            "observed_at": stamp, "error_code": nullable(text(128)),
        }, ["state", "observed_at"]),
        "trial_status": obj({
            "task_id": ref("task_identifier"), "sample_rows": integer(1, 10000),
            "publication": {"const": "NOT_PUBLISHED"},
            "upstream": mapping(identifier, 128, keys="task_identifier"),
            "validation": obj({"status": enum("PENDING", "PASSED", "REJECTED", "UNKNOWN"),
                               "error_code": nullable(text(128)), "row_count": nullable(integer()),
                               "files": nullable(integer()), "bytes": nullable(integer()),
                               "observed_at": nullable(stamp)}, ["status"]),
        }),
        "run_status": obj({
            "run_id": identifier, "pipeline_version": identifier, "status": enum(*STATES["run"], "UNKNOWN"),
            "observed_at": stamp, "projection_version": integer(), "tasks": array(ref("task_status"), 128),
            "trigger": enum("api", "schedule", "external"), "parameters": ref("parameters"),
            "root_inputs": mapping(mapping(scalar, 16), 128, keys="dataset"),
            "snapshot_sha256": sha, "created_at": stamp, "finished_at": stamp,
            "cancel_requested": boolean, "error_code": text(128),
            "mode": enum("snapshot", "backfill", "trial"), "partitions": array(ref("partition"), 10000),
            "trial": ref("trial_status"), "pipeline_id": identifier,
            # Why a QUEUED run has not started (pipeline run active, space quota, writer unavailable).
            "waiting": obj({"code": text(128), "since": stamp}, ["code", "since"]),
        }, ["run_id", "pipeline_version", "status", "observed_at", "projection_version", "tasks"]),
        "query_cancel": obj({"query_id": identifier, "status": enum(*STATES["query"], "UNKNOWN")}),
        "run_cancel": obj({"run_id": identifier, "status": enum(*STATES["run"], "UNKNOWN")}),
        "key_metadata": obj({
            "key_id": identifier, "name": text(256), "fingerprint": text(64),
            "scopes": array({"enum": SCOPES}, len(SCOPES), 1, True), "created_at": stamp,
            "expires_at": stamp, "status": enum("ACTIVE", "REVOKED", "EXPIRED"), "last_used_at": nullable(stamp),
            "replacement_key_id": identifier, "replaces_key_id": identifier,
            "creation_request_id": text(256),
        }, ["key_id", "name", "scopes", "expires_at", "status"]),
        "key_receipt": obj({"key": ref("key_metadata"), "token": text(512)}),
        "key_list": obj({"keys": array(ref("key_metadata"), 1000)}),
        "build_receipt": obj({"build_id": identifier, "status": {"const": "QUEUED"}}),
        "environment_artifact": obj({
            "s3_uri": s3, "object_version": text(1024), "sha256": sha,
            "bytes": integer(1, 262144),
        }),
        "task_object": obj({
            "s3_uri": s3, "object_version": text(1024), "sha256": sha,
            "bytes": integer(1, 1073741824),
        }),
        "task_event": obj({
            "operation": {"const": "task_execute"}, "descriptor": ref("task_object"),
        }),
        "task_environment": {"oneOf": [
            obj({
                "reference": ref("environment_reference"), "specification_sha256": sha,
                "source_commit": text(40, r"^[0-9a-f]{40}$"), "adapter_sha256": sha,
                "dependency_lock_sha256": sha, "executor_binding": ref("executor_binding"),
            }),
            # Writer SQL uses the platform writer image named by the version snapshot.
            obj({"executor_binding": obj({
                "executor": {"const": "writer"}, "profile": {"const": "writer_short"},
                "source_commit": text(40, r"^[0-9a-f]{40}$")})}),
        ]},
        "task_descriptor": obj({
            "contract_version": {"const": 2}, "space_id": identifier, "generation": identifier,
            "pipeline_id": identifier, "pipeline_version": identifier, "definition_epoch": sha,
            "run_id": identifier, "job_id": identifier, "attempt_id": identifier,
            "task": ref("task"), "environment": ref("task_environment"),
            "package": ref("task_object"), "snapshot_sha256": sha,
            "parameters": ref("parameters"),
            "inputs": mapping(ref("context_input"), 128, 1, keys="input_alias"),
            "processing": ref("context_processing"),
            "output_state": obj({
                "expected_previous_release": nullable(identifier),
                "replace_partitions": array(ref("partition")),
            }),
        }),
        "environment_runtime": obj({
            "python_version": text(32, r"^3\.12\.[0-9]+$"), "python_abi": {"const": "cp312"},
            "platform": {"const": "linux_arm64"}, "base_image": text(512, r"^.*@sha256:[0-9a-f]{64}$"),
            "adapter_sha256": sha, "source_commit": text(40, r"^[0-9a-f]{40}$"),
        }),
        "executor_binding": {
            "oneOf": [
                obj({"executor": {"const": "lambda"}, "profile": text(128), "lambda_mode": {"const": "standard"},
                     "function_arn": text(512, r"^arn:aws:lambda:[a-z]{2}(?:-[a-z]+)+-[1-9][0-9]*:[0-9]{12}:function:[A-Za-z0-9_-]+:[1-9][0-9]*$")}),
                obj({"executor": {"const": "ecs_task"}, "profile": text(128),
                     "task_definition_arn": text(512, r"^arn:aws:ecs:[a-z]{2}(?:-[a-z]+)+-[1-9][0-9]*:[0-9]{12}:task-definition/[A-Za-z0-9_-]+:[1-9][0-9]*$"),
                     "image": text(512, r"^[0-9]{12}\.dkr\.ecr\.[a-z]{2}(?:-[a-z]+)+-[1-9][0-9]*\.amazonaws\.com/[A-Za-z0-9_/-]+@sha256:[0-9a-f]{64}$")}),
            ]
        },
        "environment_metadata": obj({
            "environment_id": identifier, "version": identifier, "status": enum(*STATES["environment"], "UNKNOWN"),
            "dependency_lock_sha256": nullable(sha), "image_digest": nullable(text(71, r"^sha256:[0-9a-f]{64}$")),
            "executor_bindings": array(ref("executor_binding"), 128),
            "build_id": identifier, "codebuild_id": text(512),
            "dependency_lock": ref("environment_artifact"), "provenance": ref("environment_artifact"),
            "runtime": ref("environment_runtime"),
            "failure": obj({"phase": nullable(text(64)), "aws_status": nullable(text(64)),
                            "code": text(128), "message": nullable(text(1024))}, ["code"]),
            "dependency_source": obj({"format": {"const": "requirements"}, "content": text(65536)}),
            "created_at": stamp,
        }, ["environment_id", "version", "status"]),
        "environment_receipt": obj({"environment_id": identifier, "version": identifier, "status": {"const": "DRAFT"}}),
        "metric_name": text(128, r"^[a-z][a-z0-9_.]*$"),
        "layer_name": enum("ods", "dwd", "dws", "ads"),
        # Missing, stale or unavailable sources are UNKNOWN; STOPPED is a confirmed stop.
        "observation": obj({
            "status": enum("OK", "DEGRADED", "FAILED", "STOPPED", "UNKNOWN"), "observed_at": nullable(stamp),
            "age_seconds": nullable({"type": "number", "minimum": 0}), "source": text(128), "reason": text(256),
            "metrics": mapping(nullable(scalar), 64, keys="metric_name"),
        }, ["status", "observed_at", "source", "metrics"]),
        "dataset_observation": obj({
            "dataset": dataset, "layer": ref("layer_name"), "write_owner": enum("direct", "ingestion", "pipeline"),
            "status": enum("OK", "NOT_PUBLISHED", "UNKNOWN"), "commit_sequence": nullable(integer()),
            "release_id": nullable(identifier), "published_at": nullable(stamp),
            "snapshot_sequence": nullable(integer()), "observed_at": nullable(stamp),
        }, ["dataset", "layer", "write_owner", "status", "observed_at"]),
        "admin_status": obj({
            "space_id": identifier, "generation": identifier, "observed_at": stamp, "writer": ref("observation"),
            "queue": ref("observation"), "dlq": ref("observation"),
            "layers": mapping(ref("observation"), 4, keys="layer_name"),
            "datasets": array(ref("dataset_observation"), 64),
            "jobs": ref("observation"), "queries": ref("observation"), "scheduler": ref("observation"),
        }),
        "audit_actor": obj({"kind": enum("API_KEY", "IAM_OPERATOR", "SYSTEM"), "principal_id": identifier,
                            "key_id": nullable(identifier), "fingerprint": nullable(text(64))}),
        "audit_target": obj({"kind": text(64, r"^[a-z_]+$"), "id": text(512), "pipeline_id": identifier},
                            ["kind", "id"]),
        "audit_record": obj({
            "audit_id": identifier, "space_id": identifier, "generation": identifier, "at": stamp,
            "action": {"enum": AUDIT_ACTIONS}, "actor": ref("audit_actor"), "target": ref("audit_target"),
            "request_id": nullable(text(256)), "outcome": enum("COMMITTED", "ACCEPTED", "INTENT"),
            "details": mapping(nullable(scalar), 32, keys="metric_name"),
        }),
        "audit_list": obj({"records": array(ref("audit_record"), 200), "next_page_token": nullable(text(2048))}),
        "pipeline_summary": obj({
            "pipeline_id": identifier, "revision": identifier, "draft_revision": identifier,
            "status": enum("ENABLED", "PAUSED", "ARCHIVED"), "active_version": nullable(identifier),
            "schedule": ref("schedule_request"), "name": text(256), "tasks": integer(1, 128),
        }),
        "pipeline_list": obj({"pipelines": array(ref("pipeline_summary"), 512)}),
        "version_summary": obj({
            "pipeline_version": identifier, "draft_revision": identifier, "active": boolean,
            "created_by": nullable(identifier), "created_at": nullable(stamp),
            "code_hashes": mapping(sha, keys="relative_path"),
        }),
        "version_list": obj({"versions": array(ref("version_summary"), 2000)}),
        "run_summary": obj({
            "run_id": identifier, "pipeline_version": identifier, "status": enum(*STATES["run"], "UNKNOWN"),
            "trigger": enum("api", "schedule", "external"), "mode": enum("snapshot", "backfill", "trial"),
            "created_at": stamp, "finished_at": nullable(stamp), "error_code": nullable(text(128)),
            "task_id": nullable(ref("task_identifier")),
        }),
        "run_list": obj({"runs": array(ref("run_summary"), 100), "next_page_token": nullable(text(2048))}),
        "artifact_content": obj({
            "path": relative, "upload_id": identifier, "sha256": sha, "bytes": integer(1, 10485760),
            "encoding": enum("utf-8", "unavailable"), "content": nullable({"type": "string", "maxLength": 786432}),
        }),
        "artifact_inline_request": obj({"path": relative, "sha256": sha,
                                        "content": {"type": "string", "minLength": 1, "maxLength": 786432}}),
        "artifact_inline_receipt": obj({"upload_id": identifier, "path": relative, "sha256": sha,
                                        "bytes": integer(1, 10485760)}),
        "trial_request": obj({
            "pipeline_version": identifier, "task_id": ref("task_identifier"), "parameters": ref("parameters"),
            "sample_rows": integer(1, 10000),
        }, ["pipeline_version", "task_id", "sample_rows"]),
        "run_preview": obj({
            "run_id": identifier, "task_id": ref("task_identifier"), "publication": {"const": "NOT_PUBLISHED"},
            "columns": array(ref("query_column"), 256),
            "rows": array(array({"type": ["string", "number", "boolean", "null"]}, 256), 100),
            "row_count": integer(), "truncated": boolean, "observed_at": stamp,
        }),
        "environment_summary": obj({
            "environment_id": identifier, "version": identifier, "status": enum(*STATES["environment"], "UNKNOWN"),
            "build_id": nullable(identifier), "created_at": nullable(stamp), "created_by": nullable(identifier),
        }),
        "environment_list": obj({"environments": array(ref("environment_summary"), 64)}),
        "trace_link": obj({"kind": text(64, r"^[a-z_]+$"), "id": text(1024)}),
        "trace_node": obj({
            "kind": text(64, r"^[a-z_]+$"), "id": text(1024), "status": text(64),
            "authority": enum("NATIVE_LEDGER", "CONTROL_RECORD", "S3_OBJECT", "ECS", "LAMBDA", "CODEBUILD",
                              "QUERY_RECORD", "UNAVAILABLE"),
            "observed_at": nullable(stamp), "attributes": mapping(nullable(scalar), 64, keys="metric_name"),
            "links": array(ref("trace_link"), 128),
            "conflict": obj({"projection": nullable(text(128)), "authority": nullable(text(128))}),
        }, ["kind", "id", "status", "authority", "observed_at", "attributes", "links"]),
        "trace": obj({
            "space_id": identifier, "kind": enum("ingestion", "run", "query", "release"), "id": identifier,
            "observed_at": stamp, "status": enum("COMPLETE", "PARTIAL"), "nodes": array(ref("trace_node"), 512),
        }),
        "metric_series": obj({
            "name": ref("metric_name"), "value": nullable({"type": "number"}), "unit": text(32),
            "status": enum("OK", "UNKNOWN"), "observed_at": nullable(stamp), "window_seconds": integer(),
            "samples": integer(), "source": text(128), "reason": text(256),
        }, ["name", "value", "unit", "status", "observed_at", "window_seconds", "samples", "source"]),
        "metrics": obj({"space_id": identifier, "observed_at": stamp, "window_seconds": integer(60, 86400),
                        "series": array(ref("metric_series"), 128)}),
        "retention_policy": obj({name: integer(bounds[1] if name == "release_keep" else 60, bounds[2])
                                 for name, bounds in RETENTION_POLICY.items()}, []),
        "retention_request": obj({"state": enum("ENABLED", "DISABLED"), "policy": ref("retention_policy")},
                                 ["state", "policy"]),
        "retention_target": obj({
            "kind": enum("release", "pipeline_version", "run", "ingestion", "environment"), "id": identifier,
            "pipeline_id": identifier, "dataset": dataset,
        }, ["kind", "id"]),
        "hold_request": obj({"target": ref("retention_target"), "reason": text(256), "expires_at": stamp}),
        "retention_hold": obj({
            "hold_id": identifier, "target": ref("retention_target"), "reason": text(256), "created_at": stamp,
            "expires_at": stamp, "status": enum("ACTIVE", "RELEASED", "EXPIRED"), "created_by": identifier,
        }),
        "retention_category": obj({"category": text(64, r"^[a-z_]+$"), "rule": text(512),
                                   "collector": enum("RETENTION_COLLECTOR", "QUERY_SERVICE", "NATIVE_PROTOCOL",
                                                     "GENERATION_PURGE")}),
        "collection_summary": obj({
            "collection_id": identifier, "status": enum("QUEUED", "RUNNING", "COMPLETED", "FAILED", "UNKNOWN"),
            "dry_run": boolean, "requested_at": stamp, "finished_at": nullable(stamp),
            "collected_objects": integer(), "collected_bytes": integer(), "error_code": nullable(text(128)),
        }),
        "retention_status": obj({
            "space_id": identifier, "generation": identifier, "observed_at": stamp, "revision": identifier,
            "state": enum("ENABLED", "DISABLED"), "policy": ref("retention_policy"),
            "probe_minimum": boolean, "categories": array(ref("retention_category"), 16),
            "holds": array(ref("retention_hold"), 256), "collections": array(ref("collection_summary"), 20),
        }),
        "collection_request": obj({"dry_run": boolean}),
        "collection_receipt": obj({"collection_id": identifier, "status": {"const": "QUEUED"},
                                   "status_url": text(2048, r"^/v1/admin/spaces/")}),
        "collection_object": obj({
            "category": text(64, r"^[a-z_]+$"), "key": text(1024), "version_id": nullable(text(1024)),
            "bytes": nullable(integer()), "action": enum("COLLECTED", "RETAINED", "WOULD_COLLECT", "ABSENT"),
            "reason": text(256),
        }),
        "collection_report": obj({
            **{name: value for name, value in {
                "collection_id": identifier, "space_id": identifier, "generation": identifier,
                "status": enum("QUEUED", "RUNNING", "COMPLETED", "FAILED", "UNKNOWN"), "dry_run": boolean,
                "requested_at": stamp, "started_at": nullable(stamp), "finished_at": nullable(stamp),
                "error_code": nullable(text(128)),
                "summary": mapping(mapping(integer(), 8, keys="metric_name"), 16, keys="metric_name"),
                "receipt": nullable(ref("environment_artifact_large")),
                "objects": array(ref("collection_object"), 2000), "truncated": boolean,
                # The failing dependency call of a FAILED pass (AWS operation and error code).
                "failure": nullable(obj({"operation": text(64), "code": nullable(text(128))}, ["operation", "code"])),
            }.items()},
        }),
        "environment_artifact_large": obj({"s3_uri": s3, "object_version": text(1024), "sha256": sha,
                                           "bytes": integer(1, 67108864)}),
        "quota_policy": obj({name: integer(bounds[1], bounds[2]) for name, bounds in QUOTA_POLICY.items()}
                            | {"allowed_profiles": array({"enum": list(PROFILES)}, len(PROFILES), 1, True)}, []),
        "quota_usage": obj({
            "name": ref("metric_name"), "limit": nullable({"type": "number"}), "used": nullable({"type": "number"}),
            "unit": text(32), "status": enum("OK", "EXHAUSTED", "UNKNOWN"), "observed_at": nullable(stamp),
            "enforcement": enum("REJECT", "WAIT", "BACKPRESSURE"), "source": text(128), "code": text(128),
        }, ["name", "limit", "used", "unit", "status", "observed_at", "enforcement", "source"]),
        "quota_status": obj({"space_id": identifier, "generation": identifier, "observed_at": stamp,
                             "policy": ref("quota_policy"), "usage": array(ref("quota_usage"), 128)}),
        "cost_category": {"enum": COST_CATEGORIES},
        "cost_line": obj({
            "category": ref("cost_category"), "workload": ref("trace_link"), "usage": {"type": "number", "minimum": 0},
            "unit": text(32), "unit_price_usd": {"type": "number", "minimum": 0},
            "estimated_usd": {"type": "number", "minimum": 0}, "observed_at": nullable(stamp), "source": text(128),
            "attributes": mapping(nullable(scalar), 32, keys="metric_name"),
        }),
        "cost_report": obj({
            "space_id": identifier, "generation": identifier, "since": stamp, "until": stamp, "observed_at": stamp,
            "basis": {"const": "MEASURED_USAGE_X_LIST_PRICE"},
            "prices": obj({"region": text(64, r"^[a-z]{2}(?:-[a-z]+)+-[1-9][0-9]*$"), "currency": {"const": "USD"},
                           "retrieved": nullable(text(10, r"^\d{4}-\d\d-\d\d$")), "sources": array(text(512), 16)}),
            "lines": array(ref("cost_line"), 2000), "truncated": boolean,
            "totals": mapping({"type": "number", "minimum": 0}, 8, keys="cost_category"),
            "excluded": array(text(256), 32), "status": enum("COMPLETE", "PARTIAL", "UNKNOWN"),
            # Workloads whose usage could not be measured; PARTIAL exactly when any are listed.
            "unmeasured": array(obj({"kind": text(64), "id": text(1024), "reason": text(128)},
                                    ["kind", "id", "reason"]), 200),
        }),
    }
    d["write_request"]["allOf"] = [
        {"if": {"properties": {"operation": {"const": operation}}},
         "then": {"required": [field], "not": {"anyOf": [
             {"required": [other]} for other in ["records", "updates", "keys"] if other != field]}}}
        for operation, field in [("insert", "records"), ("upsert", "records"), ("update", "updates"), ("delete", "keys")]
    ]
    d["write_receipt"]["allOf"] = [{
        "if": {"properties": {"status": {"const": "COMMITTED"}}},
        "then": {"required": ["commit_sequence", "affected_rows", "committed_at", "dataset_watermark"],
                 "not": {"required": ["rejected_at"]}},
        "else": {"required": ["code", "rejected_at"], "not": {"anyOf": [
            {"required": ["commit_sequence"]}, {"required": ["committed_at"]},
            {"required": ["affected_rows"]}, {"required": ["dataset_watermark"]}]}},
    }]
    return {"$schema": "https://json-schema.org/draft/2020-12/schema",
            "$id": "https://colvenna-isle.invalid/contracts/v2/contracts.schema.json", "$defs": d}


# Query parameters by response definition; every other route accepts none.
QUERY_PARAMETERS = {
    "query_status": {"page_token": text(4096), "page_size": integer(1, 10000),
                     "view": enum("metadata", "result", "sql")},
    "query_result": {"token": text(4096)},
    "artifact_content": {"path": text(512)},
    "run_list": {"page_token": text(2048), "limit": integer(1, 100)},
    "audit_list": {"page_token": text(2048), "limit": integer(1, 200), "action": {"enum": AUDIT_ACTIONS}},
    "trace": {"kind": enum("ingestion", "run", "query", "release"), "id": text(128),
              "pipeline_id": text(128)},
    "metrics": {"window_seconds": integer(60, 86400)},
    "cost_report": {"since": text(40), "until": text(40)},
    "run_preview": {"rows": integer(1, 100)},
}
# Additional scopes accepted by a route (any one), beyond its required scope.
ALTERNATE_SCOPES = {
    ("GET", "ingestion_status"): ["monitor:read"], ("GET", "run_status"): ["monitor:read", "pipeline:manage"],
    ("GET", "environment_metadata"): ["monitor:read"], ("GET", "pipeline_list"): ["pipeline:run", "monitor:read"],
    ("GET", "version_list"): ["pipeline:run"], ("GET", "run_list"): ["pipeline:manage", "monitor:read"],
    ("GET", "environment_list"): ["monitor:read"], ("GET", "retention_status"): ["monitor:read"],
    ("GET", "collection_report"): ["monitor:read"],
}

# method, path, request definition, response definition, HTTP success, required scope, conditional header
ROUTES = [
    ("GET", "/admin/spaces/{space_id}/configuration", None, "space_configuration", 200, "datasets:manage", None),
    ("POST", "/admin/spaces/{space_id}/datasets", "dataset_registration", "registered_dataset", 201, "datasets:manage", "If-Match"),
    ("POST", "/spaces/{space_id}/ingestions", "ingestion_request", "ingestion_receipt", 202, "ingest:write", "Idempotency-Key"),
    ("GET", "/spaces/{space_id}/ingestions/{ingestion_id}", None, "ingestion_status", 200, "ingest:write", None),
    ("GET", "/spaces/{space_id}/ingestions/{ingestion_id}/diagnostics", None, "ingestion_diagnostics", 200, "monitor:read", None),
    ("POST", "/spaces/{space_id}/ingestions/{ingestion_id}/replay", "empty_request", "replay_receipt", 202, "ingest:replay", None),
    ("POST", "/spaces/{space_id}/writes", "write_request", "write_receipt", 200, "data:write", "Idempotency-Key"),
    ("GET", "/spaces/{space_id}/writes/{write_id}", None, "write_receipt", 200, "data:write", None),
    ("GET", "/spaces/{space_id}/catalog", None, "catalog", 200, "query:read", None),
    ("GET", "/spaces/{space_id}/datasets/{dataset_id}", None, "dataset_metadata", 200, "query:read", None),
    ("POST", "/spaces/{space_id}/queries", "query_request", "query_receipt", 202, "query:read", None),
    ("GET", "/spaces/{space_id}/queries", None, "query_history", 200, "query:read", None),
    ("GET", "/spaces/{space_id}/queries/{query_id}", None, "query_status", 200, "query:read", None),
    ("GET", "/spaces/{space_id}/queries/{query_id}/export", None, "query_result", 200, "query:read", None),
    ("POST", "/spaces/{space_id}/queries/{query_id}/cancel", "empty_request", "query_cancel", 200, "query:read", None),
    ("POST", "/spaces/{space_id}/pipelines", "draft_request", "pipeline_receipt", 201, "pipeline:manage", None),
    ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}", None, "pipeline_metadata", 200, "pipeline:manage", None),
    ("PUT", "/spaces/{space_id}/pipelines/{pipeline_id}/draft", "draft_request", "pipeline_receipt", 200, "pipeline:manage", "If-Match"),
    ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/artifact-uploads", "upload_request", "upload_receipt", 201, "pipeline:manage", None),
    ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/validate", "validate_request", "validation_result", 200, "pipeline:manage", None),
    ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/versions", "version_request", "version_receipt", 201, "pipeline:manage", "Idempotency-Key"),
    ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}/versions/{pipeline_version}", None, "version_metadata", 200, "pipeline:manage", None),
    ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/activate", "activate_request", "pipeline_metadata", 200, "pipeline:manage", "If-Match"),
    ("PUT", "/spaces/{space_id}/pipelines/{pipeline_id}/schedule", "schedule_request", "schedule_metadata", 200, "pipeline:manage", "If-Match"),
    ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/runs", "run_request", "run_receipt", 202, "pipeline:run", "Idempotency-Key"),
    ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}", None, "run_status", 200, "pipeline:run", None),
    ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}/cancel", "empty_request", "run_cancel", 200, "pipeline:run", None),
    ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}/retry", "empty_request", "run_receipt", 202, "pipeline:run", "Idempotency-Key"),
    ("POST", "/spaces/{space_id}/runtime-environments", "environment_request", "environment_receipt", 201, "runtime:manage", None),
    ("POST", "/spaces/{space_id}/runtime-environments/{environment_id}/versions/{version}/build", "empty_request", "build_receipt", 202, "runtime:manage", "Idempotency-Key"),
    ("GET", "/spaces/{space_id}/runtime-environments/{environment_id}/versions/{version}", None, "environment_metadata", 200, "runtime:manage", None),
    ("GET", "/spaces/{space_id}/pipelines", None, "pipeline_list", 200, "pipeline:manage", None),
    ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}/versions", None, "version_list", 200, "pipeline:manage", None),
    ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}/draft/artifacts", None, "artifact_content", 200, "pipeline:manage", None),
    ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/artifacts", "artifact_inline_request", "artifact_inline_receipt", 201, "pipeline:manage", None),
    ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}/runs", None, "run_list", 200, "pipeline:run", None),
    ("POST", "/spaces/{space_id}/pipelines/{pipeline_id}/trial-runs", "trial_request", "run_receipt", 202, "pipeline:run", "Idempotency-Key"),
    ("GET", "/spaces/{space_id}/pipelines/{pipeline_id}/runs/{run_id}/preview", None, "run_preview", 200, "query:read", None),
    ("GET", "/spaces/{space_id}/runtime-environments", None, "environment_list", 200, "runtime:manage", None),
    ("GET", "/admin/spaces/{space_id}/status", None, "admin_status", 200, "monitor:read", None),
    ("GET", "/admin/spaces/{space_id}/metrics", None, "metrics", 200, "monitor:read", None),
    ("GET", "/admin/spaces/{space_id}/trace", None, "trace", 200, "monitor:read", None),
    ("GET", "/admin/spaces/{space_id}/audit", None, "audit_list", 200, "audit:read", None),
    ("GET", "/admin/spaces/{space_id}/quotas", None, "quota_status", 200, "monitor:read", None),
    ("GET", "/admin/spaces/{space_id}/costs", None, "cost_report", 200, "monitor:read", None),
    ("GET", "/admin/spaces/{space_id}/retention", None, "retention_status", 200, "retention:manage", None),
    ("PUT", "/admin/spaces/{space_id}/retention", "retention_request", "retention_status", 200, "retention:manage", "If-Match"),
    ("POST", "/admin/spaces/{space_id}/retention/holds", "hold_request", "retention_hold", 201, "retention:manage", None),
    ("POST", "/admin/spaces/{space_id}/retention/holds/{hold_id}/release", "empty_request", "retention_hold", 200, "retention:manage", None),
    ("POST", "/admin/spaces/{space_id}/retention/collections", "collection_request", "collection_receipt", 202, "retention:manage", "Idempotency-Key"),
    ("GET", "/admin/spaces/{space_id}/retention/collections/{collection_id}", None, "collection_report", 200, "retention:manage", None),
    ("GET", "/admin/spaces/{space_id}/api-keys", None, "key_list", 200, "keys:manage", None),
    ("POST", "/admin/spaces/{space_id}/api-keys", "key_request", "key_receipt", 201, "keys:manage", None),
    ("POST", "/admin/spaces/{space_id}/api-keys/{key_id}/revoke", "empty_request", "key_metadata", 200, "keys:manage", None),
    ("POST", "/admin/spaces/{space_id}/api-keys/{key_id}/rotate", "key_request", "key_receipt", 201, "keys:manage", None),
]


def openapi():
    import re
    paths = {}
    for method, suffix, request, response, status, scope, conditional in ROUTES:
        path = "/v1" + suffix
        operation = {
            "operationId": method.lower()+"_"+re.sub(r"\W+", "_", suffix).strip("_"),
            "security": [{"BearerApiKey": []}], "x-required-scope": scope,
            "x-allowed-scopes": [scope],
            "x-owner-or-authorized-admin": any(name in path for name in [
                "{ingestion_id}", "{write_id}", "{query_id}", "{run_id}"]),
            "parameters": [
                {"name": name, "in": "path", "required": True,
                 "schema": text(128, r"^[A-Za-z_][A-Za-z0-9_.-]*$")}
                for name in re.findall(r"\{([^}]+)\}", path)
            ],
            "responses": {
                str(status): {"description": "Success",
                              "content": {"application/json": {"schema": {"$ref": f"contracts.schema.json#/$defs/{response}"}}}},
                "default": {"description": "Authenticated/authorized route errors; 401/403 reject access",
                            "content": {"application/json": {"schema": {"$ref": "contracts.schema.json#/$defs/error"}}}},
            },
        }
        operation["x-allowed-scopes"].extend(ALTERNATE_SCOPES.get((method, response), []))
        if response == "write_receipt":
            if method == "GET":
                operation["x-allowed-scopes"].append("datasets:manage")
            for code in ["503", "504"]:
                operation["responses"][code] = {
                    "description": "Original result UNKNOWN; reconcile this ID; timeout does not promise rollback",
                    "content": {"application/json": {"schema": {
                        "oneOf": [{"$ref": "contracts.schema.json#/$defs/write_unknown"},
                                  {"$ref": "contracts.schema.json#/$defs/error"}]}}}}
            if method == "POST":
                operation["responses"]["429"] = {
                    "description": "Confirmed not executed; no asynchronous fallback",
                    "content": {"application/json": {"schema": {
                        "$ref": "contracts.schema.json#/$defs/write_not_submitted"}}}}
                operation["responses"]["422"] = {
                    "description": "Invalid request or original native constraint rejection; batch rolled back",
                    "content": {"application/json": {"schema": {
                        "oneOf": [{"$ref": "contracts.schema.json#/$defs/write_receipt"},
                                  {"$ref": "contracts.schema.json#/$defs/error"}]}}}}
        if conditional:
            operation["parameters"].append({"name": conditional, "in": "header", "required": True,
                                            "schema": text(38, r'^"gen_[0-9a-f]{32}"$')
                                            if request == "dataset_registration" else text(130, r'^"[A-Za-z_][A-Za-z0-9_.-]*"$')
                                            if conditional == "If-Match" else text(128, r"^[A-Za-z0-9_.:-]+$")})
        for name, schema in QUERY_PARAMETERS.get(response if method == "GET" else None, {}).items():
            operation["parameters"].append({
                "name": name, "in": "query", "schema": schema,
                "required": name == "token" or response == "trace" and name in {"kind", "id"}
                or response == "artifact_content"})
        if response == "run_preview":
            # Unpublished trial rows are data: the run reader also needs query:read.
            operation["x-required-scopes"] = ["pipeline:run", "query:read"]
        if response == "replay_receipt":
            operation["x-required-scopes"] = ["ingest:write", "ingest:replay"]
            operation["responses"]["200"] = {
                "description": "Original native commit reconciled; no queue dispatch",
                "content": {"application/json": {"schema": {
                    "$ref": "contracts.schema.json#/$defs/replay_receipt"}}}}
        if request:
            operation["requestBody"] = {"required": request != "empty_request", "content": {"application/json": {
                "schema": {"$ref": f"contracts.schema.json#/$defs/{request}"}}}}
        paths.setdefault(path, {})[method.lower()] = operation
    return {
        "openapi": "3.1.0", "info": {"title": "Colvenna Isle current contract", "version": "2"},
        "security": [{"BearerApiKey": []}], "paths": paths,
        "components": {"securitySchemes": {"BearerApiKey": {"type": "http", "scheme": "bearer",
                                                           "bearerFormat": "opaque API Key"}}},
    }


def artifacts():
    return {
        "contracts.schema.json": schemas(), "openapi.json": openapi(),
        "profiles.json": {"contract_version": 2, "limits": LIMITS, "profiles": PROFILES,
                          "status": "ADMISSION_LIMITS_NOT_PERFORMANCE_PROMISES"},
        "states.json": {"contract_version": 2, "transitions": STATES,
                        "unknown_rule": "Observation only; authoritative reconciliation required before leaving UNKNOWN",
                        "terminal_rule": "PUBLISHED/SUCCEEDED/CANCELLED remain terminal; retries use new attempts and fixed bindings"},
    }
