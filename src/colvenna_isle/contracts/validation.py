"""Offline structural and semantic validation of the current product contract."""

import base64
import json
import math
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path, PurePosixPath
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from jsonschema import Draft202012Validator, FormatChecker

from .model import KEY_STATE_METADATA, LIMITS, PROFILES, QUERY_PARAMETERS, ROUTES, STATES, schemas

SCHEMA = schemas()
FORMATS = FormatChecker()


class ContractError(ValueError):
    """A deterministic rejection; the message contains paths, not payload values."""


def require(condition, message):
    if not condition:
        raise ContractError(message)


def load_document(path):
    """Reject duplicate keys, nonfinite JSON and unsafe YAML constructors."""
    path = Path(path)
    require(path.stat().st_size <= 67108864, "Contract document exceeds 64 MiB metadata quota")
    content = path.read_text()
    if path.suffix in {".yaml", ".yml"}:
        import yaml

        class UniqueLoader(yaml.SafeLoader):
            def compose_node(self, parent, index):
                if self.check_event(yaml.AliasEvent):
                    raise ContractError("YAML aliases are outside the current contract")
                return super().compose_node(parent, index)

        def construct(loader, node, deep=False):
            result = {}
            for key_node, value_node in node.value:
                key = loader.construct_object(key_node, deep=deep)
                require(isinstance(key, str) and key not in result, "Duplicate or non-string YAML key")
                result[key] = loader.construct_object(value_node, deep=deep)
            return result

        UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct)
        try:
            result = yaml.load(content, Loader=UniqueLoader)
        except (yaml.YAMLError, RecursionError) as error:
            raise ContractError("Invalid YAML") from error
    else:
        def pairs(values):
            result = {}
            for key, value in values:
                require(key not in result, "Duplicate JSON key")
                result[key] = value
            return result

        def reject_constant(_):
            raise ContractError("Nonfinite JSON number")

        try:
            result = json.loads(content, object_pairs_hook=pairs, parse_constant=reject_constant)
        except RecursionError as error:
            raise ContractError("Contract document nesting exceeded") from error
    finite_json(result)
    return result


def finite_json(value, depth=0):
    require(depth <= 64, "JSON nesting exceeds 64 levels")
    if isinstance(value, float):
        require(math.isfinite(value), "Nonfinite number")
    elif isinstance(value, dict):
        require(all(isinstance(key, str) for key in value), "Non-string object key")
        for item in value.values():
            finite_json(item, depth+1)
    elif isinstance(value, list):
        for item in value:
            finite_json(item, depth+1)
    else:
        require(value is None or type(value) in {str, int, bool}, "Non-JSON value")


def decimal_value(value, precision=None, scale=None):
    require(isinstance(value, str) and re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?", value),
            "Decimal requires a canonical finite string without exponent or float conversion")
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise ContractError("Invalid Decimal") from error
    require(result.is_finite(), "Nonfinite Decimal")
    if precision is not None:
        whole, _, fractional = value.lstrip("-").partition(".")
        require(len(fractional) <= scale and len(whole.lstrip("0")) <= precision-scale,
                "Decimal precision/scale overflow")
    return result


def parse_type(value):
    require(isinstance(value, str), "Type name is required")
    match = re.fullmatch(r"DECIMAL\((\d+),(\d+)\)", value)
    if match:
        precision, scale = map(int, match.groups())
        require(1 <= precision <= 38 and 0 <= scale <= precision, "Invalid Decimal precision/scale")
        return "DECIMAL", precision, scale
    require(value in {"BIGINT", "BOOLEAN", "VARCHAR", "VARCHAR[]", "DATE", "TIMESTAMP",
                      "TIMESTAMPTZ", "JSON", "BLOB"}, "Unsupported type")
    return value, None, None


def timestamp_value(value, with_timezone):
    pattern = (r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?"
               + (r"(?:Z|[+-]\d\d:\d\d)" if with_timezone else ""))
    require(isinstance(value, str) and re.fullmatch(pattern, value), "Invalid timestamp shape/timezone")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ContractError("Invalid timestamp calendar/time") from error
    if with_timezone:
        return result.astimezone(timezone.utc)
    return result


@FORMATS.checks("colvenna-timestamp", raises=ContractError)
def timestamp_format(value):
    if not isinstance(value, str):
        return True  # The structural type validator handles this.
    timestamp_value(value, True)
    return True


def validate_value(kind, value, nullable=False):
    if value is None:
        require(nullable, "Null in non-nullable value")
        return None
    name, precision, scale = parse_type(kind)
    if name == "BIGINT":
        if type(value) is int:
            require(abs(value) <= 9007199254740991, "Large JSON BIGINT must use a decimal string")
            result = value
        else:
            require(isinstance(value, str) and re.fullmatch(r"-?(?:0|[1-9][0-9]*)", value), "Invalid BIGINT")
            result = int(value)
        require(-(2**63) <= result < 2**63, "BIGINT overflow")
        return result
    if name == "DECIMAL":
        return decimal_value(value, precision, scale)
    if name == "BOOLEAN":
        require(type(value) is bool, "BOOLEAN requires a boolean")
    elif name == "VARCHAR":
        require(isinstance(value, str), "VARCHAR requires a string")
    elif name == "VARCHAR[]":
        require(isinstance(value, list) and all(isinstance(item, str) for item in value), "Invalid VARCHAR[]")
    elif name == "DATE":
        require(isinstance(value, str) and re.fullmatch(r"\d{4}-\d\d-\d\d", value), "Invalid DATE shape")
        try:
            return date.fromisoformat(value)
        except ValueError as error:
            raise ContractError("Invalid DATE calendar") from error
    elif name in {"TIMESTAMP", "TIMESTAMPTZ"}:
        return timestamp_value(value, name == "TIMESTAMPTZ")
    elif name == "JSON":
        finite_json(value)
    elif name == "BLOB":
        require(isinstance(value, str), "BLOB requires base64 string")
        try:
            decoded = base64.b64decode(value, validate=True)
        except ValueError as error:
            raise ContractError("Invalid BLOB base64") from error
        require(base64.b64encode(decoded).decode() == value, "Noncanonical BLOB base64")
        return decoded
    return value


def validate_schema(columns):
    names = [column["name"] for column in columns]
    require(len(names) == len(set(names)), "Duplicate schema column")
    for column in columns:
        parse_type(column["type"])


def safe_relative(value):
    path = PurePosixPath(value)
    require(not path.is_absolute() and value == path.as_posix()
            and all(part not in {"", ".", ".."} for part in value.split("/"))
            and "\\" not in value, "Noncanonical or escaping relative path")


def safe_absolute(value):
    path = PurePosixPath(value)
    require(path.is_absolute() and value == path.as_posix()
            and all(part not in {".", ".."} for part in value.split("/")),
            "Noncanonical or escaping absolute path")


def validate_schedule(schedule):
    if "timezone" in schedule:
        try:
            ZoneInfo(schedule["timezone"])
        except (ZoneInfoNotFoundError, ValueError) as error:
            raise ContractError("Unknown schedule timezone") from error
    if schedule["owner"] == "colvenna":
        require("expression" in schedule and "timezone" in schedule, "Colvenna Isle schedule requires expression/timezone")
    if "expression" in schedule:
        expression = schedule["expression"]
        rate = re.fullmatch(r"rate\(([1-9]\d*) (minute|minutes|hour|hours|day|days)\)", expression)
        cron = re.fullmatch(r"cron\(([0-9*?,/ -]+)\)", expression)
        require(rate or cron, "Unsupported rate/cron syntax; minimum granularity is one minute")
        if rate:
            require((int(rate[1]) == 1) == (rate[2] in {"minute", "hour", "day"}), "Invalid rate plurality")
        else:
            fields = cron[1].split()
            require(len(fields) == 6 and (fields[2] == "?") != (fields[4] == "?"),
                    "Cron requires six fields and exactly one ? for day-of-month/day-of-week")
            for index, (field, bounds) in enumerate(zip(fields, [
                (0, 59), (0, 23), (1, 31), (1, 12), (1, 7), (1970, 2199),
            ])):
                if field == "?":
                    require(index in {2, 4}, "Cron ? is only allowed for a day field")
                    continue
                for item in field.split(","):
                    base, separator, step = item.partition("/")
                    require(not separator or (step.isascii() and step.isdigit()
                            and 1 <= int(step) <= bounds[1]-bounds[0]+1), "Invalid cron step")
                    if base == "*":
                        continue
                    match = re.fullmatch(r"([0-9]+)(?:-([0-9]+))?", base)
                    require(match is not None, "Unsupported numeric cron field")
                    lower, upper = int(match[1]), int(match[2] or match[1])
                    require(bounds[0] <= lower <= upper <= bounds[1], "Cron field out of range")


def validate_output(output, wire=False):
    validate_schema(output["schema"])
    require(output["dataset"].split(".")[0] in {"dwd", "dws", "ads"}, "Pipelines cannot replace ingestion-owned ODS")
    columns = {column["name"]: column for column in output["schema"]}
    require(set(output["key_columns"]).issubset(columns), "Unknown output key")
    require(all(not columns[name]["nullable"] for name in output["key_columns"]), "Nullable output key")
    if output["write_mode"] == "merge_keys":
        if wire:
            require(columns.get("_colvenna_op") == {"name": "_colvenna_op", "type": "VARCHAR", "nullable": False},
                    "merge_keys requires operation wire column")
            require("target_schema" in output, "merge_keys requires target business schema")
            validate_schema(output["target_schema"])
            metadata = [columns.get(name) for name in ["_colvenna_version", "_colvenna_sequence"]]
        else:
            metadata = output.get("change_metadata", [])
        require(metadata == [
            {"name": "_colvenna_version", "type": "BIGINT", "nullable": False},
            {"name": "_colvenna_sequence", "type": "BIGINT", "nullable": False},
        ], "Current merge_keys requires source-version/sequence metadata")
        if wire:
            expected = wire_schema({"schema": output["target_schema"], "key_columns": output["key_columns"],
                                    "change_metadata": metadata})
            require(output["schema"] == expected, "merge_keys wire/target schema mismatch")
    if not wire:
        require(all(not name.startswith("_colvenna_") for name in columns), "Reserved business column")
    if output["write_mode"] == "replace_partitions":
        partition = output.get("partition_columns", [])
        require(partition and set(partition).issubset(columns), "Partition replacement requires declared columns")
        require(all(not columns[name]["nullable"] for name in partition), "Nullable partition column")


def wire_schema(output):
    """DELETE permits null non-key business fields; target schema stays unchanged."""
    return [{"name": "_colvenna_op", "type": "VARCHAR", "nullable": False}] + [
        {**column, "nullable": column["nullable"] if column["name"] in output["key_columns"] else True}
        for column in output["schema"]
    ] + output["change_metadata"]


def validate_parameters(definitions, values, require_values=True):
    require(set(values).issubset(definitions), "Undeclared parameter")
    for name, definition in definitions.items():
        require(name.lower() not in {"api_key", "apikey", "token", "aws_access_key_id",
                                     "aws_secret_access_key", "role", "executor", "target"},
                "Credentials/execution resources cannot be parameters")
        kind = definition["type"]
        require(kind in {"decimal_string", "integer"} or not (
            "minimum" in definition or "maximum" in definition), "Bounds require numeric parameter type")
        def parse(value):
            if kind == "decimal_string":
                return decimal_value(value)
            return validate_value({"date": "DATE", "timestamp": "TIMESTAMP", "timestamptz": "TIMESTAMPTZ",
                                   "integer": "BIGINT", "boolean": "BOOLEAN", "string": "VARCHAR"}[kind], value)

        bounds = {name: parse(definition[name]) for name in ["minimum", "maximum"] if name in definition}
        if "minimum" in bounds and "maximum" in bounds:
            require(bounds["minimum"] <= bounds["maximum"], "Parameter range reversed")
        if name not in values and "default" not in definition:
            require(not require_values, "Required parameter missing")
            continue
        parsed = parse(values.get(name, definition.get("default")))
        for bound, compare in [("minimum", lambda a, b: a >= b), ("maximum", lambda a, b: a <= b)]:
            if bound in bounds:
                require(compare(parsed, bounds[bound]), "Parameter outside declared range")


def validate_pipeline(definition):
    tasks = {task["task_id"]: task for task in definition["tasks"]}
    require(len(tasks) == len(definition["tasks"]), "Duplicate task ID")
    validate_schedule(definition["schedule"])
    validate_parameters(definition.get("parameters", {}), {
        name: value["default"] for name, value in definition.get("parameters", {}).items() if "default" in value
    }, require_values=False)
    visiting, complete, targets = set(), set(), set()

    def visit(name):
        require(name in tasks, "Unknown task dependency")
        require(name not in visiting, "DAG cycle")
        if name in complete:
            return
        visiting.add(name)
        for dependency in tasks[name].get("depends_on", []):
            visit(dependency)
        visiting.remove(name)
        complete.add(name)

    for task in tasks.values():
        visit(task["task_id"])
        profile = PROFILES[task["resource_profile"]]
        require(profile["executor"] == task["executor"] and task["language"] in profile["languages"],
                "Executor/language/profile mismatch")
        require(task["timeout_seconds"] <= profile["max_timeout_seconds"], "Profile timeout exceeded")
        safe_relative(task["entrypoint"])
        require(task["entrypoint"].endswith(".sql" if task["language"] == "sql" else ".py"), "Entrypoint suffix mismatch")
        if task["executor"] != "writer":
            require("runtime_environment" in task and "input_mode" in task, "External executor requires environment/input mode")
            # The controlled SQL runner binds each alias to a local fixed relation.
            require(task["language"] != "sql" or task["input_mode"] == "local_files",
                    "SQL tasks bind local fixed input relations")
        else:
            require("runtime_environment" not in task and "input_mode" not in task,
                    "Writer execution uses the platform environment and native input bindings")
        validate_output(task["output"])
        target = task["output"]["dataset"]
        require(target not in targets, "Multiple tasks target the same dataset")
        targets.add(target)
        for binding in task["inputs"].values():
            if "task" in binding:
                require(binding["task"] in task.get("depends_on", []), "Input task must be explicit dependency")
        mode, write = task["processing"]["mode"], task["output"]["write_mode"]
        selections = [binding["selection"] for binding in task["inputs"].values()]
        if mode == "full":
            require(write == "versioned_replace" and all(s == "snapshot" for s in selections),
                    "Full requires complete snapshot inputs and versioned_replace")
        elif mode == "incremental":
            require(write == "merge_keys" and "changes" in selections,
                    "Current incremental protocol requires change input and merge_keys")
            require(task["processing"].get("bootstrap") == "full"
                    and "on_missing_state" in task["processing"], "Incremental requires bootstrap/missing-state policy")
            require(any(binding.get("state") == "target_key_state" for binding in task["inputs"].values()),
                    "Current incremental mode requires bound key state including tombstones")
            # Current protocol: one committed ODS interval plus the target's own
            # key state. Other joined inputs need a separate impact protocol.
            changes = [binding for binding in task["inputs"].values() if binding["selection"] == "changes"]
            require(len(changes) == 1 and changes[0].get("dataset", "").startswith("ods.")
                    and len(task["inputs"]) == 2, "Incremental binds one ODS change interval and key state")
        else:
            partition = task["processing"].get("partition_columns", [])
            require(write == "replace_partitions" and partition == task["output"].get("partition_columns")
                    and "affected_partitions" in selections and "on_unbounded_impact" in task["processing"],
                    "Partition recompute requires full affected partitions and impact policy")
            columns = {column["name"]: column for column in task["output"]["schema"]}
            require(all(columns[name]["type"] in {"VARCHAR", "BIGINT", "DATE", "BOOLEAN"} for name in partition),
                    "Partition values must use exact scalar types")
            require(all(binding["selection"] != "changes" for binding in task["inputs"].values()),
                    "Partition impact is derived from complete upstream releases")
        require(all(binding["selection"] != "changes" or mode == "incremental"
                    for binding in task["inputs"].values()), "Change intervals require incremental processing")


def validate_context(context):
    require(sum(len(binding["files"]) for binding in context["inputs"].values()) <= LIMITS["manifest_files"],
            "Context total input file quota exceeded")
    for binding in context["inputs"].values():
        validate_schema(binding["schema"])
        seen = set()
        for file in binding["files"]:
            key = (file["s3_uri"], file["object_version"])
            require(key not in seen and file["bytes"] > 0, "Duplicate/empty input object")
            seen.add(key)
            if context["input_mode"] == "local_files":
                require("local_path" in file, "local_files requires absolute downloaded paths")
                safe_absolute(file["local_path"])
            else:
                require("local_path" not in file, "s3_files cannot declare local download paths")
    output = context["output"]
    safe_absolute(output["directory"])
    require(context["execution_id"] in PurePosixPath(output["directory"]).parts
            and context["attempt_id"] in PurePosixPath(output["directory"]).parts,
            "Output directory must isolate attempt/execution identity")
    validate_output(output, wire=output["write_mode"] == "merge_keys")
    processing = context.get("processing", {"mode": "full"})
    mode = processing["mode"]
    for part in output["replace_partitions"]:
        columns = {column["name"]: column for column in output["schema"]}
        require(set(part) == set(output.get("partition_columns", [])), "Partition column set mismatch")
        for name, value in part.items():
            validate_value(columns[name]["type"], value, nullable=False)
    partition_set(output["replace_partitions"])
    if mode == "incremental":
        require(output["write_mode"] == "merge_keys", "Current incremental context requires merge_keys")
        require("from_watermark" in processing and "to_watermark" in processing, "Incremental interval missing")
        require(processing["from_watermark"]["ods_sequence"] <= processing["to_watermark"]["ods_sequence"],
                "Watermark interval reversed")
        require("state_release_id" in processing and (
            processing["state_release_id"] is not None or processing.get("bootstrap") is True),
            "Incremental state must be fixed or explicitly bootstrapped")
    elif mode == "partition_recompute":
        require(processing.get("affected_partitions") and output["write_mode"] == "replace_partitions"
                and partition_set(processing["affected_partitions"]) == partition_set(output["replace_partitions"]),
                "Partition coverage mismatch")
    else:
        require(output["write_mode"] == "versioned_replace"
                and all(binding["selection"]["mode"] == "snapshot" for binding in context["inputs"].values()),
                "Full context requires complete snapshots and replacement")
    if mode != "partition_recompute":
        require(not output["replace_partitions"], "Unexpected partition replacement")


def validate_manifest(manifest, context=None):
    validate_schema(manifest["schema"])
    require(manifest["row_count"] == sum(file["rows"] for file in manifest["files"]), "Manifest total rows mismatch")
    seen = set()
    for file in manifest["files"]:
        safe_relative(file["path"])
        require(file["path"].endswith(".parquet") and file["path"] not in seen and file["bytes"] > 0,
                "Duplicate, empty or non-Parquet output file")
        seen.add(file["path"])
    if context is not None:
        validate("context", context)
        for name in ["contract_version", "job_id", "attempt_id", "execution_id"]:
            require(manifest[name] == context[name], "Result identity differs from dispatch")
        require(manifest["schema"] == context["output"]["schema"], "Result wire schema differs from dispatch")
        mode = context["output"]["write_mode"]
        if manifest["row_count"] == 0 and mode != "merge_keys":
            require(context["output"]["allow_empty"], "Empty replacement is forbidden")
        if mode == "replace_partitions":
            allowed = context["output"]["replace_partitions"]
            require(partition_set(manifest.get("replace_partitions", [])) == partition_set(allowed),
                    "Manifest must explicitly cover every replacement, including empty partitions")
            require(all(file["partition_values"] in allowed for file in manifest["files"]), "Output partition outside assignment")
        else:
            require(all(not file["partition_values"] for file in manifest["files"]), "Unexpected output partition")
            require(not manifest.get("replace_partitions"), "Unexpected manifest partition replacement")


def partition_set(values):
    canonical = [json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) for value in values]
    require(len(canonical) == len(set(canonical)), "Duplicate partition")
    return set(canonical)


def validate_record(columns, record, operation="upsert", key_columns=()):
    validate_schema(columns)
    known = {column["name"]: column for column in columns}
    require(set(record).issubset(known), "Unknown record column")
    require(operation in {"append", "upsert", "delete"}, "Unknown record operation")
    for name, column in known.items():
        if operation == "delete" and name not in key_columns:
            if name not in record:
                continue
            nullable = True
        else:
            require(name in record, "Missing required record column")
            nullable = column["nullable"]
        validate_value(column["type"], record[name], nullable)
    require(set(key_columns).issubset(known), "Unknown record business key")
    require(all(record.get(name) is not None for name in key_columns), "Missing/null record key")


def validate_registration(value):
    validate_schema(value["schema"])
    require(value["dataset"] == value["dataset"].lower(), "Registered dataset names must be lowercase")
    names = [column["name"].lower() for column in value["schema"]]
    require(len(names) == len(set(names)), "DuckDB column names are case insensitive")
    require(all(not name.startswith("_colvenna_") for name in names), "Reserved ingestion metadata column")
    columns = {column["name"]: column for column in value["schema"]}
    for field in ["key_columns", "partition_columns"]:
        require(all(name in columns and not columns[name]["nullable"] for name in value[field]),
                "Keys/partitions must name nonnullable registered columns")
    owner = value.get("write_owner")
    state = value.get("publication_state")
    if state:
        require(owner == "pipeline", "Publication state requires pipeline ownership")
        validate_schema(state["schema"])
        state_names = [column["name"].lower() for column in state["schema"]]
        require(len(state_names) == len(set(state_names)), "State column names are case insensitive")
        state_columns = {column["name"]: column for column in state["schema"]}
        require(state["key_columns"] == value["key_columns"] and value["key_columns"],
                "Complete state image requires the registered business keys")
        require(all(state_columns.get(name) == columns[name] for name in state["key_columns"]),
                "State keys must have the registered nonnullable types")
        if state["protocol"] == "key_state_v1":
            # merge_keys state is platform maintained: keys, accepted version
            # metadata and tombstones. An empty state is a valid bootstrap.
            require(state["schema"] == [columns[name] for name in value["key_columns"]] + KEY_STATE_METADATA
                    and state["allow_empty"] is True, "Key state requires keys plus version/tombstone metadata")
    require(not value.get("ingestion") or owner in {None, "ingestion"}, "Ingestion requires ingestion ownership")
    require(owner != "direct" or not value["dataset"].split(".")[1].startswith("_colvenna_"),
            "Internal tables cannot be direct targets")
    require(not (value.get("constraints") or value.get("indexes")) or owner == "direct",
            "Physical constraint registration requires direct ownership")
    constraints = value.get("constraints", {})
    unique_keys = ([constraints["primary_key"]] if "primary_key" in constraints else []) + constraints.get("unique", [])
    for group in [*unique_keys, *value.get("indexes", [])]:
        require(all(name in columns and columns[name]["type"] not in {"JSON", "BLOB", "VARCHAR[]"} for name in group),
                "Native key/index columns require registered scalar types")
    if "primary_key" in constraints:
        require(all(not columns[name]["nullable"] for name in constraints["primary_key"]),
                "Primary key columns must be nonnullable")
    if owner == "direct" and value["key_columns"]:
        require(value["key_columns"] in unique_keys, "Direct keys require a declared native primary or unique constraint")
    for foreign in constraints.get("foreign_keys", []):
        require(all(name in columns for name in foreign["columns"])
                and len(foreign["columns"]) == len(foreign["referenced_columns"])
                and foreign["references"] != value["dataset"], "Invalid/cyclic foreign-key registration")
    for check in constraints.get("checks", []):
        column = columns.get(check["column"])
        require(column is not None and column["type"] not in {"JSON", "BLOB", "VARCHAR[]"},
                "CHECK requires a registered scalar column")
        validate_value(column["type"], check["value"], False)
    ingestion = value.get("ingestion")
    if ingestion:
        require(value["dataset"].startswith("ods."), "Only ODS datasets accept ingestion")
        event = columns.get(ingestion["event_id_column"])
        require(event and event["type"] == "VARCHAR" and not event["nullable"],
                "Event identity must be a nonnullable VARCHAR")
        if set(ingestion["operations"]) & {"upsert", "delete"}:
            require(value["key_columns"], "Upsert/delete requires business keys")
            version = columns.get(ingestion.get("source_version_column"))
            require(version and version["type"] == "BIGINT" and not version["nullable"],
                    "Upsert/delete requires a nonnullable BIGINT source version")
        elif "source_version_column" in ingestion:
            version = columns.get(ingestion["source_version_column"])
            require(version and version["type"] == "BIGINT" and not version["nullable"],
                    "Source version must be a nonnullable BIGINT")


def validate_write(value, definition=None):
    field = {"insert": "records", "upsert": "records", "update": "updates", "delete": "keys"}[value["operation"]]
    require(set(value) & {"records", "updates", "keys"} == {field}, "Operation requires its one payload form")
    if definition is None:
        return
    validate("dataset_registration", definition)
    require(definition.get("write_owner") == "direct", "Dataset is not a direct write target")
    require(value["dataset"] == definition["dataset"] and value["schema_version"] == definition["schema_version"],
            "Current direct dataset/schema is required")
    columns = {column["name"]: column for column in definition["schema"]}
    keys = definition["key_columns"]
    require(value["operation"] == "insert" or keys, "Operation requires native unique nonnullable keys")
    seen = set()
    for item in value[field]:
        if field == "records":
            validate_record(definition["schema"], item, "upsert", keys)
            key = {name: item[name] for name in keys}
        else:
            key = item["key"] if field == "updates" else item
            require(set(key) == set(keys), "Complete exact registered key is required")
            for name in keys:
                validate_value(columns[name]["type"], key[name], False)
            if field == "updates":
                require(item["set"] and set(item["set"]).issubset(columns)
                        and not set(item["set"]) & set(keys), "Update requires non-key registered columns")
                for name, member in item["set"].items():
                    validate_value(columns[name]["type"], member, columns[name]["nullable"])
        if keys:
            # Compare native typed key values, rather than JSON spelling.
            members = [validate_value(columns[name]["type"], key[name], False) for name in keys]
            identity = tuple(("0" if member == 0 else format(member.normalize(), "f"))
                             if isinstance(member, Decimal) else member for member in members)
            require(identity not in seen, "Duplicate key within one atomic write batch")
            seen.add(identity)


def validate_change(output, record):
    """Current source-version/tombstone wire protocol, independently of publication."""
    validate_output(output)
    require(output["write_mode"] == "merge_keys", "Change row requires merge_keys")
    known = {column["name"] for column in output["schema"]}
    require(set(record).issubset(known | {"_colvenna_op", "_colvenna_version", "_colvenna_sequence"}), "Unknown change column")
    require(record.get("_colvenna_op") in {"UPSERT", "DELETE"}, "Invalid change operation")
    for field in ["_colvenna_version", "_colvenna_sequence"]:
        require(field in record and validate_value("BIGINT", record[field]) >= 0, "Missing/negative change state")
    validate_record(output["schema"], {key: value for key, value in record.items() if key in known},
                    "upsert" if record["_colvenna_op"] == "UPSERT" else "delete", output["key_columns"])


def validate_run(request, definition):
    validate("pipeline", definition)
    root = {f"{task['task_id']}.{name}" for task in definition["tasks"]
            for name, binding in task["inputs"].items() if "dataset" in binding}
    require(set(request.get("inputs", {})).issubset(root), "Only root dataset inputs can be overridden")
    validate_parameters(definition.get("parameters", {}), request.get("parameters", {}))
    if request["mode"] == "backfill":
        require(request.get("partitions") and any(
            task["processing"]["mode"] == "partition_recompute" for task in definition["tasks"]),
            "Backfill requires partitions and a partition-aware definition")
    else:
        require("partitions" not in request, "Snapshot request cannot override partitions")


def validate_http_request(request):
    normalized = {name.lower(): value for name, value in request["headers"].items()}
    require(len(normalized) == len(request["headers"]), "Duplicate case-insensitive HTTP header")
    require(all("\r" not in value and "\n" not in value for value in normalized.values()), "HTTP header injection")
    require(re.fullmatch(r"(?i:Bearer) [A-Za-z0-9._~-]{32,512}", normalized.get("authorization", "")),
            "Required opaque Bearer API Key syntax")
    require(not any(name.startswith("x-colvenna-") for name in normalized), "Public callers cannot supply internal identity headers")
    selected = None
    for method, suffix, body, response, status, scope, conditional in ROUTES:
        pattern = re.escape("/v1"+suffix)
        pattern = re.sub(r"\\\{[^}]+\\\}", r"([A-Za-z_][A-Za-z0-9_.-]*)", pattern)
        if method == request["method"] and re.fullmatch(pattern, request["path"]):
            selected = (body, conditional, response)
            break
    require(selected is not None, "Unknown HTTP contract route")
    body, conditional, response = selected
    query = request.get("query") or {}
    fields = QUERY_PARAMETERS.get(response, {}) if request["method"] == "GET" else {}
    require(not set(query) - set(fields), "Query parameters not supported on this route")
    if request["path"].endswith("/export"):
        require("token" in query, "Export requires its bounded token")
    for name, value in query.items():
        errors = list(Draft202012Validator(fields[name], format_checker=FORMATS).iter_errors(value))
        require(not errors, "Invalid query parameter " + name)
    if conditional:
        value = normalized.get(conditional.lower(), "")
        pattern = (r'^"gen_[0-9a-f]{32}"$' if body == "dataset_registration"
                   else r'^"[A-Za-z_][A-Za-z0-9_.-]*"$' if conditional == "If-Match"
                   else r"[A-Za-z0-9_.:-]{1,128}")
        require(re.fullmatch(pattern, value), "Required idempotency key or explicit If-Match revision")
    if body is None:
        require("body" not in request or request["body"] is None, "GET contract has no request body")
    else:
        value = request.get("body", {} if body == "empty_request" else None)
        if value:
            require(normalized.get("content-type", "").split(";")[0].lower() == "application/json",
                    "Request body requires application/json content type")
        validate(body, value)


def validate(name, value, *, context=None, definition=None, dataset_schema=None, key_columns=()):
    require(name in SCHEMA["$defs"], "Unknown contract name")
    finite_json(value)
    if name.endswith("_request"):
        require(len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()) <= LIMITS["api_body_bytes"],
                "API body byte quota exceeded")
    errors = sorted(Draft202012Validator({**SCHEMA, "$ref": f"#/$defs/{name}"}, format_checker=FORMATS).iter_errors(value),
                    key=lambda error: tuple(str(part) for part in error.absolute_path))
    if errors:
        path = "/" + "/".join(str(part) for part in errors[0].absolute_path)
        raise ContractError(f"{name}{path}: schema keyword {errors[0].validator} rejected value")
    if name == "pipeline":
        validate_pipeline(value)
    elif name == "context":
        validate_context(value)
    elif name == "http_request":
        validate_http_request(value)
    elif name == "output_manifest":
        validate_manifest(value, context)
    elif name == "schema":
        validate_schema(value)
    elif name == "dataset_registration":
        validate_registration(value)
    elif name == "registered_dataset":
        validate("dataset_registration", value["definition"])
    elif name == "space_configuration":
        for dataset, registered in value["datasets"].items():
            require(dataset == registered["definition"]["dataset"], "Dataset identity mismatch")
            validate("registered_dataset", registered)
    elif name == "ingestion_message":
        expected = (f"spaces/{value['space_id']}/ingest/{value['generation']}/"
                    f"{value['dataset'].removeprefix('ods.')}/{value['ingestion_id']}.json")
        require(value["source"]["key"] == expected and value["source"]["version_id"] != "null",
                "Ingestion message requires its exact current-generation immutable object")
    elif name in {"column", "type"}:
        parse_type(value["type"] if name == "column" else value)
    elif name == "input_manifest":
        validate_schema(value["schema"])
        objects = [(file["s3_uri"], file["object_version"]) for file in value["files"]]
        require(len(objects) == len(set(objects)) and all(
            "local_path" not in file and file["bytes"] > 0 for file in value["files"]),
            "Portable input manifest requires distinct fixed objects without local paths")
    elif name == "draft_request":
        validate("pipeline", value["definition"])
        require(sum(file["bytes"] for file in value["artifacts"]) <= LIMITS["artifact_package_bytes"],
                "Code package byte quota exceeded")
        paths = [file["path"] for file in value["artifacts"]]
        require(len(paths) == len(set(paths)), "Duplicate code artifact")
        require(all(not other.startswith(path + "/") for path in paths for other in paths),
                "Artifact file/directory prefix collision")
        for path in paths:
            safe_relative(path)
        require(all(task["entrypoint"] in paths for task in value["definition"]["tasks"]), "Missing task artifact")
    elif name in {"schedule", "schedule_request"}:
        validate_schedule(value)
    elif name == "query_request":
        require(value["sql"].strip() and len(value["sql"].encode()) <= LIMITS["sql_bytes"], "Empty SQL or byte quota exceeded")
        mode = value.get("consistency", "published")
        condition = {"after_ingestion": "ingestion_id", "after_write": "write_id"}.get(mode)
        ids = {name for name in ["ingestion_id", "write_id", "release_id"] if name in value}
        require(not condition or ids == {condition}, "After-commit consistency requires its exclusive receipt ID")
        require(condition or not ids or mode == "published" and ids == {"release_id"},
                "Current/published consistency conditions differ")
    elif name == "ingestion_request" and dataset_schema is not None:
        for record in value["records"]:
            validate_record(dataset_schema, record, value["operation"], key_columns)
    elif name == "write_request":
        validate_write(value, definition)
    elif name == "run_request":
        require(value["mode"] != "backfill" or "partitions" in value, "Backfill partitions missing")
        if definition is not None:
            validate_run(value, definition)
    elif name == "environment_request":
        require(not re.search(r"https?://[^/\s]+@", value["dependency_source"]["content"]),
                "Package credentials cannot be dependency content")
    elif name == "environment_metadata" and value["status"] == "READY":
        require(value.get("dependency_lock_sha256") and value.get("image_digest") and value.get("executor_bindings"),
                "READY requires immutable lock/image/executor bindings")
    elif name == "ingestion_status" and value["status"] in {"COMMITTED", "PUBLISHED"}:
        require("committed_at" in value, "Committed state requires commit observation")
        if value.get("publication", {}).get("status") == "PUBLISHED":
            require(value["publication"].get("release_id"), "Published observation requires release identity")
    elif name == "query_result" or name == "query_status" and value["status"] == "SUCCEEDED" \
            and value.get("view", "result") == "result":
        if name == "query_status":
            require(all(key in value for key in ["data_versions", "columns", "rows", "elapsed_ms"]),
                    "Successful query requires actual data/version result")
        for row in value["rows"]:
            require(len(row) == len(value["columns"]), "Query row/column count mismatch")
            for column, item in zip(value["columns"], row):
                if column["encoding"] == "native_text":
                    require(item is None or isinstance(item, str), "Native text requires a string")
                elif re.fullmatch(r"DECIMAL\(\d+,\d+\)", column["type"]):
                    validate_value(column["type"], item, nullable=True)
        for version in value["data_versions"]:
            if version.get("release_id") is None:
                require(all(key in version for key in ["generation", "commit_sequence", "snapshot_sequence"]),
                        "Native current results require actual generation and watermarks")
            if "snapshot_sequence" in version:
                require(version["commit_sequence"] <= version["snapshot_sequence"], "Watermark exceeds snapshot")
        def safe_numbers(item):
            if type(item) is int:
                require(abs(item) <= 9007199254740991, "Query JSON integers must be exact in the browser")
            elif isinstance(item, list):
                for member in item:
                    safe_numbers(member)
            elif isinstance(item, dict):
                for member in item.values():
                    safe_numbers(member)
        safe_numbers(value["rows"])
    elif name == "query_history":
        for item in value["queries"]:
            validate("query_status", item)
    elif name == "run_status":
        for task in value["tasks"]:
            validate("task_status", task)
        require(value["status"] != "PUBLISHED" or (value["tasks"] and all(
            task["status"] == "PUBLISHED" for task in value["tasks"])), "Published run requires every task release")
    elif name == "task_status" and value["status"] == "PUBLISHED":
        require("release_id" in value, "Published task requires release")
    if name == "catalog":
        for dataset in value["datasets"]:
            validate_schema(dataset["schema"])
    if name in {"version_metadata", "pipeline_metadata"} and "definition" in value:
        validate("pipeline", value["definition"])
    return value


def validate_transition(entity, before, after, evidence=None):
    evidence = evidence or {}
    require(entity in STATES, "Unknown state entity")
    states = STATES[entity]
    require(before in states or before == "UNKNOWN", "Unknown source state")
    require(after in states or after == "UNKNOWN", "Unknown target state")
    if before == after or after == "UNKNOWN":
        return
    if before == "UNKNOWN":
        require(evidence.get("reconciled") is True and evidence.get("authoritative_state") == after,
                "UNKNOWN must reconcile authoritative state before action")
        return
    require(after in states[before], "Illegal state transition")
    if entity == "query" and before == "QUEUED" and after == "CANCELLED":
        require(evidence.get("execution_never_started") is True,
                "Queued cancellation requires positive no-execution custody")
    if entity == "query" and before in {"RUNNING", "CANCELLING"} and after in {
            "SUCCEEDED", "FAILED", "CANCELLED"}:
        require(evidence.get("execution_reaped") is True or
                after != "SUCCEEDED" and evidence.get("execution_never_started") is True,
                "Query terminal requires positive custody of the original execution")
    if entity == "task" and before == "RUNNING" and after == "VALIDATING":
        require(evidence.get("executor") == "writer", "Only writer SQL can omit external STAGED")
    if before == "FAILED":
        require(evidence.get("new_attempt") is True and evidence.get("same_immutable_bindings") is True,
                "Retry requires new attempt and identical immutable bindings")
        if entity == "ingestion":
            require(evidence.get("reconciled") is True and evidence.get("authoritative_state") == after,
                    "Failed ingestion observation requires authoritative native reconciliation")
    if after == "CANCELLED" and entity in {"task", "run"}:
        require(evidence.get("writer_eligibility_revoked") is True, "Cancellation requires writer revocation")
    if before == "CANCELLING" and after in {"PUBLISHED", "SUCCEEDED"}:
        require(evidence.get("completion_confirmed") is True, "Completion/cancellation race must reconcile")


