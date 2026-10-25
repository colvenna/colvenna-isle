"""Native eligibility, immutable data/state generations and atomic publication.

The writer caller verifies the original descriptor/completion/files and supplies
fresh authorization. Every mutation uses the same native transaction as its
ledger, and executes on the writer's fair scheduler. This module does not
authorize an untrusted manifest or implement an AWS Run controller.
"""

import copy
import math
import time

from colvenna_isle.contracts.validation import validate, wire_schema
from colvenna_isle.contracts.model import TASK_BUDGETS
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.control.pipelines import DEFINITION_EPOCH
from colvenna_isle.pipeline.parquet import _native_value
from colvenna_isle.pipeline.task_descriptor import check_descriptor, descriptor_key, staging_prefix
from colvenna_isle.pipeline.task_objects import TaskObjects, input_reference
from colvenna_isle.writer.ingestion import digest, quoted, wire
from colvenna_isle.writer.native_schema import physical

CHUNK_ROWS = 512
DATA_SCHEMA = "_colvenna_data"
MAX_JOBS = 128
MAX_GENERATION_BYTES = 536870912
# processing.mode -> the only approved physical publication protocol.
MODES = {"full": "versioned_replace", "incremental": "merge_keys", "partition_recompute": "replace_partitions"}


def _key_match(columns):
    return " AND ".join(quoted(name) + "=?" for name in columns)


def _row_text(columns):
    return "CAST(ROW(" + ",".join(quoted(column["name"]) for column in columns) + ") AS VARCHAR)"


def partition_summary(connection, table, schema, partition_columns):
    """Exact per-partition row count and order-independent content hash.

    Rows are key-unique, so XOR of each row's MD5 over its typed text has no
    cancelling duplicates and needs no per-partition sort or string buffer.
    """
    from colvenna_isle.query.native import _partition_expression, partition_value

    rows = connection.execute(
        "SELECT " + _partition_expression(partition_columns) + " AS p," +
        ",".join(quoted(name) for name in sorted(partition_columns)) + ",count(*),"
        "CAST(bit_xor(md5_number(" + _row_text(schema) + ")) AS VARCHAR) "
        "FROM " + table + " GROUP BY ALL ORDER BY p").fetchall()
    return {row[0]: {"partition": {name: partition_value(value)
                                   for name, value in zip(sorted(partition_columns), row[1:-2])},
                     "rows": row[-2], "sha256": row[-1]} for row in rows}


MAX_PARTITIONS = 10000


def impact(connection, configuration, dataset, before, after, partition_columns):
    """Distinct partition values of every row that differs between two releases.

    Without partition columns the result only says whether any row differs:
    one empty partition when changed (including a schema change), none when a
    republished release holds exactly the same rows.
    """
    from colvenna_isle.query.native import partition_value

    new = resolve_release(connection, configuration, dataset, after)
    if new is None:
        raise ControlError(409, "INPUT_RELEASE_NOT_PUBLISHED")
    schema = {column["name"]: column for column in new["schema"]}
    if any(name not in schema or schema[name]["type"] not in
           {"VARCHAR", "BIGINT", "DATE", "BOOLEAN"} for name in partition_columns):
        raise ControlError(422, "INPUT_PARTITION_COLUMN")
    columns = ",".join(quoted(column["name"]) for column in new["schema"])
    names = sorted(partition_columns)
    new_table = "warehouse." + DATA_SCHEMA + "." + quoted(new["physical"]["table"])
    result = {"bounded": True, "from_release": before, "to_release": after}
    if before is None:
        difference = "SELECT " + columns + " FROM " + new_table
    else:
        old = resolve_release(connection, configuration, dataset, before)
        if old["schema"] != new["schema"]:
            if not names:
                return {**result, "partitions": [{}]}
            raise ControlError(409, "INPUT_IMPACT_SCHEMA_CHANGED")
        old_table = "warehouse." + DATA_SCHEMA + "." + quoted(old["physical"]["table"])
        difference = ("(SELECT " + columns + " FROM " + old_table + " EXCEPT SELECT " + columns + " FROM " + new_table
                      + ") UNION ALL (SELECT " + columns + " FROM " + new_table + " EXCEPT SELECT " + columns
                      + " FROM " + old_table + ")")
    if not names:
        changed = connection.execute("SELECT count(*) FROM (SELECT 1 FROM (" + difference + ") AS d LIMIT 1) AS e"
                                     ).fetchone()[0]
        return {**result, "partitions": [{}] if changed else []}
    rows = connection.execute(
        "SELECT DISTINCT " + ",".join(quoted(name) for name in names) + " FROM (" + difference + ") AS d "
        "ORDER BY ALL LIMIT " + str(MAX_PARTITIONS + 1)).fetchall()
    if len(rows) > MAX_PARTITIONS:
        return {**result, "bounded": False, "partitions": None}
    return {**result, "partitions": [{name: partition_value(value) for name, value in zip(names, row)}
                                     for row in rows]}


def _table(connection, name):
    return connection.execute(
        "SELECT count(*) FROM duckdb_tables() WHERE database_name='warehouse' "
        "AND schema_name='ctl' AND table_name=?", [name]).fetchone()[0] == 1


def resolve_release(connection, configuration, dataset, release_id=None):
    """Resolve only the native ledger's complete release, within the caller's MVCC snapshot."""
    import json

    validate("dataset", dataset)
    registered = configuration["datasets"].get(dataset)
    if not registered or registered["definition"].get("write_owner") != "pipeline":
        raise ControlError(403, "PUBLICATION_TARGET_FORBIDDEN")
    if not _table(connection, "dataset_releases"):
        return None
    current = release_id is None
    if current:
        pointer = connection.execute(
            "SELECT release_id FROM warehouse.ctl.release_pointers WHERE dataset=?", [dataset]).fetchone()
        if not pointer:
            return None
        release_id = pointer[0]
    validate("identifier", release_id)
    row = connection.execute(
        "SELECT record FROM warehouse.ctl.dataset_releases WHERE release_id=? AND dataset=?",
        [release_id, dataset]).fetchone()
    if not row:
        raise ControlError(404, "PUBLICATION_RELEASE_NOT_FOUND")
    value = json.loads(row[0])
    if value.get("status") == "COLLECTED":
        # Retention removed this historical release; it is never substituted.
        raise ControlError(410, "PUBLICATION_RELEASE_COLLECTED")
    definition = registered["definition"]
    if (value["space_id"] != configuration["space_id"]
            or value["generation"] != configuration["generation"]
            or value["dataset"] != dataset or value["release_id"] != release_id
            or value["status"] != "PUBLISHED" or value["schema"] != definition["schema"]
            or value["definition_epoch"] != DEFINITION_EPOCH
            or value["definition_sha256"] != registered["definition_sha256"]
            or digest(definition) != registered["definition_sha256"]
            or value["physical"]["schema"] != DATA_SCHEMA):
        raise ControlError(503, "PUBLICATION_RELEASE_CUSTODY_UNKNOWN")
    validate("identifier", value["physical"]["table"])
    if digest(physical(connection, DATA_SCHEMA + "." + value["physical"]["table"])) != value["physical_sha256"]:
        raise ControlError(503, "PUBLICATION_PHYSICAL_CUSTODY_UNKNOWN")
    state, contract = value["state"], definition.get("publication_state")
    if (state is None) != (contract is None):
        raise ControlError(503, "PUBLICATION_STATE_CUSTODY_UNKNOWN")
    if state is not None:
        if (state["contract_sha256"] != digest(contract)
                or any(state[name] != contract[name] for name in ["protocol", "schema", "key_columns"])
                or state["physical"]["schema"] != DATA_SCHEMA):
            raise ControlError(503, "PUBLICATION_STATE_CUSTODY_UNKNOWN")
        validate("identifier", state["physical"]["table"])
        if digest(physical(connection, DATA_SCHEMA + "." + state["physical"]["table"])) != state["physical_sha256"]:
            raise ControlError(503, "PUBLICATION_STATE_CUSTODY_UNKNOWN")
    if current:
        if not _table(connection, "state_pointers"):
            raise ControlError(503, "PUBLICATION_STATE_POINTER_CUSTODY_UNKNOWN")
        pointer = connection.execute(
            "SELECT release_id,state_record FROM warehouse.ctl.state_pointers WHERE dataset=?",
            [dataset]).fetchone()
        if pointer != (release_id, canonical(state)):
            raise ControlError(503, "PUBLICATION_STATE_POINTER_CUSTODY_UNKNOWN")
    return value


class Publications:
    """Writer-owned complete data/state images behind grant/select/chunk/publish.

    authorize(record) must recheck principal, permit, generation, current
    definition/registered target and retained native input authority. The AWS
    caller independently validates the selected original completion/Parquet.
    """

    def __init__(self, native, authorize):
        self.native, self.authorize, self.initialized = native, authorize, False
        self.connection = native.connection

    def initialize(self):
        self.native.initialize()
        if self.initialized:
            self.native.require()
            return

        def create():
            c = self.connection
            c.execute("CREATE SCHEMA IF NOT EXISTS warehouse." + DATA_SCHEMA)
            definitions = {
                "publication_jobs": "(job_id VARCHAR PRIMARY KEY,epoch BIGINT NOT NULL,record VARCHAR NOT NULL)",
                "publication_attempts": "(job_id VARCHAR,attempt_id VARCHAR,epoch BIGINT NOT NULL,"
                    "PRIMARY KEY(job_id,attempt_id))",
                "publication_generations": "(generation_id VARCHAR PRIMARY KEY,job_id VARCHAR NOT NULL,"
                    "epoch BIGINT NOT NULL,table_name VARCHAR NOT NULL,state_table_name VARCHAR,"
                    "reserved_bytes BIGINT NOT NULL,"
                    "record VARCHAR NOT NULL)",
                "publication_chunks": "(generation_id VARCHAR,image VARCHAR,file_number INTEGER,chunk_number INTEGER,"
                    "sha256 VARCHAR NOT NULL,rows INTEGER NOT NULL,commit_sequence BIGINT UNIQUE NOT NULL,"
                    "PRIMARY KEY(generation_id,image,file_number,chunk_number))",
                "generation_keys": "(generation_id VARCHAR,image VARCHAR,identity VARCHAR,"
                    "PRIMARY KEY(generation_id,image,identity))",
                "dataset_releases": "(release_id VARCHAR PRIMARY KEY,dataset VARCHAR NOT NULL,record VARCHAR NOT NULL)",
                "release_pointers": "(dataset VARCHAR PRIMARY KEY,release_id VARCHAR NOT NULL)",
                "state_pointers": "(dataset VARCHAR PRIMARY KEY,release_id VARCHAR NOT NULL,state_record VARCHAR NOT NULL)",
                "pipeline_task_watermarks": "(pipeline_id VARCHAR,task_id VARCHAR,record VARCHAR NOT NULL,"
                    "PRIMARY KEY(pipeline_id,task_id))",
                # Physical effect of each committed chunk on its generation.
                "publication_effects": "(generation_id VARCHAR,image VARCHAR,file_number INTEGER,"
                    "chunk_number INTEGER,inserted BIGINT NOT NULL,deleted BIGINT NOT NULL,"
                    "PRIMARY KEY(generation_id,image,file_number,chunk_number))",
            }
            for name, definition in definitions.items():
                c.execute("CREATE TABLE IF NOT EXISTS warehouse.ctl." + name + " " + definition)

        self.native.transaction(create)
        self.initialized = True

    def _get(self, job):
        import json

        self.initialize()
        validate("identifier", job)
        row = self.connection.execute(
            "SELECT epoch,record FROM warehouse.ctl.publication_jobs WHERE job_id=?", [job]).fetchone()
        if not row:
            return None
        value = json.loads(row[1])
        if value["job_id"] != job or value["epoch"] != row[0] or "state_contract" not in value:
            raise ControlError(503, "PUBLICATION_JOB_CUSTODY_UNKNOWN")
        return value

    def _guard(self, value):
        descriptor = value["descriptor"]
        self.native.require()
        self.authorize(value)
        configuration = self.native.configuration
        if (descriptor["space_id"] != configuration["space_id"]
                or descriptor["generation"] != configuration["generation"]):
            raise ControlError(403, "PUBLICATION_SPACE_GENERATION_CHANGED")
        if descriptor["definition_epoch"] != DEFINITION_EPOCH:
            raise ControlError(409, "PUBLICATION_DEFINITION_CHANGED")
        target = configuration["datasets"].get(descriptor["task"]["output"]["dataset"])
        if (not target or target["definition"].get("write_owner") != "pipeline"
                or target["definition"]["schema"] != descriptor["task"]["output"]["schema"]
                or target["definition"]["key_columns"] != descriptor["task"]["output"]["key_columns"]
                or digest(target["definition"]) != target["definition_sha256"]):
            raise ControlError(403, "PUBLICATION_TARGET_FORBIDDEN")
        if value.get("definition_sha256", target["definition_sha256"]) != target["definition_sha256"]:
            raise ControlError(409, "PUBLICATION_TARGET_CHANGED")
        if value.get("state_contract", target["definition"].get("publication_state")) != target["definition"].get("publication_state"):
            raise ControlError(409, "PUBLICATION_STATE_CONTRACT_CHANGED")
        return target

    def _save(self, value):
        self.connection.execute(
            "INSERT INTO warehouse.ctl.publication_jobs VALUES (?,?,?) "
            "ON CONFLICT(job_id) DO UPDATE SET epoch=excluded.epoch,record=excluded.record",
            [value["job_id"], value["epoch"], canonical(value)])

    def status(self, job):
        value = self._get(job)
        if value is None:
            raise ControlError(404, "PUBLICATION_JOB_NOT_FOUND")
        self._guard(value)
        return copy.deepcopy(value)

    def _active(self, job, epoch, attempt, execution=None):
        value = self.status(job)
        if (type(epoch) is not int or epoch < 1 or value["epoch"] != epoch
                or value["descriptor"]["attempt_id"] != attempt or value["status"] == "REVOKED"
                or execution is not None and value.get("execution_id") != execution):
            raise ControlError(409, "PUBLICATION_ELIGIBILITY_CHANGED")
        return value

    def grant(self, reference, descriptor, authority, *, expected_epoch=0):
        check_descriptor(descriptor)
        validate("task_object", reference)
        if (not isinstance(authority, dict)
                or set(authority) != {"principal_id", "input_export_id", "input_manifest"}
                or not isinstance(authority["input_manifest"], dict)):
            raise ControlError(422, "PUBLICATION_INPUT_AUTHORITY_REQUIRED")
        for name in ["principal_id", "input_export_id"]:
            validate("identifier", authority[name])
        raw = canonical(descriptor).encode()
        objects = self._objects()
        if (reference["sha256"] != digest(descriptor) or reference["bytes"] != len(raw)
                or objects.key(reference, "task-contexts") != descriptor_key(descriptor)):
            raise ControlError(422, "PUBLICATION_DESCRIPTOR_BINDING")
        processing, output = descriptor["processing"], descriptor["task"]["output"]
        if MODES.get(processing["mode"]) != output["write_mode"] or processing["mode"] != descriptor["task"]["processing"]["mode"]:
            raise ControlError(422, "PUBLICATION_MODE_NOT_READY")
        previous = descriptor["output_state"]["expected_previous_release"]
        if processing["mode"] == "incremental" and (
                processing["state_release_id"] != (None if processing.get("bootstrap") else previous)
                or processing.get("bootstrap") and processing["from_watermark"]["ods_sequence"] != 0):
            # A non-bootstrap merge always starts from the exact CAS release.
            raise ControlError(422, "PUBLICATION_STATE_BINDING")
        if type(expected_epoch) is not int or expected_epoch < 0:
            raise ControlError(400, "PUBLICATION_EPOCH_REQUIRED")
        self.initialize()
        result = None

        def change():
            nonlocal result
            prior = self._get(descriptor["job_id"])
            if (prior and prior["reference"] == reference and prior["descriptor"] == descriptor
                    and prior["authority"] == authority):
                self._guard(prior)
                result = prior
                return
            # Revocation is sticky: a cancelled job can never regain eligibility.
            if prior and (prior["epoch"] != expected_epoch or prior["status"] in {"PUBLISHED", "REVOKED"}
                    or prior["descriptor"]["attempt_id"] == descriptor["attempt_id"]):
                raise ControlError(409, "PUBLICATION_ELIGIBILITY_CHANGED")
            if not prior and expected_epoch != 0:
                raise ControlError(409, "PUBLICATION_ELIGIBILITY_CHANGED")
            # Only in-flight jobs reserve capacity; PUBLISHED/REVOKED records
            # are retained ledger custody, not active work.
            if not prior and self.connection.execute(
                    "SELECT count(*) FROM warehouse.ctl.publication_jobs WHERE "
                    "json_extract_string(record,'$.status') NOT IN ('PUBLISHED','REVOKED')").fetchone()[0] >= MAX_JOBS:
                raise ControlError(429, "PUBLICATION_JOB_QUOTA")
            if prior and ({k: v for k, v in prior["descriptor"].items() if k != "attempt_id"}
                          != {k: v for k, v in descriptor.items() if k != "attempt_id"}
                          or prior["authority"] != authority):
                raise ControlError(409, "PUBLICATION_JOB_BINDING_CONFLICT")
            if self.connection.execute(
                    "SELECT count(*) FROM warehouse.ctl.publication_attempts WHERE job_id=? AND attempt_id=?",
                    [descriptor["job_id"], descriptor["attempt_id"]]).fetchone()[0]:
                raise ControlError(409, "PUBLICATION_ATTEMPT_REUSED")
            value = {"job_id": descriptor["job_id"], "epoch": expected_epoch + 1,
                "descriptor": copy.deepcopy(descriptor), "reference": copy.deepcopy(reference),
                "authority": copy.deepcopy(authority), "status": "GRANTED"}
            target = self._guard(value)
            contract = target["definition"].get("publication_state")
            protocol = contract["protocol"] if contract else None
            if ((output["write_mode"] == "merge_keys") != (protocol == "key_state_v1")
                    or output["write_mode"] == "replace_partitions" and contract is not None
                    or output["write_mode"] == "replace_partitions"
                    and target["definition"]["partition_columns"] != output["partition_columns"]):
                raise ControlError(422, "PUBLICATION_STATE_PROTOCOL")
            value["definition_sha256"] = target["definition_sha256"]
            value["state_contract"] = copy.deepcopy(contract)
            current = resolve_release(self.connection, self.native.configuration, output["dataset"])
            observed = current["release_id"] if current else None
            if observed != previous:
                raise ControlError(409, "PUBLICATION_PREVIOUS_RELEASE_CONFLICT")
            self._continuity(descriptor, authority, current)
            self.connection.execute("INSERT INTO warehouse.ctl.publication_attempts VALUES (?,?,?)",
                [descriptor["job_id"], descriptor["attempt_id"], value["epoch"]])
            self._save(value)
            result = value

        self.native.transaction(change, lambda: self._guard(result))
        return copy.deepcopy(result)

    def _continuity(self, descriptor, authority, prior):
        """Independently require that a job covers every change since the CAS release.

        A bootstrap rebuilds from the complete history and needs no prior. A
        continuing merge must start exactly at the prior release's interval
        end. A partition job must replace at least every partition whose
        upstream rows differ since the release that the prior consumed, and
        everything when another input or the rules changed.
        """
        from colvenna_isle.query.native import partition_text

        processing, task = descriptor["processing"], descriptor["task"]
        versions = {alias: (value or {}).get("native_version") or {}
                    for alias, value in authority["input_manifest"].get("inputs", {}).items()}
        prior_versions = (prior or {}).get("input_versions") or {}
        if processing["mode"] == "incremental" and not processing.get("bootstrap"):
            alias = next(name for name, value in task["inputs"].items() if value["selection"] == "changes")
            if (prior is None or prior_versions.get(alias, {}).get("to_sequence") is None
                    or processing["from_watermark"]["ods_sequence"] != prior_versions[alias]["to_sequence"]):
                raise ControlError(409, "PUBLICATION_INTERVAL_NOT_CONTINUOUS")
        if processing["mode"] != "partition_recompute":
            return
        configuration = self.native.configuration
        schema, names = task["output"]["schema"], task["processing"]["partition_columns"]
        same_rules = (prior is not None and prior.get("pipeline_version") == descriptor["pipeline_version"]
                      and prior.get("task_sha256") == digest(task)
                      and prior.get("parameters_sha256") == digest(descriptor["parameters"]))
        driving = [alias for alias, value in task["inputs"].items() if value["selection"] == "affected_partitions"]

        def changed(alias):
            now, before = versions.get(alias, {}), prior_versions.get(alias, {})
            if now.get("release_id") is not None and before.get("release_id") is not None:
                # A full upstream task republishes each run: only changed rows count.
                return now["release_id"] != before["release_id"] and bool(impact(
                    self.connection, configuration, descriptor["inputs"][alias]["dataset"],
                    before["release_id"], now["release_id"], [])["partitions"])
            return any(now.get(name) != before.get(name) for name in ["release_id", "commit_sequence"])
        full = not same_rules or any(changed(alias) for alias in task["inputs"] if alias not in driving)
        required = set()
        for alias in driving:
            dataset, release = descriptor["inputs"][alias]["dataset"], versions.get(alias, {}).get("release_id")
            before = None if full else prior_versions.get(alias, {}).get("release_id")
            changed = impact(self.connection, configuration, dataset, before, release, names)
            if not changed["bounded"]:
                raise ControlError(409, "PUBLICATION_IMPACT_UNBOUNDED")
            required |= {partition_text(schema, part) for part in changed["partitions"]}
        if full and prior is not None:
            required |= {partition_text(schema, entry["partition"]) for entry in prior.get("partitions", [])}
        affected = {partition_text(schema, part) for part in processing["affected_partitions"]}
        if not required <= affected:
            raise ControlError(409, "PUBLICATION_IMPACT_INCOMPLETE")

    def revoke(self, job, *, expected_epoch):
        if type(expected_epoch) is not int or expected_epoch < 1:
            raise ControlError(400, "PUBLICATION_EPOCH_REQUIRED")
        result = None

        def change():
            nonlocal result
            value = self.status(job)
            if value["status"] == "PUBLISHED":
                result = value
                return
            if value["status"] == "REVOKED" and value["epoch"] == expected_epoch + 1:
                result = value
                return
            if value["epoch"] != expected_epoch:
                raise ControlError(409, "PUBLICATION_ELIGIBILITY_CHANGED")
            value.update(epoch=expected_epoch + 1, status="REVOKED")
            self._save(value)
            result = value

        self.initialize()
        self.native.transaction(change, lambda: self._guard(result))
        return copy.deepcopy(result)

    @staticmethod
    def _complete_state(value):
        contract = value["state_contract"]
        return contract is not None and contract["protocol"] == "complete_image_v1"

    def select(self, job, epoch, attempt, execution, completion, files, *, state_files=None):
        validate("identifier", execution)
        validate("task_object", completion)
        value = self._active(job, epoch, attempt)
        # Only a complete state image is produced by the task. merge_keys key
        # state is derived by this writer from the base release and each change.
        if self._complete_state(value) != (state_files is not None):
            raise ControlError(422, "PUBLICATION_STATE_PLAN_REQUIRED")
        if not isinstance(files, list) or state_files is not None and not isinstance(state_files, list):
            raise ControlError(422, "PUBLICATION_PLAN_REQUIRED")
        combined = files + (state_files or [])
        for file in combined:
            validate("input_file", file)
            if "local_path" in file:
                raise ControlError(422, "PUBLICATION_PORTABLE_FILES_REQUIRED")
        if len(combined) > 10000 or len(canonical(combined).encode()) > 1048576:
            raise ControlError(413, "PUBLICATION_PLAN_QUOTA")
        plan = {"execution_id": execution, "completion": copy.deepcopy(completion),
                "files": copy.deepcopy(files), "state_files": copy.deepcopy(state_files)}
        descriptor = value["descriptor"]
        budgets = TASK_BUDGETS[descriptor["task"]["resource_profile"]]
        if (sum(file["bytes"] for file in combined) > budgets["output_bytes"]
                or sum(file["rows"] for file in combined) > budgets["output_rows"]):
            raise ControlError(413, "PUBLICATION_OUTPUT_QUOTA")
        prefix = staging_prefix(descriptor, execution)
        objects = self._objects()
        if objects.key(completion, "staging") != prefix + "completion.json":
            raise ControlError(403, "PUBLICATION_EXECUTION_SCOPE")
        keys = [objects.key(input_reference(file), "staging") for file in combined]
        if (len(set(keys)) != len(keys)
                or any(not objects.key(input_reference(file), "staging").startswith(prefix + namespace + "/")
                       for namespace, members in [("result", files), ("state", state_files or [])]
                       for file in members)):
            raise ControlError(403, "PUBLICATION_FILE_SCOPE")
        if "selection" in value:
            if value["selection"] != plan:
                raise ControlError(409, "PUBLICATION_EXECUTION_CONFLICT")
            return copy.deepcopy(value)
        generation = "generation_" + digest([job, epoch, execution, completion])
        table = "_gen_" + digest([self.native.configuration["space_id"], generation])[:40]
        state_table = None
        if value["state_contract"] is not None:
            state_table = "_state_" + digest([self.native.configuration["space_id"], generation])[:40]
        # Every generation stores the registered business schema; merge_keys
        # wire metadata is applied to the generation and its key state only.
        columns = descriptor["task"]["output"]["schema"]

        def create():
            current = self._active(job, epoch, attempt)
            if "selection" in current:
                if current["selection"] != plan:
                    raise ControlError(409, "PUBLICATION_EXECUTION_CONFLICT")
                return
            c = self.connection
            # Reservations bound unpublished candidate generations; a published
            # generation is a retained release, reclaimed only by retention.
            count, reserved = c.execute(
                "SELECT count(*),coalesce(sum(g.reserved_bytes),0) FROM warehouse.ctl.publication_generations g "
                "JOIN warehouse.ctl.publication_jobs j USING (job_id) "
                "WHERE json_extract_string(j.record,'$.status') NOT IN ('PUBLISHED','REVOKED')").fetchone()
            reservation = sum(file["bytes"] for file in combined)
            if count >= MAX_JOBS or reserved + reservation > MAX_GENERATION_BYTES:
                raise ControlError(429, "PUBLICATION_GENERATION_QUOTA")
            for name, schema in [(table, columns)] + (
                    [(state_table, current["state_contract"]["schema"])] if state_table else []):
                c.execute("CREATE TABLE warehouse." + DATA_SCHEMA + "." + quoted(name) + " (" +
                          ",".join(quoted(column["name"]) + " " + column["type"] +
                                   ("" if column["nullable"] else " NOT NULL") for column in schema) + ")")
            base = self._copy_base(current, table, state_table, columns)
            current.update(selection=plan, execution_id=execution, generation_id=generation, status="SELECTED",
                           base=base)
            c.execute("INSERT INTO warehouse.ctl.publication_generations VALUES (?,?,?,?,?,?,?)",
                      [generation, job, epoch, table, state_table, reservation, canonical(plan)])
            self._save(current)

        self.native.transaction(create, lambda: self._guard(value))
        return self.status(job)

    def _copy_base(self, value, table, state_table, columns):
        """Copy the unaffected part of the CAS base release into the new generation.

        Copying keeps every published release immutable and readable. The
        physical copy is a measured cost; incremental computation does not
        imply an incremental physical write.
        """
        import time

        descriptor = value["descriptor"]
        output, processing = descriptor["task"]["output"], descriptor["processing"]
        previous = descriptor["output_state"]["expected_previous_release"]
        started = time.monotonic()
        base = {"release_id": None, "copied_rows": 0, "copied_state_rows": 0}
        if output["write_mode"] == "versioned_replace" or previous is None or processing.get("bootstrap"):
            base["elapsed_ms"] = 0
            return base
        release = resolve_release(self.connection, self.native.configuration, output["dataset"], previous)
        target = "warehouse." + DATA_SCHEMA + "." + quoted(table)
        source = DATA_SCHEMA + "." + quoted(release["physical"]["table"])
        names = ",".join(quoted(column["name"]) for column in columns)
        where, parameters = "", []
        if output["write_mode"] == "replace_partitions":
            from colvenna_isle.query.native import _partition_expression, partition_text

            replaced = [partition_text(columns, part) for part in descriptor["output_state"]["replace_partitions"]]
            where = " WHERE " + _partition_expression(output["partition_columns"]) + " NOT IN (SELECT unnest(?::VARCHAR[]))"
            parameters = [replaced]
        self.connection.execute("INSERT INTO " + target + " SELECT " + names + " FROM warehouse." + source + where,
                                parameters)
        base["copied_rows"] = self.connection.execute("SELECT count(*) FROM " + target).fetchone()[0]
        if state_table is not None:
            state = value["state_contract"]["schema"]
            self.connection.execute(
                "INSERT INTO warehouse." + DATA_SCHEMA + "." + quoted(state_table) + " SELECT " +
                ",".join(quoted(column["name"]) for column in state) + " FROM warehouse." + DATA_SCHEMA + "." +
                quoted(release["state"]["physical"]["table"]))
            base["copied_state_rows"] = release["state"]["row_count"]
        base.update(release_id=release["release_id"], elapsed_ms=round((time.monotonic() - started) * 1000))
        return base

    def _image(self, value, image):
        if image == "data":
            return value["descriptor"]["task"]["output"], value["selection"]["files"]
        if image == "state" and self._complete_state(value):
            return value["state_contract"], value["selection"]["state_files"]
        raise ControlError(422, "PUBLICATION_IMAGE_SCOPE")

    def chunk(self, job, epoch, attempt, execution, file_number, chunk_number, rows, *, image="data"):
        value = self._active(job, epoch, attempt, execution)
        if "selection" not in value:
            raise ControlError(409, "PUBLICATION_EXECUTION_NOT_SELECTED")
        output, files = self._image(value, image)
        if (type(file_number) is not int or type(chunk_number) is not int
                or not 0 <= file_number < len(files) or chunk_number < 0):
            raise ControlError(422, "PUBLICATION_CHUNK_SCOPE")
        file = files[file_number]
        expected = min(CHUNK_ROWS, file["rows"] - chunk_number * CHUNK_ROWS)
        if expected <= 0 or len(rows) != expected:
            raise ControlError(422, "PUBLICATION_CHUNK_ROWS")
        mode = output.get("write_mode") if image == "data" else "versioned_replace"
        columns = wire_schema(output) if mode == "merge_keys" else output["schema"]
        key_columns = output["key_columns"]
        keys, changes = [], []
        for row in rows:
            if len(row) != len(columns):
                raise ControlError(422, "PUBLICATION_ROW_TYPE")
            for column, member in zip(columns, row):
                _native_value(column, member)
            values = {column["name"]: member for column, member in zip(columns, row)}
            keys.append(canonical([wire(values[name]) for name in key_columns]))
            changes.append(values)
        if mode == "replace_partitions":
            from colvenna_isle.query.native import partition_text, partition_value

            allowed = {partition_text(columns, part) for part in value["descriptor"]["output_state"]["replace_partitions"]}
            if any(partition_text(columns, {name: partition_value(change[name]) for name in output["partition_columns"]})
                   not in allowed for change in changes):
                raise ControlError(422, "PUBLICATION_PARTITION_SCOPE")
        content_hash = digest([[wire(member) for member in row] for row in rows])
        result = None

        def write():
            nonlocal result
            current = self._active(job, epoch, attempt, execution)
            generation = current["generation_id"]
            c = self.connection
            prior = c.execute("SELECT sha256,rows,commit_sequence FROM warehouse.ctl.publication_chunks "
                "WHERE generation_id=? AND image=? AND file_number=? AND chunk_number=?",
                [generation, image, file_number, chunk_number]).fetchone()
            if prior:
                if prior[:2] != (content_hash, len(rows)):
                    raise ControlError(409, "PUBLICATION_CHUNK_CONFLICT")
                result = {"sha256": prior[0], "rows": prior[1], "commit_sequence": prior[2]}
                return
            if current["status"] not in {"SELECTED", "IMPORTING"}:
                raise ControlError(409, "PUBLICATION_ALREADY_TERMINAL")
            if mode == "merge_keys":
                # Only an uncommitted chunk is checked against the evolving
                # generation key state; a committed original returns above.
                self._check_changes(current, output, changes)
            tables = dict(zip(["data", "state"], c.execute(
                "SELECT table_name,state_table_name FROM warehouse.ctl.publication_generations "
                "WHERE generation_id=?", [generation]).fetchone()))
            table = "warehouse." + DATA_SCHEMA + "." + quoted(tables[image])
            inserted = deleted = 0
            for row, key, change in zip(rows, keys, changes):
                if c.execute("SELECT count(*) FROM warehouse.ctl.generation_keys WHERE generation_id=? "
                             "AND image=? AND identity=?", [generation, image, key]).fetchone()[0]:
                    raise ControlError(422, "PUBLICATION_DUPLICATE_KEY")
                c.execute("INSERT INTO warehouse.ctl.generation_keys VALUES (?,?,?)", [generation, image, key])
                key_values = [change[name] for name in key_columns]
                if mode == "merge_keys":
                    state = "warehouse." + DATA_SCHEMA + "." + quoted(tables["state"])
                    deleted += len(c.execute("DELETE FROM " + table + " WHERE " + _key_match(key_columns) +
                                             " RETURNING 1", key_values).fetchall())
                    target = output["schema"]
                    if change["_colvenna_op"] == "UPSERT":
                        c.execute("INSERT INTO " + table + " VALUES (" + ",".join("?" for _ in target) + ")",
                                  [change[column["name"]] for column in target])
                        inserted += 1
                    c.execute("DELETE FROM " + state + " WHERE " + _key_match(key_columns), key_values)
                    c.execute("INSERT INTO " + state + " VALUES (" + ",".join("?" for _ in key_columns) + ",?,?,?)",
                              key_values + [change["_colvenna_version"], change["_colvenna_sequence"],
                                            change["_colvenna_op"] == "DELETE"])
                    continue
                if mode == "replace_partitions" and c.execute(
                        "SELECT count(*) FROM " + table + " WHERE " + _key_match(key_columns), key_values).fetchone()[0]:
                    # A key cannot appear in both a replaced and an unaffected partition.
                    raise ControlError(422, "PUBLICATION_DUPLICATE_KEY")
                c.execute("INSERT INTO " + table + " VALUES (" + ",".join("?" for _ in columns) + ")", list(row))
                inserted += 1
            sequence = c.execute("UPDATE warehouse.ctl.commit_sequence SET sequence=sequence+1 "
                                 "WHERE slot=1 RETURNING sequence").fetchone()[0]
            c.execute("INSERT INTO warehouse.ctl.publication_chunks VALUES (?,?,?,?,?,?,?)",
                      [generation, image, file_number, chunk_number, content_hash, len(rows), sequence])
            c.execute("INSERT INTO warehouse.ctl.publication_effects VALUES (?,?,?,?,?,?)",
                      [generation, image, file_number, chunk_number, inserted, deleted])
            current["status"] = "IMPORTING"
            self._save(current)
            result = {"sha256": content_hash, "rows": len(rows), "commit_sequence": sequence}

        self.native.transaction(write, lambda: self._guard(value))
        return result

    def _check_changes(self, value, output, changes):
        """Source-version metadata may only advance accepted key state inside the bound interval."""
        processing = value["descriptor"]["processing"]
        lower, upper = processing["from_watermark"]["ods_sequence"], processing["to_watermark"]["ods_sequence"]
        target = {column["name"]: column for column in output["schema"]}
        tables = self.connection.execute(
            "SELECT state_table_name FROM warehouse.ctl.publication_generations WHERE generation_id=?",
            [value["generation_id"]]).fetchone()
        state = "warehouse." + DATA_SCHEMA + "." + quoted(tables[0])
        for change in changes:
            if change["_colvenna_op"] not in {"UPSERT", "DELETE"}:
                raise ControlError(422, "PUBLICATION_CHANGE_OPERATION")
            if not lower < change["_colvenna_sequence"] <= upper or change["_colvenna_version"] < 0:
                raise ControlError(422, "PUBLICATION_CHANGE_OUTSIDE_INTERVAL")
            if change["_colvenna_op"] == "UPSERT" and any(
                    change[name] is None and not column["nullable"] for name, column in target.items()):
                raise ControlError(422, "PUBLICATION_UPSERT_INCOMPLETE")
            prior = self.connection.execute(
                "SELECT _colvenna_version,_colvenna_sequence FROM " + state + " WHERE " + _key_match(output["key_columns"]),
                [change[name] for name in output["key_columns"]]).fetchone()
            # A late older business version can neither resurrect a deleted key
            # nor overwrite newer accepted state, even if the script emits it.
            if prior and (change["_colvenna_version"], change["_colvenna_sequence"]) <= tuple(prior):
                raise ControlError(422, "PUBLICATION_STALE_CHANGE")

    def publish(self, job, epoch, attempt, execution):
        value = self._active(job, epoch, attempt, execution)
        if value["status"] == "PUBLISHED":
            return copy.deepcopy(value["release"])
        if "selection" not in value:
            raise ControlError(409, "PUBLICATION_EXECUTION_NOT_SELECTED")
        result = None

        def change():
            nonlocal result
            current = self._active(job, epoch, attempt, execution)
            descriptor = current["descriptor"]
            output = descriptor["task"]["output"]
            target = output["dataset"]
            prior = resolve_release(self.connection, self.native.configuration, target)
            if (prior["release_id"] if prior else None) != descriptor["output_state"]["expected_previous_release"]:
                raise ControlError(409, "PUBLICATION_PREVIOUS_RELEASE_CONFLICT")
            generation = current["generation_id"]
            c = self.connection
            observed = c.execute(
                "SELECT image,file_number,chunk_number,rows FROM warehouse.ctl.publication_chunks "
                "WHERE generation_id=? ORDER BY image,file_number,chunk_number", [generation]).fetchall()
            images = ["data"] + (["state"] if self._complete_state(current) else [])
            expected = [(image, number, chunk, min(CHUNK_ROWS, file["rows"] - chunk * CHUNK_ROWS))
                        for image in images
                        for number, file in enumerate(self._image(current, image)[1])
                        for chunk in range(math.ceil(file["rows"] / CHUNK_ROWS))]
            if observed != expected:
                raise ControlError(409, "PUBLICATION_CHUNKS_INCOMPLETE")
            tables = dict(zip(["data", "state"], c.execute(
                "SELECT table_name,state_table_name FROM warehouse.ctl.publication_generations "
                "WHERE generation_id=?", [generation]).fetchone()))
            effects = dict(((row[0]), row[1:]) for row in c.execute(
                "SELECT image,sum(inserted),sum(deleted) FROM warehouse.ctl.publication_effects "
                "WHERE generation_id=? GROUP BY image", [generation]).fetchall())
            base = current.get("base", {"copied_rows": 0, "copied_state_rows": 0})
            metadata = {}
            for image in ["data"] + (["state"] if current["state_contract"] is not None else []):
                contract = output if image == "data" else current["state_contract"]
                table = tables[image]
                count = c.execute("SELECT count(*) FROM warehouse." + DATA_SCHEMA + "." + quoted(table)).fetchone()[0]
                inserted, deleted = effects.get(image, (0, 0)) if image in images else (0, 0)
                copied = base["copied_rows"] if image == "data" else base["copied_state_rows"]
                if image == "state" and output["write_mode"] == "merge_keys":
                    known = c.execute("SELECT count(*) FROM warehouse.ctl.generation_keys WHERE generation_id=? "
                                      "AND image='data' AND identity IS NOT NULL", [generation]).fetchone()[0]
                    if count < copied or count > copied + known:
                        raise ControlError(503, "PUBLICATION_GENERATION_CUSTODY_UNKNOWN")
                elif count != copied + (inserted or 0) - (deleted or 0):
                    raise ControlError(503, "PUBLICATION_GENERATION_CUSTODY_UNKNOWN")
                if count == 0 and not contract["allow_empty"]:
                    raise ControlError(422, "PUBLICATION_EMPTY_TARGET_FORBIDDEN")
                metadata[image] = {"row_count": count, "physical": {"schema": DATA_SCHEMA, "table": table},
                    "physical_sha256": digest(physical(c, DATA_SCHEMA + "." + table))}
            state = None
            if "state" in metadata:
                state = {**metadata["state"], "generation_id": generation,
                    "contract_sha256": digest(current["state_contract"]),
                    **{name: current["state_contract"][name] for name in ["protocol", "schema", "key_columns"]}}
            details = {"write_mode": output["write_mode"], "processing": copy.deepcopy(descriptor["processing"]),
                       "base": copy.deepcopy(base)}
            if output["write_mode"] == "merge_keys":
                inserted, deleted = effects.get("data", (0, 0))
                details["change_summary"] = {"changes": sum(file["rows"] for file in current["selection"]["files"]),
                                             "inserted_rows": inserted or 0, "deleted_rows": deleted or 0}
            if output["write_mode"] == "replace_partitions":
                details["partitions"] = self._partitions(current, prior, tables["data"])
            sequence = c.execute("UPDATE warehouse.ctl.commit_sequence SET sequence=sequence+1 "
                                 "WHERE slot=1 RETURNING sequence").fetchone()[0]
            manifest_inputs = current["authority"]["input_manifest"].get("inputs", {})
            result = {"status": "PUBLISHED", "release_id": "release_" + digest(
                [generation, current["selection"], current["definition_sha256"]]),
                **{name: descriptor[name] for name in ["space_id", "generation", "definition_epoch", "pipeline_id",
                    "pipeline_version", "run_id", "job_id", "attempt_id"]},
                "task_id": descriptor["task"]["task_id"], "execution_id": execution, "eligibility_epoch": epoch,
                "dataset": target, "schema": output["schema"], "published_unix": time.time(),
                "definition_sha256": current["definition_sha256"], "commit_sequence": sequence,
                **metadata["data"], "state": state, **details,
                "input_watermarks": {alias: {"dataset": binding["dataset"], "release_id": binding["release_id"],
                    "watermark": binding["watermark"], "manifest_sha256": binding["manifest_sha256"],
                    "selection": binding["selection"]["mode"]}
                    for alias, binding in descriptor["inputs"].items()},
                # The native version each fixed input artifact was exported from.
                "input_versions": {alias: copy.deepcopy(value.get("native_version"))
                                   for alias, value in sorted(manifest_inputs.items())},
                "parameters_sha256": digest(descriptor["parameters"]),
                "task_sha256": digest(descriptor["task"]),
                "expected_previous_release": descriptor["output_state"]["expected_previous_release"],
                "completion": current["selection"]["completion"]}
            c.execute("INSERT INTO warehouse.ctl.dataset_releases VALUES (?,?,?)",
                      [result["release_id"], target, canonical(result)])
            c.execute("INSERT INTO warehouse.ctl.release_pointers VALUES (?,?) "
                      "ON CONFLICT(dataset) DO UPDATE SET release_id=excluded.release_id",
                      [target, result["release_id"]])
            c.execute("INSERT INTO warehouse.ctl.state_pointers VALUES (?,?,?) "
                      "ON CONFLICT(dataset) DO UPDATE SET release_id=excluded.release_id,state_record=excluded.state_record",
                      [target, result["release_id"], canonical(state)])
            c.execute("INSERT INTO warehouse.ctl.pipeline_task_watermarks VALUES (?,?,?) "
                      "ON CONFLICT(pipeline_id,task_id) DO UPDATE SET record=excluded.record",
                      [result["pipeline_id"], result["task_id"], canonical(result)])
            current.update(status="PUBLISHED", release=result)
            self._save(current)

        self.native.transaction(change, lambda: self._guard(value))
        return copy.deepcopy(result)

    # -- retention -------------------------------------------------------------------------------

    def _collections(self):
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS warehouse.ctl.retention_collections (kind VARCHAR,identity VARCHAR,"
            "collection_id VARCHAR NOT NULL,record VARCHAR NOT NULL,PRIMARY KEY(kind,identity))")

    def protected_releases(self):
        """Releases still read by current pointers or by any task's current watermark."""
        import json

        c = self.connection
        protected = {row[0] for row in c.execute("SELECT release_id FROM warehouse.ctl.release_pointers").fetchall()}
        protected |= {row[0] for row in c.execute("SELECT release_id FROM warehouse.ctl.state_pointers").fetchall()}
        for (record,) in c.execute("SELECT record FROM warehouse.ctl.pipeline_task_watermarks").fetchall():
            value = json.loads(record)
            protected.add(value.get("release_id"))
            protected.add(value.get("expected_previous_release"))
            protected.add((value.get("processing") or {}).get("state_release_id"))
            for binding in (value.get("input_watermarks") or {}).values():
                protected.add(binding.get("release_id"))
        protected.discard(None)
        return protected

    def retention_candidates(self):
        """Every release and every unpublished candidate generation, with native custody state."""
        import json

        self.initialize()
        c = self.connection
        current = dict(c.execute("SELECT dataset,release_id FROM warehouse.ctl.release_pointers").fetchall())
        releases = []
        for release_id, dataset, record in c.execute(
                "SELECT release_id,dataset,record FROM warehouse.ctl.dataset_releases ORDER BY dataset").fetchall():
            value = json.loads(record)
            releases.append({"release_id": release_id, "dataset": dataset, "status": value["status"],
                             "commit_sequence": value["commit_sequence"], "published_unix": value.get("published_unix"),
                             "row_count": value.get("row_count"), "current": current.get(dataset) == release_id,
                             "run_id": value.get("run_id"), "pipeline_id": value.get("pipeline_id"),
                             "pipeline_version": value.get("pipeline_version")})
        generations = []
        for generation, job, epoch, table, state_table, reserved, record in c.execute(
                "SELECT g.generation_id,g.job_id,g.epoch,g.table_name,g.state_table_name,g.reserved_bytes,j.record "
                "FROM warehouse.ctl.publication_generations g JOIN warehouse.ctl.publication_jobs j USING (job_id) "
                "ORDER BY g.generation_id").fetchall():
            value = json.loads(record)
            superseded = value.get("generation_id") != generation and epoch < value["epoch"]
            leftover = value["status"] == "REVOKED" or superseded
            generations.append({"generation_id": generation, "job_id": job, "job_status": value["status"],
                                "epoch": epoch, "reserved_bytes": reserved, "leftover": leftover,
                                "reason": "REVOKED_CANDIDATE" if value["status"] == "REVOKED"
                                else "SUPERSEDED_CANDIDATE" if superseded else None})
        self._collections_present = _table(c, "retention_collections")
        collected = set()
        if self._collections_present:
            collected = {row[0] for row in c.execute(
                "SELECT identity FROM warehouse.ctl.retention_collections WHERE kind='generation'").fetchall()}
        generations = [value for value in generations if value["generation_id"] not in collected]
        return {"releases": releases, "generations": generations, "protected": sorted(self.protected_releases())}

    def collect(self, collection, release_ids, generation_ids, protected, guard):
        """Drop retired native generations and record evidence in one native transaction."""
        import json

        self.initialize()
        evidence = []

        def change():
            c = self.connection
            self._collections()
            pinned = set(protected) | self.protected_releases()
            for release_id in release_ids:
                validate("identifier", release_id)
                row = c.execute("SELECT dataset,record FROM warehouse.ctl.dataset_releases WHERE release_id=?",
                                [release_id]).fetchone()
                if row is None:
                    raise ControlError(404, "PUBLICATION_RELEASE_NOT_FOUND")
                value = json.loads(row[1])
                if value["status"] == "COLLECTED":
                    continue
                if release_id in pinned or value["status"] != "PUBLISHED":
                    raise ControlError(409, "PUBLICATION_RELEASE_RETAINED")
                tables = [value["physical"]["table"]] + ([value["state"]["physical"]["table"]]
                                                         if value.get("state") else [])
                for table in tables:
                    validate("identifier", table)
                    c.execute("DROP TABLE IF EXISTS warehouse." + DATA_SCHEMA + "." + quoted(table))
                value.update(status="COLLECTED", collection={"collection_id": collection, "collected_unix": time.time()})
                c.execute("UPDATE warehouse.ctl.dataset_releases SET record=? WHERE release_id=?",
                          [canonical(value), release_id])
                entry = {"kind": "release", "identity": release_id, "dataset": row[0], "tables": tables,
                         "row_count": value.get("row_count"), "commit_sequence": value["commit_sequence"]}
                c.execute("INSERT INTO warehouse.ctl.retention_collections VALUES (?,?,?,?)",
                          ["release", release_id, collection, canonical(entry)])
                evidence.append(entry)
            for generation in generation_ids:
                validate("identifier", generation)
                row = c.execute(
                    "SELECT g.job_id,g.epoch,g.table_name,g.state_table_name,j.record FROM "
                    "warehouse.ctl.publication_generations g JOIN warehouse.ctl.publication_jobs j USING (job_id) "
                    "WHERE g.generation_id=?", [generation]).fetchone()
                if row is None:
                    raise ControlError(404, "PUBLICATION_GENERATION_NOT_FOUND")
                job = json.loads(row[4])
                superseded = job.get("generation_id") != generation and row[1] < job["epoch"]
                if job["status"] != "REVOKED" and not superseded:
                    raise ControlError(409, "PUBLICATION_GENERATION_RETAINED")
                if c.execute("SELECT count(*) FROM warehouse.ctl.retention_collections WHERE kind='generation' "
                             "AND identity=?", [generation]).fetchone()[0]:
                    continue
                tables = [name for name in [row[2], row[3]] if name]
                for table in tables:
                    validate("identifier", table)
                    c.execute("DROP TABLE IF EXISTS warehouse." + DATA_SCHEMA + "." + quoted(table))
                entry = {"kind": "generation", "identity": generation, "job_id": row[0], "tables": tables,
                         "reason": "REVOKED_CANDIDATE" if job["status"] == "REVOKED" else "SUPERSEDED_CANDIDATE"}
                c.execute("INSERT INTO warehouse.ctl.retention_collections VALUES (?,?,?,?)",
                          ["generation", generation, collection, canonical(entry)])
                evidence.append(entry)

        self.native.transaction(change, guard)
        return evidence

    def _partitions(self, value, prior, table):
        """Every partition's rows/hash and the release whose computation produced it."""
        from colvenna_isle.query.native import partition_text

        output = value["descriptor"]["task"]["output"]
        replaced = {partition_text(output["schema"], part): part
                    for part in value["descriptor"]["output_state"]["replace_partitions"]}
        current = partition_summary(self.connection, "warehouse." + DATA_SCHEMA + "." + quoted(table),
                                    output["schema"], output["partition_columns"])
        mapped = prior is not None and "partitions" in prior
        origins = {entry["key"]: entry for entry in (prior or {}).get("partitions", [])}
        result = []
        for key in sorted(set(current) | set(replaced)):
            entry = current.get(key, {"partition": replaced.get(key), "rows": 0, "sha256": None})
            if key in replaced:
                origin = None  # Produced by this release.
            elif not mapped:
                # A full prior release has no partition map; its copied rows
                # originate from that complete release.
                origin = prior["release_id"]
            else:
                previous = origins.get(key)
                if previous is None or previous["sha256"] != entry["sha256"] or previous["rows"] != entry["rows"]:
                    raise ControlError(503, "PUBLICATION_PARTITION_CUSTODY_UNKNOWN")
                origin = previous["origin_release_id"] or prior["release_id"]
            result.append({"key": key, "partition": entry["partition"], "rows": entry["rows"],
                           "sha256": entry["sha256"], "origin_release_id": origin,
                           "replaced": key in replaced})
        return result

    def _objects(self):
        bucket = self.native.configuration["database_uri"].removeprefix("s3://").split("/", 1)[0]
        return TaskObjects(None, bucket, self.native.configuration["space_id"],
                           self.native.configuration["generation"])
