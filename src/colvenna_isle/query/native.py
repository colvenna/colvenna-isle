"""Server-only native reads; arbitrary SQL runs on the separate sealed snapshot."""

import copy
import hashlib
import json
import re
from pathlib import Path

from colvenna_isle.region import region
from colvenna_isle.contracts.validation import validate
from colvenna_isle.control.registry import ControlError, canonical
from colvenna_isle.pipeline.publications import resolve_release
from colvenna_isle.writer.database import native_uri
from colvenna_isle.writer.ingestion import digest, quoted
from colvenna_isle.writer.native_schema import physical


# Pipeline inputs keep ODS event semantics: the original operation and the
# committed sequence used for watermark intervals and source-version ordering.
ODS_INPUT_METADATA = [{"name": "_colvenna_operation", "type": "VARCHAR", "nullable": False},
                      {"name": "_colvenna_commit_sequence", "type": "BIGINT", "nullable": False}]

# Rows one query may stage across all the datasets its SQL names. They are
# counted in the same read transaction before any copy, so a larger request
# fails at once (QUERY_STAGING_LIMIT) instead of at the query timeout. The
# 1 GiB sandbox bound (QUERY_SNAPSHOT_TOO_LARGE) still applies to what is copied.
STAGING_MAX_ROWS = 2000000


def partition_text(schema, partition):
    """Encode one exact partition identically to the native _partition_expression.

    Each value is length-prefixed, so no value content can collide with
    another partition's encoding. Large BIGINT values may be decimal strings.
    """
    from colvenna_isle.contracts.validation import validate_value

    columns = {column["name"]: column for column in schema}
    values = []
    for name in sorted(partition):
        column, value = columns.get(name), partition[name]
        if column is None or column["type"] not in {"VARCHAR", "BIGINT", "DATE", "BOOLEAN"}:
            raise ControlError(422, "INPUT_PARTITION_COLUMN")
        if column["type"] == "BIGINT" and (type(value) is int or isinstance(value, str) and re.fullmatch(r"-?[0-9]{1,19}", value)):
            value = int(value)
            if not -(2 ** 63) <= value < 2 ** 63:
                raise ControlError(422, "INPUT_PARTITION_COLUMN")
        else:
            value = validate_value(column["type"], value, False)
        text = ("true" if value is True else "false" if value is False else
                value.isoformat() if column["type"] == "DATE" else str(value))
        values.append(str(len(text)) + ":" + text)
    return ",".join(values)


def partition_value(value):
    """JSON-safe partition values: dates as ISO text, unsafe integers as decimal strings."""
    from datetime import date

    if isinstance(value, date):
        return value.isoformat()
    if type(value) is int and abs(value) > 9007199254740991:
        return str(value)
    return value


def _partition_expression(names):
    parts = ["CAST(" + quoted(name) + " AS VARCHAR)" for name in sorted(names)]
    return "concat_ws(','," + ",".join("CAST(length(" + part + ") AS VARCHAR) || ':' || " + part
                                        for part in parts) + ")"


def stage(connection, configuration, target, guard, *, consistency="current", release_id=None,
          after=None, maximum_rows=None, batch_rows=512, selections=None, datasets=None):
    return _stage(connection, configuration, target, guard,
                  consistency=consistency, release_id=release_id, after=after,
                  maximum_rows=maximum_rows, batch_rows=batch_rows, selections=selections,
                  datasets=datasets)[0]


def describe(connection, configuration, guard):
    return _stage(connection, configuration, None, guard)


def _stage(connection, configuration, target, guard, *, consistency="current", release_id=None,
           after=None, maximum_rows=None, batch_rows=512, selections=None, datasets=None):
    """Copy only registered, native-backed business columns inside a read transaction.

    datasets (queries) names the registered datasets the SQL references; only
    those are copied, within STAGING_MAX_ROWS. The receipt's dataset and an exact
    release's dataset are versioned in the same transaction even when the SQL
    names none of their rows. None (catalog, pipeline inputs) covers every
    dataset of the configuration.

    selections (pipeline input exports only) fixes each dataset's exact version:
    an ODS commit interval, a named release, a partition set or the target's key
    state. Absent datasets keep the query semantics of the current version.

    A target with bulk() receives each copy as one private Parquet file written
    inside this transaction; other targets receive row batches.
    """
    import duckdb

    if consistency not in {"current", "published"} or release_id is not None and consistency != "published":
        raise ControlError(400, "INVALID_QUERY_CONSISTENCY")
    if release_id is not None:
        validate("identifier", release_id)
    if after is not None and consistency != "current":
        raise ControlError(400, "INVALID_QUERY_CONSISTENCY")
    if maximum_rows is not None and (type(maximum_rows) is not int or maximum_rows < 1):
        raise ControlError(400, "INVALID_QUERY_SOURCE_QUOTA")
    if type(batch_rows) is not int or not 1 <= batch_rows <= 512:
        raise ControlError(400, "INVALID_QUERY_SOURCE_QUOTA")
    guard()
    c = connection
    try:
        c.execute('BEGIN')
        identity = c.execute('SELECT space_id,generation,format_version FROM warehouse.ctl.space_identity '
                             'WHERE slot=1').fetchone()
        if identity != (configuration['space_id'], configuration['generation'], 6):
            raise ControlError(503, 'NATIVE_SPACE_IDENTITY_MISMATCH')
        sequence = c.execute('SELECT sequence FROM warehouse.ctl.commit_sequence WHERE slot=1').fetchone()
        if not sequence or type(sequence[0]) is not int or sequence[0] < 0:
            raise ControlError(503, 'NATIVE_VERSION_UNAVAILABLE')
        table_names = {schema+'.'+table for schema, table in c.execute(
            "SELECT schema_name,table_name FROM duckdb_tables() WHERE database_name='warehouse'").fetchall()}
        receipt = _committed_receipt(c, configuration, after, table_names) if after else None
        requested_dataset = None
        if release_id is not None:
            row = (c.execute("SELECT dataset FROM warehouse.ctl.dataset_releases WHERE release_id=?",
                             [release_id]).fetchone() if "ctl.dataset_releases" in table_names else None)
            if row is None:
                raise ControlError(503, "PUBLICATION_RELEASE_NOT_READY")
            requested_dataset = row[0]
            registered = configuration["datasets"].get(requested_dataset)
            if not registered or registered["definition"].get("write_owner") != "pipeline":
                raise ControlError(403, "PUBLICATION_TARGET_FORBIDDEN")
        named = None if datasets is None else set(datasets) & set(configuration['datasets'])
        wanted = None if named is None else named | {value for value in [
            receipt and receipt['dataset'], requested_dataset] if value}
        catalog, versions, copies = [], [], []
        for dataset, registered in sorted(configuration['datasets'].items()):
            if wanted is not None and dataset not in wanted:
                continue
            guard()
            definition = registered['definition']
            validate('dataset_registration', definition)
            if (definition['dataset'] != dataset or hashlib.sha256(canonical(definition).encode()).hexdigest()
                    != registered['definition_sha256']):
                raise ControlError(503, 'NATIVE_SCHEMA_IDENTITY_MISMATCH')
            owner = definition.get('write_owner', 'ingestion' if dataset.startswith('ods.') else 'pipeline')
            spec = (selections or {}).get(dataset)
            observed_release, source, extra, where, parameters = None, dataset, {}, [], []
            columns_override = None
            if spec is not None and spec['mode'] != 'snapshot' and owner == 'direct':
                raise ControlError(422, 'INPUT_SELECTION_UNSUPPORTED')
            if owner == 'pipeline':
                if spec is not None and spec['mode'] == 'changes':
                    raise ControlError(422, 'INPUT_SELECTION_UNSUPPORTED')
                if spec is not None and 'release_id' in spec:
                    release = (resolve_release(c, configuration, dataset, spec['release_id'])
                               if spec['release_id'] is not None else None)
                else:
                    release = resolve_release(c, configuration, dataset,
                        release_id if requested_dataset == dataset else None)
                if release is None and (spec is None or spec['mode'] != 'affected_keys'):
                    if spec is not None:
                        raise ControlError(409, 'INPUT_RELEASE_NOT_PUBLISHED')
                    continue
                if spec is not None and spec['mode'] == 'affected_keys':
                    contract = definition.get('publication_state')
                    if not contract or contract['protocol'] != 'key_state_v1':
                        raise ControlError(422, 'INPUT_KEY_STATE_UNAVAILABLE')
                    columns_override = contract['schema']
                    observed_release = release['release_id'] if release else None
                    watermark = release['commit_sequence'] if release else 0
                    extra['state_release_id'] = observed_release
                    if release is None:
                        source = None
                    else:
                        source = release['state']['physical']['schema'] + '.' + release['state']['physical']['table']
                        keys = spec['keys_from']
                        ods_table = 'warehouse.ods.' + quoted(keys['dataset'].split('.', 1)[1])
                        names = ','.join(quoted(name) for name in contract['key_columns'])
                        where.append('(' + names + ') IN (SELECT ' + names + ' FROM ' + ods_table +
                                     ' WHERE _colvenna_commit_sequence > ? AND _colvenna_commit_sequence <= ?)')
                        parameters += [keys['from_sequence'], keys['to_sequence']]
                else:
                    observed_release, watermark = release['release_id'], release['commit_sequence']
                    source = release['physical']['schema'] + '.' + release['physical']['table']
            elif consistency == "published":
                continue
            elif dataset not in table_names:
                # Registered but never written: an empty dataset at sequence 0, never an absent one.
                if owner not in {'direct', 'ingestion'}:
                    raise ControlError(503, 'NATIVE_WRITE_OWNER_UNAVAILABLE')
                if spec is not None and (spec['mode'] not in {'snapshot', 'changes'}
                                         or owner == 'direct' and spec['mode'] != 'snapshot'):
                    raise ControlError(422, 'INPUT_SELECTION_UNSUPPORTED')
                watermark, source = 0, None
                # The same nullability as the physical table its first write creates.
                required = set(definition['key_columns'])
                if owner == 'ingestion':
                    required |= {definition['ingestion']['event_id_column'],
                                 definition['ingestion'].get('source_version_column')}
                columns_override = [{**column, 'nullable': column['nullable'] if owner == 'direct'
                                     else column['name'] not in required} for column in definition['schema']]
                if spec is not None and owner == 'ingestion':
                    bound = spec.get('to_sequence')
                    if bound is not None:
                        if bound > sequence[0]:
                            raise ControlError(409, 'INPUT_WATERMARK_NOT_COMMITTED')
                        watermark = bound
                    if spec['mode'] == 'changes':
                        extra['from_sequence'] = spec['from_sequence']
                    extra['to_sequence'] = watermark
                    columns_override += copy.deepcopy(ODS_INPUT_METADATA)
            elif owner == 'direct':
                prior = c.execute('SELECT definition_sha256,physical_sha256 FROM '
                    'warehouse.ctl.direct_table_definitions WHERE dataset=?', [dataset]).fetchone()
                if not prior or prior != (registered['definition_sha256'], digest(physical(c, dataset))):
                    raise ControlError(503, 'NATIVE_SCHEMA_IDENTITY_MISMATCH')
                watermark = c.execute('SELECT commit_sequence FROM warehouse.ctl.dataset_watermarks '
                    'WHERE dataset=?', [dataset]).fetchone()
                watermark = watermark[0] if watermark else 0
            elif owner == 'ingestion':
                prior = c.execute('SELECT definition_sha256 FROM warehouse.ctl.dataset_definitions '
                    'WHERE dataset=?', [dataset]).fetchone()
                if prior != (registered['definition_sha256'],):
                    raise ControlError(503, 'NATIVE_SCHEMA_IDENTITY_MISMATCH')
                watermark = c.execute('SELECT coalesce(max(commit_sequence),0) FROM warehouse.ctl.ingestions '
                    "WHERE json_extract_string(receipt,'$.dataset')=?", [dataset]).fetchone()[0]
                if spec is not None:
                    if spec['mode'] not in {'snapshot', 'changes'}:
                        raise ControlError(422, 'INPUT_SELECTION_UNSUPPORTED')
                    bound = spec.get('to_sequence')
                    if bound is not None:
                        # A bound above the native snapshot cannot be fixed yet.
                        if bound > sequence[0]:
                            raise ControlError(409, 'INPUT_WATERMARK_NOT_COMMITTED')
                        where.append('_colvenna_commit_sequence <= ?')
                        parameters.append(bound)
                        watermark = bound
                    if spec['mode'] == 'changes':
                        where.append('_colvenna_commit_sequence > ?')
                        parameters.append(spec['from_sequence'])
                        extra['from_sequence'] = spec['from_sequence']
                    extra['to_sequence'] = watermark
            else:
                raise ControlError(503, 'NATIVE_WRITE_OWNER_UNAVAILABLE')
            if type(watermark) is not int or not 0 <= watermark <= sequence[0]:
                raise ControlError(503, 'NATIVE_VERSION_UNAVAILABLE')
            if spec is not None and spec['mode'] == 'affected_partitions':
                names = list(spec['partitions'][0]) if spec['partitions'] else definition['partition_columns']
                if not names or any(sorted(part) != sorted(names) for part in spec['partitions']):
                    raise ControlError(422, 'INPUT_PARTITION_COLUMN')
                encoded = sorted({partition_text(definition['schema'], part) for part in spec['partitions']})
                where.append(_partition_expression(names) + ' IN (SELECT unnest(?::VARCHAR[]))')
                parameters.append(encoded)
                extra['partitions_sha256'] = digest(encoded)
                extra['partition_count'] = len(encoded)
            if source is None:
                columns = columns_override
                query_definition = {**copy.deepcopy(definition), 'schema': copy.deepcopy(columns)}
                version = {'dataset': dataset, 'release_id': observed_release, 'commit_sequence': watermark,
                           'snapshot_sequence': sequence[0], 'generation': configuration['generation'], **extra}
                catalog.append({'dataset': dataset, 'schema': query_definition['schema']})
                versions.append(version)
                copies.append((query_definition, version, None))
                continue
            schema, table = source.split('.')
            columns = columns_override or definition['schema']
            actual = c.execute('SELECT column_name,data_type,is_nullable FROM duckdb_columns() WHERE '
                "database_name='warehouse' AND schema_name=? AND table_name=? ORDER BY column_index",
                [schema, table]).fetchall()
            expected = [(column['name'], str(duckdb.sqltype(column['type']))) for column in columns]
            if owner == 'ingestion':
                expected += [('_colvenna_ingestion_id', 'VARCHAR'), ('_colvenna_commit_sequence', 'BIGINT'),
                    ('_colvenna_operation', 'VARCHAR'), ('_colvenna_disposition', 'VARCHAR'),
                    ('_colvenna_received_at', 'TIMESTAMP WITH TIME ZONE')]
            if [(name, kind) for name, kind, nullable in actual] != expected:
                raise ControlError(503, 'NATIVE_QUERY_SCHEMA_CHANGED')
            query_definition = copy.deepcopy(definition)
            query_definition['schema'] = copy.deepcopy(columns)
            # ODS sparse delete events can have NULL in business columns whose
            # ingestion registration requires values on append/upsert. Describe
            # the actual query table, rather than that input validation rule.
            for column, (_, _, nullable) in zip(query_definition['schema'], actual):
                column['nullable'] = nullable
            selected = [column['name'] for column in columns]
            if spec is not None and owner == 'ingestion':
                query_definition['schema'] += copy.deepcopy(ODS_INPUT_METADATA)
                selected += [column['name'] for column in ODS_INPUT_METADATA]
            version = {'dataset': dataset, 'release_id': observed_release,
                'commit_sequence': watermark, 'snapshot_sequence': sequence[0],
                'generation': configuration['generation'], **extra}
            versions.append(version)
            if named is not None and dataset not in named:
                copies.append((None, version, None))
                continue
            catalog.append({'dataset': dataset, 'schema': query_definition['schema']})
            relation = (' FROM warehouse.' + quoted(schema) + '.' + quoted(table)
                        + (' WHERE ' + ' AND '.join(where) if where else ''))
            order = (' ORDER BY _colvenna_commit_sequence, ' + quoted(definition['ingestion']['event_id_column'])
                     if spec is not None and owner == 'ingestion' else '')
            copies.append((query_definition, version, (
                'SELECT ' + ','.join(quoted(name) for name in selected) + relation + order
                + (" LIMIT " + str(maximum_rows + 1) if maximum_rows is not None else ""),
                'SELECT count(*)' + relation, parameters)))
        # Only the datasets the SQL names decide publication availability.
        if consistency == "published" and (not catalog if named is None
                                           else named - {value['dataset'] for value in catalog}):
            raise ControlError(503, "PUBLICATION_NOT_AVAILABLE")
        if release_id is not None and not any(version["release_id"] == release_id for version in versions):
            raise ControlError(503, "PUBLICATION_RELEASE_NOT_READY")
        if receipt is not None and not any(version["dataset"] == receipt["dataset"]
                and version["commit_sequence"] >= receipt["commit_sequence"]
                and version["snapshot_sequence"] >= receipt["commit_sequence"] for version in versions):
            raise ControlError(503, "COMMIT_WATERMARK_UNCONFIRMED")
        if named is not None and target is not None:
            staged = 0
            for _, _, read in copies:
                if read is not None:
                    guard()
                    staged += c.execute(read[1], read[2]).fetchone()[0]
            if staged > STAGING_MAX_ROWS:
                raise ControlError(413, 'QUERY_STAGING_LIMIT')
            if maximum_rows is not None and staged > maximum_rows:
                raise ControlError(413, 'QUERY_SOURCE_QUOTA')
        bulk = getattr(target, 'bulk', None)
        preflight = getattr(target, 'preflight', None)
        if preflight is not None:
            preflight(c, copies)
        for query_definition, version, read in copies if target is not None else ():
            guard()
            if query_definition is None:
                target.observe(version)
            elif read is None:
                target.load(query_definition, iter(()), version)
            elif bulk is not None:
                def export(path):
                    c.execute('COPY (' + read[0] + ") TO '" + str(path).replace("'", "''")
                              + "' (FORMAT parquet)", read[2])
                    guard()

                bulk(query_definition, export, version)
            else:
                cursor = c.execute(read[0], read[2])

                def batches():
                    while True:
                        batch = cursor.fetchmany(batch_rows)
                        if not batch:
                            break
                        guard()
                        yield batch

                target.load(query_definition, batches(), version)
        guard()
        c.execute('COMMIT')
    except BaseException:
        try:
            c.execute('ROLLBACK')
        except BaseException:
            raise ControlError(503, "NATIVE_READ_CLOSURE_UNKNOWN") from None
        raise
    if target is not None:
        target.seal()
    guard()
    return catalog, versions


def _committed_receipt(connection, configuration, after, tables):
    """Authority and data belong to the same Native read transaction."""
    from colvenna_isle.control.status import validate_observation

    kind, identity = after["kind"], after["identity"]
    if kind == "write_id":
        row = (connection.execute("SELECT principal_id,receipt FROM warehouse.ctl.writes WHERE write_id=?",
                                  [identity]).fetchone() if "ctl.writes" in tables else None)
        if row and row[0] != after["principal_id"] and not after["admin"]:
            raise ControlError(403, "WRITE_OWNER_REQUIRED")
        raw = row[1] if row else None
    else:
        row = (connection.execute("SELECT receipt FROM warehouse.ctl.ingestions WHERE ingestion_id=?",
                                  [identity]).fetchone() if "ctl.ingestions" in tables else None)
        raw = row[0] if row else None
    if raw is None:
        raise ControlError(503, "COMMIT_RECEIPT_UNCONFIRMED")
    try:
        value = json.loads(raw)
        if kind == "write_id":
            validate("write_receipt", value)
        else:
            validate_observation(value, configuration["space_id"], configuration["generation"], identity)
            if value["principal_id"] != after["principal_id"] and not after["admin"]:
                raise ControlError(403, "INGESTION_OWNER_REQUIRED")
        dataset = value["dataset"]
        registered = configuration["datasets"].get(dataset)
        if (value[kind] != identity or value["space_id"] != configuration["space_id"]
                or value["generation"] != configuration["generation"]
                or not registered or registered["definition"].get("write_owner",
                    "ingestion" if dataset.startswith("ods.") else "pipeline")
                != ("direct" if kind == "write_id" else "ingestion")
                or kind == "write_id" and value["schema_version"] != registered["definition"]["schema_version"]):
            raise ValueError("Receipt target differs")
        if value["status"] != "COMMITTED":
            raise ControlError(409, "COMMIT_RECEIPT_NOT_COMMITTED")
        if type(value["commit_sequence"]) is not int or value["commit_sequence"] <= 0:
            raise ValueError("Commit sequence differs")
        return value
    except ControlError:
        raise
    except Exception:
        raise ControlError(503, "COMMIT_RECEIPT_UNCONFIRMED") from None


class Reader:
    """One fresh process opens exactly one approved native generation."""

    def __init__(self, configuration, directory, guard):
        import duckdb

        validate('space_configuration', configuration)
        guard()
        artifacts = Path('/opt/colvenna/artifacts')
        manifest = json.loads((artifacts/'manifest.json').read_text())
        if duckdb.__version__ != manifest['duckdb_version']:
            raise ControlError(503, 'NATIVE_PACKAGE_IDENTITY_MISMATCH')
        for name, expected in manifest['files'].items():
            path = artifacts/name
            if path.parent != artifacts or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
                raise ControlError(503, 'NATIVE_EXTENSION_IDENTITY_MISMATCH')
        self.configuration, self.guard = configuration, guard
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=False)
        self.connection = duckdb.connect(config={'allow_unsigned_extensions': True,
            'autoload_known_extensions': False, 'autoinstall_known_extensions': False})
        try:
            c = self.connection
            for name in ['httpfs', 'aws', 'duckdb_object_storage']:
                c.execute("LOAD '"+str(artifacts/(name+'.duckdb_extension'))+"'")
            for command in ["SET threads=2", "SET memory_limit='256MB'", "SET TimeZone='UTC'",
                    'SET duckdb_objfs_memory_cache_size=67108864',
                    'SET duckdb_objfs_metadata_cache_size=16777216', "SET max_temp_directory_size='256MB'"]:
                c.execute(command)
            c.execute('SET temp_directory=?', [str(directory)])
            uri = configuration['database_uri']
            bucket, prefix = uri.removeprefix('s3://').split('/', 1)
            for setting, value in [('duckdb_objfs_backend', 's3'), ('duckdb_objfs_bucket', bucket),
                                   ('duckdb_objfs_root', prefix)]:
                c.execute('SET '+setting+'=?', [value])
            c.execute("CREATE SECRET task_role (TYPE S3, PROVIDER credential_chain, REFRESH 'auto', "
                "REGION '" + region() + "', SCOPE '"+uri.replace("'", "''")+"')")
            guard()
            c.execute("ATTACH '"+native_uri(configuration)+"' AS warehouse (READ_ONLY)")
        except BaseException:
            self.connection.close()
            raise

    def stage(self, target, *, datasets, consistency="current", release_id=None):
        return stage(self.connection, self.configuration, target, self.guard,
                     consistency=consistency, release_id=release_id, datasets=datasets)

    def describe(self):
        return describe(self.connection, self.configuration, self.guard)

    def close(self):
        self.connection.close()
