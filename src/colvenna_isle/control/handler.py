"""API Gateway control integration; only Gateway authorizer context carries identity."""

import os
import re
from functools import lru_cache

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from colvenna_isle.region import region
from colvenna_isle.account import account
from colvenna_isle.contracts.validation import ContractError
from .registry import ControlError, Registry
from .keys import KeyStore
from .http import parse_body, principal_from_event, response, verified_principal


IDENTIFIER = r"[A-Za-z_][A-Za-z0-9_.-]*"


def query(event, name, kind=str, default=None):
    """One bounded query parameter; repeated or malformed values are rejected."""
    value = (event.get("queryStringParameters") or {}).get(name)
    if value is None:
        return default
    if not isinstance(value, str) or "," in value or len(value) > 2048:
        raise ControlError(400, "INVALID_QUERY_PARAMETER")
    if kind is int:
        if not re.fullmatch(r"[0-9]{1,6}", value):
            raise ControlError(400, "INVALID_QUERY_PARAMETER")
        return int(value)
    return value


def governance(event, principal, method, path, services):
    """Admin/console routes; returns None when the path is not one of them."""
    match = re.fullmatch(rf"/v1/admin/spaces/({IDENTIFIER})/(status|metrics|trace|audit|quotas|costs)", path)
    if match and method == "GET":
        space, resource = match.groups()
        if resource == "audit":
            principal.authorize(space, "audit:read")
            configuration = services["registry"]._read(space)
            if services["keys"].authenticate_id(principal.key_id or "") != principal:
                raise ControlError(403, "FORBIDDEN")
            return 200, services["audit"].list(principal, configuration, limit=query(event, "limit", int, 50),
                                               page_token=query(event, "page_token"), action=query(event, "action"))
        principal.authorize(space, "monitor:read")
        if resource == "status":
            return 200, services["overview"].status(principal, space)
        if resource == "metrics":
            return 200, services["overview"].metrics(principal, space, query(event, "window_seconds", int, 3600))
        if resource == "quotas":
            return 200, services["overview"].quota_status(principal, space)
        if resource == "costs":
            return 200, services["costs"].report(principal, space, query(event, "since"), query(event, "until"))
        kind, identity = query(event, "kind"), query(event, "id")
        if kind is None or identity is None:
            raise ControlError(400, "TRACE_IDENTITY_REQUIRED")
        return 200, services["trace"].get(principal, space, kind, identity, query(event, "pipeline_id"))
    match = re.fullmatch(rf"/v1/admin/spaces/({IDENTIFIER})/retention"
                         rf"(?:/(holds|collections)(?:/({IDENTIFIER})(/release)?)?)?", path)
    if match:
        space, kind, identity, release = match.groups()
        retention = services["retention"]
        headers = {name.lower(): value for name, value in event.get("headers", {}).items()}
        if kind is None and method == "GET":
            principal.authorize(space, "retention:manage" if "retention:manage" in principal.scopes else "monitor:read")
            return 200, retention.status(principal, space)
        if kind is None and method == "PUT":
            principal.authorize(space, "retention:manage")
            return 200, retention.update(principal, space, parse_body(event), headers.get("if-match"))
        if kind == "holds" and identity is None and method == "POST":
            principal.authorize(space, "retention:manage")
            return 201, retention.place_hold(principal, space, parse_body(event))
        if kind == "holds" and identity and release and method == "POST":
            from colvenna_isle.contracts.validation import validate
            validate("empty_request", parse_body(event) if event.get("body") else {})
            principal.authorize(space, "retention:manage")
            return 200, retention.release_hold(principal, space, identity)
        if kind == "collections" and identity is None and method == "POST":
            principal.authorize(space, "retention:manage")
            return 202, retention.request(principal, space, parse_body(event), headers.get("idempotency-key"))
        if kind == "collections" and identity and not release and method == "GET":
            principal.authorize(space, "retention:manage" if "retention:manage" in principal.scopes else "monitor:read")
            return 200, retention.collection(principal, space, identity)
        raise ControlError(404, "ROUTE_NOT_FOUND")
    match = re.fullmatch(rf"/v1/spaces/({IDENTIFIER})/pipelines/({IDENTIFIER})/"
                         rf"(?:(trial-runs)|(artifacts)|(draft/artifacts)|runs/({IDENTIFIER})/(preview))", path)
    if match:
        space, pipeline, trial, artifacts, draft, run_id, preview = match.groups()
        headers = {name.lower(): value for name, value in event.get("headers", {}).items()}
        if trial and method == "POST":
            principal.authorize(space, "pipeline:run")
            return 202, services["runs"].trial(principal, space, pipeline, parse_body(event),
                                               headers.get("idempotency-key"))
        if artifacts and method == "POST":
            principal.authorize(space, "pipeline:manage")
            return 201, services["pipelines"].inline_upload(principal, space, pipeline, parse_body(event))
        if draft and method == "GET":
            principal.authorize(space, "pipeline:manage")
            path_value = query(event, "path")
            if path_value is None:
                raise ControlError(400, "ARTIFACT_PATH_REQUIRED")
            return 200, services["pipelines"].artifact(principal, space, pipeline, path_value)
        if preview and method == "GET":
            principal.authorize(space, "pipeline:run")
            return 200, services["runs"].preview(principal, space, pipeline, run_id, query(event, "rows", int, 20))
        raise ControlError(404, "ROUTE_NOT_FOUND")
    return None


def handle(event, registry, keys=None, statuses=None, environments=None, pipelines=None, runs=None,
           services=None):
    request_id = event.get("requestContext", {}).get("requestId", "unavailable")
    try:
        principal = verified_principal(event, keys) if keys is not None else principal_from_event(event)
        method = event.get("requestContext", {}).get("http", {}).get("method")
        path = event.get("rawPath", "")
        if services is not None:
            routed = governance(event, principal, method, path, {
                "registry": registry, "keys": keys, "pipelines": pipelines, "runs": runs, **services})
            if routed is not None:
                return response(*routed)
        if method == "GET" and pipelines is not None and re.fullmatch(rf"/v1/spaces/({IDENTIFIER})/pipelines", path):
            space_id = path.split("/")[3]
            principal.authorize(space_id, next((name for name in ["pipeline:manage", "pipeline:run", "monitor:read"]
                                                if name in principal.scopes), "pipeline:manage"))
            return response(200, pipelines.list(principal, space_id))
        if method == "GET" and environments is not None and re.fullmatch(
                rf"/v1/spaces/({IDENTIFIER})/runtime-environments", path):
            space_id = path.split("/")[3]
            principal.authorize(space_id, "monitor:read" if "runtime:manage" not in principal.scopes
                                else "runtime:manage")
            return response(200, environments.list(principal, space_id))
        run = re.fullmatch(
            r"/v1/spaces/([A-Za-z_][A-Za-z0-9_.-]*)/pipelines/([A-Za-z_][A-Za-z0-9_.-]*)/runs"
            r"(?:/([A-Za-z_][A-Za-z0-9_.-]*)(?:/(cancel|retry))?)?", path)
        if run:
            space_id, pipeline_id, run_id, operation = run.groups()
            if runs is None:
                raise ControlError(503, "RUN_SERVICE_UNAVAILABLE")
            headers = {name.lower(): value for name, value in event.get("headers", {}).items()}
            if run_id is None and method == "GET":
                principal.authorize(space_id, next((name for name in ["pipeline:run", "pipeline:manage", "monitor:read"]
                                                    if name in principal.scopes), "pipeline:run"))
                value, status = runs.history(principal, space_id, pipeline_id, limit=query(event, "limit", int, 20),
                                             page_token=query(event, "page_token")), 200
            elif run_id is None and method == "POST":
                principal.authorize(space_id, "pipeline:run")
                value, status = runs.create(principal, space_id, pipeline_id, parse_body(event),
                                            headers.get("idempotency-key")), 202
            elif run_id is not None and operation is None and method == "GET":
                principal.authorize(space_id, "pipeline:run" if "pipeline:run" in principal.scopes else "monitor:read")
                value, status = runs.get(principal, space_id, pipeline_id, run_id), 200
            elif operation in {"cancel", "retry"} and method == "POST":
                from colvenna_isle.contracts.validation import validate
                validate("empty_request", parse_body(event) if event.get("body") else {})
                principal.authorize(space_id, "pipeline:run")
                if operation == "cancel":
                    value, status = runs.cancel(principal, space_id, pipeline_id, run_id), 200
                else:
                    value, status = runs.retry(principal, space_id, pipeline_id, run_id,
                                               headers.get("idempotency-key")), 202
            else:
                raise ControlError(404, "ROUTE_NOT_FOUND")
            return response(status, value)
        pipeline = re.fullmatch(
            r"/v1/spaces/([A-Za-z_][A-Za-z0-9_.-]*)/pipelines"
            r"(?:/([A-Za-z_][A-Za-z0-9_.-]*)(?:/(draft|artifact-uploads|validate|versions|activate|schedule)"
            r"(?:/([A-Za-z_][A-Za-z0-9_.-]*))?)?)?", path)
        if pipeline:
            space_id, pipeline_id, operation, version = pipeline.groups()
            if pipelines is None:
                raise ControlError(503, "PIPELINE_SERVICE_UNAVAILABLE")
            headers = {name.lower(): value for name, value in event.get("headers", {}).items()}
            if operation == "versions" and version is None and method == "GET":
                principal.authorize(space_id, "pipeline:manage" if "pipeline:manage" in principal.scopes
                                    else "pipeline:run")
                return response(200, pipelines.versions(principal, space_id, pipeline_id))
            principal.authorize(space_id, "pipeline:manage")
            if pipeline_id is None and method == "POST":
                value, status = pipelines.create(principal, space_id, parse_body(event)), 201
            elif pipeline_id is not None and operation is None and method == "GET":
                value, status = pipelines.get(principal, space_id, pipeline_id), 200
            elif version is not None and operation == "versions" and method == "GET":
                value, status = pipelines.version(principal, space_id, pipeline_id, version), 200
            elif pipeline_id is not None and version is None:
                body = parse_body(event)
                if operation == "draft" and method == "PUT":
                    value, status = pipelines.replace_draft(
                        principal, space_id, pipeline_id, body, headers.get("if-match")), 200
                elif operation == "artifact-uploads" and method == "POST":
                    value, status = pipelines.upload(principal, space_id, pipeline_id, body), 201
                elif operation == "validate" and method == "POST":
                    value, status = pipelines.validate(principal, space_id, pipeline_id, body), 200
                elif operation == "versions" and method == "POST":
                    value, status = pipelines.publish(
                        principal, space_id, pipeline_id, body, headers.get("idempotency-key")), 201
                elif operation == "activate" and method == "POST":
                    value, status = pipelines.activate(
                        principal, space_id, pipeline_id, body, headers.get("if-match")), 200
                elif operation == "schedule" and method == "PUT":
                    value, status = pipelines.schedule(
                        principal, space_id, pipeline_id, body, headers.get("if-match")), 200
                else:
                    raise ControlError(404, "ROUTE_NOT_FOUND")
            else:
                raise ControlError(404, "ROUTE_NOT_FOUND")
            return response(status, value)
        environment = re.fullmatch(
            r"/v1/spaces/([A-Za-z_][A-Za-z0-9_.-]*)/runtime-environments"
            r"(?:/([A-Za-z_][A-Za-z0-9_.-]*)/versions/([A-Za-z_][A-Za-z0-9_.-]*)(/build)?)?",
            path)
        if environment:
            space_id, environment_id, version, build = environment.groups()
            principal.authorize(space_id, "monitor:read" if method == "GET"
                                and "runtime:manage" not in principal.scopes else "runtime:manage")
            if environments is None:
                raise ControlError(503, "ENVIRONMENT_SERVICE_UNAVAILABLE")
            if environment_id is None and method == "POST":
                value, status = environments.create(principal, space_id, parse_body(event)), 201
            elif environment_id is not None and not build and method == "GET":
                value, status = environments.get(principal, space_id, environment_id, version), 200
            elif build and method == "POST":
                from colvenna_isle.contracts.validation import validate
                validate("empty_request", parse_body(event) if event.get("body") else {})
                headers = {name.lower(): value for name, value in event.get("headers", {}).items()}
                value, status = environments.build(
                    principal, space_id, environment_id, version, headers.get("idempotency-key")), 202
            else:
                raise ControlError(404, "ROUTE_NOT_FOUND")
            return response(status, value)
        ingestion = re.fullmatch(
            r"/v1/spaces/([A-Za-z_][A-Za-z0-9_.-]*)/ingestions/([A-Za-z_][A-Za-z0-9_.-]*)", path)
        if ingestion and method == "GET":
            if statuses is None:
                raise ControlError(503, "STATUS_UNAVAILABLE")
            return response(200, statuses.get(principal, *ingestion.groups()))
        key_match = re.fullmatch(
            r"/v1/admin/spaces/([A-Za-z_][A-Za-z0-9_.-]*)/api-keys"
            r"(?:/([A-Za-z_][A-Za-z0-9_.-]*)/(revoke|rotate))?", path)
        if key_match:
            if keys is None:
                raise ControlError(503, "KEY_SERVICE_UNAVAILABLE")
            space_id, key_id, operation = key_match.groups()
            principal.authorize(space_id, "keys:manage")
            if key_id is None and method == "GET":
                value, status = keys.list(principal, space_id), 200
            elif key_id is None and method == "POST":
                value, status = keys.create(principal, space_id, parse_body(event), request_id), 201
            elif operation == "revoke" and method == "POST":
                from colvenna_isle.contracts.validation import validate
                validate("empty_request", parse_body(event) if event.get("body") else {})
                value, status = keys.revoke(principal, space_id, key_id), 200
            elif operation == "rotate" and method == "POST":
                value, status = keys.rotate(principal, space_id, key_id, parse_body(event), request_id), 201
            else:
                raise ControlError(404, "ROUTE_NOT_FOUND")
            return response(status, value)
        match = re.fullmatch(
            r"/v1/admin/spaces/([A-Za-z_][A-Za-z0-9_.-]*)/(configuration|datasets)", path)
        if not match:
            raise ControlError(404, "ROUTE_NOT_FOUND")
        space_id, resource = match.groups()
        principal.authorize(space_id, "datasets:manage")
        if method == "GET" and resource == "configuration":
            value = registry.configuration(principal, space_id)
            status = 200
        elif method == "POST" and resource == "datasets":
            headers = {name.lower(): value for name, value in event.get("headers", {}).items()}
            expected = headers.get("if-match", "")
            if not re.fullmatch(r'"gen_[0-9a-f]{32}"', expected):
                raise ControlError(428, "SPACE_GENERATION_REQUIRED")
            value = registry.register(principal, space_id, parse_body(event), generation=expected[1:-1])
            status = 201
        else:
            raise ControlError(404, "ROUTE_NOT_FOUND")
    except ControlError as error:
        status, value = error.status, {"code": error.code, "message": error.safe_message, "request_id": request_id}
    except (ContractError, ValueError, TypeError, KeyError, RecursionError):
        status, value = 400, {"code": "INVALID_REQUEST", "message": "Invalid request", "request_id": request_id}
    except (ClientError, BotoCoreError):
        status, value = 503, {"code": "CONTROL_UNAVAILABLE", "message": "Retry the same request", "request_id": request_id}
    return response(status, value)


@lru_cache(maxsize=1)
def clients():
    from botocore.config import Config

    options = Config(connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 2})
    # Presigned uploads must bind length/checksum/conditional headers using SigV4, on the regional
    # endpoint: a new bucket's global-endpoint name answers 307 until its DNS propagates.
    s3_options = options.merge(Config(signature_version="s3v4", s3={"addressing_style": "virtual"}))
    current_region = region()
    return (boto3.client("dynamodb", config=options, region_name=current_region),
            boto3.client("s3", config=s3_options, region_name=current_region,
                         endpoint_url=f"https://s3.{current_region}.amazonaws.com"),
            boto3.client("lambda", region_name=current_region, config=Config(
                connect_timeout=3, read_timeout=23, retries={"total_max_attempts": 1})))


def handler(event, context):
    from colvenna_isle.writer.ownership import Ownership
    from .environment_artifacts import EnvironmentArtifacts
    from .environments import Environments
    from .pipeline_artifacts import PipelineArtifacts
    from .pipelines import Pipelines
    from .quotas import Quotas
    from .status import Ledger, Projection, Status

    client, s3, lam = clients()
    table, bucket = os.environ["COLVENNA_CONTROL_TABLE"], os.environ["COLVENNA_DATA_BUCKET"]
    registry = Registry(client, table, bucket)
    ledger = Ledger(Ownership(client, table), lam, os.environ["COLVENNA_WRITER_CONTROL_FUNCTION"],
                    os.environ["COLVENNA_WRITER_SOURCE_COMMIT"])
    statuses = Status(registry, Projection(client, table), ledger, s3, bucket, account())
    keys = KeyStore(client, table)
    quotas = Quotas(client, table)
    project = os.environ.get("COLVENNA_ENVIRONMENT_PROJECT", "")
    policy = decode_environment_policy(os.environ.get("COLVENNA_ENVIRONMENT_POLICY", "{}"))
    environments = None
    if project and policy:
        codebuild, ecr, ecs = environment_clients()
        environments = Environments(
            registry, codebuild, project, policy,
            EnvironmentArtifacts(s3, ecr, lam, ecs, bucket), keys, quotas)
    pipelines = Pipelines(registry, PipelineArtifacts(s3, bucket), environments, keys,
                          writer_source=os.environ["COLVENNA_WRITER_SOURCE_COMMIT"], quotas=quotas)
    runs = run_service(pipelines, keys, lam, s3, quotas)
    return handle(event, registry, keys, statuses, environments, pipelines, runs,
                  governance_services(registry, keys, ledger, runs, environments, quotas, s3, lam))


def governance_services(registry, keys, ledger, runs, environments, quotas, s3, lam):
    """Admin overview, audit, trace, retention and cost services (absent without the run service)."""
    if runs is None:
        return None
    from colvenna_isle.intake.backpressure import Admission
    from .audit import Audit
    from .costs import Costs
    from .overview import Overview
    from .retention import Retention
    from .trace import Trace

    monitoring = monitoring_clients()
    admission = Admission(monitoring[0], monitoring[1], ledger, os.environ["COLVENNA_INGRESS_QUEUE"],
                          os.environ["COLVENNA_DLQ"])
    collector = os.environ.get("COLVENNA_COLLECTOR_FUNCTION", "")

    def invoke():
        # Best effort; the collector's own schedule drains the queue.
        if collector:
            try:
                lam.invoke(FunctionName=collector, InvocationType="Event", Payload=b'{"source":"api"}')
            except (ClientError, BotoCoreError):
                pass
    ecs = environment_clients()[2]
    return {"audit": Audit(registry.client, registry.table),
            "overview": Overview(registry, keys, runs.writer.ownership, runs.writer, runs, admission, quotas,
                                 environments=environments),
            "trace": Trace(registry, keys, runs.writer, ledger, runs, s3, ecs, registry.bucket),
            "retention": Retention(registry, keys, runs, invoke=invoke),
            "costs": Costs(registry, keys, runs, environments, ecs)}


@lru_cache(maxsize=1)
def monitoring_clients():
    from botocore.config import Config

    options = Config(connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 2})
    return boto3.client("sqs", region_name=region(), config=options), boto3.client("cloudwatch", region_name=region(), config=options)


def run_service(pipelines, keys, lam, s3, quotas=None):
    """Run API: durable admission/status plus writer-ledger cancellation."""
    import json as _json
    from colvenna_isle.writer.ownership import Ownership
    from colvenna_isle.pipeline.task_objects import TaskObjects
    from .dispatch import Dispatcher
    from .runs import Runs
    from .writer_client import WriterClient

    network = os.environ.get("COLVENNA_COMPUTE_NETWORK")
    if not network:
        return None
    network = _json.loads(network)
    table, bucket = os.environ["COLVENNA_CONTROL_TABLE"], os.environ["COLVENNA_DATA_BUCKET"]
    writer = WriterClient(Ownership(pipelines.client, table), lam, os.environ["COLVENNA_WRITER_CONTROL_FUNCTION"])
    prefix = decode_environment_policy(os.environ.get("COLVENNA_ENVIRONMENT_POLICY", "{}")).get("runtime_prefix", "")
    dispatcher = Dispatcher(lam, environment_clients()[2], s3, bucket, cluster=network["cluster"],
                            subnets=network["subnets"], security_groups=network["security_groups"], prefix=prefix)
    scheduler = os.environ.get("COLVENNA_SCHEDULER_FUNCTION", "")

    def wake():
        # Best effort; the minute schedule reconciles a lost wake-up.
        if scheduler:
            try:
                lam.invoke(FunctionName=scheduler, InvocationType="Event", Payload=b'{"wake":true}')
            except (ClientError, BotoCoreError):
                pass
    return Runs(pipelines, keys, writer, dispatcher,
                lambda space, generation: TaskObjects(s3, bucket, space, generation), wake=wake, quotas=quotas)


def decode_environment_policy(content):
    from .http import decode_json

    return decode_json(content.encode())


@lru_cache(maxsize=1)
def environment_clients():
    from botocore.config import Config

    options = Config(connect_timeout=3, read_timeout=5, retries={"total_max_attempts": 1})
    return tuple(boto3.client(name, region_name=region(), config=options) for name in ["codebuild", "ecr", "ecs"])
