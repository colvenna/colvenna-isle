"""Qualification-only lost-reply injection for the run controller.

An operator arms one fault item for a synthetic ``probe_*`` space. The next
matching original call executes normally; only its reply is then discarded and
reported as UNKNOWN to the controller, which must reconcile authoritative state
instead of replaying. Product spaces can never match, and each item is consumed
exactly once by a conditional delete before the reply is discarded.
"""

from botocore.exceptions import ClientError

from .dispatch import DispatchUnknown
from .writer_client import WriterUnknown

FAULT_INDEX = "PIPELINEFAULTS#1"
# Qualification spaces only (recovery, console/diagnostics and release fault drills); never a product space.
PREFIX = ("probe_",)


class Faults:
    def __init__(self, client, table):
        self.client, self.table = client, table

    def consume(self, space, operation, identity):
        if not space.startswith(PREFIX):
            return False
        key = {"PK": {"S": FAULT_INDEX}, "SK": {"S": "#".join([space, operation, identity])}}
        try:
            self.client.delete_item(TableName=self.table, Key=key,
                                    ConditionExpression="attribute_exists(PK)")
            return True
        except ClientError as error:
            if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise


class FaultyWriter:
    """Executes the original command, then loses the reply when an armed fault matches."""

    def __init__(self, writer, faults):
        self.writer, self.faults = writer, faults

    @staticmethod
    def identity(fields):
        if fields.get("job_id") or fields.get("client_request_id"):
            return fields.get("job_id") or fields["client_request_id"]
        descriptor = (fields.get("descriptor") or {}).get("s3_uri", "")
        # .../task-contexts/<generation>/<run>/<job>/<attempt>/descriptor.json
        return descriptor.rsplit("/", 3)[-3] if descriptor.endswith("/descriptor.json") else "any"

    def call(self, principal, configuration, operation, **fields):
        result = self.writer.call(principal, configuration, operation, **fields)
        identity = self.identity(fields)
        if self.faults.consume(configuration["space_id"], operation, identity):
            raise WriterUnknown("QUALIFICATION_REPLY_LOST")
        return result


class FaultyDispatcher:
    def __init__(self, dispatcher, faults):
        self.dispatcher, self.faults = dispatcher, faults

    def __getattr__(self, name):
        return getattr(self.dispatcher, name)

    def _space(self, reference):
        return reference["s3_uri"].split("/spaces/", 1)[1].split("/", 1)[0]

    def lambda_invoke(self, binding, reference):
        result = self.dispatcher.lambda_invoke(binding, reference)
        job = reference["s3_uri"].rsplit("/", 3)[-3]
        if self.faults.consume(self._space(reference), "lambda_invoke", job):
            raise DispatchUnknown("QUALIFICATION_REPLY_LOST")
        return result

    def ecs_run(self, binding, reference, tags):
        result = self.dispatcher.ecs_run(binding, reference, tags)
        job = reference["s3_uri"].rsplit("/", 3)[-3]
        if self.faults.consume(self._space(reference), "ecs_run", job):
            raise DispatchUnknown("QUALIFICATION_REPLY_LOST")
        return result
