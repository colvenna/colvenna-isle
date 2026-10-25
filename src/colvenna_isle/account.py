"""The AWS account a deployment runs in: never part of the source.

Services receive it as `COLVENNA_ACCOUNT` from their stack. Runtime functions and tasks ask STS once per process
when the account is absent; GetCallerIdentity needs no IAM permission.
"""

import os
import re

from colvenna_isle.region import region

_resolved = None


def account():
    global _resolved
    if _resolved is None:
        value = os.environ.get("COLVENNA_ACCOUNT")
        if value is None:
            import boto3

            value = boto3.client("sts", region_name=region()).get_caller_identity()["Account"]
        if not re.fullmatch(r"[0-9]{12}", value):
            raise RuntimeError("COLVENNA_ACCOUNT must be the 12-digit deployment account")
        _resolved = value
    return _resolved
