"""One explicit AWS commercial region per deployment or runtime process."""

import os
import re


def region():
    values = {os.environ[name] for name in ("COLVENNA_REGION", "AWS_REGION", "AWS_DEFAULT_REGION")
              if os.environ.get(name)}
    if len(values) != 1:
        raise RuntimeError("Set COLVENNA_REGION or AWS_REGION; all configured region values must agree")
    value = values.pop()
    if not re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-[1-9][0-9]*", value):
        raise RuntimeError("Region must name an AWS commercial region")
    if value.startswith(("cn-", "us-gov-", "us-iso", "eu-isoe-", "eusc-")):
        raise RuntimeError("Only the AWS commercial partition (aws) is supported")
    return value


def regional(value):
    """Use CloudFormation's region in generated ARN/endpoint/environment strings."""
    if isinstance(value, str):
        return {"Fn::Sub": value} if "${AWS::Region}" in value else value
    if isinstance(value, list):
        return [regional(item) for item in value]
    if isinstance(value, dict):
        return {key: item if key == "Fn::Sub" else regional(item) for key, item in value.items()}
    return value
