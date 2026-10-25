"""Stack and runtime resource names of one Colvenna Isle deployment environment.

Every environment is an independent installation named by COLVENNA_ENVIRONMENT: its stacks and the runtime
resources its environment builder creates never share a name or an IAM wildcard with another environment.
"""

import os
import re
import hashlib

from colvenna_isle.region import region

RESERVED = {"env", "core", "control", "builder"}


def environment_name(value=None):
    name = value or os.environ.get("COLVENNA_ENVIRONMENT", "")
    if not re.fullmatch(r"[a-z][a-z0-9]{1,11}", name) or name in RESERVED:
        raise RuntimeError("Set COLVENNA_ENVIRONMENT to 2-12 lowercase letters/digits (not env, core, control or builder)")
    return name


def names(value=None):
    name = environment_name(value)
    # IAM roles are account-global. The same environment name in two regions
    # must not share a runtime role or the wildcard used during uninstall.
    namespace = hashlib.sha256((name + "/" + region()).encode()).hexdigest()[:16]
    return {"environment": name, "baseline": "colvenna-isle-" + name + "-core", "control": "colvenna-isle-" + name + "-control",
            "environments": "colvenna-isle-" + name + "-builder", "runtime_prefix": "colvenna-isle-" + namespace + "-"}
