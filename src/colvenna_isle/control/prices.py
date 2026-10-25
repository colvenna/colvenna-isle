"""Region-indexed price snapshots; an absent region has no usable prices.

List prices exclude free tier, savings plans, credits, taxes and tiered discounts.
The data file retains the retrieval date and regional AWS Price List provenance.
"""

import json
from pathlib import Path
from types import SimpleNamespace


def for_region(region):
    value = json.loads((Path(__file__).parent / "data/list-prices.json").read_text()).get(region)
    if value is None:
        return None
    result = SimpleNamespace(**value, FARGATE_MINIMUM_SECONDS=60, SHARED_TASK_VCPU=2,
                             SHARED_TASK_GB=4, MONTH_SECONDS=30 * 86400, GIB=1073741824)
    result.fargate_second = lambda vcpu, gb: (vcpu * result.FARGATE_ARM_VCPU_HOUR / 3600
                                            + gb * result.FARGATE_ARM_GB_HOUR / 3600)
    return result
