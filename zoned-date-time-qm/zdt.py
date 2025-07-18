from datetime import datetime
from zoneinfo import ZoneInfo

import mgp


# zdt.py
# TODO(colinbarry) Temp QM for use whilst developing support for ZonedDateTime.
@mgp.function
def withtz() -> mgp.Nullable[str]:
    return datetime.now().astimezone()


@mgp.function
def withouttz() -> mgp.Nullable[str]:
    return datetime.now()


@mgp.function
def hello() -> mgp.Nullable[str]:
    return "hello, world"
