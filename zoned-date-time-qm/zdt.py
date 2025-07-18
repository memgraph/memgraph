from datetime import datetime
from zoneinfo import ZoneInfo

import mgp


# zdt.py
@mgp.function
def withtz() -> mgp.Nullable[str]:
    return datetime.now().astimezone()


@mgp.function
def withouttz() -> mgp.Nullable[str]:
    return datetime.now()
