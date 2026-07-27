import datetime

import mgp
from mage.date.constants import Epoch


@mgp.read_proc
def format(
    temporal: mgp.Any,
    format: str = "ISO",
) -> mgp.Record(formatted=str):
    if not (
        isinstance(temporal, datetime.datetime)
        or isinstance(temporal, datetime.date)
        or isinstance(temporal, datetime.time)
        or isinstance(temporal, datetime.timedelta)
    ):
        return mgp.Record(formatted=str(temporal))

    if "%z" in format or "%Z" in format:
        raise Exception(
            "Memgraph works with UTC zone only so\
            '%Z' in format is not supported."
        )

    if format == "ISO" and (
        isinstance(temporal, datetime.datetime)
        or isinstance(temporal, datetime.date)
        or isinstance(temporal, datetime.time)
    ):
        return mgp.Record(formatted=temporal.isoformat())

    if isinstance(temporal, datetime.timedelta):
        temporal = Epoch.UNIX_EPOCH + temporal

    return mgp.Record(formatted=temporal.strftime(format))
