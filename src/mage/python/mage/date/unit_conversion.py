import datetime

from mage.date.constants import Units


def to_timedelta(time: int, unit: str) -> datetime.timedelta:
    if unit in Units.MILLISECOND:
        return datetime.timedelta(milliseconds=time)
    elif unit in Units.SECOND:
        return datetime.timedelta(seconds=time)
    elif unit in Units.MINUTE:
        return datetime.timedelta(minutes=time)
    elif unit in Units.HOUR:
        return datetime.timedelta(hours=time)
    elif unit in Units.DAY:
        return datetime.timedelta(days=time)
    else:
        raise TypeError(f"The unit {unit} is not correct.")


def to_int(duration: datetime.timedelta, unit: str) -> int:
    if unit in Units.MILLISECOND:
        return duration / datetime.timedelta(milliseconds=1)
    elif unit in Units.SECOND:
        return duration.total_seconds()
    elif unit in Units.MINUTE:
        return duration / datetime.timedelta(minutes=1)
    elif unit in Units.HOUR:
        return duration / datetime.timedelta(hours=1)
    elif unit in Units.DAY:
        return duration / datetime.timedelta(days=1)
    else:
        raise TypeError(f"The unit {unit} is not correct.")
