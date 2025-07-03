import re
from datetime import datetime
from enum import IntEnum
from zoneinfo import ZoneInfo

import mgp


class FormatLength(IntEnum):
    """Enum for various date/time format lengths to replace magic numbers"""

    DATE = 10  # Length of 'YYYY-MM-DD'
    TIME = 8  # Length of 'HH:MM:SS'


class DateFormatUtil:
    """
    Utility class for converting between predefined ISO date formats using Python strftime and strptime.
    """

    ISO_DATE_FORMATS = {
        "basic_iso_date": "%Y%m%d",  # BASIC_ISO_DATE: '20111203'
        "iso_local_date": "%Y-%m-%d",  # ISO_LOCAL_DATE: '2011-12-03'
        "iso_offset_date": "%Y-%m-%d%z",  # ISO_OFFSET_DATE: '2011-12-03+01:00'
        "iso_date": "%Y-%m-%d",  # ISO_DATE: '2011-12-03' or '2011-12-03+01:00' (handled separately)
        "iso_local_time": "%H:%M:%S",  # ISO_LOCAL_TIME: '10:15:30'
        "iso_offset_time": "%H:%M:%S%z",  # ISO_OFFSET_TIME: '10:15:30+01:00'
        "iso_time": "%H:%M:%S",  # ISO_TIME: '10:15:30' or '10:15:30+01:00' (handled separately)
        "iso_local_date_time": "%Y-%m-%dT%H:%M:%S",  # ISO_LOCAL_DATE_TIME: '2011-12-03T10:15:30'
        "iso_offset_date_time": "%Y-%m-%dT%H:%M:%S%z",  # ISO_OFFSET_DATE_TIME: '2011-12-03T10:15:30+01:00'
        "iso_zoned_date_time": "iso_zoned_date_time",  # Special case
        "iso_date_time": "%Y-%m-%dT%H:%M:%S",  # ISO_DATE_TIME: '2011-12-03T10:15:30+01:00[Europe/Paris]' handled as zoned
    }

    @staticmethod
    def get_format(format_str: str) -> str:
        format_lower = format_str.lower()
        if format_lower == "iso_zoned_date_time" or format_lower == "iso_date_time":
            return "iso_zoned_date_time"
        if format_lower not in DateFormatUtil.ISO_DATE_FORMATS:
            raise ValueError(f"Unsupported date format: {format_str}")
        return DateFormatUtil.ISO_DATE_FORMATS[format_lower]


@mgp.function
def convert_format(temporal: str, current_format: str, convert_to: str) -> mgp.Nullable[str]:
    """
    Converts between specified ISO date formats using Python strftime and strptime.
    Supports zoned to offset conversion by removing zone part in '[]'.
    Offset to zoned returns the same string.
    Throws if parsing fails.

    Args:
        temporal: The datetime string to convert
        current_format: The current format of the datetime string
        convert_to: The target format to convert to

    Returns:
        output: The converted datetime string or None if input is None or empty
    """
    if temporal is None or temporal.strip() == "":
        return None

    try:
        current_formatter = DateFormatUtil.get_format(current_format)
        convert_to_formatter = DateFormatUtil.get_format(convert_to)

        # Parse input string
        if current_formatter == "iso_zoned_date_time":
            # Remove zone part in [] and parse
            temporal_without_zone = temporal.split("[")[0]

            if "." in temporal_without_zone:
                temporal_without_zone = re.sub(r"(\.\d{6})\d*", r"\1", temporal_without_zone)
                dt = datetime.strptime(temporal_without_zone, "%Y-%m-%dT%H:%M:%S.%f%z")
            else:
                dt = datetime.strptime(temporal_without_zone, "%Y-%m-%dT%H:%M:%S%z")
        elif current_format.lower() == "iso_date":
            # iso_date can have optional offset, try parsing with offset first
            try:
                dt = datetime.strptime(temporal, "%Y-%m-%d%z")
            except ValueError:
                dt = datetime.strptime(temporal, "%Y-%m-%d")
        elif current_format.lower() == "iso_time":
            # iso_time can have optional offset
            try:
                dt = datetime.strptime(temporal, "%H:%M:%S%z")
            except ValueError:
                dt = datetime.strptime(temporal, "%H:%M:%S")
        else:
            try:
                dt = datetime.fromisoformat(temporal)
            except Exception:
                dt = datetime.strptime(temporal, current_formatter)

        if convert_to.lower() in ["iso_offset_date", "iso_offset_time", "iso_offset_date_time"] and dt.tzinfo is None:
            raise Exception("missing timezone")

        # Convert to target format
        if convert_to_formatter == "iso_zoned_date_time":
            # Converting to zoned date time: return offset datetime string (no zone name)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo("UTC"))
            converted = dt.isoformat()

        elif convert_to.lower() == "iso_date":
            # iso_date: include offset if timezone info is present
            if dt.tzinfo is not None:
                converted = dt.strftime("%Y-%m-%d%z")
                # Format offset as +hh:mm
                if len(converted) > FormatLength.DATE:
                    converted = f"{converted[:-2]}:{converted[-2:]}"
            else:
                converted = dt.strftime("%Y-%m-%d")
        elif convert_to.lower() == "iso_time":
            # iso_time: include offset if timezone info is present
            if dt.tzinfo is not None:
                converted = dt.strftime("%H:%M:%S%z")
                # Format offset as +hh:mm
                if len(converted) > FormatLength.TIME:
                    converted = f"{converted[:-2]}:{converted[-2:]}"
            else:
                converted = dt.strftime("%H:%M:%S")
        elif convert_to.lower() in ["iso_zoned_date_time", "iso_offset_date_time"]:
            converted = dt.isoformat()
        else:
            # For offset formats, ensure timezone is present
            if convert_to_formatter.endswith("%z") and dt.tzinfo is None:
                dt = dt.replace(tzinfo=ZoneInfo("UTC"))
            # For local formats, remove timezone info
            elif not convert_to_formatter.endswith("%z") and dt.tzinfo is not None:
                dt = dt.replace(tzinfo=None)

            converted = dt.strftime(convert_to_formatter)

            # Format offset as +hh:mm for offset formats
            if convert_to_formatter.endswith("%z") and len(converted) > FormatLength.DATE:
                converted = f"{converted[:-2]}:{converted[-2:]}"

        return converted

    except Exception as e:
        raise Exception(f"Error converting '{temporal}' from '{current_format}' to '{convert_to}': {e}")


@mgp.read_proc
def get_date_formats(context: mgp.ProcCtx) -> mgp.Record(formats=mgp.List[str]):
    """
    Returns a list of supported date formats.

    Returns:
        formats: List of supported date formats
    """
    return mgp.Record(formats=list(DateFormatUtil.ISO_DATE_FORMATS.keys()))
