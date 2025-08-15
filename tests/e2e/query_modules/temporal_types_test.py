# Copyright 2025 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import datetime
import sys
import zoneinfo

import pytest
import pytz
from common import connect, execute_and_fetch_all


def test_pymodule_taking_zoneddatetime_with_UTC():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        'RETURN temporal.to_string(datetime({year: 2024, month: 4, day: 21, hour: 14, minute: 15, second: 16, timezone: "UTC"}))',
    )[0][0]
    assert result == "2024-04-21 14:15:16+00:00"


def test_pymodule_taking_zoneddatetime_with_positive_offset():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        "RETURN temporal.to_string(datetime({year: 2024, month: 4, day: 21, hour: 14, minute: 15, second: 16, timezone: 120}))",
    )[0][0]
    assert result == "2024-04-21 14:15:16+02:00"


def test_pymodule_taking_zoneddatetime_with_negative_offset():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        "RETURN temporal.to_string(datetime({year: 2024, month: 4, day: 21, hour: 14, minute: 15, second: 16, timezone: -300}))",
    )[0][0]
    assert result == "2024-04-21 14:15:16-05:00"


def test_retrieve_zoneddatetime():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN temporal.make_zdt(2024, 1, 15, 10, 30, 45, 120)")
    assert result == [
        (datetime.datetime(2024, 1, 15, 10, 30, 45, tzinfo=datetime.timezone(datetime.timedelta(seconds=7200))),)
    ]


def test_retrieve_zoneddatetime_with_zoneinfo():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN temporal.make_zdt_with_zoneinfo(2024, 1, 15, 10, 30, 45, 'Etc/UTC')")
    assert result == [(datetime.datetime(2024, 1, 15, 10, 30, 45, tzinfo=zoneinfo.ZoneInfo("Etc/UTC")),)]


def test_retrieve_zoneddatetime_with_pytz():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor, "RETURN temporal.make_zdt_with_pytz(2024, 1, 15, 10, 30, 45, 'Asia/Ho_Chi_Minh')"
    )
    assert result == [(datetime.datetime(2024, 1, 15, 10, 30, 45, tzinfo=zoneinfo.ZoneInfo("Asia/Ho_Chi_Minh")),)]


def test_datetime_without_timezone():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN temporal.make_dt(2024, 1, 15, 10, 30, 45)")
    assert result == [(datetime.datetime(2024, 1, 15, 10, 30, 45, tzinfo=None),)]


def test_pymodule_accepting_pytz_timezone():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor, "RETURN temporal.to_string(temporal.make_zdt_with_pytz(2024, 1, 15, 10, 30, 45, 'America/New_York'))"
    )
    assert result[0][0] == "2024-01-15 10:30:45-05:00"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
