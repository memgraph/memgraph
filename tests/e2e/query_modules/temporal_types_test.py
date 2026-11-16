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

import mgclient
import pytest
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


def test_datetime_without_timezone():
    cursor = connect().cursor()
    result = execute_and_fetch_all(cursor, "RETURN temporal.make_dt(2024, 1, 15, 10, 30, 45)")
    assert result == [(datetime.datetime(2024, 1, 15, 10, 30, 45, tzinfo=None),)]


def test_temporal_string_parsing_family():
    cursor = connect().cursor()

    def to_string(cursor, value):
        try:
            return execute_and_fetch_all(cursor, f"RETURN temporal.to_string({value})")
        except mgclient.DatabaseError:
            return None

    # Date-only is midnight (UTC when no designator), and every fraction of 1-9 digits is right-padded
    # (7-9 digits truncate at microseconds). 3- and 6-digit values keep parsing to the same instant.
    accepted = [
        ('datetime("2024-01-02")', 'datetime("2024-01-02T00:00:00Z")'),
        ('datetime("2024-01-02+01:00")', 'datetime("2024-01-02T00:00:00+01:00")'),
        ('datetime("2024-01-02T03:04:05.5Z")', 'datetime("2024-01-02T03:04:05.500000Z")'),
        ('localdatetime("2024-01-02T03:04:05.12")', 'localdatetime("2024-01-02T03:04:05.120000")'),
        ('localtime("03:04:05.1234567")', 'localtime("03:04:05.123456")'),
        ('localtime("03:04:05.123456789")', 'localtime("03:04:05.123456")'),
        ('datetime("2024-01-02T03:04:05.123Z")', 'datetime("2024-01-02T03:04:05.123000Z")'),
        ('datetime("2024-01-02T03:04:05.123456Z")', 'datetime("2024-01-02T03:04:05.123456Z")'),
    ]
    for value, reference in accepted:
        actual = to_string(cursor, value)
        expected = to_string(cursor, reference)
        assert expected is not None, f"reference {reference} failed to parse"
        assert actual == expected, f"{value} parsed to {actual}, expected the value of {reference} ({expected})"

    # Invalid strings keep being rejected with a query error.
    for value in [
        'datetime("2024-13-45")',
        'localdatetime("2024-01-02.5")',
        'localtime("03:04:05.999999999999")',
        'localdatetime("2024-01-02T03:04:05s")',
    ]:
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(cursor, f"RETURN {value}")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
