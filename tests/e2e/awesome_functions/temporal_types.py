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

import pytest
from common import memgraph


def test_datetime_with_timezone_only(memgraph):
    """Specifying a map with only a timezone populates the numeric datetime
    members with the current time"""
    now_before = datetime.datetime.now(datetime.timezone.utc)
    result = next(memgraph.execute_and_fetch("RETURN datetime({timezone: 'Europe/Brussels'}) AS dt"))
    now_after = datetime.datetime.now(datetime.timezone.utc)

    dt = result["dt"]
    assert dt is not None
    assert str(dt.tzinfo) == "Europe/Brussels"

    # check the computed time is in the range of expected times
    dt_utc = dt.astimezone(datetime.timezone.utc)
    assert now_before <= dt_utc <= now_after


def test_datetime_with_partial_numeric_values(memgraph):
    """Specifying a map with one or more numeric values will use the given
    fields and assume default (minimum) values for unspecified fields"""
    result = next(memgraph.execute_and_fetch("RETURN datetime({year: 2023, month: 11}) AS dt"))
    dt = result["dt"]
    assert dt.year == 2023
    assert dt.month == 11
    assert dt.day == 1
    assert dt.hour == 0
    assert dt.minute == 0
    assert dt.second == 0


def test_datetime_with_partial_numeric_values_and_timezone(memgraph):
    """Test using a timezone with partial numeric values, where missing fields
    assume the default values."""
    result = next(
        memgraph.execute_and_fetch("RETURN datetime({year: 2021, month: 3, timezone: 'Europe/Brussels'}) AS dt")
    )
    dt = result["dt"]
    assert dt.year == 2021
    assert dt.month == 3
    assert dt.day == 1
    assert dt.hour == 0
    assert dt.minute == 0
    assert dt.second == 0
    assert dt is not None
    assert str(dt.tzinfo) == "Europe/Brussels"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
