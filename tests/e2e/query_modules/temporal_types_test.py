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

import sys

import pytest
from common import connect, execute_and_fetch_all


def test_pymodule_taking_zoneddatetime_with_named_timezone():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        'RETURN temporal.zoneddatetime_to_string(datetime({year: 2024, month: 4, day: 21, hour: 14, minute: 15, second: 16, timezone: "America/Los_Angeles"}))',
    )[0][0]
    assert result == "2024-04-21 14:15:16-07:00"


def test_pymodule_taking_zoneddatetime_with_UTC():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        'RETURN temporal.zoneddatetime_to_string(datetime({year: 2024, month: 4, day: 21, hour: 14, minute: 15, second: 16, timezone: "UTC"}))',
    )[0][0]
    assert result == "2024-04-21 14:15:16+00:00"


def test_pymodule_taking_zoneddatetime_with_positive_offset():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        "RETURN temporal.zoneddatetime_to_string(datetime({year: 2024, month: 4, day: 21, hour: 14, minute: 15, second: 16, timezone: 120}))",
    )[0][0]
    assert result == "2024-04-21 14:15:16+02:00"


def test_pymodule_taking_zoneddatetime_with_negative_offset():
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        "RETURN temporal.zoneddatetime_to_string(datetime({year: 2024, month: 4, day: 21, hour: 14, minute: 15, second: 16, timezone: -300}))",
    )[0][0]
    assert result == "2024-04-21 14:15:16-05:00"


# TODO(zoneddatetime) Add tests using `temporal.zdt` to ensure we can retrieve
# ZonedDateTimes. Also add tests to ensure that `datetime`s without a timezone
# are treated as `LocalDateTime`s.

if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
