# Copyright 2026 Memgraph Ltd.
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


def test_point2d_type_accepted():
    """Point2d value passes the type check and the procedure can inspect it."""
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        "CALL type_annotations.point2d_to_string(point({x: 15.9, y: 45.8, srid: 4326})) YIELD result RETURN result;",
    )
    s = result[0][0]
    assert "15.9" in s
    assert "45.8" in s
    assert "4326" in s


def test_point3d_type_accepted():
    """Point3d value passes the type check and the procedure can inspect it."""
    cursor = connect().cursor()
    result = execute_and_fetch_all(
        cursor,
        "CALL type_annotations.point3d_to_string(point({x: 1.0, y: 2.0, z: 3.0, srid: 4979})) YIELD result RETURN result;",
    )
    s = result[0][0]
    assert "1.0" in s
    assert "2.0" in s
    assert "3.0" in s
    assert "4979" in s


def test_enum_type_accepted():
    """Enum value passes the type check and the procedure can inspect it."""
    cursor = connect().cursor()
    execute_and_fetch_all(cursor, "CREATE ENUM Status VALUES { Good, Okay, Bad };")
    result = execute_and_fetch_all(
        cursor,
        "CALL type_annotations.enum_to_string(Status::Good) YIELD result RETURN result;",
    )
    s = result[0][0]
    assert "Status" in s
    assert "Good" in s


def test_point2d_type_mismatch():
    """Passing wrong type to a Point2d-typed parameter should fail."""
    cursor = connect().cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(
            cursor,
            "CALL type_annotations.point2d_to_string(42) YIELD result RETURN result;",
        )


def test_point3d_type_mismatch():
    """Passing wrong type to a Point3d-typed parameter should fail."""
    cursor = connect().cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(
            cursor,
            "CALL type_annotations.point3d_to_string('not a point') YIELD result RETURN result;",
        )


def test_enum_type_mismatch():
    """Passing wrong type to an Enum-typed parameter should fail."""
    cursor = connect().cursor()
    with pytest.raises(Exception):
        execute_and_fetch_all(
            cursor,
            "CALL type_annotations.enum_to_string(123) YIELD result RETURN result;",
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
