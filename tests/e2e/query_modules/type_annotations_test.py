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
from neo4j import GraphDatabase


@pytest.fixture(scope="module")
def driver():
    drv = GraphDatabase.driver("bolt://localhost:7687", auth=None)
    yield drv
    drv.close()


@pytest.fixture(autouse=True)
def cleanup(driver):
    yield
    with driver.session() as session:
        session.run("MATCH (n) DETACH DELETE n")


# -- Point2d: argument + return type validation --


def test_point2d_round_trip(driver):
    """Point2d passes arg type check, round-trips, and passes return type check."""
    with driver.session() as session:
        result = session.run(
            "CALL type_annotations.echo_point2d(point({x: 15.9, y: 45.8, srid: 4326})) YIELD result RETURN result;"
        )
        record = result.single()
        point = record["result"]
        assert point.x == 15.9
        assert point.y == 45.8
        assert point.srid == 4326


def test_point2d_arg_type_mismatch(driver):
    """Passing wrong type to a Point2d-typed parameter should fail."""
    with driver.session() as session:
        with pytest.raises(Exception):
            session.run("CALL type_annotations.echo_point2d(42) YIELD result RETURN result;").consume()


# -- Point3d: argument + return type validation --


def test_point3d_round_trip(driver):
    """Point3d passes arg type check, round-trips, and passes return type check."""
    with driver.session() as session:
        result = session.run(
            "CALL type_annotations.echo_point3d(point({x: 1.0, y: 2.0, z: 3.0, srid: 4979})) YIELD result RETURN result;"
        )
        record = result.single()
        point = record["result"]
        assert point.x == 1.0
        assert point.y == 2.0
        assert point.z == 3.0
        assert point.srid == 4979


def test_point3d_arg_type_mismatch(driver):
    """Passing wrong type to a Point3d-typed parameter should fail."""
    with driver.session() as session:
        with pytest.raises(Exception):
            session.run("CALL type_annotations.echo_point3d('not a point') YIELD result RETURN result;").consume()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
