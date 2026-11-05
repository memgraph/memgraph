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

import math
import sys

import pytest

from common import memgraph

SETUP = [
    "CREATE (:Scalar {big: 1e20, integral: 2.0, large: 3e38})",
    "CREATE (:Lists {doubles: [1e20, 2.0, 3e38], numerics: [1, 1e20, 2.0], "
    "special: [1.0/0.0, -1.0/0.0, 0.0/0.0]})",
    "CREATE VECTOR INDEX f32_idx ON :Vec32(v) WITH CONFIG {'dimension': 2, 'capacity': 10}",
    "CREATE (:Vec32 {v: [123456.789, 0.1]})",
    "CREATE VECTOR INDEX f16_idx ON :Vec16(v) WITH CONFIG {'dimension': 1, 'capacity': 10, 'scalar_kind': 'f16'}",
    "CREATE (:Vec16 {v: [70000.0]})",
    "CREATE (:Pts {big: POINT({x: 1e20, y: 2, srid: 7203}), "
    "pinf: POINT({x: 1.0/0.0, y: 2, srid: 7203}), "
    "ninf: POINT({x: -1.0/0.0, y: 2, srid: 7203}), "
    "nan: POINT({x: 0.0/0.0, y: 2, srid: 7203})})",
]

PROBES = [
    "MATCH (n:Scalar) RETURN n.big AS big, n.integral AS integral, n.large AS large",
    "MATCH (n:Lists) RETURN n.doubles AS doubles, n.numerics AS numerics, n.special AS special",
    "MATCH (n:Vec32) RETURN n.v AS v",
    "MATCH (n:Vec16) RETURN n.v AS v",
    "MATCH (n:Pts) RETURN n.big.x AS big, n.pinf.x AS pinf, n.ninf.x AS ninf, n.nan.x AS nan",
]


def read(memgraph, query):
    rows = list(memgraph.execute_and_fetch(query))
    assert rows, f"no row for {query}"
    return rows[0]


def drop_vector_indexes(memgraph):
    # gqlalchemy's drop_indexes() renders a vector index as `DROP INDEX ON ::Label(prop)`,
    # which the server rejects, so the test removes its own indexes instead.
    for index in ("f32_idx", "f16_idx"):
        try:
            memgraph.execute(f"DROP VECTOR INDEX {index}")
        except Exception:  # noqa: BLE001 - absent after a failed re-import
            pass


def test_dump_float_roundtrip(memgraph):
    """Every float the dumper writes has to be a literal the parser accepts and
    re-parses to the stored value and kind: max_digits10, an integral float
    keeps its `.0`, and a non-finite value is written as an expression that
    produces it. Index-backed floats are spelled at the index's precision."""
    for query in SETUP:
        memgraph.execute(query)
    try:
        before = [read(memgraph, query) for query in PROBES]

        dump = list(memgraph.execute_and_fetch("DUMP DATABASE"))
        statements = [row["QUERY"] for row in dump]
        assert statements, "DUMP DATABASE returned no statements"

        memgraph.execute("MATCH (n) DETACH DELETE n")
        drop_vector_indexes(memgraph)

        failed = []
        for statement in statements:
            try:
                memgraph.execute(statement)
            except Exception as error:  # noqa: BLE001 - the parser error is the assertion
                failed.append((statement, error))
        assert not failed, "dumped statements did not re-import:\n" + "\n".join(
            f"{statement}\n  -> {error}" for statement, error in failed
        )

        after = [read(memgraph, query) for query in PROBES]

        scalar_before, scalar_after = before[0], after[0]
        for name in ("big", "large"):
            assert isinstance(scalar_after[name], float), f"{name} came back as {type(scalar_after[name])}"
            assert scalar_after[name] == scalar_before[name], f"{name}: {scalar_before[name]} -> {scalar_after[name]}"
        assert isinstance(scalar_after["integral"], float), f"integral came back as {type(scalar_after['integral'])}"
        assert scalar_after["integral"] == 2.0

        lists_before, lists_after = before[1], after[1]
        assert lists_after["doubles"] == lists_before["doubles"]
        assert all(isinstance(value, float) for value in lists_after["doubles"])
        assert lists_after["numerics"] == lists_before["numerics"]
        assert isinstance(lists_after["numerics"][0], int)
        assert all(isinstance(value, float) for value in lists_after["numerics"][1:])
        special = lists_after["special"]
        assert len(special) == 3
        assert math.isinf(special[0]) and special[0] > 0
        assert math.isinf(special[1]) and special[1] < 0
        assert math.isnan(special[2])

        assert after[2]["v"] == before[2]["v"], f"vector f32: {before[2]['v']} -> {after[2]['v']}"
        vec16 = after[3]["v"]
        assert len(vec16) == 1 and math.isinf(vec16[0]), f"vector f16: {vec16}"

        points_before, points_after = before[4], after[4]
        assert points_after["big"] == points_before["big"] == 1e20
        assert math.isinf(points_after["pinf"]) and points_after["pinf"] > 0
        assert math.isinf(points_after["ninf"]) and points_after["ninf"] < 0
        assert math.isnan(points_after["nan"])
    finally:
        drop_vector_indexes(memgraph)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
