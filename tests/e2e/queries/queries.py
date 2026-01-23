# Copyright 2023 Memgraph Ltd.
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
from common import memgraph


def test_indexed_join_with_indices(memgraph):
    memgraph.execute(
        "CREATE (c:A {prop: 1})-[b:TYPE]->(p:A {prop: 1}) CREATE (cf:B:A {prop : 1}) CREATE (pf:B:A {prop : 1});"
    )
    memgraph.execute("CREATE INDEX ON :A;")
    memgraph.execute("CREATE INDEX ON :B;")
    memgraph.execute("CREATE INDEX ON :A(prop);")
    memgraph.execute("CREATE INDEX ON :B(prop);")

    results = list(
        memgraph.execute_and_fetch(
            "match (c:A)-[b:TYPE]->(p:A) match (cf:B:A {prop : c.prop}) match (pf:B:A {prop : p.prop}) return c;"
        )
    )

    assert len(results) == 4
    for res in results:
        assert res["c"].prop == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
