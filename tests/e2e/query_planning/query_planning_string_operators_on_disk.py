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
from common import memgraph

# The disk backend keeps a one-sided range inside its own type by comparing each bound's type to the
# value's. That rejects every value of a whole-type range -- the shape CONTAINS, ENDS WITH and an empty
# STARTS WITH prefix emit -- so all three silently returned nothing once they became index candidates.
#
# Its own workload: the switch to on-disk needs an empty database, and there is no way back without a
# restart, so this cannot share a server with the in-memory tests.


def _count(memgraph, query):
    return list(memgraph.execute_and_fetch(query))[0]["c"]


@pytest.fixture
def disk_graph(memgraph):
    memgraph.execute("STORAGE MODE ON_DISK_TRANSACTIONAL;")
    memgraph.execute("CREATE INDEX ON :DS(t);")
    memgraph.execute("CREATE (:DS {t: 'ab'});")
    memgraph.execute("CREATE (:DS {t: 'b'});")
    yield memgraph


@pytest.mark.parametrize(
    "predicate,expected",
    [
        ("n.t CONTAINS 'b'", 2),
        ("n.t ENDS WITH 'b'", 2),
        ("n.t STARTS WITH ''", 2),
        ("n.t STARTS WITH 'ab'", 1),
        ("n.t =~ 'b.*'", 1),
    ],
)
def test_string_predicates_on_disk(memgraph, disk_graph, predicate, expected):
    assert _count(memgraph, f"MATCH (n:DS) WHERE {predicate} RETURN count(*) AS c") == expected


def test_one_sided_range_stays_in_its_type_on_disk(memgraph, disk_graph):
    memgraph.execute("CREATE (:DS {t: 1});")
    assert _count(memgraph, "MATCH (n:DS) WHERE n.t >= 1 RETURN count(*) AS c") == 1
    assert _count(memgraph, "MATCH (n:DS) WHERE n.t >= 'a' RETURN count(*) AS c") == 2


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
