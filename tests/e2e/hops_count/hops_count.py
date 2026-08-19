# Copyright 2024 Memgraph Ltd.
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

URI = "bolt://localhost:7687"
AUTH = ("", "")


def execute_query(query: str):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            session.run(query)


def get_summary(query: str):
    with GraphDatabase.driver(URI, auth=AUTH) as driver:
        with driver.session() as session:
            result = session.run(query)
            return result.consume().metadata


def test_hops_count_1():
    # prepare simple graph
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        "CREATE (a:Person {name: 'Alice'}) "
        "CREATE (b:Person {name: 'Bob'}) "
        "CREATE (c:Person {name: 'Charlie'}) "
        "CREATE (d:Person {name: 'David'}) "
        "CREATE (e:Person {name: 'Eve'}) "
        "CREATE (a)-[:KNOWS]->(b) "
        "CREATE (b)-[:KNOWS]->(c) "
        "CREATE (c)-[:KNOWS]->(d) "
        "CREATE (d)-[:KNOWS]->(e)"
    )

    # check hops count

    # expand variable
    number_of_hops = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS*]->(e:Person {name: 'Eve'}) RETURN e")[
        "number_of_hops"
    ]
    assert number_of_hops == 4

    number_of_hops = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..2]->(e:Person {name: 'Eve'}) RETURN e")[
        "number_of_hops"
    ]
    assert number_of_hops == 2

    number_of_hops = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS*1..3]->(e:Person {name: 'Eve'}) RETURN e")[
        "number_of_hops"
    ]
    assert number_of_hops == 3

    number_of_hops = get_summary("MATCH (a:Person)-[:KNOWS*]->(e:Person) RETURN e")["number_of_hops"]
    assert number_of_hops == 10

    number_of_hops = get_summary("MATCH (a:Person)-[:KNOWS*]-(e:Person) RETURN e")["number_of_hops"]
    assert number_of_hops == 40  # already visited nodes are counted

    # bfs expand
    number_of_hops = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS *BFS]->(e:Person {name: 'Eve'}) RETURN e")[
        "number_of_hops"
    ]
    assert number_of_hops == 4

    number_of_hops = get_summary(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS *BFS 1..2]->(e:Person {name: 'Eve'}) RETURN e"
    )["number_of_hops"]
    assert number_of_hops == 2

    number_of_hops = get_summary(
        "MATCH (a:Person {name: 'Alice'})-[:KNOWS *BFS 1..3]->(e:Person {name: 'Eve'}) RETURN e"
    )["number_of_hops"]
    assert number_of_hops == 3

    number_of_hops = get_summary("MATCH (a:Person)-[:KNOWS *BFS]->(e:Person) RETURN e")["number_of_hops"]
    assert number_of_hops == 10

    number_of_hops = get_summary("MATCH (a:Person)-[:KNOWS *BFS]-(e:Person) RETURN e")["number_of_hops"]
    assert number_of_hops == 40

    # kshortest expand
    number_of_hops = get_summary(
        "MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'}) WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST]->(e) RETURN r"
    )["number_of_hops"]
    assert number_of_hops == 6

    number_of_hops = get_summary(
        "USING HOPS LIMIT 4 MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'}) WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST]->(e) RETURN r"
    )["number_of_hops"]
    assert number_of_hops == 4

    # kshortest with limit parameter
    number_of_hops = get_summary(
        "MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'}) WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST|2]->(e) RETURN r"
    )["number_of_hops"]
    assert number_of_hops == 6

    number_of_hops = get_summary(
        "MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'}) WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST|1]->(e) RETURN r"
    )["number_of_hops"]
    assert number_of_hops == 4

    # expand
    number_of_hops = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(e:Person {name: 'Eve'}) RETURN e")[
        "number_of_hops"
    ]
    assert number_of_hops == 1


def test_hops_count_2():
    # prepare simple graph
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        "CREATE (a:Person {name: 'Alice'}) "
        "CREATE (b:Person {name: 'Bob'}) "
        "CREATE (c:Person {name: 'Charlie'}) "
        "CREATE (d:Car {name: 'Audi'}) "
        "CREATE (e:Car {name: 'BMW'}) "
        "CREATE (a)-[:DRIVES {since: 2010}]->(d) "
        "CREATE (a)-[:DRIVES {since: 2015}]->(e) "
        "CREATE (b)-[:DRIVES {since: 2015}]->(e) "
        "CREATE (c)-[:DRIVES {since: 2015}]->(e)"
    )

    # check hops count

    # expand variable
    number_of_hops = get_summary("MATCH (a:Person {name: 'Alice'})-[:DRIVES*]->(e:Car {name: 'BMW'}) RETURN e")[
        "number_of_hops"
    ]
    assert number_of_hops == 2

    number_of_hops = get_summary(
        "MATCH (a:Person {name: 'Alice'})-[:DRIVES* (r, n | r.since = 2015)]->(e:Car) RETURN e;"
    )["number_of_hops"]
    assert number_of_hops == 2

    number_of_hops = get_summary(
        "MATCH (a:Person {name: 'Alice'})-[:DRIVES* (r, n | r.since = 2015)]-(e:Car) RETURN e;"
    )["number_of_hops"]
    assert number_of_hops == 7  # scans by a and then expand to e

    # bfs expand
    number_of_hops = get_summary("MATCH (a:Person {name: 'Alice'})-[:DRIVES *BFS]->(e:Car {name: 'BMW'}) RETURN e")[
        "number_of_hops"
    ]
    assert number_of_hops == 2  # first does scan by a and then expand to e

    number_of_hops = get_summary("MATCH (a:Person)-[:DRIVES *BFS (r, n | r.since = 2015)]->(e:Car) RETURN e;")[
        "number_of_hops"
    ]
    assert number_of_hops == 4

    number_of_hops = get_summary("MATCH (a:Person)-[:DRIVES *BFS (r, n | r.since = 2015)]-(e:Car) RETURN e;")[
        "number_of_hops"
    ]
    assert number_of_hops == 21

    # expand
    number_of_hops = get_summary("MATCH (a:Person {name: 'Alice'})-[:DRIVES]->(e:Car {name: 'BMW'}) RETURN e")[
        "number_of_hops"
    ]
    assert number_of_hops == 2  # scans by a and then expand to e

    number_of_hops = get_summary("MATCH (a:Person)-[:DRIVES]->(e:Car) RETURN e")["number_of_hops"]
    assert number_of_hops == 4  # scans by e and then expand to a


def test_hops_count_3():
    # prepare simple graph
    execute_query("MATCH (n) DETACH DELETE n")
    execute_query(
        "CREATE (a:Person {name: 'Alice'}) "
        "CREATE (b:Person {name: 'Bob'}) "
        "CREATE (c:Person {name: 'Charlie'}) "
        "CREATE (d:Person {name: 'David'}) "
        "CREATE (e:Person {name: 'Eve'}) "
        "CREATE (a)-[:KNOWS]->(b) "
        "CREATE (a)-[:FRIENDS]->(c) "
        "CREATE (b)-[:KNOWS]->(d) "
        "CREATE (b)-[:FRIENDS]->(e) "
    )

    # check hops count

    # expand variable
    number_of_hops = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS*..1]->(e:Person) RETURN e")["number_of_hops"]
    assert number_of_hops == 2  # scans by a and then expands to e

    # bfs expand
    number_of_hops = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS *BFS ..1]->(e:Person) RETURN e")[
        "number_of_hops"
    ]
    assert number_of_hops == 2  # first does scan by a and then expand to e

    # kshortest expand
    number_of_hops = get_summary(
        "MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'}) WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST ..1]->(e) RETURN r"
    )["number_of_hops"]
    assert number_of_hops == 2

    # kshortest with limit parameter
    number_of_hops = get_summary(
        "MATCH (a:Person {name: 'Alice'}),(e:Person {name: 'Eve'}) WITH a, e MATCH (a)-[r:KNOWS *KSHORTEST|1]->(e) RETURN r"
    )["number_of_hops"]
    assert number_of_hops == 3

    # expand
    number_of_hops = get_summary("MATCH (a:Person {name: 'Alice'})-[:KNOWS]->(e:Person) RETURN e")["number_of_hops"]
    assert number_of_hops == 2  # scans by a and then expand to e


def test_hops_count_kshortest_bounded_spur():
    # A spur search after a deviation only needs the depth left over after the deviation, not the
    # whole maximum depth. Every other KSHORTEST case here is unbounded or `..1`, where that is
    # inert: unbounded leaves the spur bound unbounded too, and at `..1` only the deviation at
    # index 0 exists, whose remaining depth is the full bound.
    execute_query("MATCH (n) DETACH DELETE n")
    # a->b->c->t is the shortest path; a->b->m->t is the second. Deviating at c must not walk the
    # dead-end c->p->q->r, which is only reachable within the *full* bound, never within what is
    # left after two hops.
    execute_query(
        "CREATE (a:N {n: 'a'}), (b:N {n: 'b'}), (c:N {n: 'c'}), (t:N {n: 't'}), (m:N {n: 'm'}), "
        "       (p:N {n: 'p'}), (q:N {n: 'q'}), (r:N {n: 'r'}) "
        "CREATE (a)-[:E]->(b), (b)-[:E]->(c), (c)-[:E]->(t), "
        "       (b)-[:E]->(m), (m)-[:E]->(t), "
        "       (c)-[:E]->(p), (p)-[:E]->(q), (q)-[:E]->(r)"
    )

    prefix = "MATCH (a:N {n: 'a'}), (t:N {n: 't'}) WITH a, t MATCH (a)-[r:E *KSHORTEST"

    # Both paths are found either way - only the hop budget differs, which is what makes this a
    # hops test and not a results test. Bounding the spur by the full depth costs 9 here.
    summary = get_summary(f"{prefix} 1..3]->(t) RETURN r")
    assert summary["number_of_hops"] == 8

    # A looser bound wastes more: the dead-end chain is three hops long, so the whole of it used to
    # be walked. This cost 10 before.
    summary = get_summary(f"{prefix} 1..4]->(t) RETURN r")
    assert summary["number_of_hops"] == 8

    # Control: with no upper bound there is nothing to subtract, so the count is unchanged. This
    # pins that the two assertions above are about the bound and not about the graph.
    summary = get_summary(f"{prefix}]->(t) RETURN r")
    assert summary["number_of_hops"] == 10


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
