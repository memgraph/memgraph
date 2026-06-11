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
from common import execute_and_fetch_all


class TestVirtualEdgesWithProcedures:
    @pytest.mark.parametrize(
        "extra_options, expected_weight",
        [
            ("", None),
            (", relationshipProperties: {weight: 99}", 99),
        ],
    )
    def test_procedure_sees_virtual_edges(self, connection, extra_options, expected_weight):
        """A procedure iterating a virtual node's out_edges should see virtual edges,
        optionally carrying relationshipProperties."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1})-[:R]->(:N {id: 2})-[:R]->(:N {id: 3});")

        results = execute_and_fetch_all(
            cursor,
            f"""
            MATCH p=(:N {{id: 1}})-[*]->(:N {{id: 3}})
            WITH derive(p, {{virtualEdgeType: 'DERIVED'{extra_options}}}) AS graph
            UNWIND graph.nodes AS n
            CALL read.subgraph_edge_info(graph, n) YIELD edge_type, weight
            RETURN edge_type, weight
            """,
        )

        assert len(results) == 1
        assert results[0][0] == "DERIVED"
        assert results[0][1] == expected_weight

    def test_procedure_sees_virtual_node_labels_and_properties(self, connection):
        """Virtual node labels and properties should be visible to procedures."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1})-[:R]->(:N {id: 2});")

        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {
                virtualEdgeType: 'V',
                sourceNodeLabels: ['Expert'],
                sourceNodeProperties: {score: 42}
            }) AS graph
            UNWIND graph.nodes AS n
            WITH graph, n WHERE 'Expert' IN labels(n)
            CALL read.subgraph_vertex_info(graph, n) YIELD labels, score
            RETURN labels, score
            """,
        )

        assert len(results) == 1
        assert "Expert" in results[0][0]
        assert results[0][1] == 42

    def test_match_derive_call_pipeline(self, connection):
        """Full pipeline: MATCH paths → derive() → CALL a graph-algo-shaped procedure → return results.

        Mirrors the intended community_detection.get(subgraph) shape, but calls a
        user-defined procedure (read.community_label) since bundled MAGE algorithms
        don't yet declare a Graph argument.
        """
        cursor = connection.cursor()
        execute_and_fetch_all(
            cursor,
            """
            CREATE (a1:Person {name: 'A1', dept: 'eng'}),
                   (a2:Person {name: 'A2', dept: 'eng'}),
                   (a3:Person {name: 'A3', dept: 'sales'}),
                   (c1:Company {name: 'ACME'}),
                   (c2:Company {name: 'Beta'}),
                   (a1)-[:WORKS_AT]->(c1),
                   (a2)-[:WORKS_AT]->(c1),
                   (a3)-[:WORKS_AT]->(c1),
                   (a3)-[:WORKS_AT]->(c2);
            """,
        )

        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(a:Person)-[:WORKS_AT]->(:Company)<-[:WORKS_AT]-(b:Person)
            WHERE id(a) < id(b)
            WITH derive(p, {
                virtualEdgeType: 'COLLEAGUES',
                relationshipProperties: {strength: 1},
                sourceNodeLabels: ['Employee'],
                targetNodeLabels: ['Employee'],
                sourceNodeProperties: {name: a.name, dept: a.dept},
                targetNodeProperties: {name: b.name, dept: b.dept}
            }) AS subgraph
            CALL read.community_label(subgraph, 'dept') YIELD node, community_id
            RETURN node.name AS person, community_id
            ORDER BY community_id, person;
            """,
        )

        # Three colleague relationships in the real graph: (a1,a2), (a1,a3), (a2,a3) — all via ACME.
        # derive() produces 3 virtual nodes {a1, a2, a3} and 3 virtual edges between them.
        # community_label groups by dept: {eng: a1, a2; sales: a3}.
        assert len(results) == 3
        people = sorted(r[0] for r in results)
        assert people == ["A1", "A2", "A3"]

        by_person = {r[0]: r[1] for r in results}
        assert by_person["A1"] == by_person["A2"], "A1 and A2 both in eng → same community"
        assert by_person["A1"] != by_person["A3"], "A3 in sales → different community"


class TestVirtualNodeConstructor:
    def test_construct_with_label_list(self, connection):
        """virtualNode(gid, [labels], props) yields a synthetic node carrying the
        given labels and properties."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH virtualNode(1, ['Expert', 'Analyst'], {score: 42, name: 'A'}) AS n
            RETURN labels(n) AS labels, n.score AS score, n.name AS name;
            """,
        )

        assert len(results) == 1
        labels, score, name = results[0]
        assert sorted(labels) == ["Analyst", "Expert"]
        assert score == 42
        assert name == "A"

    def test_construct_with_single_label(self, connection):
        """The single-label form virtualNode(gid, 'Label', props) is accepted and
        yields a node with that one label."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            "WITH virtualNode(1, 'Expert', {score: 42}) AS n RETURN labels(n) AS labels, n.score AS score;",
        )

        assert len(results) == 1
        assert results[0][0] == ["Expert"]
        assert results[0][1] == 42

    def test_synthetic_id_distinct_from_real(self, connection):
        """A constructed node carries a synthetic GID (counted down from the top of
        the id space), never the user-supplied handle and never a real node's id."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:Real);")
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH (r:Real)
            WITH r, virtualNode(1, 'V', {}) AS n
            RETURN id(n) AS vnid, id(r) AS rid;
            """,
        )

        assert len(results) == 1
        vnid, rid = results[0]
        # Real gids count up from 0; synthetic gids count down from UINT64_MAX and
        # decode as negative signed ints. The synthetic id is not the handle (1).
        assert vnid < 0
        assert vnid != rid
        assert vnid != 1


class TestVirtualNodeSet:
    def test_set_single_property_overwrites_and_adds(self, connection):
        """SET n.key = value on a synthetic node overwrites an existing overlay key
        and creates a previously-absent one; a read in the same query sees the write."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH virtualNode(1, 'V', {x: 1}) AS n
            SET n.x = 2
            SET n.y = 9
            RETURN n.x AS x, n.y AS y;
            """,
        )
        assert results == [(2, 9)]

    def test_set_single_property_null_removes_key(self, connection):
        """SET n.key = null removes the overlay key, matching real-node semantics."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH virtualNode(1, 'V', {x: 1, y: 2}) AS n
            SET n.x = null
            RETURN n.x AS x, properties(n) AS props;
            """,
        )
        assert len(results) == 1
        x, props = results[0]
        assert x is None
        assert props == {"y": 2}

    def test_set_map_merge_updates_overlay(self, connection):
        """SET n += {..} merges the map into the overlay, keeping untouched keys."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH virtualNode(1, 'V', {x: 1}) AS n
            SET n += {y: 2, z: 3}
            RETURN properties(n) AS props;
            """,
        )
        assert results[0][0] == {"x": 1, "y": 2, "z": 3}

    def test_set_map_replace_clears_overlay(self, connection):
        """SET n = {..} replaces the whole overlay: prior keys are gone."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH virtualNode(1, 'V', {x: 1, y: 2}) AS n
            SET n = {z: 3}
            RETURN properties(n) AS props;
            """,
        )
        assert results[0][0] == {"z": 3}

    def test_set_does_not_error_on_synthetic_node(self, connection):
        """A SET on a synthetic node never raises, regardless of form."""
        cursor = connection.cursor()
        # Each form should run without raising.
        execute_and_fetch_all(cursor, "WITH virtualNode(1, 'V', {}) AS n SET n.a = 1 RETURN n;")
        execute_and_fetch_all(cursor, "WITH virtualNode(1, 'V', {}) AS n SET n += {b: 1} RETURN n;")
        execute_and_fetch_all(cursor, "WITH virtualNode(1, 'V', {a: 1}) AS n SET n = {c: 1} RETURN n;")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
