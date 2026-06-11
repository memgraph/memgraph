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

import mgclient
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


class TestDeriveOverlayReadThrough:
    def test_reads_origin_property_value(self, connection):
        """An overlay node from derive() with no property override reads its origin's
        property values."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 7})-[:R]->(:N {id: 2, v: 9});")
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E'}) AS g
            UNWIND g.nodes AS n
            RETURN n.id AS id, n.v AS v ORDER BY id;
            """,
        )
        assert results == [(1, 7), (2, 9)]

    def test_read_is_lazy_not_a_copy(self, connection):
        """Reads fall through to the origin at read time, not a snapshot taken at
        construction: mutating the origin after derive() is visible through the
        overlay node. A copy would still show the pre-mutation value.
        """
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 1})-[:R]->(:N {id: 2, v: 2});")
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(a:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E'}) AS g, a
            SET a.v = 100
            RETURN [n IN g.nodes | n.v] AS vs;
            """,
        )
        # origin a.v is now 100, b.v stays 2; read-through reflects the mutation.
        assert sorted(results[0][0]) == [2, 100]

    def test_overlay_node_exposes_origin_properties(self, connection):
        """properties() over an overlay node returns the merged view (origin keys read
        through), so a returned overlay node carries its origin's properties."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 7})-[:R]->(:N {id: 2, v: 9});")
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E'}) AS g
            UNWIND g.nodes AS n
            WITH n WHERE n.id = 1
            RETURN properties(n) AS props;
            """,
        )
        assert results[0][0] == {"id": 1, "v": 7}


class TestDerivePerPropertyBinding:
    def _make_path(self, cursor):
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 7, secret: 42})-[:R]->(:N {id: 2, v: 9});")

    def test_hidden_property_invisible_to_reads(self, connection):
        """A property bound 'hidden' is absent from reads and from function calls over the
        node, while an unlisted property still reads through to the origin."""
        cursor = connection.cursor()
        self._make_path(cursor)
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E', propertyPolicy: {secret: 'hidden'}}) AS g
            UNWIND g.nodes AS n
            WITH n WHERE n.id = 1
            RETURN n.secret AS secret, n.v AS v, properties(n) AS props;
            """,
        )
        secret, v, props = results[0]
        assert secret is None
        assert v == 7  # unlisted -> read-through
        assert props == {"id": 1, "v": 7}  # 'secret' hidden from properties()

    def test_overlay_binding_with_override_shadows(self, connection):
        """A key bound 'overlay' with a sourceNodeProperties override shadows the origin
        value on read, and is not treated as a conflict."""
        cursor = connection.cursor()
        self._make_path(cursor)
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {
                virtualEdgeType: 'E',
                propertyPolicy: {v: 'overlay'},
                sourceNodeProperties: {v: 100}
            }) AS g
            UNWIND g.nodes AS n
            WITH n WHERE n.id = 1
            RETURN n.v AS v;
            """,
        )
        assert results[0][0] == 100

    def test_overlay_and_origin_binding_conflict_errors(self, connection):
        """Overlaying a key and binding it to 'origin' is a construction-time error."""
        cursor = connection.cursor()
        self._make_path(cursor)
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(
                cursor,
                """
                MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
                WITH derive(p, {
                    virtualEdgeType: 'E',
                    propertyPolicy: {v: 'origin'},
                    sourceNodeProperties: {v: 100}
                }) AS g
                RETURN g;
                """,
            )


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


class TestOverlayWriteBack:
    """SET through a derive() overlay node routes per the static per-property binding:
    a declared 'overlay' key mutates the overlay (compute-only, never persisted), while
    an 'origin'-bound or undeclared key persists to the real origin vertex."""

    def test_undeclared_key_persists_to_origin(self, connection):
        """A SET to a key the projection does not declare targets the origin and persists
        to the real node, observed by a follow-up MATCH in a separate query."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 1})-[:R]->(:N {id: 2, v: 2});")

        execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E'}) AS g
            UNWIND g.nodes AS n
            SET n.rank = 7
            RETURN count(*);
            """,
        )

        persisted = execute_and_fetch_all(cursor, "MATCH (m:N) RETURN m.id AS id, m.rank AS rank ORDER BY id;")
        assert persisted == [(1, 7), (2, 7)]

    def test_origin_bound_key_persists_to_origin(self, connection):
        """A SET to a key bound 'origin' by propertyPolicy persists to the real node."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 1})-[:R]->(:N {id: 2, v: 2});")

        execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E', propertyPolicy: {v: 'origin'}}) AS g
            UNWIND g.nodes AS n
            SET n.v = 99
            RETURN count(*);
            """,
        )

        persisted = execute_and_fetch_all(cursor, "MATCH (m:N) RETURN m.id AS id, m.v AS v ORDER BY id;")
        assert persisted == [(1, 99), (2, 99)]

    def test_overlay_key_updates_overlay_and_does_not_persist(self, connection):
        """A SET to a key declared 'overlay' (with no origin override value) mutates the
        overlay - the in-query read shows the new value - but the real node is untouched."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 1})-[:R]->(:N {id: 2, v: 2});")

        in_query = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E', propertyPolicy: {v: 'overlay'}}) AS g
            UNWIND g.nodes AS n
            SET n.v = 99
            RETURN collect(n.v) AS vs;
            """,
        )
        assert in_query[0][0] == [99, 99]

        persisted = execute_and_fetch_all(cursor, "MATCH (m:N) RETURN m.id AS id, m.v AS v ORDER BY id;")
        assert persisted == [(1, 1), (2, 2)], "overlay write must not touch the real node"

    def test_read_after_write_through_overlay_has_no_stale_shadow(self, connection):
        """After an origin-bound write, reading the same key through the overlay node returns
        the just-written value (read-through reflects the persisted write, no stale overlay)."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 1})-[:R]->(:N {id: 2, v: 2});")

        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E'}) AS g
            UNWIND g.nodes AS n
            SET n.v = n.v + 100
            RETURN n.id AS id, n.v AS v ORDER BY id;
            """,
        )
        assert results == [(1, 101), (2, 102)]

        persisted = execute_and_fetch_all(cursor, "MATCH (m:N) RETURN m.id AS id, m.v AS v ORDER BY id;")
        assert persisted == [(1, 101), (2, 102)]

    def test_algorithm_write_back_persists_scores_onto_origin(self, connection):
        """An algorithm-shaped procedure over a derive() projection yields overlay nodes; a
        SET on a yielded node persists the computed score onto the real origin node."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(
            cursor,
            """
            CREATE (a1:Person {name: 'A1', dept: 'eng'}),
                   (a2:Person {name: 'A2', dept: 'eng'}),
                   (a3:Person {name: 'A3', dept: 'sales'}),
                   (c1:Company {name: 'ACME'}),
                   (a1)-[:WORKS_AT]->(c1),
                   (a2)-[:WORKS_AT]->(c1),
                   (a3)-[:WORKS_AT]->(c1);
            """,
        )

        execute_and_fetch_all(
            cursor,
            """
            MATCH p=(a:Person)-[:WORKS_AT]->(:Company)<-[:WORKS_AT]-(b:Person)
            WHERE id(a) < id(b)
            WITH derive(p, {
                virtualEdgeType: 'COLLEAGUES',
                sourceNodeProperties: {dept: a.dept},
                targetNodeProperties: {dept: b.dept}
            }) AS subgraph
            CALL read.community_label(subgraph, 'dept') YIELD node, community_id
            SET node.community = community_id
            RETURN count(*);
            """,
        )

        persisted = execute_and_fetch_all(
            cursor, "MATCH (p:Person) RETURN p.name AS name, p.community AS community ORDER BY name;"
        )
        by_person = {name: community for name, community in persisted}
        assert all(c is not None for c in by_person.values()), "community must persist on every real node"
        assert by_person["A1"] == by_person["A2"], "A1 and A2 both in eng -> same community"
        assert by_person["A1"] != by_person["A3"], "A3 in sales -> different community"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
