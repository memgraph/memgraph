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

    def test_property_size_matches_real_node(self, connection):
        """propertySize over a synthetic node reports the value's encoded size, the
        same metric as a real node carrying that property (transparency); an absent
        key reports 0."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:Ref {name: 'hello', score: 42});")

        real_name, real_score = execute_and_fetch_all(
            cursor,
            "MATCH (n:Ref) RETURN propertySize(n, 'name') AS name, propertySize(n, 'score') AS score;",
        )[0]
        virtual_name, virtual_score, virtual_absent = execute_and_fetch_all(
            cursor,
            """
            WITH virtualNode(1, ['Ref'], {name: 'hello', score: 42}) AS n
            RETURN propertySize(n, 'name') AS name,
                   propertySize(n, 'score') AS score,
                   propertySize(n, 'absent') AS absent;
            """,
        )[0]

        assert virtual_name == real_name > 0
        assert virtual_score == real_score > 0
        assert virtual_absent == 0

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


class TestVirtualEdgeConstructor:
    def test_construct_between_virtual_nodes(self, connection):
        """virtualEdge(type, from, to) given two virtual nodes wires an edge whose
        endpoints are those nodes' synthetic ids; it carries its type and its own
        synthetic id."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH virtualNode(1, 'A', {}) AS a, virtualNode(2, 'B', {}) AS b
            WITH a, b, virtualEdge('KNOWS', a, b) AS e
            RETURN e, id(a) AS aid, id(b) AS bid, id(e) AS eid, type(e) AS etype;
            """,
        )

        assert len(results) == 1
        e, aid, bid, eid, etype = results[0]
        assert etype == "KNOWS"
        assert e.type == "KNOWS"
        # Endpoints are the nodes' synthetic ids; the edge carries its own synthetic id.
        assert e.start_id == aid
        assert e.end_id == bid
        assert aid < 0 and bid < 0 and eid < 0

    def test_construct_between_gid_handles(self, connection):
        """virtualEdge(type, 1, 2) given gid handles wires an edge between those
        gids; standalone it serializes with the handles as its endpoints, awaiting
        binding to nodes at projection assembly."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            "WITH virtualEdge('LINKS', 1, 2) AS e RETURN e, id(e) AS eid, type(e) AS etype;",
        )

        assert len(results) == 1
        e, eid, etype = results[0]
        assert etype == "LINKS"
        assert e.type == "LINKS"
        assert e.start_id == 1
        assert e.end_id == 2
        assert eid < 0

    def test_mixed_node_and_handle_endpoints(self, connection):
        """The two endpoint forms may be mixed: one virtual node, one gid handle."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH virtualNode(7, 'A', {}) AS a
            WITH a, virtualEdge('M', a, 2) AS e
            RETURN e, id(a) AS aid;
            """,
        )

        assert len(results) == 1
        e, aid = results[0]
        assert e.start_id == aid
        assert aid < 0
        assert e.end_id == 2

    def test_real_vertex_endpoint_is_rejected(self, connection):
        """A real vertex is not a valid endpoint; the error points at id() as the
        way to wire a real node by handle."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:Real);")
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(cursor, "MATCH (r:Real) RETURN virtualEdge('T', r, r) AS e;")

    def test_endpoint_node_of_unresolved_edge_errors(self, connection):
        """startNode/endNode on an edge with an unresolved handle endpoint is an
        error: it has no endpoint node until assembly binds it."""
        cursor = connection.cursor()
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(cursor, "WITH virtualEdge('T', 1, 2) AS e RETURN startNode(e);")


class TestVirtualGraphConstructor:
    def test_return_graph_value_serializes_as_nodes_and_edges(self, connection):
        """RETURN of a whole projection value decodes client-side as a map of node and edge lists
        (the ToBoltVirtualGraph path, distinct from consuming the value via USE or a procedure)."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 10}), virtualNode(2, 'N', {x: 20})] AS ns, [virtualEdge('R', 1, 2)] AS es
            RETURN virtualGraph(ns, es) AS g;
            """,
        )
        assert len(results) == 1
        g = results[0][0]
        assert set(g.keys()) == {"nodes", "edges"}
        assert {n.properties["x"] for n in g["nodes"]} == {10, 20}
        assert len(g["edges"]) == 1
        assert g["edges"][0].type == "R"

    def test_assembles_graph_consumable_by_procedure(self, connection):
        """virtualGraph(nodes, edges) imports a graph from lists; an edge given by
        handles binds to the listed nodes, and a procedure can read it."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'Person', {name: 'A'}), virtualNode(2, 'Person', {name: 'B'})] AS nodes,
                 [virtualEdge('KNOWS', 1, 2)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            UNWIND g.nodes AS n
            CALL read.subgraph_edge_info(g, n) YIELD edge_type, weight
            RETURN edge_type;
            """,
        )

        # Node 1 has an out-edge to node 2; node 2 has none, so a single edge record.
        assert len(results) == 1
        assert results[0][0] == "KNOWS"

    def test_dangling_edge_errors_by_default(self, connection):
        """An edge whose endpoint matches no listed node aborts construction by
        default."""
        cursor = connection.cursor()
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(
                cursor,
                """
                WITH [virtualNode(1, 'N', {})] AS nodes, [virtualEdge('R', 1, 2)] AS edges
                RETURN virtualGraph(nodes, edges) AS g;
                """,
            )

    def test_drop_mode_omits_dangling_edge(self, connection):
        """onDanglingEdge: 'drop' omits the dangling edge and the construction
        succeeds with the surviving nodes."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {})] AS nodes, [virtualEdge('R', 1, 2)] AS edges
            WITH virtualGraph(nodes, edges, {onDanglingEdge: 'drop'}) AS g
            RETURN size(g.nodes) AS node_count, size(g.edges) AS edge_count;
            """,
        )

        assert len(results) == 1
        assert results[0][0] == 1  # the one supplied node
        assert results[0][1] == 0  # the dangling edge was dropped

    def test_nulls_in_lists_are_skipped(self, connection):
        """Nulls in either list are ignored rather than rejected."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {}), null] AS nodes, [null] AS edges
            WITH virtualGraph(nodes, edges) AS g
            RETURN size(g.nodes) AS node_count;
            """,
        )

        assert len(results) == 1
        assert results[0][0] == 1

    def test_real_node_in_list_is_rejected(self, connection):
        """A real vertex is not a synthetic element; the node list rejects it."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:Real);")
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(cursor, "MATCH (r:Real) RETURN virtualGraph([r], []) AS g;")


class TestDeriveOverlayReadThrough:
    def test_bare_derive_projects_single_node_without_edge_type(self, connection):
        """A virtualEdgeType is only needed to project edges. derive(p, {}) over a
        single-vertex path yields the overlay node, reading through to its origin,
        with no edge type required."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 7});")
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})
            WITH derive(p, {}) AS g
            UNWIND g.nodes AS n
            RETURN n.v AS v;
            """,
        )
        assert len(results) == 1
        assert results[0][0] == 7  # read through to the origin

    def test_bare_derive_errors_when_path_has_edges(self, connection):
        """A path with edges still requires a virtualEdgeType to name the derived
        edge."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1})-[:R]->(:N {id: 2});")
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(
                cursor,
                "MATCH p=(:N {id: 1})-[:R]->(:N {id: 2}) WITH derive(p, {}) AS g RETURN g;",
            )

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


class TestBoltOverlaySerialization:
    """An overlay node serializes over Bolt at its origin's identity (so a client maps it
    back to the real node) with the merged property view; a synthetic node keeps its
    synthetic id. The Bolt Node structure shape is unchanged - only which id and which
    properties are written."""

    def test_overlay_node_serializes_at_origin_id_with_merged_properties(self, connection):
        """Each overlay node decodes with its origin's Bolt id and the origin's property
        map (no override given, so the merged view equals the origin's properties)."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 1})-[:R]->(:N {id: 2, v: 2});")

        reals = execute_and_fetch_all(cursor, "MATCH (m:N) RETURN m;")
        real_by_pid = {node.properties["id"]: node for (node,) in reals}

        rows = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E'}) AS g
            UNWIND g.nodes AS o
            RETURN o;
            """,
        )

        assert len(rows) == 2
        for (overlay,) in rows:
            real = real_by_pid[overlay.properties["id"]]
            assert overlay.id == real.id, "overlay node must serialize at its origin's Bolt id"
            origin_props = {k: v for k, v in overlay.properties.items() if not k.startswith("__mg")}
            assert origin_props == real.properties
            assert overlay.labels == real.labels

    def test_overlay_value_shadows_origin_in_serialized_map(self, connection):
        """A construction-time override shadows the origin value in the serialized property
        map, while the node still carries its origin's id."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 1})-[:R]->(:N {id: 2, v: 2});")

        real_source = {
            node.properties["id"]: node for (node,) in execute_and_fetch_all(cursor, "MATCH (m:N) RETURN m;")
        }[1]

        rows = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E', sourceNodeProperties: {v: 999}}) AS g
            UNWIND g.nodes AS o
            RETURN o;
            """,
        )

        source = next(o for (o,) in rows if o.properties["id"] == 1)
        assert source.id == real_source.id
        assert source.properties["v"] == 999, "overlay value shadows the origin in the serialized map"

    def test_synthetic_node_keeps_synthetic_id(self, connection):
        """A synthetic node (no origin) serializes with its own synthetic id, distinct from
        any real node's id."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:Real);")

        real = execute_and_fetch_all(cursor, "MATCH (r:Real) RETURN r;")[0][0]
        synthetic = execute_and_fetch_all(cursor, "RETURN virtualNode(1, 'V', {x: 1}) AS n;")[0][0]

        assert synthetic.id < 0, "synthetic gids count down from the top of the id space"
        assert synthetic.id != real.id
        assert synthetic.properties == {"x": 1}


class TestBoltProvenanceTag:
    """Each overlay node from derive() carries a reserved projection-tag property
    (__mg_overlay_ref, a plain Int) referencing its projection-schema entry. Every overlay
    node from one derive() shares the tag value (one schema entry per derive() site). Real
    nodes and synthetic virtualNode() nodes carry no tag. The tag is an ordinary Int
    property, so a generic client decodes the result unchanged and scalar values are sent
    unwrapped."""

    TAG = "__mg_overlay_ref"

    def test_overlay_nodes_carry_a_shared_int_tag(self, connection):
        """Both overlay nodes from a single derive() carry __mg_overlay_ref as an int, and
        the value is the same on both - they reference one schema entry."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 1})-[:R]->(:N {id: 2, v: 2});")

        rows = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E'}) AS g
            UNWIND g.nodes AS o
            RETURN o;
            """,
        )

        assert len(rows) == 2
        tags = [o.properties.get(self.TAG) for (o,) in rows]
        assert all(isinstance(t, int) for t in tags), "every overlay node carries an int tag"
        assert tags[0] == tags[1], "all overlay nodes from one derive() share the tag value"

    def test_real_node_carries_no_tag(self, connection):
        """A node from a plain MATCH belongs to no projection, so it carries no tag."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 7});")

        real = execute_and_fetch_all(cursor, "MATCH (n:N) RETURN n;")[0][0]
        assert self.TAG not in real.properties

    def test_synthetic_node_carries_no_tag(self, connection):
        """A synthetic virtualNode() has no origin and no projection, so it carries no tag;
        its scalar properties arrive unwrapped."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")

        synthetic = execute_and_fetch_all(cursor, "RETURN virtualNode(1, 'V', {x: 1}) AS n;")[0][0]
        assert self.TAG not in synthetic.properties
        assert synthetic.properties == {"x": 1}

    def test_tagged_overlay_keeps_origin_scalars_unwrapped(self, connection):
        """Tagging the node does not wrap the origin's scalar property values - they decode
        as plain scalars alongside the int tag."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 7, name: 'x'})-[:R]->(:N {id: 2});")

        rows = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E'}) AS g
            UNWIND g.nodes AS o
            RETURN o;
            """,
        )

        source = next(o for (o,) in rows if o.properties.get("id") == 1)
        assert source.properties["v"] == 7
        assert source.properties["name"] == "x"

    def test_param_options_derive_emits_no_tag(self, connection):
        """When derive() options are not a static map literal (here, the whole options map
        is a query parameter), the projection schema is unknowable before execution. The
        overlay nodes still serialize at their origin identity, but carry no tag - there is
        no schema entry for a projection-aware client to point them at."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1})-[:R]->(:N {id: 2});")

        rows = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, $opts) AS g
            UNWIND g.nodes AS o
            RETURN o;
            """,
            {"opts": {"virtualEdgeType": "E"}},
        )

        assert len(rows) == 2
        for (o,) in rows:
            assert self.TAG not in o.properties, "a projection with non-literal options carries no tag"


class TestUseScope:
    def test_use_scope_scans_projection(self, connection):
        """`CALL { USE g MATCH (n) ... }` runs the MATCH over the bound
        projection, returning its nodes."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 10}), virtualNode(2, 'N', {x: 20})] AS nodes, [] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (n) RETURN n.x AS x }
            RETURN x ORDER BY x;
            """,
        )
        assert results == [(10,), (20,)]

    def test_use_scope_is_scoped_to_the_projection(self, connection):
        """The bound view applies inside the block only: a MATCH outside sees the
        real graph, a MATCH inside sees the projection."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE (:Real), (:Real), (:Real);")
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH (r) WITH count(r) AS real_count
            WITH real_count, virtualGraph([virtualNode(1, 'N', {x: 10})], []) AS g
            CALL { USE g MATCH (n) RETURN n.x AS x }
            RETURN real_count, x;
            """,
        )
        assert results == [(3, 10)]

    def test_write_in_use_scope_errors(self, connection):
        """A USE scope is read-only: a write clause inside it is rejected."""
        cursor = connection.cursor()
        with pytest.raises(mgclient.DatabaseError):
            execute_and_fetch_all(
                cursor,
                """
                WITH virtualGraph([virtualNode(1, 'N', {})], []) AS g
                CALL { USE g CREATE (n) }
                RETURN 1;
                """,
            )

    def test_use_scope_expands_directed(self, connection):
        """A directed expand inside the scope returns the projection's edge with
        both endpoints bound to projection nodes."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 1}), virtualNode(2, 'N', {x: 2})] AS nodes,
                 [virtualEdge('R', 1, 2)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a)-[r]->(b) RETURN a.x AS ax, type(r) AS t, b.x AS bx }
            RETURN ax, t, bx;
            """,
        )
        assert results == [(1, "R", 2)]

    def test_use_scope_expands_undirected(self, connection):
        """An undirected expand traverses the edge from both ends."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 1}), virtualNode(2, 'N', {x: 2})] AS nodes,
                 [virtualEdge('R', 1, 2)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a)-[r]-(b) RETURN a.x AS ax, b.x AS bx }
            RETURN ax, bx ORDER BY ax;
            """,
        )
        assert results == [(1, 2), (2, 1)]

    def test_use_scope_expands_type_filtered(self, connection):
        """An edge-type filter selects only edges of that type."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 1}), virtualNode(2, 'N', {x: 2})] AS nodes,
                 [virtualEdge('R', 1, 2), virtualEdge('S', 1, 2)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a)-[r:R]->(b) RETURN type(r) AS t }
            RETURN t;
            """,
        )
        assert results == [("R",)]

    def test_use_scope_expands_self_loop(self, connection):
        """A self-loop is reachable expanding either direction; both endpoints are
        the same projection node."""
        cursor = connection.cursor()
        out_rows = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 7})] AS nodes, [virtualEdge('R', 1, 1)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a)-[r]->(b) RETURN a.x AS ax, b.x AS bx }
            RETURN ax, bx;
            """,
        )
        assert out_rows == [(7, 7)]

        in_rows = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 7})] AS nodes, [virtualEdge('R', 1, 1)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a)<-[r]-(b) RETURN a.x AS ax, b.x AS bx }
            RETURN ax, bx;
            """,
        )
        assert in_rows == [(7, 7)]

    def test_use_scope_label_filter(self, connection):
        """A label-filtered MATCH returns only the projection nodes with that
        label, via a full scan plus filter."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'A', {x: 1}), virtualNode(2, 'B', {x: 2}), virtualNode(3, 'A', {x: 3})] AS nodes,
                 [] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (n:A) RETURN n.x AS x }
            RETURN x ORDER BY x;
            """,
        )
        assert results == [(1,), (3,)]

    def test_use_scope_label_filter_ignores_real_index(self, connection):
        """A label scan over a projection stays a full scan even when the real
        graph has an index on that label: the projection is read, not the real
        graph."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "CREATE INDEX ON :A")
        execute_and_fetch_all(cursor, "CREATE (:A {x: 100})")
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'A', {x: 1}), virtualNode(2, 'B', {x: 2})] AS nodes, [] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (n:A) RETURN n.x AS x }
            RETURN x ORDER BY x;
            """,
        )
        assert results == [(1,)]

    def test_use_scope_property_predicate(self, connection):
        """A WHERE property predicate filters projection nodes by scan."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 10}), virtualNode(2, 'N', {x: 20}), virtualNode(3, 'N', {x: 30})] AS nodes,
                 [] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (n) WHERE n.x > 15 RETURN n.x AS x }
            RETURN x ORDER BY x;
            """,
        )
        assert results == [(20,), (30,)]

    def test_use_scope_variable_length(self, connection):
        """A depth-first variable-length pattern inside the scope walks the projection's
        edge index: *1..2 from node 1 along the R chain reaches nodes 2 and 3."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 1}), virtualNode(2, 'N', {x: 2}), virtualNode(3, 'N', {x: 3})] AS nodes,
                 [virtualEdge('R', 1, 2), virtualEdge('R', 2, 3)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a {x: 1})-[r:R*1..2]->(b) RETURN b.x AS bx }
            RETURN bx ORDER BY bx;
            """,
        )
        assert results == [(2,), (3,)]

    def test_use_scope_variable_length_bounded(self, connection):
        """The upper bound is honoured: *1..1 stops after a single hop."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 1}), virtualNode(2, 'N', {x: 2}), virtualNode(3, 'N', {x: 3})] AS nodes,
                 [virtualEdge('R', 1, 2), virtualEdge('R', 2, 3)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a {x: 1})-[r:R*1..1]->(b) RETURN b.x AS bx }
            RETURN bx ORDER BY bx;
            """,
        )
        assert results == [(2,)]

    def test_use_scope_breadth_first(self, connection):
        """The natural (a)-[*BFS]->(b) form is a single-source BFS over the projection,
        reaching each node at its shortest depth: node 2 at depth 1, node 3 at depth 2."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 1}), virtualNode(2, 'N', {x: 2}), virtualNode(3, 'N', {x: 3})] AS nodes,
                 [virtualEdge('R', 1, 2), virtualEdge('R', 2, 3)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a {x: 1})-[e *BFS 1..10]->(b) RETURN b.x AS bx, size(e) AS len }
            RETURN bx, len ORDER BY bx;
            """,
        )
        assert results == [(2, 1), (3, 2)]

    def test_use_scope_shortest_path_between_bound_nodes(self, connection):
        """Both endpoints pre-bound (WITH barrier) makes the *BFS an s-t shortest path,
        which walks the projection bidirectionally and reconstructs the path."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 1}), virtualNode(2, 'N', {x: 2}), virtualNode(3, 'N', {x: 3})] AS nodes,
                 [virtualEdge('R', 1, 2), virtualEdge('R', 2, 3)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a {x: 1}), (b {x: 3}) WITH a, b MATCH (a)-[e *BFS]->(b) RETURN size(e) AS len }
            RETURN len;
            """,
        )
        assert results == [(2,)]

    def test_use_scope_weighted_shortest_path(self, connection):
        """Weighted shortest path over the projection: the weight lambda reads a virtual
        node property. Node 4 is reached by the cheaper 1->2->4 (weight 2), not 1->3->4
        (weight 101); node 3 sits at weight 100."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 0}), virtualNode(2, 'N', {x: 1}), virtualNode(3, 'N', {x: 100}),
                  virtualNode(4, 'N', {x: 1})] AS nodes,
                 [virtualEdge('R', 1, 2), virtualEdge('R', 2, 4), virtualEdge('R', 1, 3), virtualEdge('R', 3, 4)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a {x: 0})-[e *wShortest 10 (edge, n | n.x) w]->(b) RETURN w AS weight, size(e) AS len }
            RETURN weight, len ORDER BY weight;
            """,
        )
        assert results == [(1, 1), (2, 2), (100, 1)]

    def test_use_scope_all_shortest_paths(self, connection):
        """All-shortest paths enumerates every equal-cost path: in the unit-weight diamond,
        node 4 is reached by two shortest paths (via 2 and via 3), so it appears twice."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 1}), virtualNode(2, 'N', {x: 2}), virtualNode(3, 'N', {x: 3}),
                  virtualNode(4, 'N', {x: 4})] AS nodes,
                 [virtualEdge('R', 1, 2), virtualEdge('R', 1, 3), virtualEdge('R', 2, 4), virtualEdge('R', 3, 4)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a {x: 1})-[e *allShortest 10 (edge, n | 1) w]->(b) RETURN b.x AS bx, size(e) AS len }
            RETURN bx, len ORDER BY bx, len;
            """,
        )
        assert results == [(2, 1), (3, 1), (4, 2), (4, 2)]

    def test_use_scope_k_shortest_paths(self, connection):
        """K-shortest paths enumerates the loopless paths between two bound nodes: the
        diamond's two length-2 paths 1->2->4 and 1->3->4 are both returned."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 1}), virtualNode(2, 'N', {x: 2}), virtualNode(3, 'N', {x: 3}),
                  virtualNode(4, 'N', {x: 4})] AS nodes,
                 [virtualEdge('R', 1, 2), virtualEdge('R', 2, 4), virtualEdge('R', 1, 3), virtualEdge('R', 3, 4)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a {x: 1}), (b {x: 4}) WITH a, b MATCH (a)-[r *kshortest..10]->(b) RETURN size(r) AS len }
            RETURN len ORDER BY len;
            """,
        )
        assert results == [(2,), (2,)]


class TestUseScopeOverDerive:
    """A USE scope over a derive() overlay projection: reads inside the scope go
    through to the origin per the per-property binding."""

    def _make_path(self, cursor):
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1, v: 7, secret: 42})-[:R]->(:N {id: 2, v: 9, secret: 43});")

    def test_use_scope_over_derive_reads_through(self, connection):
        """A MATCH inside the scope returns the origin's property values, and a
        predicate over a read-through value filters correctly."""
        cursor = connection.cursor()
        self._make_path(cursor)
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E'}) AS g
            CALL { USE g MATCH (n) WHERE n.v > 8 RETURN n.id AS id, n.v AS v }
            RETURN id, v ORDER BY id;
            """,
        )
        assert results == [(2, 9)]

    def test_use_scope_over_derive_overlay_shadows(self, connection):
        """An overlay-bound key shadows the origin value inside the scope."""
        cursor = connection.cursor()
        self._make_path(cursor)
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E', propertyPolicy: {v: 'overlay'}, sourceNodeProperties: {v: 100}}) AS g
            CALL { USE g MATCH (n) WHERE n.id = 1 RETURN n.v AS v }
            RETURN v;
            """,
        )
        assert results == [(100,)]

    def test_use_scope_over_derive_hidden_invisible(self, connection):
        """A hidden key is invisible to reads and to predicates inside the scope:
        the read yields null and `WHERE n.secret IS NULL` matches every node."""
        cursor = connection.cursor()
        self._make_path(cursor)
        read = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E', propertyPolicy: {secret: 'hidden'}}) AS g
            CALL { USE g MATCH (n) WHERE n.id = 1 RETURN n.secret AS s }
            RETURN s;
            """,
        )
        assert read == [(None,)]

        pred = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E', propertyPolicy: {secret: 'hidden'}}) AS g
            CALL { USE g MATCH (n) WHERE n.secret IS NULL RETURN n.id AS id }
            RETURN id ORDER BY id;
            """,
        )
        assert pred == [(1,), (2,)]

    def test_use_scope_over_derive_expands_with_read_through_endpoints(self, connection):
        """Expanding the derived edge inside the scope binds endpoints that read
        through to their origins."""
        cursor = connection.cursor()
        self._make_path(cursor)
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E'}) AS g
            CALL { USE g MATCH (a)-[r]->(b) RETURN a.id AS aid, type(r) AS t, b.id AS bid }
            RETURN aid, t, bid;
            """,
        )
        assert results == [(1, "E", 2)]

    def test_use_scope_over_derive_degree_counts_projection_edges(self, connection):
        """degree/outDegree/inDegree over a projection node count the projection's
        edges, which differ from the node's real-graph degree."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        # a has real out-degree 2.
        execute_and_fetch_all(cursor, "CREATE (a:N {id: 1})-[:R]->(:N {id: 2}), (a)-[:R]->(:N {id: 3});")
        # The projection derived from the single path a->b has one edge.
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E'}) AS g
            CALL { USE g MATCH (n) WHERE n.id = 1 RETURN degree(n) AS d, outDegree(n) AS od, inDegree(n) AS ind }
            RETURN d, od, ind;
            """,
        )
        assert results == [(1, 1, 0)]
        real = execute_and_fetch_all(cursor, "MATCH (a:N {id: 1}) RETURN outDegree(a) AS od;")
        assert real == [(2,)]

    def test_use_scope_reads_virtual_edge_property(self, connection):
        """A projected edge reads a property through the same call site as a real
        edge, by name lookup and by subscript."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:N {id: 1})-[:R]->(:N {id: 2});")
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:N {id: 1})-[:R]->(:N {id: 2})
            WITH derive(p, {virtualEdgeType: 'E', relationshipProperties: {weight: 99}}) AS g
            CALL { USE g MATCH ()-[r]->() RETURN r.weight AS by_name, r['weight'] AS by_subscript }
            RETURN by_name, by_subscript;
            """,
        )
        assert results == [(99, 99)]


class TestUseScopeOverSubgraph:
    """A USE scope over a project() subgraph: scans its real member nodes and
    keeps expansion within membership, through the same seam as projections."""

    def test_use_scope_over_subgraph_scans_members(self, connection):
        """A MATCH inside the scope returns only the subgraph's member nodes, not
        other real-graph nodes."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:A {name: 'a'})-[:R]->(:B {name: 'b'}), (:C {name: 'c'});")
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:A)-[:R]->(:B)
            WITH project(p) AS sg
            CALL { USE sg MATCH (n) RETURN n.name AS name }
            RETURN name ORDER BY name;
            """,
        )
        assert results == [("a",), ("b",)]

    def test_use_scope_over_subgraph_expansion_respects_membership(self, connection):
        """Expanding inside the scope stays within the subgraph: a member node's
        edge to a non-member is not traversed."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(
            cursor,
            "CREATE (a:A {name: 'a'})-[:R]->(b:B {name: 'b'}), (b)-[:R2]->(:C {name: 'c'});",
        )
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:A)-[:R]->(:B)
            WITH project(p) AS sg
            CALL { USE sg MATCH (x)-[r]->(y) RETURN x.name AS xn, y.name AS yn }
            RETURN xn, yn ORDER BY xn;
            """,
        )
        assert results == [("a", "b")]

    def test_use_scope_over_subgraph_degree_counts_member_edges(self, connection):
        """degree over a subgraph member counts only member edges, differing from
        the node's real-graph degree."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        # b has real degree 2: an in-edge from a and an out-edge to c.
        execute_and_fetch_all(cursor, "CREATE (a:A)-[:R]->(b:B {id: 1}), (b)-[:R2]->(:C);")
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:A)-[:R]->(:B)
            WITH project(p) AS sg
            CALL { USE sg MATCH (n:B) RETURN degree(n) AS d, inDegree(n) AS ind, outDegree(n) AS od }
            RETURN d, ind, od;
            """,
        )
        assert results == [(1, 1, 0)]
        real = execute_and_fetch_all(cursor, "MATCH (b:B {id: 1}) RETURN degree(b) AS d;")
        assert real == [(2,)]

    def test_use_scope_over_subgraph_variable_length_respects_membership(self, connection):
        """Variable-length expansion stays within membership too, exactly as single-hop: a path
        cannot leak out of the subgraph via a non-member edge (issue 49). The subgraph holds a->b;
        the real graph also has b->c (same type), reachable only via a non-member edge."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(
            cursor,
            "CREATE (a:A {name: 'a'})-[:R]->(b:B {name: 'b'}), (b)-[:R]->(:C {name: 'c'});",
        )
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:A)-[:R]->(:B)
            WITH project(p) AS sg
            CALL { USE sg MATCH (x:A)-[:R*1..3]->(y) RETURN y.name AS yn }
            RETURN yn ORDER BY yn;
            """,
        )
        assert results == [("b",)]

    def test_use_scope_over_subgraph_bfs_respects_membership(self, connection):
        """The shortest-path family (here BFS) respects subgraph membership as well: the same
        member-filtered traversal applies to every arm, not just plain variable-length (issue 49)."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(
            cursor,
            "CREATE (a:A {name: 'a'})-[:R]->(b:B {name: 'b'}), (b)-[:R]->(:C {name: 'c'});",
        )
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:A)-[:R]->(:B)
            WITH project(p) AS sg
            CALL { USE sg MATCH (x:A)-[*BFS]->(y) RETURN y.name AS yn }
            RETURN yn ORDER BY yn;
            """,
        )
        assert results == [("b",)]


class TestUseScopeProceduresHonorAmbientView:
    """Inside a `CALL { USE g ... }` scope, an unmodified read procedure that
    reads `ctx.graph` operates on the bound view, not the real graph. The
    procedures here take no graph argument, so the only graph they can see is
    the ambient one routed in by the USE scope."""

    def test_ambient_virtualgraph_view_iterates_projection_vertices(self, connection):
        """An ambient virtualGraph projection: an unmodified vertex-iterating
        procedure yields the projection's nodes."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 10}), virtualNode(2, 'N', {x: 20})] AS nodes, [] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g CALL read.subgraph_get_vertices() YIELD node RETURN node.x AS x }
            RETURN x ORDER BY x;
            """,
        )
        assert results == [(10,), (20,)]

    def test_ambient_virtualgraph_view_expands_projection_edges(self, connection):
        """An ambient virtualGraph projection: an unmodified edge-expanding
        procedure traverses the projection's edges."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH [virtualNode(1, 'N', {x: 1}), virtualNode(2, 'N', {x: 2})] AS nodes,
                 [virtualEdge('R', 1, 2)] AS edges
            WITH virtualGraph(nodes, edges) AS g
            CALL { USE g MATCH (a {x: 1}) CALL read.subgraph_get_out_edges(a) YIELD edge RETURN type(edge) AS t }
            RETURN t;
            """,
        )
        assert results == [("R",)]

    def test_ambient_subgraph_view_iterates_member_vertices(self, connection):
        """An ambient project() subgraph: an unmodified vertex-iterating procedure
        yields only the subgraph's member nodes."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:A {name: 'a'})-[:R]->(:B {name: 'b'}), (:C {name: 'c'});")
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:A)-[:R]->(:B)
            WITH project(p) AS sg
            CALL { USE sg CALL read.subgraph_get_vertices() YIELD node RETURN node.name AS name }
            RETURN name ORDER BY name;
            """,
        )
        assert results == [("a",), ("b",)]

    def test_ambient_subgraph_view_expands_member_edges(self, connection):
        """An ambient project() subgraph: an unmodified edge-expanding procedure
        traverses a member node's edges through the bound subgraph."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:A {name: 'a'})-[:R]->(:B {name: 'b'}), (:C {name: 'c'});")
        results = execute_and_fetch_all(
            cursor,
            """
            MATCH p=(:A)-[:R]->(:B)
            WITH project(p) AS sg
            CALL { USE sg MATCH (x:A) CALL read.subgraph_get_out_edges(x) YIELD edge RETURN type(edge) AS t }
            RETURN t;
            """,
        )
        assert results == [("R",)]

    def test_procedure_outside_use_scope_sees_real_graph(self, connection):
        """With no USE scope, the same procedure reads the real graph."""
        cursor = connection.cursor()
        execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")
        execute_and_fetch_all(cursor, "CREATE (:Real {name: 'r1'}), (:Real {name: 'r2'});")
        results = execute_and_fetch_all(
            cursor,
            "CALL read.subgraph_get_vertices() YIELD node RETURN node.name AS name ORDER BY name;",
        )
        assert results == [("r1",), ("r2",)]

    def test_explicit_graph_argument_wins_over_ambient_view(self, connection):
        """An explicit graph argument takes precedence over the ambient view:
        `USE h { CALL proc(g) }` runs the procedure over g, not h."""
        cursor = connection.cursor()
        results = execute_and_fetch_all(
            cursor,
            """
            WITH virtualGraph([virtualNode(1, 'N', {x: 10})], []) AS g,
                 virtualGraph([virtualNode(2, 'N', {x: 20})], []) AS h
            CALL (g) { USE h CALL read.subgraph_get_vertices(g) YIELD node RETURN node.x AS x }
            RETURN x;
            """,
        )
        assert results == [(10,)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
