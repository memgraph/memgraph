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

VECTOR = "CREATE VECTOR INDEX vi ON :L(emb) WITH CONFIG {'dimension': 2, 'capacity': 10}"
VECTOR_EDGE = "CREATE VECTOR EDGE INDEX ve ON :T(e) WITH CONFIG {'dimension': 2, 'capacity': 10}"
VECTOR_Z = "CREATE VECTOR INDEX vz ON :L(z) WITH CONFIG {'dimension': 2, 'capacity': 10}"
VECTOR_2 = "CREATE VECTOR INDEX v2 ON :L(emb) WITH CONFIG {'dimension': 2, 'capacity': 10}"
VECTOR_EDGE_2 = "CREATE VECTOR EDGE INDEX ve2 ON :T(e) WITH CONFIG {'dimension': 2, 'capacity': 10}"

VERTEX_ORDINARY = [
    ("CREATE INDEX ON :L(emb)", "label+property index :L(emb)"),
    ("CREATE INDEX ON :M(x, emb)", "label+property index :M(x, emb)"),
    ("CREATE INDEX ON :M(emb, x)", "label+property index :M(emb, x)"),
    ('CREATE INDEX ON :L(emb) WITH CONFIG {"order": "DESC"}', "label+property index :L(emb)"),
    ("CREATE GLOBAL INDEX ON :(emb)", "global vertex property index :(emb)"),
    ("CREATE CONSTRAINT ON (n:K) ASSERT n.emb IS UNIQUE", "unique constraint :K(emb)"),
]
EDGE_ORDINARY = [
    ("CREATE EDGE INDEX ON :T(e)", "edge-type+property index :T(e)"),
    ("CREATE EDGE INDEX ON :U(e)", "edge-type+property index :U(e)"),
    ("CREATE GLOBAL EDGE INDEX ON :(e)", "global edge property index :(e)"),
]
KINDS = [
    pytest.param(VECTOR, "vi", "vector index", VECTOR_2, "v2", VERTEX_ORDINARY, id="vertex"),
    pytest.param(VECTOR_EDGE, "ve", "vector edge index", VECTOR_EDGE_2, "ve2", EDGE_ORDINARY, id="edge"),
]
EXPLANATION = "A property index or unique constraint on a vector-indexed property returns wrong results."


@pytest.fixture
def conn():
    connection = mgclient.connect(host="localhost", port=7687)
    connection.autocommit = True
    yield connection
    for query in ("DROP ALL INDEXES", "DROP ALL CONSTRAINTS", "MATCH (n) DETACH DELETE n"):
        _run(connection, query)
    connection.close()


def _run(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    return cur.fetchall()


def _refused(conn, query):
    with pytest.raises(mgclient.DatabaseError) as exc:
        _run(conn, query)
    return str(exc.value)


def _schema(conn):
    return (
        _run(conn, "SHOW INDEX INFO"),
        _run(conn, "SHOW CONSTRAINT INFO"),
        _run(conn, "SHOW VECTOR INDEX INFO"),
    )


@pytest.mark.parametrize("vector_ddl, name, kind, vector_ddl_2, name_2, ordinary", KINDS)
def test_ordinary_refused_after_vector(conn, vector_ddl, name, kind, vector_ddl_2, name_2, ordinary):
    for ddl, _ in ordinary:
        _run(conn, vector_ddl)
        before = _schema(conn)
        message = _refused(conn, ddl)
        prop = "emb" if "emb" in ddl else "e"
        assert "Cannot create" in message
        assert f"property {prop} is already indexed by {kind} {name}" in message
        assert EXPLANATION in message
        assert f"DROP VECTOR INDEX {name};" in message
        assert _schema(conn) == before
        _run(conn, f"DROP VECTOR INDEX {name}")


@pytest.mark.parametrize("vector_ddl, name, kind, vector_ddl_2, name_2, ordinary", KINDS)
def test_vector_refused_after_ordinary(conn, vector_ddl, name, kind, vector_ddl_2, name_2, ordinary):
    for ddl, other in ordinary:
        _run(conn, ddl)
        message = _refused(conn, vector_ddl_2)
        assert f"Cannot create {kind} {name_2}: property " in message
        assert f"is already covered by {other}" in message
        assert EXPLANATION in message
        assert _run(conn, "SHOW VECTOR INDEX INFO") == []
        _run(conn, "DROP ALL INDEXES")
        _run(conn, "DROP ALL CONSTRAINTS")


@pytest.mark.parametrize(
    "vector_ddl, allowed",
    [
        (VECTOR, ["CREATE INDEX ON :L", "CREATE INDEX ON :L(other)", "CREATE EDGE INDEX ON :T"]),
        (VECTOR_Z, ["CREATE EDGE INDEX ON :T(z)", "CREATE GLOBAL EDGE INDEX ON :(z)"]),
        (VECTOR_EDGE, ["CREATE INDEX ON :L(e)", "CREATE GLOBAL INDEX ON :(e)"]),
    ],
    ids=["unrelated", "vertex_vector_vs_edge_property", "edge_vector_vs_vertex_property"],
)
def test_still_allowed(conn, vector_ddl, allowed):
    _run(conn, vector_ddl)
    for ddl in allowed:
        _run(conn, ddl)


def test_drop_hint_quotes_name(conn):
    _run(conn, "CREATE VECTOR INDEX `my-vi` ON :L(emb) WITH CONFIG {'dimension': 2, 'capacity': 10}")
    message = _refused(conn, "CREATE INDEX ON :L(emb)")
    assert "DROP VECTOR INDEX `my-vi`;" in message
    _run(conn, "DROP VECTOR INDEX `my-vi`")
    _run(conn, "CREATE INDEX ON :L(emb)")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
