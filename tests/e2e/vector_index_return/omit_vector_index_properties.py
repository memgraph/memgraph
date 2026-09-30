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

SETTING = "storage.omit_vector_index_properties_on_return"


def _connect():
    conn = mgclient.connect(host="localhost", port=7687)
    conn.autocommit = True
    return conn


def _run(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    try:
        return cur.fetchall()
    except mgclient.DatabaseError:
        return []


def _set(conn, value):
    _run(conn, f'SET DATABASE SETTING "{SETTING}" TO "{value}"')


def _node(conn):
    return _run(conn, "MATCH (n:Doc) RETURN n")[0][0]


def _scalar(conn, query):
    return _run(conn, query)[0][0]


def _dump(conn):
    return "\n".join(str(row[0]) for row in _run(conn, "DUMP DATABASE"))


def test_omit_vector_index_properties_on_return():
    conn = _connect()
    _set(conn, "false")
    _run(conn, "MATCH (n) DETACH DELETE n")
    _run(conn, 'CREATE VECTOR INDEX doc_emb ON :Doc(embedding) WITH CONFIG {"dimension": 3, "capacity": 100}')
    _run(conn, "CREATE (:Doc {title: 'a', embedding: [1.0, 2.0, 3.0]})")

    # Default (flag off): a whole-object return carries the embedding.
    node = _node(conn)
    assert "embedding" in node.properties
    assert node.properties["title"] == "a"
    dump_off = _dump(conn)

    _set(conn, "true")

    # Flag on: the embedding is omitted from the whole-object return, other properties stay.
    node = _node(conn)
    assert "embedding" not in node.properties
    assert node.properties["title"] == "a"

    # Explicit access is the escape hatch and still returns the full vector.
    assert _scalar(conn, "MATCH (n:Doc) RETURN n.embedding AS e") == [1.0, 2.0, 3.0]
    assert _scalar(conn, "MATCH (n:Doc) RETURN properties(n) AS p")["embedding"] == [1.0, 2.0, 3.0]

    # The flag is a client-return concern only: DUMP DATABASE is byte-for-byte unaffected.
    assert _dump(conn) == dump_off

    # Flipping it back restores the full object at runtime, no restart.
    _set(conn, "false")
    assert "embedding" in _node(conn).properties


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
