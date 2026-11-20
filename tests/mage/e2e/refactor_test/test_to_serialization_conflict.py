import mgclient
import pytest

HOST = "127.0.0.1"
PORT = 7687


def _connect(autocommit: bool) -> mgclient.Connection:
    conn = mgclient.connect(host=HOST, port=PORT)
    conn.autocommit = autocommit
    return conn


def test_to_fails_and_leaves_graph_unchanged_on_serialization_conflict():
    conn = _connect(True)
    other = _connect(False)
    try:
        cursor = conn.cursor()
        cursor.execute("MATCH (n) DETACH DELETE n")
        cursor.execute("CREATE (:Node {id: 0})-[:R {w: 1}]->(:Node {id: 1}), (:Node {id: 2})")

        other_cursor = other.cursor()
        other_cursor.execute("MATCH ()-[r:R]->() SET r.w = 2")
        other_cursor.fetchall()

        with pytest.raises(mgclient.DatabaseError, match="(?i)serialization"):
            cursor.execute("MATCH ()-[r:R]->(), (c:Node {id: 2}) CALL refactor.to(r, c) YIELD input RETURN input")
            cursor.fetchall()

        other.commit()

        cursor.execute("MATCH (a)-[r]->(b) RETURN a.id, b.id, r.w")
        assert cursor.fetchall() == [(0, 1, 2)]
    finally:
        other.close()
        conn.cursor().execute("MATCH (n) DETACH DELETE n")
        conn.close()
