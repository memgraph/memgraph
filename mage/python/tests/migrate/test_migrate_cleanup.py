"""Integration-style tests: after a fetch exception, the same query can be
re-initialised. Before the fix a RuntimeError("already running") would block
any retry until the module was reloaded."""

from unittest.mock import MagicMock

import pytest


class _FetchBoom(Exception):
    pass


def _make_sql_cursor_that_explodes_on_fetch():
    """A cursor whose description works (used during init) but whose
    fetchmany raises on the first call (simulates conversion errors, timeouts,
    network blips during streaming)."""
    cursor = MagicMock()
    cursor.description = [("col_a",), ("col_b",)]
    cursor.fetchmany.side_effect = _FetchBoom("simulated mid-stream failure")
    return cursor


def _make_sql_connection(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_mysql_eviction_on_fetch_exception(migrate_module, monkeypatch):
    cursor = _make_sql_cursor_that_explodes_on_fetch()
    conn = _make_sql_connection(cursor)
    monkeypatch.setattr(migrate_module.mysql_connector, "connect", lambda **cfg: conn)

    args = ("SELECT 1", {"host": "h"}, "", None)

    migrate_module.init_migrate_mysql(*args)
    with pytest.raises(_FetchBoom):
        migrate_module.mysql(*args)

    # Entry was evicted — a re-init with identical args must succeed.
    migrate_module.init_migrate_mysql(*args)

    # And the teardown path actually closed the connection.
    assert conn.close.called


def test_postgresql_eviction_on_fetch_exception(migrate_module, monkeypatch):
    cursor = MagicMock()
    # psycopg2 uses .name on description columns
    col_a = MagicMock()
    col_a.name = "col_a"  # noqa: E702
    col_b = MagicMock()
    col_b.name = "col_b"  # noqa: E702
    cursor.description = [col_a, col_b]
    cursor.fetchmany.side_effect = _FetchBoom("boom")
    conn = _make_sql_connection(cursor)
    monkeypatch.setattr(migrate_module.psycopg2, "connect", lambda **cfg: conn)

    args = ("SELECT 1", {"host": "h"}, "", None)

    migrate_module.init_migrate_postgresql(*args)
    with pytest.raises(_FetchBoom):
        migrate_module.postgresql(*args)

    migrate_module.init_migrate_postgresql(*args)
    assert conn.close.called


def test_sql_server_eviction_on_fetch_exception(migrate_module, monkeypatch):
    cursor = _make_sql_cursor_that_explodes_on_fetch()
    conn = _make_sql_connection(cursor)
    monkeypatch.setattr(migrate_module.pyodbc, "connect", lambda **cfg: conn)

    args = ("SELECT 1", {"driver": "x"}, "", None)

    migrate_module.init_migrate_sql_server(*args)
    with pytest.raises(_FetchBoom):
        migrate_module.sql_server(*args)

    migrate_module.init_migrate_sql_server(*args)
    assert conn.close.called


def test_oracle_eviction_on_fetch_exception(migrate_module, monkeypatch):
    cursor = _make_sql_cursor_that_explodes_on_fetch()
    conn = _make_sql_connection(cursor)
    monkeypatch.setattr(migrate_module.oracledb, "connect", lambda **cfg: conn)

    args = ("SELECT 1", {"user": "u"}, "", None)

    migrate_module.init_migrate_oracle_db(*args)
    with pytest.raises(_FetchBoom):
        migrate_module.oracle_db(*args)

    migrate_module.init_migrate_oracle_db(*args)
    assert conn.close.called


def test_duckdb_eviction_on_fetch_exception(migrate_module, monkeypatch):
    cursor = _make_sql_cursor_that_explodes_on_fetch()
    conn = MagicMock()
    conn.cursor.return_value = cursor
    monkeypatch.setattr(migrate_module.duckDB, "connect", lambda: conn)

    query = "SELECT 1"
    migrate_module.init_migrate_duckdb(query)
    with pytest.raises(_FetchBoom):
        migrate_module.duckdb(query)

    migrate_module.init_migrate_duckdb(query)
    assert conn.close.called


def test_servicenow_eviction_on_fetch_exception(migrate_module, monkeypatch):
    # Make init succeed, then force an error from the iterator during fetch.
    response = MagicMock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"result": [{"a": 1}, {"a": 2}]}
    monkeypatch.setattr(
        migrate_module.requests,
        "get",
        lambda *a, **kw: response,
    )

    args = ("http://example/api", {"username": "u", "password": "p"}, "", None)
    migrate_module.init_migrate_servicenow(*args)

    # Replace the cursor inside the state with our exploding iterator.
    query_hash = migrate_module._get_query_hash("http://example/api", {"username": "u", "password": "p"}, None)
    migrate_module.servicenow_state._entries[query_hash][migrate_module.Constants.CURSOR] = _BoomIter()

    with pytest.raises(_FetchBoom):
        migrate_module.servicenow(*args)

    # Re-init must succeed.
    migrate_module.init_migrate_servicenow(*args)


class _BoomIter:
    def __iter__(self):
        return self

    def __next__(self):
        raise _FetchBoom("boom")


def test_s3_eviction_on_fetch_exception(migrate_module, monkeypatch):
    import io

    csv_bytes = b"col_a,col_b\n1,2\n3,4\n"
    response = {"Body": io.BytesIO(csv_bytes)}
    s3_client = MagicMock()
    s3_client.get_object.return_value = response
    monkeypatch.setattr(migrate_module.boto3, "client", lambda *a, **kw: s3_client)

    file_path = "s3://bucket/data.csv"
    cfg = {"region_name": "us-east-1"}
    migrate_module.init_migrate_s3(file_path, cfg, "")

    # Replace the underlying CSV iterator with one that raises during fetch.
    query_hash = migrate_module._get_query_hash(file_path, cfg)
    migrate_module.s3_state._entries[query_hash][migrate_module.Constants.CURSOR] = _BoomIter()

    with pytest.raises(_FetchBoom):
        migrate_module.s3(file_path, cfg, "")

    # Re-init must succeed (was previously permanently blocked).
    # Fresh BytesIO since the previous one was consumed.
    response["Body"] = io.BytesIO(csv_bytes)
    migrate_module.init_migrate_s3(file_path, cfg, "")


def test_neo4j_eviction_on_fetch_exception(migrate_module, monkeypatch):
    session = MagicMock()
    session.run.return_value = _BoomIter()
    driver = MagicMock()
    driver.session.return_value = session
    monkeypatch.setattr(migrate_module.GraphDatabase, "driver", lambda *a, **kw: driver)

    args = ("MATCH (n) RETURN n", {"host": "h"}, "", None)
    migrate_module.init_migrate_neo4j(*args)
    with pytest.raises(_FetchBoom):
        migrate_module.neo4j(*args)

    # Re-init succeeds and exception path closed session + driver.
    session.run.return_value = _BoomIter()
    migrate_module.init_migrate_neo4j(*args)
    assert session.close.called
    assert driver.close.called


def test_arrow_flight_eviction_on_fetch_exception(migrate_module, monkeypatch):
    client = MagicMock()
    client.get_flight_info.return_value = MagicMock(endpoints=[])
    monkeypatch.setattr(migrate_module.flight, "connect", lambda *a, **kw: client)
    # FlightCallOptions / FlightDescriptor are already stubbed in conftest.

    args = ("SELECT 1", {"host": "h", "port": 1234}, "")
    migrate_module.init_migrate_arrow_flight(*args)

    # Swap cursor with an exploding iterator.
    query_hash = migrate_module._get_query_hash(args[0], args[1])
    migrate_module.arrow_flight_state._entries[query_hash][migrate_module.Constants.CURSOR] = _BoomIter()

    with pytest.raises(_FetchBoom):
        migrate_module.arrow_flight(*args)

    migrate_module.init_migrate_arrow_flight(*args)
    assert client.close.called


def test_memgraph_eviction_on_fetch_exception(migrate_module, monkeypatch):
    mg = MagicMock()
    mg.execute_and_fetch.return_value = _BoomIter()
    monkeypatch.setattr(migrate_module, "Memgraph", lambda **cfg: mg)

    args = ("MATCH (n) RETURN n", {"host": "h"}, "", None)
    migrate_module.init_migrate_memgraph(*args)
    with pytest.raises(_FetchBoom):
        migrate_module.memgraph(*args)

    migrate_module.init_migrate_memgraph(*args)
    assert mg.close.called
