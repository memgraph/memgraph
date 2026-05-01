import base64
import csv
import datetime
import hashlib
import io
import json
import os
import re
from decimal import Decimal
from typing import Any, Dict, List

import boto3
import duckdb as duckDB
import mgp
import mysql.connector as mysql_connector
import oracledb
import psycopg2
import pyarrow.flight as flight
import pyodbc
from gqlalchemy import Memgraph
from neo4j import GraphDatabase
from neo4j.time import Date as Neo4jDate
from neo4j.time import DateTime as Neo4jDateTime

import requests


class Constants:
    BATCH_SIZE = 1000
    COLUMN_NAMES = "column_names"
    CONNECTION = "connection"
    CURSOR = "cursor"
    DATABASE = "database"
    DRIVER = "driver"
    HOST = "host"
    I_COLUMN_NAME = 0
    PASSWORD = "password"
    PORT = "port"
    RESULT = "result"
    SESSION = "session"
    URI_SCHEME = "uri_scheme"
    USERNAME = "username"


def _get_query_hash(query: str, config: mgp.Map, params: mgp.Nullable[mgp.Any] = None) -> str:
    """
    Create a hash from query, config, and params to use as a cache key.

    :param query: The query string (or table name, endpoint, file path, etc.)
    :param config: Configuration map
    :param params: Optional query parameters
    """
    config_dict = dict(config)
    config_str = json.dumps(config_dict, sort_keys=True, default=str)

    params_str = ""
    if params is not None:
        if isinstance(params, dict):
            params_str = json.dumps(params, sort_keys=True, default=str)
        elif isinstance(params, (list, tuple)):
            params_str = json.dumps(list(params), sort_keys=False, default=str)
        else:
            params_str = str(params)

    hash_input = f"{query}|{config_str}|{params_str}"
    return hashlib.sha256(hash_input.encode("utf-8")).hexdigest()


class _BatchedQueryState:
    """
    Lifecycle helper for batched cross-database procedures.

    State is keyed by a content hash of (query, config, params). `fetch`
    wraps the caller's fetcher in try/except so the entry is always evicted
    (and its resources closed) when the fetcher raises, preventing the
    "already running" error from permanently blocking a hash after a
    transient fetch failure.

    Concurrent invocations with the same hash still raise "already running" —
    fixing that requires exposing a per-invocation ID from the mgp binding
    and is tracked separately.
    """

    _ALREADY_RUNNING_MSG = (
        "Migrate module with these parameters is already running. "
        "Please wait for it to finish before starting a new one."
    )

    def __init__(self, backend_name, closer):
        self._name = backend_name
        self._closer = closer
        self._entries = {}

    def start(self, query_hash, entry):
        if query_hash in self._entries:
            raise RuntimeError(self._ALREADY_RUNNING_MSG)
        self._entries[query_hash] = entry

    def get(self, query_hash):
        return self._entries[query_hash]

    def fetch(self, query_hash, fetcher):
        entry = self._entries[query_hash]
        try:
            result = fetcher(entry)
        except BaseException:
            self._evict(query_hash)
            raise
        if not result:
            self._evict(query_hash)
        return result

    def _evict(self, query_hash):
        entry = self._entries.pop(query_hash, None)
        if entry is None:
            return
        try:
            self._closer(entry)
        except Exception:
            pass


def _close_dbapi_entry(entry, commit=True):
    """Close a DB-API-style entry: cursor then (optionally commit and) connection.
    Errors during close/commit are swallowed; the helper is best-effort."""
    cursor = entry.get(Constants.CURSOR)
    if cursor is not None:
        try:
            cursor.close()
        except Exception:
            pass
    conn = entry.get(Constants.CONNECTION)
    if conn is None:
        return
    if commit:
        try:
            conn.commit()
        except Exception:
            pass
    try:
        conn.close()
    except Exception:
        pass


# MYSQL


mysql_state = _BatchedQueryState("mysql", _close_dbapi_entry)


def init_migrate_mysql(
    table_or_sql: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
):
    if params:
        _check_params_type(params)
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    if _query_is_table(table_or_sql):
        table_or_sql = f"SELECT * FROM {table_or_sql};"

    query_hash = _get_query_hash(table_or_sql, config, params)

    connection = mysql_connector.connect(**config)
    cursor = connection.cursor()
    cursor.execute(table_or_sql, params=params)

    mysql_state.start(
        query_hash,
        {
            Constants.CONNECTION: connection,
            Constants.CURSOR: cursor,
            Constants.COLUMN_NAMES: [column[Constants.I_COLUMN_NAME] for column in cursor.description],
        },
    )


def mysql(
    table_or_sql: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
) -> mgp.Record(row=mgp.Map):
    """
    With migrate.mysql you can access MySQL and execute queries.
    The result table is converted into a stream, and returned rows can be
    used to create graph structures. Config must be at least empty map.
    If config_path is passed, every key,value pair from JSON file will
    overwrite any values in config file.

    :param table_or_sql: Table name or an SQL query
    :param config: Connection configuration parameters
                   (as in mysql.connector.connect)
    :param config_path: Path to the JSON file containing configuration
                        parameters (as in mysql.connector.connect)
    :param params: Optionally, queries may be parameterized. In that case,
                   `params` provides parameter values
    :return: The result table as a stream of rows
    """
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    if _query_is_table(table_or_sql):
        table_or_sql = f"SELECT * FROM {table_or_sql};"

    query_hash = _get_query_hash(table_or_sql, config, params)

    def _fetch(entry):
        rows = entry[Constants.CURSOR].fetchmany(Constants.BATCH_SIZE)
        column_names = entry[Constants.COLUMN_NAMES]
        return [mgp.Record(row=_name_row_cells_mysql(row, column_names)) for row in rows]

    return mysql_state.fetch(query_hash, _fetch)


def cleanup_migrate_mysql():
    pass


mgp.add_batch_read_proc(mysql, init_migrate_mysql, cleanup_migrate_mysql)

# SQL SERVER


sql_server_state = _BatchedQueryState("sql_server", _close_dbapi_entry)


def init_migrate_sql_server(
    table_or_sql: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
):
    if params:
        _check_params_type(params, (list, tuple))
    else:
        params = []

    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    if _query_is_table(table_or_sql):
        table_or_sql = f"SELECT * FROM {table_or_sql};"

    query_hash = _get_query_hash(table_or_sql, config, params)

    connection = pyodbc.connect(**config)
    cursor = connection.cursor()
    cursor.execute(table_or_sql, *params)

    sql_server_state.start(
        query_hash,
        {
            Constants.CONNECTION: connection,
            Constants.CURSOR: cursor,
            Constants.COLUMN_NAMES: [column[Constants.I_COLUMN_NAME] for column in cursor.description],
        },
    )


def sql_server(
    table_or_sql: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
) -> mgp.Record(row=mgp.Map):
    """
    With migrate.sql_server you can access SQL Server and execute queries.
    The result table is converted into a stream, and returned rows can be
    used to create graph structures. Config must be at least empty map.
    If config_path is passed, every key,value pair from JSON file will
    overwrite any values in config file.

    :param table_or_sql: Table name or an SQL query
    :param config: Connection configuration parameters (as in pyodbc.connect)
    :param config_path: Path to the JSON file containing configuration
                        parameters (as in pyodbc.connect)
    :param params: Optionally, queries may be parameterized. In that case,
                   `params` provides parameter values
    :return: The result table as a stream of rows
    """
    if not params:
        params = []

    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    if _query_is_table(table_or_sql):
        table_or_sql = f"SELECT * FROM {table_or_sql};"

    query_hash = _get_query_hash(table_or_sql, config, params)

    def _fetch(entry):
        rows = entry[Constants.CURSOR].fetchmany(Constants.BATCH_SIZE)
        column_names = entry[Constants.COLUMN_NAMES]
        return [mgp.Record(row=_name_row_cells(row, column_names)) for row in rows]

    return sql_server_state.fetch(query_hash, _fetch)


def cleanup_migrate_sql_server():
    pass


mgp.add_batch_read_proc(sql_server, init_migrate_sql_server, cleanup_migrate_sql_server)

# Oracle DB


oracle_db_state = _BatchedQueryState("oracle_db", _close_dbapi_entry)


def init_migrate_oracle_db(
    table_or_sql: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
):
    if params:
        _check_params_type(params)

    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    if _query_is_table(table_or_sql):
        table_or_sql = f"SELECT * FROM {table_or_sql}"

    if not config:
        config = {}

    # To prevent query execution from hanging
    config["disable_oob"] = True

    query_hash = _get_query_hash(table_or_sql, config, params)

    connection = oracledb.connect(**config)
    cursor = connection.cursor()

    if not params:
        cursor.execute(table_or_sql)
    elif isinstance(params, (list, tuple)):
        cursor.execute(table_or_sql, params)
    else:
        cursor.execute(table_or_sql, **params)

    oracle_db_state.start(
        query_hash,
        {
            Constants.CONNECTION: connection,
            Constants.CURSOR: cursor,
            Constants.COLUMN_NAMES: [column[Constants.I_COLUMN_NAME] for column in cursor.description],
        },
    )


def oracle_db(
    table_or_sql: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
) -> mgp.Record(row=mgp.Map):
    """
    With migrate.oracle_db you can access Oracle DB and execute queries.
    The result table is converted into a stream, and returned rows can be
    used to create graph structures. Config must be at least empty map.
    If config_path is passed, every key,value pair from JSON file will
    overwrite any values in config file.

    :param table_or_sql: Table name or an SQL query
    :param config: Connection configuration parameters (as in oracledb.connect)
    :param config_path: Path to the JSON file containing configuration
                        parameters (as in oracledb.connect)
    :param params: Optionally, queries may be parameterized. In that case,
                   `params` provides parameter values
    :return: The result table as a stream of rows
    """
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    if _query_is_table(table_or_sql):
        table_or_sql = f"SELECT * FROM {table_or_sql}"

    if not config:
        config = {}
    config["disable_oob"] = True

    query_hash = _get_query_hash(table_or_sql, config, params)

    def _fetch(entry):
        rows = entry[Constants.CURSOR].fetchmany(Constants.BATCH_SIZE)
        column_names = entry[Constants.COLUMN_NAMES]
        return [mgp.Record(row=_name_row_cells(row, column_names)) for row in rows]

    return oracle_db_state.fetch(query_hash, _fetch)


def cleanup_migrate_oracle_db():
    pass


mgp.add_batch_read_proc(oracle_db, init_migrate_oracle_db, cleanup_migrate_oracle_db)


# PostgreSQL


postgresql_state = _BatchedQueryState("postgresql", _close_dbapi_entry)


def init_migrate_postgresql(
    table_or_sql: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
):
    if params:
        _check_params_type(params, (list, tuple))
    else:
        params = []

    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    if _query_is_table(table_or_sql):
        table_or_sql = f"SELECT * FROM {table_or_sql};"

    query_hash = _get_query_hash(table_or_sql, config, params)

    connection = psycopg2.connect(**config)
    cursor = connection.cursor()
    cursor.execute(table_or_sql, params)

    postgresql_state.start(
        query_hash,
        {
            Constants.CONNECTION: connection,
            Constants.CURSOR: cursor,
            Constants.COLUMN_NAMES: [column.name for column in cursor.description],
        },
    )


def postgresql(
    table_or_sql: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
) -> mgp.Record(row=mgp.Map):
    """
    With migrate.postgresql you can access PostgreSQL and execute queries.
    The result table is converted into a stream, and returned rows can be
    used to create graph structures. Config must be at least empty map.
    If config_path is passed, every key,value pair from JSON file will
    overwrite any values in config file.

    :param table_or_sql: Table name or an SQL query
    :param config: Connection configuration parameters (as in psycopg2.connect)
    :param config_path: Path to the JSON file containing configuration
                        parameters (as in psycopg2.connect)
    :param params: Optionally, queries may be parameterized. In that case,
                   `params` provides parameter values
    :return: The result table as a stream of rows
    """
    if not params:
        params = []

    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    if _query_is_table(table_or_sql):
        table_or_sql = f"SELECT * FROM {table_or_sql};"

    query_hash = _get_query_hash(table_or_sql, config, params)

    def _fetch(entry):
        rows = entry[Constants.CURSOR].fetchmany(Constants.BATCH_SIZE)
        column_names = entry[Constants.COLUMN_NAMES]
        return [mgp.Record(row=_name_row_cells(row, column_names)) for row in rows]

    return postgresql_state.fetch(query_hash, _fetch)


def cleanup_migrate_postgresql():
    pass


mgp.add_batch_read_proc(postgresql, init_migrate_postgresql, cleanup_migrate_postgresql)


# S3


def _close_s3_entry(entry):
    # The CSV reader wraps a TextIOWrapper over the S3 response body.
    # Closing it releases the underlying HTTP connection.
    text_stream = entry.get("text_stream")
    if text_stream is not None:
        try:
            text_stream.close()
        except Exception:
            pass


s3_state = _BatchedQueryState("s3", _close_s3_entry)


def init_migrate_s3(
    file_path: str,
    config: mgp.Map,
    config_path: str = "",
):
    """
    Initialize an S3 connection and prepare to stream a CSV file.

    :param file_path: S3 file path in the format
                      's3://bucket-name/path/to/file.csv'
    :param config: Configuration map containing AWS credentials
                   (access_key, secret_key, region, etc.)
    :param config_path: Path to a JSON file containing configuration parameters
    """
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    # Extract S3 bucket and key
    if not file_path.startswith("s3://"):
        raise ValueError("Invalid S3 path format. " "Expected 's3://bucket-name/path'.")

    file_path_no_protocol = file_path[5:]
    bucket_name, *key_parts = file_path_no_protocol.split("/")
    s3_key = "/".join(key_parts)

    query_hash = _get_query_hash(file_path, config)

    # Initialize S3 client
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=config.get("aws_access_key_id", os.getenv("AWS_ACCESS_KEY_ID", None)),
        aws_secret_access_key=config.get("aws_secret_access_key", os.getenv("AWS_SECRET_ACCESS_KEY", None)),
        aws_session_token=config.get("aws_session_token", os.getenv("AWS_SESSION_TOKEN", None)),
        region_name=config.get("region_name", os.getenv("AWS_REGION", None)),
    )

    # Fetch and read file as a streaming object
    response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
    # Convert binary stream to text stream
    text_stream = io.TextIOWrapper(response["Body"], encoding="utf-8")

    # Read CSV headers
    csv_reader = csv.reader(text_stream)
    column_names = next(csv_reader)  # First row contains column names

    s3_state.start(
        query_hash,
        {
            Constants.CURSOR: csv_reader,
            Constants.COLUMN_NAMES: column_names,
            "text_stream": text_stream,
        },
    )


def s3(
    file_path: str,
    config: mgp.Map,
    config_path: str = "",
) -> mgp.Record(row=mgp.Map):
    """
    Fetch rows from an S3 CSV file in batches.

    :param file_path: S3 file path in the format
                      's3://bucket-name/path/to/file.csv'
    :param config: AWS S3 connection parameters (AWS credentials, region, etc.)
    :param config_path: Optional path to a JSON file containing AWS credentials
    :return: The result table as a stream of rows
    """
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    query_hash = _get_query_hash(file_path, config)

    def _fetch(entry):
        csv_reader = entry[Constants.CURSOR]
        column_names = entry[Constants.COLUMN_NAMES]
        batch_rows = []
        for _ in range(Constants.BATCH_SIZE):
            try:
                row = next(csv_reader)
            except StopIteration:
                break
            batch_rows.append(mgp.Record(row=_name_row_cells(row, column_names)))
        return batch_rows

    return s3_state.fetch(query_hash, _fetch)


def cleanup_migrate_s3():
    pass


mgp.add_batch_read_proc(s3, init_migrate_s3, cleanup_migrate_s3)


def _close_neo4j_entry(entry):
    session = entry.get(Constants.SESSION)
    if session is not None:
        try:
            session.close()
        except Exception:
            pass
    driver = entry.get(Constants.DRIVER)
    if driver is not None:
        try:
            driver.close()
        except Exception:
            pass


neo4j_state = _BatchedQueryState("neo4j", _close_neo4j_entry)


def init_migrate_neo4j(
    label_or_rel_or_query: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
):
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    query = _formulate_cypher_query(label_or_rel_or_query)
    query_hash = _get_query_hash(query, config, params)

    uri = _build_neo4j_uri(config)
    username = config.get(Constants.USERNAME, "neo4j")
    password = config.get(Constants.PASSWORD, "password")
    database = config.get(Constants.DATABASE, None)  # None means default database

    driver = GraphDatabase.driver(uri, auth=(username, password))

    # Create session with optional database parameter
    if database:
        session = driver.session(database=database)
    else:
        session = driver.session()

    # Neo4j expects params to be a dict or None
    cypher_params = params if params is not None else {}
    result = session.run(query, parameters=cypher_params)

    neo4j_state.start(
        query_hash,
        {
            Constants.DRIVER: driver,
            Constants.SESSION: session,
            Constants.RESULT: result,
        },
    )


def neo4j(
    label_or_rel_or_query: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
) -> mgp.Record(row=mgp.Map):
    """
    Migrate data from Neo4j to Memgraph. Can migrate a specific node label, relationship type, or execute a custom Cypher query.

    :param label_or_rel_or_query: Node label, relationship type, or a Cypher query
    :param config: Connection configuration for Neo4j
    :param config_path: Path to a JSON file containing connection parameters
    :param params: Optional query parameters
    :return: Stream of rows from Neo4j
    """
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    query = _formulate_cypher_query(label_or_rel_or_query)
    query_hash = _get_query_hash(query, config, params)

    def _fetch(entry):
        result = entry[Constants.RESULT]
        batch = []
        for record in result:
            batch.append(mgp.Record(row=_convert_neo4j_record(record)))
            if len(batch) >= Constants.BATCH_SIZE:
                break
        return batch

    return neo4j_state.fetch(query_hash, _fetch)


def cleanup_migrate_neo4j():
    pass


mgp.add_batch_read_proc(neo4j, init_migrate_neo4j, cleanup_migrate_neo4j)


def _close_arrow_flight_entry(entry):
    client = entry.get(Constants.CONNECTION)
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


arrow_flight_state = _BatchedQueryState("arrow_flight", _close_arrow_flight_entry)


def init_migrate_arrow_flight(
    query: str,
    config: mgp.Map,
    config_path: str = "",
):
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    query_hash = _get_query_hash(query, config)

    host = config.get(Constants.HOST, None)
    port = config.get(Constants.PORT, None)
    username = config.get(Constants.USERNAME, "")
    password = config.get(Constants.PASSWORD, "")

    # Encode credentials
    auth_string = f"{username}:{password}".encode("utf-8")
    encoded_auth = base64.b64encode(auth_string).decode("utf-8")

    # Establish Flight connection
    client = flight.connect(f"grpc://{host}:{port}")

    # Authenticate
    options = flight.FlightCallOptions(headers=[(b"authorization", f"Basic {encoded_auth}".encode("utf-8"))])

    flight_info = client.get_flight_info(flight.FlightDescriptor.for_command(query), options)

    arrow_flight_state.start(
        query_hash,
        {
            Constants.CONNECTION: client,
            Constants.CURSOR: iter(_fetch_flight_data(client, flight_info, options)),
        },
    )


def _fetch_flight_data(client, flight_info, options):
    """
    Efficiently fetches data in batches from Arrow Flight using RecordBatchReader.
    This prevents high memory usage by avoiding full table loading.
    """
    for endpoint in flight_info.endpoints:
        reader = client.do_get(endpoint.ticket, options)  # Stream the data
        for chunk in reader:  # Iterate over RecordBatches
            batch = chunk.data  # Convert each batch to an Arrow Table
            yield from batch.to_pylist()  # Convert to row dictionaries on demand


def arrow_flight(
    query: str,
    config: mgp.Map,
    config_path: str = "",
) -> mgp.Record(row=mgp.Map):
    """
    Execute a SQL query on Arrow Flight and stream results into Memgraph.

    :param query: SQL query to execute
    :param config: Arrow Flight connection configuration
    :param config_path: Path to a JSON config file
    :return: Stream of rows from Arrow Flight
    """
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    query_hash = _get_query_hash(query, config)

    def _fetch(entry):
        cursor = entry[Constants.CURSOR]
        batch = []
        for _ in range(Constants.BATCH_SIZE):
            try:
                row = _convert_row_types(next(cursor))
            except StopIteration:
                break
            batch.append(mgp.Record(row=row))
        return batch

    return arrow_flight_state.fetch(query_hash, _fetch)


def cleanup_migrate_arrow_flight():
    pass


mgp.add_batch_read_proc(arrow_flight, init_migrate_arrow_flight, cleanup_migrate_arrow_flight)


duckdb_state = _BatchedQueryState("duckdb", lambda e: _close_dbapi_entry(e, commit=False))


def _duckdb_query_hash(query, setup_queries):
    setup_queries_str = json.dumps(setup_queries, sort_keys=False) if setup_queries else ""
    return hashlib.sha256(f"{query}|{setup_queries_str}".encode("utf-8")).hexdigest()


def init_migrate_duckdb(query: str, setup_queries: mgp.Nullable[List[str]] = None):
    """
    Initialize an in-memory DuckDB connection and execute the query.

    :param query: SQL query to execute
    :param setup_queries: Optional list of setup queries to execute before the main query
    """
    query_hash = _duckdb_query_hash(query, setup_queries)

    # Ensure a fresh in-memory DuckDB instance for each query
    connection = duckDB.connect()
    cursor = connection.cursor()
    if setup_queries is not None:
        for setup_query in setup_queries:
            cursor.execute(setup_query)

    cursor.execute(query)

    duckdb_state.start(
        query_hash,
        {
            Constants.CONNECTION: connection,
            Constants.CURSOR: cursor,
            Constants.COLUMN_NAMES: [desc[0] for desc in cursor.description],
        },
    )


def duckdb(query: str, setup_queries: mgp.Nullable[List[str]] = None) -> mgp.Record(row=mgp.Map):
    """
    Fetch rows from DuckDB in batches.

    :param query: SQL query to execute
    :param setup_queries: Optional list of setup queries to execute before the main query
    :return: The result table as a stream of rows
    """
    query_hash = _duckdb_query_hash(query, setup_queries)

    def _fetch(entry):
        rows = entry[Constants.CURSOR].fetchmany(Constants.BATCH_SIZE)
        column_names = entry[Constants.COLUMN_NAMES]
        return [mgp.Record(row=_name_row_cells(row, column_names)) for row in rows]

    return duckdb_state.fetch(query_hash, _fetch)


def cleanup_migrate_duckdb():
    pass


mgp.add_batch_read_proc(duckdb, init_migrate_duckdb, cleanup_migrate_duckdb)


def _close_memgraph_entry(entry):
    conn = entry.get(Constants.CONNECTION)
    if conn is not None:
        try:
            conn.close()
        except Exception:
            pass


memgraph_state = _BatchedQueryState("memgraph", _close_memgraph_entry)


def init_migrate_memgraph(
    label_or_rel_or_query: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
):
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    query = _formulate_cypher_query(label_or_rel_or_query)
    query_hash = _get_query_hash(query, config, params)

    memgraph_db = Memgraph(**config)
    cursor = memgraph_db.execute_and_fetch(query, params)

    memgraph_state.start(
        query_hash,
        {Constants.CONNECTION: memgraph_db, Constants.CURSOR: cursor},
    )


def memgraph(
    label_or_rel_or_query: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
) -> mgp.Record(row=mgp.Map):
    """
    Migrate data from Memgraph to another Memgraph instance. Can migrate a specific node label, relationship type, or execute a custom Cypher query.

    :param label_or_rel_or_query: Node label, relationship type, or a Cypher query
    :param config: Connection configuration for Memgraph
    :param config_path: Path to a JSON file containing connection parameters
    :param params: Optional query parameters
    :return: Stream of rows from Memgraph
    """
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    query = _formulate_cypher_query(label_or_rel_or_query)
    query_hash = _get_query_hash(query, config, params)

    def _fetch(entry):
        cursor = entry[Constants.CURSOR]
        return [
            mgp.Record(row=row) for row in (next(cursor, None) for _ in range(Constants.BATCH_SIZE)) if row is not None
        ]

    return memgraph_state.fetch(query_hash, _fetch)


def cleanup_migrate_memgraph():
    pass


mgp.add_batch_read_proc(memgraph, init_migrate_memgraph, cleanup_migrate_memgraph)


def _close_servicenow_entry(entry):
    # No resource to release — the response body was already consumed into
    # an in-memory iterator during init.
    return


servicenow_state = _BatchedQueryState("servicenow", _close_servicenow_entry)


def init_migrate_servicenow(
    endpoint: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
):
    """
    Initialize the connection to the ServiceNow REST API and fetch the JSON data.

    :param endpoint: ServiceNow API endpoint (full URL)
    :param config: Configuration map containing authentication details (username, password, instance URL, etc.)
    :param config_path: Optional path to a JSON file containing authentication details
    :param params: Optional query parameters for filtering results
    """
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    query_hash = _get_query_hash(endpoint, config, params)

    auth = (config.get(Constants.USERNAME), config.get(Constants.PASSWORD))
    headers = {"Accept": "application/json"}

    response = requests.get(endpoint, auth=auth, headers=headers, params=params)
    response.raise_for_status()

    data = response.json().get(Constants.RESULT, [])
    if not data:
        raise ValueError("No data found in ServiceNow response")

    servicenow_state.start(query_hash, {Constants.CURSOR: iter(data)})


def servicenow(
    endpoint: str,
    config: mgp.Map,
    config_path: str = "",
    params: mgp.Nullable[mgp.Any] = None,
) -> mgp.Record(row=mgp.Map):
    """
    Fetch rows from the ServiceNow REST API in batches.

    :param endpoint: ServiceNow API endpoint (full URL)
    :param config: Authentication details (username, password, instance URL, etc.)
    :param config_path: Optional path to a JSON file containing authentication details
    :param params: Optional query parameters for filtering results
    :return: The result data as a stream of rows
    """
    if len(config_path) > 0:
        config = _combine_config(config=config, config_path=config_path)

    query_hash = _get_query_hash(endpoint, config, params)

    def _fetch(entry):
        data_iter = entry[Constants.CURSOR]
        batch_rows = []
        for _ in range(Constants.BATCH_SIZE):
            try:
                row = next(data_iter)
            except StopIteration:
                break
            batch_rows.append(mgp.Record(row=row))
        return batch_rows

    return servicenow_state.fetch(query_hash, _fetch)


def cleanup_migrate_servicenow():
    pass


mgp.add_batch_read_proc(servicenow, init_migrate_servicenow, cleanup_migrate_servicenow)


def _formulate_cypher_query(label_or_rel_or_query: str) -> str:
    words = label_or_rel_or_query.split()
    if len(words) > 1:
        return label_or_rel_or_query  # Treat it as a Cypher query if multiple words exist

    # Try to see if the syntax matches similar to (:Label) to migrate only nodes
    node_match = re.match(r"^\(\s*:(\w+)\s*\)$", label_or_rel_or_query)

    # Try to see if the syntax matches similar to [:REL_TYPE] to migrate only relationships
    rel_match = re.match(r"^\[\s*:(\w+)\s*\]$", label_or_rel_or_query)

    if node_match:
        label = node_match.group(1)
        return f"MATCH (n:{label}) RETURN labels(n) as labels, properties(n) as properties"

    if rel_match:
        rel_type = rel_match.group(1)
        return f"""
    MATCH (n)-[r:{rel_type}]->(m)
    RETURN
        labels(n) as from_labels,
        labels(m) as to_labels,
        properties(n) as from_properties,
        properties(r) as edge_properties,
        properties(m) as to_properties
    """
    return label_or_rel_or_query  # Assume it's a valid query


def _query_is_table(table_or_sql: str) -> bool:
    return len(table_or_sql.split()) == 1


def _combine_config(config: mgp.Map, config_path: str) -> Dict[str, Any]:
    assert len(config_path), "Path must not be empty"

    file_config = None
    try:
        with open(config_path, "r") as file:
            file_config = json.load(file)
    except Exception:
        raise OSError("Could not open/read file.")

    config.update(file_config)
    return config


def _name_row_cells(row_cells, column_names) -> Dict[str, Any]:
    return {
        column: (value if not isinstance(value, Decimal) else float(value))
        for column, value in zip(column_names, row_cells)
    }


def _name_row_cells_mysql(row_cells, column_names) -> Dict[str, Any]:
    """
    Convert MySQL row cells to Memgraph-compatible types.
    Handles MySQL-specific types that might cause PyObject conversion errors.
    """
    return {column: _convert_mysql_value(value) for column, value in zip(column_names, row_cells)}


def _convert_mysql_value(value: Any) -> Any:
    """
    Convert a MySQL value to a Memgraph-compatible type.
    Returns None for unsupported types and logs a warning.
    """
    if value is None:
        return None

    # Handle Decimal types
    if isinstance(value, Decimal):
        return float(value)
    # Handle datetime types
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        # Use ISO 8601 format for consistency
        try:
            return value.isoformat()
        except Exception:
            return str(value)
    # Handle timedelta
    if isinstance(value, datetime.timedelta):
        return str(value)

    # Handle binary data (BLOB, BINARY, VARBINARY)
    if isinstance(value, (bytes, bytearray)):
        try:
            # Try to decode as UTF-8 string first
            return value.decode("utf-8")
        except UnicodeDecodeError:
            # If not valid UTF-8, convert to base64 string
            return base64.b64encode(value).decode("ascii")

    # Handle geometry types (convert to string representation)
    if hasattr(value, "__class__") and "geometry" in str(value.__class__).lower():
        return str(value) if value else None

    # Handle MySQL-specific numeric types
    if isinstance(value, (int, float, bool)):
        return value

    # Handle string types
    if isinstance(value, str):
        return value

    # Handle list/array types
    if isinstance(value, (list, tuple)):
        return [_convert_mysql_value(item) for item in value]

    # Handle dictionary/map types
    if isinstance(value, dict):
        return {k: _convert_mysql_value(v) for k, v in value.items()}

    # For any other unsupported types, convert to string or return None
    try:
        # Try to convert to string
        str_value = str(value)
        return str_value
    except (ValueError, TypeError):
        # If string conversion fails, return None
        return None


def _convert_row_types(row_cells) -> Dict[str, Any]:
    return {column: (value if not isinstance(value, Decimal) else float(value)) for column, value in row_cells.items()}


def _check_params_type(params: Any, types=(dict, list, tuple)) -> None:
    if not isinstance(params, types):
        raise TypeError(
            "Database query parameter values must be passed in a container of type List[Any] (or Map, if migrating from MySQL or Oracle DB)"
        )


def _convert_neo4j_value(value):
    """Convert Neo4j values to Python-compatible formats."""
    if value is None:
        return None

    # Handle Neo4j DateTime objects
    try:
        if isinstance(value, Neo4jDateTime) or isinstance(value, Neo4jDate):
            return value.to_native()
    except ImportError:
        pass

    # Handle lists and dicts recursively
    if isinstance(value, list):
        return [_convert_neo4j_value(item) for item in value]

    if isinstance(value, dict):
        return {key: _convert_neo4j_value(val) for key, val in value.items()}

    # For other types, return as is
    return value


def _convert_neo4j_record(record):
    """Convert a Neo4j record to a Python dict with proper type conversion."""
    return {key: _convert_neo4j_value(value) for key, value in record.items()}


def _build_neo4j_uri(config: mgp.Map) -> str:
    host = config.get(Constants.HOST, "localhost")
    port = config.get(Constants.PORT, 7687)
    uri_scheme = config.get(Constants.URI_SCHEME, "bolt")
    return f"{uri_scheme}://{host}:{port}"
