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

"""
Comprehensive E2E coverage for per-database memory tracking.

The suite is organized by memory ownership domain:

1. DB storage memory:
   - vertices / edges / properties
   - long-lived schema/index structures
   - trigger store entries
2. DB query memory:
   - PMR-backed query execution state while a query is still alive
3. DB embedding memory:
   - vector index allocations
4. Multi-tenant lifecycle:
   - per-DB isolation
   - create / switch / drop database
   - global tracker release after DB drop

Stream-ingestion memory tracking is intentionally not covered here because the
current configuration E2E environment does not provision Kafka/Pulsar brokers.
"""

import gc
import re
import sys
import threading
import time
import uuid

import mgclient
import pytest
from neo4j import GraphDatabase

BOLT_PORT = 7687
TEMP_DB_PREFIX = "e2e_mem_"


def debug_log(message):
    print(f"[db_memory_tracking {time.monotonic():.3f}] {message}", file=sys.stderr, flush=True)


def connect(database="memgraph", autocommit=True):
    conn = mgclient.connect(host="localhost", port=BOLT_PORT)
    conn.autocommit = True
    if database != "memgraph":
        cursor = conn.cursor()
        cursor.execute(f"USE DATABASE {database}")
    conn.autocommit = autocommit
    return conn


def execute(cursor, query, params=None):
    if params is None:
        cursor.execute(query)
    else:
        cursor.execute(query, params)


def fetch_all(cursor, query, params=None):
    execute(cursor, query, params)
    return cursor.fetchall()


def get_storage_info(cursor):
    return {row[0]: row[1] for row in fetch_all(cursor, "SHOW STORAGE INFO")}


def parse_size_bytes(size_str):
    if not size_str:
        return 0
    match = re.match(r"^(\d+(?:\.\d+)?)\s*(B|KiB|MiB|GiB|TiB)$", size_str.strip())
    if not match:
        return 0
    units = {"B": 1, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    return int(float(match.group(1)) * units[match.group(2)])


def storage_metric_bytes(cursor, key):
    return parse_size_bytes(get_storage_info(cursor)[key])


def wait_until(predicate, timeout=15.0, interval=0.2, message="condition not met"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(message)


def make_temp_db_name(suffix):
    return f"{TEMP_DB_PREFIX}{suffix}_{uuid.uuid4().hex[:8]}"


def list_databases(cursor):
    return [row[0] for row in fetch_all(cursor, "SHOW DATABASES")]


def create_database(cursor, name):
    try:
        execute(cursor, f"CREATE DATABASE {name}")
    except mgclient.DatabaseError as exc:
        lowered = str(exc).lower()
        if "enterprise" in lowered or "not supported" in lowered:
            pytest.skip("CREATE DATABASE requires enterprise features")
        raise
    wait_until(lambda: _database_exists(cursor, name), message=f"database {name} was not created")


def _database_exists(cursor, name):
    return name in list_databases(cursor)


def drop_database(cursor, name):
    deadline = time.time() + 20.0
    while time.time() < deadline:
        try:
            execute(cursor, "USE DATABASE memgraph")
            execute(cursor, f"DROP DATABASE {name}")
            return
        except mgclient.DatabaseError as exc:
            if "does not exist" in str(exc):
                return
            time.sleep(0.2)

    raise AssertionError(f"Failed to drop database {name}")


def create_nodes(cursor, count, label="Node", property_name="id"):
    execute(cursor, f"UNWIND range(1, {count}) AS i CREATE (n:{label} {{{property_name}: i}})")


def create_edges(cursor, count, edge_type="REL"):
    execute(
        cursor,
        f"UNWIND range(1, {count}) AS i " f"CREATE (:Src {{id: i}})-[:{edge_type} {{weight: i}}]->(:Dst {{id: i}})",
    )


def create_embedding_nodes(cursor, count, label="Embedding", property_name="vec", dimension=8):
    vector_literal = "[" + ", ".join(["1.0"] * dimension) + "]"
    execute(
        cursor, f"UNWIND range(1, {count}) AS i " f"CREATE (n:{label} {{{property_name}: {vector_literal}, id: i}})"
    )


def drop_all_triggers(cursor):
    for row in fetch_all(cursor, "SHOW TRIGGERS"):
        execute(cursor, f"DROP TRIGGER {row[0]}")


def metric_triplet(cursor):
    info = get_storage_info(cursor)
    return {
        "db_memory_tracked": parse_size_bytes(info["db_memory_tracked"]),
        "db_storage_memory_tracked": parse_size_bytes(info["db_storage_memory_tracked"]),
        "db_embedding_memory_tracked": parse_size_bytes(info["db_embedding_memory_tracked"]),
        "db_query_memory_tracked": parse_size_bytes(info["db_query_memory_tracked"]),
        "memory_tracked": parse_size_bytes(info["memory_tracked"]),
        "graph_memory_tracked": parse_size_bytes(info["graph_memory_tracked"]),
        "query_memory_tracked": parse_size_bytes(info["query_memory_tracked"]),
        "vector_index_memory_tracked": parse_size_bytes(info["vector_index_memory_tracked"]),
    }


def set_session_isolation_level(cursor, isolation_level):
    execute(cursor, f"SET SESSION TRANSACTION ISOLATION LEVEL {isolation_level}")


def assert_metrics_close(lhs, rhs, tolerance_bytes, message):
    assert (
        abs(lhs - rhs) <= tolerance_bytes
    ), f"{message}: lhs={lhs}, rhs={rhs}, tolerance={tolerance_bytes}, diff={abs(lhs - rhs)}"


def assert_metric_returns_near_baseline(cursor, key, baseline, tolerance_bytes, timeout=20.0, message=None):
    if message is None:
        message = f"{key} did not return near baseline"
    wait_until(
        lambda: metric_triplet(cursor)[key] <= baseline + tolerance_bytes,
        timeout=timeout,
        message=message,
    )


def release_query_memory_hold(cursor, signal_id):
    debug_log(f"release proc start signal_id={signal_id}")
    rows = fetch_all(
        cursor,
        "CALL libdb_query_tracking_proc.release_hold_query_memory($signal_id) YIELD released RETURN released",
        {"signal_id": signal_id},
    )
    debug_log(f"release proc done signal_id={signal_id} rows={rows}")
    assert rows == [(True,)]


def run_sleeping_query_memory_proc(database, signal_id, mebibytes=32, max_wait_ms=15000, autocommit=True):
    state = {
        "started": threading.Event(),
        "finished": threading.Event(),
        "result": None,
        "error": None,
        "timeline": [],
    }

    def worker():
        driver = None
        try:
            debug_log(f"worker connect start db={database} signal_id={signal_id}")
            driver = GraphDatabase.driver(f"bolt://localhost:{BOLT_PORT}", auth=("", ""))
            with driver.session(database=database) as session:
                session.run("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED").consume()
                state["timeline"].append(("worker_ready", time.monotonic()))
                debug_log(f"worker ready db={database} signal_id={signal_id}")
                state["started"].set()
                state["timeline"].append(("worker_execute_start", time.monotonic()))
                debug_log(
                    f"worker execute start db={database} signal_id={signal_id} mebibytes={mebibytes} max_wait_ms={max_wait_ms}"
                )
                result = session.run(
                    "CALL libdb_query_tracking_proc.hold_query_memory($signal_id, $mebibytes, $max_wait_ms) "
                    "YIELD allocated_bytes RETURN allocated_bytes",
                    signal_id=signal_id,
                    mebibytes=mebibytes,
                    max_wait_ms=max_wait_ms,
                )
                state["timeline"].append(("worker_execute_done", time.monotonic()))
                debug_log(f"worker execute done db={database} signal_id={signal_id}")
                state["result"] = [tuple(record.values()) for record in result]
                state["timeline"].append(("worker_fetch_done", time.monotonic()))
                debug_log(f"worker fetch done db={database} signal_id={signal_id} result={state['result']}")
        except Exception as exc:
            state["error"] = exc
            state["timeline"].append(("worker_error", time.monotonic(), repr(exc)))
            debug_log(f"worker error db={database} signal_id={signal_id} error={exc!r}")
        finally:
            if driver is not None:
                driver.close()
            state["timeline"].append(("worker_finished", time.monotonic()))
            debug_log(f"worker finished db={database} signal_id={signal_id}")
            state["finished"].set()

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    return thread, state


@pytest.fixture(scope="module", autouse=True)
def require_multitenancy():
    conn = connect()
    cursor = conn.cursor()
    probe_db = make_temp_db_name("probe")
    try:
        create_database(cursor, probe_db)
        drop_database(cursor, probe_db)
    finally:
        conn.close()


@pytest.fixture(autouse=True)
def cleanup_temp_databases():
    yield
    gc.collect()
    conn = connect()
    cursor = conn.cursor()
    for db_name in list_databases(cursor):
        if db_name.startswith(TEMP_DB_PREFIX):
            try:
                drop_database(cursor, db_name)
            except AssertionError:
                pass
    conn.close()


def test_show_storage_info_contains_db_split_fields():
    conn = connect()
    cursor = conn.cursor()
    info = get_storage_info(cursor)
    conn.close()

    required = {
        "db_memory_tracked",
        "db_storage_memory_tracked",
        "db_embedding_memory_tracked",
        "db_query_memory_tracked",
        "memory_tracked",
        "graph_memory_tracked",
        "query_memory_tracked",
        "vector_index_memory_tracked",
    }
    assert required.issubset(set(info.keys())), f"Missing keys: {required - set(info.keys())}"


def test_db_total_equals_storage_plus_embedding_plus_query():
    conn = connect()
    cursor = conn.cursor()
    info = metric_triplet(cursor)
    conn.close()

    assert_metrics_close(
        info["db_memory_tracked"],
        info["db_storage_memory_tracked"] + info["db_embedding_memory_tracked"] + info["db_query_memory_tracked"],
        64 * 1024,
        "db total should match storage+embedding+query within readable-size rounding tolerance",
    )


def test_storage_memory_grows_for_autocommit_node_and_edge_creation():
    db_name = make_temp_db_name("storage_autocommit")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    conn = connect(db_name)
    cursor = conn.cursor()
    before = metric_triplet(cursor)

    create_nodes(cursor, 5000, label="AutoNode")
    create_edges(cursor, 3000, edge_type="AUTO_REL")

    after = metric_triplet(cursor)

    conn.close()
    admin.close()

    assert after["db_storage_memory_tracked"] > before["db_storage_memory_tracked"]
    assert after["db_memory_tracked"] > before["db_memory_tracked"]
    assert after["db_query_memory_tracked"] <= before["db_query_memory_tracked"] + 128 * 1024


def test_explicit_transaction_storage_visible_before_commit_and_persists_after_commit():
    db_name = make_temp_db_name("storage_explicit_commit")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    tx_conn = connect(db_name, autocommit=False)
    tx_cursor = tx_conn.cursor()
    inspect_conn = connect(db_name)
    inspect_cursor = inspect_conn.cursor()

    before = metric_triplet(inspect_cursor)

    create_nodes(tx_cursor, 2500, label="TxCommit")

    wait_until(
        lambda: metric_triplet(inspect_cursor)["db_storage_memory_tracked"] > before["db_storage_memory_tracked"],
        message="uncommitted storage allocations did not become visible",
    )
    mid = metric_triplet(inspect_cursor)

    tx_conn.commit()
    wait_until(
        lambda: metric_triplet(inspect_cursor)["db_storage_memory_tracked"] >= mid["db_storage_memory_tracked"],
        message="committed storage allocations disappeared unexpectedly",
    )
    after = metric_triplet(inspect_cursor)

    tx_conn.close()
    inspect_conn.close()
    admin.close()

    assert mid["db_storage_memory_tracked"] > before["db_storage_memory_tracked"]
    assert after["db_storage_memory_tracked"] >= mid["db_storage_memory_tracked"]


def test_explicit_transaction_rollback_returns_storage_near_baseline():
    db_name = make_temp_db_name("storage_rollback")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    tx_conn = connect(db_name, autocommit=False)
    tx_cursor = tx_conn.cursor()
    inspect_conn = connect(db_name)
    inspect_cursor = inspect_conn.cursor()

    before = metric_triplet(inspect_cursor)
    create_nodes(tx_cursor, 2500, label="TxRollback")

    wait_until(
        lambda: metric_triplet(inspect_cursor)["db_storage_memory_tracked"] > before["db_storage_memory_tracked"],
        message="rollback test never observed storage growth",
    )

    tx_conn.rollback()
    execute(inspect_cursor, "FREE MEMORY")
    assert_metric_returns_near_baseline(
        inspect_cursor,
        "db_storage_memory_tracked",
        before["db_storage_memory_tracked"],
        1024 * 1024,
        message="storage memory did not return near baseline after rollback",
    )
    after = metric_triplet(inspect_cursor)

    tx_conn.close()
    inspect_conn.close()
    admin.close()

    assert after["db_storage_memory_tracked"] <= before["db_storage_memory_tracked"] + 1024 * 1024


def test_storage_memory_drops_after_delete_and_free_memory():
    db_name = make_temp_db_name("storage_delete")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    conn = connect(db_name)
    cursor = conn.cursor()
    before = metric_triplet(cursor)

    create_nodes(cursor, 5000, label="DeleteNode")
    create_edges(cursor, 2500, edge_type="DELETE_REL")
    after_create = metric_triplet(cursor)

    execute(cursor, "MATCH (n) DETACH DELETE n")
    execute(cursor, "FREE MEMORY")
    after_delete = metric_triplet(cursor)

    conn.close()
    admin.close()

    assert after_create["db_storage_memory_tracked"] > before["db_storage_memory_tracked"]
    assert after_delete["db_storage_memory_tracked"] < after_create["db_storage_memory_tracked"] - 2 * 1024 * 1024
    assert after_delete["db_storage_memory_tracked"] <= before["db_storage_memory_tracked"] + 3 * 1024 * 1024


def test_query_memory_is_visible_while_query_is_executing_and_drops_after_finish():
    db_name = make_temp_db_name("query_alive")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    inspect_conn = connect(db_name)
    inspect_cursor = inspect_conn.cursor()
    set_session_isolation_level(inspect_cursor, "READ UNCOMMITTED")

    before = metric_triplet(inspect_cursor)
    signal_id = uuid.uuid4().hex
    debug_log(f"query test start db={db_name} signal_id={signal_id} before={before}")
    worker, state = run_sleeping_query_memory_proc(db_name, signal_id, mebibytes=32)
    assert state["started"].wait(timeout=5.0)
    debug_log(f"worker signaled start signal_id={signal_id}")
    time.sleep(0.5)

    query_growth_threshold = 8 * 1024 * 1024
    during = None
    try:
        deadline = time.time() + 5.0
        while time.time() < deadline:
            current = metric_triplet(inspect_cursor)
            debug_log(
                "poll "
                f"signal_id={signal_id} db_query_memory_tracked={current['db_query_memory_tracked']} "
                f"thread_alive={worker.is_alive()} finished={state['finished'].is_set()}"
            )
            if current["db_query_memory_tracked"] >= before["db_query_memory_tracked"] + query_growth_threshold:
                during = current
                debug_log(f"query memory became visible signal_id={signal_id} during={during}")
                break
            time.sleep(0.2)
    finally:
        debug_log(f"about to release signal_id={signal_id} thread_alive={worker.is_alive()}")
        release_query_memory_hold(inspect_cursor, signal_id)
        worker.join(timeout=10.0)
        debug_log(
            f"after join signal_id={signal_id} thread_alive={worker.is_alive()} "
            f"finished={state['finished'].is_set()} timeline={state['timeline']}"
        )
        assert not worker.is_alive(), "query memory helper thread did not finish"
        assert state["error"] is None, f"query memory helper failed: {state['error']}"
        assert state["result"] == [(32 * 1024 * 1024,)]

    assert during is not None, "query memory did not become visible while the query was executing"

    assert_metric_returns_near_baseline(
        inspect_cursor,
        "db_query_memory_tracked",
        before["db_query_memory_tracked"],
        128 * 1024,
        message="query memory did not drop near baseline after cursor consumption",
    )
    after = metric_triplet(inspect_cursor)

    inspect_conn.close()
    admin.close()

    assert during["db_query_memory_tracked"] >= before["db_query_memory_tracked"] + query_growth_threshold
    assert after["db_query_memory_tracked"] <= before["db_query_memory_tracked"] + 128 * 1024


def test_query_memory_is_isolated_per_database():
    db_a = make_temp_db_name("query_iso_a")
    db_b = make_temp_db_name("query_iso_b")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_a)
    create_database(admin_cursor, db_b)

    inspect_a_conn = connect(db_a)
    inspect_a = inspect_a_conn.cursor()
    inspect_b_conn = connect(db_b)
    inspect_b = inspect_b_conn.cursor()
    set_session_isolation_level(inspect_a, "READ UNCOMMITTED")
    set_session_isolation_level(inspect_b, "READ UNCOMMITTED")

    before_a = metric_triplet(inspect_a)
    before_b = metric_triplet(inspect_b)
    signal_id = uuid.uuid4().hex
    debug_log(f"query isolation test start db_a={db_a} db_b={db_b} signal_id={signal_id}")
    worker, state = run_sleeping_query_memory_proc(db_a, signal_id, mebibytes=32)
    assert state["started"].wait(timeout=5.0)
    debug_log(f"query isolation worker started signal_id={signal_id}")

    query_growth_threshold = 8 * 1024 * 1024
    during_a = None
    during_b = None
    try:
        deadline = time.time() + 5.0
        while time.time() < deadline:
            current_a = metric_triplet(inspect_a)
            current_b = metric_triplet(inspect_b)
            debug_log(
                "isolation poll "
                f"signal_id={signal_id} db_a_query={current_a['db_query_memory_tracked']} "
                f"db_b_query={current_b['db_query_memory_tracked']} "
                f"thread_alive={worker.is_alive()} finished={state['finished'].is_set()}"
            )
            if current_a["db_query_memory_tracked"] >= before_a["db_query_memory_tracked"] + query_growth_threshold:
                during_a = current_a
                during_b = current_b
                debug_log(
                    f"query isolation memory became visible signal_id={signal_id} during_a={during_a} during_b={during_b}"
                )
                break
            time.sleep(0.2)
    finally:
        debug_log(f"query isolation releasing signal_id={signal_id} thread_alive={worker.is_alive()}")
        release_query_memory_hold(inspect_a, signal_id)
        worker.join(timeout=10.0)
        debug_log(
            f"query isolation after join signal_id={signal_id} thread_alive={worker.is_alive()} "
            f"finished={state['finished'].is_set()} timeline={state['timeline']}"
        )
        assert not worker.is_alive(), "query memory helper thread did not finish"
        assert state["error"] is None, f"query memory helper failed: {state['error']}"
        assert state["result"] == [(32 * 1024 * 1024,)]

    inspect_a_conn.close()
    inspect_b_conn.close()
    admin.close()

    assert during_a is not None, "db A query memory did not grow"
    assert during_a["db_query_memory_tracked"] >= before_a["db_query_memory_tracked"] + query_growth_threshold
    assert during_b["db_query_memory_tracked"] <= before_b["db_query_memory_tracked"] + 128 * 1024


def test_query_memory_in_explicit_transaction_drops_after_query_finishes_before_commit():
    db_name = make_temp_db_name("query_explicit_tx")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    inspect_conn = connect(db_name)
    inspect_cursor = inspect_conn.cursor()
    set_session_isolation_level(inspect_cursor, "READ UNCOMMITTED")

    before = metric_triplet(inspect_cursor)
    signal_id = uuid.uuid4().hex
    debug_log(f"query explicit-tx test start db={db_name} signal_id={signal_id}")
    worker, state = run_sleeping_query_memory_proc(db_name, signal_id, mebibytes=32, autocommit=False)
    assert state["started"].wait(timeout=5.0)
    debug_log(f"query explicit-tx worker started signal_id={signal_id}")

    query_growth_threshold = 8 * 1024 * 1024
    during = None
    try:
        deadline = time.time() + 5.0
        while time.time() < deadline:
            current = metric_triplet(inspect_cursor)
            debug_log(
                "explicit-tx poll "
                f"signal_id={signal_id} db_query_memory_tracked={current['db_query_memory_tracked']} "
                f"thread_alive={worker.is_alive()} finished={state['finished'].is_set()}"
            )
            if current["db_query_memory_tracked"] >= before["db_query_memory_tracked"] + query_growth_threshold:
                during = current
                debug_log(f"query explicit-tx memory became visible signal_id={signal_id} during={during}")
                break
            time.sleep(0.2)
    finally:
        debug_log(f"query explicit-tx releasing signal_id={signal_id} thread_alive={worker.is_alive()}")
        release_query_memory_hold(inspect_cursor, signal_id)
        worker.join(timeout=10.0)
        debug_log(
            f"query explicit-tx after join signal_id={signal_id} thread_alive={worker.is_alive()} "
            f"finished={state['finished'].is_set()} timeline={state['timeline']}"
        )
        assert not worker.is_alive(), "query memory helper thread did not finish"
        assert state["error"] is None, f"query memory helper failed: {state['error']}"
        assert state["result"] == [(32 * 1024 * 1024,)]

    assert during is not None, "explicit transaction query memory did not become visible while the query was executing"
    assert_metric_returns_near_baseline(
        inspect_cursor,
        "db_query_memory_tracked",
        before["db_query_memory_tracked"],
        128 * 1024,
        message="explicit transaction query memory did not return near baseline after the query finished",
    )
    after = metric_triplet(inspect_cursor)

    inspect_conn.close()
    admin.close()

    assert during["db_query_memory_tracked"] >= before["db_query_memory_tracked"] + query_growth_threshold
    assert after["db_query_memory_tracked"] <= before["db_query_memory_tracked"] + 128 * 1024


def test_query_memory_stays_high_while_result_is_partially_pulled():
    db_name = make_temp_db_name("query_partial_pull")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    inspect_conn = connect(db_name)
    inspect_cursor = inspect_conn.cursor()
    before = metric_triplet(inspect_cursor)
    debug_log(f"partial-pull before db={db_name} metrics={before}")

    with GraphDatabase.driver(f"bolt://localhost:{BOLT_PORT}", auth=("", "")) as driver:
        with driver.session(database=db_name, fetch_size=100) as session:
            result = session.run(
                "UNWIND range(1, $count) AS i " "WITH collect(i) AS xs " "UNWIND xs AS x " "RETURN x",
                count=300000,
            )

            first_batch = result.fetch(100)
            assert len(first_batch) == 100
            wait_until(
                lambda: metric_triplet(inspect_cursor)["db_query_memory_tracked"]
                >= before["db_query_memory_tracked"] + 1024 * 1024,
                message="query memory did not become visible after the first partial pull",
            )
            after_first_fetch = metric_triplet(inspect_cursor)
            debug_log(f"partial-pull after first fetch db={db_name} metrics={after_first_fetch}")

            second_batch = result.fetch(100)
            assert len(second_batch) == 100
            after_second_fetch = metric_triplet(inspect_cursor)
            debug_log(f"partial-pull after second fetch db={db_name} metrics={after_second_fetch}")

            result.consume()

    assert_metric_returns_near_baseline(
        inspect_cursor,
        "db_query_memory_tracked",
        before["db_query_memory_tracked"],
        128 * 1024,
        message="query memory did not return near baseline after the partially pulled result was consumed",
    )
    after_consume = metric_triplet(inspect_cursor)
    debug_log(f"partial-pull after consume db={db_name} metrics={after_consume}")

    inspect_conn.close()
    admin.close()

    assert after_first_fetch["db_query_memory_tracked"] >= before["db_query_memory_tracked"] + 1024 * 1024
    assert after_second_fetch["db_query_memory_tracked"] >= before["db_query_memory_tracked"] + 1024 * 1024
    assert after_consume["db_query_memory_tracked"] <= before["db_query_memory_tracked"] + 128 * 1024


def test_label_and_property_index_creation_grows_db_storage_memory():
    db_name = make_temp_db_name("indices")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    conn = connect(db_name)
    cursor = conn.cursor()
    # SHOW STORAGE INFO tracks committed DB-arena pages, not precise live bytes.
    # Small index builds can fit into arena slack already committed by node creation,
    # so use a larger dataset to force an observable committed-page increase.
    create_nodes(cursor, 100000, label="Indexed", property_name="value")
    execute(cursor, "FREE MEMORY")
    execute(cursor, "FREE MEMORY")

    before = metric_triplet(cursor)
    debug_log(f"index create before db={db_name} metrics={before}")
    execute(cursor, "CREATE INDEX ON :Indexed;")
    execute(cursor, "CREATE INDEX ON :Indexed(value);")
    after = metric_triplet(cursor)
    debug_log(f"index create after db={db_name} metrics={after}")

    conn.close()
    admin.close()

    assert after["db_storage_memory_tracked"] > before["db_storage_memory_tracked"]
    assert after["db_memory_tracked"] > before["db_memory_tracked"]


def test_index_and_constraint_drop_returns_storage_near_baseline():
    db_name = make_temp_db_name("indices_drop")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    conn = connect(db_name)
    cursor = conn.cursor()
    create_nodes(cursor, 100000, label="Indexed", property_name="value")
    create_edges(cursor, 50000, edge_type="TRACKED")
    create_nodes(cursor, 50000, label="UniqueNode", property_name="uid")
    execute(cursor, "FREE MEMORY")
    execute(cursor, "FREE MEMORY")

    before = metric_triplet(cursor)
    debug_log(f"index/constraint drop before db={db_name} metrics={before}")
    execute(cursor, "CREATE INDEX ON :Indexed;")
    execute(cursor, "CREATE INDEX ON :Indexed(value);")
    execute(cursor, "CREATE EDGE INDEX ON :TRACKED;")
    execute(cursor, "CREATE EDGE INDEX ON :TRACKED(weight);")
    execute(cursor, "CREATE CONSTRAINT ON (n:UniqueNode) ASSERT n.uid IS UNIQUE;")
    after_create = metric_triplet(cursor)
    debug_log(f"index/constraint drop after create db={db_name} metrics={after_create}")

    execute(cursor, "DROP INDEX ON :Indexed;")
    execute(cursor, "DROP INDEX ON :Indexed(value);")
    execute(cursor, "DROP EDGE INDEX ON :TRACKED;")
    execute(cursor, "DROP EDGE INDEX ON :TRACKED(weight);")
    execute(cursor, "DROP CONSTRAINT ON (n:UniqueNode) ASSERT n.uid IS UNIQUE;")
    execute(cursor, "FREE MEMORY")
    execute(cursor, "FREE MEMORY")
    assert_metric_returns_near_baseline(
        cursor,
        "db_storage_memory_tracked",
        before["db_storage_memory_tracked"],
        1024 * 1024,
        message="index/constraint storage memory did not return near baseline after drop",
    )
    after_drop = metric_triplet(cursor)
    debug_log(f"index/constraint drop after drop db={db_name} metrics={after_drop}")

    conn.close()
    admin.close()

    assert after_create["db_storage_memory_tracked"] > before["db_storage_memory_tracked"]
    assert after_drop["db_storage_memory_tracked"] <= before["db_storage_memory_tracked"] + 1024 * 1024


def test_edge_indices_and_unique_constraint_grow_db_storage_memory():
    db_name = make_temp_db_name("edge_indices")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    conn = connect(db_name)
    cursor = conn.cursor()
    create_edges(cursor, 50000, edge_type="TRACKED")
    create_nodes(cursor, 50000, label="UniqueNode", property_name="uid")
    execute(cursor, "FREE MEMORY")
    execute(cursor, "FREE MEMORY")

    before = metric_triplet(cursor)
    debug_log(f"edge-index/constraint before db={db_name} metrics={before}")
    execute(cursor, "CREATE EDGE INDEX ON :TRACKED;")
    execute(cursor, "CREATE EDGE INDEX ON :TRACKED(weight);")
    execute(cursor, "CREATE CONSTRAINT ON (n:UniqueNode) ASSERT n.uid IS UNIQUE;")
    after = metric_triplet(cursor)
    debug_log(f"edge-index/constraint after db={db_name} metrics={after}")

    conn.close()
    admin.close()

    assert after["db_storage_memory_tracked"] > before["db_storage_memory_tracked"]
    assert after["db_memory_tracked"] > before["db_memory_tracked"]


def test_trigger_store_entries_and_after_commit_trigger_writes_are_attributed():
    db_name = make_temp_db_name("triggers")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    conn = connect(db_name)
    cursor = conn.cursor()
    before = metric_triplet(cursor)

    for i in range(50):
        execute(
            cursor,
            f"""
            CREATE TRIGGER trig_{i}
            ON () CREATE AFTER COMMIT EXECUTE
            CREATE (:Audit {{trigger_id: {i}}});
            """,
        )

    after_define = metric_triplet(cursor)
    execute(cursor, "CREATE (:TriggerSource {id: 1})")
    wait_until(
        lambda: fetch_all(cursor, "MATCH (n:Audit) RETURN count(n)")[0][0] == 50,
        message="after-commit triggers did not create audit nodes",
    )
    after_fire = metric_triplet(cursor)

    drop_all_triggers(cursor)

    conn.close()
    admin.close()

    assert after_define["db_storage_memory_tracked"] > before["db_storage_memory_tracked"]
    assert after_fire["db_storage_memory_tracked"] > after_define["db_storage_memory_tracked"]


def test_trigger_definition_drop_returns_storage_near_baseline():
    db_name = make_temp_db_name("trigger_drop")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    conn = connect(db_name)
    cursor = conn.cursor()
    before = metric_triplet(cursor)

    for i in range(50):
        execute(
            cursor,
            f"""
            CREATE TRIGGER drop_trig_{i}
            ON () CREATE AFTER COMMIT EXECUTE
            CREATE (:DropAudit {{trigger_id: {i}}});
            """,
        )

    after_create = metric_triplet(cursor)
    assert len(fetch_all(cursor, "SHOW TRIGGERS")) == 50

    drop_all_triggers(cursor)
    execute(cursor, "FREE MEMORY")
    assert len(fetch_all(cursor, "SHOW TRIGGERS")) == 0
    assert_metric_returns_near_baseline(
        cursor,
        "db_storage_memory_tracked",
        before["db_storage_memory_tracked"],
        512 * 1024,
        message="trigger definition memory did not return near baseline after dropping triggers",
    )
    after_drop = metric_triplet(cursor)

    conn.close()
    admin.close()

    assert after_create["db_storage_memory_tracked"] > before["db_storage_memory_tracked"]
    assert after_drop["db_storage_memory_tracked"] <= before["db_storage_memory_tracked"] + 512 * 1024


def test_use_database_switch_changes_visible_db_metrics():
    db_a = make_temp_db_name("switch_a")
    db_b = make_temp_db_name("switch_b")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_a)
    create_database(admin_cursor, db_b)

    conn_a = connect(db_a)
    cur_a = conn_a.cursor()
    create_nodes(cur_a, 4000, label="SwitchNode")
    create_edges(cur_a, 1500, edge_type="SWITCH_REL")
    conn_a.close()

    inspect_conn = connect("memgraph")
    inspect_cursor = inspect_conn.cursor()

    execute(inspect_cursor, f"USE DATABASE {db_a}")
    metrics_a = metric_triplet(inspect_cursor)

    execute(inspect_cursor, f"USE DATABASE {db_b}")
    metrics_b = metric_triplet(inspect_cursor)

    inspect_conn.close()
    admin.close()

    assert metrics_a["db_storage_memory_tracked"] > metrics_b["db_storage_memory_tracked"] + 512 * 1024


def test_vector_index_memory_uses_embedding_tracker_and_returns_on_drop():
    db_name = make_temp_db_name("vector")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    conn = connect(db_name)
    cursor = conn.cursor()
    create_embedding_nodes(cursor, 10000, label="Embedding", property_name="vec", dimension=8)
    execute(cursor, "FREE MEMORY")
    execute(cursor, "FREE MEMORY")

    before = metric_triplet(cursor)
    debug_log(f"vector index before db={db_name} metrics={before}")
    execute(cursor, 'CREATE VECTOR INDEX emb_idx ON :Embedding(vec) WITH CONFIG {"dimension": 8, "capacity": 20000};')
    after_create = metric_triplet(cursor)
    debug_log(f"vector index after create db={db_name} metrics={after_create}")

    execute(cursor, "DROP VECTOR INDEX emb_idx")
    execute(cursor, "FREE MEMORY")
    execute(cursor, "FREE MEMORY")
    assert_metric_returns_near_baseline(
        cursor,
        "db_embedding_memory_tracked",
        before["db_embedding_memory_tracked"],
        128 * 1024,
        message="embedding memory did not return near baseline after dropping the vector index",
    )
    after_drop = metric_triplet(cursor)
    debug_log(f"vector index after drop db={db_name} metrics={after_drop}")

    conn.close()
    admin.close()

    assert after_create["db_embedding_memory_tracked"] > before["db_embedding_memory_tracked"]
    assert after_create["db_memory_tracked"] > before["db_memory_tracked"]
    assert after_drop["db_embedding_memory_tracked"] <= before["db_embedding_memory_tracked"] + 128 * 1024


def test_vector_index_memory_is_isolated_per_database():
    db_a = make_temp_db_name("vector_a")
    db_b = make_temp_db_name("vector_b")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_a)
    create_database(admin_cursor, db_b)

    conn_a = connect(db_a)
    cur_a = conn_a.cursor()
    conn_b = connect(db_b)
    cur_b = conn_b.cursor()

    create_embedding_nodes(cur_a, 2000, label="Embedding", property_name="vec", dimension=4)
    before_a = metric_triplet(cur_a)
    before_b = metric_triplet(cur_b)
    execute(cur_a, 'CREATE VECTOR INDEX emb_idx ON :Embedding(vec) WITH CONFIG {"dimension": 4, "capacity": 4000};')
    after_a = metric_triplet(cur_a)
    after_b = metric_triplet(cur_b)

    conn_a.close()
    conn_b.close()
    admin.close()

    assert after_a["db_embedding_memory_tracked"] > before_a["db_embedding_memory_tracked"]
    assert after_b["db_embedding_memory_tracked"] <= before_b["db_embedding_memory_tracked"] + 128 * 1024


def test_vector_deletes_do_not_free_embedding_memory_until_index_drop():
    db_name = make_temp_db_name("vector_delete")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    conn = connect(db_name)
    cursor = conn.cursor()
    create_embedding_nodes(cursor, 2500, label="Embedding", property_name="vec", dimension=8)

    before = metric_triplet(cursor)
    execute(cursor, 'CREATE VECTOR INDEX emb_idx ON :Embedding(vec) WITH CONFIG {"dimension": 8, "capacity": 5000};')
    after_index = metric_triplet(cursor)

    execute(cursor, "MATCH (n:Embedding) REMOVE n.vec")
    execute(cursor, "MATCH (n:Embedding) DETACH DELETE n")
    execute(cursor, "FREE MEMORY")
    after_delete = metric_triplet(cursor)

    execute(cursor, "DROP VECTOR INDEX emb_idx")
    assert_metric_returns_near_baseline(
        cursor,
        "db_embedding_memory_tracked",
        before["db_embedding_memory_tracked"],
        128 * 1024,
        message="embedding memory did not return near baseline after vector index drop",
    )
    after_drop = metric_triplet(cursor)

    conn.close()
    admin.close()

    assert after_index["db_embedding_memory_tracked"] > before["db_embedding_memory_tracked"]
    assert after_delete["db_embedding_memory_tracked"] >= after_index["db_embedding_memory_tracked"] - 128 * 1024
    assert after_drop["db_embedding_memory_tracked"] <= before["db_embedding_memory_tracked"] + 128 * 1024


def test_drop_database_releases_global_memory():
    db_name = make_temp_db_name("drop_release")
    admin = connect()
    admin_cursor = admin.cursor()
    create_database(admin_cursor, db_name)

    memgraph_conn = connect("memgraph")
    memgraph_cursor = memgraph_conn.cursor()
    execute(memgraph_cursor, "FREE MEMORY")
    execute(memgraph_cursor, "FREE MEMORY")
    baseline = metric_triplet(memgraph_cursor)
    debug_log(f"drop-db baseline db={db_name} metrics={baseline}")

    db_conn = connect(db_name)
    db_cursor = db_conn.cursor()
    create_nodes(db_cursor, 100000, label="DropNode")
    execute(db_cursor, "CREATE INDEX ON :DropNode;")
    execute(db_cursor, "FREE MEMORY")
    execute(db_cursor, "FREE MEMORY")
    db_after_alloc = metric_triplet(memgraph_cursor)
    debug_log(f"drop-db after alloc db={db_name} metrics={db_after_alloc}")
    assert db_after_alloc["graph_memory_tracked"] > baseline["graph_memory_tracked"]

    db_conn.close()
    drop_database(admin_cursor, db_name)
    execute(memgraph_cursor, "FREE MEMORY")
    execute(memgraph_cursor, "FREE MEMORY")
    assert_metric_returns_near_baseline(
        memgraph_cursor,
        "graph_memory_tracked",
        baseline["graph_memory_tracked"],
        1024 * 1024,
        message="global graph tracker did not return near baseline after database drop",
    )
    after_drop = metric_triplet(memgraph_cursor)
    debug_log(f"drop-db after drop db={db_name} metrics={after_drop}")

    memgraph_conn.close()
    admin.close()

    assert after_drop["graph_memory_tracked"] <= baseline["graph_memory_tracked"] + 1024 * 1024


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA", "-v"]))
