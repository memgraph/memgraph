# Copyright 2023 Memgraph Ltd.
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
from common import execute_and_fetch_all, first_connection, memgraph, second_connection


def test_concurrency_if_no_delta_on_same_node_property_update(first_connection, second_connection):
    m1c = first_connection.cursor()
    m2c = second_connection.cursor()

    execute_and_fetch_all(m1c, "CREATE (:Node {prop: 1})")
    first_connection.commit()

    test_has_error = False
    try:
        m1c.execute("MATCH (n) SET n.prop = 1")
        m2c.execute("MATCH (n) SET n.prop = 1")
        first_connection.commit()
        second_connection.commit()
    except Exception as e:
        test_has_error = True

    assert test_has_error is False


def test_metrics_on_write_write_conflicts_increment(first_connection, second_connection, memgraph):
    m1c = first_connection.cursor()
    m2c = second_connection.cursor()

    begin_metrics = list(memgraph.execute_and_fetch("SHOW METRICS INFO"))
    begin_amount_of_conflicts = [x for x in begin_metrics if x["name"] == "WriteWriteConflicts"][0]["value"]
    begin_amount_of_transient_errors = [x for x in begin_metrics if x["name"] == "TransientErrors"][0]["value"]

    execute_and_fetch_all(m1c, "CREATE (:Node {prop: 1})")
    first_connection.commit()

    try:
        m1c.execute("MATCH (n) SET n.prop = 2")
        m2c.execute("MATCH (n) SET n.prop = 2")
        first_connection.commit()
        second_connection.commit()
    except Exception as e:
        pass

    end_metrics = list(memgraph.execute_and_fetch("SHOW METRICS INFO"))

    end_amount_of_conflicts = [x for x in end_metrics if x["name"] == "WriteWriteConflicts"][0]["value"]
    end_amount_of_transient_errors = [x for x in end_metrics if x["name"] == "TransientErrors"][0]["value"]
    assert end_amount_of_conflicts == begin_amount_of_conflicts + 1
    assert end_amount_of_transient_errors == begin_amount_of_transient_errors + 1


def test_concurrency_if_no_delta_on_same_edge_property_update(first_connection, second_connection):
    m1c = first_connection.cursor()
    m2c = second_connection.cursor()

    execute_and_fetch_all(m1c, "CREATE ()-[:TYPE {prop: 1}]->()")
    first_connection.commit()

    test_has_error = False
    try:
        m1c.execute("MATCH (n)-[r]->(m) SET r.prop = 1")
        m2c.execute("MATCH (n)-[r]->(m) SET n.prop = 1")
        first_connection.commit()
        second_connection.commit()
    except Exception as e:
        test_has_error = True

    assert test_has_error is False


def test_concurrency_read_only_v_shared_storage_acc(first_connection, second_connection):
    first_connection.autocommit = False  # needed so the data query to trigger a transaction (assumed write)
    second_connection.autocommit = True  # needed so the index query does not trigger a transaction

    m1c = first_connection.cursor()
    m2c = second_connection.cursor()

    # m1c takes and holds on to shared storage acc (data query)
    execute_and_fetch_all(m1c, "RETURN 1")
    # m2c tries to take a read only storage accessor (index query); should timeout
    m2c_timeout = False
    try:
        execute_and_fetch_all(m2c, "CREATE INDEX ON :L")
    except Exception as e:
        assert (
            str(e)
            == "Cannot get read only access to the storage. Try stopping other queries that are running in parallel."
        )
        m2c_timeout = True

    first_connection.commit()
    assert m2c_timeout is True


def test_concurrency_unique_v_shared_storage_acc(first_connection, second_connection):
    first_connection.autocommit = False  # needed so the data query to trigger a transaction (assumed write)
    second_connection.autocommit = True  # needed so the drop graph query does not trigger a transaction

    m1c = first_connection.cursor()
    m2c = second_connection.cursor()

    # m1c takes and holds on to shared storage acc (data query)
    execute_and_fetch_all(m1c, "RETURN 1")
    # m2c tries to take a unique storage accessor (drop graph query); should timeout
    m2c_timeout = False
    try:
        execute_and_fetch_all(m2c, "DROP GRAPH")
    except Exception as e:
        assert (
            str(e)
            == "Cannot get unique access to the storage. Try stopping other queries that are running in parallel."
        )
        m2c_timeout = True

    first_connection.commit()
    assert m2c_timeout is True


def test_plan_cache_invalidation_on_index_drop(first_connection, second_connection):
    first_connection.autocommit = True
    second_connection.autocommit = False

    # TX1: Create the index
    c = first_connection.cursor()
    execute_and_fetch_all(c, "CREATE INDEX ON :Label(prop)")

    # TX2: Begin transaction on second connection
    tx2_cursor = second_connection.cursor()
    res = execute_and_fetch_all(tx2_cursor, "EXPLAIN MATCH (n:Label) WHERE n.prop = 42 RETURN n")
    # Check we use the index
    assert res == [(" * Produce {n}",), (" * ScanAllByLabelProperties (n :Label {prop})",), (" * Once",)]

    # TX3: Drop index
    c = first_connection.cursor()
    execute_and_fetch_all(c, "DROP INDEX ON :Label(prop)")

    # NOTE: the plan cache has now been cleared

    # TX2: Run the same query again in same txn
    res = execute_and_fetch_all(tx2_cursor, "EXPLAIN MATCH (n:Label) WHERE n.prop = 42 RETURN n")
    # A new plan has been made, still using index since transaction has kept the indexes alive
    assert res == [(" * Produce {n}",), (" * ScanAllByLabelProperties (n :Label {prop})",), (" * Once",)]
    second_connection.commit()

    # NOTE: now the index is no longer existing anywhere

    # TX4: Run the same query again in a new transaction
    c = second_connection.cursor()
    res = execute_and_fetch_all(c, "EXPLAIN MATCH (n:Label) WHERE n.prop = 42 RETURN n")
    # The previous cached plan is now invalid -> because the used index no longer exists
    # Hence plan is removed. A new plan is now made without the index
    assert res == [(" * Produce {n}",), (" * Filter (n :Label), {n.prop}",), (" * ScanAll (n)",), (" * Once",)]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
