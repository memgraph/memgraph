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
from common import execute_and_fetch_all, first_connection, second_connection


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


def test_concurrency_unique_v_shared_storage_acc(first_connection, second_connection):
    first_connection.autocommit = False  # needed so the data query to trigger a transaction
    second_connection.autocommit = True  # needed so the index query does not trigger a transaction

    m1c = first_connection.cursor()
    m2c = second_connection.cursor()

    # m1c takes and holds on to shared storage acc (data query)
    execute_and_fetch_all(m1c, "RETURN 1")
    # m2c tries to take a unique storage accessor (index query); should timeout
    m2c_timeout = False
    try:
        execute_and_fetch_all(m2c, "CREATE INDEX ON :L")
    except Exception as e:
        assert (
            str(e)
            == "Cannot get unique access to the storage. Try stopping other queries that are running in parallel."
        )
        m2c_timeout = True

    first_connection.commit()
    assert m2c_timeout is True


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
