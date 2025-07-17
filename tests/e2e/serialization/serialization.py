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
import time
from threading import Barrier, Thread

import pytest
from common import connect, execute_and_fetch_all, make_connection


def test_wait_on_creating_edges(connect, make_connection):
    c = connect.cursor()
    execute_and_fetch_all(c, "CREATE (:L1), (:L2)")
    connect.commit()

    has_error = False

    barrier = Barrier(2)

    # TODO maybe make these global modifier functions, as they will be reused
    # throughout the tests
    def add_edge_with_barrier(edge_label, precommit_duration):
        nonlocal has_error
        try:
            connection = make_connection()
            cursor = connection.cursor()
            cursor.execute("MATCH (m:L1), (n:L2) CREATE (m)-[:$edge_label]->(n)", {"edge_label": edge_label})
            barrier.wait()
            if precommit_duration:
                time.sleep(precommit_duration)
            connection.commit()
        except Exception as e:
            print(str(e))
            has_error = True

    def add_edge(edge_label):
        nonlocal has_error
        try:
            connection = make_connection()
            cursor = connection.cursor()
            cursor.execute("MATCH (m:L1), (n:L2) CREATE (m)-[:$edge_label]->(n)", {"edge_label": edge_label})
            connection.commit()
        except Exception as e:
            print(str(e))
            has_error = True

    t1 = Thread(target=add_edge_with_barrier, args=("ALFA", 3))
    t2 = Thread(target=add_edge, args=("BRAVO",))
    t1.start()
    barrier.wait()
    t2.start()
    t1.join()
    t2.join()

    assert not has_error


def test_wait_on_updating_props(connect, make_connection):
    c = connect.cursor()
    execute_and_fetch_all(c, "CREATE (:L1)")
    connect.commit()

    has_error = False

    barrier = Barrier(2)

    def set_prop_with_barrier(prop_value, precommit_duration):
        nonlocal has_error
        try:
            connection = make_connection()
            cursor = connection.cursor()
            cursor.execute("MATCH (m:L1) SET m.prop_value = $prop_value", {"prop_value": prop_value})
            barrier.wait()
            if precommit_duration:
                time.sleep(precommit_duration)
            connection.commit()
        except Exception as e:
            print(str(e))
            has_error = True

    def set_prop(prop_value):
        nonlocal has_error
        try:
            connection = make_connection()
            cursor = connection.cursor()
            cursor.execute("MATCH (m:L1) SET m.prop_value = $prop_value", {"prop_value": prop_value})
            connection.commit()
        except Exception as e:
            print(str(e))
            has_error = True

    t1 = Thread(target=set_prop_with_barrier, args=("ALFA", 3))
    t2 = Thread(target=set_prop, args=("BRAVO",))
    t1.start()
    barrier.wait()
    t2.start()
    t1.join()
    t2.join()

    assert not has_error


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
