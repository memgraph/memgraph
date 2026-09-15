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

# isort: off
import sys
import pytest

from common import execute_and_fetch_all, has_n_result_row, has_one_result_row
from conftest import get_connection
from mgclient import DatabaseError


@pytest.mark.parametrize(
    "is_write",
    [
        True,
        False,
    ],
)
def test_graph_mutability(is_write: bool, connection):
    cursor = connection.cursor()

    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    module = "write" if is_write else "read"

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL batch_py_{module}.graph_is_mutable() " "YIELD mutable, init_called RETURN mutable, init_called",
        )
    )
    assert result == [(is_write, True)]

    execute_and_fetch_all(cursor, "CREATE ()")
    result = list(
        execute_and_fetch_all(
            cursor,
            "MATCH (n) "
            f"CALL batch_py_{module}.underlying_graph_is_mutable(n) "
            "YIELD mutable, init_called RETURN mutable, init_called",
        )
    )
    assert result == [(is_write, True)]

    execute_and_fetch_all(cursor, "CREATE ()-[:TYPE]->()")
    result = list(
        execute_and_fetch_all(
            cursor,
            "MATCH (n)-[e]->(m) "
            f"CALL batch_py_{module}.underlying_graph_is_mutable(e) "
            "YIELD mutable, init_called RETURN mutable, init_called",
        )
    )
    assert result == [(is_write, True)]


def test_batching_nums(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL batch_py_read.batch_nums() " "YIELD num, init_called, is_valid RETURN num, init_called, is_valid",
        )
    )
    assert result == [(i, True, True) for i in range(1, 11)]

    execute_and_fetch_all(cursor, "CREATE () CREATE ()")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 2)
    result = list(
        execute_and_fetch_all(
            cursor,
            "MATCH (n) "
            "CALL batch_py_read.batch_nums() "
            "YIELD num, init_called, is_valid RETURN num, init_called, is_valid ",
        )
    )
    assert result == [(i, True, True) for i in range(1, 11)] * 2


def test_batching_vertices(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    execute_and_fetch_all(cursor, f"CREATE () CREATE ()")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 2)

    with pytest.raises(DatabaseError):
        result = list(
            execute_and_fetch_all(
                cursor, f"CALL batch_py_read.batch_vertices() " "YIELD vertex, init_called RETURN vertex, init_called"
            )
        )


def test_batching_nums_c(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    num_ints = 10
    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL batch_c_read.batch_nums({num_ints}) " "YIELD output RETURN output",
        )
    )
    result_list = [item[0] for item in result]
    print(result_list)
    print([i for i in range(1, num_ints + 1)])
    assert result_list == [i for i in range(1, num_ints + 1)]


def test_batching_strings_c(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, f"MATCH (n) DETACH DELETE n")
    assert has_n_result_row(cursor, "MATCH (n) RETURN n", 0)

    num_strings = 10
    result = list(
        execute_and_fetch_all(
            cursor,
            f"CALL batch_c_read.batch_strings({num_strings}) " "YIELD output RETURN output",
        )
    )
    assert len(result) == num_strings


def test_batching_in_subquery(connection):
    # A subquery restarts its branch for every input row, so the batched stream has to be torn down
    # and re-initialized per row instead of staying where the previous row left it.
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")

    result = list(
        execute_and_fetch_all(
            cursor,
            "UNWIND [1, 2] AS x " "CALL (x) { CALL batch_py_read.batch_nums() YIELD num RETURN num } " "RETURN x, num",
        )
    )
    assert result == [(x, i) for x in (1, 2) for i in range(1, 11)]

    # LIMIT leaves the stream mid-batch, so the restart interrupts a live stream. `num` alone cannot see
    # that -- a cursor that wrongly resumes replays the same numbers -- so check `init_called`.
    result = list(
        execute_and_fetch_all(
            cursor,
            "UNWIND [1, 2] AS x "
            "CALL (x) { CALL batch_py_read.batch_nums() YIELD num, init_called RETURN num, init_called LIMIT 2 } "
            "RETURN x, num, init_called",
        )
    )
    assert result == [(x, i, True) for x in (1, 2) for i in range(1, 3)]


def test_batching_in_subquery_c(connection):
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")

    num_ints = 3
    result = list(
        execute_and_fetch_all(
            cursor,
            "UNWIND [1, 2] AS x "
            f"CALL (x) {{ CALL batch_c_read.batch_nums({num_ints}) YIELD output RETURN output }} "
            "RETURN x, output",
        )
    )
    assert result == [(x, i) for x in (1, 2) for i in range(1, num_ints + 1)]


def test_shutdown_reaches_below_a_procedure(connection):
    # The probe is the lower of two procedure calls and LIMIT stops the query mid-stream, so its
    # cleanup can only come from the teardown the procedure above it starts.
    cursor = connection.cursor()

    execute_and_fetch_all(
        cursor,
        "CALL batch_py_read.teardown_probe_rows() YIELD num "
        "CALL mg.procedures() YIELD name "
        "RETURN num, name LIMIT 1",
    )

    cleanup_ran = execute_and_fetch_all(
        cursor, "CALL batch_py_read.teardown_probe_cleanup_ran() YIELD cleanup_ran RETURN cleanup_ran"
    )[0][0]
    assert cleanup_ran


def test_one_cleanup_per_initializer(connection):
    # Every stream the procedure starts is torn down exactly once: by the next pull's cleanup, by the
    # shutdown, or by the cursor's destructor. A reset must not tear it down a second time.
    cursor = connection.cursor()
    execute_and_fetch_all(cursor, "CALL batch_py_read.teardown_probe_reset_counts() YIELD ok RETURN ok")

    execute_and_fetch_all(
        cursor,
        "UNWIND [1, 2] AS x "
        "CALL (x) { CALL batch_py_read.teardown_probe_rows() YIELD num RETURN num LIMIT 2 } "
        "RETURN x, num",
    )

    inits, cleanups = execute_and_fetch_all(
        cursor, "CALL batch_py_read.teardown_probe_counts() YIELD inits, cleanups RETURN inits, cleanups"
    )[0]
    assert inits == 2
    assert cleanups == inits


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
