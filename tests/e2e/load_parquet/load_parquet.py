# Copyright 2025 Memgraph Ltd.
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
from common import connect, execute_and_fetch_all, get_file_path


def is_hexadecimal(value):
    try:
        int(value, 16)
        return True
    except ValueError:
        print(f"{value!r} is not a valid hexadecimal string")
        return False


def test_small_file_nodes():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD PARQUET FROM '{get_file_path('nodes_100.parquet')}' AS row CREATE (n:N {{id: row.id, name: row.name, age: row.age, city: row.city}})"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_small_file_edges():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD PARQUET FROM '{get_file_path('nodes_100.parquet')}' AS row CREATE (n:Person {{id: row.id, name: row.name, age: row.age, city: row.city}})"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    load_query = f"LOAD PARQUET FROM '{get_file_path('edges_100.parquet')}' AS row MATCH (a:Person {{id: row.START_ID}}) MATCH (b:Person {{id: row.END_ID}}) CREATE (a)-[r:KNOWS {{type: row.TYPE, since: row.since, strength: row.strength}}]->(b);"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n)-[e]->(m) return count (e)")[0][0] == 100
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_null_nodes():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD PARQUET FROM '{get_file_path('nodes_with_nulls.parquet')}' AS row CREATE (n:N {{id: row.id, name: row.name, age: row.age, city: row.city}});"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 50
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_null_edges():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD PARQUET FROM '{get_file_path('nodes_with_nulls.parquet')}' AS row CREATE (n:Person {{id: row.id, name: row.name, age: row.age, city: row.city}});"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 50
    load_query = f"LOAD PARQUET FROM '{get_file_path('edges_with_nulls.parquet')}' AS row MATCH (a:Person {{id: row.START_ID}}) MATCH (b:Person {{id: row.END_ID}}) CREATE (a)-[r:KNOWS {{type: row.TYPE, since: row.since, strength: row.strength}}]->(b);"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n)-[e]->(m) return count (e)")[0][0] == 30
    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_numeric_types():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = (
        f"LOAD PARQUET FROM '{get_file_path('nodes_numeric.parquet')}' AS row CREATE (n:Person {{id: row.id, age_int64: row.age_int64, score_int32: row.score_int32, level_int16: row.level_int16,"
        f"rank_int8: row.rank_int8, points_uint64: row.points_uint64, coins_uint32: row.coins_uint32, badges_uint16: row.badges_uint16, lives_uint8: row.lives_uint8, weight_double: row.weight_double, height_float: row.height_float,"
        f"ratio_half: row.ratio_half}});"
    )
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.id)")[0][0] == "INTEGER"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.age_int64)")[0][0] == "INTEGER"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.score_int32)")[0][0] == "INTEGER"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.level_int16)")[0][0] == "INTEGER"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.rank_int8)")[0][0] == "INTEGER"
    assert (
        execute_and_fetch_all(cursor, "match (n) return valueType(n.points_uint64)")[0][0] == "INTEGER"
    )  # With static cast in the code
    assert (
        execute_and_fetch_all(cursor, "match (n) return valueType(n.coins_uint32)")[0][0] == "INTEGER"
    )  # With static cast in the code
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.badges_uint16)")[0][0] == "INTEGER"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.lives_uint8)")[0][0] == "INTEGER"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.weight_double)")[0][0] == "FLOAT"
    assert (
        execute_and_fetch_all(cursor, "match (n) return valueType(n.height_float)")[0][0] == "FLOAT"
    )  # MG doesn't support floats
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.ratio_half)")[0][0] == "FLOAT"

    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_temporal_types():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = (
        f"LOAD PARQUET FROM '{get_file_path('nodes_temporal.parquet')}' AS row CREATE (n:N {{id: row.id, name: row.name, registration_date64: row.registration_date64, "
        f"birth_date32: row.birth_date32, exact_time32_ms: row.exact_time32_ms, precise_time64_us: row.precise_time64_us, ultra_precise_time64_ns: row.ultra_precise_time64_ns, "
        f"last_login_timestamp: row.last_login_timestamp, session_duration_ms: row.session_duration_ms}});"
    )
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.birth_date32)")[0][0] == "DATE"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.registration_date64)")[0][0] == "DATE"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.exact_time32_ms)")[0][0] == "LOCAL_TIME"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.precise_time64_us)")[0][0] == "LOCAL_TIME"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.ultra_precise_time64_ns)")[0][0] == "LOCAL_TIME"
    assert (
        execute_and_fetch_all(cursor, "match (n) return valueType(n.last_login_timestamp)")[0][0] == "LOCAL_DATE_TIME"
    )
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.session_duration_ms)")[0][0] == "DURATION"
    execute_and_fetch_all(cursor, "match (n) detach delete n")


# All binary types are converted to hex string
def test_binary_types():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = (
        f"LOAD PARQUET FROM '{get_file_path('nodes_binary.parquet')}' AS x CREATE (p:Person {{id: x.id, name: x.name, uuid: x.uuid_bytes, "
        f"sha256: x.sha256_hash, ipv4: x.ipv4_address, file_size: size(x.file_content)}});"
    )
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100

    assert is_hexadecimal(execute_and_fetch_all(cursor, "match (n) return n.uuid")[0][0]) == True
    assert is_hexadecimal(execute_and_fetch_all(cursor, "match (n) return n.sha256")[0][0]) == True
    assert is_hexadecimal(execute_and_fetch_all(cursor, "match (n) return n.ipv4")[0][0]) == True
    execute_and_fetch_all(cursor, "match (n) detach delete n")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
