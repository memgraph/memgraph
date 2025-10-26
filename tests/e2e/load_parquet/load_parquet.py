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

import os
import sys

import pytest
from common import connect, execute_and_fetch_all, get_file_path


def test_aws_config_builder():
    cursor = connect(host="localhost", port=7687).cursor()
    # Settings
    aws_region_setting = "aws.region"
    aws_access_setting = "aws.access_key"
    aws_secret_setting = "aws.secret_key"
    # Env vars
    aws_region_env = "AWS_REGION"
    aws_access_env = "AWS_ACCESS_KEY"
    aws_secret_env = "AWS_SECRET_KEY"

    # 1. Test that LOAD PARQUET will execute successfully with only env vars
    # 2. Test that LOAD PARQUET will execute successfully with only run-time settings
    # 3. Test that LOAD PARQUET will execute successfully with only query parameters
    # 4. Test that all 3 configs can be combined. E.g. Set AWS_REGION through query, AWS_ACCESS_KEY through run-time
    # setting and set AWS_SECRET_KEY through env variable


def test_aws_settings():
    cursor = connect(host="localhost", port=7687).cursor()
    aws_region_key = "aws.region"
    aws_access_key = "aws.access_key"
    aws_secret_key = "aws.secret_key"

    settings = dict(execute_and_fetch_all(cursor, "show database settings"))
    assert aws_region_key in settings
    assert aws_access_key in settings
    assert aws_secret_key in settings

    execute_and_fetch_all(cursor, f"set database setting '{aws_region_key}' to 'eu-west-1'")
    execute_and_fetch_all(cursor, f"set database setting '{aws_access_key}' to 'acc_key'")
    execute_and_fetch_all(cursor, f"set database setting '{aws_secret_key}' to 'secret_key'")

    settings = dict(execute_and_fetch_all(cursor, "show database settings"))
    assert settings[aws_region_key] == "eu-west-1"
    assert settings[aws_access_key] == "acc_key"
    assert settings[aws_secret_key] == "secret_key"


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
        f"ratio_half: row.ratio_half, price_decimal32: row.price_decimal32, balance_decimal64: row.balance_decimal64, precision_decimal128: row.precision_decimal128, huge_decimal256: row.huge_decimal256}});"
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
    # Test decimal types - they should be converted to FLOAT in Memgraph
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.price_decimal32)")[0][0] == "FLOAT"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.balance_decimal64)")[0][0] == "FLOAT"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.precision_decimal128)")[0][0] == "FLOAT"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.huge_decimal256)")[0][0] == "FLOAT"

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


def test_collection_types():
    cursor = connect(host="localhost", port=7687).cursor()
    # Test simple collections first
    load_query = (
        f"LOAD PARQUET FROM '{get_file_path('nodes_collections_simple.parquet')}' AS row "
        f"CREATE (n:Collection {{id: row.id, name: row.name, numbers: row.numbers_list, "
        f"tags: row.tags_list, properties: row.properties_map}})"
    )
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100

    # Check that lists are loaded as LIST type
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.numbers)")[0][0] == "LIST"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.tags)")[0][0] == "LIST"

    # Check that maps are loaded as MAP type
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.properties)")[0][0] == "MAP"

    # Verify we can query list elements
    result = execute_and_fetch_all(cursor, "match (n) where n.id = 1 return n.numbers[0]")
    assert result[0][0] is not None  # Should have at least one element

    # Verify we can query map values
    result = execute_and_fetch_all(cursor, "match (n) where n.id = 1 return n.properties.theme")
    assert result[0][0] is not None  # Should have theme property

    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_complex_collection_types():
    cursor = connect(host="localhost", port=7687).cursor()
    # Test complex collections with nested structures
    load_query = (
        f"LOAD PARQUET FROM '{get_file_path('nodes_collections.parquet')}' AS row "
        f"CREATE (n:ComplexCollection {{id: row.id, name: row.name, "
        f"favorite_numbers: row.favorite_numbers, tags: row.tags, scores: row.scores, "
        f"matrix: row.matrix_2d, settings: row.settings, metadata: row.metadata, "
        f"preferences: row.preferences}})"
    )
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 100

    # Check nested list (list of lists)
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.matrix)")[0][0] == "LIST"

    # Check map with list values
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.preferences)")[0][0] == "MAP"

    # Verify we can access nested list elements
    result = execute_and_fetch_all(cursor, "match (n) where n.id = 1 return n.matrix[0][0]")
    assert result[0][0] is not None  # Should have nested element

    # Verify map with string values
    result = execute_and_fetch_all(cursor, "match (n) where n.id = 1 return n.metadata.country")
    assert result[0][0] is not None  # Should have country in metadata

    execute_and_fetch_all(cursor, "match (n) detach delete n")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
