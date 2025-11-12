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


# Invalid token is just skipped
def test_invalid_fields():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD JSONL FROM '{get_file_path('invalid.jsonl')}' AS row \
                CREATE (n:N {{id: row.id, name: row.name}})"
    execute_and_fetch_all(cursor, load_query)

    assert execute_and_fetch_all(cursor, "match (n) where n.id = 1 return n.name")[0][0] == None

    execute_and_fetch_all(cursor, "match (n) detach delete n")


# Null is ignored
def test_null_fields():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD JSONL FROM '{get_file_path('null.jsonl')}' AS row \
                CREATE (n:N {{id: row.id, name: row.name}})"
    execute_and_fetch_all(cursor, load_query)

    assert execute_and_fetch_all(cursor, "match (n) where n.id = 1 return n.name")[0][0] == None

    execute_and_fetch_all(cursor, "match (n) detach delete n")


def test_small_file_nodes():
    cursor = connect(host="localhost", port=7687).cursor()
    load_query = f"LOAD JSONL FROM '{get_file_path('test_types.jsonl')}' AS row \
    CREATE (n:N {{id: row.id, name: row.name, age: row.age, score: row.score, \
        active: row.active, address: row.address, balance: row.balance, \
        tags: row.tags, scores: row.scores, numbers: row.numbers, mixed: row.mixed, \
        nested: row.nested, data: row.data, empty: row.empty, matrix: row.matrix, \
        items: row.items, prices: row.prices, quantities: row.quantities, deep: row.deep, \
        values: row.values, floats: row.flaots, strings: row.strings, combo: row.combo, \
        metadata: row.metadata, config: row.config, person: row.person, nested_2: row.nested_2, \
        mixed_2: row.mixed_2, stats: row.stats, empty_2: row.empty_2, location: row.location, \
        product: row.product, complex: row.complex}})"
    execute_and_fetch_all(cursor, load_query)
    assert execute_and_fetch_all(cursor, "match (n) return count(n)")[0][0] == 120
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.id)")[0][0] == "INTEGER"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.name)")[0][0] == "STRING"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.age)")[0][0] == "INTEGER"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.score)")[0][0] == "FLOAT"
    assert execute_and_fetch_all(cursor, "match (n) return valueType(n.active)")[0][0] == "BOOLEAN"
    # Address can be null or string
    address_type = execute_and_fetch_all(cursor, "match (n) return valueType(n.address)")[0][0]
    assert address_type == "STRING" or address_type == "NULL"
    # Big integer is converted to string if larger than int64_t
    big_val_type = execute_and_fetch_all(cursor, "match (n) return valueType(n.id)")[0][0]
    assert big_val_type == "INTEGER" or big_val_type == "STRING"

    assert execute_and_fetch_all(cursor, "match (n:N) where n.id = 101 return valueType(n.tags)")[0][0] == "LIST"
    assert execute_and_fetch_all(cursor, "match (n:N) where n.id = 103 return n.data")[0][0][1] == 200.5
    assert execute_and_fetch_all(cursor, "match (n:N) where n.id = 110 return n.combo")[0][0][0] == ["a", "b"]

    assert execute_and_fetch_all(cursor, "match (n:N) where n.id = 111 return valueType(n.metadata)")[0][0] == "MAP"
    assert execute_and_fetch_all(cursor, "match (n:N) where n.id = 111 return n.metadata")[0][0]["tags"] == [
        "test",
        "object",
    ]
    assert (
        execute_and_fetch_all(cursor, "match (n:N) where n.id = 114 return n.nested_2")[0][0]["level1"]["level2"][
            "level3"
        ]["value"]
        == "deep"
    )

    execute_and_fetch_all(cursor, "match (n) detach delete n")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
