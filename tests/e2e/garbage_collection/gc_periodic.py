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

import re
import sys
import time

import pytest
from common import execute_and_fetch_all


def remove_non_numeric_suffix(text):
    match = re.search(r"\D*$", text)
    if match:
        non_numeric_suffix = match.group(0)
        return text[: -len(non_numeric_suffix)]
    else:
        return text


def get_memory_from_list(list):
    for list_item in list:
        if list_item[0] == "memory_tracked":
            return float(remove_non_numeric_suffix(list_item[1]))
    return None


def get_memory(cursor):
    return get_memory_from_list(execute_and_fetch_all(cursor, "SHOW STORAGE INFO"))


def test_gc_periodic(connection):
    cursor = connection.cursor()

    memory_pre_creation = get_memory(cursor)
    execute_and_fetch_all(cursor, "UNWIND range(1, 100000) AS index CREATE (:Node);")
    memory_after_creation = get_memory(cursor)
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n;")

    for i in range(5):
        time.sleep(2)
        memory_after_gc = get_memory(cursor)

        if memory_after_gc < memory_pre_creation + (memory_after_creation - memory_pre_creation) / 4:
            return

    raise MemoryError("GC didn't clean the memory after 10 seconds")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
