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

import re
import sys
import urllib.request

import mgclient
import pytest
from common import connect, execute_and_fetch_all

ABORTED_QUERIES = re.compile(r"^memgraph_aborted_queries_total\{(.*)\} (\S+)$", re.MULTILINE)
LABEL = re.compile(r'(\w+)="([^"]*)"')


def aborted_queries(reason, database="memgraph"):
    with urllib.request.urlopen("http://localhost:9091/metrics") as response:
        body = response.read().decode("utf-8")
    for labels, value in ABORTED_QUERIES.findall(body):
        found = dict(LABEL.findall(labels))
        if found.get("reason") == reason and found.get("database") == database:
            return float(value)
    return None


def test_memory_limit_abort_is_counted(connect):
    cursor = connect.cursor()
    before = aborted_queries("memory_limit")
    assert before is not None

    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(
            cursor, "UNWIND range(1, 1000000) AS x WITH collect(x) AS xs RETURN size(xs) QUERY MEMORY LIMIT 1KB;"
        )

    assert aborted_queries("memory_limit") == before + 1


def test_timeout_abort_is_counted(connect):
    cursor = connect.cursor()
    before = aborted_queries("timeout")
    assert before is not None

    with pytest.raises(mgclient.DatabaseError):
        execute_and_fetch_all(cursor, "UNWIND range(1, 100000) AS x UNWIND range(1, 100000) AS y RETURN count(*);")

    assert aborted_queries("timeout") == before + 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
