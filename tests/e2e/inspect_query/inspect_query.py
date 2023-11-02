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

import mgclient
import pytest
from common import memgraph


def test_label_index_hint(memgraph):
    explain_query = "EXPLAIN MATCH (n:Node) RETURN n.property1;"
    profile_query = "PROFILE MATCH (n:Node) RETURN n.property1;"

    expected_profile = ["* Produce {n.property1}", "* Filter (n :Node)", "* ScanAll (n)", "* Once"]
    expected_explain = [" * Produce {n.property1}", " * Filter (n :Node)", " * ScanAll (n)", " * Once"]

    explain_empty = [row["QUERY PLAN"] for row in memgraph.execute_and_fetch(explain_query)]
    profile_empty = [row["OPERATOR"] for row in memgraph.execute_and_fetch(profile_query)]

    assert explain_empty == expected_explain and profile_empty == expected_profile

    memgraph.execute("CREATE (n:WrongLabel {property1: 2});")

    explain_none_matched = [row["QUERY PLAN"] for row in memgraph.execute_and_fetch(explain_query)]
    profile_none_matched = [row["OPERATOR"] for row in memgraph.execute_and_fetch(profile_query)]

    assert explain_none_matched == expected_explain and profile_none_matched == expected_profile

    memgraph.execute("CREATE (n:Node {property1: 2});")

    explain_has_match = [row["QUERY PLAN"] for row in memgraph.execute_and_fetch(explain_query)]
    profile_has_match = [row["OPERATOR"] for row in memgraph.execute_and_fetch(profile_query)]

    assert explain_has_match == expected_explain and profile_has_match == expected_profile


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
