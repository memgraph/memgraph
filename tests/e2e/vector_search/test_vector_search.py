# Copyright 2024 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import json
import re
import sys

import gqlalchemy
import mgclient
import pytest
from common import memgraph, memgraph_with_vector_indexed_data

# TODO(gitbuda): The basic create/drop index are redundant here because that's
# also tested under gql_behave -> remove it from here.


def test_create_index(memgraph):
    memgraph.execute("""CREATE VECTOR INDEX vectorIndexName ON :Label(property) OPTIONS {};""")
    index_info = memgraph.execute_and_fetch("""SHOW INDEX INFO""")
    assert list(index_info) == []


def test_drop_index(memgraph):
    memgraph.execute("""DROP VECTOR INDEX vectorIndexName;""")
    index_info = memgraph.execute_and_fetch("""SHOW INDEX INFO""")
    assert list(index_info) == []


def test_create_existing_index(memgraph):
    memgraph.execute("""CREATE VECTOR INDEX duplicatedIndex ON :Label(property) OPTIONS {};""")
    with pytest.raises(
        gqlalchemy.exceptions.GQLAlchemyDatabaseError, match='Vector index "duplicatedIndex" already exists.'
    ) as _:
        memgraph.execute("""CREATE VECTOR INDEX duplicatedIndex ON :Label(property) OPTIONS {};""")
    memgraph.execute("""DROP VECTOR INDEX duplicatedIndex;""")  # cleanup


def test_drop_nonexistent_index(memgraph):
    with pytest.raises(
        gqlalchemy.exceptions.GQLAlchemyDatabaseError, match='Vector index "noSuchIndex" doesn’t exist.'
    ) as _:
        memgraph.execute("""DROP VECTOR INDEX noSuchIndex;""")


def test_vector_search(memgraph_with_vector_indexed_data):
    QUERY = """
        CALL vector_search.query_nodes('vectorIndexName', 10, [1.0, 2.0, 3.0, ...])
        YIELD node, score
        RETURN node, score;
    """
    result = list(memgraph_with_vector_indexed_data.execute_and_fetch(REGEX_QUERY))
    assert len(result) == 10


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
