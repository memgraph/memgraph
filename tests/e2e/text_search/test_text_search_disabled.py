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
import sys

import gqlalchemy
import pytest
from common import memgraph

TEXT_SEARCH_DISABLED_ERROR = (
    "To use text indices and text search, start Memgraph with the experimental text search feature enabled."
)


def test_create_index(memgraph):
    with pytest.raises(gqlalchemy.exceptions.GQLAlchemyDatabaseError, match=TEXT_SEARCH_DISABLED_ERROR) as _:
        memgraph.execute("""CREATE TEXT INDEX exampleIndex ON :Document;""")


def test_drop_index(memgraph):
    with pytest.raises(gqlalchemy.exceptions.GQLAlchemyDatabaseError, match=TEXT_SEARCH_DISABLED_ERROR) as _:
        memgraph.execute("""DROP TEXT INDEX exampleIndex;""")


def test_text_search_given_property(memgraph):
    with pytest.raises(gqlalchemy.exceptions.GQLAlchemyDatabaseError, match=TEXT_SEARCH_DISABLED_ERROR) as _:
        memgraph.execute(
            """CALL text_search.search("complianceDocuments", "data.title:Rules2024") YIELD node
             RETURN node;"""
        )


def test_text_search_all_properties(memgraph):
    with pytest.raises(gqlalchemy.exceptions.GQLAlchemyDatabaseError, match=TEXT_SEARCH_DISABLED_ERROR) as _:
        memgraph.execute(
            """CALL text_search.search_all("complianceDocuments", "Rules2024") YIELD node
             RETURN node;"""
        )


def test_regex_text_search(memgraph):
    with pytest.raises(gqlalchemy.exceptions.GQLAlchemyDatabaseError, match=TEXT_SEARCH_DISABLED_ERROR) as _:
        memgraph.execute(
            """CALL text_search.regex_search("complianceDocuments", "wor.*s") YIELD node
             RETURN node;"""
        )


def test_text_search_aggregate(memgraph):
    with pytest.raises(gqlalchemy.exceptions.GQLAlchemyDatabaseError, match=TEXT_SEARCH_DISABLED_ERROR) as _:
        input_aggregation = json.dumps({"count": {"value_count": {"field": "metadata.gid"}}}, separators=(",", ":"))

        memgraph.execute(
            f"""CALL text_search.aggregate("complianceDocuments", "wor.*s", '{input_aggregation}') YIELD aggregation
            RETURN aggregation;"""
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
