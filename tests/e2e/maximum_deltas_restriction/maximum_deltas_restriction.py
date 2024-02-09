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

import pytest
from common import (
    MAXIMUM_DELETE_DELTAS_RESTRICTION_PLACEHOLDER,
    MAXIMUM_DELTAS_RESTRICTION_PLACEHOLDER,
    memgraph,
)
from gqlalchemy import GQLAlchemyError

CREATE_EMPTY_NODES_PLACEHOLDER = "FOREACH (i in range(1, $0) | CREATE ());"
CREATE_FULL_NODES_PLACEHOLDER = "FOREACH (i in range(1, $0) | CREATE (:Node {id: i}));"
DELETE_EVERYTHING_QUERY = "MATCH (n) DETACH DELETE n;"


def test_given_no_restrictions_on_the_database_when_executing_commands_then_everything_should_pass(memgraph):
    memgraph.execute(CREATE_EMPTY_NODES_PLACEHOLDER.replace("$0", "100"))
    memgraph.execute(CREATE_FULL_NODES_PLACEHOLDER.replace("$0", "100"))
    memgraph.execute(DELETE_EVERYTHING_QUERY)


def test_given_maximum_restriction_ingestion_fails_when_inserting_more_nodes(memgraph):
    memgraph.execute(MAXIMUM_DELTAS_RESTRICTION_PLACEHOLDER.replace("$0", "100"))

    memgraph.execute(CREATE_EMPTY_NODES_PLACEHOLDER.replace("$0", "100"))
    memgraph.execute(DELETE_EVERYTHING_QUERY)

    with pytest.raises(GQLAlchemyError):
        memgraph.execute(CREATE_EMPTY_NODES_PLACEHOLDER.replace("$0", "101"))


def test_given_maximum_delete_restriction_deletion_fails_when_deleting_more_nodes(memgraph):
    memgraph.execute(MAXIMUM_DELETE_DELTAS_RESTRICTION_PLACEHOLDER.replace("$0", "100"))

    memgraph.execute(CREATE_EMPTY_NODES_PLACEHOLDER.replace("$0", "2000"))

    with pytest.raises(GQLAlchemyError):
        memgraph.execute(DELETE_EVERYTHING_QUERY)


def test_given_maximum_delete_restriction_deletion_fails_when_deleting_more_edges(memgraph):
    memgraph.execute(MAXIMUM_DELETE_DELTAS_RESTRICTION_PLACEHOLDER.replace("$0", "100"))

    memgraph.execute(CREATE_EMPTY_NODES_PLACEHOLDER.replace("$0", "2000"))
    memgraph.execute("CREATE (s:Supernode)")
    memgraph.execute("MATCH (s:Supernode) MATCH (n) CREATE (s)-[:HAS]->(n)")

    with pytest.raises(GQLAlchemyError):
        memgraph.execute(DELETE_EVERYTHING_QUERY)


def test_given_maximum_delta_restriction_fails_when_deleting_everything_on_batch_ingested_nodes(memgraph):
    memgraph.execute(MAXIMUM_DELTAS_RESTRICTION_PLACEHOLDER.replace("$0", "100"))

    memgraph.execute(CREATE_EMPTY_NODES_PLACEHOLDER.replace("$0", "90"))
    memgraph.execute(CREATE_EMPTY_NODES_PLACEHOLDER.replace("$0", "90"))

    with pytest.raises(GQLAlchemyError):
        memgraph.execute(DELETE_EVERYTHING_QUERY)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
