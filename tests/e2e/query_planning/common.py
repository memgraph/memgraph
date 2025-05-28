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

import pytest
from gqlalchemy import Memgraph


@pytest.fixture
def memgraph(**kwargs) -> Memgraph:
    memgraph = Memgraph()

    yield memgraph

    # Dropping the index here works around the fact that GQLAlchemy will
    # fail on `drop_indexes` because it tries to hash the composite
    # index properties. Once GQLAlchemy correctly supports these, this
    # hack can be removed.
    memgraph.execute("DROP INDEX ON :Label1(prop0)")
    memgraph.execute("DROP INDEX ON :Label1(prop1)")
    memgraph.execute("DROP INDEX ON :Node(id)")

    memgraph.drop_indexes()
    memgraph.ensure_constraints([])
    memgraph.drop_database()


@pytest.fixture
def memgraph_optional(**kwargs) -> Memgraph:
    memgraph = Memgraph()

    yield memgraph
    memgraph.execute("DROP EDGE INDEX ON :HAS_KID;")
    memgraph.execute("DROP EDGE INDEX ON :ET1;")
    memgraph.drop_indexes()
    memgraph.ensure_constraints([])
    memgraph.drop_database()


@pytest.fixture
def memgraph_reset_plan_cache(**kwargs) -> Memgraph:
    memgraph = Memgraph()

    yield memgraph
    memgraph.execute("DROP INDEX ON :Node;")
    memgraph.execute("DROP INDEX ON :Node(id);")
    memgraph.execute("DROP INDEX ON :Supernode;")
    memgraph.execute("DROP INDEX ON :Supernode(id);")
    memgraph.drop_indexes()
    memgraph.ensure_constraints([])
    memgraph.drop_database()
