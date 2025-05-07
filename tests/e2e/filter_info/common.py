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
    memgraph.execute("DROP INDEX ON :l(p)")

    memgraph.drop_database()
    memgraph.drop_indexes()
