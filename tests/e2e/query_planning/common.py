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

    memgraph.drop_indexes()
    memgraph.ensure_constraints([])
    memgraph.drop_database()
    
@pytest.fixture
def memgraph_optional(**kwargs) -> Memgraph:
    memgraph = Memgraph()

    yield memgraph
    memgraph.execute("DROP EDGE INDEX ON :HAS_KID;")
    memgraph.drop_indexes()
    memgraph.ensure_constraints([])
    memgraph.drop_database()

