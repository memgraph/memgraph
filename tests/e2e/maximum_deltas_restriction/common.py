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

MAXIMUM_DELTAS_RESTRICTION_PLACEHOLDER = "SET DATABASE SETTING 'maximum-deltas-per-transaction' TO '$0';"
MAXIMUM_DELETE_DELTAS_RESTRICTION_PLACEHOLDER = "SET DATABASE SETTING 'maximum-delete-deltas-per-transaction' TO '$0';"


@pytest.fixture
def memgraph(**kwargs) -> Memgraph:
    memgraph = Memgraph()

    yield memgraph

    memgraph.execute(MAXIMUM_DELTAS_RESTRICTION_PLACEHOLDER.replace("$0", "-1"))
    memgraph.execute(MAXIMUM_DELETE_DELTAS_RESTRICTION_PLACEHOLDER.replace("$0", "-1"))
    memgraph.drop_database()
