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


def get_results_length(memgraph, query):
    return len(list(memgraph.execute_and_fetch(query)))


@pytest.fixture
def memgraph(**kwargs) -> Memgraph:
    memgraph = Memgraph()

    memgraph.execute("STORAGE MODE IN_MEMORY_TRANSACTIONAL")
    memgraph.drop_indexes()
    memgraph.ensure_constraints([])
    trigger_names = [x["trigger name"] for x in list(memgraph.execute_and_fetch("SHOW TRIGGERS"))]
    for trigger_name in trigger_names:
        memgraph.execute(f"DROP TRIGGER {trigger_name}")

    yield memgraph

    memgraph.execute("STORAGE MODE IN_MEMORY_TRANSACTIONAL")
    memgraph.drop_indexes()
    memgraph.ensure_constraints([])
    trigger_names = [x["trigger name"] for x in list(memgraph.execute_and_fetch("SHOW TRIGGERS"))]
    for trigger_name in trigger_names:
        memgraph.execute(f"DROP TRIGGER {trigger_name}")

    memgraph.drop_database()
