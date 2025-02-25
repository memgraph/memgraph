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
from common import memgraph
from gqlalchemy.exceptions import GQLAlchemyDatabaseError


def test_match_dynamic_variable_node_error(memgraph):
    query = """
    WITH {name: 'TYPE'} as d
    MATCH (n:d.name)
    RETURN n;
    """

    with pytest.raises(GQLAlchemyDatabaseError):
        memgraph.execute(query)


def test_merge_dynamic_variable_node_error(memgraph):
    query = """
    WITH {name: 'TYPE'} as d
    MERGE (n:d.name)
    RETURN n;
    """

    with pytest.raises(GQLAlchemyDatabaseError):
        memgraph.execute(query)


def test_create_dynamic_node(memgraph):
    memgraph.execute("WITH {name: 'TYPE'} as d CREATE (:d.name);")

    result = list(memgraph.execute_and_fetch("MATCH (n) RETURN labels(n)[0] as label"))

    assert len(result) == 1
    assert result[0]["label"] == "TYPE"


def test_create_dynamic_node_param(memgraph):
    memgraph.execute("CREATE (:$name);", {"name": "TYPE"})

    result = list(memgraph.execute_and_fetch("MATCH (n) RETURN labels(n)[0] as label"))

    assert len(result) == 1
    assert result[0]["label"] == "TYPE"
    

def test_create_multiple_dynamic_labels_on_node_param(memgraph):
    memgraph.execute("CREATE (:$name);", {"name": ["Foo", "Bar"]})

    result = list(memgraph.execute_and_fetch("MATCH (n) RETURN labels(n) as labels"))

    assert len(result) == 1
    assert len(result[0]["labels"]) == 2
    assert result[0]["labels"][0] == "Foo"
    assert result[0]["labels"][1] == "Bar"


def test_create_multiple_dynamic_labels_on_node_with_variable(memgraph):
    memgraph.execute("WITH {labels: ['Foo', 'Bar']} as var CREATE (:var.labels);")

    result = list(memgraph.execute_and_fetch("MATCH (n) RETURN labels(n) as labels"))

    assert len(result) == 1
    assert len(result[0]["labels"]) == 2
    assert result[0]["labels"][0] == "Foo"
    assert result[0]["labels"][1] == "Bar"


def test_match_dynamic_variable_relationship_error(memgraph):
    query = """
    WITH {name: 'TYPE'} as d
    MATCH ()-[:d.name]->()
    RETURN n;
    """

    with pytest.raises(GQLAlchemyDatabaseError):
        memgraph.execute(query)


def test_merge_dynamic_variable_relationship_error(memgraph):
    query = """
    WITH {name: 'TYPE'} as d
    MERGE ()-[:d.name]->()
    RETURN n;
    """

    with pytest.raises(GQLAlchemyDatabaseError):
        memgraph.execute(query)


def test_create_dynamic_relationship(memgraph):
    memgraph.execute("WITH {name: 'TYPE'} as d CREATE ()-[:d.name]->()")

    result = list(memgraph.execute_and_fetch("MATCH (n)-[r]->(m) RETURN type(r) as rel_type"))

    assert len(result) == 1
    assert result[0]["rel_type"] == "TYPE"


def test_create_dynamic_relationship_param(memgraph):
    memgraph.execute("CREATE ()-[:$name]->()", {"name": "TYPE"})

    result = list(memgraph.execute_and_fetch("MATCH (n)-[r]->(m) RETURN type(r) as rel_type"))

    assert len(result) == 1
    assert result[0]["rel_type"] == "TYPE"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
