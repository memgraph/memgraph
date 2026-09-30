import sys

import mgclient
import pytest
from common import memgraph
from gqlalchemy import Memgraph


def get_explain(memgraph, query):
    return [row["QUERY PLAN"] for row in memgraph.execute_and_fetch(f"EXPLAIN {query}")]


def test_create_label_index_plan_invalidation(memgraph):
    label = "label"

    # populate the plan cache
    memgraph.execute(f"MATCH (n:{label}) RETURN n")

    memgraph.execute(f"CREATE INDEX ON :{label}")
    memgraph.execute(f"CREATE (n:{label})")

    results = get_explain(memgraph, f"MATCH (n:{label}) RETURN n")
    assert results == [" * Produce {n}", " * ScanAllByLabel (n :label)", " * Once"]


def test_create_label_property_index_plan_invalidation(memgraph):
    label = "label"
    prop = "prop"

    # populate the plan cache
    memgraph.execute(f"MATCH (n:{label}) WHERE n.{prop} IS NOT NULL RETURN n")

    memgraph.execute(f"CREATE INDEX ON :{label}({prop})")
    memgraph.execute(f"CREATE (n:{label} {{{prop}: 'value'}})")

    results = get_explain(memgraph, f"MATCH (n:{label}) WHERE n.{prop} IS NOT NULL RETURN n")
    assert results == [" * Produce {n}", f" * ScanAllByLabelProperties (n :{label} {{{prop}}})", " * Once"]


def test_create_edge_type_index_plan_invalidation(memgraph):
    edge_type = "TYPE"

    # populate the plan cache
    memgraph.execute(f"MATCH (n)-[r:{edge_type}]->(m) RETURN r")

    memgraph.execute(f"CREATE EDGE INDEX ON :{edge_type}")
    memgraph.execute(f"CREATE (n)-[:{edge_type}]->(m)")

    results = get_explain(memgraph, f"MATCH (n)-[r:{edge_type}]->(m) RETURN r")
    assert results == [" * Produce {r}", f" * ScanAllByEdgeType (n)-[r:{edge_type}]->(m)", " * Once"]


def test_create_edge_type_property_index_plan_invalidation(memgraph):
    edge_type = "TYPE"
    prop = "prop"

    # populate the plan cache
    memgraph.execute(f"MATCH (n)-[r:{edge_type}]->(m) WHERE r.{prop} IS NOT NULL RETURN r")

    memgraph.execute(f"CREATE EDGE INDEX ON :{edge_type}({prop})")
    memgraph.execute(f"CREATE (n)-[:{edge_type} {{{prop}: 'value'}}]->(m)")

    results = get_explain(memgraph, f"MATCH (n)-[r:{edge_type}]->(m) WHERE r.{prop} IS NOT NULL RETURN r")
    assert results == [" * Produce {r}", f" * ScanAllByEdgeTypeProperty (n)-[r:{edge_type} {{{prop}}}]->(m)", " * Once"]


def test_create_global_edge_property_index_plan_invalidation(memgraph):
    prop = "prop"

    # populate the plan cache
    memgraph.execute(f"MATCH (n)-[r]->(m) WHERE r.{prop} IS NOT NULL RETURN r")

    memgraph.execute(f"CREATE GLOBAL EDGE INDEX ON :({prop})")
    memgraph.execute(f"CREATE (n)-[:type {{{prop}: 'value'}}]->(m)")

    results = get_explain(memgraph, f"MATCH (n)-[r]->(m) WHERE r.{prop} IS NOT NULL RETURN r")
    assert results == [" * Produce {r}", f" * ScanAllByEdgeProperty (n)-[r {{{prop}}}]->(m)", " * Once"]

if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
