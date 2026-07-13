# Copyright 2026 Memgraph Ltd.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
# License, and you may not use this file except in compliance with the Business Source License.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0, included in the file
# licenses/APL.txt.

import re
import sys
import urllib.request

import mgclient
import pytest

# OpenMetrics: expected per-database family base names (without memgraph_ prefix).
# Histograms appear as base_name_bucket, base_name_count, base_name_sum — we strip suffixes.
EXPECTED_OPENMETRICS_PER_DB_FAMILIES = {
    "vertex_count",
    "edge_count",
    "disk_usage_bytes",
    "db_memory_tracked_bytes",
    "db_peak_memory_tracked_bytes",
    "db_storage_memory_tracked_bytes",
    "db_embedding_memory_tracked_bytes",
    "db_query_memory_tracked_bytes",
    # Operators
    "once_operator_total",
    "create_node_operator_total",
    "create_expand_operator_total",
    "scan_all_operator_total",
    "scan_all_by_label_operator_total",
    "scan_all_by_label_properties_operator_total",
    "scan_all_by_id_operator_total",
    "scan_all_by_edge_operator_total",
    "scan_all_by_edge_type_operator_total",
    "scan_all_by_edge_type_property_operator_total",
    "scan_all_by_edge_type_property_value_operator_total",
    "scan_all_by_edge_type_property_range_operator_total",
    "scan_all_by_edge_property_operator_total",
    "scan_all_by_edge_property_value_operator_total",
    "scan_all_by_edge_property_range_operator_total",
    "scan_all_by_vertex_property_operator_total",
    "scan_all_by_vertex_property_value_operator_total",
    "scan_all_by_vertex_property_range_operator_total",
    "scan_all_by_edge_id_operator_total",
    "scan_all_by_point_distance_operator_total",
    "scan_all_by_point_withinbbox_operator_total",
    "expand_operator_total",
    "expand_variable_operator_total",
    "construct_named_path_operator_total",
    "filter_operator_total",
    "produce_operator_total",
    "delete_operator_total",
    "set_property_operator_total",
    "set_properties_operator_total",
    "set_labels_operator_total",
    "remove_property_operator_total",
    "remove_labels_operator_total",
    "edge_uniqueness_filter_operator_total",
    "empty_result_operator_total",
    "accumulate_operator_total",
    "aggregate_operator_total",
    "skip_operator_total",
    "limit_operator_total",
    "order_by_operator_total",
    "merge_operator_total",
    "optional_operator_total",
    "unwind_operator_total",
    "distinct_operator_total",
    "union_operator_total",
    "cartesian_operator_total",
    "call_procedure_operator_total",
    "foreach_operator_total",
    "evaluate_pattern_filter_operator_total",
    "apply_operator_total",
    "indexed_join_operator_total",
    "hash_join_operator_total",
    "roll_up_apply_operator_total",
    "periodic_commit_operator_total",
    "periodic_subquery_operator_total",
    "set_nested_property_operator_total",
    "remove_nested_property_operator_total",
    # Index
    "active_label_indices",
    "active_label_property_indices",
    "active_edge_type_indices",
    "active_edge_type_property_indices",
    "active_edge_property_indices",
    "active_point_indices",
    "active_text_indices",
    "active_text_edge_indices",
    "active_vector_indices",
    "active_vector_edge_indices",
    "active_vertex_property_indices",
    # Constraint
    "active_existence_constraints",
    "active_unique_constraints",
    "active_type_constraints",
    # Stream
    "streams_created_total",
    "messages_consumed_total",
    # Trigger
    "triggers_created_total",
    "triggers_executed_total",
    # Transaction
    "active_transactions",
    "committed_transactions_total",
    "rolled_back_transactions_total",
    "failed_queries_total",
    "failed_prepares_total",
    "failed_pulls_total",
    "successful_queries_total",
    "write_write_conflicts_total",
    "transient_errors_total",
    "unreleased_delta_objects",
    # QueryType
    "read_queries_total",
    "write_queries_total",
    "read_write_queries_total",
    # TTL
    "deleted_nodes_total",
    "deleted_edges_total",
    # SchemaInfo
    "show_schema_total",
    # StorageInfo
    "show_storage_info_total",
    # Histograms (base names — _bucket/_count/_sum are stripped)
    "query_execution_latency_seconds",
    "snapshot_creation_latency_seconds",
    "snapshot_recovery_latency_seconds",
    "gc_latency_seconds",
    "gc_skiplist_cleanup_latency_seconds",
}


# -- Helpers -----------------------------------------------------------------


def execute(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


@pytest.fixture(scope="module")
def populated_databases():
    """Create two databases with known state for metric value verification."""
    conn = mgclient.connect(host="localhost", port=7687)
    conn.autocommit = True
    cursor = conn.cursor()

    # Clean up from any previous run
    execute(cursor, "USE DATABASE memgraph")
    try:
        execute(cursor, "DROP DATABASE db2")
    except Exception:
        pass
    execute(cursor, "MATCH (n) DETACH DELETE n")

    # Set up memgraph (default) database:
    #   2 label indices, 1 label-property index, 1 existence constraint
    #   3 vertices, 1 edge, 1 read query
    execute(cursor, "CREATE INDEX ON :Person;")
    execute(cursor, "CREATE INDEX ON :Company;")
    execute(cursor, "CREATE INDEX ON :Person(name);")
    execute(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute(cursor, "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Company {name: 'Acme'})")
    execute(cursor, "MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (a)-[:WORKS_AT]->(c)")
    execute(cursor, "MATCH (n) RETURN n")

    # Set up db2:
    #   1 label index, 0 label-property indices, 0 constraints
    #   2 vertices, 0 edges
    execute(cursor, "CREATE DATABASE db2")
    execute(cursor, "USE DATABASE db2")
    execute(cursor, "CREATE INDEX ON :Item;")
    execute(cursor, "CREATE (:Item {name: 'X'}), (:Item {name: 'Y'})")

    yield

    # Teardown
    execute(cursor, "USE DATABASE memgraph")
    try:
        execute(cursor, "DROP DATABASE db2")
    except Exception:
        pass
    execute(cursor, "DROP INDEX ON :Person;")
    execute(cursor, "DROP INDEX ON :Company;")
    execute(cursor, "DROP INDEX ON :Person(name);")
    execute(cursor, "DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute(cursor, "MATCH (n) DETACH DELETE n")
    conn.close()


# -- OpenMetrics endpoint ----------------------------------------------------


def scrape_openmetrics():
    req = urllib.request.Request(
        "http://localhost:9091/metrics",
        headers={"Accept": "application/openmetrics-text; version=1.0.0; charset=utf-8"},
    )
    with urllib.request.urlopen(req) as resp:
        return resp.read().decode("utf-8")


_METRIC_RE = re.compile(r"^(\w+)\{([^}]*)\}\s+([\d.eE+\-]+)$")
_HISTOGRAM_SUFFIX_RE = re.compile(r"_(bucket|count|sum|created)$")


def parse_openmetrics(body):
    """Parse OpenMetrics text into {(metric_name, db_name): value}."""
    result = {}
    for line in body.splitlines():
        m = _METRIC_RE.match(line)
        if not m:
            continue
        name, labels_str, value = m.group(1), m.group(2), float(m.group(3))
        db = None
        for label in labels_str.split(","):
            label = label.strip()
            if label.startswith("database="):
                db = label.split("=", 1)[1].strip('"')
        result[(name, db)] = value
    return result


def get_per_db_family_names(body):
    """Extract the set of per-database family base names from OpenMetrics text."""
    raw_names = set()
    for line in body.splitlines():
        m = _METRIC_RE.match(line)
        if not m:
            continue
        name, labels_str = m.group(1), m.group(2)
        if "database=" not in labels_str:
            continue
        if not name.startswith("memgraph_"):
            continue
        raw_names.add(name[len("memgraph_") :])
    # Identify histogram base names from _bucket entries, then collapse
    histogram_bases = {n.removesuffix("_bucket") for n in raw_names if n.endswith("_bucket")}
    families = set()
    for n in raw_names:
        base = _HISTOGRAM_SUFFIX_RE.sub("", n)
        if base in histogram_bases:
            families.add(base)
        else:
            families.add(n)
    return families


def om_get(metrics, name, db=None):
    return metrics.get((name, db))


def test_openmetrics_no_unexpected_per_db_families(populated_databases):
    body = scrape_openmetrics()
    actual = get_per_db_family_names(body)
    unexpected = actual - EXPECTED_OPENMETRICS_PER_DB_FAMILIES
    assert not unexpected, f"Unexpected per-db OpenMetrics families: {unexpected}"
    missing = EXPECTED_OPENMETRICS_PER_DB_FAMILIES - actual
    assert not missing, f"Missing per-db OpenMetrics families: {missing}"


def test_openmetrics_per_db_vertex_and_edge_counts(populated_databases):
    m = parse_openmetrics(scrape_openmetrics())
    assert om_get(m, "memgraph_vertex_count", "memgraph") == 3
    assert om_get(m, "memgraph_edge_count", "memgraph") == 1
    assert om_get(m, "memgraph_vertex_count", "db2") == 2
    assert om_get(m, "memgraph_edge_count", "db2") == 0


def test_openmetrics_per_db_index_gauges(populated_databases):
    m = parse_openmetrics(scrape_openmetrics())
    assert om_get(m, "memgraph_active_label_indices", "memgraph") == 2
    assert om_get(m, "memgraph_active_label_indices", "db2") == 1
    assert om_get(m, "memgraph_active_label_property_indices", "memgraph") == 1
    assert om_get(m, "memgraph_active_label_property_indices", "db2") == 0


def test_openmetrics_per_db_constraint_gauges(populated_databases):
    m = parse_openmetrics(scrape_openmetrics())
    assert om_get(m, "memgraph_active_existence_constraints", "memgraph") == 1
    assert om_get(m, "memgraph_active_existence_constraints", "db2") == 0


def test_openmetrics_per_db_transaction_counters(populated_databases):
    m = parse_openmetrics(scrape_openmetrics())
    assert om_get(m, "memgraph_committed_transactions_total", "memgraph") > 0
    assert om_get(m, "memgraph_committed_transactions_total", "db2") > 0


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
