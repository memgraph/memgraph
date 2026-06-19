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

import json
import sys
import urllib.request

import mgclient
import pytest

METRICS_HTTP_TIMEOUT_S = 30.0

# Expected JSON categories and metric names. If metrics or categories are
# added, this list must also be amended because "unexpected" metrics also
# rank as failures.
EXPECTED_JSON_METRICS = {
    "General": {
        "vertex_count",
        "edge_count",
        "average_degree",
        "memory_usage",
        "peak_memory_usage",
        "unreleased_delta_objects",
        "disk_usage",
        "SocketConnect_us_50p",
        "SocketConnect_us_90p",
        "SocketConnect_us_99p",
    },
    "Index": {
        "ActiveLabelIndices",
        "ActiveLabelPropertyIndices",
        "ActiveEdgeTypeIndices",
        "ActiveEdgeTypePropertyIndices",
        "ActiveEdgePropertyIndices",
        "ActivePointIndices",
        "ActiveTextIndices",
        "ActiveTextEdgeIndices",
        "ActiveVectorIndices",
        "ActiveVectorEdgeIndices",
    },
    "Constraint": {
        "ActiveExistenceConstraints",
        "ActiveUniqueConstraints",
        "ActiveTypeConstraints",
    },
    "Operator": {
        "OnceOperator",
        "CreateNodeOperator",
        "CreateExpandOperator",
        "ScanAllOperator",
        "ScanAllByLabelOperator",
        "ScanAllByLabelPropertiesOperator",
        "ScanAllByIdOperator",
        "ScanAllByEdgeOperator",
        "ScanAllByEdgeTypeOperator",
        "ScanAllByEdgeTypePropertyOperator",
        "ScanAllByEdgeTypePropertyValueOperator",
        "ScanAllByEdgeTypePropertyRangeOperator",
        "ScanAllByEdgePropertyOperator",
        "ScanAllByEdgePropertyValueOperator",
        "ScanAllByEdgePropertyRangeOperator",
        "ScanAllByEdgeIdOperator",
        "ScanAllByPointDistanceOperator",
        "ScanAllByPointWithinbboxOperator",
        "ExpandOperator",
        "ExpandVariableOperator",
        "ConstructNamedPathOperator",
        "FilterOperator",
        "ProduceOperator",
        "DeleteOperator",
        "SetPropertyOperator",
        "SetPropertiesOperator",
        "SetLabelsOperator",
        "RemovePropertyOperator",
        "RemoveLabelsOperator",
        "EdgeUniquenessFilterOperator",
        "EmptyResultOperator",
        "AccumulateOperator",
        "AggregateOperator",
        "SkipOperator",
        "LimitOperator",
        "OrderByOperator",
        "MergeOperator",
        "OptionalOperator",
        "UnwindOperator",
        "DistinctOperator",
        "UnionOperator",
        "CartesianOperator",
        "CallProcedureOperator",
        "ForeachOperator",
        "EvaluatePatternFilterOperator",
        "ApplyOperator",
        "IndexedJoinOperator",
        "HashJoinOperator",
        "RollUpApplyOperator",
        "PeriodicCommitOperator",
        "PeriodicSubqueryOperator",
        "SetNestedPropertyOperator",
        "RemoveNestedPropertyOperator",
    },
    "Transaction": {
        "ActiveTransactions",
        "CommitedTransactions",
        "RollbackedTransactions",
        "FailedQuery",
        "FailedPrepare",
        "FailedPull",
        "SuccessfulQuery",
        "WriteWriteConflicts",
        "TransientErrors",
    },
    "QueryType": {
        "ReadQuery",
        "WriteQuery",
        "ReadWriteQuery",
    },
    "Query": {
        "QueryExecutionLatency_us_50p",
        "QueryExecutionLatency_us_90p",
        "QueryExecutionLatency_us_99p",
    },
    "Session": {
        "ActiveBoltSessions",
        "ActiveSSLSessions",
        "ActiveSessions",
        "ActiveTCPSessions",
        "ActiveWebSocketSessions",
        "BoltMessages",
    },
    "Snapshot": {
        "SnapshotCreationLatency_us_50p",
        "SnapshotCreationLatency_us_90p",
        "SnapshotCreationLatency_us_99p",
        "SnapshotRecoveryLatency_us_50p",
        "SnapshotRecoveryLatency_us_90p",
        "SnapshotRecoveryLatency_us_99p",
    },
    "Stream": {
        "StreamsCreated",
        "MessagesConsumed",
    },
    "TTL": {
        "DeletedNodes",
        "DeletedEdges",
    },
    "Trigger": {
        "TriggersCreated",
        "TriggersExecuted",
    },
    "HighAvailability": {
        "BecomeLeaderSuccess",
        "DemoteInstance",
        "DemoteMainToReplicaRpcFail",
        "DemoteMainToReplicaRpcSuccess",
        "EnableWritingOnMainRpcFail",
        "EnableWritingOnMainRpcSuccess",
        "FailedToBecomeLeader",
        "GetDatabaseHistoriesRpcFail",
        "GetDatabaseHistoriesRpcSuccess",
        "NoAliveInstanceFailedFailovers",
        "PromoteToMainRpcFail",
        "PromoteToMainRpcSuccess",
        "RaftFailedFailovers",
        "RegisterReplicaOnMainRpcFail",
        "RegisterReplicaOnMainRpcSuccess",
        "RemoveCoordInstance",
        "ReplicaRecoveryFail",
        "ReplicaRecoverySkip",
        "ReplicaRecoverySuccess",
        "ShowInstance",
        "ShowInstances",
        "StateCheckRpcFail",
        "StateCheckRpcSuccess",
        "SuccessfulFailovers",
        "SwapMainUUIDRpcFail",
        "SwapMainUUIDRpcSuccess",
        "UnregisterReplInstance",
        "UnregisterReplicaRpcFail",
        "UnregisterReplicaRpcSuccess",
        "UpdateDataInstanceConfigRpcFail",
        "UpdateDataInstanceConfigRpcSuccess",
        "ChooseMostUpToDateInstance_us_50p",
        "ChooseMostUpToDateInstance_us_90p",
        "ChooseMostUpToDateInstance_us_99p",
        "CurrentWalRpc_us_50p",
        "CurrentWalRpc_us_90p",
        "CurrentWalRpc_us_99p",
        "DataFailover_us_50p",
        "DataFailover_us_90p",
        "DataFailover_us_99p",
        "DemoteMainToReplicaRpc_us_50p",
        "DemoteMainToReplicaRpc_us_90p",
        "DemoteMainToReplicaRpc_us_99p",
        "EnableWritingOnMainRpc_us_50p",
        "EnableWritingOnMainRpc_us_90p",
        "EnableWritingOnMainRpc_us_99p",
        "FinalizeTxnReplication_us_50p",
        "FinalizeTxnReplication_us_90p",
        "FinalizeTxnReplication_us_99p",
        "FrequentHeartbeatRpc_us_50p",
        "FrequentHeartbeatRpc_us_90p",
        "FrequentHeartbeatRpc_us_99p",
        "GetDatabaseHistoriesRpc_us_50p",
        "GetDatabaseHistoriesRpc_us_90p",
        "GetDatabaseHistoriesRpc_us_99p",
        "GetHistories_us_50p",
        "GetHistories_us_90p",
        "GetHistories_us_99p",
        "HeartbeatRpc_us_50p",
        "HeartbeatRpc_us_90p",
        "HeartbeatRpc_us_99p",
        "InstanceFailCallback_us_50p",
        "InstanceFailCallback_us_90p",
        "InstanceFailCallback_us_99p",
        "InstanceSuccCallback_us_50p",
        "InstanceSuccCallback_us_90p",
        "InstanceSuccCallback_us_99p",
        "PrepareCommitRpc_us_50p",
        "PrepareCommitRpc_us_90p",
        "PrepareCommitRpc_us_99p",
        "PromoteToMainRpc_us_50p",
        "PromoteToMainRpc_us_90p",
        "PromoteToMainRpc_us_99p",
        "RegisterReplicaOnMainRpc_us_50p",
        "RegisterReplicaOnMainRpc_us_90p",
        "RegisterReplicaOnMainRpc_us_99p",
        "ReplicaStream_us_50p",
        "ReplicaStream_us_90p",
        "ReplicaStream_us_99p",
        "SnapshotRpc_us_50p",
        "SnapshotRpc_us_90p",
        "SnapshotRpc_us_99p",
        "StartTxnReplication_us_50p",
        "StartTxnReplication_us_90p",
        "StartTxnReplication_us_99p",
        "StateCheckRpc_us_50p",
        "StateCheckRpc_us_90p",
        "StateCheckRpc_us_99p",
        "SystemRecoveryRpc_us_50p",
        "SystemRecoveryRpc_us_90p",
        "SystemRecoveryRpc_us_99p",
        "UnregisterReplicaRpc_us_50p",
        "UnregisterReplicaRpc_us_90p",
        "UnregisterReplicaRpc_us_99p",
        "UpdateDataInstanceConfigRpc_us_50p",
        "UpdateDataInstanceConfigRpc_us_90p",
        "UpdateDataInstanceConfigRpc_us_99p",
        "WalFilesRpc_us_50p",
        "WalFilesRpc_us_90p",
        "WalFilesRpc_us_99p",
    },
    "Memory": {
        "PeakMemoryRes",
        "UnreleasedDeltaObjects",
        "GCLatency_us_50p",
        "GCLatency_us_90p",
        "GCLatency_us_99p",
        "GCSkiplistCleanupLatency_us_50p",
        "GCSkiplistCleanupLatency_us_90p",
        "GCSkiplistCleanupLatency_us_99p",
    },
    "SchemaInfo": {
        "ShowSchema",
    },
    "StorageInfo": {
        "ShowStorageInfoOnDatabase",
        "ShowStorageInfo",
    },
}


def execute(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


@pytest.fixture(scope="module")
def populated_databases():
    """Create two databases with known state for metric value verification."""
    conn = mgclient.connect(host="localhost", port=7687)
    conn.autocommit = True
    cursor = conn.cursor()

    execute(cursor, "USE DATABASE memgraph")
    try:
        execute(cursor, "DROP DATABASE db2")
    except Exception:
        pass
    execute(cursor, "MATCH (n) DETACH DELETE n")

    execute(cursor, "CREATE INDEX ON :Person;")
    execute(cursor, "CREATE INDEX ON :Company;")
    execute(cursor, "CREATE INDEX ON :Person(name);")
    execute(cursor, "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    execute(cursor, "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Company {name: 'Acme'})")
    execute(cursor, "MATCH (a:Person {name: 'Alice'}), (c:Company {name: 'Acme'}) CREATE (a)-[:WORKS_AT]->(c)")
    execute(cursor, "MATCH (n) RETURN n")

    execute(cursor, "CREATE DATABASE db2")
    execute(cursor, "USE DATABASE db2")
    execute(cursor, "CREATE INDEX ON :Item;")
    execute(cursor, "CREATE (:Item {name: 'X'}), (:Item {name: 'Y'})")

    yield

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


def scrape_json():
    with urllib.request.urlopen("http://localhost:9091/metrics", timeout=METRICS_HTTP_TIMEOUT_S) as resp:
        return json.loads(resp.read())


def test_json_no_unexpected_metrics(populated_databases):
    m = scrape_json()
    actual_categories = set(m.keys())
    expected_categories = set(EXPECTED_JSON_METRICS.keys())
    unexpected = actual_categories - expected_categories
    assert not unexpected, f"Unexpected JSON categories: {unexpected}"
    missing_categories = expected_categories - actual_categories
    assert not missing_categories, f"Missing JSON categories: {missing_categories}"

    for category, expected_names in EXPECTED_JSON_METRICS.items():
        actual_names = set(m[category].keys())
        unexpected_names = actual_names - expected_names
        assert not unexpected_names, f"Unexpected metrics in {category}: {unexpected_names}"
        missing_names = expected_names - actual_names
        assert not missing_names, f"Missing metrics in {category}: {missing_names}"


def test_json_storage_fields_reflect_default_db(populated_databases):
    m = scrape_json()
    # JSON endpoint reflects only the default database (memgraph)
    assert m["General"]["vertex_count"] == 3
    assert m["General"]["edge_count"] == 1


def test_json_index_gauges_are_global(populated_databases):
    m = scrape_json()
    # Index counters are global (event counters), not per-db
    assert m["Index"]["ActiveLabelIndices"] == 3
    assert m["Index"]["ActiveLabelPropertyIndices"] == 1


def test_json_constraint_gauges(populated_databases):
    m = scrape_json()
    assert m["Constraint"]["ActiveExistenceConstraints"] == 1


def test_json_transaction_counters_incremented(populated_databases):
    m = scrape_json()
    assert m["Transaction"]["CommitedTransactions"] > 0
    assert m["Transaction"]["SuccessfulQuery"] > 0


def test_json_query_type_counters_incremented(populated_databases):
    m = scrape_json()
    assert m["QueryType"]["WriteQuery"] > 0
    assert m["QueryType"]["ReadQuery"] > 0


def test_json_operator_counters_incremented(populated_databases):
    m = scrape_json()
    assert m["Operator"]["CreateNodeOperator"] > 0
    assert m["Operator"]["ScanAllOperator"] > 0


def test_json_session_gauges(populated_databases):
    m = scrape_json()
    assert m["Session"]["ActiveSessions"] >= 1
    assert m["Session"]["ActiveBoltSessions"] >= 1


def test_json_storage_fields_are_default_db_only(populated_databases):
    """Storage fields (vertex_count etc.) reflect the default DB, not an aggregate."""
    m = scrape_json()
    assert m["General"]["vertex_count"] == 3


def test_json_transaction_counters_are_aggregate_across_databases(populated_databases):
    """Transaction counters aggregate across all databases."""
    conn = mgclient.connect(host="localhost", port=7687)
    conn.autocommit = True
    cursor = conn.cursor()

    before = scrape_json()["Transaction"]["CommitedTransactions"]

    execute(cursor, "USE DATABASE memgraph")
    execute(cursor, "CREATE ()")
    execute(cursor, "USE DATABASE db2")
    execute(cursor, "CREATE ()")

    after = scrape_json()["Transaction"]["CommitedTransactions"]
    assert after - before >= 2
    conn.close()


def test_json_query_type_counters_are_aggregate_across_databases(populated_databases):
    """Query type counters aggregate across all databases."""
    conn = mgclient.connect(host="localhost", port=7687)
    conn.autocommit = True
    cursor = conn.cursor()

    before = scrape_json()["QueryType"]["WriteQuery"]

    execute(cursor, "USE DATABASE memgraph")
    execute(cursor, "CREATE ()")
    execute(cursor, "USE DATABASE db2")
    execute(cursor, "CREATE ()")

    after = scrape_json()["QueryType"]["WriteQuery"]
    assert after - before >= 2
    conn.close()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
