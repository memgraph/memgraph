# Copyright 2025 Memgraph Ltd.
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
from common import connect, execute_and_fetch_all, memgraph


def test_all_show_metrics_info_values_are_present(memgraph):
    expected_metrics = [
        # Constraint (alphabetical)
        {"name": "ActiveExistenceConstraints", "type": "Constraint", "metric type": "Gauge"},
        {"name": "ActiveTypeConstraints", "type": "Constraint", "metric type": "Gauge"},
        {"name": "ActiveUniqueConstraints", "type": "Constraint", "metric type": "Gauge"},
        # General (alphabetical)
        {"name": "AverageDegree", "type": "General", "metric type": "Gauge"},
        {"name": "EdgeCount", "type": "General", "metric type": "Gauge"},
        {"name": "VertexCount", "type": "General", "metric type": "Gauge"},
        # HighAvailability counters (alphabetical)
        {"name": "BecomeLeaderSuccess", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "DemoteInstance", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "DemoteMainToReplicaRpcFail", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "DemoteMainToReplicaRpcSuccess", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "EnableWritingOnMainRpcFail", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "EnableWritingOnMainRpcSuccess", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "FailedToBecomeLeader", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "GetDatabaseHistoriesRpcFail", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "GetDatabaseHistoriesRpcSuccess", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "NoAliveInstanceFailedFailovers", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "PromoteToMainRpcFail", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "PromoteToMainRpcSuccess", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "RaftFailedFailovers", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "RegisterReplicaOnMainRpcFail", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "RegisterReplicaOnMainRpcSuccess", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "RemoveCoordInstance", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "ReplicaRecoveryFail", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "ReplicaRecoverySkip", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "ReplicaRecoverySuccess", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "ShowInstance", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "ShowInstances", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "StateCheckRpcFail", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "StateCheckRpcSuccess", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "SuccessfulFailovers", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "SwapMainUUIDRpcFail", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "SwapMainUUIDRpcSuccess", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "UnregisterReplInstance", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "UnregisterReplicaRpcFail", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "UnregisterReplicaRpcSuccess", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "UpdateDataInstanceConfigRpcFail", "type": "HighAvailability", "metric type": "Counter"},
        {"name": "UpdateDataInstanceConfigRpcSuccess", "type": "HighAvailability", "metric type": "Counter"},
        # HighAvailability histograms (alphabetical by base name)
        {"name": "ChooseMostUpToDateInstance_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "ChooseMostUpToDateInstance_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "ChooseMostUpToDateInstance_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "CurrentWalRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "CurrentWalRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "CurrentWalRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "DataFailover_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "DataFailover_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "DataFailover_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "DemoteMainToReplicaRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "DemoteMainToReplicaRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "DemoteMainToReplicaRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "EnableWritingOnMainRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "EnableWritingOnMainRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "EnableWritingOnMainRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "FinalizeTxnReplication_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "FinalizeTxnReplication_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "FinalizeTxnReplication_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "FrequentHeartbeatRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "FrequentHeartbeatRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "FrequentHeartbeatRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "GetDatabaseHistoriesRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "GetDatabaseHistoriesRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "GetDatabaseHistoriesRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "GetHistories_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "GetHistories_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "GetHistories_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "HeartbeatRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "HeartbeatRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "HeartbeatRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "InstanceFailCallback_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "InstanceFailCallback_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "InstanceFailCallback_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "InstanceSuccCallback_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "InstanceSuccCallback_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "InstanceSuccCallback_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "PrepareCommitRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "PrepareCommitRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "PrepareCommitRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "PromoteToMainRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "PromoteToMainRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "PromoteToMainRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "RegisterReplicaOnMainRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "RegisterReplicaOnMainRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "RegisterReplicaOnMainRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "ReplicaStream_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "ReplicaStream_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "ReplicaStream_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "SnapshotRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "SnapshotRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "SnapshotRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "SocketConnect_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "SocketConnect_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "SocketConnect_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "StartTxnReplication_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "StartTxnReplication_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "StartTxnReplication_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "StateCheckRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "StateCheckRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "StateCheckRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "SystemRecoveryRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "SystemRecoveryRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "SystemRecoveryRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "UnregisterReplicaRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "UnregisterReplicaRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "UnregisterReplicaRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "UpdateDataInstanceConfigRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "UpdateDataInstanceConfigRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "UpdateDataInstanceConfigRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "WalFilesRpc_us_50p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "WalFilesRpc_us_90p", "type": "HighAvailability", "metric type": "Histogram"},
        {"name": "WalFilesRpc_us_99p", "type": "HighAvailability", "metric type": "Histogram"},
        # Index (alphabetical)
        {"name": "ActiveEdgePropertyIndices", "type": "Index", "metric type": "Gauge"},
        {"name": "ActiveEdgeTypeIndices", "type": "Index", "metric type": "Gauge"},
        {"name": "ActiveEdgeTypePropertyIndices", "type": "Index", "metric type": "Gauge"},
        {"name": "ActiveLabelIndices", "type": "Index", "metric type": "Gauge"},
        {"name": "ActiveLabelPropertyIndices", "type": "Index", "metric type": "Gauge"},
        {"name": "ActivePointIndices", "type": "Index", "metric type": "Gauge"},
        {"name": "ActiveTextEdgeIndices", "type": "Index", "metric type": "Gauge"},
        {"name": "ActiveTextIndices", "type": "Index", "metric type": "Gauge"},
        {"name": "ActiveVectorEdgeIndices", "type": "Index", "metric type": "Gauge"},
        {"name": "ActiveVectorIndices", "type": "Index", "metric type": "Gauge"},
        # Memory (alphabetical)
        {"name": "DiskUsage", "type": "Memory", "metric type": "Gauge"},
        {"name": "MemoryRes", "type": "Memory", "metric type": "Gauge"},
        {"name": "PeakMemoryRes", "type": "Memory", "metric type": "Gauge"},
        {"name": "UnreleasedDeltaObjects", "type": "Memory", "metric type": "Gauge"},
        {"name": "GCLatency_us_50p", "type": "Memory", "metric type": "Histogram"},
        {"name": "GCLatency_us_90p", "type": "Memory", "metric type": "Histogram"},
        {"name": "GCLatency_us_99p", "type": "Memory", "metric type": "Histogram"},
        {"name": "GCSkiplistCleanupLatency_us_50p", "type": "Memory", "metric type": "Histogram"},
        {"name": "GCSkiplistCleanupLatency_us_90p", "type": "Memory", "metric type": "Histogram"},
        {"name": "GCSkiplistCleanupLatency_us_99p", "type": "Memory", "metric type": "Histogram"},
        # Operator (alphabetical)
        {"name": "AccumulateOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "AggregateOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ApplyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "CallProcedureOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "CartesianOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ConstructNamedPathOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "CreateExpandOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "CreateNodeOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "DeleteOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "DistinctOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "EdgeUniquenessFilterOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "EmptyResultOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "EvaluatePatternFilterOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ExpandOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ExpandVariableOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "FilterOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ForeachOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "HashJoinOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "IndexedJoinOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "LimitOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "MergeOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "OnceOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "OptionalOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "OrderByOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "PeriodicCommitOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "PeriodicSubqueryOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ProduceOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "RemoveLabelsOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "RemoveNestedPropertyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "RemovePropertyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "RollUpApplyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByEdgeIdOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByEdgeOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByEdgePropertyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByEdgePropertyRangeOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByEdgePropertyValueOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByEdgeTypeOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByEdgeTypePropertyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByEdgeTypePropertyRangeOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByEdgeTypePropertyValueOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByIdOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByLabelOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByLabelPropertiesOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByPointDistanceOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByPointWithinbboxOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "SetLabelsOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "SetNestedPropertyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "SetPropertiesOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "SetPropertyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "SkipOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "UnionOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "UnwindOperator", "type": "Operator", "metric type": "Counter"},
        # Query
        {"name": "QueryExecutionLatency_us_50p", "type": "Query", "metric type": "Histogram"},
        {"name": "QueryExecutionLatency_us_90p", "type": "Query", "metric type": "Histogram"},
        {"name": "QueryExecutionLatency_us_99p", "type": "Query", "metric type": "Histogram"},
        # QueryType
        {"name": "ReadQuery", "type": "QueryType", "metric type": "Counter"},
        {"name": "ReadWriteQuery", "type": "QueryType", "metric type": "Counter"},
        {"name": "WriteQuery", "type": "QueryType", "metric type": "Counter"},
        # SchemaInfo
        {"name": "ShowSchema", "type": "SchemaInfo", "metric type": "Counter"},
        # Session (BoltMessages first, then Gauges alphabetical)
        {"name": "BoltMessages", "type": "Session", "metric type": "Counter"},
        {"name": "ActiveBoltSessions", "type": "Session", "metric type": "Gauge"},
        {"name": "ActiveSSLSessions", "type": "Session", "metric type": "Gauge"},
        {"name": "ActiveSessions", "type": "Session", "metric type": "Gauge"},
        {"name": "ActiveTCPSessions", "type": "Session", "metric type": "Gauge"},
        {"name": "ActiveWebSocketSessions", "type": "Session", "metric type": "Gauge"},
        # Snapshot
        {"name": "SnapshotCreationLatency_us_50p", "type": "Snapshot", "metric type": "Histogram"},
        {"name": "SnapshotCreationLatency_us_90p", "type": "Snapshot", "metric type": "Histogram"},
        {"name": "SnapshotCreationLatency_us_99p", "type": "Snapshot", "metric type": "Histogram"},
        {"name": "SnapshotRecoveryLatency_us_50p", "type": "Snapshot", "metric type": "Histogram"},
        {"name": "SnapshotRecoveryLatency_us_90p", "type": "Snapshot", "metric type": "Histogram"},
        {"name": "SnapshotRecoveryLatency_us_99p", "type": "Snapshot", "metric type": "Histogram"},
        # Stream
        {"name": "MessagesConsumed", "type": "Stream", "metric type": "Counter"},
        {"name": "StreamsCreated", "type": "Stream", "metric type": "Counter"},
        # TTL
        {"name": "DeletedEdges", "type": "TTL", "metric type": "Counter"},
        {"name": "DeletedNodes", "type": "TTL", "metric type": "Counter"},
        # Transaction (counters alphabetical, then Gauge)
        {"name": "CommitedTransactions", "type": "Transaction", "metric type": "Counter"},
        {"name": "FailedPrepare", "type": "Transaction", "metric type": "Counter"},
        {"name": "FailedPull", "type": "Transaction", "metric type": "Counter"},
        {"name": "FailedQuery", "type": "Transaction", "metric type": "Counter"},
        {"name": "RollbackedTransactions", "type": "Transaction", "metric type": "Counter"},
        {"name": "SuccessfulQuery", "type": "Transaction", "metric type": "Counter"},
        {"name": "TransientErrors", "type": "Transaction", "metric type": "Counter"},
        {"name": "WriteWriteConflicts", "type": "Transaction", "metric type": "Counter"},
        {"name": "ActiveTransactions", "type": "Transaction", "metric type": "Gauge"},
        # Trigger
        {"name": "TriggersCreated", "type": "Trigger", "metric type": "Counter"},
        {"name": "TriggersExecuted", "type": "Trigger", "metric type": "Counter"},
    ]
    results = list(memgraph.execute_and_fetch("SHOW METRICS INFO"))
    actual_metrics = [{"name": x["name"], "type": x["type"], "metric type": x["metric type"]} for x in results]

    for expected, actual in zip(expected_metrics, actual_metrics):
        assert expected == actual


def get_metric_value(memgraph, metric_name, on_clause=None):
    query = "SHOW METRICS INFO"
    if on_clause:
        query += f" {on_clause}"
    results = list(memgraph.execute_and_fetch(query))
    for r in results:
        if r["name"] == metric_name:
            return r["value"]
    return None


def test_constraint_metrics_are_updated(memgraph):
    """Test that constraint metrics are correctly incremented and decremented."""
    # Get initial values
    initial_existence = get_metric_value(memgraph, "ActiveExistenceConstraints")
    initial_unique = get_metric_value(memgraph, "ActiveUniqueConstraints")
    initial_type = get_metric_value(memgraph, "ActiveTypeConstraints")

    assert initial_existence == 0
    assert initial_unique == 0
    assert initial_type == 0

    # Create existence constraint
    memgraph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    assert get_metric_value(memgraph, "ActiveExistenceConstraints") == 1

    # Create unique constraint
    memgraph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")
    assert get_metric_value(memgraph, "ActiveUniqueConstraints") == 1

    # Create type constraint
    memgraph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT n.age IS TYPED INTEGER;")
    assert get_metric_value(memgraph, "ActiveTypeConstraints") == 1

    # Create additional constraints to verify counting
    memgraph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS (n.email);")
    assert get_metric_value(memgraph, "ActiveExistenceConstraints") == 2

    memgraph.execute("CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE;")
    assert get_metric_value(memgraph, "ActiveUniqueConstraints") == 2

    # Drop constraints and verify metrics decrement
    memgraph.execute("DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.name);")
    assert get_metric_value(memgraph, "ActiveExistenceConstraints") == 1

    memgraph.execute("DROP CONSTRAINT ON (n:Person) ASSERT n.id IS UNIQUE;")
    assert get_metric_value(memgraph, "ActiveUniqueConstraints") == 1

    memgraph.execute("DROP CONSTRAINT ON (n:Person) ASSERT n.age IS TYPED INTEGER;")
    assert get_metric_value(memgraph, "ActiveTypeConstraints") == 0

    # Clean up remaining constraints
    memgraph.execute("DROP CONSTRAINT ON (n:Person) ASSERT EXISTS (n.email);")
    memgraph.execute("DROP CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE;")

    # Verify all back to zero
    assert get_metric_value(memgraph, "ActiveExistenceConstraints") == 0
    assert get_metric_value(memgraph, "ActiveUniqueConstraints") == 0
    assert get_metric_value(memgraph, "ActiveTypeConstraints") == 0


def test_index_metrics_are_updated(memgraph):
    initial_label = get_metric_value(memgraph, "ActiveLabelIndices")
    initial_label_prop = get_metric_value(memgraph, "ActiveLabelPropertyIndices")

    memgraph.execute("CREATE INDEX ON :Person;")
    assert get_metric_value(memgraph, "ActiveLabelIndices") == initial_label + 1

    memgraph.execute("CREATE INDEX ON :Company;")
    assert get_metric_value(memgraph, "ActiveLabelIndices") == initial_label + 2

    memgraph.execute("CREATE INDEX ON :Person(name);")
    assert get_metric_value(memgraph, "ActiveLabelPropertyIndices") == initial_label_prop + 1

    memgraph.execute("CREATE INDEX ON :Company(name);")
    assert get_metric_value(memgraph, "ActiveLabelPropertyIndices") == initial_label_prop + 2

    memgraph.execute("DROP INDEX ON :Company;")
    assert get_metric_value(memgraph, "ActiveLabelIndices") == initial_label + 1

    memgraph.execute("DROP INDEX ON :Person;")
    assert get_metric_value(memgraph, "ActiveLabelIndices") == initial_label

    memgraph.execute("DROP INDEX ON :Company(name);")
    assert get_metric_value(memgraph, "ActiveLabelPropertyIndices") == initial_label_prop + 1

    memgraph.execute("DROP INDEX ON :Person(name);")
    assert get_metric_value(memgraph, "ActiveLabelPropertyIndices") == initial_label_prop


def test_transaction_and_query_type_counters_are_updated(memgraph):
    initial_committed = get_metric_value(memgraph, "CommitedTransactions")
    initial_successful = get_metric_value(memgraph, "SuccessfulQuery")
    initial_read = get_metric_value(memgraph, "ReadQuery")
    initial_write = get_metric_value(memgraph, "WriteQuery")

    memgraph.execute("MATCH (n) RETURN n")
    assert get_metric_value(memgraph, "ReadQuery") > initial_read
    assert get_metric_value(memgraph, "SuccessfulQuery") > initial_successful
    assert get_metric_value(memgraph, "CommitedTransactions") > initial_committed

    initial_write = get_metric_value(memgraph, "WriteQuery")
    memgraph.execute("CREATE (n:TestNode)")
    assert get_metric_value(memgraph, "WriteQuery") > initial_write


def test_operator_counters_are_updated(memgraph):
    initial_scan_all = get_metric_value(memgraph, "ScanAllOperator")
    initial_create_node = get_metric_value(memgraph, "CreateNodeOperator")
    initial_delete = get_metric_value(memgraph, "DeleteOperator")

    memgraph.execute("MATCH (n) RETURN n")
    assert get_metric_value(memgraph, "ScanAllOperator") > initial_scan_all

    memgraph.execute("CREATE (n:TestNode)")
    assert get_metric_value(memgraph, "CreateNodeOperator") > initial_create_node

    memgraph.execute("MATCH (n:TestNode) DELETE n")
    assert get_metric_value(memgraph, "DeleteOperator") > initial_delete


def test_general_gauges_reflect_graph_state(memgraph):
    initial_vertices = get_metric_value(memgraph, "VertexCount", "ON CURRENT")
    initial_edges = get_metric_value(memgraph, "EdgeCount", "ON CURRENT")

    memgraph.execute("CREATE (a), (b)")
    assert get_metric_value(memgraph, "VertexCount", "ON CURRENT") == initial_vertices + 2

    memgraph.execute("MATCH (a), (b) WHERE id(a) < id(b) WITH a, b LIMIT 1 CREATE (a)-[:R]->(b)")
    assert get_metric_value(memgraph, "EdgeCount", "ON CURRENT") == initial_edges + 1

    memgraph.execute("MATCH (n) DETACH DELETE n")
    memgraph.execute("FREE MEMORY")
    assert get_metric_value(memgraph, "VertexCount", "ON CURRENT") == 0
    assert get_metric_value(memgraph, "EdgeCount", "ON CURRENT") == 0


def test_on_current_syntax(memgraph):
    initial = get_metric_value(memgraph, "VertexCount", "ON CURRENT")
    memgraph.execute("CREATE (n:TestNode)")
    assert get_metric_value(memgraph, "VertexCount", "ON CURRENT") == initial + 1


def test_on_database_syntax(memgraph):
    initial = get_metric_value(memgraph, "VertexCount", "ON DATABASE memgraph")
    memgraph.execute("CREATE (n:TestNode)")
    assert get_metric_value(memgraph, "VertexCount", "ON DATABASE memgraph") == initial + 1


def test_per_database_metric_isolation(connect):
    cursor = connect.cursor()

    execute_and_fetch_all(cursor, "USE DATABASE memgraph")
    execute_and_fetch_all(cursor, "MATCH (n) DETACH DELETE n")
    execute_and_fetch_all(cursor, "FREE MEMORY")

    def get_value(on_clause, name):
        rows = execute_and_fetch_all(cursor, f"SHOW METRICS INFO {on_clause}")
        for row in rows:
            if row[0] == name:
                return row[3]
        return None

    initial_global = get_value("", "VertexCount")

    execute_and_fetch_all(cursor, "CREATE (n:A), (n2:A), (n3:A)")

    execute_and_fetch_all(cursor, "CREATE DATABASE alt")
    execute_and_fetch_all(cursor, "USE DATABASE alt")
    execute_and_fetch_all(cursor, "CREATE (n:B)")

    execute_and_fetch_all(cursor, "USE DATABASE memgraph")

    assert get_value("ON DATABASE memgraph", "VertexCount") == 3
    assert get_value("ON DATABASE alt", "VertexCount") == 1
    assert get_value("", "VertexCount") == initial_global + 4


def test_query_execution_latency_histogram_has_observations(memgraph):
    memgraph.execute("MATCH (n) RETURN n")
    assert get_metric_value(memgraph, "QueryExecutionLatency_us_50p") > 0


def test_session_metrics_reflect_active_connection(memgraph):
    assert get_metric_value(memgraph, "ActiveSessions") >= 1
    assert get_metric_value(memgraph, "ActiveBoltSessions") >= 1


def test_failed_query_incremented_on_parse_error(connect):
    cursor = connect.cursor()
    initial = next(
        row[3] for row in execute_and_fetch_all(cursor, "SHOW METRICS INFO ON CURRENT") if row[0] == "FailedQuery"
    )
    try:
        cursor.execute("METCH (n) RETURN n")
    except Exception:
        pass
    after = next(
        row[3] for row in execute_and_fetch_all(cursor, "SHOW METRICS INFO ON CURRENT") if row[0] == "FailedQuery"
    )
    assert after == initial + 1


def test_failed_prepare_incremented_on_prepare_error(connect):
    cursor = connect.cursor()
    initial = next(
        row[3] for row in execute_and_fetch_all(cursor, "SHOW METRICS INFO ON CURRENT") if row[0] == "FailedPrepare"
    )
    try:
        cursor.execute("BEGIN")
        cursor.execute("CREATE USER test_user")
    except Exception:
        pass
    after = next(
        row[3] for row in execute_and_fetch_all(cursor, "SHOW METRICS INFO ON CURRENT") if row[0] == "FailedPrepare"
    )
    assert after == initial + 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
