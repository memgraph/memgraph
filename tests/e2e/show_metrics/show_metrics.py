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


def test_all_show_metrics_info_values_are_present(memgraph):
    expected_metrics = [
        {"name": "AverageDegree", "type": "General", "metric type": "Gauge"},
        {"name": "EdgeCount", "type": "General", "metric type": "Gauge"},
        {"name": "VertexCount", "type": "General", "metric type": "Gauge"},
        {"name": "ActiveLabelIndices", "type": "Index", "metric type": "Counter"},
        {"name": "ActiveLabelPropertyIndices", "type": "Index", "metric type": "Counter"},
        {"name": "ActiveTextIndices", "type": "Index", "metric type": "Counter"},
        {"name": "UnreleasedDeltaObjects", "type": "Memory", "metric type": "Counter"},
        {"name": "DiskUsage", "type": "Memory", "metric type": "Gauge"},
        {"name": "MemoryRes", "type": "Memory", "metric type": "Gauge"},
        {"name": "PeakMemoryRes", "type": "Memory", "metric type": "Gauge"},
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
        {"name": "RemovePropertyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "RollUpApplyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByEdgeIdOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByEdgeTypeOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByIdOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByLabelOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByLabelPropertyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByLabelPropertyRangeOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllByLabelPropertyValueOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "ScanAllOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "SetLabelsOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "SetPropertiesOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "SetPropertyOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "SkipOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "UnionOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "UnwindOperator", "type": "Operator", "metric type": "Counter"},
        {"name": "QueryExecutionLatency_us_50p", "type": "Query", "metric type": "Histogram"},
        {"name": "QueryExecutionLatency_us_90p", "type": "Query", "metric type": "Histogram"},
        {"name": "QueryExecutionLatency_us_99p", "type": "Query", "metric type": "Histogram"},
        {"name": "ReadQuery", "type": "QueryType", "metric type": "Counter"},
        {"name": "ReadWriteQuery", "type": "QueryType", "metric type": "Counter"},
        {"name": "WriteQuery", "type": "QueryType", "metric type": "Counter"},
        {"name": "ActiveBoltSessions", "type": "Session", "metric type": "Counter"},
        {"name": "ActiveSSLSessions", "type": "Session", "metric type": "Counter"},
        {"name": "ActiveSessions", "type": "Session", "metric type": "Counter"},
        {"name": "ActiveTCPSessions", "type": "Session", "metric type": "Counter"},
        {"name": "ActiveWebSocketSessions", "type": "Session", "metric type": "Counter"},
        {"name": "BoltMessages", "type": "Session", "metric type": "Counter"},
        {"name": "SnapshotCreationLatency_us_50p", "type": "Snapshot", "metric type": "Histogram"},
        {"name": "SnapshotCreationLatency_us_90p", "type": "Snapshot", "metric type": "Histogram"},
        {"name": "SnapshotCreationLatency_us_99p", "type": "Snapshot", "metric type": "Histogram"},
        {"name": "SnapshotRecoveryLatency_us_50p", "type": "Snapshot", "metric type": "Histogram"},
        {"name": "SnapshotRecoveryLatency_us_90p", "type": "Snapshot", "metric type": "Histogram"},
        {"name": "SnapshotRecoveryLatency_us_99p", "type": "Snapshot", "metric type": "Histogram"},
        {"name": "MessagesConsumed", "type": "Stream", "metric type": "Counter"},
        {"name": "StreamsCreated", "type": "Stream", "metric type": "Counter"},
        {"name": "DeletedEdges", "type": "TTL", "metric type": "Counter"},
        {"name": "DeletedNodes", "type": "TTL", "metric type": "Counter"},
        {"name": "ActiveTransactions", "type": "Transaction", "metric type": "Counter"},
        {"name": "CommitedTransactions", "type": "Transaction", "metric type": "Counter"},
        {"name": "FailedPrepare", "type": "Transaction", "metric type": "Counter"},
        {"name": "FailedPull", "type": "Transaction", "metric type": "Counter"},
        {"name": "FailedQuery", "type": "Transaction", "metric type": "Counter"},
        {"name": "RollbackedTransactions", "type": "Transaction", "metric type": "Counter"},
        {"name": "SuccessfulQuery", "type": "Transaction", "metric type": "Counter"},
        {"name": "TriggersCreated", "type": "Trigger", "metric type": "Counter"},
        {"name": "TriggersExecuted", "type": "Trigger", "metric type": "Counter"},
    ]
    results = list(memgraph.execute_and_fetch("SHOW METRICS INFO"))
    actual_metrics = [{"name": x["name"], "type": x["type"], "metric type": x["metric type"]} for x in results]

    for expected, actual in zip(expected_metrics, actual_metrics):
        assert expected == actual


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-rA"]))
