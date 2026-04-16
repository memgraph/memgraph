// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "metrics/prometheus_metrics.hpp"

#include <fmt/format.h>
#include <prometheus/client_metric.h>
#include <prometheus/detail/builder.h>
#include <array>

#include <ranges>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "flags/coord_flag_env_handler.hpp"
#include "utils/logging.hpp"

namespace r = std::ranges;
namespace rv = r::views;

namespace memgraph::metrics {

namespace {

using prometheus::ClientMetric;

bool IsLegacyCoordinatorDeltaMetric(std::string_view name) {
  static constexpr std::array<std::string_view, 28> kLegacyCoordinatorDeltaMetrics{
      "SuccessfulFailovers",
      "RaftFailedFailovers",
      "NoAliveInstanceFailedFailovers",
      "BecomeLeaderSuccess",
      "FailedToBecomeLeader",
      "ShowInstance",
      "ShowInstances",
      "DemoteInstance",
      "UnregisterReplInstance",
      "RemoveCoordInstance",
      "StateCheckRpcFail",
      "StateCheckRpcSuccess",
      "UnregisterReplicaRpcFail",
      "UnregisterReplicaRpcSuccess",
      "EnableWritingOnMainRpcFail",
      "EnableWritingOnMainRpcSuccess",
      "PromoteToMainRpcFail",
      "PromoteToMainRpcSuccess",
      "DemoteMainToReplicaRpcFail",
      "DemoteMainToReplicaRpcSuccess",
      "RegisterReplicaOnMainRpcFail",
      "RegisterReplicaOnMainRpcSuccess",
      "SwapMainUUIDRpcFail",
      "SwapMainUUIDRpcSuccess",
      "GetDatabaseHistoriesRpcFail",
      "GetDatabaseHistoriesRpcSuccess",
      "UpdateDataInstanceConfigRpcFail",
      "UpdateDataInstanceConfigRpcSuccess",
  };
  return std::ranges::find(kLegacyCoordinatorDeltaMetrics, name) != kLegacyCoordinatorDeltaMetrics.end();
}

// 16 buckets covering 10µs to 120s
prometheus::Histogram::BucketBoundaries const kLatencyBuckets{
    0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0};

}  // namespace

PrometheusMetrics::PrometheusMetrics()
    // Per-database families
    : vertex_count_family_{prometheus::BuildGauge()
                               .Name("memgraph_vertex_count")
                               .Help("Number of vertices in the database")
                               .Register(registry_)},
      edge_count_family_{prometheus::BuildGauge()
                             .Name("memgraph_edge_count")
                             .Help("Number of edges in the database")
                             .Register(registry_)},
      disk_usage_family_{prometheus::BuildGauge()
                             .Name("memgraph_disk_usage_bytes")
                             .Help("Disk usage of the database in bytes")
                             .Register(registry_)},
      memory_res_family_{prometheus::BuildGauge()
                             .Name("memgraph_memory_res_bytes")
                             .Help("Resident memory usage of the database in bytes")
                             .Register(registry_)},
      active_transactions_family_{prometheus::BuildGauge()
                                      .Name("memgraph_active_transactions")
                                      .Help("Number of active transactions")
                                      .Register(registry_)},
      committed_transactions_family_{prometheus::BuildCounter()
                                         .Name("memgraph_committed_transactions_total")
                                         .Help("Total number of committed transactions")
                                         .Register(registry_)},
      rolled_back_transactions_family_{prometheus::BuildCounter()
                                           .Name("memgraph_rolled_back_transactions_total")
                                           .Help("Total number of rolled back transactions")
                                           .Register(registry_)},
      failed_query_family_{prometheus::BuildCounter()
                               .Name("memgraph_failed_queries_total")
                               .Help("Total number of failed queries")
                               .Register(registry_)},
      read_query_family_{prometheus::BuildCounter()
                             .Name("memgraph_read_queries_total")
                             .Help("Total number of read queries")
                             .Register(registry_)},
      write_query_family_{prometheus::BuildCounter()
                              .Name("memgraph_write_queries_total")
                              .Help("Total number of write queries")
                              .Register(registry_)},
      read_write_query_family_{prometheus::BuildCounter()
                                   .Name("memgraph_read_write_queries_total")
                                   .Help("Total number of read-write queries")
                                   .Register(registry_)},
      // Operators
      once_operator_family_{prometheus::BuildCounter()
                                .Name("memgraph_once_operator_total")
                                .Help("Number of times Once operator was used")
                                .Register(registry_)},
      create_node_operator_family_{prometheus::BuildCounter()
                                       .Name("memgraph_create_node_operator_total")
                                       .Help("Number of times CreateNode operator was used")
                                       .Register(registry_)},
      create_expand_operator_family_{prometheus::BuildCounter()
                                         .Name("memgraph_create_expand_operator_total")
                                         .Help("Number of times CreateExpand operator was used")
                                         .Register(registry_)},
      scan_all_operator_family_{prometheus::BuildCounter()
                                    .Name("memgraph_scan_all_operator_total")
                                    .Help("Number of times ScanAll operator was used")
                                    .Register(registry_)},
      scan_all_by_label_operator_family_{prometheus::BuildCounter()
                                             .Name("memgraph_scan_all_by_label_operator_total")
                                             .Help("Number of times ScanAllByLabel operator was used")
                                             .Register(registry_)},
      scan_all_by_label_properties_operator_family_{
          prometheus::BuildCounter()
              .Name("memgraph_scan_all_by_label_properties_operator_total")
              .Help("Number of times ScanAllByLabelProperties operator was used")
              .Register(registry_)},
      scan_all_by_id_operator_family_{prometheus::BuildCounter()
                                          .Name("memgraph_scan_all_by_id_operator_total")
                                          .Help("Number of times ScanAllById operator was used")
                                          .Register(registry_)},
      scan_all_by_edge_operator_family_{prometheus::BuildCounter()
                                            .Name("memgraph_scan_all_by_edge_operator_total")
                                            .Help("Number of times ScanAllByEdge operator was used")
                                            .Register(registry_)},
      scan_all_by_edge_type_operator_family_{prometheus::BuildCounter()
                                                 .Name("memgraph_scan_all_by_edge_type_operator_total")
                                                 .Help("Number of times ScanAllByEdgeType operator was used")
                                                 .Register(registry_)},
      scan_all_by_edge_type_property_operator_family_{
          prometheus::BuildCounter()
              .Name("memgraph_scan_all_by_edge_type_property_operator_total")
              .Help("Number of times ScanAllByEdgeTypeProperty operator was used")
              .Register(registry_)},
      scan_all_by_edge_type_property_value_operator_family_{
          prometheus::BuildCounter()
              .Name("memgraph_scan_all_by_edge_type_property_value_operator_total")
              .Help("Number of times ScanAllByEdgeTypePropertyValue operator was used")
              .Register(registry_)},
      scan_all_by_edge_type_property_range_operator_family_{
          prometheus::BuildCounter()
              .Name("memgraph_scan_all_by_edge_type_property_range_operator_total")
              .Help("Number of times ScanAllByEdgeTypePropertyRange operator was used")
              .Register(registry_)},
      scan_all_by_edge_property_operator_family_{prometheus::BuildCounter()
                                                     .Name("memgraph_scan_all_by_edge_property_operator_total")
                                                     .Help("Number of times ScanAllByEdgeProperty operator was used")
                                                     .Register(registry_)},
      scan_all_by_edge_property_value_operator_family_{
          prometheus::BuildCounter()
              .Name("memgraph_scan_all_by_edge_property_value_operator_total")
              .Help("Number of times ScanAllByEdgePropertyValue operator was used")
              .Register(registry_)},
      scan_all_by_edge_property_range_operator_family_{
          prometheus::BuildCounter()
              .Name("memgraph_scan_all_by_edge_property_range_operator_total")
              .Help("Number of times ScanAllByEdgePropertyRange operator was used")
              .Register(registry_)},
      scan_all_by_edge_id_operator_family_{prometheus::BuildCounter()
                                               .Name("memgraph_scan_all_by_edge_id_operator_total")
                                               .Help("Number of times ScanAllByEdgeId operator was used")
                                               .Register(registry_)},
      scan_all_by_point_distance_operator_family_{prometheus::BuildCounter()
                                                      .Name("memgraph_scan_all_by_point_distance_operator_total")
                                                      .Help("Number of times ScanAllByPointDistance operator was used")
                                                      .Register(registry_)},
      scan_all_by_point_withinbbox_operator_family_{
          prometheus::BuildCounter()
              .Name("memgraph_scan_all_by_point_withinbbox_operator_total")
              .Help("Number of times ScanAllByPointWithinbbox operator was used")
              .Register(registry_)},
      expand_operator_family_{prometheus::BuildCounter()
                                  .Name("memgraph_expand_operator_total")
                                  .Help("Number of times Expand operator was used")
                                  .Register(registry_)},
      expand_variable_operator_family_{prometheus::BuildCounter()
                                           .Name("memgraph_expand_variable_operator_total")
                                           .Help("Number of times ExpandVariable operator was used")
                                           .Register(registry_)},
      construct_named_path_operator_family_{prometheus::BuildCounter()
                                                .Name("memgraph_construct_named_path_operator_total")
                                                .Help("Number of times ConstructNamedPath operator was used")
                                                .Register(registry_)},
      filter_operator_family_{prometheus::BuildCounter()
                                  .Name("memgraph_filter_operator_total")
                                  .Help("Number of times Filter operator was used")
                                  .Register(registry_)},
      produce_operator_family_{prometheus::BuildCounter()
                                   .Name("memgraph_produce_operator_total")
                                   .Help("Number of times Produce operator was used")
                                   .Register(registry_)},
      delete_operator_family_{prometheus::BuildCounter()
                                  .Name("memgraph_delete_operator_total")
                                  .Help("Number of times Delete operator was used")
                                  .Register(registry_)},
      set_property_operator_family_{prometheus::BuildCounter()
                                        .Name("memgraph_set_property_operator_total")
                                        .Help("Number of times SetProperty operator was used")
                                        .Register(registry_)},
      set_properties_operator_family_{prometheus::BuildCounter()
                                          .Name("memgraph_set_properties_operator_total")
                                          .Help("Number of times SetProperties operator was used")
                                          .Register(registry_)},
      set_labels_operator_family_{prometheus::BuildCounter()
                                      .Name("memgraph_set_labels_operator_total")
                                      .Help("Number of times SetLabels operator was used")
                                      .Register(registry_)},
      remove_property_operator_family_{prometheus::BuildCounter()
                                           .Name("memgraph_remove_property_operator_total")
                                           .Help("Number of times RemoveProperty operator was used")
                                           .Register(registry_)},
      remove_labels_operator_family_{prometheus::BuildCounter()
                                         .Name("memgraph_remove_labels_operator_total")
                                         .Help("Number of times RemoveLabels operator was used")
                                         .Register(registry_)},
      edge_uniqueness_filter_operator_family_{prometheus::BuildCounter()
                                                  .Name("memgraph_edge_uniqueness_filter_operator_total")
                                                  .Help("Number of times EdgeUniquenessFilter operator was used")
                                                  .Register(registry_)},
      empty_result_operator_family_{prometheus::BuildCounter()
                                        .Name("memgraph_empty_result_operator_total")
                                        .Help("Number of times EmptyResult operator was used")
                                        .Register(registry_)},
      accumulate_operator_family_{prometheus::BuildCounter()
                                      .Name("memgraph_accumulate_operator_total")
                                      .Help("Number of times Accumulate operator was used")
                                      .Register(registry_)},
      aggregate_operator_family_{prometheus::BuildCounter()
                                     .Name("memgraph_aggregate_operator_total")
                                     .Help("Number of times Aggregate operator was used")
                                     .Register(registry_)},
      skip_operator_family_{prometheus::BuildCounter()
                                .Name("memgraph_skip_operator_total")
                                .Help("Number of times Skip operator was used")
                                .Register(registry_)},
      limit_operator_family_{prometheus::BuildCounter()
                                 .Name("memgraph_limit_operator_total")
                                 .Help("Number of times Limit operator was used")
                                 .Register(registry_)},
      order_by_operator_family_{prometheus::BuildCounter()
                                    .Name("memgraph_order_by_operator_total")
                                    .Help("Number of times OrderBy operator was used")
                                    .Register(registry_)},
      merge_operator_family_{prometheus::BuildCounter()
                                 .Name("memgraph_merge_operator_total")
                                 .Help("Number of times Merge operator was used")
                                 .Register(registry_)},
      optional_operator_family_{prometheus::BuildCounter()
                                    .Name("memgraph_optional_operator_total")
                                    .Help("Number of times Optional operator was used")
                                    .Register(registry_)},
      unwind_operator_family_{prometheus::BuildCounter()
                                  .Name("memgraph_unwind_operator_total")
                                  .Help("Number of times Unwind operator was used")
                                  .Register(registry_)},
      distinct_operator_family_{prometheus::BuildCounter()
                                    .Name("memgraph_distinct_operator_total")
                                    .Help("Number of times Distinct operator was used")
                                    .Register(registry_)},
      union_operator_family_{prometheus::BuildCounter()
                                 .Name("memgraph_union_operator_total")
                                 .Help("Number of times Union operator was used")
                                 .Register(registry_)},
      cartesian_operator_family_{prometheus::BuildCounter()
                                     .Name("memgraph_cartesian_operator_total")
                                     .Help("Number of times Cartesian operator was used")
                                     .Register(registry_)},
      call_procedure_operator_family_{prometheus::BuildCounter()
                                          .Name("memgraph_call_procedure_operator_total")
                                          .Help("Number of times CallProcedure operator was used")
                                          .Register(registry_)},
      foreach_operator_family_{prometheus::BuildCounter()
                                   .Name("memgraph_foreach_operator_total")
                                   .Help("Number of times Foreach operator was used")
                                   .Register(registry_)},
      evaluate_pattern_filter_operator_family_{prometheus::BuildCounter()
                                                   .Name("memgraph_evaluate_pattern_filter_operator_total")
                                                   .Help("Number of times EvaluatePatternFilter operator was used")
                                                   .Register(registry_)},
      apply_operator_family_{prometheus::BuildCounter()
                                 .Name("memgraph_apply_operator_total")
                                 .Help("Number of times Apply operator was used")
                                 .Register(registry_)},
      indexed_join_operator_family_{prometheus::BuildCounter()
                                        .Name("memgraph_indexed_join_operator_total")
                                        .Help("Number of times IndexedJoin operator was used")
                                        .Register(registry_)},
      hash_join_operator_family_{prometheus::BuildCounter()
                                     .Name("memgraph_hash_join_operator_total")
                                     .Help("Number of times HashJoin operator was used")
                                     .Register(registry_)},
      roll_up_apply_operator_family_{prometheus::BuildCounter()
                                         .Name("memgraph_roll_up_apply_operator_total")
                                         .Help("Number of times RollUpApply operator was used")
                                         .Register(registry_)},
      periodic_commit_operator_family_{prometheus::BuildCounter()
                                           .Name("memgraph_periodic_commit_operator_total")
                                           .Help("Number of times PeriodicCommit operator was used")
                                           .Register(registry_)},
      periodic_subquery_operator_family_{prometheus::BuildCounter()
                                             .Name("memgraph_periodic_subquery_operator_total")
                                             .Help("Number of times PeriodicSubquery operator was used")
                                             .Register(registry_)},
      set_nested_property_operator_family_{prometheus::BuildCounter()
                                               .Name("memgraph_set_nested_property_operator_total")
                                               .Help("Number of times SetNestedProperty operator was used")
                                               .Register(registry_)},
      remove_nested_property_operator_family_{prometheus::BuildCounter()
                                                  .Name("memgraph_remove_nested_property_operator_total")
                                                  .Help("Number of times RemoveNestedProperty operator was used")
                                                  .Register(registry_)},
      // Index
      active_label_indices_family_{prometheus::BuildGauge()
                                       .Name("memgraph_active_label_indices")
                                       .Help("Number of active label indices")
                                       .Register(registry_)},
      active_label_property_indices_family_{prometheus::BuildGauge()
                                                .Name("memgraph_active_label_property_indices")
                                                .Help("Number of active label property indices")
                                                .Register(registry_)},
      active_edge_type_indices_family_{prometheus::BuildGauge()
                                           .Name("memgraph_active_edge_type_indices")
                                           .Help("Number of active edge type indices")
                                           .Register(registry_)},
      active_edge_type_property_indices_family_{prometheus::BuildGauge()
                                                    .Name("memgraph_active_edge_type_property_indices")
                                                    .Help("Number of active edge type property indices")
                                                    .Register(registry_)},
      active_edge_property_indices_family_{prometheus::BuildGauge()
                                               .Name("memgraph_active_edge_property_indices")
                                               .Help("Number of active edge property indices")
                                               .Register(registry_)},
      active_point_indices_family_{prometheus::BuildGauge()
                                       .Name("memgraph_active_point_indices")
                                       .Help("Number of active point indices")
                                       .Register(registry_)},
      active_text_indices_family_{prometheus::BuildGauge()
                                      .Name("memgraph_active_text_indices")
                                      .Help("Number of active text indices")
                                      .Register(registry_)},
      active_text_edge_indices_family_{prometheus::BuildGauge()
                                           .Name("memgraph_active_text_edge_indices")
                                           .Help("Number of active text edge indices")
                                           .Register(registry_)},
      active_vector_indices_family_{prometheus::BuildGauge()
                                        .Name("memgraph_active_vector_indices")
                                        .Help("Number of active vector indices")
                                        .Register(registry_)},
      active_vector_edge_indices_family_{prometheus::BuildGauge()
                                             .Name("memgraph_active_vector_edge_indices")
                                             .Help("Number of active vector edge indices")
                                             .Register(registry_)},
      // Constraint
      active_existence_constraints_family_{prometheus::BuildGauge()
                                               .Name("memgraph_active_existence_constraints")
                                               .Help("Number of active existence constraints")
                                               .Register(registry_)},
      active_unique_constraints_family_{prometheus::BuildGauge()
                                            .Name("memgraph_active_unique_constraints")
                                            .Help("Number of active unique constraints")
                                            .Register(registry_)},
      active_type_constraints_family_{prometheus::BuildGauge()
                                          .Name("memgraph_active_type_constraints")
                                          .Help("Number of active type constraints")
                                          .Register(registry_)},
      // Stream
      streams_created_family_{prometheus::BuildCounter()
                                  .Name("memgraph_streams_created_total")
                                  .Help("Number of streams created")
                                  .Register(registry_)},
      messages_consumed_family_{prometheus::BuildCounter()
                                    .Name("memgraph_messages_consumed_total")
                                    .Help("Number of consumed streamed messages")
                                    .Register(registry_)},
      // Trigger
      triggers_created_family_{prometheus::BuildCounter()
                                   .Name("memgraph_triggers_created_total")
                                   .Help("Number of triggers created")
                                   .Register(registry_)},
      triggers_executed_family_{prometheus::BuildCounter()
                                    .Name("memgraph_triggers_executed_total")
                                    .Help("Number of triggers executed")
                                    .Register(registry_)},
      // Session
      active_sessions_family_{prometheus::BuildGauge()
                                  .Name("memgraph_active_sessions")
                                  .Help("Number of active connections")
                                  .Register(registry_)},
      active_bolt_sessions_family_{prometheus::BuildGauge()
                                       .Name("memgraph_active_bolt_sessions")
                                       .Help("Number of active Bolt connections")
                                       .Register(registry_)},
      active_tcp_sessions_family_{prometheus::BuildGauge()
                                      .Name("memgraph_active_tcp_sessions")
                                      .Help("Number of active TCP connections")
                                      .Register(registry_)},
      active_ssl_sessions_family_{prometheus::BuildGauge()
                                      .Name("memgraph_active_ssl_sessions")
                                      .Help("Number of active SSL connections")
                                      .Register(registry_)},
      active_websocket_sessions_family_{prometheus::BuildGauge()
                                            .Name("memgraph_active_websocket_sessions")
                                            .Help("Number of active WebSocket connections")
                                            .Register(registry_)},
      bolt_messages_family_{prometheus::BuildCounter()
                                .Name("memgraph_bolt_messages_total")
                                .Help("Number of Bolt messages sent")
                                .Register(registry_)},
      failed_prepare_family_{prometheus::BuildCounter()
                                 .Name("memgraph_failed_prepares_total")
                                 .Help("Total number of failed query preparations")
                                 .Register(registry_)},
      failed_pull_family_{prometheus::BuildCounter()
                              .Name("memgraph_failed_pulls_total")
                              .Help("Total number of failed query pulls")
                              .Register(registry_)},
      successful_query_family_{prometheus::BuildCounter()
                                   .Name("memgraph_successful_queries_total")
                                   .Help("Total number of successful queries")
                                   .Register(registry_)},
      write_write_conflicts_family_{prometheus::BuildCounter()
                                        .Name("memgraph_write_write_conflicts_total")
                                        .Help("Total number of write-write conflicts")
                                        .Register(registry_)},
      transient_errors_family_{prometheus::BuildCounter()
                                   .Name("memgraph_transient_errors_total")
                                   .Help("Total number of transient errors")
                                   .Register(registry_)},
      unreleased_delta_objects_family_{prometheus::BuildGauge()
                                           .Name("memgraph_unreleased_delta_objects")
                                           .Help("Total number of unreleased delta objects in memory")
                                           .Register(registry_)},
      // TTL
      deleted_nodes_family_{prometheus::BuildCounter()
                                .Name("memgraph_deleted_nodes_total")
                                .Help("Number of nodes deleted via TTL")
                                .Register(registry_)},
      deleted_edges_family_{prometheus::BuildCounter()
                                .Name("memgraph_deleted_edges_total")
                                .Help("Number of edges deleted via TTL")
                                .Register(registry_)},
      // SchemaInfo
      show_schema_family_{prometheus::BuildCounter()
                              .Name("memgraph_show_schema_total")
                              .Help("Number of times SHOW SCHEMA INFO was called")
                              .Register(registry_)},
      // Memory
      peak_memory_res_family_{prometheus::BuildGauge()
                                  .Name("memgraph_peak_memory_res_bytes")
                                  .Help("Peak resident memory usage in bytes")
                                  .Register(registry_)},
      // HighAvailability counters
      successful_failovers_family_{prometheus::BuildCounter()
                                       .Name("memgraph_successful_failovers_total")
                                       .Help("Number of successful failovers")
                                       .Register(registry_)},
      raft_failed_failovers_family_{prometheus::BuildCounter()
                                        .Name("memgraph_raft_failed_failovers_total")
                                        .Help("Number of failed failovers due to Raft")
                                        .Register(registry_)},
      no_alive_instance_failed_failovers_family_{prometheus::BuildCounter()
                                                     .Name("memgraph_no_alive_instance_failed_failovers_total")
                                                     .Help("Number of failed failovers due to no alive instance")
                                                     .Register(registry_)},
      become_leader_success_family_{prometheus::BuildCounter()
                                        .Name("memgraph_become_leader_success_total")
                                        .Help("Number of times coordinator became leader successfully")
                                        .Register(registry_)},
      failed_to_become_leader_family_{prometheus::BuildCounter()
                                          .Name("memgraph_failed_to_become_leader_total")
                                          .Help("Number of times coordinator failed to become leader")
                                          .Register(registry_)},
      show_instance_family_{prometheus::BuildCounter()
                                .Name("memgraph_show_instance_total")
                                .Help("Number of times SHOW INSTANCE was called")
                                .Register(registry_)},
      show_instances_family_{prometheus::BuildCounter()
                                 .Name("memgraph_show_instances_total")
                                 .Help("Number of times SHOW INSTANCES was called")
                                 .Register(registry_)},
      demote_instance_family_{prometheus::BuildCounter()
                                  .Name("memgraph_demote_instance_total")
                                  .Help("Number of times DEMOTE INSTANCE was called")
                                  .Register(registry_)},
      unregister_repl_instance_family_{prometheus::BuildCounter()
                                           .Name("memgraph_unregister_repl_instance_total")
                                           .Help("Number of times UNREGISTER INSTANCE was called")
                                           .Register(registry_)},
      remove_coord_instance_family_{prometheus::BuildCounter()
                                        .Name("memgraph_remove_coord_instance_total")
                                        .Help("Number of times REMOVE COORDINATOR was called")
                                        .Register(registry_)},
      replica_recovery_success_family_{prometheus::BuildCounter()
                                           .Name("memgraph_replica_recovery_success_total")
                                           .Help("Number of times replica recovery finished successfully")
                                           .Register(registry_)},
      replica_recovery_fail_family_{prometheus::BuildCounter()
                                        .Name("memgraph_replica_recovery_fail_total")
                                        .Help("Number of times replica recovery finished unsuccessfully")
                                        .Register(registry_)},
      replica_recovery_skip_family_{prometheus::BuildCounter()
                                        .Name("memgraph_replica_recovery_skip_total")
                                        .Help("Number of times replica recovery was skipped")
                                        .Register(registry_)},
      state_check_rpc_success_family_{prometheus::BuildCounter()
                                          .Name("memgraph_state_check_rpc_success_total")
                                          .Help("Number of successful StateCheckRpc calls")
                                          .Register(registry_)},
      state_check_rpc_fail_family_{prometheus::BuildCounter()
                                       .Name("memgraph_state_check_rpc_fail_total")
                                       .Help("Number of failed StateCheckRpc calls")
                                       .Register(registry_)},
      unregister_replica_rpc_success_family_{prometheus::BuildCounter()
                                                 .Name("memgraph_unregister_replica_rpc_success_total")
                                                 .Help("Number of successful UnregisterReplicaRpc calls")
                                                 .Register(registry_)},
      unregister_replica_rpc_fail_family_{prometheus::BuildCounter()
                                              .Name("memgraph_unregister_replica_rpc_fail_total")
                                              .Help("Number of failed UnregisterReplicaRpc calls")
                                              .Register(registry_)},
      enable_writing_on_main_rpc_success_family_{prometheus::BuildCounter()
                                                     .Name("memgraph_enable_writing_on_main_rpc_success_total")
                                                     .Help("Number of successful EnableWritingOnMainRpc calls")
                                                     .Register(registry_)},
      enable_writing_on_main_rpc_fail_family_{prometheus::BuildCounter()
                                                  .Name("memgraph_enable_writing_on_main_rpc_fail_total")
                                                  .Help("Number of failed EnableWritingOnMainRpc calls")
                                                  .Register(registry_)},
      promote_to_main_rpc_success_family_{prometheus::BuildCounter()
                                              .Name("memgraph_promote_to_main_rpc_success_total")
                                              .Help("Number of successful PromoteToMainRpc calls")
                                              .Register(registry_)},
      promote_to_main_rpc_fail_family_{prometheus::BuildCounter()
                                           .Name("memgraph_promote_to_main_rpc_fail_total")
                                           .Help("Number of failed PromoteToMainRpc calls")
                                           .Register(registry_)},
      demote_main_to_replica_rpc_success_family_{prometheus::BuildCounter()
                                                     .Name("memgraph_demote_main_to_replica_rpc_success_total")
                                                     .Help("Number of successful DemoteMainToReplicaRpc calls")
                                                     .Register(registry_)},
      demote_main_to_replica_rpc_fail_family_{prometheus::BuildCounter()
                                                  .Name("memgraph_demote_main_to_replica_rpc_fail_total")
                                                  .Help("Number of failed DemoteMainToReplicaRpc calls")
                                                  .Register(registry_)},
      register_replica_on_main_rpc_success_family_{prometheus::BuildCounter()
                                                       .Name("memgraph_register_replica_on_main_rpc_success_total")
                                                       .Help("Number of successful RegisterReplicaOnMainRpc calls")
                                                       .Register(registry_)},
      register_replica_on_main_rpc_fail_family_{prometheus::BuildCounter()
                                                    .Name("memgraph_register_replica_on_main_rpc_fail_total")
                                                    .Help("Number of failed RegisterReplicaOnMainRpc calls")
                                                    .Register(registry_)},
      swap_main_uuid_rpc_success_family_{prometheus::BuildCounter()
                                             .Name("memgraph_swap_main_uuid_rpc_success_total")
                                             .Help("Number of successful SwapMainUUIDRpc calls")
                                             .Register(registry_)},
      swap_main_uuid_rpc_fail_family_{prometheus::BuildCounter()
                                          .Name("memgraph_swap_main_uuid_rpc_fail_total")
                                          .Help("Number of failed SwapMainUUIDRpc calls")
                                          .Register(registry_)},
      get_database_histories_rpc_success_family_{prometheus::BuildCounter()
                                                     .Name("memgraph_get_database_histories_rpc_success_total")
                                                     .Help("Number of successful GetDatabaseHistoriesRpc calls")
                                                     .Register(registry_)},
      get_database_histories_rpc_fail_family_{prometheus::BuildCounter()
                                                  .Name("memgraph_get_database_histories_rpc_fail_total")
                                                  .Help("Number of failed GetDatabaseHistoriesRpc calls")
                                                  .Register(registry_)},
      update_data_instance_config_rpc_success_family_{
          prometheus::BuildCounter()
              .Name("memgraph_update_data_instance_config_rpc_success_total")
              .Help("Number of successful UpdateDataInstanceConfigRpc calls")
              .Register(registry_)},
      update_data_instance_config_rpc_fail_family_{prometheus::BuildCounter()
                                                       .Name("memgraph_update_data_instance_config_rpc_fail_total")
                                                       .Help("Number of failed UpdateDataInstanceConfigRpc calls")
                                                       .Register(registry_)},
      // Histograms
      query_execution_latency_family_{prometheus::BuildHistogram()
                                          .Name("memgraph_query_execution_latency_seconds")
                                          .Help("Query execution latency in seconds")
                                          .Register(registry_)},
      snapshot_creation_latency_family_{prometheus::BuildHistogram()
                                            .Name("memgraph_snapshot_creation_latency_seconds")
                                            .Help("Snapshot creation latency in seconds")
                                            .Register(registry_)},
      snapshot_recovery_latency_family_{prometheus::BuildHistogram()
                                            .Name("memgraph_snapshot_recovery_latency_seconds")
                                            .Help("Snapshot recovery latency in seconds")
                                            .Register(registry_)},
      instance_succ_callback_family_{prometheus::BuildHistogram()
                                         .Name("memgraph_instance_succ_callback_seconds")
                                         .Help("Instance success callback latency in seconds")
                                         .Register(registry_)},
      instance_fail_callback_family_{prometheus::BuildHistogram()
                                         .Name("memgraph_instance_fail_callback_seconds")
                                         .Help("Instance failure callback latency in seconds")
                                         .Register(registry_)},
      choose_most_up_to_date_instance_family_{prometheus::BuildHistogram()
                                                  .Name("memgraph_choose_most_up_to_date_instance_seconds")
                                                  .Help("Latency of choosing next main in seconds")
                                                  .Register(registry_)},
      socket_connect_family_{prometheus::BuildHistogram()
                                 .Name("memgraph_socket_connect_seconds")
                                 .Help("Latency of Socket::Connect in seconds")
                                 .Register(registry_)},
      replica_stream_family_{prometheus::BuildHistogram()
                                 .Name("memgraph_replica_stream_seconds")
                                 .Help("Latency of creating replica stream in seconds")
                                 .Register(registry_)},
      data_failover_family_{prometheus::BuildHistogram()
                                .Name("memgraph_data_failover_seconds")
                                .Help("Latency of the failover procedure in seconds")
                                .Register(registry_)},
      start_txn_replication_family_{prometheus::BuildHistogram()
                                        .Name("memgraph_start_txn_replication_seconds")
                                        .Help("Latency of starting txn replication in seconds")
                                        .Register(registry_)},
      finalize_txn_replication_family_{prometheus::BuildHistogram()
                                           .Name("memgraph_finalize_txn_replication_seconds")
                                           .Help("Latency of finishing txn replication in seconds")
                                           .Register(registry_)},
      promote_to_main_rpc_histogram_family_{prometheus::BuildHistogram()
                                                .Name("memgraph_promote_to_main_rpc_seconds")
                                                .Help("Latency of PromoteToMainRpc in seconds")
                                                .Register(registry_)},
      demote_main_to_replica_rpc_histogram_family_{prometheus::BuildHistogram()
                                                       .Name("memgraph_demote_main_to_replica_rpc_seconds")
                                                       .Help("Latency of DemoteMainToReplicaRpc in seconds")
                                                       .Register(registry_)},
      register_replica_on_main_rpc_histogram_family_{prometheus::BuildHistogram()
                                                         .Name("memgraph_register_replica_on_main_rpc_seconds")
                                                         .Help("Latency of RegisterReplicaOnMainRpc in seconds")
                                                         .Register(registry_)},
      unregister_replica_rpc_histogram_family_{prometheus::BuildHistogram()
                                                   .Name("memgraph_unregister_replica_rpc_seconds")
                                                   .Help("Latency of UnregisterReplicaRpc in seconds")
                                                   .Register(registry_)},
      enable_writing_on_main_rpc_histogram_family_{prometheus::BuildHistogram()
                                                       .Name("memgraph_enable_writing_on_main_rpc_seconds")
                                                       .Help("Latency of EnableWritingOnMainRpc in seconds")
                                                       .Register(registry_)},
      state_check_rpc_histogram_family_{prometheus::BuildHistogram()
                                            .Name("memgraph_state_check_rpc_seconds")
                                            .Help("Latency of StateCheckRpc in seconds")
                                            .Register(registry_)},
      get_database_histories_rpc_histogram_family_{prometheus::BuildHistogram()
                                                       .Name("memgraph_get_database_histories_rpc_seconds")
                                                       .Help("Latency of GetDatabaseHistoriesRpc in seconds")
                                                       .Register(registry_)},
      heartbeat_rpc_family_{prometheus::BuildHistogram()
                                .Name("memgraph_heartbeat_rpc_seconds")
                                .Help("Latency of HeartbeatRpc in seconds")
                                .Register(registry_)},
      prepare_commit_rpc_family_{prometheus::BuildHistogram()
                                     .Name("memgraph_prepare_commit_rpc_seconds")
                                     .Help("Latency of PrepareCommitRpc in seconds")
                                     .Register(registry_)},
      snapshot_rpc_family_{prometheus::BuildHistogram()
                               .Name("memgraph_snapshot_rpc_seconds")
                               .Help("Latency of SnapshotRpc in seconds")
                               .Register(registry_)},
      current_wal_rpc_family_{prometheus::BuildHistogram()
                                  .Name("memgraph_current_wal_rpc_seconds")
                                  .Help("Latency of CurrentWalRpc in seconds")
                                  .Register(registry_)},
      wal_files_rpc_family_{prometheus::BuildHistogram()
                                .Name("memgraph_wal_files_rpc_seconds")
                                .Help("Latency of WalFilesRpc in seconds")
                                .Register(registry_)},
      frequent_heartbeat_rpc_family_{prometheus::BuildHistogram()
                                         .Name("memgraph_frequent_heartbeat_rpc_seconds")
                                         .Help("Latency of FrequentHeartbeatRpc in seconds")
                                         .Register(registry_)},
      system_recovery_rpc_family_{prometheus::BuildHistogram()
                                      .Name("memgraph_system_recovery_rpc_seconds")
                                      .Help("Latency of SystemRecoveryRpc in seconds")
                                      .Register(registry_)},
      update_data_instance_config_rpc_histogram_family_{prometheus::BuildHistogram()
                                                            .Name("memgraph_update_data_instance_config_rpc_seconds")
                                                            .Help("Latency of UpdateDataInstanceConfigRpc in seconds")
                                                            .Register(registry_)},
      get_histories_family_{prometheus::BuildHistogram()
                                .Name("memgraph_get_histories_seconds")
                                .Help("Latency of retrieving instances history in seconds")
                                .Register(registry_)},
      gc_latency_family_{prometheus::BuildHistogram()
                             .Name("memgraph_gc_latency_seconds")
                             .Help("GC execution latency in seconds")
                             .Register(registry_)},
      gc_skiplist_cleanup_latency_family_{prometheus::BuildHistogram()
                                              .Name("memgraph_gc_skiplist_cleanup_latency_seconds")
                                              .Help("GC skiplist cleanup latency in seconds")
                                              .Register(registry_)}
#ifdef MG_ENTERPRISE
      ,
      instance_up_family_{prometheus::BuildGauge()
                              .Name("memgraph_instance_up")
                              .Help("1 if the instance is up, 0 if down")
                              .Register(registry_)},
      instance_is_leader_family_{prometheus::BuildGauge()
                                     .Name("memgraph_instance_is_leader")
                                     .Help("1 if the instance is the coordinator leader, 0 otherwise")
                                     .Register(registry_)},
      instance_is_main_family_{prometheus::BuildGauge()
                                   .Name("memgraph_instance_is_main")
                                   .Help("1 if the instance is the replication main, 0 otherwise")
                                   .Register(registry_)},
      instance_last_response_seconds_family_{prometheus::BuildGauge()
                                                 .Name("memgraph_instance_last_response_seconds")
                                                 .Help("Seconds since the last successful response from the instance")
                                                 .Register(registry_)}
#endif
{
  // Populate GlobalMetricHandles — only session, memory, and HA metrics
  prometheus::Labels const no_labels{};

  global.active_sessions = &active_sessions_family_.Add(no_labels);
  global.active_bolt_sessions = &active_bolt_sessions_family_.Add(no_labels);
  global.active_tcp_sessions = &active_tcp_sessions_family_.Add(no_labels);
  global.active_ssl_sessions = &active_ssl_sessions_family_.Add(no_labels);
  global.active_websocket_sessions = &active_websocket_sessions_family_.Add(no_labels);
  global.bolt_messages = &bolt_messages_family_.Add(no_labels);

  global.peak_memory_res_bytes = &peak_memory_res_family_.Add(no_labels);

  global.successful_failovers = &successful_failovers_family_.Add(no_labels);
  global.raft_failed_failovers = &raft_failed_failovers_family_.Add(no_labels);
  global.no_alive_instance_failed_failovers = &no_alive_instance_failed_failovers_family_.Add(no_labels);
  global.become_leader_success = &become_leader_success_family_.Add(no_labels);
  global.failed_to_become_leader = &failed_to_become_leader_family_.Add(no_labels);
  global.show_instance = &show_instance_family_.Add(no_labels);
  global.show_instances = &show_instances_family_.Add(no_labels);
  global.demote_instance = &demote_instance_family_.Add(no_labels);
  global.unregister_repl_instance = &unregister_repl_instance_family_.Add(no_labels);
  global.remove_coord_instance = &remove_coord_instance_family_.Add(no_labels);
  global.replica_recovery_success = &replica_recovery_success_family_.Add(no_labels);
  global.replica_recovery_fail = &replica_recovery_fail_family_.Add(no_labels);
  global.replica_recovery_skip = &replica_recovery_skip_family_.Add(no_labels);
  global.state_check_rpc_success = &state_check_rpc_success_family_.Add(no_labels);
  global.state_check_rpc_fail = &state_check_rpc_fail_family_.Add(no_labels);
  global.unregister_replica_rpc_success = &unregister_replica_rpc_success_family_.Add(no_labels);
  global.unregister_replica_rpc_fail = &unregister_replica_rpc_fail_family_.Add(no_labels);
  global.enable_writing_on_main_rpc_success = &enable_writing_on_main_rpc_success_family_.Add(no_labels);
  global.enable_writing_on_main_rpc_fail = &enable_writing_on_main_rpc_fail_family_.Add(no_labels);
  global.promote_to_main_rpc_success = &promote_to_main_rpc_success_family_.Add(no_labels);
  global.promote_to_main_rpc_fail = &promote_to_main_rpc_fail_family_.Add(no_labels);
  global.demote_main_to_replica_rpc_success = &demote_main_to_replica_rpc_success_family_.Add(no_labels);
  global.demote_main_to_replica_rpc_fail = &demote_main_to_replica_rpc_fail_family_.Add(no_labels);
  global.register_replica_on_main_rpc_success = &register_replica_on_main_rpc_success_family_.Add(no_labels);
  global.register_replica_on_main_rpc_fail = &register_replica_on_main_rpc_fail_family_.Add(no_labels);
  global.swap_main_uuid_rpc_success = &swap_main_uuid_rpc_success_family_.Add(no_labels);
  global.swap_main_uuid_rpc_fail = &swap_main_uuid_rpc_fail_family_.Add(no_labels);
  global.get_database_histories_rpc_success = &get_database_histories_rpc_success_family_.Add(no_labels);
  global.get_database_histories_rpc_fail = &get_database_histories_rpc_fail_family_.Add(no_labels);
  global.update_data_instance_config_rpc_success = &update_data_instance_config_rpc_success_family_.Add(no_labels);
  global.update_data_instance_config_rpc_fail = &update_data_instance_config_rpc_fail_family_.Add(no_labels);

  global.instance_succ_callback_seconds = &instance_succ_callback_family_.Add(no_labels, kLatencyBuckets);
  global.instance_fail_callback_seconds = &instance_fail_callback_family_.Add(no_labels, kLatencyBuckets);
  global.choose_most_up_to_date_instance_seconds =
      &choose_most_up_to_date_instance_family_.Add(no_labels, kLatencyBuckets);
  global.socket_connect_seconds = &socket_connect_family_.Add(no_labels, kLatencyBuckets);
  global.replica_stream_seconds = &replica_stream_family_.Add(no_labels, kLatencyBuckets);
  global.data_failover_seconds = &data_failover_family_.Add(no_labels, kLatencyBuckets);
  global.start_txn_replication_seconds = &start_txn_replication_family_.Add(no_labels, kLatencyBuckets);
  global.finalize_txn_replication_seconds = &finalize_txn_replication_family_.Add(no_labels, kLatencyBuckets);
  global.promote_to_main_rpc_seconds = &promote_to_main_rpc_histogram_family_.Add(no_labels, kLatencyBuckets);
  global.demote_main_to_replica_rpc_seconds =
      &demote_main_to_replica_rpc_histogram_family_.Add(no_labels, kLatencyBuckets);
  global.register_replica_on_main_rpc_seconds =
      &register_replica_on_main_rpc_histogram_family_.Add(no_labels, kLatencyBuckets);
  global.unregister_replica_rpc_seconds = &unregister_replica_rpc_histogram_family_.Add(no_labels, kLatencyBuckets);
  global.enable_writing_on_main_rpc_seconds =
      &enable_writing_on_main_rpc_histogram_family_.Add(no_labels, kLatencyBuckets);
  global.state_check_rpc_seconds = &state_check_rpc_histogram_family_.Add(no_labels, kLatencyBuckets);
  global.get_database_histories_rpc_seconds =
      &get_database_histories_rpc_histogram_family_.Add(no_labels, kLatencyBuckets);
  global.heartbeat_rpc_seconds = &heartbeat_rpc_family_.Add(no_labels, kLatencyBuckets);
  global.prepare_commit_rpc_seconds = &prepare_commit_rpc_family_.Add(no_labels, kLatencyBuckets);
  global.snapshot_rpc_seconds = &snapshot_rpc_family_.Add(no_labels, kLatencyBuckets);
  global.current_wal_rpc_seconds = &current_wal_rpc_family_.Add(no_labels, kLatencyBuckets);
  global.wal_files_rpc_seconds = &wal_files_rpc_family_.Add(no_labels, kLatencyBuckets);
  global.frequent_heartbeat_rpc_seconds = &frequent_heartbeat_rpc_family_.Add(no_labels, kLatencyBuckets);
  global.system_recovery_rpc_seconds = &system_recovery_rpc_family_.Add(no_labels, kLatencyBuckets);
  global.update_data_instance_config_rpc_seconds =
      &update_data_instance_config_rpc_histogram_family_.Add(no_labels, kLatencyBuckets);
  global.get_histories_seconds = &get_histories_family_.Add(no_labels, kLatencyBuckets);
}

void PrometheusMetrics::SetStorageSnapshotResolver(StorageSnapshotResolver resolver) {
  std::lock_guard const lock{snapshot_resolver_mutex_};
  storage_snapshot_resolver_ = std::move(resolver);
}

#ifdef MG_ENTERPRISE
void PrometheusMetrics::SetInstanceStatusResolver(InstanceStatusResolver resolver) {
  std::lock_guard const lock{instance_resolver_mutex_};
  instance_status_resolver_ = std::move(resolver);
}
#endif

StorageSnapshot PrometheusMetrics::ResolveStorageSnapshot(std::string_view db_name) const {
  StorageSnapshotResolver resolver;
  {
    std::lock_guard const lock{snapshot_resolver_mutex_};
    resolver = storage_snapshot_resolver_;
  }
  if (resolver) {
    if (auto const snap = resolver(db_name)) return *snap;
  }
  return StorageSnapshot{};
}

DatabaseMetricHandles *PrometheusMetrics::AddDatabase(std::string_view db_name) {
  std::lock_guard const lock{databases_mutex_};
  if (auto it = std::ranges::find_if(databases_, [db_name](auto const &e) { return e.db_name == db_name; });
      it != databases_.end()) {
    ++it->ref_count;
    return &it->handles;
  }
  prometheus::Labels const labels{{"database", std::string(db_name)}};
  databases_.push_back({
      .db_name = std::string(db_name),
      .handles =
          DatabaseMetricHandles{
              .vertex_count = &vertex_count_family_.Add(labels),
              .edge_count = &edge_count_family_.Add(labels),
              .disk_usage_bytes = &disk_usage_family_.Add(labels),
              .memory_res_bytes = &memory_res_family_.Add(labels),
              .once_operator = &once_operator_family_.Add(labels),
              .create_node_operator = &create_node_operator_family_.Add(labels),
              .create_expand_operator = &create_expand_operator_family_.Add(labels),
              .scan_all_operator = &scan_all_operator_family_.Add(labels),
              .scan_all_by_label_operator = &scan_all_by_label_operator_family_.Add(labels),
              .scan_all_by_label_properties_operator = &scan_all_by_label_properties_operator_family_.Add(labels),
              .scan_all_by_id_operator = &scan_all_by_id_operator_family_.Add(labels),
              .scan_all_by_edge_operator = &scan_all_by_edge_operator_family_.Add(labels),
              .scan_all_by_edge_type_operator = &scan_all_by_edge_type_operator_family_.Add(labels),
              .scan_all_by_edge_type_property_operator = &scan_all_by_edge_type_property_operator_family_.Add(labels),
              .scan_all_by_edge_type_property_value_operator =
                  &scan_all_by_edge_type_property_value_operator_family_.Add(labels),
              .scan_all_by_edge_type_property_range_operator =
                  &scan_all_by_edge_type_property_range_operator_family_.Add(labels),
              .scan_all_by_edge_property_operator = &scan_all_by_edge_property_operator_family_.Add(labels),
              .scan_all_by_edge_property_value_operator = &scan_all_by_edge_property_value_operator_family_.Add(labels),
              .scan_all_by_edge_property_range_operator = &scan_all_by_edge_property_range_operator_family_.Add(labels),
              .scan_all_by_edge_id_operator = &scan_all_by_edge_id_operator_family_.Add(labels),
              .scan_all_by_point_distance_operator = &scan_all_by_point_distance_operator_family_.Add(labels),
              .scan_all_by_point_withinbbox_operator = &scan_all_by_point_withinbbox_operator_family_.Add(labels),
              .expand_operator = &expand_operator_family_.Add(labels),
              .expand_variable_operator = &expand_variable_operator_family_.Add(labels),
              .construct_named_path_operator = &construct_named_path_operator_family_.Add(labels),
              .filter_operator = &filter_operator_family_.Add(labels),
              .produce_operator = &produce_operator_family_.Add(labels),
              .delete_operator = &delete_operator_family_.Add(labels),
              .set_property_operator = &set_property_operator_family_.Add(labels),
              .set_properties_operator = &set_properties_operator_family_.Add(labels),
              .set_labels_operator = &set_labels_operator_family_.Add(labels),
              .remove_property_operator = &remove_property_operator_family_.Add(labels),
              .remove_labels_operator = &remove_labels_operator_family_.Add(labels),
              .edge_uniqueness_filter_operator = &edge_uniqueness_filter_operator_family_.Add(labels),
              .empty_result_operator = &empty_result_operator_family_.Add(labels),
              .accumulate_operator = &accumulate_operator_family_.Add(labels),
              .aggregate_operator = &aggregate_operator_family_.Add(labels),
              .skip_operator = &skip_operator_family_.Add(labels),
              .limit_operator = &limit_operator_family_.Add(labels),
              .order_by_operator = &order_by_operator_family_.Add(labels),
              .merge_operator = &merge_operator_family_.Add(labels),
              .optional_operator = &optional_operator_family_.Add(labels),
              .unwind_operator = &unwind_operator_family_.Add(labels),
              .distinct_operator = &distinct_operator_family_.Add(labels),
              .union_operator = &union_operator_family_.Add(labels),
              .cartesian_operator = &cartesian_operator_family_.Add(labels),
              .call_procedure_operator = &call_procedure_operator_family_.Add(labels),
              .foreach_operator = &foreach_operator_family_.Add(labels),
              .evaluate_pattern_filter_operator = &evaluate_pattern_filter_operator_family_.Add(labels),
              .apply_operator = &apply_operator_family_.Add(labels),
              .indexed_join_operator = &indexed_join_operator_family_.Add(labels),
              .hash_join_operator = &hash_join_operator_family_.Add(labels),
              .roll_up_apply_operator = &roll_up_apply_operator_family_.Add(labels),
              .periodic_commit_operator = &periodic_commit_operator_family_.Add(labels),
              .periodic_subquery_operator = &periodic_subquery_operator_family_.Add(labels),
              .set_nested_property_operator = &set_nested_property_operator_family_.Add(labels),
              .remove_nested_property_operator = &remove_nested_property_operator_family_.Add(labels),
              .active_label_indices = &active_label_indices_family_.Add(labels),
              .active_label_property_indices = &active_label_property_indices_family_.Add(labels),
              .active_edge_type_indices = &active_edge_type_indices_family_.Add(labels),
              .active_edge_type_property_indices = &active_edge_type_property_indices_family_.Add(labels),
              .active_edge_property_indices = &active_edge_property_indices_family_.Add(labels),
              .active_point_indices = &active_point_indices_family_.Add(labels),
              .active_text_indices = &active_text_indices_family_.Add(labels),
              .active_text_edge_indices = &active_text_edge_indices_family_.Add(labels),
              .active_vector_indices = &active_vector_indices_family_.Add(labels),
              .active_vector_edge_indices = &active_vector_edge_indices_family_.Add(labels),
              .active_existence_constraints = &active_existence_constraints_family_.Add(labels),
              .active_unique_constraints = &active_unique_constraints_family_.Add(labels),
              .active_type_constraints = &active_type_constraints_family_.Add(labels),
              .streams_created = &streams_created_family_.Add(labels),
              .messages_consumed = &messages_consumed_family_.Add(labels),
              .triggers_created = &triggers_created_family_.Add(labels),
              .triggers_executed = &triggers_executed_family_.Add(labels),
              .active_transactions = &active_transactions_family_.Add(labels),
              .committed_transactions = &committed_transactions_family_.Add(labels),
              .rolled_back_transactions = &rolled_back_transactions_family_.Add(labels),
              .failed_query = &failed_query_family_.Add(labels),
              .failed_prepare = &failed_prepare_family_.Add(labels),
              .failed_pull = &failed_pull_family_.Add(labels),
              .successful_query = &successful_query_family_.Add(labels),
              .write_write_conflicts = &write_write_conflicts_family_.Add(labels),
              .transient_errors = &transient_errors_family_.Add(labels),
              .unreleased_delta_objects = &unreleased_delta_objects_family_.Add(labels),
              .read_query = &read_query_family_.Add(labels),
              .write_query = &write_query_family_.Add(labels),
              .read_write_query = &read_write_query_family_.Add(labels),
              .deleted_nodes = &deleted_nodes_family_.Add(labels),
              .deleted_edges = &deleted_edges_family_.Add(labels),
              .show_schema = &show_schema_family_.Add(labels),
              .query_execution_latency_seconds = &query_execution_latency_family_.Add(labels, kLatencyBuckets),
              .snapshot_creation_latency_seconds = &snapshot_creation_latency_family_.Add(labels, kLatencyBuckets),
              .snapshot_recovery_latency_seconds = &snapshot_recovery_latency_family_.Add(labels, kLatencyBuckets),
              .gc_latency_seconds = &gc_latency_family_.Add(labels, kLatencyBuckets),
              .gc_skiplist_cleanup_latency_seconds = &gc_skiplist_cleanup_latency_family_.Add(labels, kLatencyBuckets),
          },
  });
  return &databases_.back().handles;
}

void PrometheusMetrics::RemoveDatabase(DatabaseMetricHandles const *handles) {
  std::lock_guard const lock{databases_mutex_};
  auto it = std::ranges::find_if(databases_, [handles](auto const &e) { return &e.handles == handles; });
  MG_ASSERT(it != databases_.end(), "Attempted to remove unregistered database from PrometheusMetrics");
  if (--it->ref_count > 0) return;
  auto &h = it->handles;
  vertex_count_family_.Remove(h.vertex_count);
  edge_count_family_.Remove(h.edge_count);
  disk_usage_family_.Remove(h.disk_usage_bytes);
  memory_res_family_.Remove(h.memory_res_bytes);
  once_operator_family_.Remove(h.once_operator);
  create_node_operator_family_.Remove(h.create_node_operator);
  create_expand_operator_family_.Remove(h.create_expand_operator);
  scan_all_operator_family_.Remove(h.scan_all_operator);
  scan_all_by_label_operator_family_.Remove(h.scan_all_by_label_operator);
  scan_all_by_label_properties_operator_family_.Remove(h.scan_all_by_label_properties_operator);
  scan_all_by_id_operator_family_.Remove(h.scan_all_by_id_operator);
  scan_all_by_edge_operator_family_.Remove(h.scan_all_by_edge_operator);
  scan_all_by_edge_type_operator_family_.Remove(h.scan_all_by_edge_type_operator);
  scan_all_by_edge_type_property_operator_family_.Remove(h.scan_all_by_edge_type_property_operator);
  scan_all_by_edge_type_property_value_operator_family_.Remove(h.scan_all_by_edge_type_property_value_operator);
  scan_all_by_edge_type_property_range_operator_family_.Remove(h.scan_all_by_edge_type_property_range_operator);
  scan_all_by_edge_property_operator_family_.Remove(h.scan_all_by_edge_property_operator);
  scan_all_by_edge_property_value_operator_family_.Remove(h.scan_all_by_edge_property_value_operator);
  scan_all_by_edge_property_range_operator_family_.Remove(h.scan_all_by_edge_property_range_operator);
  scan_all_by_edge_id_operator_family_.Remove(h.scan_all_by_edge_id_operator);
  scan_all_by_point_distance_operator_family_.Remove(h.scan_all_by_point_distance_operator);
  scan_all_by_point_withinbbox_operator_family_.Remove(h.scan_all_by_point_withinbbox_operator);
  expand_operator_family_.Remove(h.expand_operator);
  expand_variable_operator_family_.Remove(h.expand_variable_operator);
  construct_named_path_operator_family_.Remove(h.construct_named_path_operator);
  filter_operator_family_.Remove(h.filter_operator);
  produce_operator_family_.Remove(h.produce_operator);
  delete_operator_family_.Remove(h.delete_operator);
  set_property_operator_family_.Remove(h.set_property_operator);
  set_properties_operator_family_.Remove(h.set_properties_operator);
  set_labels_operator_family_.Remove(h.set_labels_operator);
  remove_property_operator_family_.Remove(h.remove_property_operator);
  remove_labels_operator_family_.Remove(h.remove_labels_operator);
  edge_uniqueness_filter_operator_family_.Remove(h.edge_uniqueness_filter_operator);
  empty_result_operator_family_.Remove(h.empty_result_operator);
  accumulate_operator_family_.Remove(h.accumulate_operator);
  aggregate_operator_family_.Remove(h.aggregate_operator);
  skip_operator_family_.Remove(h.skip_operator);
  limit_operator_family_.Remove(h.limit_operator);
  order_by_operator_family_.Remove(h.order_by_operator);
  merge_operator_family_.Remove(h.merge_operator);
  optional_operator_family_.Remove(h.optional_operator);
  unwind_operator_family_.Remove(h.unwind_operator);
  distinct_operator_family_.Remove(h.distinct_operator);
  union_operator_family_.Remove(h.union_operator);
  cartesian_operator_family_.Remove(h.cartesian_operator);
  call_procedure_operator_family_.Remove(h.call_procedure_operator);
  foreach_operator_family_.Remove(h.foreach_operator);
  evaluate_pattern_filter_operator_family_.Remove(h.evaluate_pattern_filter_operator);
  apply_operator_family_.Remove(h.apply_operator);
  indexed_join_operator_family_.Remove(h.indexed_join_operator);
  hash_join_operator_family_.Remove(h.hash_join_operator);
  roll_up_apply_operator_family_.Remove(h.roll_up_apply_operator);
  periodic_commit_operator_family_.Remove(h.periodic_commit_operator);
  periodic_subquery_operator_family_.Remove(h.periodic_subquery_operator);
  set_nested_property_operator_family_.Remove(h.set_nested_property_operator);
  remove_nested_property_operator_family_.Remove(h.remove_nested_property_operator);
  active_label_indices_family_.Remove(h.active_label_indices);
  active_label_property_indices_family_.Remove(h.active_label_property_indices);
  active_edge_type_indices_family_.Remove(h.active_edge_type_indices);
  active_edge_type_property_indices_family_.Remove(h.active_edge_type_property_indices);
  active_edge_property_indices_family_.Remove(h.active_edge_property_indices);
  active_point_indices_family_.Remove(h.active_point_indices);
  active_text_indices_family_.Remove(h.active_text_indices);
  active_text_edge_indices_family_.Remove(h.active_text_edge_indices);
  active_vector_indices_family_.Remove(h.active_vector_indices);
  active_vector_edge_indices_family_.Remove(h.active_vector_edge_indices);
  active_existence_constraints_family_.Remove(h.active_existence_constraints);
  active_unique_constraints_family_.Remove(h.active_unique_constraints);
  active_type_constraints_family_.Remove(h.active_type_constraints);
  streams_created_family_.Remove(h.streams_created);
  messages_consumed_family_.Remove(h.messages_consumed);
  triggers_created_family_.Remove(h.triggers_created);
  triggers_executed_family_.Remove(h.triggers_executed);
  active_transactions_family_.Remove(h.active_transactions);
  committed_transactions_family_.Remove(h.committed_transactions);
  rolled_back_transactions_family_.Remove(h.rolled_back_transactions);
  failed_query_family_.Remove(h.failed_query);
  failed_prepare_family_.Remove(h.failed_prepare);
  failed_pull_family_.Remove(h.failed_pull);
  successful_query_family_.Remove(h.successful_query);
  write_write_conflicts_family_.Remove(h.write_write_conflicts);
  transient_errors_family_.Remove(h.transient_errors);
  unreleased_delta_objects_family_.Remove(h.unreleased_delta_objects);
  read_query_family_.Remove(h.read_query);
  write_query_family_.Remove(h.write_query);
  read_write_query_family_.Remove(h.read_write_query);
  deleted_nodes_family_.Remove(h.deleted_nodes);
  deleted_edges_family_.Remove(h.deleted_edges);
  show_schema_family_.Remove(h.show_schema);
  query_execution_latency_family_.Remove(h.query_execution_latency_seconds);
  snapshot_creation_latency_family_.Remove(h.snapshot_creation_latency_seconds);
  snapshot_recovery_latency_family_.Remove(h.snapshot_recovery_latency_seconds);
  gc_latency_family_.Remove(h.gc_latency_seconds);
  gc_skiplist_cleanup_latency_family_.Remove(h.gc_skiplist_cleanup_latency_seconds);
  databases_.erase(it);
}

void PrometheusMetrics::UpdateGauges() {
  std::vector<std::string> names;
  {
    std::shared_lock const lock{databases_mutex_};
    names.reserve(databases_.size());
    r::transform(databases_, std::back_inserter(names), &DatabaseEntry::db_name);
  }

  auto const snaps = names | rv::transform([&](auto const &name) { return ResolveStorageSnapshot(name); }) |
                     r::to<std::vector<StorageSnapshot>>();

  {
    std::shared_lock const lock{databases_mutex_};
    for (size_t i = 0; i < names.size(); ++i) {
      auto it = std::ranges::find_if(databases_, [&](auto const &e) { return e.db_name == names[i]; });
      if (it == databases_.end()) continue;
      auto const &snapshot = snaps[i];
      it->handles.vertex_count->Set(static_cast<double>(snapshot.vertex_count));
      it->handles.edge_count->Set(static_cast<double>(snapshot.edge_count));
      it->handles.disk_usage_bytes->Set(static_cast<double>(snapshot.disk_usage));
      it->handles.memory_res_bytes->Set(static_cast<double>(snapshot.memory_res));
    }
  }

#ifdef MG_ENTERPRISE
  std::vector<coordination::InstanceStatus> instances;
  {
    std::lock_guard const lock{instance_resolver_mutex_};
    if (instance_status_resolver_) instances = instance_status_resolver_();
  }

  // Remove gauges for instances no longer present
  auto const active_names = instances | rv::transform(&coordination::InstanceStatus::instance_name) |
                            r::to<std::unordered_set<std::string>>();
  for (auto it = instance_up_gauges_.begin(); it != instance_up_gauges_.end();) {
    if (!active_names.contains(it->first)) {
      instance_up_family_.Remove(it->second);
      instance_is_leader_family_.Remove(instance_is_leader_gauges_.at(it->first));
      instance_is_main_family_.Remove(instance_is_main_gauges_.at(it->first));
      instance_last_response_seconds_family_.Remove(instance_last_response_seconds_gauges_.at(it->first));
      instance_is_leader_gauges_.erase(it->first);
      instance_is_main_gauges_.erase(it->first);
      instance_last_response_seconds_gauges_.erase(it->first);
      it = instance_up_gauges_.erase(it);
    } else {
      ++it;
    }
  }

  // Add or update gauges for current instances
  for (auto const &inst : instances) {
    prometheus::Labels const labels{{"mg_instance", inst.instance_name}};
    if (!instance_up_gauges_.contains(inst.instance_name)) {
      instance_up_gauges_.emplace(inst.instance_name, &instance_up_family_.Add(labels));
      instance_is_leader_gauges_.emplace(inst.instance_name, &instance_is_leader_family_.Add(labels));
      instance_is_main_gauges_.emplace(inst.instance_name, &instance_is_main_family_.Add(labels));
      instance_last_response_seconds_gauges_.emplace(inst.instance_name,
                                                     &instance_last_response_seconds_family_.Add(labels));
    }
    instance_up_gauges_.at(inst.instance_name)->Set(inst.health == "up" ? 1.0 : 0.0);
    instance_is_leader_gauges_.at(inst.instance_name)->Set(inst.cluster_role == "leader" ? 1.0 : 0.0);
    instance_is_main_gauges_.at(inst.instance_name)->Set(inst.cluster_role == "main" ? 1.0 : 0.0);
    instance_last_response_seconds_gauges_.at(inst.instance_name)
        ->Set(static_cast<double>(inst.last_succ_resp_ms) / 1000.0);
  }
#endif
}

namespace {

// Compute percentile from a prometheus histogram's cumulative bucket data.
// Returns 0 if there are no observations.
double HistogramPercentile(prometheus::Histogram const &h, double quantile) {
  auto const cm = h.Collect();
  auto const &hdata = cm.histogram;
  if (hdata.sample_count == 0) return 0.0;

  double const target = quantile * static_cast<double>(hdata.sample_count);
  double prev_bound = 0.0;
  uint64_t prev_count = 0;
  for (auto const &bucket : hdata.bucket) {
    if (static_cast<double>(bucket.cumulative_count) >= target) {
      if (bucket.cumulative_count == prev_count) return prev_bound;
      double const frac =
          (target - static_cast<double>(prev_count)) / static_cast<double>(bucket.cumulative_count - prev_count);
      return prev_bound + frac * (bucket.upper_bound - prev_bound);
    }
    prev_bound = bucket.upper_bound;
    prev_count = bucket.cumulative_count;
  }
  return prev_bound;
}

void AppendHistogramPercentiles(std::vector<MetricInfo> &out, std::string const &name, std::string const &type,
                                prometheus::Histogram const &h) {
  // Histograms are stored in seconds; multiply by 1e6 to emit microseconds in SHOW METRICS INFO output.
  for (auto const [quantile, label] :
       {std::pair{0.50, "_us_50p"}, std::pair{0.90, "_us_90p"}, std::pair{0.99, "_us_99p"}}) {
    out.push_back({name + label, type, "Histogram", HistogramPercentile(h, quantile) * 1e6});
  }
}

// Compute percentile from merged cumulative bucket counts (same boundaries across all histograms).
double MergedHistogramPercentile(std::vector<prometheus::ClientMetric::Histogram> const &hdatas, double quantile) {
  if (hdatas.empty()) return 0.0;

  uint64_t total_count = 0;
  for (auto const &hd : hdatas) total_count += hd.sample_count;
  if (total_count == 0) return 0.0;

  std::size_t const nbuckets = hdatas[0].bucket.size();
  double const target = quantile * static_cast<double>(total_count);
  double prev_bound = 0.0;
  uint64_t prev_cumulative = 0;
  for (std::size_t i = 0; i < nbuckets; ++i) {
    uint64_t cumulative = 0;
    for (auto const &hd : hdatas) cumulative += hd.bucket[i].cumulative_count;
    double const upper = hdatas[0].bucket[i].upper_bound;
    if (static_cast<double>(cumulative) >= target) {
      if (cumulative == prev_cumulative) return prev_bound;
      double const frac =
          (target - static_cast<double>(prev_cumulative)) / static_cast<double>(cumulative - prev_cumulative);
      return prev_bound + frac * (upper - prev_bound);
    }
    prev_bound = upper;
    prev_cumulative = cumulative;
  }
  return prev_bound;
}

void AppendMergedHistogramPercentiles(std::vector<MetricInfo> &out, std::string const &name, std::string const &type,
                                      std::vector<prometheus::ClientMetric::Histogram> const &hdatas) {
  // Histograms are stored in seconds; multiply by 1e6 to emit microseconds in SHOW METRICS INFO output.
  for (auto const [quantile, label] :
       {std::pair{0.50, "_us_50p"}, std::pair{0.90, "_us_90p"}, std::pair{0.99, "_us_99p"}}) {
    out.push_back({name + label, type, "Histogram", MergedHistogramPercentile(hdatas, quantile) * 1e6});
  }
}

}  // namespace

std::expected<std::vector<MetricInfo>, std::string> PrometheusMetrics::GetDbMetricsInfo(
    std::string_view db_name) const {
  auto const snapshot = ResolveStorageSnapshot(db_name);
  std::shared_lock const lock{databases_mutex_};
  auto const it = std::ranges::find_if(databases_, [db_name](auto const &e) { return e.db_name == db_name; });
  if (it == databases_.end()) {
    return std::unexpected(fmt::format("Database '{}' not found in metrics registry", db_name));
  }

  auto const &h = it->handles;
  std::vector<MetricInfo> out;

  // General
  auto const vertex_count = static_cast<int64_t>(snapshot.vertex_count);
  auto const edge_count = static_cast<int64_t>(snapshot.edge_count);
  out.push_back({"VertexCount", "General", "Gauge", vertex_count});
  out.push_back({"EdgeCount", "General", "Gauge", edge_count});
  out.push_back({"AverageDegree",
                 "General",
                 "Gauge",
                 vertex_count > 0 ? 2.0 * static_cast<double>(edge_count) / static_cast<double>(vertex_count) : 0.0});

  // Memory
  out.push_back({"DiskUsage", "Memory", "Gauge", static_cast<int64_t>(snapshot.disk_usage)});
  out.push_back({"MemoryRes", "Memory", "Gauge", static_cast<int64_t>(snapshot.memory_res)});
  out.push_back(
      {"UnreleasedDeltaObjects", "Memory", "Gauge", static_cast<int64_t>(h.unreleased_delta_objects->Value())});
  AppendHistogramPercentiles(out, "GCLatency", "Memory", *h.gc_latency_seconds);
  AppendHistogramPercentiles(out, "GCSkiplistCleanupLatency", "Memory", *h.gc_skiplist_cleanup_latency_seconds);

  // Operator
  out.push_back({"OnceOperator", "Operator", "Counter", static_cast<int64_t>(h.once_operator->Value())});
  out.push_back({"CreateNodeOperator", "Operator", "Counter", static_cast<int64_t>(h.create_node_operator->Value())});
  out.push_back(
      {"CreateExpandOperator", "Operator", "Counter", static_cast<int64_t>(h.create_expand_operator->Value())});
  out.push_back({"ScanAllOperator", "Operator", "Counter", static_cast<int64_t>(h.scan_all_operator->Value())});
  out.push_back(
      {"ScanAllByLabelOperator", "Operator", "Counter", static_cast<int64_t>(h.scan_all_by_label_operator->Value())});
  out.push_back({"ScanAllByLabelPropertiesOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.scan_all_by_label_properties_operator->Value())});
  out.push_back(
      {"ScanAllByIdOperator", "Operator", "Counter", static_cast<int64_t>(h.scan_all_by_id_operator->Value())});
  out.push_back(
      {"ScanAllByEdgeOperator", "Operator", "Counter", static_cast<int64_t>(h.scan_all_by_edge_operator->Value())});
  out.push_back({"ScanAllByEdgeTypeOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.scan_all_by_edge_type_operator->Value())});
  out.push_back({"ScanAllByEdgeTypePropertyOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.scan_all_by_edge_type_property_operator->Value())});
  out.push_back({"ScanAllByEdgeTypePropertyValueOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.scan_all_by_edge_type_property_value_operator->Value())});
  out.push_back({"ScanAllByEdgeTypePropertyRangeOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.scan_all_by_edge_type_property_range_operator->Value())});
  out.push_back({"ScanAllByEdgePropertyOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.scan_all_by_edge_property_operator->Value())});
  out.push_back({"ScanAllByEdgePropertyValueOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.scan_all_by_edge_property_value_operator->Value())});
  out.push_back({"ScanAllByEdgePropertyRangeOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.scan_all_by_edge_property_range_operator->Value())});
  out.push_back({"ScanAllByEdgeIdOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.scan_all_by_edge_id_operator->Value())});
  out.push_back({"ScanAllByPointDistanceOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.scan_all_by_point_distance_operator->Value())});
  out.push_back({"ScanAllByPointWithinbboxOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.scan_all_by_point_withinbbox_operator->Value())});
  out.push_back({"ExpandOperator", "Operator", "Counter", static_cast<int64_t>(h.expand_operator->Value())});
  out.push_back(
      {"ExpandVariableOperator", "Operator", "Counter", static_cast<int64_t>(h.expand_variable_operator->Value())});
  out.push_back({"ConstructNamedPathOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.construct_named_path_operator->Value())});
  out.push_back({"FilterOperator", "Operator", "Counter", static_cast<int64_t>(h.filter_operator->Value())});
  out.push_back({"ProduceOperator", "Operator", "Counter", static_cast<int64_t>(h.produce_operator->Value())});
  out.push_back({"DeleteOperator", "Operator", "Counter", static_cast<int64_t>(h.delete_operator->Value())});
  out.push_back({"SetPropertyOperator", "Operator", "Counter", static_cast<int64_t>(h.set_property_operator->Value())});
  out.push_back(
      {"SetPropertiesOperator", "Operator", "Counter", static_cast<int64_t>(h.set_properties_operator->Value())});
  out.push_back({"SetLabelsOperator", "Operator", "Counter", static_cast<int64_t>(h.set_labels_operator->Value())});
  out.push_back(
      {"RemovePropertyOperator", "Operator", "Counter", static_cast<int64_t>(h.remove_property_operator->Value())});
  out.push_back(
      {"RemoveLabelsOperator", "Operator", "Counter", static_cast<int64_t>(h.remove_labels_operator->Value())});
  out.push_back({"EdgeUniquenessFilterOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.edge_uniqueness_filter_operator->Value())});
  out.push_back({"EmptyResultOperator", "Operator", "Counter", static_cast<int64_t>(h.empty_result_operator->Value())});
  out.push_back({"AccumulateOperator", "Operator", "Counter", static_cast<int64_t>(h.accumulate_operator->Value())});
  out.push_back({"AggregateOperator", "Operator", "Counter", static_cast<int64_t>(h.aggregate_operator->Value())});
  out.push_back({"SkipOperator", "Operator", "Counter", static_cast<int64_t>(h.skip_operator->Value())});
  out.push_back({"LimitOperator", "Operator", "Counter", static_cast<int64_t>(h.limit_operator->Value())});
  out.push_back({"OrderByOperator", "Operator", "Counter", static_cast<int64_t>(h.order_by_operator->Value())});
  out.push_back({"MergeOperator", "Operator", "Counter", static_cast<int64_t>(h.merge_operator->Value())});
  out.push_back({"OptionalOperator", "Operator", "Counter", static_cast<int64_t>(h.optional_operator->Value())});
  out.push_back({"UnwindOperator", "Operator", "Counter", static_cast<int64_t>(h.unwind_operator->Value())});
  out.push_back({"DistinctOperator", "Operator", "Counter", static_cast<int64_t>(h.distinct_operator->Value())});
  out.push_back({"UnionOperator", "Operator", "Counter", static_cast<int64_t>(h.union_operator->Value())});
  out.push_back({"CartesianOperator", "Operator", "Counter", static_cast<int64_t>(h.cartesian_operator->Value())});
  out.push_back(
      {"CallProcedureOperator", "Operator", "Counter", static_cast<int64_t>(h.call_procedure_operator->Value())});
  out.push_back({"ForeachOperator", "Operator", "Counter", static_cast<int64_t>(h.foreach_operator->Value())});
  out.push_back({"EvaluatePatternFilterOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.evaluate_pattern_filter_operator->Value())});
  out.push_back({"ApplyOperator", "Operator", "Counter", static_cast<int64_t>(h.apply_operator->Value())});
  out.push_back({"IndexedJoinOperator", "Operator", "Counter", static_cast<int64_t>(h.indexed_join_operator->Value())});
  out.push_back({"HashJoinOperator", "Operator", "Counter", static_cast<int64_t>(h.hash_join_operator->Value())});
  out.push_back(
      {"RollUpApplyOperator", "Operator", "Counter", static_cast<int64_t>(h.roll_up_apply_operator->Value())});
  out.push_back(
      {"PeriodicCommitOperator", "Operator", "Counter", static_cast<int64_t>(h.periodic_commit_operator->Value())});
  out.push_back(
      {"PeriodicSubqueryOperator", "Operator", "Counter", static_cast<int64_t>(h.periodic_subquery_operator->Value())});
  out.push_back({"SetNestedPropertyOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.set_nested_property_operator->Value())});
  out.push_back({"RemoveNestedPropertyOperator",
                 "Operator",
                 "Counter",
                 static_cast<int64_t>(h.remove_nested_property_operator->Value())});

  // Index
  out.push_back({"ActiveLabelIndices", "Index", "Gauge", static_cast<int64_t>(h.active_label_indices->Value())});
  out.push_back(
      {"ActiveLabelPropertyIndices", "Index", "Gauge", static_cast<int64_t>(h.active_label_property_indices->Value())});
  out.push_back({"ActiveEdgeTypeIndices", "Index", "Gauge", static_cast<int64_t>(h.active_edge_type_indices->Value())});
  out.push_back({"ActiveEdgeTypePropertyIndices",
                 "Index",
                 "Gauge",
                 static_cast<int64_t>(h.active_edge_type_property_indices->Value())});
  out.push_back(
      {"ActiveEdgePropertyIndices", "Index", "Gauge", static_cast<int64_t>(h.active_edge_property_indices->Value())});
  out.push_back({"ActivePointIndices", "Index", "Gauge", static_cast<int64_t>(h.active_point_indices->Value())});
  out.push_back({"ActiveTextIndices", "Index", "Gauge", static_cast<int64_t>(h.active_text_indices->Value())});
  out.push_back({"ActiveTextEdgeIndices", "Index", "Gauge", static_cast<int64_t>(h.active_text_edge_indices->Value())});
  out.push_back({"ActiveVectorIndices", "Index", "Gauge", static_cast<int64_t>(h.active_vector_indices->Value())});
  out.push_back(
      {"ActiveVectorEdgeIndices", "Index", "Gauge", static_cast<int64_t>(h.active_vector_edge_indices->Value())});

  // Constraint
  out.push_back({"ActiveExistenceConstraints",
                 "Constraint",
                 "Gauge",
                 static_cast<int64_t>(h.active_existence_constraints->Value())});
  out.push_back(
      {"ActiveUniqueConstraints", "Constraint", "Gauge", static_cast<int64_t>(h.active_unique_constraints->Value())});
  out.push_back(
      {"ActiveTypeConstraints", "Constraint", "Gauge", static_cast<int64_t>(h.active_type_constraints->Value())});

  // Stream
  out.push_back({"StreamsCreated", "Stream", "Counter", static_cast<int64_t>(h.streams_created->Value())});
  out.push_back({"MessagesConsumed", "Stream", "Counter", static_cast<int64_t>(h.messages_consumed->Value())});

  // Trigger
  out.push_back({"TriggersCreated", "Trigger", "Counter", static_cast<int64_t>(h.triggers_created->Value())});
  out.push_back({"TriggersExecuted", "Trigger", "Counter", static_cast<int64_t>(h.triggers_executed->Value())});

  // Transaction
  out.push_back({"ActiveTransactions", "Transaction", "Gauge", static_cast<int64_t>(h.active_transactions->Value())});
  out.push_back(
      {"CommitedTransactions", "Transaction", "Counter", static_cast<int64_t>(h.committed_transactions->Value())});
  out.push_back(
      {"RolledBackTransactions", "Transaction", "Counter", static_cast<int64_t>(h.rolled_back_transactions->Value())});
  out.push_back({"FailedQuery", "Transaction", "Counter", static_cast<int64_t>(h.failed_query->Value())});
  out.push_back({"FailedPrepare", "Transaction", "Counter", static_cast<int64_t>(h.failed_prepare->Value())});
  out.push_back({"FailedPull", "Transaction", "Counter", static_cast<int64_t>(h.failed_pull->Value())});
  out.push_back({"SuccessfulQuery", "Transaction", "Counter", static_cast<int64_t>(h.successful_query->Value())});
  out.push_back(
      {"WriteWriteConflicts", "Transaction", "Counter", static_cast<int64_t>(h.write_write_conflicts->Value())});
  out.push_back({"TransientErrors", "Transaction", "Counter", static_cast<int64_t>(h.transient_errors->Value())});

  // QueryType
  out.push_back({"ReadQuery", "QueryType", "Counter", static_cast<int64_t>(h.read_query->Value())});
  out.push_back({"WriteQuery", "QueryType", "Counter", static_cast<int64_t>(h.write_query->Value())});
  out.push_back({"ReadWriteQuery", "QueryType", "Counter", static_cast<int64_t>(h.read_write_query->Value())});

  // TTL
  out.push_back({"DeletedNodes", "TTL", "Counter", static_cast<int64_t>(h.deleted_nodes->Value())});
  out.push_back({"DeletedEdges", "TTL", "Counter", static_cast<int64_t>(h.deleted_edges->Value())});

  // SchemaInfo
  out.push_back({"ShowSchema", "SchemaInfo", "Counter", static_cast<int64_t>(h.show_schema->Value())});

  // Query
  AppendHistogramPercentiles(out, "QueryExecutionLatency", "Query", *h.query_execution_latency_seconds);

  // Snapshot
  AppendHistogramPercentiles(out, "SnapshotCreationLatency", "Snapshot", *h.snapshot_creation_latency_seconds);
  AppendHistogramPercentiles(out, "SnapshotRecoveryLatency", "Snapshot", *h.snapshot_recovery_latency_seconds);

  return out;
}

std::vector<MetricInfo> PrometheusMetrics::GetGlobalMetricsInfo() const {
  std::vector<std::string> db_names;
  {
    std::shared_lock const lock{databases_mutex_};
    db_names.reserve(databases_.size());
    for (auto const &entry : databases_) db_names.push_back(entry.db_name);
  }
  std::unordered_map<std::string, StorageSnapshot> snap_by_db;
  snap_by_db.reserve(db_names.size());
  for (auto const &n : db_names) {
    snap_by_db.emplace(n, ResolveStorageSnapshot(n));
  }

  std::shared_lock const lock{databases_mutex_};
  auto const &g = global;
  std::vector<MetricInfo> out;

  // General — aggregate across all databases
  int64_t total_vertex_count = 0;
  int64_t total_edge_count = 0;
  for (auto const &entry : databases_) {
    auto const snap_it = snap_by_db.find(entry.db_name);
    auto const snapshot = snap_it != snap_by_db.end() ? snap_it->second : ResolveStorageSnapshot(entry.db_name);
    total_vertex_count += static_cast<int64_t>(snapshot.vertex_count);
    total_edge_count += static_cast<int64_t>(snapshot.edge_count);
  }
  out.push_back({"VertexCount", "General", "Gauge", total_vertex_count});
  out.push_back({"EdgeCount", "General", "Gauge", total_edge_count});
  out.push_back({"AverageDegree",
                 "General",
                 "Gauge",
                 total_vertex_count > 0
                     ? 2.0 * static_cast<double>(total_edge_count) / static_cast<double>(total_vertex_count)
                     : 0.0});

  // Memory — aggregate across all databases + global peak
  int64_t total_disk_usage = 0;
  int64_t total_memory_res = 0;
  int64_t total_unreleased_deltas = 0;
  for (auto const &entry : databases_) {
    auto const snap_it = snap_by_db.find(entry.db_name);
    auto const snapshot = snap_it != snap_by_db.end() ? snap_it->second : ResolveStorageSnapshot(entry.db_name);
    total_disk_usage += static_cast<int64_t>(snapshot.disk_usage);
    total_memory_res += static_cast<int64_t>(snapshot.memory_res);
    total_unreleased_deltas += static_cast<int64_t>(entry.handles.unreleased_delta_objects->Value());
  }
  out.push_back({"DiskUsage", "Memory", "Gauge", total_disk_usage});
  out.push_back({"MemoryRes", "Memory", "Gauge", total_memory_res});
  out.push_back({"PeakMemoryRes", "Memory", "Gauge", static_cast<int64_t>(g.peak_memory_res_bytes->Value())});
  out.push_back({"UnreleasedDeltaObjects", "Memory", "Gauge", total_unreleased_deltas});

  // Aggregate per-db counters/gauges
  int64_t total_once_operator = 0, total_create_node_operator = 0, total_create_expand_operator = 0;
  int64_t total_scan_all_operator = 0, total_scan_all_by_label_operator = 0;
  int64_t total_scan_all_by_label_properties_operator = 0, total_scan_all_by_id_operator = 0;
  int64_t total_scan_all_by_edge_operator = 0, total_scan_all_by_edge_type_operator = 0;
  int64_t total_scan_all_by_edge_type_property_operator = 0;
  int64_t total_scan_all_by_edge_type_property_value_operator = 0;
  int64_t total_scan_all_by_edge_type_property_range_operator = 0;
  int64_t total_scan_all_by_edge_property_operator = 0;
  int64_t total_scan_all_by_edge_property_value_operator = 0;
  int64_t total_scan_all_by_edge_property_range_operator = 0;
  int64_t total_scan_all_by_edge_id_operator = 0;
  int64_t total_scan_all_by_point_distance_operator = 0, total_scan_all_by_point_withinbbox_operator = 0;
  int64_t total_expand_operator = 0, total_expand_variable_operator = 0;
  int64_t total_construct_named_path_operator = 0, total_filter_operator = 0, total_produce_operator = 0;
  int64_t total_delete_operator = 0, total_set_property_operator = 0, total_set_properties_operator = 0;
  int64_t total_set_labels_operator = 0, total_remove_property_operator = 0, total_remove_labels_operator = 0;
  int64_t total_edge_uniqueness_filter_operator = 0, total_empty_result_operator = 0;
  int64_t total_accumulate_operator = 0, total_aggregate_operator = 0;
  int64_t total_skip_operator = 0, total_limit_operator = 0, total_order_by_operator = 0;
  int64_t total_merge_operator = 0, total_optional_operator = 0, total_unwind_operator = 0;
  int64_t total_distinct_operator = 0, total_union_operator = 0, total_cartesian_operator = 0;
  int64_t total_call_procedure_operator = 0, total_foreach_operator = 0;
  int64_t total_evaluate_pattern_filter_operator = 0, total_apply_operator = 0;
  int64_t total_indexed_join_operator = 0, total_hash_join_operator = 0, total_roll_up_apply_operator = 0;
  int64_t total_periodic_commit_operator = 0, total_periodic_subquery_operator = 0;
  int64_t total_set_nested_property_operator = 0, total_remove_nested_property_operator = 0;
  int64_t total_active_label_indices = 0, total_active_label_property_indices = 0;
  int64_t total_active_edge_type_indices = 0, total_active_edge_type_property_indices = 0;
  int64_t total_active_edge_property_indices = 0, total_active_point_indices = 0;
  int64_t total_active_text_indices = 0, total_active_text_edge_indices = 0;
  int64_t total_active_vector_indices = 0, total_active_vector_edge_indices = 0;
  int64_t total_active_existence_constraints = 0, total_active_unique_constraints = 0;
  int64_t total_active_type_constraints = 0;
  int64_t total_streams_created = 0, total_messages_consumed = 0;
  int64_t total_triggers_created = 0, total_triggers_executed = 0;
  int64_t total_active_transactions = 0, total_committed_transactions = 0;
  int64_t total_rolled_back_transactions = 0, total_failed_query = 0;
  int64_t total_failed_prepare = 0, total_failed_pull = 0, total_successful_query = 0;
  int64_t total_write_write_conflicts = 0, total_transient_errors = 0;
  int64_t total_read_query = 0, total_write_query = 0, total_read_write_query = 0;
  int64_t total_deleted_nodes = 0, total_deleted_edges = 0;
  int64_t total_show_schema = 0;
  std::vector<prometheus::ClientMetric::Histogram> query_exec_hdatas;
  std::vector<prometheus::ClientMetric::Histogram> snapshot_creation_hdatas;
  std::vector<prometheus::ClientMetric::Histogram> snapshot_recovery_hdatas;
  std::vector<prometheus::ClientMetric::Histogram> gc_hdatas;
  std::vector<prometheus::ClientMetric::Histogram> gc_skiplist_hdatas;

  for (auto const &entry : databases_) {
    auto const &h = entry.handles;
    total_once_operator += static_cast<int64_t>(h.once_operator->Value());
    total_create_node_operator += static_cast<int64_t>(h.create_node_operator->Value());
    total_create_expand_operator += static_cast<int64_t>(h.create_expand_operator->Value());
    total_scan_all_operator += static_cast<int64_t>(h.scan_all_operator->Value());
    total_scan_all_by_label_operator += static_cast<int64_t>(h.scan_all_by_label_operator->Value());
    total_scan_all_by_label_properties_operator +=
        static_cast<int64_t>(h.scan_all_by_label_properties_operator->Value());
    total_scan_all_by_id_operator += static_cast<int64_t>(h.scan_all_by_id_operator->Value());
    total_scan_all_by_edge_operator += static_cast<int64_t>(h.scan_all_by_edge_operator->Value());
    total_scan_all_by_edge_type_operator += static_cast<int64_t>(h.scan_all_by_edge_type_operator->Value());
    total_scan_all_by_edge_type_property_operator +=
        static_cast<int64_t>(h.scan_all_by_edge_type_property_operator->Value());
    total_scan_all_by_edge_type_property_value_operator +=
        static_cast<int64_t>(h.scan_all_by_edge_type_property_value_operator->Value());
    total_scan_all_by_edge_type_property_range_operator +=
        static_cast<int64_t>(h.scan_all_by_edge_type_property_range_operator->Value());
    total_scan_all_by_edge_property_operator += static_cast<int64_t>(h.scan_all_by_edge_property_operator->Value());
    total_scan_all_by_edge_property_value_operator +=
        static_cast<int64_t>(h.scan_all_by_edge_property_value_operator->Value());
    total_scan_all_by_edge_property_range_operator +=
        static_cast<int64_t>(h.scan_all_by_edge_property_range_operator->Value());
    total_scan_all_by_edge_id_operator += static_cast<int64_t>(h.scan_all_by_edge_id_operator->Value());
    total_scan_all_by_point_distance_operator += static_cast<int64_t>(h.scan_all_by_point_distance_operator->Value());
    total_scan_all_by_point_withinbbox_operator +=
        static_cast<int64_t>(h.scan_all_by_point_withinbbox_operator->Value());
    total_expand_operator += static_cast<int64_t>(h.expand_operator->Value());
    total_expand_variable_operator += static_cast<int64_t>(h.expand_variable_operator->Value());
    total_construct_named_path_operator += static_cast<int64_t>(h.construct_named_path_operator->Value());
    total_filter_operator += static_cast<int64_t>(h.filter_operator->Value());
    total_produce_operator += static_cast<int64_t>(h.produce_operator->Value());
    total_delete_operator += static_cast<int64_t>(h.delete_operator->Value());
    total_set_property_operator += static_cast<int64_t>(h.set_property_operator->Value());
    total_set_properties_operator += static_cast<int64_t>(h.set_properties_operator->Value());
    total_set_labels_operator += static_cast<int64_t>(h.set_labels_operator->Value());
    total_remove_property_operator += static_cast<int64_t>(h.remove_property_operator->Value());
    total_remove_labels_operator += static_cast<int64_t>(h.remove_labels_operator->Value());
    total_edge_uniqueness_filter_operator += static_cast<int64_t>(h.edge_uniqueness_filter_operator->Value());
    total_empty_result_operator += static_cast<int64_t>(h.empty_result_operator->Value());
    total_accumulate_operator += static_cast<int64_t>(h.accumulate_operator->Value());
    total_aggregate_operator += static_cast<int64_t>(h.aggregate_operator->Value());
    total_skip_operator += static_cast<int64_t>(h.skip_operator->Value());
    total_limit_operator += static_cast<int64_t>(h.limit_operator->Value());
    total_order_by_operator += static_cast<int64_t>(h.order_by_operator->Value());
    total_merge_operator += static_cast<int64_t>(h.merge_operator->Value());
    total_optional_operator += static_cast<int64_t>(h.optional_operator->Value());
    total_unwind_operator += static_cast<int64_t>(h.unwind_operator->Value());
    total_distinct_operator += static_cast<int64_t>(h.distinct_operator->Value());
    total_union_operator += static_cast<int64_t>(h.union_operator->Value());
    total_cartesian_operator += static_cast<int64_t>(h.cartesian_operator->Value());
    total_call_procedure_operator += static_cast<int64_t>(h.call_procedure_operator->Value());
    total_foreach_operator += static_cast<int64_t>(h.foreach_operator->Value());
    total_evaluate_pattern_filter_operator += static_cast<int64_t>(h.evaluate_pattern_filter_operator->Value());
    total_apply_operator += static_cast<int64_t>(h.apply_operator->Value());
    total_indexed_join_operator += static_cast<int64_t>(h.indexed_join_operator->Value());
    total_hash_join_operator += static_cast<int64_t>(h.hash_join_operator->Value());
    total_roll_up_apply_operator += static_cast<int64_t>(h.roll_up_apply_operator->Value());
    total_periodic_commit_operator += static_cast<int64_t>(h.periodic_commit_operator->Value());
    total_periodic_subquery_operator += static_cast<int64_t>(h.periodic_subquery_operator->Value());
    total_set_nested_property_operator += static_cast<int64_t>(h.set_nested_property_operator->Value());
    total_remove_nested_property_operator += static_cast<int64_t>(h.remove_nested_property_operator->Value());
    total_active_label_indices += static_cast<int64_t>(h.active_label_indices->Value());
    total_active_label_property_indices += static_cast<int64_t>(h.active_label_property_indices->Value());
    total_active_edge_type_indices += static_cast<int64_t>(h.active_edge_type_indices->Value());
    total_active_edge_type_property_indices += static_cast<int64_t>(h.active_edge_type_property_indices->Value());
    total_active_edge_property_indices += static_cast<int64_t>(h.active_edge_property_indices->Value());
    total_active_point_indices += static_cast<int64_t>(h.active_point_indices->Value());
    total_active_text_indices += static_cast<int64_t>(h.active_text_indices->Value());
    total_active_text_edge_indices += static_cast<int64_t>(h.active_text_edge_indices->Value());
    total_active_vector_indices += static_cast<int64_t>(h.active_vector_indices->Value());
    total_active_vector_edge_indices += static_cast<int64_t>(h.active_vector_edge_indices->Value());
    total_active_existence_constraints += static_cast<int64_t>(h.active_existence_constraints->Value());
    total_active_unique_constraints += static_cast<int64_t>(h.active_unique_constraints->Value());
    total_active_type_constraints += static_cast<int64_t>(h.active_type_constraints->Value());
    total_streams_created += static_cast<int64_t>(h.streams_created->Value());
    total_messages_consumed += static_cast<int64_t>(h.messages_consumed->Value());
    total_triggers_created += static_cast<int64_t>(h.triggers_created->Value());
    total_triggers_executed += static_cast<int64_t>(h.triggers_executed->Value());
    total_active_transactions += static_cast<int64_t>(h.active_transactions->Value());
    total_committed_transactions += static_cast<int64_t>(h.committed_transactions->Value());
    total_rolled_back_transactions += static_cast<int64_t>(h.rolled_back_transactions->Value());
    total_failed_query += static_cast<int64_t>(h.failed_query->Value());
    total_failed_prepare += static_cast<int64_t>(h.failed_prepare->Value());
    total_failed_pull += static_cast<int64_t>(h.failed_pull->Value());
    total_successful_query += static_cast<int64_t>(h.successful_query->Value());
    total_write_write_conflicts += static_cast<int64_t>(h.write_write_conflicts->Value());
    total_transient_errors += static_cast<int64_t>(h.transient_errors->Value());
    total_read_query += static_cast<int64_t>(h.read_query->Value());
    total_write_query += static_cast<int64_t>(h.write_query->Value());
    total_read_write_query += static_cast<int64_t>(h.read_write_query->Value());
    total_deleted_nodes += static_cast<int64_t>(h.deleted_nodes->Value());
    total_deleted_edges += static_cast<int64_t>(h.deleted_edges->Value());
    total_show_schema += static_cast<int64_t>(h.show_schema->Value());
    query_exec_hdatas.push_back(h.query_execution_latency_seconds->Collect().histogram);
    snapshot_creation_hdatas.push_back(h.snapshot_creation_latency_seconds->Collect().histogram);
    snapshot_recovery_hdatas.push_back(h.snapshot_recovery_latency_seconds->Collect().histogram);
    gc_hdatas.push_back(h.gc_latency_seconds->Collect().histogram);
    gc_skiplist_hdatas.push_back(h.gc_skiplist_cleanup_latency_seconds->Collect().histogram);
  }

  // Operator
  out.push_back({"OnceOperator", "Operator", "Counter", total_once_operator});
  out.push_back({"CreateNodeOperator", "Operator", "Counter", total_create_node_operator});
  out.push_back({"CreateExpandOperator", "Operator", "Counter", total_create_expand_operator});
  out.push_back({"ScanAllOperator", "Operator", "Counter", total_scan_all_operator});
  out.push_back({"ScanAllByLabelOperator", "Operator", "Counter", total_scan_all_by_label_operator});
  out.push_back(
      {"ScanAllByLabelPropertiesOperator", "Operator", "Counter", total_scan_all_by_label_properties_operator});
  out.push_back({"ScanAllByIdOperator", "Operator", "Counter", total_scan_all_by_id_operator});
  out.push_back({"ScanAllByEdgeOperator", "Operator", "Counter", total_scan_all_by_edge_operator});
  out.push_back({"ScanAllByEdgeTypeOperator", "Operator", "Counter", total_scan_all_by_edge_type_operator});
  out.push_back(
      {"ScanAllByEdgeTypePropertyOperator", "Operator", "Counter", total_scan_all_by_edge_type_property_operator});
  out.push_back({"ScanAllByEdgeTypePropertyValueOperator",
                 "Operator",
                 "Counter",
                 total_scan_all_by_edge_type_property_value_operator});
  out.push_back({"ScanAllByEdgeTypePropertyRangeOperator",
                 "Operator",
                 "Counter",
                 total_scan_all_by_edge_type_property_range_operator});
  out.push_back({"ScanAllByEdgePropertyOperator", "Operator", "Counter", total_scan_all_by_edge_property_operator});
  out.push_back(
      {"ScanAllByEdgePropertyValueOperator", "Operator", "Counter", total_scan_all_by_edge_property_value_operator});
  out.push_back(
      {"ScanAllByEdgePropertyRangeOperator", "Operator", "Counter", total_scan_all_by_edge_property_range_operator});
  out.push_back({"ScanAllByEdgeIdOperator", "Operator", "Counter", total_scan_all_by_edge_id_operator});
  out.push_back({"ScanAllByPointDistanceOperator", "Operator", "Counter", total_scan_all_by_point_distance_operator});
  out.push_back(
      {"ScanAllByPointWithinbboxOperator", "Operator", "Counter", total_scan_all_by_point_withinbbox_operator});
  out.push_back({"ExpandOperator", "Operator", "Counter", total_expand_operator});
  out.push_back({"ExpandVariableOperator", "Operator", "Counter", total_expand_variable_operator});
  out.push_back({"ConstructNamedPathOperator", "Operator", "Counter", total_construct_named_path_operator});
  out.push_back({"FilterOperator", "Operator", "Counter", total_filter_operator});
  out.push_back({"ProduceOperator", "Operator", "Counter", total_produce_operator});
  out.push_back({"DeleteOperator", "Operator", "Counter", total_delete_operator});
  out.push_back({"SetPropertyOperator", "Operator", "Counter", total_set_property_operator});
  out.push_back({"SetPropertiesOperator", "Operator", "Counter", total_set_properties_operator});
  out.push_back({"SetLabelsOperator", "Operator", "Counter", total_set_labels_operator});
  out.push_back({"RemovePropertyOperator", "Operator", "Counter", total_remove_property_operator});
  out.push_back({"RemoveLabelsOperator", "Operator", "Counter", total_remove_labels_operator});
  out.push_back({"EdgeUniquenessFilterOperator", "Operator", "Counter", total_edge_uniqueness_filter_operator});
  out.push_back({"EmptyResultOperator", "Operator", "Counter", total_empty_result_operator});
  out.push_back({"AccumulateOperator", "Operator", "Counter", total_accumulate_operator});
  out.push_back({"AggregateOperator", "Operator", "Counter", total_aggregate_operator});
  out.push_back({"SkipOperator", "Operator", "Counter", total_skip_operator});
  out.push_back({"LimitOperator", "Operator", "Counter", total_limit_operator});
  out.push_back({"OrderByOperator", "Operator", "Counter", total_order_by_operator});
  out.push_back({"MergeOperator", "Operator", "Counter", total_merge_operator});
  out.push_back({"OptionalOperator", "Operator", "Counter", total_optional_operator});
  out.push_back({"UnwindOperator", "Operator", "Counter", total_unwind_operator});
  out.push_back({"DistinctOperator", "Operator", "Counter", total_distinct_operator});
  out.push_back({"UnionOperator", "Operator", "Counter", total_union_operator});
  out.push_back({"CartesianOperator", "Operator", "Counter", total_cartesian_operator});
  out.push_back({"CallProcedureOperator", "Operator", "Counter", total_call_procedure_operator});
  out.push_back({"ForeachOperator", "Operator", "Counter", total_foreach_operator});
  out.push_back({"EvaluatePatternFilterOperator", "Operator", "Counter", total_evaluate_pattern_filter_operator});
  out.push_back({"ApplyOperator", "Operator", "Counter", total_apply_operator});
  out.push_back({"IndexedJoinOperator", "Operator", "Counter", total_indexed_join_operator});
  out.push_back({"HashJoinOperator", "Operator", "Counter", total_hash_join_operator});
  out.push_back({"RollUpApplyOperator", "Operator", "Counter", total_roll_up_apply_operator});
  out.push_back({"PeriodicCommitOperator", "Operator", "Counter", total_periodic_commit_operator});
  out.push_back({"PeriodicSubqueryOperator", "Operator", "Counter", total_periodic_subquery_operator});
  out.push_back({"SetNestedPropertyOperator", "Operator", "Counter", total_set_nested_property_operator});
  out.push_back({"RemoveNestedPropertyOperator", "Operator", "Counter", total_remove_nested_property_operator});

  // Index
  out.push_back({"ActiveLabelIndices", "Index", "Gauge", total_active_label_indices});
  out.push_back({"ActiveLabelPropertyIndices", "Index", "Gauge", total_active_label_property_indices});
  out.push_back({"ActiveEdgeTypeIndices", "Index", "Gauge", total_active_edge_type_indices});
  out.push_back({"ActiveEdgeTypePropertyIndices", "Index", "Gauge", total_active_edge_type_property_indices});
  out.push_back({"ActiveEdgePropertyIndices", "Index", "Gauge", total_active_edge_property_indices});
  out.push_back({"ActivePointIndices", "Index", "Gauge", total_active_point_indices});
  out.push_back({"ActiveTextIndices", "Index", "Gauge", total_active_text_indices});
  out.push_back({"ActiveTextEdgeIndices", "Index", "Gauge", total_active_text_edge_indices});
  out.push_back({"ActiveVectorIndices", "Index", "Gauge", total_active_vector_indices});
  out.push_back({"ActiveVectorEdgeIndices", "Index", "Gauge", total_active_vector_edge_indices});

  // Constraint
  out.push_back({"ActiveExistenceConstraints", "Constraint", "Gauge", total_active_existence_constraints});
  out.push_back({"ActiveUniqueConstraints", "Constraint", "Gauge", total_active_unique_constraints});
  out.push_back({"ActiveTypeConstraints", "Constraint", "Gauge", total_active_type_constraints});

  // Stream
  out.push_back({"StreamsCreated", "Stream", "Counter", total_streams_created});
  out.push_back({"MessagesConsumed", "Stream", "Counter", total_messages_consumed});

  // Trigger
  out.push_back({"TriggersCreated", "Trigger", "Counter", total_triggers_created});
  out.push_back({"TriggersExecuted", "Trigger", "Counter", total_triggers_executed});

  // Transaction
  out.push_back({"ActiveTransactions", "Transaction", "Gauge", total_active_transactions});
  out.push_back({"CommitedTransactions", "Transaction", "Counter", total_committed_transactions});
  out.push_back({"RolledBackTransactions", "Transaction", "Counter", total_rolled_back_transactions});
  out.push_back({"FailedQuery", "Transaction", "Counter", total_failed_query});
  out.push_back({"FailedPrepare", "Transaction", "Counter", total_failed_prepare});
  out.push_back({"FailedPull", "Transaction", "Counter", total_failed_pull});
  out.push_back({"SuccessfulQuery", "Transaction", "Counter", total_successful_query});
  out.push_back({"WriteWriteConflicts", "Transaction", "Counter", total_write_write_conflicts});
  out.push_back({"TransientErrors", "Transaction", "Counter", total_transient_errors});

  // QueryType
  out.push_back({"ReadQuery", "QueryType", "Counter", total_read_query});
  out.push_back({"WriteQuery", "QueryType", "Counter", total_write_query});
  out.push_back({"ReadWriteQuery", "QueryType", "Counter", total_read_write_query});

  // TTL
  out.push_back({"DeletedNodes", "TTL", "Counter", total_deleted_nodes});
  out.push_back({"DeletedEdges", "TTL", "Counter", total_deleted_edges});

  // SchemaInfo
  out.push_back({"ShowSchema", "SchemaInfo", "Counter", total_show_schema});

  // Query
  AppendMergedHistogramPercentiles(out, "QueryExecutionLatency", "Query", query_exec_hdatas);

  // Snapshot
  AppendMergedHistogramPercentiles(out, "SnapshotCreationLatency", "Snapshot", snapshot_creation_hdatas);
  AppendMergedHistogramPercentiles(out, "SnapshotRecoveryLatency", "Snapshot", snapshot_recovery_hdatas);

  // Memory (GC histograms)
  AppendMergedHistogramPercentiles(out, "GCLatency", "Memory", gc_hdatas);
  AppendMergedHistogramPercentiles(out, "GCSkiplistCleanupLatency", "Memory", gc_skiplist_hdatas);

  // Session
  out.push_back({"ActiveSessions", "Session", "Gauge", static_cast<int64_t>(g.active_sessions->Value())});
  out.push_back({"ActiveBoltSessions", "Session", "Gauge", static_cast<int64_t>(g.active_bolt_sessions->Value())});
  out.push_back({"ActiveTCPSessions", "Session", "Gauge", static_cast<int64_t>(g.active_tcp_sessions->Value())});
  out.push_back({"ActiveSSLSessions", "Session", "Gauge", static_cast<int64_t>(g.active_ssl_sessions->Value())});
  out.push_back(
      {"ActiveWebSocketSessions", "Session", "Gauge", static_cast<int64_t>(g.active_websocket_sessions->Value())});
  out.push_back({"BoltMessages", "Session", "Counter", static_cast<int64_t>(g.bolt_messages->Value())});

  // HighAvailability counters
  out.push_back(
      {"SuccessfulFailovers", "HighAvailability", "Counter", static_cast<int64_t>(g.successful_failovers->Value())});
  out.push_back(
      {"RaftFailedFailovers", "HighAvailability", "Counter", static_cast<int64_t>(g.raft_failed_failovers->Value())});
  out.push_back({"NoAliveInstanceFailedFailovers",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.no_alive_instance_failed_failovers->Value())});
  out.push_back(
      {"BecomeLeaderSuccess", "HighAvailability", "Counter", static_cast<int64_t>(g.become_leader_success->Value())});
  out.push_back({"FailedToBecomeLeader",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.failed_to_become_leader->Value())});
  out.push_back({"ShowInstance", "HighAvailability", "Counter", static_cast<int64_t>(g.show_instance->Value())});
  out.push_back({"ShowInstances", "HighAvailability", "Counter", static_cast<int64_t>(g.show_instances->Value())});
  out.push_back({"DemoteInstance", "HighAvailability", "Counter", static_cast<int64_t>(g.demote_instance->Value())});
  out.push_back({"UnregisterReplInstance",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.unregister_repl_instance->Value())});
  out.push_back(
      {"RemoveCoordInstance", "HighAvailability", "Counter", static_cast<int64_t>(g.remove_coord_instance->Value())});
  out.push_back({"ReplicaRecoverySuccess",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.replica_recovery_success->Value())});
  out.push_back(
      {"ReplicaRecoveryFail", "HighAvailability", "Counter", static_cast<int64_t>(g.replica_recovery_fail->Value())});
  out.push_back(
      {"ReplicaRecoverySkip", "HighAvailability", "Counter", static_cast<int64_t>(g.replica_recovery_skip->Value())});
  out.push_back({"StateCheckRpcSuccess",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.state_check_rpc_success->Value())});
  out.push_back(
      {"StateCheckRpcFail", "HighAvailability", "Counter", static_cast<int64_t>(g.state_check_rpc_fail->Value())});
  out.push_back({"UnregisterReplicaRpcSuccess",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.unregister_replica_rpc_success->Value())});
  out.push_back({"UnregisterReplicaRpcFail",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.unregister_replica_rpc_fail->Value())});
  out.push_back({"EnableWritingOnMainRpcSuccess",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.enable_writing_on_main_rpc_success->Value())});
  out.push_back({"EnableWritingOnMainRpcFail",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.enable_writing_on_main_rpc_fail->Value())});
  out.push_back({"PromoteToMainRpcSuccess",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.promote_to_main_rpc_success->Value())});
  out.push_back({"PromoteToMainRpcFail",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.promote_to_main_rpc_fail->Value())});
  out.push_back({"DemoteMainToReplicaRpcSuccess",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.demote_main_to_replica_rpc_success->Value())});
  out.push_back({"DemoteMainToReplicaRpcFail",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.demote_main_to_replica_rpc_fail->Value())});
  out.push_back({"RegisterReplicaOnMainRpcSuccess",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.register_replica_on_main_rpc_success->Value())});
  out.push_back({"RegisterReplicaOnMainRpcFail",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.register_replica_on_main_rpc_fail->Value())});
  out.push_back({"SwapMainUUIDRpcSuccess",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.swap_main_uuid_rpc_success->Value())});
  out.push_back(
      {"SwapMainUUIDRpcFail", "HighAvailability", "Counter", static_cast<int64_t>(g.swap_main_uuid_rpc_fail->Value())});
  out.push_back({"GetDatabaseHistoriesRpcSuccess",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.get_database_histories_rpc_success->Value())});
  out.push_back({"GetDatabaseHistoriesRpcFail",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.get_database_histories_rpc_fail->Value())});
  out.push_back({"UpdateDataInstanceConfigRpcSuccess",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.update_data_instance_config_rpc_success->Value())});
  out.push_back({"UpdateDataInstanceConfigRpcFail",
                 "HighAvailability",
                 "Counter",
                 static_cast<int64_t>(g.update_data_instance_config_rpc_fail->Value())});

  // HighAvailability histograms
  AppendHistogramPercentiles(out, "InstanceSuccCallback", "HighAvailability", *g.instance_succ_callback_seconds);
  AppendHistogramPercentiles(out, "InstanceFailCallback", "HighAvailability", *g.instance_fail_callback_seconds);
  AppendHistogramPercentiles(
      out, "ChooseMostUpToDateInstance", "HighAvailability", *g.choose_most_up_to_date_instance_seconds);
  AppendHistogramPercentiles(out, "SocketConnect", "HighAvailability", *g.socket_connect_seconds);
  AppendHistogramPercentiles(out, "ReplicaStream", "HighAvailability", *g.replica_stream_seconds);
  AppendHistogramPercentiles(out, "DataFailover", "HighAvailability", *g.data_failover_seconds);
  AppendHistogramPercentiles(out, "StartTxnReplication", "HighAvailability", *g.start_txn_replication_seconds);
  AppendHistogramPercentiles(out, "FinalizeTxnReplication", "HighAvailability", *g.finalize_txn_replication_seconds);
  AppendHistogramPercentiles(out, "PromoteToMainRpc", "HighAvailability", *g.promote_to_main_rpc_seconds);
  AppendHistogramPercentiles(out, "DemoteMainToReplicaRpc", "HighAvailability", *g.demote_main_to_replica_rpc_seconds);
  AppendHistogramPercentiles(
      out, "RegisterReplicaOnMainRpc", "HighAvailability", *g.register_replica_on_main_rpc_seconds);
  AppendHistogramPercentiles(out, "UnregisterReplicaRpc", "HighAvailability", *g.unregister_replica_rpc_seconds);
  AppendHistogramPercentiles(out, "EnableWritingOnMainRpc", "HighAvailability", *g.enable_writing_on_main_rpc_seconds);
  AppendHistogramPercentiles(out, "StateCheckRpc", "HighAvailability", *g.state_check_rpc_seconds);
  AppendHistogramPercentiles(out, "GetDatabaseHistoriesRpc", "HighAvailability", *g.get_database_histories_rpc_seconds);
  AppendHistogramPercentiles(out, "HeartbeatRpc", "HighAvailability", *g.heartbeat_rpc_seconds);
  AppendHistogramPercentiles(out, "PrepareCommitRpc", "HighAvailability", *g.prepare_commit_rpc_seconds);
  AppendHistogramPercentiles(out, "SnapshotRpc", "HighAvailability", *g.snapshot_rpc_seconds);
  AppendHistogramPercentiles(out, "CurrentWalRpc", "HighAvailability", *g.current_wal_rpc_seconds);
  AppendHistogramPercentiles(out, "WalFilesRpc", "HighAvailability", *g.wal_files_rpc_seconds);
  AppendHistogramPercentiles(out, "FrequentHeartbeatRpc", "HighAvailability", *g.frequent_heartbeat_rpc_seconds);
  AppendHistogramPercentiles(out, "SystemRecoveryRpc", "HighAvailability", *g.system_recovery_rpc_seconds);
  AppendHistogramPercentiles(
      out, "UpdateDataInstanceConfigRpc", "HighAvailability", *g.update_data_instance_config_rpc_seconds);
  AppendHistogramPercentiles(out, "GetHistories", "HighAvailability", *g.get_histories_seconds);

  return out;
}

std::vector<MetricInfo> PrometheusMetrics::GetGlobalMetricsInfoForLegacyJson() {
  auto out = GetGlobalMetricsInfo();

  if (!flags::CoordinationSetupInstance().IsCoordinator()) {
    return out;
  }

  std::lock_guard const lock{legacy_json_delta_mutex_};
  for (auto &info : out) {
    if (info.type != "HighAvailability" || info.metric_type != "Counter" ||
        !IsLegacyCoordinatorDeltaMetric(info.name)) {
      continue;
    }
    auto *current = std::get_if<int64_t>(&info.value);
    if (current == nullptr) continue;

    auto [it, inserted] = legacy_json_prev_ha_counter_values_.emplace(info.name, *current);
    if (inserted) {
      continue;
    }

    int64_t const delta = *current - it->second;
    it->second = *current;
    info.value = delta;
  }
  return out;
}

PrometheusMetrics &Metrics() {
  static PrometheusMetrics instance;
  return instance;
}

}  // namespace memgraph::metrics
