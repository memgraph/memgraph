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

#include "utils/logging.hpp"

namespace memgraph::metrics {

namespace {

using prometheus::ClientMetric;

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
      rollbacked_transactions_family_{prometheus::BuildCounter()
                                          .Name("memgraph_rollbacked_transactions_total")
                                          .Help("Total number of rollbacked transactions")
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
                                              .Register(registry_)} {
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

DatabaseMetricHandles *PrometheusMetrics::AddDatabase(std::string_view db_name,
                                                      std::function<StorageSnapshot()> get_snapshot) {
  prometheus::Labels const labels{{"database", std::string(db_name)}};
  MG_ASSERT(!vertex_count_family_.Has(labels), "Database '{}' already registered in PrometheusMetrics", db_name);
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
              .rollbacked_transactions = &rollbacked_transactions_family_.Add(labels),
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
      .get_snapshot = std::move(get_snapshot),
  });
  return &databases_.back().handles;
}

void PrometheusMetrics::RemoveDatabase(DatabaseMetricHandles const *handles) {
  auto it = std::ranges::find_if(databases_, [handles](auto const &e) { return &e.handles == handles; });
  MG_ASSERT(it != databases_.end(), "Attempted to remove unregistered database from PrometheusMetrics");
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
  rollbacked_transactions_family_.Remove(h.rollbacked_transactions);
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
  for (auto &entry : databases_) {
    auto const snapshot = entry.get_snapshot();
    entry.handles.vertex_count->Set(static_cast<double>(snapshot.vertex_count));
    entry.handles.edge_count->Set(static_cast<double>(snapshot.edge_count));
    entry.handles.disk_usage_bytes->Set(static_cast<double>(snapshot.disk_usage));
    entry.handles.memory_res_bytes->Set(static_cast<double>(snapshot.memory_res));
  }
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
  for (auto const [quantile, label] : {std::pair{0.50, "p50"}, std::pair{0.90, "p90"}, std::pair{0.99, "p99"}}) {
    out.push_back({name + "_" + label, type, "Histogram", HistogramPercentile(h, quantile)});
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
  for (auto const [quantile, label] : {std::pair{0.50, "p50"}, std::pair{0.90, "p90"}, std::pair{0.99, "p99"}}) {
    out.push_back({name + "_" + label, type, "Histogram", MergedHistogramPercentile(hdatas, quantile)});
  }
}

}  // namespace

std::expected<std::vector<MetricInfo>, std::string> PrometheusMetrics::GetDbMetricsInfo(
    std::string_view db_name) const {
  auto const it = std::ranges::find_if(databases_, [db_name](auto const &e) { return e.db_name == db_name; });
  if (it == databases_.end()) {
    return std::unexpected(fmt::format("Database '{}' not found in metrics registry", db_name));
  }

  auto const &h = it->handles;
  auto const snapshot = it->get_snapshot();
  std::vector<MetricInfo> out;

  // General
  double const vertex_count = static_cast<double>(snapshot.vertex_count);
  double const edge_count = static_cast<double>(snapshot.edge_count);
  out.push_back({"vertex_count", "General", "Gauge", vertex_count});
  out.push_back({"edge_count", "General", "Gauge", edge_count});
  out.push_back({"average_degree", "General", "Gauge", vertex_count > 0.0 ? 2.0 * edge_count / vertex_count : 0.0});

  // Memory
  out.push_back({"disk_usage_bytes", "Memory", "Gauge", static_cast<double>(snapshot.disk_usage)});
  out.push_back({"memory_res_bytes", "Memory", "Gauge", static_cast<double>(snapshot.memory_res)});
  out.push_back({"unreleased_delta_objects", "Memory", "Gauge", h.unreleased_delta_objects->Value()});
  AppendHistogramPercentiles(out, "gc_latency_seconds", "Memory", *h.gc_latency_seconds);
  AppendHistogramPercentiles(
      out, "gc_skiplist_cleanup_latency_seconds", "Memory", *h.gc_skiplist_cleanup_latency_seconds);

  // Operator
  out.push_back({"once_operator", "Operator", "Counter", h.once_operator->Value()});
  out.push_back({"create_node_operator", "Operator", "Counter", h.create_node_operator->Value()});
  out.push_back({"create_expand_operator", "Operator", "Counter", h.create_expand_operator->Value()});
  out.push_back({"scan_all_operator", "Operator", "Counter", h.scan_all_operator->Value()});
  out.push_back({"scan_all_by_label_operator", "Operator", "Counter", h.scan_all_by_label_operator->Value()});
  out.push_back({"scan_all_by_label_properties_operator",
                 "Operator",
                 "Counter",
                 h.scan_all_by_label_properties_operator->Value()});
  out.push_back({"scan_all_by_id_operator", "Operator", "Counter", h.scan_all_by_id_operator->Value()});
  out.push_back({"scan_all_by_edge_operator", "Operator", "Counter", h.scan_all_by_edge_operator->Value()});
  out.push_back({"scan_all_by_edge_type_operator", "Operator", "Counter", h.scan_all_by_edge_type_operator->Value()});
  out.push_back({"scan_all_by_edge_type_property_operator",
                 "Operator",
                 "Counter",
                 h.scan_all_by_edge_type_property_operator->Value()});
  out.push_back({"scan_all_by_edge_type_property_value_operator",
                 "Operator",
                 "Counter",
                 h.scan_all_by_edge_type_property_value_operator->Value()});
  out.push_back({"scan_all_by_edge_type_property_range_operator",
                 "Operator",
                 "Counter",
                 h.scan_all_by_edge_type_property_range_operator->Value()});
  out.push_back(
      {"scan_all_by_edge_property_operator", "Operator", "Counter", h.scan_all_by_edge_property_operator->Value()});
  out.push_back({"scan_all_by_edge_property_value_operator",
                 "Operator",
                 "Counter",
                 h.scan_all_by_edge_property_value_operator->Value()});
  out.push_back({"scan_all_by_edge_property_range_operator",
                 "Operator",
                 "Counter",
                 h.scan_all_by_edge_property_range_operator->Value()});
  out.push_back({"scan_all_by_edge_id_operator", "Operator", "Counter", h.scan_all_by_edge_id_operator->Value()});
  out.push_back(
      {"scan_all_by_point_distance_operator", "Operator", "Counter", h.scan_all_by_point_distance_operator->Value()});
  out.push_back({"scan_all_by_point_withinbbox_operator",
                 "Operator",
                 "Counter",
                 h.scan_all_by_point_withinbbox_operator->Value()});
  out.push_back({"expand_operator", "Operator", "Counter", h.expand_operator->Value()});
  out.push_back({"expand_variable_operator", "Operator", "Counter", h.expand_variable_operator->Value()});
  out.push_back({"construct_named_path_operator", "Operator", "Counter", h.construct_named_path_operator->Value()});
  out.push_back({"filter_operator", "Operator", "Counter", h.filter_operator->Value()});
  out.push_back({"produce_operator", "Operator", "Counter", h.produce_operator->Value()});
  out.push_back({"delete_operator", "Operator", "Counter", h.delete_operator->Value()});
  out.push_back({"set_property_operator", "Operator", "Counter", h.set_property_operator->Value()});
  out.push_back({"set_properties_operator", "Operator", "Counter", h.set_properties_operator->Value()});
  out.push_back({"set_labels_operator", "Operator", "Counter", h.set_labels_operator->Value()});
  out.push_back({"remove_property_operator", "Operator", "Counter", h.remove_property_operator->Value()});
  out.push_back({"remove_labels_operator", "Operator", "Counter", h.remove_labels_operator->Value()});
  out.push_back({"edge_uniqueness_filter_operator", "Operator", "Counter", h.edge_uniqueness_filter_operator->Value()});
  out.push_back({"empty_result_operator", "Operator", "Counter", h.empty_result_operator->Value()});
  out.push_back({"accumulate_operator", "Operator", "Counter", h.accumulate_operator->Value()});
  out.push_back({"aggregate_operator", "Operator", "Counter", h.aggregate_operator->Value()});
  out.push_back({"skip_operator", "Operator", "Counter", h.skip_operator->Value()});
  out.push_back({"limit_operator", "Operator", "Counter", h.limit_operator->Value()});
  out.push_back({"order_by_operator", "Operator", "Counter", h.order_by_operator->Value()});
  out.push_back({"merge_operator", "Operator", "Counter", h.merge_operator->Value()});
  out.push_back({"optional_operator", "Operator", "Counter", h.optional_operator->Value()});
  out.push_back({"unwind_operator", "Operator", "Counter", h.unwind_operator->Value()});
  out.push_back({"distinct_operator", "Operator", "Counter", h.distinct_operator->Value()});
  out.push_back({"union_operator", "Operator", "Counter", h.union_operator->Value()});
  out.push_back({"cartesian_operator", "Operator", "Counter", h.cartesian_operator->Value()});
  out.push_back({"call_procedure_operator", "Operator", "Counter", h.call_procedure_operator->Value()});
  out.push_back({"foreach_operator", "Operator", "Counter", h.foreach_operator->Value()});
  out.push_back(
      {"evaluate_pattern_filter_operator", "Operator", "Counter", h.evaluate_pattern_filter_operator->Value()});
  out.push_back({"apply_operator", "Operator", "Counter", h.apply_operator->Value()});
  out.push_back({"indexed_join_operator", "Operator", "Counter", h.indexed_join_operator->Value()});
  out.push_back({"hash_join_operator", "Operator", "Counter", h.hash_join_operator->Value()});
  out.push_back({"roll_up_apply_operator", "Operator", "Counter", h.roll_up_apply_operator->Value()});
  out.push_back({"periodic_commit_operator", "Operator", "Counter", h.periodic_commit_operator->Value()});
  out.push_back({"periodic_subquery_operator", "Operator", "Counter", h.periodic_subquery_operator->Value()});
  out.push_back({"set_nested_property_operator", "Operator", "Counter", h.set_nested_property_operator->Value()});
  out.push_back({"remove_nested_property_operator", "Operator", "Counter", h.remove_nested_property_operator->Value()});

  // Index
  out.push_back({"active_label_indices", "Index", "Gauge", h.active_label_indices->Value()});
  out.push_back({"active_label_property_indices", "Index", "Gauge", h.active_label_property_indices->Value()});
  out.push_back({"active_edge_type_indices", "Index", "Gauge", h.active_edge_type_indices->Value()});
  out.push_back({"active_edge_type_property_indices", "Index", "Gauge", h.active_edge_type_property_indices->Value()});
  out.push_back({"active_edge_property_indices", "Index", "Gauge", h.active_edge_property_indices->Value()});
  out.push_back({"active_point_indices", "Index", "Gauge", h.active_point_indices->Value()});
  out.push_back({"active_text_indices", "Index", "Gauge", h.active_text_indices->Value()});
  out.push_back({"active_text_edge_indices", "Index", "Gauge", h.active_text_edge_indices->Value()});
  out.push_back({"active_vector_indices", "Index", "Gauge", h.active_vector_indices->Value()});
  out.push_back({"active_vector_edge_indices", "Index", "Gauge", h.active_vector_edge_indices->Value()});

  // Constraint
  out.push_back({"active_existence_constraints", "Constraint", "Gauge", h.active_existence_constraints->Value()});
  out.push_back({"active_unique_constraints", "Constraint", "Gauge", h.active_unique_constraints->Value()});
  out.push_back({"active_type_constraints", "Constraint", "Gauge", h.active_type_constraints->Value()});

  // Stream
  out.push_back({"streams_created", "Stream", "Counter", h.streams_created->Value()});
  out.push_back({"messages_consumed", "Stream", "Counter", h.messages_consumed->Value()});

  // Trigger
  out.push_back({"triggers_created", "Trigger", "Counter", h.triggers_created->Value()});
  out.push_back({"triggers_executed", "Trigger", "Counter", h.triggers_executed->Value()});

  // Transaction
  out.push_back({"active_transactions", "Transaction", "Gauge", h.active_transactions->Value()});
  out.push_back({"committed_transactions", "Transaction", "Counter", h.committed_transactions->Value()});
  out.push_back({"rollbacked_transactions", "Transaction", "Counter", h.rollbacked_transactions->Value()});
  out.push_back({"failed_query", "Transaction", "Counter", h.failed_query->Value()});
  out.push_back({"failed_prepare", "Transaction", "Counter", h.failed_prepare->Value()});
  out.push_back({"failed_pull", "Transaction", "Counter", h.failed_pull->Value()});
  out.push_back({"successful_query", "Transaction", "Counter", h.successful_query->Value()});
  out.push_back({"write_write_conflicts", "Transaction", "Counter", h.write_write_conflicts->Value()});
  out.push_back({"transient_errors", "Transaction", "Counter", h.transient_errors->Value()});

  // QueryType
  out.push_back({"read_query", "QueryType", "Counter", h.read_query->Value()});
  out.push_back({"write_query", "QueryType", "Counter", h.write_query->Value()});
  out.push_back({"read_write_query", "QueryType", "Counter", h.read_write_query->Value()});

  // TTL
  out.push_back({"deleted_nodes", "TTL", "Counter", h.deleted_nodes->Value()});
  out.push_back({"deleted_edges", "TTL", "Counter", h.deleted_edges->Value()});

  // SchemaInfo
  out.push_back({"show_schema", "SchemaInfo", "Counter", h.show_schema->Value()});

  // Query
  AppendHistogramPercentiles(out, "query_execution_latency_seconds", "Query", *h.query_execution_latency_seconds);

  // Snapshot
  AppendHistogramPercentiles(
      out, "snapshot_creation_latency_seconds", "Snapshot", *h.snapshot_creation_latency_seconds);
  AppendHistogramPercentiles(
      out, "snapshot_recovery_latency_seconds", "Snapshot", *h.snapshot_recovery_latency_seconds);

  return out;
}

std::vector<MetricInfo> PrometheusMetrics::GetGlobalMetricsInfo() const {
  auto const &g = global;
  std::vector<MetricInfo> out;

  // General — aggregate across all databases
  double total_vertex_count = 0.0;
  double total_edge_count = 0.0;
  for (auto const &entry : databases_) {
    auto const snapshot = entry.get_snapshot();
    total_vertex_count += static_cast<double>(snapshot.vertex_count);
    total_edge_count += static_cast<double>(snapshot.edge_count);
  }
  out.push_back({"vertex_count", "General", "Gauge", total_vertex_count});
  out.push_back({"edge_count", "General", "Gauge", total_edge_count});
  out.push_back({"average_degree",
                 "General",
                 "Gauge",
                 total_vertex_count > 0.0 ? 2.0 * total_edge_count / total_vertex_count : 0.0});

  // Memory — aggregate across all databases + global peak
  double total_disk_usage = 0.0;
  double total_memory_res = 0.0;
  double total_unreleased_deltas = 0.0;
  for (auto const &entry : databases_) {
    auto const snapshot = entry.get_snapshot();
    total_disk_usage += static_cast<double>(snapshot.disk_usage);
    total_memory_res += static_cast<double>(snapshot.memory_res);
    total_unreleased_deltas += entry.handles.unreleased_delta_objects->Value();
  }
  out.push_back({"disk_usage_bytes", "Memory", "Gauge", total_disk_usage});
  out.push_back({"memory_res_bytes", "Memory", "Gauge", total_memory_res});
  out.push_back({"peak_memory_res_bytes", "Memory", "Gauge", g.peak_memory_res_bytes->Value()});
  out.push_back({"unreleased_delta_objects", "Memory", "Gauge", total_unreleased_deltas});

  // Aggregate per-db counters/gauges
  double total_once_operator = 0.0, total_create_node_operator = 0.0, total_create_expand_operator = 0.0;
  double total_scan_all_operator = 0.0, total_scan_all_by_label_operator = 0.0;
  double total_scan_all_by_label_properties_operator = 0.0, total_scan_all_by_id_operator = 0.0;
  double total_scan_all_by_edge_operator = 0.0, total_scan_all_by_edge_type_operator = 0.0;
  double total_scan_all_by_edge_type_property_operator = 0.0;
  double total_scan_all_by_edge_type_property_value_operator = 0.0;
  double total_scan_all_by_edge_type_property_range_operator = 0.0;
  double total_scan_all_by_edge_property_operator = 0.0;
  double total_scan_all_by_edge_property_value_operator = 0.0;
  double total_scan_all_by_edge_property_range_operator = 0.0;
  double total_scan_all_by_edge_id_operator = 0.0;
  double total_scan_all_by_point_distance_operator = 0.0, total_scan_all_by_point_withinbbox_operator = 0.0;
  double total_expand_operator = 0.0, total_expand_variable_operator = 0.0;
  double total_construct_named_path_operator = 0.0, total_filter_operator = 0.0, total_produce_operator = 0.0;
  double total_delete_operator = 0.0, total_set_property_operator = 0.0, total_set_properties_operator = 0.0;
  double total_set_labels_operator = 0.0, total_remove_property_operator = 0.0, total_remove_labels_operator = 0.0;
  double total_edge_uniqueness_filter_operator = 0.0, total_empty_result_operator = 0.0;
  double total_accumulate_operator = 0.0, total_aggregate_operator = 0.0;
  double total_skip_operator = 0.0, total_limit_operator = 0.0, total_order_by_operator = 0.0;
  double total_merge_operator = 0.0, total_optional_operator = 0.0, total_unwind_operator = 0.0;
  double total_distinct_operator = 0.0, total_union_operator = 0.0, total_cartesian_operator = 0.0;
  double total_call_procedure_operator = 0.0, total_foreach_operator = 0.0;
  double total_evaluate_pattern_filter_operator = 0.0, total_apply_operator = 0.0;
  double total_indexed_join_operator = 0.0, total_hash_join_operator = 0.0, total_roll_up_apply_operator = 0.0;
  double total_periodic_commit_operator = 0.0, total_periodic_subquery_operator = 0.0;
  double total_set_nested_property_operator = 0.0, total_remove_nested_property_operator = 0.0;
  double total_active_label_indices = 0.0, total_active_label_property_indices = 0.0;
  double total_active_edge_type_indices = 0.0, total_active_edge_type_property_indices = 0.0;
  double total_active_edge_property_indices = 0.0, total_active_point_indices = 0.0;
  double total_active_text_indices = 0.0, total_active_text_edge_indices = 0.0;
  double total_active_vector_indices = 0.0, total_active_vector_edge_indices = 0.0;
  double total_active_existence_constraints = 0.0, total_active_unique_constraints = 0.0;
  double total_active_type_constraints = 0.0;
  double total_streams_created = 0.0, total_messages_consumed = 0.0;
  double total_triggers_created = 0.0, total_triggers_executed = 0.0;
  double total_active_transactions = 0.0, total_committed_transactions = 0.0;
  double total_rollbacked_transactions = 0.0, total_failed_query = 0.0;
  double total_failed_prepare = 0.0, total_failed_pull = 0.0, total_successful_query = 0.0;
  double total_write_write_conflicts = 0.0, total_transient_errors = 0.0;
  double total_read_query = 0.0, total_write_query = 0.0, total_read_write_query = 0.0;
  double total_deleted_nodes = 0.0, total_deleted_edges = 0.0;
  double total_show_schema = 0.0;
  std::vector<prometheus::ClientMetric::Histogram> query_exec_hdatas;
  std::vector<prometheus::ClientMetric::Histogram> snapshot_creation_hdatas;
  std::vector<prometheus::ClientMetric::Histogram> snapshot_recovery_hdatas;
  std::vector<prometheus::ClientMetric::Histogram> gc_hdatas;
  std::vector<prometheus::ClientMetric::Histogram> gc_skiplist_hdatas;

  for (auto const &entry : databases_) {
    auto const &h = entry.handles;
    total_once_operator += h.once_operator->Value();
    total_create_node_operator += h.create_node_operator->Value();
    total_create_expand_operator += h.create_expand_operator->Value();
    total_scan_all_operator += h.scan_all_operator->Value();
    total_scan_all_by_label_operator += h.scan_all_by_label_operator->Value();
    total_scan_all_by_label_properties_operator += h.scan_all_by_label_properties_operator->Value();
    total_scan_all_by_id_operator += h.scan_all_by_id_operator->Value();
    total_scan_all_by_edge_operator += h.scan_all_by_edge_operator->Value();
    total_scan_all_by_edge_type_operator += h.scan_all_by_edge_type_operator->Value();
    total_scan_all_by_edge_type_property_operator += h.scan_all_by_edge_type_property_operator->Value();
    total_scan_all_by_edge_type_property_value_operator += h.scan_all_by_edge_type_property_value_operator->Value();
    total_scan_all_by_edge_type_property_range_operator += h.scan_all_by_edge_type_property_range_operator->Value();
    total_scan_all_by_edge_property_operator += h.scan_all_by_edge_property_operator->Value();
    total_scan_all_by_edge_property_value_operator += h.scan_all_by_edge_property_value_operator->Value();
    total_scan_all_by_edge_property_range_operator += h.scan_all_by_edge_property_range_operator->Value();
    total_scan_all_by_edge_id_operator += h.scan_all_by_edge_id_operator->Value();
    total_scan_all_by_point_distance_operator += h.scan_all_by_point_distance_operator->Value();
    total_scan_all_by_point_withinbbox_operator += h.scan_all_by_point_withinbbox_operator->Value();
    total_expand_operator += h.expand_operator->Value();
    total_expand_variable_operator += h.expand_variable_operator->Value();
    total_construct_named_path_operator += h.construct_named_path_operator->Value();
    total_filter_operator += h.filter_operator->Value();
    total_produce_operator += h.produce_operator->Value();
    total_delete_operator += h.delete_operator->Value();
    total_set_property_operator += h.set_property_operator->Value();
    total_set_properties_operator += h.set_properties_operator->Value();
    total_set_labels_operator += h.set_labels_operator->Value();
    total_remove_property_operator += h.remove_property_operator->Value();
    total_remove_labels_operator += h.remove_labels_operator->Value();
    total_edge_uniqueness_filter_operator += h.edge_uniqueness_filter_operator->Value();
    total_empty_result_operator += h.empty_result_operator->Value();
    total_accumulate_operator += h.accumulate_operator->Value();
    total_aggregate_operator += h.aggregate_operator->Value();
    total_skip_operator += h.skip_operator->Value();
    total_limit_operator += h.limit_operator->Value();
    total_order_by_operator += h.order_by_operator->Value();
    total_merge_operator += h.merge_operator->Value();
    total_optional_operator += h.optional_operator->Value();
    total_unwind_operator += h.unwind_operator->Value();
    total_distinct_operator += h.distinct_operator->Value();
    total_union_operator += h.union_operator->Value();
    total_cartesian_operator += h.cartesian_operator->Value();
    total_call_procedure_operator += h.call_procedure_operator->Value();
    total_foreach_operator += h.foreach_operator->Value();
    total_evaluate_pattern_filter_operator += h.evaluate_pattern_filter_operator->Value();
    total_apply_operator += h.apply_operator->Value();
    total_indexed_join_operator += h.indexed_join_operator->Value();
    total_hash_join_operator += h.hash_join_operator->Value();
    total_roll_up_apply_operator += h.roll_up_apply_operator->Value();
    total_periodic_commit_operator += h.periodic_commit_operator->Value();
    total_periodic_subquery_operator += h.periodic_subquery_operator->Value();
    total_set_nested_property_operator += h.set_nested_property_operator->Value();
    total_remove_nested_property_operator += h.remove_nested_property_operator->Value();
    total_active_label_indices += h.active_label_indices->Value();
    total_active_label_property_indices += h.active_label_property_indices->Value();
    total_active_edge_type_indices += h.active_edge_type_indices->Value();
    total_active_edge_type_property_indices += h.active_edge_type_property_indices->Value();
    total_active_edge_property_indices += h.active_edge_property_indices->Value();
    total_active_point_indices += h.active_point_indices->Value();
    total_active_text_indices += h.active_text_indices->Value();
    total_active_text_edge_indices += h.active_text_edge_indices->Value();
    total_active_vector_indices += h.active_vector_indices->Value();
    total_active_vector_edge_indices += h.active_vector_edge_indices->Value();
    total_active_existence_constraints += h.active_existence_constraints->Value();
    total_active_unique_constraints += h.active_unique_constraints->Value();
    total_active_type_constraints += h.active_type_constraints->Value();
    total_streams_created += h.streams_created->Value();
    total_messages_consumed += h.messages_consumed->Value();
    total_triggers_created += h.triggers_created->Value();
    total_triggers_executed += h.triggers_executed->Value();
    total_active_transactions += h.active_transactions->Value();
    total_committed_transactions += h.committed_transactions->Value();
    total_rollbacked_transactions += h.rollbacked_transactions->Value();
    total_failed_query += h.failed_query->Value();
    total_failed_prepare += h.failed_prepare->Value();
    total_failed_pull += h.failed_pull->Value();
    total_successful_query += h.successful_query->Value();
    total_write_write_conflicts += h.write_write_conflicts->Value();
    total_transient_errors += h.transient_errors->Value();
    total_read_query += h.read_query->Value();
    total_write_query += h.write_query->Value();
    total_read_write_query += h.read_write_query->Value();
    total_deleted_nodes += h.deleted_nodes->Value();
    total_deleted_edges += h.deleted_edges->Value();
    total_show_schema += h.show_schema->Value();
    query_exec_hdatas.push_back(h.query_execution_latency_seconds->Collect().histogram);
    snapshot_creation_hdatas.push_back(h.snapshot_creation_latency_seconds->Collect().histogram);
    snapshot_recovery_hdatas.push_back(h.snapshot_recovery_latency_seconds->Collect().histogram);
    gc_hdatas.push_back(h.gc_latency_seconds->Collect().histogram);
    gc_skiplist_hdatas.push_back(h.gc_skiplist_cleanup_latency_seconds->Collect().histogram);
  }

  // Operator
  out.push_back({"once_operator", "Operator", "Counter", total_once_operator});
  out.push_back({"create_node_operator", "Operator", "Counter", total_create_node_operator});
  out.push_back({"create_expand_operator", "Operator", "Counter", total_create_expand_operator});
  out.push_back({"scan_all_operator", "Operator", "Counter", total_scan_all_operator});
  out.push_back({"scan_all_by_label_operator", "Operator", "Counter", total_scan_all_by_label_operator});
  out.push_back(
      {"scan_all_by_label_properties_operator", "Operator", "Counter", total_scan_all_by_label_properties_operator});
  out.push_back({"scan_all_by_id_operator", "Operator", "Counter", total_scan_all_by_id_operator});
  out.push_back({"scan_all_by_edge_operator", "Operator", "Counter", total_scan_all_by_edge_operator});
  out.push_back({"scan_all_by_edge_type_operator", "Operator", "Counter", total_scan_all_by_edge_type_operator});
  out.push_back({"scan_all_by_edge_type_property_operator",
                 "Operator",
                 "Counter",
                 total_scan_all_by_edge_type_property_operator});
  out.push_back({"scan_all_by_edge_type_property_value_operator",
                 "Operator",
                 "Counter",
                 total_scan_all_by_edge_type_property_value_operator});
  out.push_back({"scan_all_by_edge_type_property_range_operator",
                 "Operator",
                 "Counter",
                 total_scan_all_by_edge_type_property_range_operator});
  out.push_back(
      {"scan_all_by_edge_property_operator", "Operator", "Counter", total_scan_all_by_edge_property_operator});
  out.push_back({"scan_all_by_edge_property_value_operator",
                 "Operator",
                 "Counter",
                 total_scan_all_by_edge_property_value_operator});
  out.push_back({"scan_all_by_edge_property_range_operator",
                 "Operator",
                 "Counter",
                 total_scan_all_by_edge_property_range_operator});
  out.push_back({"scan_all_by_edge_id_operator", "Operator", "Counter", total_scan_all_by_edge_id_operator});
  out.push_back(
      {"scan_all_by_point_distance_operator", "Operator", "Counter", total_scan_all_by_point_distance_operator});
  out.push_back(
      {"scan_all_by_point_withinbbox_operator", "Operator", "Counter", total_scan_all_by_point_withinbbox_operator});
  out.push_back({"expand_operator", "Operator", "Counter", total_expand_operator});
  out.push_back({"expand_variable_operator", "Operator", "Counter", total_expand_variable_operator});
  out.push_back({"construct_named_path_operator", "Operator", "Counter", total_construct_named_path_operator});
  out.push_back({"filter_operator", "Operator", "Counter", total_filter_operator});
  out.push_back({"produce_operator", "Operator", "Counter", total_produce_operator});
  out.push_back({"delete_operator", "Operator", "Counter", total_delete_operator});
  out.push_back({"set_property_operator", "Operator", "Counter", total_set_property_operator});
  out.push_back({"set_properties_operator", "Operator", "Counter", total_set_properties_operator});
  out.push_back({"set_labels_operator", "Operator", "Counter", total_set_labels_operator});
  out.push_back({"remove_property_operator", "Operator", "Counter", total_remove_property_operator});
  out.push_back({"remove_labels_operator", "Operator", "Counter", total_remove_labels_operator});
  out.push_back({"edge_uniqueness_filter_operator", "Operator", "Counter", total_edge_uniqueness_filter_operator});
  out.push_back({"empty_result_operator", "Operator", "Counter", total_empty_result_operator});
  out.push_back({"accumulate_operator", "Operator", "Counter", total_accumulate_operator});
  out.push_back({"aggregate_operator", "Operator", "Counter", total_aggregate_operator});
  out.push_back({"skip_operator", "Operator", "Counter", total_skip_operator});
  out.push_back({"limit_operator", "Operator", "Counter", total_limit_operator});
  out.push_back({"order_by_operator", "Operator", "Counter", total_order_by_operator});
  out.push_back({"merge_operator", "Operator", "Counter", total_merge_operator});
  out.push_back({"optional_operator", "Operator", "Counter", total_optional_operator});
  out.push_back({"unwind_operator", "Operator", "Counter", total_unwind_operator});
  out.push_back({"distinct_operator", "Operator", "Counter", total_distinct_operator});
  out.push_back({"union_operator", "Operator", "Counter", total_union_operator});
  out.push_back({"cartesian_operator", "Operator", "Counter", total_cartesian_operator});
  out.push_back({"call_procedure_operator", "Operator", "Counter", total_call_procedure_operator});
  out.push_back({"foreach_operator", "Operator", "Counter", total_foreach_operator});
  out.push_back({"evaluate_pattern_filter_operator", "Operator", "Counter", total_evaluate_pattern_filter_operator});
  out.push_back({"apply_operator", "Operator", "Counter", total_apply_operator});
  out.push_back({"indexed_join_operator", "Operator", "Counter", total_indexed_join_operator});
  out.push_back({"hash_join_operator", "Operator", "Counter", total_hash_join_operator});
  out.push_back({"roll_up_apply_operator", "Operator", "Counter", total_roll_up_apply_operator});
  out.push_back({"periodic_commit_operator", "Operator", "Counter", total_periodic_commit_operator});
  out.push_back({"periodic_subquery_operator", "Operator", "Counter", total_periodic_subquery_operator});
  out.push_back({"set_nested_property_operator", "Operator", "Counter", total_set_nested_property_operator});
  out.push_back({"remove_nested_property_operator", "Operator", "Counter", total_remove_nested_property_operator});

  // Index
  out.push_back({"active_label_indices", "Index", "Gauge", total_active_label_indices});
  out.push_back({"active_label_property_indices", "Index", "Gauge", total_active_label_property_indices});
  out.push_back({"active_edge_type_indices", "Index", "Gauge", total_active_edge_type_indices});
  out.push_back({"active_edge_type_property_indices", "Index", "Gauge", total_active_edge_type_property_indices});
  out.push_back({"active_edge_property_indices", "Index", "Gauge", total_active_edge_property_indices});
  out.push_back({"active_point_indices", "Index", "Gauge", total_active_point_indices});
  out.push_back({"active_text_indices", "Index", "Gauge", total_active_text_indices});
  out.push_back({"active_text_edge_indices", "Index", "Gauge", total_active_text_edge_indices});
  out.push_back({"active_vector_indices", "Index", "Gauge", total_active_vector_indices});
  out.push_back({"active_vector_edge_indices", "Index", "Gauge", total_active_vector_edge_indices});

  // Constraint
  out.push_back({"active_existence_constraints", "Constraint", "Gauge", total_active_existence_constraints});
  out.push_back({"active_unique_constraints", "Constraint", "Gauge", total_active_unique_constraints});
  out.push_back({"active_type_constraints", "Constraint", "Gauge", total_active_type_constraints});

  // Stream
  out.push_back({"streams_created", "Stream", "Counter", total_streams_created});
  out.push_back({"messages_consumed", "Stream", "Counter", total_messages_consumed});

  // Trigger
  out.push_back({"triggers_created", "Trigger", "Counter", total_triggers_created});
  out.push_back({"triggers_executed", "Trigger", "Counter", total_triggers_executed});

  // Transaction
  out.push_back({"active_transactions", "Transaction", "Gauge", total_active_transactions});
  out.push_back({"committed_transactions", "Transaction", "Counter", total_committed_transactions});
  out.push_back({"rollbacked_transactions", "Transaction", "Counter", total_rollbacked_transactions});
  out.push_back({"failed_query", "Transaction", "Counter", total_failed_query});
  out.push_back({"failed_prepare", "Transaction", "Counter", total_failed_prepare});
  out.push_back({"failed_pull", "Transaction", "Counter", total_failed_pull});
  out.push_back({"successful_query", "Transaction", "Counter", total_successful_query});
  out.push_back({"write_write_conflicts", "Transaction", "Counter", total_write_write_conflicts});
  out.push_back({"transient_errors", "Transaction", "Counter", total_transient_errors});

  // QueryType
  out.push_back({"read_query", "QueryType", "Counter", total_read_query});
  out.push_back({"write_query", "QueryType", "Counter", total_write_query});
  out.push_back({"read_write_query", "QueryType", "Counter", total_read_write_query});

  // TTL
  out.push_back({"deleted_nodes", "TTL", "Counter", total_deleted_nodes});
  out.push_back({"deleted_edges", "TTL", "Counter", total_deleted_edges});

  // SchemaInfo
  out.push_back({"show_schema", "SchemaInfo", "Counter", total_show_schema});

  // Query
  AppendMergedHistogramPercentiles(out, "query_execution_latency_seconds", "Query", query_exec_hdatas);

  // Snapshot
  AppendMergedHistogramPercentiles(out, "snapshot_creation_latency_seconds", "Snapshot", snapshot_creation_hdatas);
  AppendMergedHistogramPercentiles(out, "snapshot_recovery_latency_seconds", "Snapshot", snapshot_recovery_hdatas);

  // Memory (GC histograms)
  AppendMergedHistogramPercentiles(out, "gc_latency_seconds", "Memory", gc_hdatas);
  AppendMergedHistogramPercentiles(out, "gc_skiplist_cleanup_latency_seconds", "Memory", gc_skiplist_hdatas);

  // Session
  out.push_back({"active_sessions", "Session", "Gauge", g.active_sessions->Value()});
  out.push_back({"active_bolt_sessions", "Session", "Gauge", g.active_bolt_sessions->Value()});
  out.push_back({"active_tcp_sessions", "Session", "Gauge", g.active_tcp_sessions->Value()});
  out.push_back({"active_ssl_sessions", "Session", "Gauge", g.active_ssl_sessions->Value()});
  out.push_back({"active_websocket_sessions", "Session", "Gauge", g.active_websocket_sessions->Value()});
  out.push_back({"bolt_messages", "Session", "Counter", g.bolt_messages->Value()});

  // HighAvailability counters
  out.push_back({"successful_failovers", "HighAvailability", "Counter", g.successful_failovers->Value()});
  out.push_back({"raft_failed_failovers", "HighAvailability", "Counter", g.raft_failed_failovers->Value()});
  out.push_back({"no_alive_instance_failed_failovers",
                 "HighAvailability",
                 "Counter",
                 g.no_alive_instance_failed_failovers->Value()});
  out.push_back({"become_leader_success", "HighAvailability", "Counter", g.become_leader_success->Value()});
  out.push_back({"failed_to_become_leader", "HighAvailability", "Counter", g.failed_to_become_leader->Value()});
  out.push_back({"show_instance", "HighAvailability", "Counter", g.show_instance->Value()});
  out.push_back({"show_instances", "HighAvailability", "Counter", g.show_instances->Value()});
  out.push_back({"demote_instance", "HighAvailability", "Counter", g.demote_instance->Value()});
  out.push_back({"unregister_repl_instance", "HighAvailability", "Counter", g.unregister_repl_instance->Value()});
  out.push_back({"remove_coord_instance", "HighAvailability", "Counter", g.remove_coord_instance->Value()});
  out.push_back({"replica_recovery_success", "HighAvailability", "Counter", g.replica_recovery_success->Value()});
  out.push_back({"replica_recovery_fail", "HighAvailability", "Counter", g.replica_recovery_fail->Value()});
  out.push_back({"replica_recovery_skip", "HighAvailability", "Counter", g.replica_recovery_skip->Value()});
  out.push_back({"state_check_rpc_success", "HighAvailability", "Counter", g.state_check_rpc_success->Value()});
  out.push_back({"state_check_rpc_fail", "HighAvailability", "Counter", g.state_check_rpc_fail->Value()});
  out.push_back(
      {"unregister_replica_rpc_success", "HighAvailability", "Counter", g.unregister_replica_rpc_success->Value()});
  out.push_back({"unregister_replica_rpc_fail", "HighAvailability", "Counter", g.unregister_replica_rpc_fail->Value()});
  out.push_back({"enable_writing_on_main_rpc_success",
                 "HighAvailability",
                 "Counter",
                 g.enable_writing_on_main_rpc_success->Value()});
  out.push_back(
      {"enable_writing_on_main_rpc_fail", "HighAvailability", "Counter", g.enable_writing_on_main_rpc_fail->Value()});
  out.push_back({"promote_to_main_rpc_success", "HighAvailability", "Counter", g.promote_to_main_rpc_success->Value()});
  out.push_back({"promote_to_main_rpc_fail", "HighAvailability", "Counter", g.promote_to_main_rpc_fail->Value()});
  out.push_back({"demote_main_to_replica_rpc_success",
                 "HighAvailability",
                 "Counter",
                 g.demote_main_to_replica_rpc_success->Value()});
  out.push_back(
      {"demote_main_to_replica_rpc_fail", "HighAvailability", "Counter", g.demote_main_to_replica_rpc_fail->Value()});
  out.push_back({"register_replica_on_main_rpc_success",
                 "HighAvailability",
                 "Counter",
                 g.register_replica_on_main_rpc_success->Value()});
  out.push_back({"register_replica_on_main_rpc_fail",
                 "HighAvailability",
                 "Counter",
                 g.register_replica_on_main_rpc_fail->Value()});
  out.push_back({"swap_main_uuid_rpc_success", "HighAvailability", "Counter", g.swap_main_uuid_rpc_success->Value()});
  out.push_back({"swap_main_uuid_rpc_fail", "HighAvailability", "Counter", g.swap_main_uuid_rpc_fail->Value()});
  out.push_back({"get_database_histories_rpc_success",
                 "HighAvailability",
                 "Counter",
                 g.get_database_histories_rpc_success->Value()});
  out.push_back(
      {"get_database_histories_rpc_fail", "HighAvailability", "Counter", g.get_database_histories_rpc_fail->Value()});
  out.push_back({"update_data_instance_config_rpc_success",
                 "HighAvailability",
                 "Counter",
                 g.update_data_instance_config_rpc_success->Value()});
  out.push_back({"update_data_instance_config_rpc_fail",
                 "HighAvailability",
                 "Counter",
                 g.update_data_instance_config_rpc_fail->Value()});

  // HighAvailability histograms
  AppendHistogramPercentiles(
      out, "instance_succ_callback_seconds", "HighAvailability", *g.instance_succ_callback_seconds);
  AppendHistogramPercentiles(
      out, "instance_fail_callback_seconds", "HighAvailability", *g.instance_fail_callback_seconds);
  AppendHistogramPercentiles(
      out, "choose_most_up_to_date_instance_seconds", "HighAvailability", *g.choose_most_up_to_date_instance_seconds);
  AppendHistogramPercentiles(out, "socket_connect_seconds", "HighAvailability", *g.socket_connect_seconds);
  AppendHistogramPercentiles(out, "replica_stream_seconds", "HighAvailability", *g.replica_stream_seconds);
  AppendHistogramPercentiles(out, "data_failover_seconds", "HighAvailability", *g.data_failover_seconds);
  AppendHistogramPercentiles(
      out, "start_txn_replication_seconds", "HighAvailability", *g.start_txn_replication_seconds);
  AppendHistogramPercentiles(
      out, "finalize_txn_replication_seconds", "HighAvailability", *g.finalize_txn_replication_seconds);
  AppendHistogramPercentiles(out, "promote_to_main_rpc_seconds", "HighAvailability", *g.promote_to_main_rpc_seconds);
  AppendHistogramPercentiles(
      out, "demote_main_to_replica_rpc_seconds", "HighAvailability", *g.demote_main_to_replica_rpc_seconds);
  AppendHistogramPercentiles(
      out, "register_replica_on_main_rpc_seconds", "HighAvailability", *g.register_replica_on_main_rpc_seconds);
  AppendHistogramPercentiles(
      out, "unregister_replica_rpc_seconds", "HighAvailability", *g.unregister_replica_rpc_seconds);
  AppendHistogramPercentiles(
      out, "enable_writing_on_main_rpc_seconds", "HighAvailability", *g.enable_writing_on_main_rpc_seconds);
  AppendHistogramPercentiles(out, "state_check_rpc_seconds", "HighAvailability", *g.state_check_rpc_seconds);
  AppendHistogramPercentiles(
      out, "get_database_histories_rpc_seconds", "HighAvailability", *g.get_database_histories_rpc_seconds);
  AppendHistogramPercentiles(out, "heartbeat_rpc_seconds", "HighAvailability", *g.heartbeat_rpc_seconds);
  AppendHistogramPercentiles(out, "prepare_commit_rpc_seconds", "HighAvailability", *g.prepare_commit_rpc_seconds);
  AppendHistogramPercentiles(out, "snapshot_rpc_seconds", "HighAvailability", *g.snapshot_rpc_seconds);
  AppendHistogramPercentiles(out, "current_wal_rpc_seconds", "HighAvailability", *g.current_wal_rpc_seconds);
  AppendHistogramPercentiles(out, "wal_files_rpc_seconds", "HighAvailability", *g.wal_files_rpc_seconds);
  AppendHistogramPercentiles(
      out, "frequent_heartbeat_rpc_seconds", "HighAvailability", *g.frequent_heartbeat_rpc_seconds);
  AppendHistogramPercentiles(out, "system_recovery_rpc_seconds", "HighAvailability", *g.system_recovery_rpc_seconds);
  AppendHistogramPercentiles(
      out, "update_data_instance_config_rpc_seconds", "HighAvailability", *g.update_data_instance_config_rpc_seconds);
  AppendHistogramPercentiles(out, "get_histories_seconds", "HighAvailability", *g.get_histories_seconds);

  return out;
}

PrometheusMetrics &Metrics() {
  static PrometheusMetrics instance;
  return instance;
}

}  // namespace memgraph::metrics
