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

#include <prometheus/detail/builder.h>

#include "utils/logging.hpp"

namespace memgraph::metrics {

namespace {

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
      // Transaction (global)
      global_active_transactions_family_{prometheus::BuildGauge()
                                             .Name("memgraph_global_active_transactions")
                                             .Help("Total number of active transactions globally")
                                             .Register(registry_)},
      global_committed_transactions_family_{prometheus::BuildCounter()
                                                .Name("memgraph_global_committed_transactions_total")
                                                .Help("Total number of committed transactions globally")
                                                .Register(registry_)},
      global_rollbacked_transactions_family_{prometheus::BuildCounter()
                                                 .Name("memgraph_global_rollbacked_transactions_total")
                                                 .Help("Total number of rollbacked transactions globally")
                                                 .Register(registry_)},
      global_failed_query_family_{prometheus::BuildCounter()
                                      .Name("memgraph_global_failed_queries_total")
                                      .Help("Total number of failed queries globally")
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
  // Populate GlobalMetricHandles — all global metrics have no labels
  prometheus::Labels const no_labels{};

  global.once_operator = &once_operator_family_.Add(no_labels);
  global.create_node_operator = &create_node_operator_family_.Add(no_labels);
  global.create_expand_operator = &create_expand_operator_family_.Add(no_labels);
  global.scan_all_operator = &scan_all_operator_family_.Add(no_labels);
  global.scan_all_by_label_operator = &scan_all_by_label_operator_family_.Add(no_labels);
  global.scan_all_by_label_properties_operator = &scan_all_by_label_properties_operator_family_.Add(no_labels);
  global.scan_all_by_id_operator = &scan_all_by_id_operator_family_.Add(no_labels);
  global.scan_all_by_edge_operator = &scan_all_by_edge_operator_family_.Add(no_labels);
  global.scan_all_by_edge_type_operator = &scan_all_by_edge_type_operator_family_.Add(no_labels);
  global.scan_all_by_edge_type_property_operator = &scan_all_by_edge_type_property_operator_family_.Add(no_labels);
  global.scan_all_by_edge_type_property_value_operator =
      &scan_all_by_edge_type_property_value_operator_family_.Add(no_labels);
  global.scan_all_by_edge_type_property_range_operator =
      &scan_all_by_edge_type_property_range_operator_family_.Add(no_labels);
  global.scan_all_by_edge_property_operator = &scan_all_by_edge_property_operator_family_.Add(no_labels);
  global.scan_all_by_edge_property_value_operator = &scan_all_by_edge_property_value_operator_family_.Add(no_labels);
  global.scan_all_by_edge_property_range_operator = &scan_all_by_edge_property_range_operator_family_.Add(no_labels);
  global.scan_all_by_edge_id_operator = &scan_all_by_edge_id_operator_family_.Add(no_labels);
  global.scan_all_by_point_distance_operator = &scan_all_by_point_distance_operator_family_.Add(no_labels);
  global.scan_all_by_point_withinbbox_operator = &scan_all_by_point_withinbbox_operator_family_.Add(no_labels);
  global.expand_operator = &expand_operator_family_.Add(no_labels);
  global.expand_variable_operator = &expand_variable_operator_family_.Add(no_labels);
  global.construct_named_path_operator = &construct_named_path_operator_family_.Add(no_labels);
  global.filter_operator = &filter_operator_family_.Add(no_labels);
  global.produce_operator = &produce_operator_family_.Add(no_labels);
  global.delete_operator = &delete_operator_family_.Add(no_labels);
  global.set_property_operator = &set_property_operator_family_.Add(no_labels);
  global.set_properties_operator = &set_properties_operator_family_.Add(no_labels);
  global.set_labels_operator = &set_labels_operator_family_.Add(no_labels);
  global.remove_property_operator = &remove_property_operator_family_.Add(no_labels);
  global.remove_labels_operator = &remove_labels_operator_family_.Add(no_labels);
  global.edge_uniqueness_filter_operator = &edge_uniqueness_filter_operator_family_.Add(no_labels);
  global.empty_result_operator = &empty_result_operator_family_.Add(no_labels);
  global.accumulate_operator = &accumulate_operator_family_.Add(no_labels);
  global.aggregate_operator = &aggregate_operator_family_.Add(no_labels);
  global.skip_operator = &skip_operator_family_.Add(no_labels);
  global.limit_operator = &limit_operator_family_.Add(no_labels);
  global.order_by_operator = &order_by_operator_family_.Add(no_labels);
  global.merge_operator = &merge_operator_family_.Add(no_labels);
  global.optional_operator = &optional_operator_family_.Add(no_labels);
  global.unwind_operator = &unwind_operator_family_.Add(no_labels);
  global.distinct_operator = &distinct_operator_family_.Add(no_labels);
  global.union_operator = &union_operator_family_.Add(no_labels);
  global.cartesian_operator = &cartesian_operator_family_.Add(no_labels);
  global.call_procedure_operator = &call_procedure_operator_family_.Add(no_labels);
  global.foreach_operator = &foreach_operator_family_.Add(no_labels);
  global.evaluate_pattern_filter_operator = &evaluate_pattern_filter_operator_family_.Add(no_labels);
  global.apply_operator = &apply_operator_family_.Add(no_labels);
  global.indexed_join_operator = &indexed_join_operator_family_.Add(no_labels);
  global.hash_join_operator = &hash_join_operator_family_.Add(no_labels);
  global.roll_up_apply_operator = &roll_up_apply_operator_family_.Add(no_labels);
  global.periodic_commit_operator = &periodic_commit_operator_family_.Add(no_labels);
  global.periodic_subquery_operator = &periodic_subquery_operator_family_.Add(no_labels);
  global.set_nested_property_operator = &set_nested_property_operator_family_.Add(no_labels);
  global.remove_nested_property_operator = &remove_nested_property_operator_family_.Add(no_labels);

  global.active_label_indices = &active_label_indices_family_.Add(no_labels);
  global.active_label_property_indices = &active_label_property_indices_family_.Add(no_labels);
  global.active_edge_type_indices = &active_edge_type_indices_family_.Add(no_labels);
  global.active_edge_type_property_indices = &active_edge_type_property_indices_family_.Add(no_labels);
  global.active_edge_property_indices = &active_edge_property_indices_family_.Add(no_labels);
  global.active_point_indices = &active_point_indices_family_.Add(no_labels);
  global.active_text_indices = &active_text_indices_family_.Add(no_labels);
  global.active_text_edge_indices = &active_text_edge_indices_family_.Add(no_labels);
  global.active_vector_indices = &active_vector_indices_family_.Add(no_labels);
  global.active_vector_edge_indices = &active_vector_edge_indices_family_.Add(no_labels);

  global.active_existence_constraints = &active_existence_constraints_family_.Add(no_labels);
  global.active_unique_constraints = &active_unique_constraints_family_.Add(no_labels);
  global.active_type_constraints = &active_type_constraints_family_.Add(no_labels);

  global.streams_created = &streams_created_family_.Add(no_labels);
  global.messages_consumed = &messages_consumed_family_.Add(no_labels);

  global.triggers_created = &triggers_created_family_.Add(no_labels);
  global.triggers_executed = &triggers_executed_family_.Add(no_labels);

  global.active_sessions = &active_sessions_family_.Add(no_labels);
  global.active_bolt_sessions = &active_bolt_sessions_family_.Add(no_labels);
  global.active_tcp_sessions = &active_tcp_sessions_family_.Add(no_labels);
  global.active_ssl_sessions = &active_ssl_sessions_family_.Add(no_labels);
  global.active_websocket_sessions = &active_websocket_sessions_family_.Add(no_labels);
  global.bolt_messages = &bolt_messages_family_.Add(no_labels);

  global.active_transactions = &global_active_transactions_family_.Add(no_labels);
  global.committed_transactions = &global_committed_transactions_family_.Add(no_labels);
  global.rollbacked_transactions = &global_rollbacked_transactions_family_.Add(no_labels);
  global.failed_query = &global_failed_query_family_.Add(no_labels);
  global.failed_prepare = &failed_prepare_family_.Add(no_labels);
  global.failed_pull = &failed_pull_family_.Add(no_labels);
  global.successful_query = &successful_query_family_.Add(no_labels);
  global.write_write_conflicts = &write_write_conflicts_family_.Add(no_labels);
  global.transient_errors = &transient_errors_family_.Add(no_labels);
  global.unreleased_delta_objects = &unreleased_delta_objects_family_.Add(no_labels);

  global.deleted_nodes = &deleted_nodes_family_.Add(no_labels);
  global.deleted_edges = &deleted_edges_family_.Add(no_labels);

  global.show_schema = &show_schema_family_.Add(no_labels);

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

  global.query_execution_latency_seconds = &query_execution_latency_family_.Add(no_labels, kLatencyBuckets);
  global.snapshot_creation_latency_seconds = &snapshot_creation_latency_family_.Add(no_labels, kLatencyBuckets);
  global.snapshot_recovery_latency_seconds = &snapshot_recovery_latency_family_.Add(no_labels, kLatencyBuckets);
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
  global.gc_latency_seconds = &gc_latency_family_.Add(no_labels, kLatencyBuckets);
  global.gc_skiplist_cleanup_latency_seconds = &gc_skiplist_cleanup_latency_family_.Add(no_labels, kLatencyBuckets);
}

DatabaseMetricHandles *PrometheusMetrics::AddDatabase(std::string_view db_name,
                                                      std::function<StorageSnapshot()> get_snapshot) {
  prometheus::Labels const labels{{"database", std::string(db_name)}};
  MG_ASSERT(!vertex_count_family_.Has(labels), "Database '{}' already registered in PrometheusMetrics", db_name);
  databases_.push_back({
      .handles =
          DatabaseMetricHandles{
              .vertex_count = &vertex_count_family_.Add(labels),
              .edge_count = &edge_count_family_.Add(labels),
              .disk_usage_bytes = &disk_usage_family_.Add(labels),
              .memory_res_bytes = &memory_res_family_.Add(labels),
              .active_transactions = &active_transactions_family_.Add(labels),
              .committed_transactions = &committed_transactions_family_.Add(labels),
              .rollbacked_transactions = &rollbacked_transactions_family_.Add(labels),
              .failed_query = &failed_query_family_.Add(labels),
              .read_query = &read_query_family_.Add(labels),
              .write_query = &write_query_family_.Add(labels),
              .read_write_query = &read_write_query_family_.Add(labels),
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
  active_transactions_family_.Remove(h.active_transactions);
  committed_transactions_family_.Remove(h.committed_transactions);
  rollbacked_transactions_family_.Remove(h.rollbacked_transactions);
  failed_query_family_.Remove(h.failed_query);
  read_query_family_.Remove(h.read_query);
  write_query_family_.Remove(h.write_query);
  read_write_query_family_.Remove(h.read_write_query);
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

}  // namespace memgraph::metrics
