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

#pragma once

#include <cstdint>
#include <expected>
#include <functional>
#include <list>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <variant>
#include <vector>

#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include "coordination/include/coordination/instance_status.hpp"

namespace memgraph::metrics {

struct MetricInfo {
  std::string name;
  std::string type;
  std::string metric_type;
  std::variant<int64_t, double> value;
};

struct StorageSnapshot {
  uint64_t vertex_count;
  uint64_t edge_count;
  uint64_t disk_usage;
  uint64_t memory_res;
};

/// Retrieves `StorageSnapshot` for the given `db_name`, or `std::nullopt` if
/// there is no such database.
using StorageSnapshotResolver = std::function<std::optional<StorageSnapshot>(std::string_view db_name)>;

#ifdef MG_ENTERPRISE
using InstanceStatusResolver = std::function<std::vector<coordination::InstanceStatus>()>;
#endif

struct DatabaseMetricHandles {
  // Storage
  prometheus::Gauge *vertex_count;
  prometheus::Gauge *edge_count;
  prometheus::Gauge *disk_usage_bytes;
  prometheus::Gauge *memory_res_bytes;

  // Operators
  prometheus::Counter *once_operator;
  prometheus::Counter *create_node_operator;
  prometheus::Counter *create_expand_operator;
  prometheus::Counter *scan_all_operator;
  prometheus::Counter *scan_all_by_label_operator;
  prometheus::Counter *scan_all_by_label_properties_operator;
  prometheus::Counter *scan_all_by_id_operator;
  prometheus::Counter *scan_all_by_edge_operator;
  prometheus::Counter *scan_all_by_edge_type_operator;
  prometheus::Counter *scan_all_by_edge_type_property_operator;
  prometheus::Counter *scan_all_by_edge_type_property_value_operator;
  prometheus::Counter *scan_all_by_edge_type_property_range_operator;
  prometheus::Counter *scan_all_by_edge_property_operator;
  prometheus::Counter *scan_all_by_edge_property_value_operator;
  prometheus::Counter *scan_all_by_edge_property_range_operator;
  prometheus::Counter *scan_all_by_edge_id_operator;
  prometheus::Counter *scan_all_by_point_distance_operator;
  prometheus::Counter *scan_all_by_point_withinbbox_operator;
  prometheus::Counter *expand_operator;
  prometheus::Counter *expand_variable_operator;
  prometheus::Counter *construct_named_path_operator;
  prometheus::Counter *filter_operator;
  prometheus::Counter *produce_operator;
  prometheus::Counter *delete_operator;
  prometheus::Counter *set_property_operator;
  prometheus::Counter *set_properties_operator;
  prometheus::Counter *set_labels_operator;
  prometheus::Counter *remove_property_operator;
  prometheus::Counter *remove_labels_operator;
  prometheus::Counter *edge_uniqueness_filter_operator;
  prometheus::Counter *empty_result_operator;
  prometheus::Counter *accumulate_operator;
  prometheus::Counter *aggregate_operator;
  prometheus::Counter *skip_operator;
  prometheus::Counter *limit_operator;
  prometheus::Counter *order_by_operator;
  prometheus::Counter *merge_operator;
  prometheus::Counter *optional_operator;
  prometheus::Counter *unwind_operator;
  prometheus::Counter *distinct_operator;
  prometheus::Counter *union_operator;
  prometheus::Counter *cartesian_operator;
  prometheus::Counter *call_procedure_operator;
  prometheus::Counter *foreach_operator;
  prometheus::Counter *evaluate_pattern_filter_operator;
  prometheus::Counter *apply_operator;
  prometheus::Counter *indexed_join_operator;
  prometheus::Counter *hash_join_operator;
  prometheus::Counter *roll_up_apply_operator;
  prometheus::Counter *periodic_commit_operator;
  prometheus::Counter *periodic_subquery_operator;
  prometheus::Counter *set_nested_property_operator;
  prometheus::Counter *remove_nested_property_operator;

  // Index
  prometheus::Gauge *active_label_indices;
  prometheus::Gauge *active_label_property_indices;
  prometheus::Gauge *active_edge_type_indices;
  prometheus::Gauge *active_edge_type_property_indices;
  prometheus::Gauge *active_edge_property_indices;
  prometheus::Gauge *active_point_indices;
  prometheus::Gauge *active_text_indices;
  prometheus::Gauge *active_text_edge_indices;
  prometheus::Gauge *active_vector_indices;
  prometheus::Gauge *active_vector_edge_indices;

  // Constraint
  prometheus::Gauge *active_existence_constraints;
  prometheus::Gauge *active_unique_constraints;
  prometheus::Gauge *active_type_constraints;

  // Stream
  prometheus::Counter *streams_created;
  prometheus::Counter *messages_consumed;

  // Trigger
  prometheus::Counter *triggers_created;
  prometheus::Counter *triggers_executed;

  // Transaction
  prometheus::Gauge *active_transactions;
  prometheus::Counter *committed_transactions;
  prometheus::Counter *rollbacked_transactions;
  prometheus::Counter *failed_query;
  prometheus::Counter *failed_prepare;
  prometheus::Counter *failed_pull;
  prometheus::Counter *successful_query;
  prometheus::Counter *write_write_conflicts;
  prometheus::Counter *transient_errors;
  prometheus::Gauge *unreleased_delta_objects;

  // Query type
  prometheus::Counter *read_query;
  prometheus::Counter *write_query;
  prometheus::Counter *read_write_query;

  // TTL
  prometheus::Counter *deleted_nodes;
  prometheus::Counter *deleted_edges;

  // SchemaInfo
  prometheus::Counter *show_schema;

  // Histograms
  prometheus::Histogram *query_execution_latency_seconds;
  prometheus::Histogram *snapshot_creation_latency_seconds;
  prometheus::Histogram *snapshot_recovery_latency_seconds;
  prometheus::Histogram *gc_latency_seconds;
  prometheus::Histogram *gc_skiplist_cleanup_latency_seconds;
};

struct GlobalMetricHandles {
  // Session
  prometheus::Gauge *active_sessions;
  prometheus::Gauge *active_bolt_sessions;
  prometheus::Gauge *active_tcp_sessions;
  prometheus::Gauge *active_ssl_sessions;
  prometheus::Gauge *active_websocket_sessions;
  prometheus::Counter *bolt_messages;

  // Memory
  prometheus::Gauge *peak_memory_res_bytes;

  // HighAvailability counters
  prometheus::Counter *successful_failovers;
  prometheus::Counter *raft_failed_failovers;
  prometheus::Counter *no_alive_instance_failed_failovers;
  prometheus::Counter *become_leader_success;
  prometheus::Counter *failed_to_become_leader;
  prometheus::Counter *show_instance;
  prometheus::Counter *show_instances;
  prometheus::Counter *demote_instance;
  prometheus::Counter *unregister_repl_instance;
  prometheus::Counter *remove_coord_instance;
  prometheus::Counter *replica_recovery_success;
  prometheus::Counter *replica_recovery_fail;
  prometheus::Counter *replica_recovery_skip;
  prometheus::Counter *state_check_rpc_success;
  prometheus::Counter *state_check_rpc_fail;
  prometheus::Counter *unregister_replica_rpc_success;
  prometheus::Counter *unregister_replica_rpc_fail;
  prometheus::Counter *enable_writing_on_main_rpc_success;
  prometheus::Counter *enable_writing_on_main_rpc_fail;
  prometheus::Counter *promote_to_main_rpc_success;
  prometheus::Counter *promote_to_main_rpc_fail;
  prometheus::Counter *demote_main_to_replica_rpc_success;
  prometheus::Counter *demote_main_to_replica_rpc_fail;
  prometheus::Counter *register_replica_on_main_rpc_success;
  prometheus::Counter *register_replica_on_main_rpc_fail;
  prometheus::Counter *swap_main_uuid_rpc_success;
  prometheus::Counter *swap_main_uuid_rpc_fail;
  prometheus::Counter *get_database_histories_rpc_success;
  prometheus::Counter *get_database_histories_rpc_fail;
  prometheus::Counter *update_data_instance_config_rpc_success;
  prometheus::Counter *update_data_instance_config_rpc_fail;

  // HA Histograms
  prometheus::Histogram *instance_succ_callback_seconds;
  prometheus::Histogram *instance_fail_callback_seconds;
  prometheus::Histogram *choose_most_up_to_date_instance_seconds;
  prometheus::Histogram *socket_connect_seconds;
  prometheus::Histogram *replica_stream_seconds;
  prometheus::Histogram *data_failover_seconds;
  prometheus::Histogram *start_txn_replication_seconds;
  prometheus::Histogram *finalize_txn_replication_seconds;
  prometheus::Histogram *promote_to_main_rpc_seconds;
  prometheus::Histogram *demote_main_to_replica_rpc_seconds;
  prometheus::Histogram *register_replica_on_main_rpc_seconds;
  prometheus::Histogram *unregister_replica_rpc_seconds;
  prometheus::Histogram *enable_writing_on_main_rpc_seconds;
  prometheus::Histogram *state_check_rpc_seconds;
  prometheus::Histogram *get_database_histories_rpc_seconds;
  prometheus::Histogram *heartbeat_rpc_seconds;
  prometheus::Histogram *prepare_commit_rpc_seconds;
  prometheus::Histogram *snapshot_rpc_seconds;
  prometheus::Histogram *current_wal_rpc_seconds;
  prometheus::Histogram *wal_files_rpc_seconds;
  prometheus::Histogram *frequent_heartbeat_rpc_seconds;
  prometheus::Histogram *system_recovery_rpc_seconds;
  prometheus::Histogram *update_data_instance_config_rpc_seconds;
  prometheus::Histogram *get_histories_seconds;
};

class PrometheusMetrics {
 public:
  PrometheusMetrics();

  PrometheusMetrics(PrometheusMetrics const &) = delete;
  PrometheusMetrics(PrometheusMetrics &&) = delete;
  PrometheusMetrics &operator=(PrometheusMetrics const &) = delete;
  PrometheusMetrics &operator=(PrometheusMetrics &&) = delete;
  ~PrometheusMetrics() = default;

  DatabaseMetricHandles *AddDatabase(std::string_view db_name);
  void RemoveDatabase(DatabaseMetricHandles const *handles);
  void UpdateGauges();

  void SetStorageSnapshotResolver(StorageSnapshotResolver resolver);
#ifdef MG_ENTERPRISE
  void SetInstanceStatusResolver(InstanceStatusResolver resolver);
#endif

  std::expected<std::vector<MetricInfo>, std::string> GetDbMetricsInfo(std::string_view db_name) const;
  std::vector<MetricInfo> GetGlobalMetricsInfo() const;
  std::vector<MetricInfo> GetGlobalMetricsInfoForLegacyJson();

  prometheus::Registry &registry() { return registry_; }

  GlobalMetricHandles global;

 private:
  struct DatabaseEntry {
    std::string db_name;
    DatabaseMetricHandles handles;
    // Ref count to handle multiple Database instances with the same name
    // sharing the same prometheus objects. This occurs only in unit tests that
    // simulate multi-node setups (e..g. main + replicas) in-process.
    uint32_t ref_count{1};
  };

  StorageSnapshot ResolveStorageSnapshot(std::string_view db_name) const;

  prometheus::Registry registry_;
  mutable std::shared_mutex databases_mutex_;
  mutable std::mutex legacy_json_delta_mutex_;
  std::unordered_map<std::string, int64_t> legacy_json_prev_ha_counter_values_;
  mutable std::mutex snapshot_resolver_mutex_;
  StorageSnapshotResolver storage_snapshot_resolver_;
#ifdef MG_ENTERPRISE
  mutable std::mutex instance_resolver_mutex_;
  InstanceStatusResolver instance_status_resolver_;
#endif
  std::list<DatabaseEntry> databases_;

  // Per-database metric families — storage
  prometheus::Family<prometheus::Gauge> &vertex_count_family_;
  prometheus::Family<prometheus::Gauge> &edge_count_family_;
  prometheus::Family<prometheus::Gauge> &disk_usage_family_;
  prometheus::Family<prometheus::Gauge> &memory_res_family_;

  // Per-database metric families — transaction (partial: active/committed/rollbacked/failed)
  prometheus::Family<prometheus::Gauge> &active_transactions_family_;
  prometheus::Family<prometheus::Counter> &committed_transactions_family_;
  prometheus::Family<prometheus::Counter> &rollbacked_transactions_family_;
  prometheus::Family<prometheus::Counter> &failed_query_family_;

  // Per-database metric families — query type
  prometheus::Family<prometheus::Counter> &read_query_family_;
  prometheus::Family<prometheus::Counter> &write_query_family_;
  prometheus::Family<prometheus::Counter> &read_write_query_family_;

  // Per-database metric families — operators
  prometheus::Family<prometheus::Counter> &once_operator_family_;
  prometheus::Family<prometheus::Counter> &create_node_operator_family_;
  prometheus::Family<prometheus::Counter> &create_expand_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_label_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_label_properties_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_id_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_edge_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_edge_type_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_edge_type_property_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_edge_type_property_value_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_edge_type_property_range_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_edge_property_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_edge_property_value_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_edge_property_range_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_edge_id_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_point_distance_operator_family_;
  prometheus::Family<prometheus::Counter> &scan_all_by_point_withinbbox_operator_family_;
  prometheus::Family<prometheus::Counter> &expand_operator_family_;
  prometheus::Family<prometheus::Counter> &expand_variable_operator_family_;
  prometheus::Family<prometheus::Counter> &construct_named_path_operator_family_;
  prometheus::Family<prometheus::Counter> &filter_operator_family_;
  prometheus::Family<prometheus::Counter> &produce_operator_family_;
  prometheus::Family<prometheus::Counter> &delete_operator_family_;
  prometheus::Family<prometheus::Counter> &set_property_operator_family_;
  prometheus::Family<prometheus::Counter> &set_properties_operator_family_;
  prometheus::Family<prometheus::Counter> &set_labels_operator_family_;
  prometheus::Family<prometheus::Counter> &remove_property_operator_family_;
  prometheus::Family<prometheus::Counter> &remove_labels_operator_family_;
  prometheus::Family<prometheus::Counter> &edge_uniqueness_filter_operator_family_;
  prometheus::Family<prometheus::Counter> &empty_result_operator_family_;
  prometheus::Family<prometheus::Counter> &accumulate_operator_family_;
  prometheus::Family<prometheus::Counter> &aggregate_operator_family_;
  prometheus::Family<prometheus::Counter> &skip_operator_family_;
  prometheus::Family<prometheus::Counter> &limit_operator_family_;
  prometheus::Family<prometheus::Counter> &order_by_operator_family_;
  prometheus::Family<prometheus::Counter> &merge_operator_family_;
  prometheus::Family<prometheus::Counter> &optional_operator_family_;
  prometheus::Family<prometheus::Counter> &unwind_operator_family_;
  prometheus::Family<prometheus::Counter> &distinct_operator_family_;
  prometheus::Family<prometheus::Counter> &union_operator_family_;
  prometheus::Family<prometheus::Counter> &cartesian_operator_family_;
  prometheus::Family<prometheus::Counter> &call_procedure_operator_family_;
  prometheus::Family<prometheus::Counter> &foreach_operator_family_;
  prometheus::Family<prometheus::Counter> &evaluate_pattern_filter_operator_family_;
  prometheus::Family<prometheus::Counter> &apply_operator_family_;
  prometheus::Family<prometheus::Counter> &indexed_join_operator_family_;
  prometheus::Family<prometheus::Counter> &hash_join_operator_family_;
  prometheus::Family<prometheus::Counter> &roll_up_apply_operator_family_;
  prometheus::Family<prometheus::Counter> &periodic_commit_operator_family_;
  prometheus::Family<prometheus::Counter> &periodic_subquery_operator_family_;
  prometheus::Family<prometheus::Counter> &set_nested_property_operator_family_;
  prometheus::Family<prometheus::Counter> &remove_nested_property_operator_family_;

  // Per-database metric families — index
  prometheus::Family<prometheus::Gauge> &active_label_indices_family_;
  prometheus::Family<prometheus::Gauge> &active_label_property_indices_family_;
  prometheus::Family<prometheus::Gauge> &active_edge_type_indices_family_;
  prometheus::Family<prometheus::Gauge> &active_edge_type_property_indices_family_;
  prometheus::Family<prometheus::Gauge> &active_edge_property_indices_family_;
  prometheus::Family<prometheus::Gauge> &active_point_indices_family_;
  prometheus::Family<prometheus::Gauge> &active_text_indices_family_;
  prometheus::Family<prometheus::Gauge> &active_text_edge_indices_family_;
  prometheus::Family<prometheus::Gauge> &active_vector_indices_family_;
  prometheus::Family<prometheus::Gauge> &active_vector_edge_indices_family_;

  // Per-database metric families — constraint
  prometheus::Family<prometheus::Gauge> &active_existence_constraints_family_;
  prometheus::Family<prometheus::Gauge> &active_unique_constraints_family_;
  prometheus::Family<prometheus::Gauge> &active_type_constraints_family_;

  // Per-database metric families — stream
  prometheus::Family<prometheus::Counter> &streams_created_family_;
  prometheus::Family<prometheus::Counter> &messages_consumed_family_;

  // Per-database metric families — trigger
  prometheus::Family<prometheus::Counter> &triggers_created_family_;
  prometheus::Family<prometheus::Counter> &triggers_executed_family_;

  // Global metric families — session
  prometheus::Family<prometheus::Gauge> &active_sessions_family_;
  prometheus::Family<prometheus::Gauge> &active_bolt_sessions_family_;
  prometheus::Family<prometheus::Gauge> &active_tcp_sessions_family_;
  prometheus::Family<prometheus::Gauge> &active_ssl_sessions_family_;
  prometheus::Family<prometheus::Gauge> &active_websocket_sessions_family_;
  prometheus::Family<prometheus::Counter> &bolt_messages_family_;

  // Per-database metric families — transaction (remainder)
  prometheus::Family<prometheus::Counter> &failed_prepare_family_;
  prometheus::Family<prometheus::Counter> &failed_pull_family_;
  prometheus::Family<prometheus::Counter> &successful_query_family_;
  prometheus::Family<prometheus::Counter> &write_write_conflicts_family_;
  prometheus::Family<prometheus::Counter> &transient_errors_family_;
  prometheus::Family<prometheus::Gauge> &unreleased_delta_objects_family_;

  // Per-database metric families — TTL
  prometheus::Family<prometheus::Counter> &deleted_nodes_family_;
  prometheus::Family<prometheus::Counter> &deleted_edges_family_;

  // Per-database metric families — schema info
  prometheus::Family<prometheus::Counter> &show_schema_family_;

  // Global metric families — memory
  prometheus::Family<prometheus::Gauge> &peak_memory_res_family_;

  // Global metric families — HA counters
  prometheus::Family<prometheus::Counter> &successful_failovers_family_;
  prometheus::Family<prometheus::Counter> &raft_failed_failovers_family_;
  prometheus::Family<prometheus::Counter> &no_alive_instance_failed_failovers_family_;
  prometheus::Family<prometheus::Counter> &become_leader_success_family_;
  prometheus::Family<prometheus::Counter> &failed_to_become_leader_family_;
  prometheus::Family<prometheus::Counter> &show_instance_family_;
  prometheus::Family<prometheus::Counter> &show_instances_family_;
  prometheus::Family<prometheus::Counter> &demote_instance_family_;
  prometheus::Family<prometheus::Counter> &unregister_repl_instance_family_;
  prometheus::Family<prometheus::Counter> &remove_coord_instance_family_;
  prometheus::Family<prometheus::Counter> &replica_recovery_success_family_;
  prometheus::Family<prometheus::Counter> &replica_recovery_fail_family_;
  prometheus::Family<prometheus::Counter> &replica_recovery_skip_family_;
  prometheus::Family<prometheus::Counter> &state_check_rpc_success_family_;
  prometheus::Family<prometheus::Counter> &state_check_rpc_fail_family_;
  prometheus::Family<prometheus::Counter> &unregister_replica_rpc_success_family_;
  prometheus::Family<prometheus::Counter> &unregister_replica_rpc_fail_family_;
  prometheus::Family<prometheus::Counter> &enable_writing_on_main_rpc_success_family_;
  prometheus::Family<prometheus::Counter> &enable_writing_on_main_rpc_fail_family_;
  prometheus::Family<prometheus::Counter> &promote_to_main_rpc_success_family_;
  prometheus::Family<prometheus::Counter> &promote_to_main_rpc_fail_family_;
  prometheus::Family<prometheus::Counter> &demote_main_to_replica_rpc_success_family_;
  prometheus::Family<prometheus::Counter> &demote_main_to_replica_rpc_fail_family_;
  prometheus::Family<prometheus::Counter> &register_replica_on_main_rpc_success_family_;
  prometheus::Family<prometheus::Counter> &register_replica_on_main_rpc_fail_family_;
  prometheus::Family<prometheus::Counter> &swap_main_uuid_rpc_success_family_;
  prometheus::Family<prometheus::Counter> &swap_main_uuid_rpc_fail_family_;
  prometheus::Family<prometheus::Counter> &get_database_histories_rpc_success_family_;
  prometheus::Family<prometheus::Counter> &get_database_histories_rpc_fail_family_;
  prometheus::Family<prometheus::Counter> &update_data_instance_config_rpc_success_family_;
  prometheus::Family<prometheus::Counter> &update_data_instance_config_rpc_fail_family_;

  // Per-database metric families — histograms
  prometheus::Family<prometheus::Histogram> &query_execution_latency_family_;
  prometheus::Family<prometheus::Histogram> &snapshot_creation_latency_family_;
  prometheus::Family<prometheus::Histogram> &snapshot_recovery_latency_family_;

  // Global metric families — HA histograms
  prometheus::Family<prometheus::Histogram> &instance_succ_callback_family_;
  prometheus::Family<prometheus::Histogram> &instance_fail_callback_family_;
  prometheus::Family<prometheus::Histogram> &choose_most_up_to_date_instance_family_;
  prometheus::Family<prometheus::Histogram> &socket_connect_family_;
  prometheus::Family<prometheus::Histogram> &replica_stream_family_;
  prometheus::Family<prometheus::Histogram> &data_failover_family_;
  prometheus::Family<prometheus::Histogram> &start_txn_replication_family_;
  prometheus::Family<prometheus::Histogram> &finalize_txn_replication_family_;
  prometheus::Family<prometheus::Histogram> &promote_to_main_rpc_histogram_family_;
  prometheus::Family<prometheus::Histogram> &demote_main_to_replica_rpc_histogram_family_;
  prometheus::Family<prometheus::Histogram> &register_replica_on_main_rpc_histogram_family_;
  prometheus::Family<prometheus::Histogram> &unregister_replica_rpc_histogram_family_;
  prometheus::Family<prometheus::Histogram> &enable_writing_on_main_rpc_histogram_family_;
  prometheus::Family<prometheus::Histogram> &state_check_rpc_histogram_family_;
  prometheus::Family<prometheus::Histogram> &get_database_histories_rpc_histogram_family_;
  prometheus::Family<prometheus::Histogram> &heartbeat_rpc_family_;
  prometheus::Family<prometheus::Histogram> &prepare_commit_rpc_family_;
  prometheus::Family<prometheus::Histogram> &snapshot_rpc_family_;
  prometheus::Family<prometheus::Histogram> &current_wal_rpc_family_;
  prometheus::Family<prometheus::Histogram> &wal_files_rpc_family_;
  prometheus::Family<prometheus::Histogram> &frequent_heartbeat_rpc_family_;
  prometheus::Family<prometheus::Histogram> &system_recovery_rpc_family_;
  prometheus::Family<prometheus::Histogram> &update_data_instance_config_rpc_histogram_family_;
  prometheus::Family<prometheus::Histogram> &get_histories_family_;

  // Per-database metric families — GC histograms
  prometheus::Family<prometheus::Histogram> &gc_latency_family_;
  prometheus::Family<prometheus::Histogram> &gc_skiplist_cleanup_latency_family_;

#ifdef MG_ENTERPRISE
  // Global metric families — HA instance status
  prometheus::Family<prometheus::Gauge> &instance_up_family_;
  prometheus::Family<prometheus::Gauge> &instance_is_leader_family_;
  prometheus::Family<prometheus::Gauge> &instance_is_main_family_;
  prometheus::Family<prometheus::Gauge> &instance_last_response_ms_family_;
  // Tracks currently registered instance gauges by instance_name for lifecycle management
  std::unordered_map<std::string, prometheus::Gauge *> instance_up_gauges_;
  std::unordered_map<std::string, prometheus::Gauge *> instance_is_leader_gauges_;
  std::unordered_map<std::string, prometheus::Gauge *> instance_is_main_gauges_;
  std::unordered_map<std::string, prometheus::Gauge *> instance_last_response_ms_gauges_;
#endif
};

PrometheusMetrics &Metrics();

}  // namespace memgraph::metrics
