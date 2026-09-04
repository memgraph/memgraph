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
#include <nlohmann/json_fwd.hpp>

#include "metrics/metric_handles.hpp"
#include "utils/uuid.hpp"

#ifdef MG_ENTERPRISE
namespace memgraph::coordination {
struct InstanceStatus;
}
#endif

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
  int64_t db_memory_tracked;
  int64_t db_peak_memory_tracked;
  int64_t db_storage_memory_tracked;
  int64_t db_embedding_memory_tracked;
  int64_t db_query_memory_tracked;
};

struct string_hash {
  using is_transparent = void;

  [[nodiscard]] size_t operator()(const char *txt) const { return std::hash<std::string_view>{}(txt); }

  [[nodiscard]] size_t operator()(std::string_view txt) const { return std::hash<std::string_view>{}(txt); }

  [[nodiscard]] size_t operator()(std::string const &txt) const { return std::hash<std::string>{}(txt); }
};

struct DurabilityThroughput {
  mutable std::mutex mutex;
  std::unordered_map<std::string, prometheus::Histogram *, string_hash, std::equal_to<>> by_instance;
};

/// Retrieves `StorageSnapshot` for the given database UUID, or `std::nullopt`
/// if there is no such database.
using StorageSnapshotResolver = std::function<std::optional<StorageSnapshot>(utils::UUID const &uuid)>;

#ifdef MG_ENTERPRISE
using InstanceStatusResolver = std::function<std::vector<coordination::InstanceStatus>()>;
#endif

struct GlobalMetricHandles {
  // Session
  prometheus::Gauge *active_sessions;
  prometheus::Gauge *active_bolt_sessions;
  prometheus::Gauge *active_tcp_sessions;
  prometheus::Gauge *active_ssl_sessions;
  prometheus::Gauge *active_websocket_sessions;
  prometheus::Counter *bolt_messages;

  // Memory
  prometheus::Gauge *memory_res_bytes;
  prometheus::Gauge *peak_memory_res_bytes;

  // Hot/cold tenants (global — the cold set is process-wide, not per-database since a COLD tenant has no
  // live storage to attribute a per-db series to). suspends/resumes count user-driven operations; the
  // gauge tracks the current COLD set size.
  prometheus::Counter *database_suspends;
  prometheus::Counter *database_resumes;
  prometheus::Gauge *cold_databases;
  // Wall-clock latency of a successful SUSPEND / RESUME (observed once per successful operation, on the
  // same path as the counters above). Global for the same reason: a COLD tenant has no live storage.
  prometheus::Histogram *database_suspend_latency_seconds;
  prometheus::Histogram *database_resume_latency_seconds;

  // Transaction (global) — incremented when no per-db context is available
  prometheus::Counter *transient_errors;
  prometheus::Counter *failed_query;
  prometheus::Counter *failed_prepare;
  prometheus::Counter *failed_pull;
  prometheus::Counter *successful_query;
  prometheus::Counter *read_query;
  prometheus::Counter *write_query;
  prometheus::Counter *read_write_query;

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

  // StorageInfo global/system level
  prometheus::Counter *show_storage_info;
};

class PrometheusMetrics {
 public:
  PrometheusMetrics();

  PrometheusMetrics(PrometheusMetrics const &) = delete;
  PrometheusMetrics(PrometheusMetrics &&) = delete;
  PrometheusMetrics &operator=(PrometheusMetrics const &) = delete;
  PrometheusMetrics &operator=(PrometheusMetrics &&) = delete;
  ~PrometheusMetrics() = default;

  /// Owns one registration of a database's metrics and releases it on destruction. Move-only, so
  /// exactly one object releases each registration. It releases the entry it registered rather than
  /// one matching a uuid or a name, so a database that is renamed or rebound still releases its own.
  class Registration {
   public:
    Registration() = default;
    ~Registration();

    Registration(Registration &&other) noexcept;
    Registration &operator=(Registration &&other) noexcept;
    Registration(Registration const &) = delete;
    Registration &operator=(Registration const &) = delete;

    DatabaseMetricHandles const &handles() const { return handles_; }

    DatabaseMetricHandles &handles() { return handles_; }

   private:
    friend class PrometheusMetrics;

    Registration(PrometheusMetrics *registry, uint64_t entry_id, DatabaseMetricHandles handles)
        : registry_(registry), entry_id_(entry_id), handles_(std::move(handles)) {}

    void Release() noexcept;

    PrometheusMetrics *registry_{nullptr};
    uint64_t entry_id_{0};
    DatabaseMetricHandles handles_{};
  };

  [[nodiscard]] Registration AddDatabase(utils::UUID const &uuid, std::string_view name);

  /// Relabels the default database's entry onto @p new_uuid. The metric objects stay put, so
  /// every outstanding handle, ScopedGauge and delta_container keeps pointing at a live object.
  DatabaseMetricHandles RebindDefaultDatabaseUUID(utils::UUID const &new_uuid);

  /// Refresh any gauges whose values are pulled from current storage state,
  /// rather than updated at point of use.
  void UpdateGauges();

  /// Thread-safe update of the global peak_memory_res_bytes gauge.
  /// Sets the gauge to max(current, previous) and returns the new peak.
  uint64_t UpdateAndGetPeakMemoryRes(uint64_t current) const;
  void ObserveSnapshotThroughput(std::string const &instance_name, double bytes_per_second);
  void ObserveWalThroughput(std::string const &instance_name, double bytes_per_second);

  // Drops the per-instance replication throughput series (snapshot + WAL) for `instance_name`. Call when a replica is
  // unregistered so the throughput maps and the registry don't grow unbounded across the main's lifetime.
  void RemoveReplicationThroughput(std::string_view instance_name);

  void SetStorageSnapshotResolver(StorageSnapshotResolver resolver);
#ifdef MG_ENTERPRISE
  void SetInstanceStatusResolver(InstanceStatusResolver resolver);
#endif

  // Returns metrics for the current database for SHOW METRICS INFO.
  std::expected<std::vector<MetricInfo>, std::string> GetDbMetricsInfo(utils::UUID const &uuid) const;

  // Returns truly global metrics: session gauges, HA counters/histograms, and peak memory.
  std::vector<MetricInfo> GetGlobalMetricsInfo() const;

  // Returns metrics for the legacy JSON endpoint. For backwards compatibility,
  // storage fields (vertex/edge count, disk/memory usage) reflect the default
  // database only. All other per-db counters and histograms are aggregated
  // across all databases, matching the pre-multi-tenant behaviour where a
  // single global counter tracked the entire process.
  std::vector<MetricInfo> GetGlobalMetricsInfoForJson();

  nlohmann::json GetTelemetryCounters() const;

  /// Collects every family for a scrape, substituting each per-database entry's current uuid for the
  /// internal entry-id label. This is the only way out of the registry, because the entry-id label
  /// keys the families internally and must never be exposed.
  std::vector<prometheus::MetricFamily> CollectForScrape();

  GlobalMetricHandles global;

 private:
  struct DatabaseEntry {
    // Identifies the entry for its whole life, unlike the uuid and the name, either of which can
    // change while registrations are outstanding.
    uint64_t id;
    // Used for every lookup, and substituted into the scrape output by CollectForScrape. The
    // default database's uuid changes when the instance joins a cluster, and the metric objects
    // must outlive that change, so it is presented at collection time rather than baked into the
    // family key.
    utils::UUID uuid;
    std::string db_name;
    DatabaseMetricHandles handles;
    // Registrations of one database share every handle, because a family returns the metric it
    // already holds for a label set. The metrics go when the last registration does.
    std::size_t registrations{1};
  };

  /// Drops one registration of the entry, and the metrics with the last of them.
  void ReleaseRegistration(uint64_t entry_id);

  // Caller must hold databases_.mutex.
  DatabaseMetricHandles CreateHandles(std::string_view name, uint64_t entry_id);
  void RemoveHandlesFromFamilies(DatabaseMetricHandles const &h);
  void RemoveEntryAt(std::list<DatabaseEntry>::iterator it);
  DatabaseMetricHandles AddDatabaseUnsafe(utils::UUID const &uuid, std::string_view name);

  StorageSnapshot ResolveStorageSnapshot(utils::UUID const &uuid) const;

  prometheus::Registry registry_;

  struct {
    mutable std::shared_mutex mutex;
    std::list<DatabaseEntry> entries;
    uint64_t next_entry_id{1};
  } databases_;

  std::unordered_map<std::string, int64_t> legacy_json_prev_ha_counter_values_;
  StorageSnapshotResolver storage_snapshot_resolver_;
  std::optional<utils::UUID> default_db_uuid_;
#ifdef MG_ENTERPRISE
  InstanceStatusResolver instance_status_resolver_;
#endif

  // Per-database metric families — storage
  prometheus::Family<prometheus::Gauge> &vertex_count_family_;
  prometheus::Family<prometheus::Gauge> &edge_count_family_;
  prometheus::Family<prometheus::Gauge> &disk_usage_family_;
  prometheus::Family<prometheus::Gauge> &db_memory_tracked_family_;
  prometheus::Family<prometheus::Gauge> &db_peak_memory_tracked_family_;
  prometheus::Family<prometheus::Gauge> &db_storage_memory_tracked_family_;
  prometheus::Family<prometheus::Gauge> &db_embedding_memory_tracked_family_;
  prometheus::Family<prometheus::Gauge> &db_query_memory_tracked_family_;

  // Per-database metric families — transaction (partial: active/committed/rolled_back/failed)
  prometheus::Family<prometheus::Gauge> &active_transactions_family_;
  prometheus::Family<prometheus::Counter> &committed_transactions_family_;
  prometheus::Family<prometheus::Counter> &rolled_back_transactions_family_;
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
  prometheus::Family<prometheus::Counter> &scan_all_by_vertex_property_operator_family_;
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
  prometheus::Family<prometheus::Gauge> &active_vertex_property_indices_family_;
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

  // Per-database metric families — storage info
  prometheus::Family<prometheus::Counter> &show_storage_info_family_;

  // Global metric families — memory
  prometheus::Family<prometheus::Gauge> &memory_res_family_;
  prometheus::Family<prometheus::Gauge> &peak_memory_res_family_;

  // Global metric families — hot/cold databases
  prometheus::Family<prometheus::Counter> &database_suspends_family_;
  prometheus::Family<prometheus::Counter> &database_resumes_family_;
  prometheus::Family<prometheus::Gauge> &cold_databases_family_;
  prometheus::Family<prometheus::Histogram> &database_suspend_latency_family_;
  prometheus::Family<prometheus::Histogram> &database_resume_latency_family_;

  // No separate global families needed — global no-db counters reuse the per-db families with no label

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
  prometheus::Family<prometheus::Counter> &gc_index_sweeps_family_;

  // Global metric family — per-instance snapshot throughput (bytes/s)
  prometheus::Family<prometheus::Histogram> &snapshot_throughput_family_;
  prometheus::Family<prometheus::Histogram> &wal_throughput_family_;

  DurabilityThroughput snapshot_throughput_;
  DurabilityThroughput wal_throughput_;

#ifdef MG_ENTERPRISE
  // Global metric families — HA instance status
  prometheus::Family<prometheus::Gauge> &instance_up_family_;
  prometheus::Family<prometheus::Gauge> &instance_is_leader_family_;
  prometheus::Family<prometheus::Gauge> &instance_is_main_family_;
  prometheus::Family<prometheus::Gauge> &instance_last_response_seconds_family_;

  struct {
    std::mutex mutex;
    std::unordered_map<std::string, prometheus::Gauge *> up;
    std::unordered_map<std::string, prometheus::Gauge *> is_leader;
    std::unordered_map<std::string, prometheus::Gauge *> is_main;
    std::unordered_map<std::string, prometheus::Gauge *> last_response_seconds;
  } instance_gauges_;
#endif
};

PrometheusMetrics &Metrics();

}  // namespace memgraph::metrics
