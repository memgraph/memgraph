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

#include <algorithm>
#include <array>
#include <filesystem>
#include <utility>

#include <gtest/gtest.h>
#include <prometheus/metric_family.h>
#include <prometheus/registry.h>

#include "dbms/database.hpp"
#include "disk_test_utils.hpp"
#include "flags/general.hpp"
#include "metrics/scoped_gauge.hpp"
#include "metrics/scoped_histogram_timer.hpp"
#include "replication_coordination_glue/mode.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/replication/enums.hpp"
#include "storage/v2/storage.hpp"

namespace {

std::optional<double> FindSample(std::vector<prometheus::MetricFamily> const &families, std::string_view name,
                                 std::string_view db_name) {
  for (auto const &family : families) {
    if (family.name != name) continue;
    for (auto const &metric : family.metric) {
      auto const has_db_label =
          std::ranges::any_of(metric.label, [&](auto const &l) { return l.name == "database" && l.value == db_name; });
      if (!has_db_label) continue;
      if (family.type == prometheus::MetricType::Gauge) return metric.gauge.value;
      if (family.type == prometheus::MetricType::Counter) return metric.counter.value;
    }
  }
  return std::nullopt;
}

// Type-agnostic presence check for a series carrying label_name=label_value (works for histograms
// too, where FindSample returns nullopt).
bool HasSeriesWithLabel(std::vector<prometheus::MetricFamily> const &families, std::string_view name,
                        std::string_view label_name, std::string_view label_value) {
  for (auto const &family : families) {
    if (family.name != name) continue;
    for (auto const &metric : family.metric) {
      if (std::ranges::any_of(metric.label,
                              [&](auto const &l) { return l.name == label_name && l.value == label_value; })) {
        return true;
      }
    }
  }
  return false;
}

// Value of the (first) gauge series carrying every (name, value) pair in `required`.
std::optional<double> GaugeValueWithLabels(std::vector<prometheus::MetricFamily> const &families, std::string_view name,
                                           std::vector<std::pair<std::string_view, std::string_view>> const &required) {
  for (auto const &family : families) {
    if (family.name != name) continue;
    for (auto const &metric : family.metric) {
      bool const all_present = std::ranges::all_of(required, [&](auto const &req) {
        return std::ranges::any_of(metric.label,
                                   [&](auto const &l) { return l.name == req.first && l.value == req.second; });
      });
      if (all_present) return metric.gauge.value;
    }
  }
  return std::nullopt;
}

// Value of a scalar metric from a MetricInfo list (SHOW METRICS INFO / global), as a double.
std::optional<double> MetricInfoValue(std::vector<memgraph::metrics::MetricInfo> const &metrics,
                                      std::string_view name) {
  for (auto const &m : metrics) {
    if (m.name != name) continue;
    return std::visit([](auto v) { return static_cast<double>(v); }, m.value);
  }
  return std::nullopt;
}

}  // namespace

TEST(PrometheusMetrics, GetOrAddDatabaseRegistersMetrics) {
  FLAGS_metrics_format = "OpenMetrics";
  memgraph::metrics::PrometheusMetrics pm;
  auto handles = pm.AddDatabase(memgraph::utils::UUID{}, "db1");

  handles.vertex_count.Set(42.0);
  handles.committed_transactions.Increment(5.0);

  auto const families = pm.registry().Collect();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db1"), 42.0);
  EXPECT_EQ(FindSample(families, "memgraph_committed_transactions_total", "db1"), 5.0);
}

TEST(PrometheusMetrics, MultipleDatabasesAreIsolated) {
  FLAGS_metrics_format = "OpenMetrics";
  memgraph::metrics::PrometheusMetrics pm;
  auto h1 = pm.AddDatabase(memgraph::utils::UUID{}, "db1");
  auto h2 = pm.AddDatabase(memgraph::utils::UUID{}, "db2");

  h1.vertex_count.Set(10.0);
  h2.vertex_count.Set(20.0);

  auto const families = pm.registry().Collect();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db1"), 10.0);
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db2"), 20.0);
}

TEST(PrometheusMetrics, UpdateGaugesSetsStorageValues) {
  FLAGS_metrics_format = "OpenMetrics";
  memgraph::metrics::PrometheusMetrics pm;
  memgraph::metrics::StorageSnapshot snapshot{.vertex_count = 7,
                                              .edge_count = 3,
                                              .disk_usage = 1024,
                                              .db_memory_tracked = 4096,
                                              .db_peak_memory_tracked = 8192};
  memgraph::utils::UUID const db1_uuid{};
  pm.SetStorageSnapshotResolver(
      [&snapshot, &db1_uuid](memgraph::utils::UUID const &uuid) -> std::optional<memgraph::metrics::StorageSnapshot> {
        if (uuid == db1_uuid) return snapshot;
        return std::nullopt;
      });
  pm.AddDatabase(db1_uuid, "db1");

  pm.UpdateGauges();

  auto const families = pm.registry().Collect();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db1"), 7.0);
  EXPECT_EQ(FindSample(families, "memgraph_edge_count", "db1"), 3.0);
  EXPECT_EQ(FindSample(families, "memgraph_disk_usage_bytes", "db1"), 1024.0);
  EXPECT_EQ(FindSample(families, "memgraph_db_memory_tracked_bytes", "db1"), 4096.0);
  EXPECT_EQ(FindSample(families, "memgraph_db_peak_memory_tracked_bytes", "db1"), 8192.0);
}

TEST(PrometheusMetrics, RemoveDatabaseRemovesMetrics) {
  memgraph::metrics::PrometheusMetrics pm;
  memgraph::utils::UUID const uuid{};
  auto handles = pm.AddDatabase(uuid, "db1");
  handles.vertex_count.Set(99.0);

  pm.RemoveDatabase(uuid);

  auto const families = pm.registry().Collect();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db1"), std::nullopt);
}

namespace {
using memgraph::replication_coordination_glue::ReplicationMode;
using memgraph::storage::replication::ReplicaState;

// Resolve a (replica, db) bundle and write one value to every series so it exists in the registry.
void SeedReplicaSeries(memgraph::metrics::PrometheusMetrics &pm, std::string_view instance,
                       memgraph::utils::UUID const &uuid, std::string_view name,
                       ReplicationMode mode = ReplicationMode::SYNC, ReplicaState state = ReplicaState::READY) {
  auto const &h = pm.EnsureReplicaHandles(instance, uuid, name, mode);
  h.ObserveReplicaStream(0.001);
  h.ObserveStartTxn(0.001);
  h.ObserveFinalizeTxn(0.001);
  h.SetCommitTimestamp(100);
  h.SetTimestampLag(5);
  h.SetLagSeconds(2.0);
  h.SetState(state);
}

// All per-(replica, db) families, plus the per-db main_commit_timestamp.
constexpr std::array kReplicaFamilies = {"memgraph_replica_stream_seconds",
                                         "memgraph_start_txn_replication_seconds",
                                         "memgraph_finalize_txn_replication_seconds",
                                         "memgraph_replica_commit_timestamp",
                                         "memgraph_replica_timestamp_lag",
                                         "memgraph_replica_lag_seconds",
                                         "memgraph_replica_state"};
}  // namespace

TEST(PrometheusMetrics, RemoveDatabaseErasesReplicaSeriesForThatDb) {
  FLAGS_metrics_format = "OpenMetrics";
  memgraph::metrics::PrometheusMetrics pm;

  memgraph::utils::UUID const uuid1{};
  memgraph::utils::UUID const uuid2{};
  ASSERT_NE(uuid1, uuid2);

  // AddDatabase registers main_commit_timestamp; RemoveDatabase early-returns unless the db was added.
  pm.AddDatabase(uuid1, "db1");
  pm.AddDatabase(uuid2, "db2");
  SeedReplicaSeries(pm, "replica_a", uuid1, "db1");
  SeedReplicaSeries(pm, "replica_a", uuid2, "db2");

  // Both databases have every replica series plus main_commit_timestamp before removal.
  auto const before = pm.registry().Collect();
  for (auto const *name : kReplicaFamilies) {
    EXPECT_TRUE(HasSeriesWithLabel(before, name, "database", "db1")) << name << " missing for db1 before removal";
    EXPECT_TRUE(HasSeriesWithLabel(before, name, "database", "db2")) << name << " missing for db2 before removal";
  }
  EXPECT_TRUE(HasSeriesWithLabel(before, "memgraph_main_commit_timestamp", "database", "db1"));
  EXPECT_TRUE(HasSeriesWithLabel(before, "memgraph_main_commit_timestamp", "database", "db2"));

  pm.RemoveDatabase(uuid1);

  // db1's series are erased across every family (incl. main_commit_timestamp); db2's remain untouched.
  auto const after = pm.registry().Collect();
  for (auto const *name : kReplicaFamilies) {
    EXPECT_FALSE(HasSeriesWithLabel(after, name, "database", "db1")) << name << " should be erased for db1";
    EXPECT_TRUE(HasSeriesWithLabel(after, name, "database", "db2")) << name << " should remain for db2";
  }
  EXPECT_FALSE(HasSeriesWithLabel(after, "memgraph_main_commit_timestamp", "database", "db1"));
  EXPECT_TRUE(HasSeriesWithLabel(after, "memgraph_main_commit_timestamp", "database", "db2"));
}

TEST(PrometheusMetrics, RemoveReplicaInstanceMetricsErasesSeriesForThatInstance) {
  FLAGS_metrics_format = "OpenMetrics";
  memgraph::metrics::PrometheusMetrics pm;
  memgraph::utils::UUID const db_uuid{};

  pm.AddDatabase(db_uuid, "db1");  // owns the per-db main_commit_timestamp
  SeedReplicaSeries(pm, "replica_a", db_uuid, "db1");
  SeedReplicaSeries(pm, "replica_b", db_uuid, "db1");

  auto const before = pm.registry().Collect();
  for (auto const *name : kReplicaFamilies) {
    EXPECT_TRUE(HasSeriesWithLabel(before, name, "mg_instance", "replica_a")) << name << " missing for replica_a";
    EXPECT_TRUE(HasSeriesWithLabel(before, name, "mg_instance", "replica_b")) << name << " missing for replica_b";
  }

  pm.RemoveReplicaInstanceMetrics("replica_a");

  // replica_a's series are erased; replica_b's remain, and the per-db main_commit_timestamp is untouched.
  auto const after = pm.registry().Collect();
  for (auto const *name : kReplicaFamilies) {
    EXPECT_FALSE(HasSeriesWithLabel(after, name, "mg_instance", "replica_a")) << name << " should be erased";
    EXPECT_TRUE(HasSeriesWithLabel(after, name, "mg_instance", "replica_b")) << name << " should remain";
  }
  EXPECT_TRUE(HasSeriesWithLabel(after, "memgraph_main_commit_timestamp", "database", "db1"))
      << "main_commit_timestamp is per-db, not per-instance";
}

TEST(PrometheusMetrics, ReplicaStateSeriesIsOneHot) {
  FLAGS_metrics_format = "OpenMetrics";
  memgraph::metrics::PrometheusMetrics pm;
  memgraph::utils::UUID const db_uuid{};

  auto const &h = pm.EnsureReplicaHandles("replica_a", db_uuid, "db1", ReplicationMode::SYNC);
  h.SetState(ReplicaState::REPLICATING);

  auto value_for = [&](std::string_view state) {
    return GaugeValueWithLabels(
        pm.registry().Collect(), "memgraph_replica_state", {{"mg_instance", "replica_a"}, {"state", state}});
  };
  EXPECT_EQ(value_for("REPLICATING"), 1.0);
  EXPECT_EQ(value_for("READY"), 0.0);
  EXPECT_EQ(value_for("DIVERGED_FROM_MAIN"), 0.0);

  // Transition flips exactly one series on and the previous one off.
  h.SetState(ReplicaState::READY);
  EXPECT_EQ(value_for("READY"), 1.0);
  EXPECT_EQ(value_for("REPLICATING"), 0.0);
}

TEST(PrometheusMetrics, ReplicaSeriesCarrySyncModeLabel) {
  FLAGS_metrics_format = "OpenMetrics";
  memgraph::metrics::PrometheusMetrics pm;
  memgraph::utils::UUID const db_uuid{};

  SeedReplicaSeries(pm, "replica_async", db_uuid, "db1", ReplicationMode::ASYNC);
  auto const families = pm.registry().Collect();
  EXPECT_TRUE(HasSeriesWithLabel(families, "memgraph_replica_lag_seconds", "sync_mode", "async"));
  EXPECT_TRUE(HasSeriesWithLabel(families, "memgraph_replica_state", "sync_mode", "async"));
}

TEST(PrometheusMetrics, GlobalMetricsInfoReportsReplicaLagSummaries) {
  FLAGS_metrics_format = "OpenMetrics";
  memgraph::metrics::PrometheusMetrics pm;
  memgraph::utils::UUID const db_uuid{};

  // replica_a: caught up (READY); replica_b: behind and RECOVERY.
  auto const &a = pm.EnsureReplicaHandles("replica_a", db_uuid, "db1", ReplicationMode::SYNC);
  a.SetTimestampLag(3);
  a.SetLagSeconds(1.5);
  a.SetState(ReplicaState::READY);
  auto const &b = pm.EnsureReplicaHandles("replica_b", db_uuid, "db1", ReplicationMode::ASYNC);
  b.SetTimestampLag(42);
  b.SetLagSeconds(7.5);
  b.SetState(ReplicaState::RECOVERY);

  auto const global = pm.GetGlobalMetricsInfo();
  EXPECT_EQ(MetricInfoValue(global, "MaxReplicaTimestampLag"), 42.0);
  EXPECT_EQ(MetricInfoValue(global, "MaxReplicaLagSeconds"), 7.5);
  EXPECT_EQ(MetricInfoValue(global, "ReplicasNotReady"), 1.0);  // only replica_b
}

TEST(DatabaseMetrics, SwitchToOnDiskUpdatesSnapshotCallback) {
  disk_test_utils::RemoveRocksDbDirs("SwitchToOnDiskMetrics");
  auto config = disk_test_utils::GenerateOnDiskConfig("SwitchToOnDiskMetrics");
  config.durability.storage_directory = std::filesystem::temp_directory_path() / "mg_test_switch_to_on_disk_metrics";
  std::filesystem::remove_all(config.durability.storage_directory);

  {
    memgraph::dbms::Database db{config};
    memgraph::metrics::Metrics().SetStorageSnapshotResolver(
        [&db](memgraph::utils::UUID const &uuid) -> std::optional<memgraph::metrics::StorageSnapshot> {
          if (uuid != db.uuid()) return std::nullopt;
          auto const info = db.storage()->GetBaseInfo();
          return memgraph::metrics::StorageSnapshot{
              .vertex_count = info.vertex_count, .edge_count = info.edge_count, .disk_usage = info.disk_usage};
        });
    memgraph::metrics::Metrics().UpdateGauges();

    db.SwitchToOnDisk();
    EXPECT_NO_FATAL_FAILURE(memgraph::metrics::Metrics().UpdateGauges());

    memgraph::metrics::Metrics().SetStorageSnapshotResolver({});
  }

  std::filesystem::remove_all(config.durability.storage_directory);
  disk_test_utils::RemoveRocksDbDirs("SwitchToOnDiskMetrics");
}

TEST(PrometheusMetrics, UpdateGaugesReturnsZeroAfterDefaultDbUuidChange) {
  FLAGS_metrics_format = "OpenMetrics";
  memgraph::metrics::PrometheusMetrics pm;

  memgraph::utils::UUID const uuid_a{};
  memgraph::utils::UUID const uuid_b{};
  ASSERT_NE(uuid_a, uuid_b);

  memgraph::metrics::StorageSnapshot const snapshot{.vertex_count = 42, .edge_count = 10, .disk_usage = 2048};

  // Snapshot resolver will simulate return returning "stale" settings if
  // requesting any database with a uuid other than the HA cluster's default
  // db uuid.
  pm.SetStorageSnapshotResolver(
      [&](memgraph::utils::UUID const &uuid) -> std::optional<memgraph::metrics::StorageSnapshot> {
        if (uuid == uuid_b) return snapshot;
        return std::nullopt;
      });

  // Metrics registered with original uuid_a, as happens at startup
  pm.AddDatabase(uuid_a, "memgraph");

  // Simulate HA UUID realignment on joining cluster: storage now answers to
  // uuid_b
  pm.RebindDefaultDatabaseUUID(uuid_b);

  pm.UpdateGauges();

  auto const families = pm.registry().Collect();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "memgraph"), 42.0);
  EXPECT_EQ(FindSample(families, "memgraph_edge_count", "memgraph"), 10.0);
  EXPECT_EQ(FindSample(families, "memgraph_disk_usage_bytes", "memgraph"), 2048.0);
}

TEST(MetricHandles, GaugeHandleNullSafety) {
  memgraph::metrics::GaugeHandle h{};
  EXPECT_NO_FATAL_FAILURE(h.Increment());
  EXPECT_NO_FATAL_FAILURE(h.Decrement());
  EXPECT_NO_FATAL_FAILURE(h.Set(5.0));
  EXPECT_EQ(h.Value(), 0.0);
}

TEST(MetricHandles, CounterHandleNullSafety) {
  memgraph::metrics::CounterHandle h{};
  EXPECT_NO_FATAL_FAILURE(h.Increment());
  EXPECT_NO_FATAL_FAILURE(h.Increment(10.0));
  EXPECT_EQ(h.Value(), 0.0);
}

TEST(MetricHandles, ScopedGaugeIncrementsAndDecrements) {
  auto registry = std::make_shared<prometheus::Registry>();
  auto &gauge = prometheus::BuildGauge().Name("test").Register(*registry).Add({});
  EXPECT_EQ(gauge.Value(), 0.0);
  {
    memgraph::metrics::ScopedGauge scoped{&gauge};
    EXPECT_EQ(gauge.Value(), 1.0);
  }
  EXPECT_EQ(gauge.Value(), 0.0);
}

TEST(MetricHandles, ScopedGaugeNullSafety) {
  EXPECT_NO_FATAL_FAILURE([] { memgraph::metrics::ScopedGauge scoped{nullptr}; }());
}

TEST(MetricHandles, ScopedHistogramTimerNullSafety) {
  EXPECT_NO_FATAL_FAILURE([] { memgraph::metrics::ScopedHistogramTimer timer{nullptr}; }());
}
