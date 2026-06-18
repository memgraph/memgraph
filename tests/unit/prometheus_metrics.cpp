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
#include <filesystem>

#include <gtest/gtest.h>
#include <prometheus/metric_family.h>
#include <prometheus/registry.h>

#include "dbms/database.hpp"
#include "disk_test_utils.hpp"
#include "flags/general.hpp"
#include "metrics/scoped_gauge.hpp"
#include "metrics/scoped_histogram_timer.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/storage.hpp"

namespace {

namespace r = std::ranges;

std::optional<double> FindSample(std::vector<prometheus::MetricFamily> const &families, std::string_view name,
                                 std::string_view db_name) {
  for (auto const &family : families) {
    if (family.name != name) continue;
    for (auto const &metric : family.metric) {
      auto const has_db_label =
          r::any_of(metric.label, [&](auto const &l) { return l.name == "database" && l.value == db_name; });
      if (!has_db_label) continue;
      if (family.type == prometheus::MetricType::Gauge) return metric.gauge.value;
      if (family.type == prometheus::MetricType::Counter) return metric.counter.value;
    }
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

TEST(PrometheusMetrics, RebindDefaultDatabaseUUIDUpdatesUuidLabel) {
  FLAGS_metrics_format = "OpenMetrics";
  memgraph::metrics::PrometheusMetrics pm;

  memgraph::utils::UUID const uuid_a{};
  memgraph::utils::UUID const uuid_b{};

  pm.AddDatabase(uuid_a, "memgraph");
  pm.RebindDefaultDatabaseUUID(uuid_b);

  auto const families = pm.registry().Collect();
  auto const find_uuid_label = [&](std::string_view name, std::string_view db_name) -> std::optional<std::string> {
    for (auto const &family : families) {
      if (family.name != name) continue;
      for (auto const &metric : family.metric) {
        auto const has_db =
            r::any_of(metric.label, [&](auto const &l) { return l.name == "database" && l.value == db_name; });
        if (!has_db) continue;
        auto const it = r::find_if(metric.label, [](auto const &l) { return l.name == "uuid"; });
        if (it != metric.label.end()) return it->value;
      }
    }
    return std::nullopt;
  };
  auto const label = find_uuid_label("memgraph_vertex_count", "memgraph");
  ASSERT_TRUE(label.has_value());
  EXPECT_EQ(*label, std::string(uuid_b)) << "uuid label should reflect the new UUID after rebind";

  auto const has_old_uuid = r::any_of(families, [&](auto const &family) {
    return r::any_of(family.metric, [&](auto const &metric) {
      return r::any_of(metric.label, [&](auto const &l) { return l.name == "uuid" && l.value == std::string(uuid_a); });
    });
  });
  EXPECT_FALSE(has_old_uuid) << "old UUID series should be fully removed after rebind";
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
