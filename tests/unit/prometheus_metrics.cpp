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

#include "dbms/constants.hpp"
#include "dbms/database.hpp"
#include "disk_test_utils.hpp"
#include "metrics/scoped_gauge.hpp"
#include "metrics/scoped_histogram_timer.hpp"
#include "storage/v2/config.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "tests/test_commit_args_helper.hpp"

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

std::optional<double> FindSampleByUuid(std::vector<prometheus::MetricFamily> const &families, std::string_view name,
                                       memgraph::utils::UUID const &uuid) {
  auto const wanted = std::string{uuid};
  for (auto const &family : families) {
    if (family.name != name) continue;
    for (auto const &metric : family.metric) {
      auto const has_uuid_label =
          std::ranges::any_of(metric.label, [&](auto const &l) { return l.name == "uuid" && l.value == wanted; });
      if (!has_uuid_label) continue;
      if (family.type == prometheus::MetricType::Gauge) return metric.gauge.value;
      if (family.type == prometheus::MetricType::Counter) return metric.counter.value;
    }
  }
  return std::nullopt;
}

}  // namespace

TEST(PrometheusMetrics, GetOrAddDatabaseRegistersMetrics) {
  memgraph::metrics::PrometheusMetrics pm;
  auto reg = pm.AddDatabase(memgraph::utils::UUID{}, "db1");

  reg.handles().vertex_count.Set(42.0);
  reg.handles().committed_transactions.Increment(5.0);

  auto const families = pm.registry().Collect();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db1"), 42.0);
  EXPECT_EQ(FindSample(families, "memgraph_committed_transactions_total", "db1"), 5.0);
}

TEST(PrometheusMetrics, MultipleDatabasesAreIsolated) {
  memgraph::metrics::PrometheusMetrics pm;
  auto db1 = pm.AddDatabase(memgraph::utils::UUID{}, "db1");
  auto db2 = pm.AddDatabase(memgraph::utils::UUID{}, "db2");

  db1.handles().vertex_count.Set(10.0);
  db2.handles().vertex_count.Set(20.0);

  auto const families = pm.registry().Collect();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db1"), 10.0);
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db2"), 20.0);
}

TEST(PrometheusMetrics, UpdateGaugesSetsStorageValues) {
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
  auto const db1 = pm.AddDatabase(db1_uuid, "db1");

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
  auto reg = pm.AddDatabase(uuid, "db1");
  reg.handles().vertex_count.Set(99.0);

  reg = {};

  auto const families = pm.registry().Collect();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "db1"), std::nullopt);
}

// Metrics survive a deregistration while another registration still holds them.
TEST(PrometheusMetrics, MetricsSurviveWhileAnotherRegistrationHoldsThem) {
  memgraph::metrics::PrometheusMetrics pm;
  memgraph::utils::UUID const uuid{};

  auto first = pm.AddDatabase(uuid, "db1");
  auto second = pm.AddDatabase(uuid, "db1");
  ASSERT_EQ(first.handles().vertex_count.get(), second.handles().vertex_count.get()) << "one label set, one metric";

  first = {};

  // Checked before the handle is used: writing through a dropped metric is the bug under test.
  ASSERT_NE(FindSample(pm.registry().Collect(), "memgraph_vertex_count", "db1"), std::nullopt);

  second.handles().vertex_count.Set(7.0);
  EXPECT_EQ(FindSample(pm.registry().Collect(), "memgraph_vertex_count", "db1"), 7.0);

  second = {};
  EXPECT_EQ(FindSample(pm.registry().Collect(), "memgraph_vertex_count", "db1"), std::nullopt);
}

// A database's metrics are identified by the labels it registered under, so deregistering one name
// must not take the entry a second name registered under the same uuid.
TEST(PrometheusMetrics, RemovingOneNameLeavesTheOtherRegisteredUnderThatUuid) {
  memgraph::metrics::PrometheusMetrics pm;
  memgraph::utils::UUID const uuid{};

  auto under_old_name = pm.AddDatabase(uuid, "old");
  auto under_new_name = pm.AddDatabase(uuid, "new");

  under_new_name = {};

  under_old_name.handles().vertex_count.Set(5.0);
  EXPECT_EQ(FindSample(pm.registry().Collect(), "memgraph_vertex_count", "old"), 5.0);
  EXPECT_EQ(FindSample(pm.registry().Collect(), "memgraph_vertex_count", "new"), std::nullopt);
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
  auto const registration = pm.AddDatabase(uuid_a, "memgraph");

  // Simulate HA UUID realignment on joining cluster: storage now answers to
  // uuid_b
  pm.RebindDefaultDatabaseUUID(uuid_b);

  pm.UpdateGauges();

  auto const families = pm.CollectForScrape();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "memgraph"), 42.0);
  EXPECT_EQ(FindSample(families, "memgraph_edge_count", "memgraph"), 10.0);
  EXPECT_EQ(FindSample(families, "memgraph_disk_usage_bytes", "memgraph"), 2048.0);
}

TEST(PrometheusMetrics, RebindDefaultDatabaseUUIDUpdatesUuidLabel) {
  memgraph::metrics::PrometheusMetrics pm;

  memgraph::utils::UUID const uuid_a{};
  memgraph::utils::UUID const uuid_b{};

  auto reg = pm.AddDatabase(uuid_a, "memgraph");
  pm.RebindDefaultDatabaseUUID(uuid_b);

  auto const families = pm.CollectForScrape();
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
  EXPECT_FALSE(has_old_uuid) << "the relabelled series must no longer present the pre-rebind uuid";

  auto const leaks_entry_label = r::any_of(families, [](auto const &family) {
    return r::any_of(family.metric, [](auto const &metric) {
      return r::any_of(metric.label, [](auto const &l) { return l.name == "mgentry"; });
    });
  });
  EXPECT_FALSE(leaks_entry_label) << "the entry-id label keys the family internally and must never be scraped";
}

// CollectForScrape snapshots the entries before collecting, so a database registered in between is
// collected without a uuid to present. Dropping those metrics keeps two such series from colliding
// on {database, uuid=""}, which would make Prometheus reject the whole scrape.
TEST(PrometheusMetrics, ScrapeDropsSeriesWithNoLiveEntry) {
  memgraph::metrics::PrometheusMetrics pm;

  auto reg = pm.AddDatabase(memgraph::utils::UUID{}, "present");
  reg.handles().vertex_count.Set(3.0);

  {
    auto orphan = pm.AddDatabase(memgraph::utils::UUID{}, "orphan");
    orphan.handles().vertex_count.Set(9.0);
    ASSERT_EQ(FindSample(pm.CollectForScrape(), "memgraph_vertex_count", "orphan"), 9.0)
        << "a live entry must be scraped normally";
  }

  auto const families = pm.CollectForScrape();
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "present"), 3.0) << "the live series was dropped";
  EXPECT_EQ(FindSample(families, "memgraph_vertex_count", "orphan"), std::nullopt)
      << "a released database must leave no series behind";

  auto const leaks_entry_label = r::any_of(families, [](auto const &family) {
    return r::any_of(family.metric, [](auto const &metric) {
      return r::any_of(metric.label, [](auto const &l) { return l.name == "mgentry"; });
    });
  });
  EXPECT_FALSE(leaks_entry_label);
}

TEST(PrometheusMetrics, RebindKeepsMetricObjectsAlive) {
  memgraph::metrics::PrometheusMetrics pm;

  memgraph::utils::UUID const uuid_a{};
  memgraph::utils::UUID const uuid_b{};

  auto reg = pm.AddDatabase(uuid_a, "memgraph");
  auto const before = reg.handles();
  reg.handles().vertex_count.Set(7.0);

  auto const after = pm.RebindDefaultDatabaseUUID(uuid_b);

  // Every handle must still name the same object: holders all over the codebase (ScopedGauge,
  // delta_container, per-index gauges, in-flight query snapshots) copied these pointers out and
  // are not reachable from here, so a rebind that replaced them would leave them dangling.
  EXPECT_EQ(after.vertex_count.gauge, before.vertex_count.gauge);
  EXPECT_EQ(after.active_transactions.gauge, before.active_transactions.gauge);
  EXPECT_EQ(after.unreleased_delta_objects.gauge, before.unreleased_delta_objects.gauge);
  EXPECT_EQ(after.query_execution_latency_seconds.histogram, before.query_execution_latency_seconds.histogram);

  // The accumulated value survives, because the series was relabelled rather than recreated.
  EXPECT_EQ(FindSample(pm.CollectForScrape(), "memgraph_vertex_count", "memgraph"), 7.0);
}

TEST(PrometheusMetrics, RebindPropagatesHandlesToIndicesAndConstraints) {
  memgraph::metrics::PrometheusMetrics pm;

  memgraph::utils::UUID const uuid_a{};
  memgraph::utils::UUID const uuid_b{};

  // Register default db with uuid_a and build a storage over those handles.
  auto handles_a = pm.AddDatabase(uuid_a, "memgraph");
  auto storage = std::make_unique<memgraph::storage::InMemoryStorage>(
      memgraph::storage::Config{},
      std::nullopt,
      std::make_unique<memgraph::storage::PlanInvalidatorDefault>(),
      handles_a.handles());

  // Rebind to uuid_b. The storage keeps the handles it was built with: they still point at the
  // same live objects, so nothing needs re-installing.
  pm.RebindDefaultDatabaseUUID(uuid_b);

  // Create index + constraint: these increment gauges via handles stored inside indices_/constraints_
  memgraph::storage::LabelId label;
  memgraph::storage::PropertyId prop;
  {
    auto acc = storage->Access(memgraph::storage::WRITE);
    label = acc->NameToLabel("TestLabel");
    prop = acc->NameToProperty("test_prop");
  }
  {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateIndex(label).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }
  {
    auto acc = storage->ReadOnlyAccess();
    ASSERT_TRUE(acc->CreateExistenceConstraint(label, prop).has_value());
    ASSERT_TRUE(acc->PrepareForCommitPhase(memgraph::tests::MakeMainCommitArgs()).has_value());
  }

  // Scrape and verify gauges appear under uuid_b, not the uuid the storage was built with.
  auto const families = pm.CollectForScrape();
  EXPECT_EQ(FindSampleByUuid(families, "memgraph_active_label_indices", uuid_a), std::nullopt)
      << "the series still presents the pre-rebind uuid";
  auto const idx_val = FindSampleByUuid(families, "memgraph_active_label_indices", uuid_b);
  ASSERT_TRUE(idx_val.has_value());
  EXPECT_EQ(*idx_val, 1.0);

  auto const constr_val = FindSampleByUuid(families, "memgraph_active_existence_constraints", uuid_b);
  ASSERT_TRUE(constr_val.has_value());
  EXPECT_EQ(*constr_val, 1.0);
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

namespace {

// Samples carry the labels the database registered under, so a stale series is found by its uuid
// rather than by the name a later database may reuse.

auto MakeConfig(std::filesystem::path const &dir, std::string_view name, memgraph::utils::UUID const &uuid)
    -> memgraph::storage::Config {
  memgraph::storage::Config config;
  config.durability.storage_directory = dir;
  config.salient.name = std::string{name};
  config.salient.uuid = uuid;
  return config;
}

}  // namespace

// Two databases can hold one database's identity at once: a resume that rebuilds a tenant before its
// predecessor has finished dying gives both the same name and uuid, and so the same metrics. The
// metrics belong to both until the second one goes.
TEST(DatabaseMetrics, MetricsOutliveADatabaseWhileAnotherSharesItsIdentity) {
  auto const root = std::filesystem::temp_directory_path() / "mg_test_metrics_shared_identity";
  std::filesystem::remove_all(root);
  memgraph::utils::UUID const uuid{};

  {
    memgraph::dbms::Database second{MakeConfig(root / "second", "shared_identity", uuid)};
    {
      memgraph::dbms::Database first{MakeConfig(root / "first", "shared_identity", uuid)};
      ASSERT_NE(FindSampleByUuid(memgraph::metrics::Metrics().CollectForScrape(), "memgraph_vertex_count", uuid),
                std::nullopt);
    }

    // `first` is gone; `second` is still running, and its garbage collector writes to these metrics
    // every pass, so they must still be registered.
    EXPECT_NE(FindSampleByUuid(memgraph::metrics::Metrics().CollectForScrape(), "memgraph_vertex_count", uuid),
              std::nullopt)
        << "the surviving database's metrics were freed with the other's";
  }

  std::filesystem::remove_all(root);
}

// A replica joining a cluster realigns its default database onto the main's uuid in place, which is
// what DbmsHandler::Update does for the default database. The database must still take its metrics
// with it when it goes.
TEST(DatabaseMetrics, ARealignedDatabaseStillTakesItsMetricsWithIt) {
  auto const root = std::filesystem::temp_directory_path() / "mg_test_metrics_realigned";
  std::filesystem::remove_all(root);
  memgraph::utils::UUID const registered_uuid{};
  memgraph::utils::UUID const realigned_uuid{};
  ASSERT_NE(registered_uuid, realigned_uuid);

  {
    memgraph::dbms::Database db{MakeConfig(root, memgraph::dbms::kDefaultDB, registered_uuid)};
    ASSERT_NE(
        FindSampleByUuid(memgraph::metrics::Metrics().CollectForScrape(), "memgraph_vertex_count", registered_uuid),
        std::nullopt);
    memgraph::metrics::Metrics().RebindDefaultDatabaseUUID(realigned_uuid);
  }

  EXPECT_EQ(FindSampleByUuid(memgraph::metrics::Metrics().CollectForScrape(), "memgraph_vertex_count", registered_uuid),
            std::nullopt)
      << "the realigned database left its metrics behind";

  std::filesystem::remove_all(root);
}
