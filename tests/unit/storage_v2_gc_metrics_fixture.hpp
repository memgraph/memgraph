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

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string>

#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/resource_lock.hpp"
#include "utils/uuid.hpp"

// Forces a synchronous GC pass by handing FreeMemory a hold it must adopt rather than acquire.
inline auto UniqueGuard(memgraph::utils::ResourceLock &lock) {
  return memgraph::utils::ResourceLockGuard{lock, memgraph::utils::ResourceLockGuard::UNIQUE};
}

/// A storage registered with the metrics registry, so a test can read what a GC pass reported
/// about itself. Each test gets its own database name and UUID, so the counters it reads belong
/// to its own writes.
class StorageV2GcMetricsTest : public testing::Test {
 protected:
  void SetUp() override {
    db_name_ = testing::UnitTest::GetInstance()->current_test_info()->name();
    InitStorage(std::chrono::seconds(3600));
  }

  void TearDown() override {
    memgraph::metrics::Metrics().SetStorageSnapshotResolver({});
    storage.reset();
    registration_ = {};
    uuid_ = {};
  }

  void InitStorage(std::chrono::milliseconds interval) {
    memgraph::metrics::Metrics().SetStorageSnapshotResolver({});
    storage.reset();
    registration_ = {};
    memgraph::storage::Config config;
    config.salient.name = db_name_;
    config.gc = {.type = memgraph::storage::Config::Gc::Type::PERIODIC, .interval = interval};
    uuid_ = memgraph::utils::UUID{};
    registration_ = memgraph::metrics::Metrics().AddDatabase(uuid_, db_name_);
    storage = std::make_unique<memgraph::storage::InMemoryStorage>(
        config, std::nullopt, std::make_unique<memgraph::storage::PlanInvalidatorDefault>(), *registration_.handles());
    memgraph::metrics::Metrics().SetStorageSnapshotResolver(
        [this](memgraph::utils::UUID const &uuid) -> std::optional<memgraph::metrics::StorageSnapshot> {
          if (uuid != uuid_ || !storage) return std::nullopt;
          auto const info = storage->GetBaseInfo();
          return memgraph::metrics::StorageSnapshot{
              .vertex_count = info.vertex_count,
              .edge_count = info.edge_count,
              .disk_usage = info.disk_usage,
          };
        });
  }

  std::unique_ptr<memgraph::storage::Storage> storage;

  auto handles() { return registration_.handles(); }

  memgraph::metrics::PrometheusMetrics::Registration registration_{};
  memgraph::utils::UUID uuid_{};

 private:
  std::string db_name_;
};
