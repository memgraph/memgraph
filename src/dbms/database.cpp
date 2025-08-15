// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "dbms/database.hpp"
#include "dbms/inmemory/storage_helper.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/storage_mode.hpp"

#include <memory>

template struct memgraph::utils::Gatekeeper<memgraph::dbms::Database>;

namespace memgraph::dbms {

struct PlanInvalidatorForDatabase : storage::PlanInvalidator {
  explicit PlanInvalidatorForDatabase(query::PlanCacheLRU &planCache) : plan_cache(planCache) {}

  auto invalidate_for_timestamp_wrapper(std::function<bool(uint64_t)> func) -> std::function<bool(uint64_t)> override {
    return [&plan_cache = plan_cache, func = std::move(func)](uint64_t timestamp) {
      return plan_cache.WithLock([&](query::PlanCache_t &cache) {
        auto do_reset = func(timestamp);
        if (do_reset) {
          cache.reset();
        }
        return do_reset;
      });
    };
  }

  bool invalidate_now(std::function<bool()> func) override {
    return plan_cache.WithLock([&](query::PlanCache_t &cache) {
      auto do_reset = func();
      if (do_reset) {
        cache.reset();
      }
      return do_reset;
    });
  }

 private:
  // Storage and Plan cache exist in Database
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-const-or-ref-data-members)
  query::PlanCacheLRU &plan_cache;
};

Database::Database(storage::Config config,
                   utils::Synchronized<replication::ReplicationState, utils::RWSpinLock> &repl_state,
                   std::function<storage::DatabaseProtectorPtr()> database_protector_factory)
    : trigger_store_(config.durability.storage_directory / "triggers"),
      streams_{config.durability.storage_directory / "streams"},
      plan_cache_{FLAGS_query_plan_cache_max_size} {
  std::unique_ptr<storage::PlanInvalidator> invalidator = std::make_unique<PlanInvalidatorForDatabase>(plan_cache_);

  if (config.salient.storage_mode == memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL || config.force_on_disk ||
      utils::DirExists(config.disk.main_storage_directory)) {
    config.salient.storage_mode = memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL;
    storage_ =
        std::make_unique<storage::DiskStorage>(std::move(config), std::move(invalidator), database_protector_factory);
  } else {
    storage_ =
        dbms::CreateInMemoryStorage(std::move(config), repl_state, std::move(invalidator), database_protector_factory);
  }
}

void Database::SwitchToOnDisk() {
  // Preserve the database protector factory from the previous storage
  // This ensures consistent behavior for async operations (indexer, TTL) across storage transitions
  auto preserved_factory = storage_->get_database_protector_factory();

  storage_ = std::make_unique<memgraph::storage::DiskStorage>(
      std::move(storage_->config_), std::make_unique<storage::PlanInvalidatorDefault>(), preserved_factory);
}

}  // namespace memgraph::dbms
