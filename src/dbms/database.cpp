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

#include "dbms/database.hpp"

#include <memory>

#include "dbms/inmemory/storage_helper.hpp"
#include "memory/db_arena.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#if USE_JEMALLOC
#include <jemalloc/jemalloc.h>
#endif

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

Database::~Database() = default;

Database::Database(storage::Config config, std::function<storage::DatabaseProtectorPtr()> database_protector_factory)
    :
#if USE_JEMALLOC
      db_arena_(&db_memory_tracker_),
      arena_scope_(ArenaIdx()),
#endif
      trigger_store_(
          std::make_unique<query::TriggerStore>(config.durability.storage_directory / "triggers", ArenaIdx())),
      streams_(std::make_unique<query::stream::Streams>(config.durability.storage_directory / "streams")),
      plan_cache_{FLAGS_query_plan_cache_max_size},
      after_commit_trigger_pool_{1} {
  std::unique_ptr<storage::PlanInvalidator> invalidator = std::make_unique<PlanInvalidatorForDatabase>(plan_cache_);

#if USE_JEMALLOC
  // Route all constructor-body allocations (storage init, recovery, index structures) to this DB's arena.
  const memory::DbArenaFullScope db_arena_scope{ArenaIdx()};
#endif

  config.arena_idx = ArenaIdx();
  streams_.SetArenaIdx(ArenaIdx());

#if USE_JEMALLOC
  // Pin the after-commit trigger thread to this DB's arena so trigger allocations are attributed correctly.
  if (const unsigned idx = ArenaIdx(); idx != 0) {
    after_commit_trigger_pool_.AddTask([idx] {
      static thread_local bool arena_pinned = false;
      if (!arena_pinned) {
        je_mallctl("thread.arena", nullptr, nullptr, const_cast<unsigned *>(&idx), sizeof(unsigned));
        memory::tls_db_arena_idx = idx;
        arena_pinned = true;
      }
    });
  }
#endif

  if (config.salient.storage_mode == memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL || config.force_on_disk ||
      utils::DirExists(config.disk.main_storage_directory)) {
    config.salient.storage_mode = memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL;
    storage_ =
        std::make_unique<storage::DiskStorage>(std::move(config), std::move(invalidator), database_protector_factory);
  } else {
    storage_ = dbms::CreateInMemoryStorage(std::move(config), std::move(invalidator), database_protector_factory);
  }
}

DatabaseInfo Database::GetInfo() const {
  DatabaseInfo info;
  info.storage_info = storage_->GetInfo();
  info.triggers = trigger_store_->GetTriggerInfo().size();
  info.streams = streams_->GetStreamInfo().size();
  return info;
}

void Database::StopAllBackgroundTasks() {
  streams()->Shutdown();
  thread_pool()->ShutDown();
  storage_->StopAllBackgroundTasks();
}

void Database::SwitchToOnDisk() {
  // Preserve the database protector factory from the previous storage
  // This ensures consistent behavior for async operations (indexer, TTL) across storage transitions
  auto preserved_factory = storage_->get_database_protector_factory();

#if USE_JEMALLOC
  const memory::DbArenaFullScope db_arena_scope{ArenaIdx()};
#endif
  storage_ = std::make_unique<memgraph::storage::DiskStorage>(
      std::move(storage_->config_), std::make_unique<storage::PlanInvalidatorDefault>(), preserved_factory);
}

}  // namespace memgraph::dbms
