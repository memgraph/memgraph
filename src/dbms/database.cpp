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
#endif
      trigger_store_(
          std::make_unique<query::TriggerStore>(config.durability.storage_directory / "triggers", ArenaIdx())),
      after_commit_trigger_pool_{1},
      streams_(std::make_unique<query::stream::Streams>(config.durability.storage_directory / "streams")),
      plan_cache_{FLAGS_query_plan_cache_max_size} {
  std::unique_ptr<storage::PlanInvalidator> invalidator = std::make_unique<PlanInvalidatorForDatabase>(plan_cache_);

#if USE_JEMALLOC
  // Route all constructor-body allocations (storage init, recovery, index structures) to this DB's arena.
  const memory::DbArenaScope db_arena_scope{ArenaIdx()};
#endif

  config.arena_registration = memgraph::memory::ArenaRegistration{&db_arena_};
  config.db_embedding_memory_tracker = &db_embedding_memory_tracker_;
  streams()->SetArenaIdx(ArenaIdx());

#if USE_JEMALLOC
  // Pin the after-commit trigger thread to this DB's arena so that all trigger allocations
  // (including raw operator new / std::string) are attributed to the right DB MemoryTracker.
  //
  // Ordering guarantee: this task is submitted before any trigger tasks can reach the pool
  // (the Database constructor has not returned yet, so no caller can AddTask). With pool size 1
  // the single thread processes tasks in FIFO order, so the pinning task always runs first.
  //
  // Thread replacement: ThreadPool never restarts a thread. If the pool thread terminates
  // (e.g. uncaught exception in a task), subsequent tasks are never processed — that is a
  // different bug. For normal operation the pinning task runs exactly once and persists for
  // the pool's lifetime.
  if (unsigned idx = ArenaIdx(); idx != 0) {
    after_commit_trigger_pool_.AddTask([idx]() mutable {
      if (int ret = je_mallctl("thread.arena", nullptr, nullptr, &idx, sizeof(unsigned)); ret != 0) {
        spdlog::error("Failed to pin after_commit_trigger_pool thread to arena {}: je_mallctl returned {}", idx, ret);
        return;
      }
      memory::tls_db_arena_state.arena = idx;
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
  info.db_memory_tracked = DbMemoryUsage();
  info.db_peak_memory_tracked = DbPeakMemoryUsage();
  info.db_storage_memory_tracked = DbStorageMemoryUsage();
  info.db_embedding_memory_tracked = DbEmbeddingMemoryUsage();
  info.db_query_memory_tracked = DbQueryMemoryUsage();
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
  const memory::DbArenaScope db_arena_scope{ArenaIdx()};
#endif
  storage_ = std::make_unique<memgraph::storage::DiskStorage>(
      std::move(storage_->config_), std::make_unique<storage::PlanInvalidatorDefault>(), preserved_factory);
}

}  // namespace memgraph::dbms

// DbArenaScope constructor implementation (Database* variant) - defined here
// to avoid circular include between db_arena.cpp and database.hpp
#if USE_JEMALLOC
namespace memgraph::memory {

DbArenaScope::DbArenaScope(memgraph::dbms::Database *db) : prev_arena_(tls_db_arena_state.arena) {
  if (db != nullptr) {
    tls_db_arena_state.arena = db->Arena().AcquireThreadArena();
  }
}

}  // namespace memgraph::memory
#endif  // USE_JEMALLOC
