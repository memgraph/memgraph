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

#if USE_JEMALLOC
memory::ArenaPool &Database::Arena() noexcept { return *db_arena_; }

memory::ArenaPool &Database::Arena() const noexcept { return *db_arena_; }
#endif

Database::Database(storage::Config config, std::function<storage::DatabaseProtectorPtr()> database_protector_factory)
    :
#if USE_JEMALLOC
      db_arena_(std::make_unique<memory::ArenaPool>(&db_memory_tracker_)),
#endif
      after_commit_trigger_pool_{1,
#if USE_JEMALLOC
                                 // After-commit triggers run on a dedicated DB worker.
                                 // Keep a DB arena scope alive for the full worker lifetime.
                                 [this]() -> utils::ThreadPool::TaskSignature {
                                   auto db_arena_scope = std::make_unique<memory::DbArenaScope>(db_arena_.get());
                                   return [db_arena_scope = std::move(db_arena_scope)]() mutable { db_arena_scope.reset(); };
                                 }
#else
                                 {}
#endif
      },
      streams_(std::make_unique<query::stream::Streams>(
          config.durability.storage_directory / "streams"
#if USE_JEMALLOC
          ,
          db_arena_.get()
#endif
          )),
      plan_cache_{FLAGS_query_plan_cache_max_size} {
  // Route all constructor-body allocations (storage init, recovery, index structures) to this DB's arena.
  const memory::DbArenaScope db_arena_scope{this, memory::DbArenaScope::Type::FORCE};

  // Postpone creation after the scope has been created
  trigger_store_ = std::make_unique<query::TriggerStore>(config.durability.storage_directory / "triggers");
  std::unique_ptr<storage::PlanInvalidator> invalidator = std::make_unique<PlanInvalidatorForDatabase>(plan_cache_);

  if (config.salient.storage_mode == memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL || config.force_on_disk ||
      utils::DirExists(config.disk.main_storage_directory)) {
    config.salient.storage_mode = memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL;
    storage_ = std::make_unique<storage::DiskStorage>(std::move(config),
                                                      std::move(invalidator),
                                                      database_protector_factory,
#if USE_JEMALLOC
                                                      db_arena_.get(),
#else
                                                      nullptr,
#endif
                                                      &db_embedding_memory_tracker_);
  } else {
    storage_ = dbms::CreateInMemoryStorage(std::move(config),
                                           std::move(invalidator),
                                           database_protector_factory,
#if USE_JEMALLOC
                                           db_arena_.get(),
#else
                                           nullptr,
#endif
                                           &db_embedding_memory_tracker_);
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
  const memory::DbArenaScope db_arena_scope{this};
  storage_ = std::make_unique<memgraph::storage::DiskStorage>(std::move(storage_->config_),
                                                              std::make_unique<storage::PlanInvalidatorDefault>(),
                                                              preserved_factory,
#if USE_JEMALLOC
                                                              db_arena_.get(),
#else
                                                              nullptr,
#endif
                                                              &db_embedding_memory_tracker_);
}

}  // namespace memgraph::dbms

// DbArenaScope constructor implementation (Database* variant) - defined here
// to avoid circular include between db_arena.cpp and database.hpp
namespace memgraph::memory {
#if USE_JEMALLOC
DbArenaScope::DbArenaScope(const memgraph::dbms::Database *db, DbArenaScope::Type type)
    : DbArenaScope(db != nullptr ? &db->Arena() : nullptr, type) {}
#else
DbArenaScope::DbArenaScope(const memgraph::dbms::Database * /*db*/, DbArenaScope::Type /*type*/) : DbArenaScope() {}
#endif
}  // namespace memgraph::memory
