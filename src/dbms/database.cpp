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
#include <set>

#include <nlohmann/json.hpp>

#include "dbms/database_info.hpp"
#include "dbms/inmemory/storage_helper.hpp"
#include "flags/coord_flag_env_handler.hpp"
#include "flags/general.hpp"
#include "kvstore/kvstore.hpp"
#include "memory/db_arena.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/ttl.hpp"
#include "utils/file.hpp"
#include "versioning/version_store.hpp"

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

std::unique_ptr<storage::Accessor> Database::Access(storage::StorageAccessType rw_type,
                                                    std::optional<storage::IsolationLevel> override_isolation_level,
                                                    std::optional<std::chrono::milliseconds> timeout) {
  return storage_->Access(rw_type, override_isolation_level, timeout);
}

std::unique_ptr<storage::Accessor> Database::UniqueAccess(
    std::optional<storage::IsolationLevel> override_isolation_level, std::optional<std::chrono::milliseconds> timeout) {
  return storage_->UniqueAccess(override_isolation_level, timeout);
}

std::unique_ptr<storage::Accessor> Database::ReadOnlyAccess(
    std::optional<storage::IsolationLevel> override_isolation_level, std::optional<std::chrono::milliseconds> timeout) {
  return storage_->ReadOnlyAccess(override_isolation_level, timeout);
}

std::string Database::name() const { return storage_->name(); }

utils::SafeString::ConstSafeWrapper Database::name_view() const { return storage_->name_view(); }

const utils::UUID &Database::uuid() const { return storage_->uuid(); }

const storage::Config &Database::config() const { return storage_->config_; }

storage::StorageMode Database::GetStorageMode() const noexcept { return storage_->GetStorageMode(); }

storage::ttl::TTL &Database::ttl() { return storage_->ttl_; }

memory::ArenaPool &Database::Arena() noexcept { return *db_arena_; }

memory::ArenaPool &Database::Arena() const noexcept { return *db_arena_; }

Database::Database(storage::Config config, std::function<storage::DatabaseProtectorPtr()> database_protector_factory)
    : db_arena_(std::make_unique<memory::ArenaPool>(&db_memory_tracker_)),
      metrics_(config.salient.uuid, ([&config]() -> metrics::DatabaseMetricHandles {
                 if (!config.register_metrics) {
                   return {};
                 }
                 auto const should_register =
                     !(FLAGS_metrics_format == "OpenMetrics" && flags::CoordinationSetupInstance().IsCoordinator());
                 if (!should_register) {
                   return {};
                 }
                 return metrics::Metrics().AddDatabase(config.salient.uuid, config.salient.name.str());
               })()),
      after_commit_trigger_pool_{1,
                                 // After-commit triggers run on a dedicated DB worker.
                                 // Keep a DB arena scope alive for the full worker lifetime.
                                 [this]() -> utils::ThreadPool::TaskSignature {
                                   auto db_arena_scope = std::make_unique<memory::DbArenaScope>(this);
                                   return [db_arena_scope = std::move(db_arena_scope)]() mutable {
                                     db_arena_scope.reset();
                                   };
                                 }},
      streams_(
          std::make_unique<query::stream::Streams>(config.durability.storage_directory / "streams", db_arena_.get())),
      plan_cache_{FLAGS_query_plan_cache_max_size} {
  // Route all constructor-body allocations (storage init, recovery, index structures) to this DB's arena.
  const memory::DbArenaScope db_arena_scope{this};

  // Postpone creation after the scope has been created
  trigger_store_ = std::make_unique<query::TriggerStore>(config.durability.storage_directory / "triggers");
  // Captured before config is std::move()'d into whichever storage engine gets constructed below;
  // version_store_ itself can only be constructed after storage_ exists (its GC-pin callbacks bind
  // to the concrete InMemoryStorage instance).
  const auto versioning_directory = config.durability.storage_directory / "versioning";
  std::unique_ptr<storage::PlanInvalidator> invalidator = std::make_unique<PlanInvalidatorForDatabase>(plan_cache_);

  // Graph Versioning v1 branch durability (S3d, design doc opencode-work/versioning-v1/
  // 2026-07-13--durability-S2S3-design-v4.html §2). Pre-read any persisted branches' fork_ts values
  // BEFORE storage construction below, so InMemoryStorage's recovery ctor sequence knows how far
  // back it must reconstruct main's history (see storage::Config::Durability::
  // recover_oldest_fork_ts's doc-comment, config.hpp). This uses a SEPARATE, throwaway KVStore
  // handle over the SAME versioning/ directory the real versioning::VersionStore (below) will open
  // -- it must be fully closed (out of scope) before that happens, because kvstore's backing
  // RocksDB instance takes an exclusive, process-wide directory lock; two concurrently-live
  // KVStore handles over the same path is undefined behavior (see kvstore::KVStore's own ctor
  // doc-comment). Scoped in the block below so the throwaway handle's destructor runs before
  // `storage_` (and later `version_store_`) are ever touched.
  //
  // If the directory doesn't exist yet, no branch has ever been created against this database --
  // leave both new Config fields at their default (nullopt / empty set), which keeps every
  // downstream recovery code path byte-identical to today (see recover_oldest_fork_ts's
  // doc-comment: "no branches" is the fast path, unconditionally).
  if (utils::DirExists(versioning_directory)) {
    std::set<uint64_t> fork_timestamps;
    {
      kvstore::KVStore throwaway_kv{versioning_directory};
      for (auto const &[name, data] : throwaway_kv) {
        // Reserved, never-a-branch-name key for the monotonic branch-number counter (mirrors
        // versioning::VersionStore's private kNextNumberKey, version_store.cpp) -- must be skipped
        // here exactly like VersionStore's own ctor skips it.
        if (name == ".next_number") continue;

        auto json_data = nlohmann::json::parse(data, /*cb=*/nullptr, /*allow_exceptions=*/false);
        if (json_data.is_discarded() || !json_data.is_object() || !json_data.contains("fork_ts") ||
            !json_data["fork_ts"].is_number_unsigned()) {
          // Mirrors VersionStore's own tolerant "warn and skip" handling of a corrupt/legacy record
          // (see its ctor, version_store.cpp) -- a single bad record must not abort the whole
          // database's startup. This branch's history simply won't be reconstructed by the
          // windowed-replay pass below; VersionStore's own load (later) will independently hit the
          // same corrupt record and warn again.
          spdlog::warn(
              "Graph Versioning v1: failed to pre-read branch '{}' for durability recovery (corrupt or "
              "missing fork_ts) -- this branch's history may not be reconstructable after this restart.",
              name);
          continue;
        }
        fork_timestamps.insert(json_data["fork_ts"].get<uint64_t>());
      }
      // `throwaway_kv` goes out of scope here, releasing the RocksDB directory lock before
      // `storage_`/`version_store_` are constructed below.
    }
    if (!fork_timestamps.empty()) {
      config.durability.recover_oldest_fork_ts = *fork_timestamps.begin();  // std::set is ordered ascending
      config.durability.recover_fork_timestamps = std::move(fork_timestamps);
    }
  }

  // Bound the per-DB cap by the global --memory-limit; SetHardLimit(0) falls back to it.
  if (auto global_max = utils::total_memory_tracker.MaximumHardLimit(); global_max > 0) {
    db_total_memory_tracker_.SetMaximumHardLimit(global_max);
    db_total_memory_tracker_.SetHardLimit(0);
  }

  if (config.salient.storage_mode == memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL || config.force_on_disk ||
      utils::DirExists(config.disk.main_storage_directory)) {
    config.salient.storage_mode = memgraph::storage::StorageMode::ON_DISK_TRANSACTIONAL;
    storage_ = std::make_unique<storage::DiskStorage>(std::move(config),
                                                      std::move(invalidator),
                                                      metrics_.handles(),
                                                      database_protector_factory,
                                                      db_arena_.get(),
                                                      &db_embedding_memory_tracker_);
  } else {
    storage_ = dbms::CreateInMemoryStorage(std::move(config),
                                           std::move(invalidator),
                                           metrics_.handles(),
                                           database_protector_factory,
                                           db_arena_.get(),
                                           &db_embedding_memory_tracker_);
  }

  // Graph versioning (branches) only runs against IN_MEMORY_TRANSACTIONAL storage (chunk-0 gate);
  // version_store_ stays null for ON_DISK_TRANSACTIONAL and IN_MEMORY_ANALYTICAL. Safe to downcast:
  // InMemoryStorage is the only concrete Storage subclass that ever reports IN_MEMORY_TRANSACTIONAL.
  if (storage_->GetStorageMode() == storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    auto *in_memory_storage = static_cast<storage::InMemoryStorage *>(storage_.get());
    version_store_ = std::make_unique<versioning::VersionStore>(
        versioning_directory,
        [in_memory_storage] { return in_memory_storage->RegisterForkPin(); },
        [in_memory_storage](uint64_t fork_ts) { in_memory_storage->ReleaseForkPin(fork_ts); });
  }
}

Database::DatabaseMetricsRegistration::~DatabaseMetricsRegistration() { metrics::Metrics().RemoveDatabase(uuid_); }

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
                                                              metrics_.handles(),
                                                              preserved_factory,
                                                              db_arena_.get(),
                                                              &db_embedding_memory_tracker_);
}

}  // namespace memgraph::dbms

// DbArenaScope constructor implementation (Database* variant) - defined here
// to avoid circular include between db_arena.cpp and database.hpp
namespace memgraph::memory {
DbArenaScope::DbArenaScope(const memgraph::dbms::Database *db) : DbArenaScope(db != nullptr ? &db->Arena() : nullptr) {}
}  // namespace memgraph::memory
