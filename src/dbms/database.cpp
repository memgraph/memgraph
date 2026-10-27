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

#include <algorithm>
#include <cctype>
#include <charconv>
#include <exception>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <fmt/format.h>

#include "spdlog/spdlog.h"

#include "dbms/database_info.hpp"
#include "dbms/inmemory/history_replay.hpp"
#include "dbms/inmemory/storage_helper.hpp"
#include "flags/coord_flag_env_handler.hpp"
#include "flags/general.hpp"
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
#include "versioning/branch_ledger.hpp"
#include "versioning/branch_recovery.hpp"
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

namespace {

// A coordinator reports no per-database metrics under OpenMetrics, and some databases ask not to be
// measured at all; both hold an empty registration, which releases nothing.
auto RegisterMetrics(storage::Config const &config) -> metrics::PrometheusMetrics::Registration {
  if (!config.register_metrics) return {};
  if (FLAGS_metrics_format == "OpenMetrics" && flags::CoordinationSetupInstance().IsCoordinator()) return {};
  return metrics::Metrics().AddDatabase(config.salient.uuid, config.salient.name.str());
}

struct BranchRecoveryPlan {
  // Branches ordered by fork_ts; empty when the ledger is missing or unreadable.
  versioning::LedgerContents ledger;
  bool ledger_unreadable{false};
  // Set when branches can be rebuilt: main is recovered up to this timestamp, the rest replayed.
  std::optional<uint64_t> bound;
};

std::string BranchNames(const versioning::LedgerContents &ledger) {
  std::string names;
  for (const auto &branch : ledger.branches) {
    if (!names.empty()) names += ", ";
    names += branch.name;
  }
  return names;
}

// One past the highest numeric directory name under branches/; ignores non-numeric entries such as the ledger file.
uint64_t NextFreeBranchNumber(const std::filesystem::path &branches_directory) {
  uint64_t next = 0;
  std::error_code ec;
  for (std::filesystem::directory_iterator it{branches_directory, ec}, end; !ec && it != end; it.increment(ec)) {
    const auto name = it->path().filename().string();
    if (name.empty() || !std::ranges::all_of(name, [](unsigned char c) { return std::isdigit(c) != 0; })) continue;
    uint64_t number = 0;
    const auto [ptr, parse_ec] = std::from_chars(name.data(), name.data() + name.size(), number);
    if (parse_ec != std::errc{} || ptr != name.data() + name.size()) continue;
    next = std::max(next, number + 1);
  }
  return next;
}

// Runs before the storage exists; branches can only be rebuilt from the WAL.
BranchRecoveryPlan PlanBranchRecovery(const storage::Config &config, const std::filesystem::path &ledger_file) {
  BranchRecoveryPlan plan;
  auto read = versioning::BranchLedger{ledger_file}.Read();
  if (!read) {
    spdlog::warn("Graph Versioning: the branch ledger '{}' cannot be read ({}); all branches are dropped.",
                 ledger_file.string(),
                 read.error());
    plan.ledger_unreadable = true;
    return plan;
  }
  if (!read->has_value()) return plan;

  plan.ledger = std::move(**read);
  std::ranges::stable_sort(plan.ledger.branches, std::less{}, [](const auto &b) { return b.info.fork_ts; });
  if (plan.ledger.branches.empty()) return plan;

  const bool can_replay =
      config.salient.storage_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL &&
      config.durability.recover_on_startup &&
      config.durability.snapshot_wal_mode == storage::Config::Durability::SnapshotWalMode::PERIODIC_SNAPSHOT_WITH_WAL;
  if (can_replay) {
    plan.bound = BranchRecoveryBound(plan.ledger);
  } else {
    spdlog::warn(
        "Graph Versioning: branches [{}] cannot be recovered without recovery on startup and a write-ahead log; they "
        "are dropped.",
        BranchNames(plan.ledger));
  }
  return plan;
}

}  // namespace

Database::Database(storage::Config config, std::function<storage::DatabaseProtectorPtr()> database_protector_factory)
    : metrics_(RegisterMetrics(config)),
      db_arena_(std::make_unique<memory::ArenaPool>(&db_memory_tracker_)),
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
  // Captured before config is std::move()'d into the storage engine below.
  const auto versioning_directory = config.durability.storage_directory / "versioning";
  const auto branch_ledger_file = config.durability.storage_directory / "branches" / "ledger";
  std::unique_ptr<storage::PlanInvalidator> invalidator = std::make_unique<PlanInvalidatorForDatabase>(plan_cache_);

  // versioning/ is a legacy directory left by an older build.
  if (utils::DirExists(versioning_directory) && !utils::DeleteDir(versioning_directory)) {
    spdlog::warn("Graph Versioning: failed to remove legacy branch directory '{}'.", versioning_directory.string());
  }

  // Bound the per-DB cap by the global --memory-limit; SetHardLimit(0) falls back to it.
  if (auto global_max = utils::total_memory_tracker.MaximumHardLimit(); global_max > 0) {
    db_total_memory_tracker_.SetMaximumHardLimit(global_max);
    db_total_memory_tracker_.SetHardLimit(0);
  }

  BranchRecoveryPlan branch_plan;
  // Runtime fork pins of the restored branches, parallel to branch_plan.ledger.branches.
  std::vector<uint64_t> branch_pins;

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
    branch_plan = PlanBranchRecovery(config, branch_ledger_file);
    // Never reuse the number of a branch directory that is still on disk.
    branch_plan.ledger.next_number =
        std::max(branch_plan.ledger.next_number, NextFreeBranchNumber(branch_ledger_file.parent_path()));
    const auto create_storage = [&](storage::Config storage_config, storage::PlanInvalidatorPtr plan_invalidator) {
      return dbms::CreateInMemoryStorage(std::move(storage_config),
                                         std::move(plan_invalidator),
                                         metrics_.handles(),
                                         database_protector_factory,
                                         db_arena_.get(),
                                         &db_embedding_memory_tracker_);
    };

    if (!branch_plan.bound) {
      storage_ = create_storage(std::move(config), std::move(invalidator));
    } else {
      // Anything below that goes wrong drops the branches and recovers main as it would without them.
      auto unbounded_config = config;
      config.durability.recovery_history_bound = branch_plan.bound;
      std::string failure;
      try {
        storage_ = create_storage(std::move(config), std::move(invalidator));
        if (storage_->IsBroken()) failure = "recovery up to the oldest fork point failed";
      } catch (const std::exception &e) {
        failure = fmt::format("recovery up to the oldest fork point failed: {}", e.what());
      }

      if (failure.empty()) {
        auto &recovered = static_cast<storage::InMemoryStorage &>(*storage_);
        std::vector<uint64_t> fork_ts;
        fork_ts.reserve(branch_plan.ledger.branches.size());
        for (const auto &branch : branch_plan.ledger.branches) fork_ts.push_back(branch.info.fork_ts);
        auto tail = recovered.TakeRecoveryTail().value_or(storage::durability::RecoveryTail{});
        auto pins = ReplayHistoryTail(recovered, tail, fork_ts);
        if (pins) {
          branch_pins = std::move(*pins);
        } else {
          failure = std::move(pins.error());
        }
      }

      if (!failure.empty()) {
        spdlog::warn("Graph Versioning: {}; branches [{}] are dropped and main is recovered without them.",
                     failure,
                     BranchNames(branch_plan.ledger));
        if (storage_) {
          // The discarded storage holds a partial state that an exit snapshot must not persist.
          storage_->config_.durability.snapshot_on_exit = false;
          storage_.reset();
        }
        branch_plan.bound.reset();
        storage_ =
            create_storage(std::move(unbounded_config), std::make_unique<PlanInvalidatorForDatabase>(plan_cache_));
      }
    }
  }

  // Branches need IN_MEMORY_TRANSACTIONAL; InMemoryStorage is the only Storage reporting that mode.
  if (storage_->GetStorageMode() == storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    auto *in_memory_storage = static_cast<storage::InMemoryStorage *>(storage_.get());
    version_store_ = std::make_unique<versioning::VersionStore>(
        [in_memory_storage] { return in_memory_storage->RegisterBranchForkPin(); },
        [in_memory_storage](uint64_t fork_ts) { in_memory_storage->ReleaseForkPin(fork_ts); });
    version_store_->SetRetentionFloorCallback(
        [in_memory_storage](std::optional<uint64_t> floor) { in_memory_storage->SetRetentionFloor(floor); });
    version_store_->AttachLedger(branch_ledger_file);

    if (branch_plan.bound) {
      version_store_->Restore(branch_plan.ledger, branch_pins);
      spdlog::info("Graph Versioning: restored branches [{}].", BranchNames(branch_plan.ledger));
      versioning::RecoverBranchContents(*version_store_, *in_memory_storage);
    } else if (branch_plan.ledger_unreadable || !branch_plan.ledger.branches.empty()) {
      version_store_->ResetLedger(branch_plan.ledger.next_number);
    } else {
      version_store_->Restore(branch_plan.ledger, {});
    }
  }

  // Recovery adopts the uuid of the snapshot or WAL it recovered from, which need not be the one the
  // registration was made under.
  metrics_.Rebind(storage_->uuid());
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

void Database::AddTask(utils::ThreadPool::TaskSignature new_task) {
  if (!after_commit_trigger_pool_.AddTask(std::move(new_task))) {
    spdlog::warn(
        "Database '{}': could not schedule an after commit trigger because the database's background tasks are "
        "already shut down (database is being dropped or the process is shutting down); the trigger will not run.",
        name());
  }
}

void Database::StopAllBackgroundTasks() {
  streams()->Shutdown();
  auto const discarded = thread_pool()->ShutDown();
  if (discarded != 0) {
    spdlog::warn(
        "Database '{}': shutting down background tasks discarded {} queued after commit trigger execution(s); "
        "these triggers will not run.",
        name(),
        discarded);
  }
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
