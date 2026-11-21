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

#include <functional>
#include <memory>
#ifdef MG_ENTERPRISE
#include <algorithm>
#include <exception>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <fmt/format.h>
#endif

#include "spdlog/spdlog.h"

#include "dbms/database_info.hpp"
#include "dbms/database_protector.hpp"
#ifdef MG_ENTERPRISE
#include "dbms/inmemory/history_replay.hpp"
#endif
#include "dbms/inmemory/replication_handlers.hpp"
#include "dbms/inmemory/storage_helper.hpp"
#include "flags/coord_flag_env_handler.hpp"
#include "flags/general.hpp"
#include "memory/db_arena.hpp"
#include "metrics/prometheus_metrics.hpp"
#include "query/stream/streams.hpp"
#include "query/trigger.hpp"
#include "storage/v2/database_protector.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/ttl.hpp"
#ifdef MG_ENTERPRISE
#include "storage/v2/inmemory/storage.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/gate.hpp"
#include "versioning/version_store.hpp"
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

Database::~Database() {
  // Every teardown path (drop, suspend, drop-recreate, process exit) destroys via this destructor, and
  // the cached 2PC accessor is storage-level, not gatekeeper-counted -- this is the one choke point
  // that reaches it. Safe: the body runs before storage_ is destroyed.
  //
  // Must not call any utils::Gatekeeper method here -- finish_suspend() holds GKInternals::mutex_
  // across ~Database (gatekeeper.hpp:394), so re-entry would self-deadlock.
  try {
    // GatekeeperGuard clears arena TLS around Database destruction; re-establish it so Abort()'s
    // delta walk allocates against this tenant's arena, not stale TLS.
    const memory::DbArenaScope db_arena_scope{this};
    InMemoryReplicationHandlers::AbortTwoPCForTenant(uuid());
  } catch (const std::exception &e) {
    // spdlog can throw; swallow it so it can't escape this implicitly-noexcept destructor (mirrors ~Gatekeeper).
    try {
      spdlog::error(
          "Database::~Database: failed to abort cached 2PC for tenant {}: {} -- prepared transaction stays pinned in "
          "the commit log.",
          std::string{uuid()},
          e.what());
    } catch (...) {  // NOLINT(bugprone-empty-catch)
    }
  } catch (...) {
    try {
      spdlog::error(
          "Database::~Database: failed to abort cached 2PC for tenant {} (unknown exception) -- prepared transaction "
          "stays pinned in the commit log.",
          std::string{uuid()});
    } catch (...) {  // NOLINT(bugprone-empty-catch)
    }
  }
}

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

void Database::Rename(std::string_view new_name) {
  storage_->config_.salient.name = new_name;
  metrics_.Rename(new_name);
}

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

#ifdef MG_ENTERPRISE
struct BranchStartup {
  // Ordered by durable_fork_ts.
  versioning::LedgerContents ledger;
  // Set when branches can be rebuilt: main is recovered up to this timestamp, the rest replayed.
  std::optional<uint64_t> bound;
  // Set when main could not be recovered up to the oldest fork: main recovers on its own and every branch is
  // unavailable with this reason.
  std::optional<std::string> unavailable;
  // The ledger cannot be read: the message every branch operation fails with. No VersionStore exists, so the ledger is
  // never written.
  std::optional<std::string> ledger_unreadable;
};

// Runs before the storage exists. Never refuses the start: what cannot be rebuilt is made unavailable, main is never
// bounded by branches it cannot rebuild.
BranchStartup PlanBranchStartup(const storage::Config &config, const versioning::BranchLedger &ledger_store) {
  BranchStartup plan;
  auto read = ledger_store.Read();
  if (!read) {
    std::string_view reason{read.error()};
    if (reason.ends_with('.')) reason.remove_suffix(1);
    plan.ledger_unreadable = fmt::format(
        "The branch ledger of database '{}' cannot be read, so its Graph Versioning branches are unavailable: {}. Main "
        "works as usual, but no branch can be listed, created, used or dropped. The ledger is left as it is: to "
        "continue, restore the DBMS durability store (databases/.durability) from a backup and restart Memgraph.",
        config.salient.name.str(),
        reason);
    spdlog::warn("Graph Versioning: {}", *plan.ledger_unreadable);
    return plan;
  }
  if (!read->has_value()) return plan;

  plan.ledger = std::move(**read);
  if (plan.ledger.branches.empty()) return plan;
  std::ranges::stable_sort(
      plan.ledger.branches, std::less{}, [](const auto &b) { return b.info.fork.durable_fork_ts; });

  plan.bound = BranchRecoveryBound(plan.ledger);
  return plan;
}

uint64_t LastDurableTimestamp(const storage::InMemoryStorage &storage) {
  return storage.repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;
}

// The fork a new branch records. Order matters: main's last durable commit is read before the pin (a lower bound), the
// id counters right after it (every gid visible at the fork is below them).
versioning::ForkPoint AcquireForkPoint(storage::InMemoryStorage &main) {
  const auto main_durable_ts = LastDurableTimestamp(main);
  const auto durable_fork_ts = main.RegisterBranchForkPin();
  const auto vertex_gid_floor = main.vertex_id_.load(std::memory_order_acquire);
  const auto edge_gid_floor = main.edge_id_.load(std::memory_order_acquire);
  const auto counts = main.GetApproximateCounts();
  return {.durable_fork_ts = durable_fork_ts,
          .main_durable_ts = main_durable_ts,
          .vertex_gid_floor = vertex_gid_floor,
          .edge_gid_floor = edge_gid_floor,
          .counts = {.vertices = counts.vertices, .edges = counts.edges}};
}
#endif  // MG_ENTERPRISE

}  // namespace

Database::Database(storage::Config config, [[maybe_unused]] kvstore::KVStore *durability)
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
#ifdef MG_ENTERPRISE
  // Captured before config is std::move()'d into the storage engine below.
  const auto branches_directory = config.durability.storage_directory / "branches";
  const bool versioning_enabled = config.experimental_versioning;
  // With versioning off the ledger is not looked at, as on master.
  std::optional<versioning::BranchLedger> branch_ledger;
  if (versioning_enabled) {
    DMG_ASSERT(durability != nullptr, "Graph Versioning requires the DBMS durability store");
    branch_ledger.emplace(*durability, config.salient.uuid);
  }
  BranchStartup branches;
  std::vector<uint64_t> fork_pins;
#endif
  std::unique_ptr<storage::PlanInvalidator> invalidator = std::make_unique<PlanInvalidatorForDatabase>(plan_cache_);
  auto database_protector_factory = [this]() -> storage::DatabaseProtectorPtr {
    if (auto db_acc = gatekeeper_ref_.load(std::memory_order_acquire).access()) {
      return std::make_unique<DatabaseProtector>(*std::move(db_acc));
    }
    return nullptr;
  };

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
#ifdef MG_ENTERPRISE
    if (versioning_enabled && config.salient.storage_mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
      branches = PlanBranchStartup(config, *branch_ledger);
    }
    // Branches never change what main recovers; they only keep its history recoverable down to their oldest fork.
    const storage::durability::HistoryBounds history_bounds{
        .floor = BranchRecoveryBound(branches.ledger).value_or(storage::durability::kNoRetentionFloor)};
    // What main recovers with when it cannot be recovered up to the oldest fork.
    std::optional<storage::Config> main_only;
    if (branches.bound) main_only = config;

    const auto build_storage = [&](storage::Config storage_config,
                                   storage::durability::HistoryBounds bounds,
                                   std::unique_ptr<storage::PlanInvalidator>
                                       inval) {
      storage_ = dbms::CreateInMemoryStorage(std::move(storage_config),
                                             std::move(inval),
                                             metrics_.handles(),
                                             database_protector_factory,
                                             db_arena_.get(),
                                             &db_embedding_memory_tracker_,
                                             bounds);
    };

    // Phase 1 recovers main up to the oldest fork, phase 2 replays the rest and pins each fork on the way. Any failure,
    // of whatever kind, falls back to the plain recovery below.
    std::string failure;
    try {
      auto bounds = history_bounds;
      bounds.recover_only_to_floor = branches.bound.has_value();
      build_storage(std::move(config), bounds, std::move(invalidator));
      if (branches.bound) {
        if (storage_->IsBroken()) {
          failure = "recovery up to the oldest fork point failed";
        } else {
          auto &recovered = static_cast<storage::InMemoryStorage &>(*storage_);
          std::vector<uint64_t> fork_ts;
          fork_ts.reserve(branches.ledger.branches.size());
          for (const auto &branch : branches.ledger.branches) fork_ts.push_back(branch.info.fork.durable_fork_ts);
          const auto tail = recovered.TakeRecoveryTail().value_or(storage::durability::RecoveryTail{});
          if (auto pins = ReplayHistoryTail(recovered, tail, fork_ts)) {
            fork_pins = std::move(*pins);
          } else {
            failure = std::move(pins.error());
          }
        }
      }
    } catch (const std::exception &e) {
      if (!branches.bound) throw;
      failure = fmt::format("recovery up to the oldest fork point failed: {}", e.what());
    } catch (...) {
      if (!branches.bound) throw;
      failure = "recovery up to the oldest fork point failed";
    }

    if (!failure.empty()) {
      // A storage left behind by a failure must not write an exit snapshot over the files it recovered from.
      if (storage_) {
        storage_->config_.durability.snapshot_on_exit = false;
        storage_.reset();
      }
      // Main starts as it would without branches (its own recovery failing is not ours to mask).
      branches.bound.reset();
      branches.unavailable = fmt::format("main cannot be recovered up to the branch's fork point: {}", failure);
      build_storage(std::move(*main_only), history_bounds, std::make_unique<PlanInvalidatorForDatabase>(plan_cache_));
    }
#else
    storage_ = dbms::CreateInMemoryStorage(std::move(config),
                                           std::move(invalidator),
                                           metrics_.handles(),
                                           database_protector_factory,
                                           db_arena_.get(),
                                           &db_embedding_memory_tracker_);
#endif
  }

#ifdef MG_ENTERPRISE
  if (branches.ledger_unreadable) branches_unavailable_ = std::move(branches.ledger_unreadable);

  // Branches need IN_MEMORY_TRANSACTIONAL; InMemoryStorage is the only Storage reporting that mode.
  if (versioning_enabled && !branches_unavailable_ &&
      storage_->GetStorageMode() == storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    auto &in_memory = static_cast<storage::InMemoryStorage &>(*storage_);
    version_store_ = std::make_unique<versioning::VersionStore>(
        [&in_memory] { return AcquireForkPoint(in_memory); },
        [&in_memory](uint64_t fork_ts) { in_memory.ReleaseForkPin(fork_ts); },
        [&in_memory](uint64_t floor) { in_memory.SetRetentionFloor(floor); },
        std::move(*branch_ledger),
        branches_directory);

    if (branches.unavailable) {
      // Main has no replayed history to pin. Later commits must not land inside a recorded fork's base, or a restart
      // that recovers the branch would include them.
      in_memory.BumpClockPastDurable(branches.ledger.branches.back().info.fork.durable_fork_ts);
      for (size_t i = 0; i < branches.ledger.branches.size(); ++i) fork_pins.push_back(in_memory.RegisterForkPin());
    }
    DMG_ASSERT(fork_pins.size() == branches.ledger.branches.size(), "Every restored branch needs its fork pin");
    std::vector<versioning::VersionStore::RestoredBranch> restored;
    restored.reserve(fork_pins.size());
    for (size_t i = 0; i < fork_pins.size(); ++i) {
      restored.push_back({.branch = branches.ledger.branches[i], .runtime_pin = fork_pins[i]});
    }
    version_store_->Restore(branches.ledger.next_branch_id, restored);

    const auto last_durable_ts = LastDurableTimestamp(in_memory);
    for (const auto &[name, info] : branches.ledger.branches) {
      std::optional<std::string> reason = branches.unavailable;
      if (!reason && info.fork.main_durable_ts > last_durable_ts) {
        reason = fmt::format("main recovered only up to durable timestamp {}, but the branch forked from {}",
                             last_durable_ts,
                             info.fork.main_durable_ts);
      }
      if (!reason) continue;
      version_store_->MarkUnavailable(info.id, *reason);
      spdlog::warn("Graph Versioning: branch '{}' of database '{}' is unavailable: {}.", name, this->name(), *reason);
    }
    // Branch WAL replay and the raising of main's gid counters past the branches' gids belong here: before the
    // database is published and serves writes.
  }
#endif

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
  // Signal any in-flight after-commit trigger to abort, so the pool join below cannot block on a
  // long-running trigger that holds an accessor pinning this database.
  after_commit_trigger_status_.store(query::TransactionStatus::TERMINATED, std::memory_order_release);
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
#ifdef MG_ENTERPRISE
  // Its callbacks hold the InMemoryStorage about to be replaced; the interpreter refuses the switch while a branch
  // exists, so the registry is empty.
  version_store_.reset();
#endif
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
