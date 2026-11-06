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
#include <charconv>
#include <exception>
#include <filesystem>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <unordered_map>
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
#include "utils/string.hpp"
#include "versioning/branch_ledger.hpp"
#include "versioning/branch_recovery.hpp"
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
struct BranchRecoveryPlan {
  // Branches ordered by fork_ts; empty when the ledger is missing.
  versioning::LedgerContents ledger;
  // Set when branches can be rebuilt: main is recovered up to this timestamp, the rest replayed.
  std::optional<uint64_t> bound;
  // Versioning is off and the ledger holds branches: main recovers normally; ledger and branch files stay untouched.
  bool hidden{false};
  // Versioning is on and the ledger cannot be read: the message that every branch operation fails with.
  std::optional<std::string> ledger_unreadable;
  // Versioning is on but the branches cannot be rebuilt: main recovers on its own and every branch is set aside as
  // unavailable with this reason.
  std::optional<std::string> set_aside;
};

std::string BranchNames(const versioning::LedgerContents &ledger) {
  return utils::Join(ledger.branches | std::views::transform(&versioning::LedgerBranch::name), ", ");
}

// Non-numeric entries such as the ledger file are ignored.
std::vector<uint64_t> BranchDirectoryNumbers(const std::filesystem::path &branches_directory) {
  std::vector<uint64_t> numbers;
  std::error_code ec;
  for (std::filesystem::directory_iterator it{branches_directory, ec}, end; !ec && it != end; it.increment(ec)) {
    const auto name = it->path().filename().string();
    uint64_t number = 0;
    const auto [ptr, parse_ec] = std::from_chars(name.data(), name.data() + name.size(), number);
    if (parse_ec != std::errc{} || ptr != name.data() + name.size()) continue;
    numbers.push_back(number);
  }
  std::ranges::sort(numbers);
  return numbers;
}

uint64_t NextFreeBranchNumber(const std::filesystem::path &branches_directory) {
  const auto numbers = BranchDirectoryNumbers(branches_directory);
  return numbers.empty() ? 0 : numbers.back() + 1;
}

// Runs before the storage exists; branches can only be rebuilt from the WAL. Never refuses the start: what cannot be
// rebuilt is set aside.
BranchRecoveryPlan PlanBranchRecovery(const storage::Config &config, const std::filesystem::path &ledger_file) {
  BranchRecoveryPlan plan;
  const bool versioning = config.experimental_versioning;
  auto read = versioning::BranchLedger{ledger_file}.Read();
  if (!read) {
    std::string_view reason{read.error()};
    if (reason.ends_with('.')) reason.remove_suffix(1);
    if (!versioning) {
      spdlog::warn(
          "Graph Versioning is disabled, and the branch ledger '{}' of database '{}' cannot be read: {}. The branches "
          "in "
          "it are not known, so main's snapshots and WAL are not kept for them. The ledger is left as it is: repair or "
          "restore it before starting with --experimental-enabled=versioning.",
          ledger_file.string(),
          config.salient.name.str(),
          reason);
      return plan;
    }
    // Branches are unknown, so none is recovered; main's snapshots and WAL are all kept (floor 0) so the history they
    // fork from survives.
    plan.ledger_unreadable = fmt::format(
        "The branch ledger of database '{}' cannot be read, so its Graph Versioning branches are unavailable: {}. Main "
        "works as usual, but no branch can be listed, created, used or dropped, and main's snapshots and write-ahead "
        "log files are all kept meanwhile. To continue: restore the ledger file '{}' from a backup and restart "
        "Memgraph; or, to discard every branch, stop Memgraph, move the 'branches' directory away and start it again.",
        config.salient.name.str(),
        reason,
        ledger_file.string());
    spdlog::warn("Graph Versioning: {}", *plan.ledger_unreadable);
    return plan;
  }
  if (!read->has_value()) {
    if (const auto orphans = BranchDirectoryNumbers(ledger_file.parent_path()); !orphans.empty()) {
      spdlog::warn(
          "Graph Versioning: database '{}' has branch directories [{}] in '{}' but no branch ledger '{}'. No branch is "
          "restored and the data in those directories is not used; restore the ledger file to get the branches back.",
          config.salient.name.str(),
          utils::Join(orphans | std::views::transform([](uint64_t n) { return std::to_string(n); }), ", "),
          ledger_file.parent_path().string(),
          ledger_file.string());
    }
    return plan;
  }

  plan.ledger = std::move(**read);
  std::ranges::stable_sort(plan.ledger.branches, std::less{}, [](const auto &b) { return b.info.fork_ts; });
  if (plan.ledger.branches.empty()) return plan;

  // Checked first: branches that could not be rebuilt later are not worth hiding. Same on-disk test as the ctor's.
  const bool on_disk = config.salient.storage_mode == storage::StorageMode::ON_DISK_TRANSACTIONAL ||
                       config.force_on_disk || utils::DirExists(config.disk.main_storage_directory);
  const auto mode = on_disk ? storage::StorageMode::ON_DISK_TRANSACTIONAL : config.salient.storage_mode;
  // Startup flags that cannot host branches are refused before any database is built (versioning on); only a database
  // that differs from them, or a direct construction, gets here.
  std::optional<std::string> unrecoverable;
  if (mode != storage::StorageMode::IN_MEMORY_TRANSACTIONAL) {
    unrecoverable = fmt::format("the {} storage mode does not support them", storage::StorageModeToString(mode));
  } else if (!versioning::BranchesDurable(config)) {
    unrecoverable = "they are rebuilt from the write-ahead log, which needs recovery on startup and the WAL";
  }
  if (unrecoverable && versioning) {
    spdlog::warn(
        "Graph Versioning: the branches [{}] of database '{}' are unavailable: {}. They stay in the ledger and on "
        "disk.",
        BranchNames(plan.ledger),
        config.salient.name.str(),
        *unrecoverable);
    if (mode == storage::StorageMode::IN_MEMORY_TRANSACTIONAL) plan.set_aside = std::move(unrecoverable);
    return plan;
  }

  if (!versioning) {
    if (unrecoverable) {
      spdlog::warn(
          "Graph Versioning is disabled and the existing branches [{}] of database '{}' could not be recovered with "
          "these settings: {}. They stay in the ledger and on disk. --experimental-enabled=versioning needs the "
          "IN_MEMORY_TRANSACTIONAL storage mode, the WAL and --data-recovery-on-startup=true, and does not start "
          "without them.",
          BranchNames(plan.ledger),
          config.salient.name.str(),
          *unrecoverable);
    }
    spdlog::info(
        "Graph Versioning is disabled (--experimental-enabled=versioning is not set); the existing branches [{}] are "
        "hidden: SHOW BRANCHES lists them and DROP BRANCH removes them. They come back when versioning is enabled "
        "again, unless this instance is or becomes a REPLICA, which has no branches and deletes them.",
        BranchNames(plan.ledger));
    plan.hidden = true;
    return plan;
  }

  plan.bound = BranchRecoveryBound(plan.ledger);
  return plan;
}
#endif  // MG_ENTERPRISE

}  // namespace

Database::Database(storage::Config config)
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
  const auto branch_ledger_file = config.durability.storage_directory / "branches" / "ledger";
  const bool versioning_enabled = config.experimental_versioning;
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

#ifdef MG_ENTERPRISE
  auto branch_plan = PlanBranchRecovery(config, branch_ledger_file);
  std::unordered_map<uint64_t, std::string> history_shortfalls;
  // Never reuse the number of a branch directory that is still on disk.
  branch_plan.ledger.next_number =
      std::max(branch_plan.ledger.next_number, NextFreeBranchNumber(branch_ledger_file.parent_path()));
#endif

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
    // Main's WAL and snapshots must keep the history the hidden branches fork from; the snapshot thread starts with
    // the storage, so the floor goes in through the config.
    if (branch_plan.hidden || branch_plan.set_aside) {
      config.durability.retention_floor = BranchRecoveryBound(branch_plan.ledger);
    }
    // The branches' fork points are unknown, so no snapshot or WAL file of main may be deleted.
    if (branch_plan.ledger_unreadable) config.durability.retain_all_files = true;
    // What main recovers with when the branches cannot be rebuilt: as without them, the history kept for them.
    std::optional<storage::Config> main_only;
    if (branch_plan.bound) {
      main_only = config;
      main_only->durability.retention_floor = branch_plan.bound;
      config.durability.recovery_history_bound = branch_plan.bound;
    }
    const auto build_storage = [&](storage::Config storage_config, std::unique_ptr<storage::PlanInvalidator> inval) {
      storage_ = dbms::CreateInMemoryStorage(std::move(storage_config),
                                             std::move(inval),
                                             metrics_.handles(),
                                             database_protector_factory,
                                             db_arena_.get(),
                                             &db_embedding_memory_tracker_);
    };
    std::string failure;
    try {
      build_storage(std::move(config), std::move(invalidator));
      if (branch_plan.bound && storage_->IsBroken()) failure = "recovery up to the oldest fork point failed";
    } catch (const std::exception &e) {
      if (!branch_plan.bound) throw;
      failure = fmt::format("recovery up to the oldest fork point failed: {}", e.what());
    }

    if (branch_plan.bound && failure.empty()) {
      auto &recovered = static_cast<storage::InMemoryStorage &>(*storage_);
      std::vector<uint64_t> fork_ts;
      fork_ts.reserve(branch_plan.ledger.branches.size());
      for (const auto &branch : branch_plan.ledger.branches) fork_ts.push_back(branch.info.fork_ts);
      auto tail = recovered.TakeRecoveryTail().value_or(storage::durability::RecoveryTail{});
      std::vector<uint64_t> durable_at_fork;
      if (auto pins = ReplayHistoryTail(recovered, tail, fork_ts, &durable_at_fork)) {
        history_shortfalls = versioning::FindHistoryShortfalls(branch_plan.ledger, durable_at_fork);
        // The ledger keeps recorded_fork_ts; the store runs on the pins taken on the rebuilt history.
        for (size_t i = 0; i < pins->size(); ++i) branch_plan.ledger.branches[i].info.fork_ts = (*pins)[i];
      } else {
        failure = std::move(pins.error());
      }
    }

    if (!failure.empty()) {
      // A storage left behind by a failure must not write an exit snapshot over the files it recovered from.
      if (storage_) {
        storage_->config_.durability.snapshot_on_exit = false;
        storage_.reset();
      }
      // Main starts as it would without branches (its own recovery failing is not ours to mask); the branches are
      // set aside.
      branch_plan.set_aside = fmt::format("main cannot be recovered up to the branch's fork point: {}", failure);
      build_storage(std::move(*main_only), std::make_unique<PlanInvalidatorForDatabase>(plan_cache_));
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
  // Branches need IN_MEMORY_TRANSACTIONAL; InMemoryStorage is the only Storage reporting that mode. With versioning off
  // the registry exists only to keep the branches already in the ledger, so a database without any has none.
  if (storage_->GetStorageMode() == storage::StorageMode::IN_MEMORY_TRANSACTIONAL &&
      (versioning_enabled || !branch_plan.ledger.branches.empty())) {
    auto *in_memory_storage = static_cast<storage::InMemoryStorage *>(storage_.get());
    version_store_ = std::make_unique<versioning::VersionStore>(
        [in_memory_storage] {
          // Read before the pin: every commit at or below it is below the fork.
          const auto durable_ts =
              in_memory_storage->repl_storage_state_.commit_ts_info_.load(std::memory_order_acquire).ldt_;
          const auto fork_ts = in_memory_storage->RegisterBranchForkPin();
          const auto counts = in_memory_storage->GetApproximateCounts();
          return versioning::ForkPoint{.fork_ts = fork_ts,
                                       .counts = {.vertices = counts.vertices, .edges = counts.edges},
                                       .durable_ts = durable_ts};
        },
        [in_memory_storage](uint64_t fork_ts) { in_memory_storage->ReleaseForkPin(fork_ts); });
    version_store_->SetRetentionFloorCallback(
        [in_memory_storage](std::optional<uint64_t> floor) { in_memory_storage->SetRetentionFloor(floor); });
    version_store_->AttachLedger(branch_ledger_file);

    if (branch_plan.ledger_unreadable) {
      version_store_->RestoreLedgerUnreadable(*branch_plan.ledger_unreadable);
    } else if (branch_plan.set_aside) {
      // The pin DropBranch releases; main has no replayed history to pin, so it is taken now.
      for (auto &branch : branch_plan.ledger.branches) branch.info.fork_ts = in_memory_storage->RegisterForkPin();
      version_store_->Restore(branch_plan.ledger);
      std::unordered_map<uint64_t, std::string> aside;
      for (const auto &branch : branch_plan.ledger.branches) aside.emplace(branch.info.number, *branch_plan.set_aside);
      versioning::RecoverBranches(*version_store_, *in_memory_storage, aside);
    } else if (branch_plan.bound) {
      version_store_->Restore(branch_plan.ledger);
      spdlog::info("Graph Versioning: restored branches [{}].", BranchNames(branch_plan.ledger));
      versioning::RecoverBranches(*version_store_, *in_memory_storage, history_shortfalls);
    } else if (branch_plan.hidden) {
      versioning::KeepMainClearOfHiddenBranches(branch_plan.ledger, *in_memory_storage);
      version_store_->RestoreHidden(branch_plan.ledger);
    } else {
      version_store_->Restore(branch_plan.ledger);
    }
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
