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

#include "versioning/version_store.hpp"

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <shared_mutex>
#include <system_error>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <spdlog/spdlog.h>

#include "metrics/prometheus_metrics.hpp"
#include "storage/v2/durability/retention_floor.hpp"
#include "utils/logging.hpp"

namespace memgraph::versioning {

namespace {
std::string DescribeUnavailableBranch(std::string_view name, std::string_view reason,
                                      const std::filesystem::path &directory) {
  while (reason.ends_with('.')) reason.remove_suffix(1);
  return fmt::format(
      "Branch '{0}' is unavailable: {1}. Its files were left as they are in '{2}'. To bring it back, restore them from "
      "a backup and restart Memgraph; to remove the branch and its files, run DROP BRANCH '{0}'.",
      name,
      reason,
      directory.string());
}

// Clears what an earlier branch of this id left (a failed create, a lost ledger) so a new branch never replays its WAL,
// and creates the WAL directory: a recovery finding it missing knows the branch's files are gone.
std::expected<void, std::string> ResetBranchDirectory(const std::filesystem::path &directory) {
  std::error_code ec;
  std::filesystem::remove_all(directory, ec);
  if (ec) return std::unexpected(fmt::format("Cannot clear '{}': {}", directory.string(), ec.message()));
  std::filesystem::create_directories(directory / "wal", ec);
  if (ec) return std::unexpected(fmt::format("Cannot create '{}': {}", (directory / "wal").string(), ec.message()));
  return {};
}
}  // namespace

Branch::Branch(std::string name, BranchInfo info, uint64_t fork_ts, std::filesystem::path directory,
               std::function<void(uint64_t)> release_pin)
    : name_{std::move(name)},
      info_{info},
      fork_ts_{fork_ts},
      directory_{std::move(directory)},
      release_pin_{std::move(release_pin)} {}

Branch::~Branch() {
  try {
    core_.reset();
    release_pin_(fork_ts_);
    if (dropped() && !directory_.empty()) {
      std::error_code ec;
      std::filesystem::remove_all(directory_, ec);
      if (ec) {
        spdlog::warn("Graph Versioning: cannot remove branch directory {}: {}", directory_.string(), ec.message());
      }
    }
  } catch (const std::exception &e) {
    spdlog::error("Graph Versioning: releasing branch '{}' failed: {}", name_, e.what());
  } catch (...) {
    spdlog::error("Graph Versioning: releasing branch '{}' failed", name_);
  }
}

CoreResult Branch::GetOrBuildCore(const CoreBuilder &build) {
  const std::scoped_lock guard{build_mutex_};
  if (!core_) {
    auto built = build();
    if (!built) return built;
    core_ = std::move(*built);
  }
  return core_;
}

VersionStore::VersionStore(std::function<ForkPoint()> acquire_pin, std::function<void(uint64_t)> release_pin,
                           std::function<void(uint64_t)> set_retention_floor, BranchLedger ledger,
                           std::filesystem::path branches_root)
    : acquire_pin_{std::move(acquire_pin)},
      release_pin_{std::move(release_pin)},
      set_retention_floor_{std::move(set_retention_floor)},
      ledger_{std::move(ledger)},
      branches_root_{std::move(branches_root)} {
  DMG_ASSERT(acquire_pin_ && release_pin_ && set_retention_floor_, "VersionStore callbacks are required");
  DMG_ASSERT(!branches_root_.empty(), "VersionStore needs a branches directory");
}

void BranchCheckout::reset() {
  if (branch_ != nullptr) store_->Release(std::exchange(branch_, nullptr));
  store_ = nullptr;
}

VersionStore::~VersionStore() {
  DMG_ASSERT(dropped_.empty() &&
                 std::ranges::all_of(
                     branches_, [](const auto &e) { return e.second.sessions_.load(std::memory_order_relaxed) == 0; }),
             "VersionStore destroyed with live checkouts");
  memgraph::metrics::Metrics().global.versioning_active_branches->Decrement(static_cast<double>(branches_.size()));
}

void VersionStore::PublishRetentionFloorLocked() const {
  uint64_t floor = storage::durability::kNoRetentionFloor;
  for (const auto &[_, branch] : branches_) floor = std::min(floor, RecoverableBelow(branch.info()));
  set_retention_floor_(floor);
}

LedgerContents VersionStore::CreateSnapshot_() const {
  LedgerContents out;
  out.next_branch_id = next_branch_id_;
  out.branches.reserve(branches_.size());
  for (const auto &[name, branch] : branches_) out.branches.push_back({.name = name, .info = branch.info()});
  std::ranges::sort(out.branches, std::less{}, [](const auto &b) { return b.info.id; });
  return out;
}

std::expected<void, std::string> VersionStore::PersistLedger_() {
  try {
    if (auto res = ledger_.Write(CreateSnapshot_()); !res) {
      spdlog::error("Graph Versioning: {}", res.error());
      return std::unexpected(fmt::format("Cannot persist the branch registry: {}", res.error()));
    }
  } catch (const std::exception &e) {
    spdlog::error("Graph Versioning: ledger write threw: {}", e.what());
    return std::unexpected(fmt::format("Cannot persist the branch registry: {}", e.what()));
  }
  return {};
}

void VersionStore::Restore(uint64_t next_branch_id, std::span<const RestoredBranch> branches) {
  const std::unique_lock guard{mutex_};
  next_branch_id_ = next_branch_id;
  for (const auto &[ledger_branch, runtime_pin] : branches) {
    const auto &[name, info] = ledger_branch;
    branches_.try_emplace(name, name, info, runtime_pin, branches_root_ / std::to_string(info.id), release_pin_);
  }
  memgraph::metrics::Metrics().global.versioning_active_branches->Increment(static_cast<double>(branches.size()));
  PublishRetentionFloorLocked();
}

void VersionStore::MarkUnavailable(uint64_t id, std::string reason) {
  const std::unique_lock guard{mutex_};
  const auto it = std::ranges::find(branches_, id, [](const auto &entry) { return entry.second.info().id; });
  if (it != branches_.end()) it->second.SetUnavailable(std::move(reason));
}

std::expected<BranchInfo, std::string> VersionStore::CreateBranch(std::string name) {
  // Declared before the lock so a failed create's ~Branch (which releases the pin) runs after unlock.
  BranchNode failed;
  const std::unique_lock guard{mutex_};
  if (branches_.contains(name)) return std::unexpected(fmt::format("Branch '{}' already exists.", name));

  if (auto prepared = ResetBranchDirectory(branches_root_ / std::to_string(next_branch_id_)); !prepared) {
    return std::unexpected(std::move(prepared.error()));
  }

  const auto fork = acquire_pin_();
  const auto id = next_branch_id_++;
  const auto created_at_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count());
  const BranchInfo info{.id = id, .fork = fork, .created_at_us = created_at_us};

  BranchMap::iterator it;
  try {
    it =
        branches_.try_emplace(name, name, info, fork.durable_fork_ts, branches_root_ / std::to_string(id), release_pin_)
            .first;
  } catch (...) {
    --next_branch_id_;
    // No Branch owns the pin yet. Mirror ~Branch: a throw here must not replace the one in flight.
    try {
      release_pin_(fork.durable_fork_ts);
      PublishRetentionFloorLocked();
    } catch (...) {
      spdlog::error("Graph Versioning: releasing the pin of branch '{}' failed", name);
    }
    throw;
  }
  if (auto written = PersistLedger_(); !written) {
    failed = branches_.extract(it);
    --next_branch_id_;
    PublishRetentionFloorLocked();
    return std::unexpected(std::move(written.error()));
  }

  auto &global = memgraph::metrics::Metrics().global;
  global.versioning_branches_created->Increment();
  global.versioning_active_branches->Increment();
  return info;
}

std::expected<DropResult, std::string> VersionStore::DropBranch(std::string_view name, bool force) {
  // Declared before the lock: unless parked, destroyed after unlock, removing the directory.
  BranchNode doomed;
  const std::unique_lock guard{mutex_};
  const auto it = branches_.find(name);
  if (it == branches_.end()) return DropResult::kNotFound;
  const bool parked = it->second.sessions_.load(std::memory_order_relaxed) > 0;
  if (parked && !force) return DropResult::kInUse;
  // Reserved before the ledger write: an OOM afterwards must not destroy a branch that has live sessions.
  if (parked) dropped_.reserve(dropped_.size() + 1);

  doomed = branches_.extract(it);
  if (auto written = PersistLedger_(); !written) {
    branches_.insert(std::move(doomed));
    return std::unexpected(std::move(written.error()));
  }

  doomed.mapped().dropped_.store(true, std::memory_order_release);
  if (parked) dropped_.push_back(std::move(doomed));
  auto &global = memgraph::metrics::Metrics().global;
  global.versioning_branches_dropped->Increment();
  global.versioning_active_branches->Decrement();
  PublishRetentionFloorLocked();
  return DropResult::kDropped;
}

VersionStore::Listing VersionStore::DescribeLocked(const std::string &name, const Branch &branch) {
  return {.name = name,
          .info = branch.info(),
          .fork_ts = branch.fork_ts(),
          .unavailable_reason = branch.unavailable_reason(),
          .sessions = branch.sessions_.load(std::memory_order_relaxed)};
}

std::optional<VersionStore::Listing> VersionStore::Get(std::string_view name) const {
  const std::shared_lock guard{mutex_};
  const auto it = branches_.find(name);
  if (it == branches_.end()) return std::nullopt;
  return DescribeLocked(it->first, it->second);
}

std::vector<VersionStore::Listing> VersionStore::List() const {
  std::vector<Listing> result;
  {
    const std::shared_lock guard{mutex_};
    result.reserve(branches_.size());
    for (const auto &[name, branch] : branches_) result.push_back(DescribeLocked(name, branch));
  }
  std::ranges::sort(result, std::less{}, [](const auto &l) { return l.info.id; });
  return result;
}

bool VersionStore::Exists(std::string_view name) const {
  const std::shared_lock guard{mutex_};
  return branches_.contains(name);
}

bool VersionStore::Empty() const {
  const std::shared_lock guard{mutex_};
  return branches_.empty();
}

std::expected<BranchCheckout, std::string> VersionStore::Checkout(std::string_view name) {
  const std::shared_lock guard{mutex_};
  const auto it = branches_.find(name);
  if (it == branches_.end()) return std::unexpected(fmt::format("Branch '{}' does not exist.", name));
  Branch &branch = it->second;
  if (const auto &reason = branch.unavailable_reason()) {
    return std::unexpected(DescribeUnavailableBranch(name, *reason, branches_root_ / std::to_string(branch.info().id)));
  }
  // Relaxed: the shared lock orders this against DropBranch's exclusive-lock check of the count.
  branch.sessions_.fetch_add(1, std::memory_order_relaxed);
  memgraph::metrics::Metrics().global.versioning_active_sessions->Increment();
  return BranchCheckout{this, &branch};
}

void VersionStore::Release(Branch *branch) {
  // Destroyed after the lock is released: removes the directory of a dropped branch.
  BranchNode doomed;
  {
    std::shared_lock shared{mutex_};
    // acq_rel: the thread seeing 1 must observe every other releaser's use of the branch before destroying it.
    const auto previous = branch->sessions_.fetch_sub(1, std::memory_order_acq_rel);
    memgraph::metrics::Metrics().global.versioning_active_sessions->Decrement();
    // dropped_ was set under DropBranch's exclusive lock, which this shared lock was acquired after, so it is visible.
    if (previous != 1 || !branch->dropped()) return;
    // A dropped branch is outside branches_: nobody can check it out again, and no other release is pending.
    shared.unlock();
    const std::unique_lock exclusive{mutex_};
    const auto it = std::ranges::find_if(dropped_, [&](auto &node) { return &node.mapped() == branch; });
    doomed = std::move(*it);
    dropped_.erase(it);
  }
}

bool DropBranchesOnReplica(VersionStore &store, std::string_view database, std::string_view occasion) {
  std::vector<std::string> dropped;
  bool ok = true;
  for (const auto &listing : store.List()) {
    const auto result = store.DropBranch(listing.name, true);
    if (!result) {
      spdlog::error(
          "Failed to drop branch '{}' of database {} {}: {}", listing.name, database, occasion, result.error());
      ok = false;
    } else if (*result == DropResult::kDropped) {
      dropped.push_back(listing.name);
    }
  }
  if (!dropped.empty()) {
    spdlog::warn("Graph Versioning: dropped branches [{}] of database {} {}; a REPLICA has no branches.",
                 fmt::join(dropped, ", "),
                 database,
                 occasion);
  }
  return ok;
}

}  // namespace memgraph::versioning
