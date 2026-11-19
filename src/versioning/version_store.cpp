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
#include <system_error>

#include <fmt/format.h>
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

// Held by every checkout; its release ends the session.
struct SessionGuard {
  explicit SessionGuard(std::shared_ptr<Branch> branch) : branch{std::move(branch)} {
    memgraph::metrics::Metrics().global.versioning_active_sessions->Increment();
  }

  SessionGuard(const SessionGuard &) = delete;
  SessionGuard &operator=(const SessionGuard &) = delete;

  ~SessionGuard() { memgraph::metrics::Metrics().global.versioning_active_sessions->Decrement(); }

  std::shared_ptr<Branch> branch;
};
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
                           std::function<void(uint64_t)> set_retention_floor)
    : acquire_pin_{std::move(acquire_pin)},
      release_pin_{std::move(release_pin)},
      set_retention_floor_{std::move(set_retention_floor)} {}

VersionStore::~VersionStore() {
  memgraph::metrics::Metrics().global.versioning_active_branches->Decrement(static_cast<double>(branches_.size()));
}

void VersionStore::PublishRetentionFloorLocked() const {
  if (!set_retention_floor_) return;
  uint64_t floor = storage::durability::kNoRetentionFloor;
  for (const auto &[_, branch] : branches_) floor = std::min(floor, RecoverableBelow(branch->info()));
  set_retention_floor_(floor);
}

void VersionStore::AttachLedger(BranchLedger ledger, std::filesystem::path branches_root) {
  const std::scoped_lock guard{mutex_};
  ledger_.emplace(std::move(ledger));
  branches_root_ = std::move(branches_root);
}

LedgerContents VersionStore::SnapshotLocked() const {
  LedgerContents out;
  out.next_branch_id = next_branch_id_;
  out.branches.reserve(branches_.size() + 1);
  for (const auto &[name, branch] : branches_) out.branches.push_back({.name = name, .info = branch->info()});
  std::ranges::sort(out.branches, std::less{}, [](const auto &b) { return b.info.id; });
  return out;
}

std::expected<void, std::string> VersionStore::WriteLedger(const LedgerContents &contents) {
  if (!ledger_) return {};
  auto res = ledger_->Write(contents);
  if (!res) {
    spdlog::error("Graph Versioning: {}", res.error());
    return std::unexpected(fmt::format("Cannot persist the branch registry: {}", res.error()));
  }
  return {};
}

void VersionStore::Restore(const LedgerContents &contents, std::span<const uint64_t> fork_ts) {
  DMG_ASSERT(fork_ts.size() == contents.branches.size(), "Restore needs one fork pin per ledger branch");
  const std::scoped_lock guard{mutex_};
  next_branch_id_ = contents.next_branch_id;
  for (size_t i = 0; i < contents.branches.size(); ++i) {
    const auto &[name, info] = contents.branches[i];
    const auto directory = branches_root_.empty() ? std::filesystem::path{} : branches_root_ / std::to_string(info.id);
    branches_.emplace(name, std::make_shared<Branch>(name, info, fork_ts[i], directory, release_pin_));
  }
  memgraph::metrics::Metrics().global.versioning_active_branches->Increment(
      static_cast<double>(contents.branches.size()));
  PublishRetentionFloorLocked();
}

void VersionStore::MarkUnavailable(uint64_t id, std::string reason) {
  const std::scoped_lock guard{mutex_};
  const auto it = std::ranges::find(branches_, id, [](const auto &entry) { return entry.second->info().id; });
  if (it != branches_.end()) it->second->SetUnavailable(std::move(reason));
}

std::expected<BranchInfo, std::string> VersionStore::CreateBranch(std::string name) {
  // Destroyed after mutex_ is released; on failure its destructor releases the pin.
  std::shared_ptr<Branch> branch;
  const std::unique_lock guard{mutex_};
  if (branches_.contains(name)) return std::unexpected(fmt::format("Branch '{}' already exists.", name));

  const auto fork = acquire_pin_();
  const auto id = next_branch_id_;
  const auto directory = branches_root_.empty() ? std::filesystem::path{} : branches_root_ / std::to_string(id);
  const auto created_at_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count());
  branch = std::make_shared<Branch>(name,
                                    BranchInfo{.id = id, .fork = fork, .created_at_us = created_at_us},
                                    fork.durable_fork_ts,
                                    directory,
                                    release_pin_);

  auto next = SnapshotLocked();
  next.next_branch_id = id + 1;
  next.branches.push_back({.name = name, .info = branch->info()});
  if (auto written = WriteLedger(next); !written) {
    PublishRetentionFloorLocked();
    return std::unexpected(std::move(written.error()));
  }

  branches_.emplace(std::move(name), branch);
  next_branch_id_ = next.next_branch_id;
  auto &global = memgraph::metrics::Metrics().global;
  global.versioning_branches_created->Increment();
  global.versioning_active_branches->Increment();
  return branch->info();
}

std::expected<DropResult, std::string> VersionStore::DropBranch(std::string_view name, bool force) {
  // Destroyed after mutex_ is released: it may be the last holder, which removes the directory.
  std::shared_ptr<Branch> doomed;
  const std::unique_lock guard{mutex_};
  const auto it = branches_.find(name);
  if (it == branches_.end()) return DropResult::kNotFound;
  if (!force && it->second.use_count() > 1) return DropResult::kInUse;

  auto next = SnapshotLocked();
  std::erase_if(next.branches, [&](const LedgerBranch &b) { return b.name == name; });
  if (auto written = WriteLedger(next); !written) return std::unexpected(std::move(written.error()));

  doomed = std::move(it->second);
  doomed->dropped_.store(true, std::memory_order_release);
  branches_.erase(it);
  auto &global = memgraph::metrics::Metrics().global;
  global.versioning_branches_dropped->Increment();
  global.versioning_active_branches->Decrement();
  PublishRetentionFloorLocked();
  return DropResult::kDropped;
}

VersionStore::Listing VersionStore::DescribeLocked(const std::string &name, const std::shared_ptr<Branch> &branch) {
  return {.name = name,
          .info = branch->info(),
          .fork_ts = branch->fork_ts(),
          .unavailable_reason = branch->unavailable_reason(),
          .sessions = static_cast<size_t>(branch.use_count() - 1)};
}

std::optional<VersionStore::Listing> VersionStore::Get(std::string_view name) const {
  const std::scoped_lock guard{mutex_};
  const auto it = branches_.find(name);
  if (it == branches_.end()) return std::nullopt;
  return DescribeLocked(it->first, it->second);
}

std::vector<VersionStore::Listing> VersionStore::List() const {
  std::vector<Listing> result;
  {
    const std::scoped_lock guard{mutex_};
    result.reserve(branches_.size());
    for (const auto &[name, branch] : branches_) result.push_back(DescribeLocked(name, branch));
  }
  std::ranges::sort(result, std::less{}, [](const auto &l) { return l.info.id; });
  return result;
}

bool VersionStore::Exists(std::string_view name) const {
  const std::scoped_lock guard{mutex_};
  return branches_.contains(name);
}

bool VersionStore::Empty() const {
  const std::scoped_lock guard{mutex_};
  return branches_.empty();
}

std::expected<std::shared_ptr<Branch>, std::string> VersionStore::Checkout(std::string_view name) {
  const std::scoped_lock guard{mutex_};
  const auto it = branches_.find(name);
  if (it == branches_.end()) return std::unexpected(fmt::format("Branch '{}' does not exist.", name));
  const auto &branch = it->second;
  if (const auto &reason = branch->unavailable_reason()) {
    return std::unexpected(
        DescribeUnavailableBranch(name, *reason, branches_root_ / std::to_string(branch->info().id)));
  }
  auto session = std::make_shared<SessionGuard>(branch);
  return std::shared_ptr<Branch>{std::move(session), branch.get()};
}

}  // namespace memgraph::versioning
