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
#include <iterator>
#include <mutex>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <spdlog/spdlog.h>

#include "metrics/prometheus_metrics.hpp"
#include "utils/logging.hpp"

namespace memgraph::versioning {

namespace {
constexpr std::string_view kFrozenMessage = "Graph Versioning branches are not available on a REPLICA.";

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
}  // namespace

std::string DescribeCoreFailure(std::string_view branch, const std::string &error) {
  if (error == kBranchDroppedMessage) return fmt::format("Branch '{}' was dropped.", branch);
  return error;
}

VersionStore::VersionStore(std::function<ForkPoint()> acquire_pin, std::function<void(uint64_t)> release_pin)
    : acquire_pin_{std::move(acquire_pin)}, release_pin_{std::move(release_pin)} {}

VersionStore::~VersionStore() {
  size_t total_checkouts = 0;
  for (const auto &[number, count] : checked_out_) total_checkouts += count;
  auto &global = memgraph::metrics::Metrics().global;
  global.versioning_active_branches->Decrement(static_cast<double>(branches_.size()));
  global.versioning_active_sessions->Decrement(static_cast<double>(total_checkouts));
}

void VersionStore::SetRetentionFloorCallback(std::function<void(std::optional<uint64_t>)> callback) {
  retention_floor_callback_ = std::move(callback);
}

void VersionStore::PublishRetentionFloor() const {
  if (!retention_floor_callback_) return;
  std::optional<uint64_t> floor;
  {
    const std::scoped_lock guard{lock_};
    const auto note = [&](const BranchInfo &info) {
      floor = floor ? std::min(*floor, RecoverableBelow(info)) : RecoverableBelow(info);
    };
    for (const auto &[_, info] : branches_) note(info);
    for (const auto &branch : hidden_) note(branch.info);
  }
  retention_floor_callback_(floor);
}

void VersionStore::AttachLedger(std::filesystem::path file) {
  const std::scoped_lock guard{ledger_mutex_};
  auto root = file.parent_path();
  ledger_.emplace(std::move(file));
  // TryAcquireCheckout reads the root under lock_ alone.
  const std::scoped_lock registry{lock_};
  branches_root_ = std::move(root);
}

LedgerContents VersionStore::SnapshotLocked() const {
  LedgerContents out;
  out.next_number = next_number_;
  out.branches.reserve(branches_.size() + hidden_.size());
  for (const auto &[name, info] : branches_) out.branches.emplace_back(name, info);
  out.branches.insert(out.branches.end(), hidden_.begin(), hidden_.end());
  std::ranges::sort(out.branches, std::less{}, [](const auto &b) { return b.info.number; });
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

void VersionStore::Restore(const LedgerContents &contents) {
  {
    const std::scoped_lock guard{lock_};
    next_number_ = contents.next_number;
    for (const auto &[name, info] : contents.branches) branches_.emplace(name, info);
    memgraph::metrics::Metrics().global.versioning_active_branches->Increment(
        static_cast<double>(contents.branches.size()));
  }
  PublishRetentionFloor();
}

void VersionStore::RestoreHidden(const LedgerContents &contents) {
  {
    const std::scoped_lock guard{lock_};
    next_number_ = contents.next_number;
    hidden_ = contents.branches;
  }
  PublishRetentionFloor();
}

void VersionStore::MarkUnavailable(uint64_t number, std::string reason) {
  const std::scoped_lock guard{lock_};
  const auto branch = std::ranges::find(branches_, number, [](const auto &entry) { return entry.second.number; });
  if (branch != branches_.end()) branch->second.unavailable_reason = std::move(reason);
}

void VersionStore::RestoreLedgerUnreadable(std::string message) {
  const std::scoped_lock guard{lock_};
  ledger_unreadable_ = std::move(message);
}

std::optional<std::string> VersionStore::LedgerUnreadable() const {
  const std::scoped_lock guard{lock_};
  return ledger_unreadable_;
}

std::expected<BranchInfo, std::string> VersionStore::CreateBranch(std::string name) {
  const std::scoped_lock ledger_guard{ledger_mutex_};

  LedgerContents next;
  {
    const std::scoped_lock guard{lock_};
    if (ledger_unreadable_) return std::unexpected(*ledger_unreadable_);
    if (frozen_) return std::unexpected(std::string{kFrozenMessage});
    if (branches_.contains(name) || std::ranges::any_of(hidden_, [&](const auto &b) { return b.name == name; })) {
      return std::unexpected(fmt::format("Branch '{}' already exists.", name));
    }
    next = SnapshotLocked();
  }

  // ledger_mutex_ serializes every registry mutation, so the checks above still hold after the lock_ gap.
  const auto fork = acquire_pin_();
  BranchInfo info{
      .number = next.next_number,
      .parent = "main",
      .fork_ts = fork.fork_ts,
      .recorded_fork_ts = fork.fork_ts,
      .created_at_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
              .count()),
      .fork_counts = fork.counts,
      .fork_durable_ts = fork.durable_ts,
  };

  const auto release_fork = [&] {
    release_pin_(info.fork_ts);
    PublishRetentionFloor();
  };

  // The pin is unowned until the commit below; staging the node keeps that commit allocation-free.
  decltype(branches_) staged;
  try {
    ++next.next_number;
    next.branches.push_back({.name = name, .info = info});
    staged.emplace(std::move(name), info);
    if (auto written = WriteLedger(next); !written) {
      release_fork();
      return std::unexpected(std::move(written.error()));
    }
  } catch (...) {
    release_fork();
    throw;
  }

  {
    const std::scoped_lock guard{lock_};
    next_number_ = next.next_number;
    branches_.insert(staged.extract(staged.begin()));

    memgraph::metrics::Metrics().global.versioning_branches_created->Increment();
    memgraph::metrics::Metrics().global.versioning_active_branches->Increment();
  }
  return info;
}

std::expected<DropResult, std::string> VersionStore::DropBranch(std::string_view name,
                                                                std::optional<uint64_t> own_checkout) {
  return DropBranchImpl(name, /*force=*/false, own_checkout);
}

std::expected<DropResult, std::string> VersionStore::ForceDropBranch(std::string_view name) {
  return DropBranchImpl(name, /*force=*/true, std::nullopt);
}

std::expected<DropResult, std::string> VersionStore::DropBranchImpl(std::string_view name, bool force,
                                                                    std::optional<uint64_t> own_checkout) {
  std::unique_lock ledger_guard{ledger_mutex_};

  LedgerContents next;
  // A hidden branch has no session, pin or core: only its ledger entry and directory go.
  std::optional<uint64_t> hidden_number;
  {
    const std::scoped_lock guard{lock_};
    if (ledger_unreadable_) return std::unexpected(*ledger_unreadable_);
    auto it = branches_.find(name);
    if (it == branches_.end()) {
      const auto hidden = std::ranges::find(hidden_, name, &LedgerBranch::name);
      if (hidden == hidden_.end()) return DropResult::kNotFound;
      hidden_number = hidden->info.number;
    } else if (!force) {
      const auto number = it->second.number;
      const auto attached = checked_out_.find(number);
      const size_t own = own_checkout == number ? 1 : 0;
      if (attached != checked_out_.end() && attached->second > own) return DropResult::kInUse;
    }
    next = SnapshotLocked();
    std::erase_if(next.branches, [&](const LedgerBranch &b) { return b.name == name; });
    if (!hidden_number) dropping_.emplace(name);
  }

  if (auto written = WriteLedger(next); !written) {
    if (!hidden_number) {
      const std::scoped_lock guard{lock_};
      dropping_.erase(dropping_.find(name));
    }
    return std::unexpected(std::move(written.error()));
  }

  Retired retired{.branches_root = branches_root_};
  uint64_t fork_ts = 0;
  {
    const std::scoped_lock guard{lock_};
    if (hidden_number) {
      std::erase_if(hidden_, [&](const LedgerBranch &b) { return b.name == name; });
      retired.cores.emplace_back(*hidden_number, CoreSlot{});
      memgraph::metrics::Metrics().global.versioning_branches_dropped->Increment();
    } else {
      auto it = branches_.find(name);
      fork_ts = it->second.fork_ts;
      RetireCoreLocked(it->second.number, retired);
      branches_.erase(it);
      dropping_.erase(dropping_.find(name));

      memgraph::metrics::Metrics().global.versioning_branches_dropped->Increment();
      memgraph::metrics::Metrics().global.versioning_active_branches->Decrement();
    }
  }
  if (!hidden_number) release_pin_(fork_ts);
  PublishRetentionFloor();
  ledger_guard.unlock();
  Finish(std::move(retired));
  return DropResult::kDropped;
}

bool VersionStore::FreezeIfEmpty() {
  // lock_ alone is not enough: an in-flight CreateBranch checks frozen_, drops lock_, and inserts later.
  const std::unique_lock ledger_guard{ledger_mutex_};
  const std::scoped_lock guard{lock_};
  if (!branches_.empty() || !hidden_.empty()) return false;
  frozen_ = true;
  return true;
}

std::expected<std::vector<std::string>, std::string> VersionStore::FreezeAndDropAll() {
  std::unique_lock ledger_guard{ledger_mutex_};

  LedgerContents next;
  bool was_frozen = false;
  {
    const std::scoped_lock guard{lock_};
    was_frozen = frozen_;
    frozen_ = true;
    // No ledger write: a REPLICA that never had branches must not gain a branches directory.
    if (branches_.empty() && hidden_.empty()) return {};
    next = SnapshotLocked();
    next.branches.clear();
  }

  if (auto written = WriteLedger(next); !written) {
    const std::scoped_lock guard{lock_};
    frozen_ = was_frozen;
    return std::unexpected(std::move(written.error()));
  }

  // Cores are retired with or without a ledger: a cached core of a dropped number would otherwise outlive the drop.
  Retired retired{.branches_root = branches_root_};
  std::vector<uint64_t> pins;
  std::vector<std::string> dropped_names;
  {
    const std::scoped_lock guard{lock_};
    pins.reserve(branches_.size());
    dropped_names.reserve(branches_.size() + hidden_.size());
    for (auto &branch : hidden_) {
      retired.cores.emplace_back(branch.info.number, CoreSlot{});
      dropped_names.push_back(std::move(branch.name));
    }
    hidden_.clear();
    for (const auto &[branch_name, info] : branches_) {
      pins.push_back(info.fork_ts);
      RetireCoreLocked(info.number, retired);
      dropped_names.push_back(branch_name);
    }
    const auto dropped = static_cast<double>(branches_.size());
    branches_.clear();

    auto &global = memgraph::metrics::Metrics().global;
    global.versioning_branches_dropped->Increment(dropped);
    global.versioning_active_branches->Decrement(dropped);
  }
  for (const auto fork_ts : pins) release_pin_(fork_ts);
  PublishRetentionFloor();
  ledger_guard.unlock();
  Finish(std::move(retired));
  return dropped_names;
}

void VersionStore::Unfreeze() {
  const std::scoped_lock guard{lock_};
  frozen_ = false;
}

bool VersionStore::Frozen() const {
  const std::scoped_lock guard{lock_};
  return frozen_;
}

std::expected<void, std::string> VersionStore::RecordGidFloors(uint64_t number, uint64_t vertex_gid_floor,
                                                               uint64_t edge_gid_floor) {
  const auto already_settled = [&] {
    const std::scoped_lock guard{lock_};
    auto it = std::ranges::find(branches_, number, [](const auto &kv) { return kv.second.number; });
    return it == branches_.end() || it->second.vertex_gid_floor.has_value();
  };
  // Fast path: skip ledger_mutex_, which CreateBranch holds across the slow ledger write.
  if (already_settled()) return {};

  const std::scoped_lock ledger_guard{ledger_mutex_};

  LedgerContents next;
  std::string name;
  {
    const std::scoped_lock guard{lock_};
    auto it = std::ranges::find(branches_, number, [](const auto &kv) { return kv.second.number; });
    if (it == branches_.end() || it->second.vertex_gid_floor.has_value()) return {};
    name = it->first;
    next = SnapshotLocked();
  }
  for (auto &b : next.branches) {
    if (b.name == name) {
      b.info.vertex_gid_floor = vertex_gid_floor;
      b.info.edge_gid_floor = edge_gid_floor;
    }
  }
  if (auto written = WriteLedger(next); !written) return std::unexpected(std::move(written.error()));

  const std::scoped_lock guard{lock_};
  if (auto it = branches_.find(name); it != branches_.end()) {
    it->second.vertex_gid_floor = vertex_gid_floor;
    it->second.edge_gid_floor = edge_gid_floor;
  }
  return {};
}

std::expected<uint64_t, std::string> VersionStore::ReserveGids(uint64_t number, GidKind kind, uint64_t through) {
  // Under ledger_mutex_ like every ledger write: a racing reserver waits for the write and then finds it covers it.
  const std::scoped_lock ledger_guard{ledger_mutex_};

  LedgerContents next;
  std::string name;
  {
    const std::scoped_lock guard{lock_};
    auto it = std::ranges::find(branches_, number, [](const auto &kv) { return kv.second.number; });
    if (it == branches_.end()) return through;
    if (const auto held = GidsHeldBelow(it->second, kind); through <= held) return held;
    name = it->first;
    next = SnapshotLocked();
  }
  const uint64_t ceiling = through + kGidReserveChunk;
  for (auto &b : next.branches) {
    if (b.name == name) (kind == GidKind::kVertex ? b.info.vertex_gid_ceiling : b.info.edge_gid_ceiling) = ceiling;
  }
  if (auto written = WriteLedger(next); !written) return std::unexpected(std::move(written.error()));

  const std::scoped_lock guard{lock_};
  if (auto it = branches_.find(name); it != branches_.end()) {
    (kind == GidKind::kVertex ? it->second.vertex_gid_ceiling : it->second.edge_gid_ceiling) = ceiling;
  }
  return ceiling;
}

GidReserver VersionStore::GidReserverOf(uint64_t number) {
  return [this, number](GidKind kind, uint64_t through) { return ReserveGids(number, kind, through); };
}

std::optional<BranchInfo> VersionStore::Get(std::string_view name) const {
  const std::scoped_lock guard{lock_};
  auto it = branches_.find(name);
  if (it == branches_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::vector<std::pair<std::string, BranchInfo>> VersionStore::List() const {
  std::vector<std::pair<std::string, BranchInfo>> result;
  {
    const std::scoped_lock guard{lock_};
    result.assign(branches_.begin(), branches_.end());
  }
  std::ranges::sort(result, std::less{}, [](const auto &kv) { return kv.second.number; });
  return result;
}

std::vector<std::pair<std::string, BranchInfo>> VersionStore::ListHidden() const {
  std::vector<std::pair<std::string, BranchInfo>> result;
  {
    const std::scoped_lock guard{lock_};
    for (const auto &branch : hidden_) result.emplace_back(branch.name, branch.info);
  }
  std::ranges::sort(result, std::less{}, [](const auto &kv) { return kv.second.number; });
  return result;
}

bool VersionStore::Exists(std::string_view name) const {
  const std::scoped_lock guard{lock_};
  return branches_.contains(name);
}

bool VersionStore::IsLive(std::string_view name, uint64_t number) const {
  const std::scoped_lock guard{lock_};
  auto it = branches_.find(name);
  return it != branches_.end() && it->second.number == number;
}

bool VersionStore::IsLiveNumberLocked(uint64_t number) const {
  return std::ranges::any_of(branches_, [&](const auto &entry) { return entry.second.number == number; });
}

bool VersionStore::Empty() const {
  const std::scoped_lock guard{lock_};
  return branches_.empty() && hidden_.empty() && !ledger_unreadable_;
}

bool VersionStore::HasHidden() const {
  const std::scoped_lock guard{lock_};
  return !hidden_.empty();
}

std::expected<BranchInfo, std::string> VersionStore::TryAcquireCheckout(std::string_view name) {
  const std::scoped_lock guard{lock_};
  if (ledger_unreadable_) return std::unexpected(*ledger_unreadable_);
  if (frozen_) return std::unexpected(std::string{kFrozenMessage});
  auto it = branches_.find(name);
  if (it == branches_.end() || dropping_.contains(name)) {
    return std::unexpected(fmt::format("Branch '{}' does not exist.", name));
  }
  if (it->second.unavailable_reason) {
    return std::unexpected(DescribeUnavailableBranch(
        name, *it->second.unavailable_reason, branches_root_ / std::to_string(it->second.number)));
  }
  ++checked_out_[it->second.number];

  memgraph::metrics::Metrics().global.versioning_active_sessions->Increment();
  return it->second;
}

void VersionStore::ReleaseCheckout(uint64_t number) {
  std::vector<std::filesystem::path> removable;
  {
    const std::scoped_lock guard{lock_};
    if (auto it = checked_out_.find(number); it != checked_out_.end()) {
      if (--it->second == 0) checked_out_.erase(it);
      memgraph::metrics::Metrics().global.versioning_active_sessions->Decrement();
    }
    removable = TakeRemovableLocked();
  }
  for (const auto &directory : removable) RemoveDirectory(directory);
}

size_t VersionStore::CheckoutCount(std::string_view name) const {
  const std::scoped_lock guard{lock_};
  const auto branch = branches_.find(name);
  if (branch == branches_.end()) return 0;
  const auto it = checked_out_.find(branch->second.number);
  return it == checked_out_.end() ? 0 : it->second;
}

VersionStore::CoreResult VersionStore::GetOrBuildCore(uint64_t number, const CoreBuilder &build) {
  CoreSlot slot;
  std::optional<std::promise<CoreResult>> building;
  {
    const std::scoped_lock guard{lock_};
    if (!IsLiveNumberLocked(number)) return std::unexpected(std::string{kBranchDroppedMessage});
    auto [it, inserted] = cores_.try_emplace(number);
    if (inserted) it->second = building.emplace().get_future().share();
    slot = it->second;
  }
  if (building) {
    // A failed build leaves no slot, so the next checkout retries; a drop may have taken the slot already.
    const auto forget = [&] {
      CoreSlot failed;
      const std::scoped_lock guard{lock_};
      if (auto it = cores_.find(number); it != cores_.end()) {
        failed = std::move(it->second);
        cores_.erase(it);
      }
    };
    try {
      auto built = build();
      if (!built) forget();
      building->set_value(std::move(built));
    } catch (...) {
      forget();
      building->set_exception(std::current_exception());
    }
  }
  const auto &result = slot.get();
  if (!result) return std::unexpected(result.error());
  // A drop that retired the slot before this caller got here must not hand out the core of a dropped number.
  {
    const std::scoped_lock guard{lock_};
    if (!IsLiveNumberLocked(number)) return std::unexpected(std::string{kBranchDroppedMessage});
  }
  return *result;
}

void VersionStore::RetireCoreLocked(uint64_t number, Retired &retired) {
  CoreSlot slot;
  if (auto it = cores_.find(number); it != cores_.end()) {
    slot = std::move(it->second);
    cores_.erase(it);
  }
  retired.cores.emplace_back(number, std::move(slot));
}

std::vector<std::filesystem::path> VersionStore::TakeRemovableLocked() {
  std::vector<std::filesystem::path> removable;
  // A holder's checkout stays counted while its core is destroyed, so a removable entry has no holder mid-teardown.
  for (auto it = pending_removals_.begin(); it != pending_removals_.end();) {
    if (it->core.expired() && !checked_out_.contains(it->number)) {
      removable.push_back(std::move(it->directory));
      it = pending_removals_.erase(it);
    } else {
      ++it;
    }
  }
  return removable;
}

void VersionStore::Finish(Retired retired) {
  std::vector<std::pair<uint64_t, std::weak_ptr<BranchCore>>> remaining;
  remaining.reserve(retired.cores.size());
  for (auto &[number, slot] : retired.cores) {
    std::weak_ptr<BranchCore> core;
    if (slot.valid() && slot.wait_for(std::chrono::seconds{0}) == std::future_status::ready && slot.get()) {
      core = *slot.get();
    }
    slot = {};
    remaining.emplace_back(number, std::move(core));
  }
  if (retired.branches_root.empty()) return;

  std::vector<std::filesystem::path> deferred;
  std::vector<std::filesystem::path> removable;
  {
    const std::scoped_lock guard{lock_};
    for (auto &[number, core] : remaining) {
      auto directory = retired.branches_root / std::to_string(number);
      if (core.expired() && !checked_out_.contains(number)) {
        removable.push_back(std::move(directory));
      } else {
        deferred.push_back(directory);
        pending_removals_.push_back({.core = std::move(core), .number = number, .directory = std::move(directory)});
      }
    }
    std::ranges::move(TakeRemovableLocked(), std::back_inserter(removable));
  }
  for (const auto &directory : deferred) {
    spdlog::info("Graph Versioning: branch directory {} will be removed once its engine is released.",
                 directory.string());
  }
  for (const auto &directory : removable) RemoveDirectory(directory);
}

void VersionStore::RemoveDirectory(const std::filesystem::path &directory) {
  std::error_code ec;
  std::filesystem::remove_all(directory, ec);
  if (ec) {
    spdlog::warn("Graph Versioning: cannot remove branch directory {}: {}", directory.string(), ec.message());
  }
}

bool DropBranchesOnReplica(VersionStore &store, std::string_view database, std::string_view occasion) {
  auto const dropped = store.FreezeAndDropAll();
  if (!dropped) {
    spdlog::error("Failed to drop the branches of database {} {}: {}", database, occasion, dropped.error());
    return false;
  }
  if (!dropped->empty()) {
    spdlog::warn("Graph Versioning: dropped branches [{}] of database {} {}; not supported with replication in v1.",
                 fmt::join(*dropped, ", "),
                 database,
                 occasion);
  }
  return true;
}

}  // namespace memgraph::versioning
