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
#include <mutex>

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "metrics/prometheus_metrics.hpp"
#include "utils/logging.hpp"
#include "versioning/branch_engine.hpp"

namespace memgraph::versioning {

VersionStore::VersionStore(std::function<uint64_t()> acquire_pin, std::function<void(uint64_t)> release_pin)
    : acquire_pin_{std::move(acquire_pin)}, release_pin_{std::move(release_pin)} {}

void VersionStore::SetRetentionFloorCallback(std::function<void(std::optional<uint64_t>)> callback) {
  retention_floor_callback_ = std::move(callback);
}

void VersionStore::PublishRetentionFloor() const {
  if (!retention_floor_callback_) return;
  std::optional<uint64_t> oldest_fork_ts;
  {
    std::lock_guard guard{lock_};
    for (const auto &[_, info] : branches_) {
      oldest_fork_ts = oldest_fork_ts ? std::min(*oldest_fork_ts, info.recorded_fork_ts) : info.recorded_fork_ts;
    }
  }
  if (!oldest_fork_ts) return retention_floor_callback_(std::nullopt);
  retention_floor_callback_(*oldest_fork_ts == 0 ? 0 : *oldest_fork_ts - 1);
}

void VersionStore::AttachLedger(std::filesystem::path file) {
  std::lock_guard guard{ledger_mutex_};
  ledger_.emplace(std::move(file));
}

LedgerContents VersionStore::SnapshotLocked() const {
  LedgerContents out;
  out.next_number = next_number_;
  out.branches.reserve(branches_.size());
  for (const auto &[name, info] : branches_) {
    auto &entry = out.branches.emplace_back(name, info);
    entry.info.fork_ts = info.recorded_fork_ts;
  }
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

void VersionStore::Restore(const LedgerContents &contents, std::span<const uint64_t> pins) {
  MG_ASSERT(pins.size() == contents.branches.size(), "Restore needs one fork pin per branch");
  {
    std::lock_guard guard{lock_};
    next_number_ = contents.next_number;
    for (size_t i = 0; i < pins.size(); ++i) {
      const auto &recorded = contents.branches[i];
      BranchInfo info = recorded.info;
      info.fork_ts = pins[i];
      branches_.emplace(recorded.name, std::move(info));
    }
    memgraph::metrics::Metrics().global.versioning_active_branches->Set(static_cast<double>(branches_.size()));
  }
  PublishRetentionFloor();
}

void VersionStore::ResetLedger(uint64_t next_number) {
  std::lock_guard ledger_guard{ledger_mutex_};
  {
    std::lock_guard guard{lock_};
    next_number_ = next_number;
  }
  (void)WriteLedger(LedgerContents{.next_number = next_number, .branches = {}});
}

std::expected<BranchInfo, std::string> VersionStore::CreateBranch(std::string name, std::string parent,
                                                                  std::optional<std::string> description) {
  std::lock_guard ledger_guard{ledger_mutex_};

  LedgerContents next;
  {
    std::lock_guard guard{lock_};
    if (branches_.contains(name)) {
      return std::unexpected(fmt::format("Branch '{}' already exists.", name));
    }
    if (parent != "main" && !branches_.contains(parent)) {
      return std::unexpected(fmt::format("Parent branch '{}' does not exist.", parent));
    }
    next = SnapshotLocked();
  }

  // ledger_mutex_ serializes every registry mutation, so the checks above still hold after the lock_ gap.
  const auto fork_ts = acquire_pin_();
  BranchInfo info{
      .number = next.next_number,
      .parent = std::move(parent),
      .fork_ts = fork_ts,
      .recorded_fork_ts = fork_ts,
      .description = std::move(description),
  };

  ++next.next_number;
  next.branches.push_back({name, info});
  if (auto written = WriteLedger(next); !written) {
    release_pin_(info.fork_ts);
    PublishRetentionFloor();
    return std::unexpected(std::move(written.error()));
  }

  {
    std::lock_guard guard{lock_};
    next_number_ = next.next_number;
    branches_.emplace(std::move(name), info);

    memgraph::metrics::Metrics().global.versioning_branches_created->Increment();
    memgraph::metrics::Metrics().global.versioning_active_branches->Set(static_cast<double>(branches_.size()));
  }
  return info;
}

std::expected<bool, std::string> VersionStore::DropBranch(std::string_view name) {
  std::lock_guard ledger_guard{ledger_mutex_};

  LedgerContents next;
  {
    std::lock_guard guard{lock_};
    auto it = branches_.find(name);
    if (it == branches_.end()) {
      return false;
    }
    if (checked_out_.contains(name)) {
      return false;
    }
    next = SnapshotLocked();
    std::erase_if(next.branches, [&](const LedgerBranch &b) { return b.name == name; });
    dropping_.emplace(name);
  }

  if (auto written = WriteLedger(next); !written) {
    std::lock_guard guard{lock_};
    dropping_.erase(dropping_.find(name));
    return std::unexpected(std::move(written.error()));
  }

  uint64_t number = 0;
  {
    std::lock_guard guard{lock_};
    auto it = branches_.find(name);
    number = it->second.number;
    release_pin_(it->second.fork_ts);
    branches_.erase(it);
    dropping_.erase(dropping_.find(name));

    memgraph::metrics::Metrics().global.versioning_branches_dropped->Increment();
    memgraph::metrics::Metrics().global.versioning_active_branches->Set(static_cast<double>(branches_.size()));
  }
  PublishRetentionFloor();
  RetireCore(number);
  return true;
}

std::expected<void, std::string> VersionStore::RecordGidFloors(uint64_t number, uint64_t vertex_gid_floor,
                                                               uint64_t edge_gid_floor) {
  std::lock_guard ledger_guard{ledger_mutex_};

  LedgerContents next;
  std::string name;
  {
    std::lock_guard guard{lock_};
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

  std::lock_guard guard{lock_};
  if (auto it = branches_.find(name); it != branches_.end()) {
    it->second.vertex_gid_floor = vertex_gid_floor;
    it->second.edge_gid_floor = edge_gid_floor;
  }
  return {};
}

std::optional<BranchInfo> VersionStore::Get(std::string_view name) const {
  std::lock_guard guard{lock_};
  auto it = branches_.find(name);
  if (it == branches_.end()) {
    return std::nullopt;
  }
  return it->second;
}

std::vector<std::pair<std::string, BranchInfo>> VersionStore::List() const {
  std::lock_guard guard{lock_};
  std::vector<std::pair<std::string, BranchInfo>> result;
  result.reserve(branches_.size());
  for (const auto &[name, info] : branches_) {
    result.emplace_back(name, info);
  }
  std::ranges::sort(result, std::less{}, [](const auto &kv) { return kv.second.number; });
  return result;
}

bool VersionStore::Exists(std::string_view name) const {
  std::lock_guard guard{lock_};
  return branches_.contains(name);
}

bool VersionStore::Empty() const {
  std::lock_guard guard{lock_};
  return branches_.empty();
}

std::expected<BranchInfo, std::string> VersionStore::TryAcquireCheckout(std::string_view name) {
  std::lock_guard guard{lock_};
  auto it = branches_.find(name);
  if (it == branches_.end() || dropping_.contains(name)) {
    return std::unexpected(fmt::format("Version '{}' does not exist.", name));
  }
  ++checked_out_[std::string{name}];
  ++total_checkouts_;

  memgraph::metrics::Metrics().global.versioning_active_checkouts->Set(static_cast<double>(total_checkouts_));
  return it->second;
}

void VersionStore::ReleaseCheckout(std::string_view name) {
  std::lock_guard guard{lock_};
  if (auto it = checked_out_.find(name); it != checked_out_.end()) {
    --total_checkouts_;
    if (--it->second == 0) checked_out_.erase(it);
  }

  memgraph::metrics::Metrics().global.versioning_active_checkouts->Set(static_cast<double>(total_checkouts_));
}

size_t VersionStore::CheckoutCount(std::string_view name) const {
  std::lock_guard guard{lock_};
  auto it = checked_out_.find(name);
  return it == checked_out_.end() ? 0 : it->second;
}

std::expected<std::shared_ptr<BranchCore>, std::string> VersionStore::GetOrBuildCore(uint64_t number,
                                                                                     const CoreBuilder &build) {
  {
    std::lock_guard guard{cores_mutex_};
    if (auto it = cores_.find(number); it != cores_.end()) return it->second;
  }
  std::lock_guard build_guard{build_mutex_};
  {
    std::lock_guard guard{cores_mutex_};
    if (auto it = cores_.find(number); it != cores_.end()) return it->second;
  }
  auto built = build();
  if (!built) return std::unexpected(std::move(built.error()));
  std::shared_ptr<BranchCore> winner;
  {
    std::lock_guard guard{cores_mutex_};
    winner = cores_.try_emplace(number, *built).first->second;
  }
  return winner;
}

void VersionStore::RetireCore(uint64_t number) {
  std::shared_ptr<BranchCore> retired;
  {
    std::lock_guard guard{cores_mutex_};
    if (auto it = cores_.find(number); it != cores_.end()) {
      retired = std::move(it->second);
      cores_.erase(it);
    }
  }
}

}  // namespace memgraph::versioning
