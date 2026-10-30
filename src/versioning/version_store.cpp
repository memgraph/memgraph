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
#include <mutex>

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "metrics/prometheus_metrics.hpp"
#include "utils/logging.hpp"

namespace memgraph::versioning {

VersionStore::VersionStore(std::function<uint64_t()> acquire_pin, std::function<void(uint64_t)> release_pin)
    : acquire_pin_{std::move(acquire_pin)}, release_pin_{std::move(release_pin)} {}

VersionStore::~VersionStore() {
  memgraph::metrics::Metrics().global.versioning_active_branches->Decrement(static_cast<double>(branches_.size()));
}

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
  retention_floor_callback_(RecoverableBelow(*oldest_fork_ts));
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
    memgraph::metrics::Metrics().global.versioning_active_branches->Increment(static_cast<double>(pins.size()));
  }
  PublishRetentionFloor();
}

std::expected<BranchInfo, std::string> VersionStore::CreateBranch(std::string name, std::string parent) {
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
      .created_at_us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
              .count()),
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
    memgraph::metrics::Metrics().global.versioning_active_branches->Increment();
  }
  return info;
}

std::expected<bool, std::string> VersionStore::DropBranch(std::string_view name) {
  std::lock_guard ledger_guard{ledger_mutex_};

  LedgerContents next;
  {
    std::lock_guard guard{lock_};
    if (!branches_.contains(name)) return false;
    next = SnapshotLocked();
    std::erase_if(next.branches, [&](const LedgerBranch &b) { return b.name == name; });
  }

  if (auto written = WriteLedger(next); !written) return std::unexpected(std::move(written.error()));

  {
    std::lock_guard guard{lock_};
    auto it = branches_.find(name);
    release_pin_(it->second.fork_ts);
    branches_.erase(it);

    memgraph::metrics::Metrics().global.versioning_branches_dropped->Increment();
    memgraph::metrics::Metrics().global.versioning_active_branches->Decrement();
  }
  PublishRetentionFloor();
  return true;
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

}  // namespace memgraph::versioning
