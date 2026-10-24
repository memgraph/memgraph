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

#include <fmt/format.h>
#include <spdlog/spdlog.h>

#include "metrics/prometheus_metrics.hpp"
#include "versioning/branch_engine.hpp"

namespace memgraph::versioning {

VersionStore::VersionStore(std::function<uint64_t()> acquire_pin, std::function<void(uint64_t)> release_pin)
    : acquire_pin_{std::move(acquire_pin)}, release_pin_{std::move(release_pin)} {}

std::expected<BranchInfo, std::string> VersionStore::CreateBranch(std::string name, std::string parent,
                                                                  std::optional<std::string> description) {
  std::lock_guard guard{lock_};

  if (branches_.contains(name)) {
    return std::unexpected(fmt::format("Branch '{}' already exists.", name));
  }
  if (parent != "main" && !branches_.contains(parent)) {
    return std::unexpected(fmt::format("Parent branch '{}' does not exist.", parent));
  }

  const BranchInfo info{
      .number = next_number_,
      .parent = std::move(parent),
      .fork_ts = acquire_pin_(),
      .description = std::move(description),
  };

  ++next_number_;
  branches_.emplace(std::move(name), info);

  memgraph::metrics::Metrics().global.versioning_branches_created->Increment();
  memgraph::metrics::Metrics().global.versioning_active_branches->Set(static_cast<double>(branches_.size()));
  return info;
}

bool VersionStore::DropBranch(std::string_view name) {
  uint64_t number = 0;
  {
    std::lock_guard guard{lock_};

    auto it = branches_.find(name);
    if (it == branches_.end()) {
      return false;
    }
    if (checked_out_.contains(name)) {
      return false;
    }

    number = it->second.number;
    release_pin_(it->second.fork_ts);
    branches_.erase(it);

    memgraph::metrics::Metrics().global.versioning_branches_dropped->Increment();
    memgraph::metrics::Metrics().global.versioning_active_branches->Set(static_cast<double>(branches_.size()));
  }
  RetireCore(number);
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

std::expected<BranchInfo, std::string> VersionStore::TryAcquireCheckout(std::string_view name) {
  std::lock_guard guard{lock_};
  auto it = branches_.find(name);
  if (it == branches_.end()) {
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
