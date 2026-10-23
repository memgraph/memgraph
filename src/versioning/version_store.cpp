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
#include <stdexcept>

#include <fmt/format.h>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

#include "metrics/prometheus_metrics.hpp"
#include "versioning/branch_engine.hpp"

namespace memgraph::versioning {

// kVersion / kNextNumberKey are declared in version_store.hpp -- shared with dbms::Database's
// restart pre-read (database.cpp), see the doc-comment there.

VersionStore::VersionStore(std::filesystem::path directory, std::function<uint64_t()> acquire_pin,
                           std::function<void(uint64_t)> release_pin)
    : storage_{std::move(directory)}, acquire_pin_{std::move(acquire_pin)}, release_pin_{std::move(release_pin)} {
  // Load persisted branches. Deliberately do NOT call acquire_pin_() here -- see the restart note
  // in version_store.hpp.
  uint64_t max_number = 1;
  for (const auto &[name, data] : storage_) {
    if (name == kNextNumberKey) {
      continue;  // the counter record, not a branch -- handled separately below
    }

    auto json_data = nlohmann::json::parse(data, /*cb=*/nullptr, /*allow_exceptions=*/false);
    if (json_data.is_discarded() || !json_data.is_object()) {
      spdlog::warn("VersionStore: failed to load branch '{}', corrupt record.", name);
      continue;
    }
    if (!json_data["version"].is_number_unsigned()) {
      spdlog::warn("VersionStore: failed to load branch '{}', missing version field.", name);
      continue;
    }
    if (const auto version = json_data["version"].get<uint64_t>(); version != kVersion) {
      // Single dispatch point for a future format migration; no prior versions exist yet, so
      // there is nothing to migrate from -- just refuse to load an unrecognized record.
      spdlog::warn("VersionStore: failed to load branch '{}', unsupported record version {} (expected {}).",
                   name,
                   version,
                   kVersion);
      continue;
    }
    if (!json_data["number"].is_number_unsigned() || !json_data["parent"].is_string() ||
        !json_data["fork_ts"].is_number_unsigned()) {
      spdlog::warn("VersionStore: failed to load branch '{}', invalid record shape.", name);
      continue;
    }

    BranchInfo info{
        .number = json_data["number"].get<uint64_t>(),
        .parent = json_data["parent"].get<std::string>(),
        .fork_ts = json_data["fork_ts"].get<uint64_t>(),
        .description = std::nullopt,
    };
    if (const auto &desc = json_data["description"]; desc.is_string()) {
      info.description = desc.get<std::string>();
    }

    max_number = std::max(max_number, info.number);
    branches_.emplace(name, std::move(info));
  }

  // The counter must be sourced independently of which branches are currently live: DropBranch
  // deletes the branch's own kvstore record outright, so deriving next_number_ purely from
  // surviving records would let a dropped branch's number be handed out again (spec S3:
  // "monotonic and never reused, even after a branch is dropped"). Fall back to the old
  // derive-from-max-number behavior only for a legacy/empty store that predates this counter key.
  if (auto persisted_next = storage_.Get(kNextNumberKey); persisted_next) {
    try {
      next_number_ = std::stoull(*persisted_next);
    } catch (const std::exception &e) {
      spdlog::warn("VersionStore: corrupt '{}' record ('{}'): {}. Falling back to a derived value.",
                   kNextNumberKey,
                   *persisted_next,
                   e.what());
      next_number_ = max_number + 1;
    }
  } else {
    next_number_ = max_number + 1;
  }

  // Seed the active-branches gauge so it reflects persisted state immediately after restart.
  // Without this, the gauge reads 0 until the first CREATE/DROP mutates it.
  memgraph::metrics::Metrics().global.versioning_active_branches->Set(static_cast<double>(branches_.size()));
}

std::expected<BranchInfo, std::string> VersionStore::CreateBranch(std::string name, std::string parent,
                                                                  std::optional<std::string> description) {
  std::lock_guard guard{lock_};

  if (branches_.contains(name)) {
    return std::unexpected(fmt::format("Branch '{}' already exists.", name));
  }
  if (parent != "main" && !branches_.contains(parent)) {
    return std::unexpected(fmt::format("Parent branch '{}' does not exist.", parent));
  }

  BranchInfo info{
      .number = next_number_,
      .parent = std::move(parent),
      .fork_ts = acquire_pin_(),
      .description = std::move(description),
  };

  nlohmann::json data = nlohmann::json::object();
  data["version"] = kVersion;
  data["number"] = info.number;
  data["parent"] = info.parent;
  data["fork_ts"] = info.fork_ts;
  data["description"] = info.description ? nlohmann::json(*info.description) : nlohmann::json(nullptr);

  // The counter must be persisted in the SAME write as the branch record: DropBranch deletes the
  // branch's own record outright, so the counter cannot be re-derived from surviving records after
  // a restart (spec S3, "monotonic and never reused, even after a branch is dropped"). PutMultiple
  // writes both keys as a single atomic batch, so a crash never leaves the counter behind (or
  // ahead of) the branch record it's supposed to account for.
  const uint64_t new_next_number = next_number_ + 1;
  const std::map<std::string, std::string> batch{{name, data.dump()},
                                                 {std::string{kNextNumberKey}, std::to_string(new_next_number)}};
  if (!storage_.PutMultiple(batch)) {
    // Persisting failed: release the pin we just took so we never leak a live GC pin for a branch
    // that isn't actually (durably) registered.
    release_pin_(info.fork_ts);
    return std::unexpected(fmt::format("Failed to persist branch '{}'.", name));
  }

  next_number_ = new_next_number;
  branches_.emplace(std::move(name), info);

  // Graph Versioning v1 chunk 10 (observability): branch created + refresh the active-branches gauge.
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
    if (HasChildrenLocked(name)) {
      return false;
    }
    // A branch with an attached session must not be dropped out from under it; each session must
    // CHECKOUT BRANCH 'main' (detach) first.
    if (checked_out_.contains(name)) {
      return false;
    }

    number = it->second.number;
    release_pin_(it->second.fork_ts);
    storage_.Delete(name);
    branches_.erase(it);

    // Graph Versioning v1 chunk 10 (observability): branch dropped + refresh the active-branches gauge.
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

bool VersionStore::HasChildren(std::string_view name) const {
  std::lock_guard guard{lock_};
  return HasChildrenLocked(name);
}

bool VersionStore::HasChildrenLocked(std::string_view name) const {
  return std::ranges::any_of(branches_, [&](const auto &kv) { return kv.second.parent == name; });
}

std::expected<BranchInfo, std::string> VersionStore::TryAcquireCheckout(std::string_view name) {
  std::lock_guard guard{lock_};
  auto it = branches_.find(name);
  if (it == branches_.end()) {
    return std::unexpected(fmt::format("Version '{}' does not exist.", name));
  }
  ++checked_out_[std::string{name}];
  ++total_checkouts_;

  // Graph Versioning v1 chunk 10 (observability): refresh the active-checkouts gauge.
  memgraph::metrics::Metrics().global.versioning_active_checkouts->Set(static_cast<double>(total_checkouts_));
  return it->second;
}

void VersionStore::ReleaseCheckout(std::string_view name) {
  std::lock_guard guard{lock_};
  if (auto it = checked_out_.find(name); it != checked_out_.end()) {
    --total_checkouts_;
    if (--it->second == 0) checked_out_.erase(it);
  }

  // Graph Versioning v1 chunk 10 (observability): refresh the active-checkouts gauge.
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
  // If another session won the race, `built` (the loser) is destroyed here, outside the mutex.
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
