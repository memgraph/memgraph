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

namespace memgraph::versioning {

namespace {
// When the format of the persisted branch record changes, bump this (mirrors
// query::TriggerStore's kVersion, src/query/trigger.cpp). Only one version exists today; this is
// the dispatch point a future format bump would branch on (see TriggerStore::RestoreTrigger /
// MigrateTriggerData for the analogous pattern).
inline constexpr uint64_t kVersion{1};

// Reserved kvstore key for the never-reused branch-number counter. Spec S4.1 forbids branch names
// from starting with '.', so this can never collide with an actual branch record.
constexpr std::string_view kNextNumberKey{".next_number"};
}  // namespace

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
  // HIGH-2 fix: refuse to fork off a parent that is mid-merge (BeginMerge'd but not yet
  // Finish/AbortMerge'd) -- otherwise a child could appear between a merge's own atomic
  // existence/no-children check and its completion, which is exactly the race BeginMerge exists
  // to close (see its doc-comment in version_store.hpp).
  if (merging_.contains(parent)) {
    return std::unexpected(
        fmt::format("Parent branch '{}' is currently being merged; retry after it completes.", parent));
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
  return info;
}

bool VersionStore::DropBranch(std::string_view name) {
  std::lock_guard guard{lock_};

  auto it = branches_.find(name);
  if (it == branches_.end()) {
    return false;
  }
  if (HasChildrenLocked(name)) {
    return false;
  }
  // HIGH-2 fix: a branch mid-merge (BeginMerge'd) must not be droppable out from under the
  // merge in progress -- FinishMerge (not a bare DropBranch) is the only way to remove it once a
  // merge has begun.
  if (merging_.contains(name)) {
    return false;
  }

  release_pin_(it->second.fork_ts);
  storage_.Delete(name);
  branches_.erase(it);
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

bool VersionStore::HasChildren(std::string_view name) const {
  std::lock_guard guard{lock_};
  return HasChildrenLocked(name);
}

bool VersionStore::HasChildrenLocked(std::string_view name) const {
  return std::ranges::any_of(branches_, [&](const auto &kv) { return kv.second.parent == name; });
}

std::expected<BranchInfo, std::string> VersionStore::BeginMerge(std::string_view name) {
  std::lock_guard guard{lock_};

  auto it = branches_.find(name);
  if (it == branches_.end()) {
    return std::unexpected(fmt::format("Branch '{}' does not exist.", name));
  }
  if (HasChildrenLocked(name)) {
    return std::unexpected(fmt::format("Branch '{}' has child branches.", name));
  }
  if (merging_.contains(name)) {
    return std::unexpected(fmt::format("Branch '{}' is already being merged.", name));
  }

  merging_.emplace(name);
  return it->second;
}

void VersionStore::FinishMerge(std::string_view name) {
  std::lock_guard guard{lock_};

  // BeginMerge already excluded new children and a second concurrent merge/drop for the ENTIRE
  // window since it was called, so `it` existing (and childless) here is guaranteed -- this is
  // not a re-check, just retrieving the fork pin to release.
  if (auto it = branches_.find(name); it != branches_.end()) {
    release_pin_(it->second.fork_ts);
    storage_.Delete(name);
    branches_.erase(it);
  }
  merging_.erase(std::string{name});
}

void VersionStore::AbortMerge(std::string_view name) {
  std::lock_guard guard{lock_};
  merging_.erase(std::string{name});
}

}  // namespace memgraph::versioning
