// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "system/state.hpp"

namespace memgraph::system {

namespace {

constexpr std::string_view kSystemDir = ".system";
constexpr std::string_view kVersion = "version";  // Key for version durability
constexpr std::string_view kVersionV1 = "V1";     // Value for version 1

auto InitializeSystemDurability(std::optional<std::filesystem::path> storage, bool recovery_on_startup)
    -> std::optional<memgraph::kvstore::KVStore> {
  if (!storage) return std::nullopt;

  auto const &path = *storage;
  memgraph::utils::EnsureDir(path);
  auto system_dir = path / kSystemDir;
  memgraph::utils::EnsureDir(system_dir);
  auto durability = memgraph::kvstore::KVStore{std::move(system_dir)};

  auto version = durability.Get(kVersion);
  // TODO: migration schemes here in the future
  if (!version || *version != kVersionV1) {
    // ensure we start out with V1
    durability.Put(kVersion, kVersionV1);
  }

  if (!recovery_on_startup) {
    // reset last_committed_system_ts
    durability.Delete(kLastCommitedSystemTsKey);
  }

  return durability;
}

auto LoadLastCommittedSystemTimestamp(std::optional<kvstore::KVStore> const &store) -> uint64_t {
  auto lcst = store ? store->Get(kLastCommitedSystemTsKey) : std::nullopt;
  return lcst ? std::stoul(*lcst) : 0U;
}

}  // namespace

State::State(std::optional<std::filesystem::path> storage, bool recovery_on_startup)
    : durability_{InitializeSystemDurability(std::move(storage), recovery_on_startup)},
      last_committed_system_timestamp_{LoadLastCommittedSystemTimestamp(durability_)} {}
}  // namespace memgraph::system
