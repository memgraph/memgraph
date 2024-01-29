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

#pragma once

#include <atomic>
#include <cstdint>

#include "kvstore/kvstore.hpp"
#include "utils/file.hpp"

namespace memgraph::system {

namespace {
constexpr std::string_view kLastCommitedSystemTsKey = "last_committed_system_ts";  // Key for timestamp durability
}

struct State {
  explicit State(std::optional<std::filesystem::path> storage, bool recovery_on_startup);

  void FinializeTransaction(std::uint64_t timestamp) {
    if (durability_) {
      durability_->Put(kLastCommitedSystemTsKey, std::to_string(timestamp));
    }
    last_committed_system_timestamp_.store(timestamp);
  }

  auto LastCommittedSystemTimestamp() -> uint64_t { return last_committed_system_timestamp_.load(); }

 private:
  friend struct ReplicaHandlerAccessToState;

  friend struct Transaction;
  std::optional<kvstore::KVStore> durability_;

  std::atomic_uint64_t last_committed_system_timestamp_{};
};

struct ReplicaHandlerAccessToState {
  explicit ReplicaHandlerAccessToState(memgraph::system::State &state) : state_{&state} {}

  auto LastCommitedTS() const -> uint64_t { return state_->last_committed_system_timestamp_.load(); }

  void SetLastCommitedTS(uint64_t new_timestamp) { state_->last_committed_system_timestamp_.store(new_timestamp); }

 private:
  State *state_;
};

}  // namespace memgraph::system
