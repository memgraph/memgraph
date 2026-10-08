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

#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include "kvstore/kvstore.hpp"
#include "utils/uuid.hpp"

namespace memgraph::system {

namespace {
constexpr std::string_view kLastCommitedSystemTsKey = "last_committed_system_ts";  // Key for timestamp durability
}  // namespace

struct State {
  explicit State(std::optional<std::filesystem::path> storage, bool recovery_on_startup);

  void FinalizeTransaction(std::uint64_t timestamp) {
    if (durability_) {
      durability_->Put(kLastCommitedSystemTsKey, std::to_string(timestamp));
    }
    last_committed_system_timestamp_.store(timestamp, std::memory_order_release);
  }

  auto LastCommittedSystemTimestamp() const -> uint64_t {
    return last_committed_system_timestamp_.load(std::memory_order_acquire);
  }

 private:
  friend struct ReplicaHandlerAccessToState;
  friend struct Transaction;

  std::optional<kvstore::KVStore> durability_;
  std::atomic_uint64_t last_committed_system_timestamp_{};
};

struct ReplicaHandlerAccessToState {
  explicit ReplicaHandlerAccessToState(memgraph::system::State &state)
      : state_{&state}, note_{std::make_shared<DeltaNote>()} {}

  // Records new_ts for main_uuid even when expected_ts mismatches (a stale recovery can land after a rejected delta).
  [[nodiscard]] bool CheckDelta(utils::UUID const &main_uuid, uint64_t expected_ts, uint64_t new_ts) {
    {
      std::lock_guard const lock{note_->mtx};
      if (note_->main_uuid != main_uuid) {
        note_->main_uuid = main_uuid;
        note_->ts = new_ts;
      } else {
        note_->ts = std::max(note_->ts, new_ts);
      }
    }
    return expected_ts == LastCommitedTS();
  }

  // True if forced_ts predates a delta already announced by this MAIN. Resets the note so a MAIN whose ts
  // legitimately regressed under the same uuid (restart) recovers on its next attempt.
  [[nodiscard]] bool RefuseStaleRecovery(utils::UUID const &main_uuid, uint64_t forced_ts) {
    std::lock_guard const lock{note_->mtx};
    if (note_->main_uuid == main_uuid && forced_ts < note_->ts) {
      note_->main_uuid.reset();
      note_->ts = 0;
      return true;
    }
    return false;
  }

  auto LastCommitedTS() const -> uint64_t {
    return state_->last_committed_system_timestamp_.load(std::memory_order_acquire);
  }

  void SetLastCommitedTS(uint64_t new_timestamp) { state_->FinalizeTransaction(new_timestamp); }

 private:
  // Leaf lock: never held across any apply. Shared by accessor copies; fresh per replica-role Register.
  struct DeltaNote {
    std::mutex mtx;
    std::optional<utils::UUID> main_uuid;
    uint64_t ts{};
  };

  State *state_;
  std::shared_ptr<DeltaNote> note_;
};

}  // namespace memgraph::system
