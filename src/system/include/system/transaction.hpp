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

#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include "replication/state.hpp"
#include "system/action.hpp"
#include "system/state.hpp"

namespace memgraph::system {

enum class AllSyncReplicaStatus : std::uint8_t {
  AllCommitsConfirmed,
  SomeCommitsUnconfirmed,
};

struct Transaction;

template <typename T>
concept ReplicationPolicy = requires(T handler, ISystemAction const &action, Transaction const &txn) {
  { handler.apply_action(action, txn) } -> std::same_as<AllSyncReplicaStatus>;
};

struct System;

struct Transaction {
  template <std::derived_from<ISystemAction> TAction, typename... Args>
  requires std::constructible_from<TAction, Args...>
  void add_action(Args &&...args) { actions_.emplace_back(std::make_unique<TAction>(std::forward<Args>(args)...)); }

  template <ReplicationPolicy Handler>
  auto commit(Handler handler) -> AllSyncReplicaStatus {
    if (!lock_.owns_lock() || actions_.empty()) {
      // If no actions, we do not increment the last commited ts, since there is no delta to send to the REPLICA
      abort();
      return AllSyncReplicaStatus::AllCommitsConfirmed;  // TODO: some kind of error
    }

    auto sync_status = AllSyncReplicaStatus::AllCommitsConfirmed;

    while (!actions_.empty()) {
      auto &action = actions_.front();

      /// durability
      action->do_durability();

      /// replication prep
      auto action_sync_status = handler.apply_action(*action, *this);
      if (action_sync_status != AllSyncReplicaStatus::AllCommitsConfirmed) {
        sync_status = AllSyncReplicaStatus::SomeCommitsUnconfirmed;
      }

      actions_.pop_front();
    }

    state_->FinializeTransaction(timestamp_);
    lock_.unlock();

    return sync_status;
  }

  void abort() {
    if (lock_.owns_lock()) {
      lock_.unlock();
    }
    actions_.clear();
  }

  auto last_committed_system_timestamp() const -> uint64_t { return state_->last_committed_system_timestamp_.load(); }
  auto timestamp() const -> uint64_t { return timestamp_; }

 private:
  friend struct System;
  Transaction(State &state, std::unique_lock<std::timed_mutex> lock, std::uint64_t timestamp)
      : state_{std::addressof(state)}, lock_(std::move(lock)), timestamp_{timestamp} {}

  State *state_;
  std::unique_lock<std::timed_mutex> lock_;
  std::uint64_t timestamp_;
  std::list<std::unique_ptr<ISystemAction>> actions_;
};

struct DoReplication {
  explicit DoReplication(replication::RoleMainData &mainData) : main_data_{mainData} {}
  auto apply_action(ISystemAction const &action, Transaction const &txn) -> AllSyncReplicaStatus {
    auto sync_status = AllSyncReplicaStatus::AllCommitsConfirmed;

    for (auto &client : main_data_.registered_replicas_) {
      bool completed = action.do_replication(client, main_data_.epoch_, txn);
      if (!completed && client.mode_ == replication_coordination_glue::ReplicationMode::SYNC) {
        sync_status = AllSyncReplicaStatus::SomeCommitsUnconfirmed;
      }
    }

    action.post_replication();
    return sync_status;
  }

 private:
  replication::RoleMainData &main_data_;
};
static_assert(ReplicationPolicy<DoReplication>);

struct DoNothing {
  auto apply_action(ISystemAction const & /*action*/, Transaction const & /*txn*/) -> AllSyncReplicaStatus {
    return AllSyncReplicaStatus::AllCommitsConfirmed;
  }
};
static_assert(ReplicationPolicy<DoNothing>);

}  // namespace memgraph::system
