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

#include "system/state.hpp"
#include "system/transaction.hpp"

namespace memgraph::system {

struct TransactionGuard {
  explicit TransactionGuard(std::unique_lock<std::timed_mutex> guard) : guard_(std::move(guard)) {}

 private:
  std::unique_lock<std::timed_mutex> guard_;
};

struct System {
  // NOTE: default arguments to make testing easier.
  System(std::optional<std::filesystem::path> storage = std::nullopt, bool recovery_on_startup = false)
      : state_(std::move(storage), recovery_on_startup), timestamp_{state_.LastCommittedSystemTimestamp()} {}

  auto TryCreateTransaction(std::chrono::microseconds try_time = std::chrono::milliseconds{100})
      -> std::optional<Transaction> {
    auto system_unique = std::unique_lock{mtx_, std::defer_lock};
    if (!system_unique.try_lock_for(try_time)) {
      return std::nullopt;
    }
    return Transaction{state_, std::move(system_unique), timestamp_++};
  }

  // TODO: this and LastCommittedSystemTimestamp maybe not needed
  auto GenTransactionGuard() -> TransactionGuard { return TransactionGuard{std::unique_lock{mtx_}}; }
  auto LastCommittedSystemTimestamp() -> uint64_t { return state_.LastCommittedSystemTimestamp(); }

  auto CreateSystemStateAccess() -> ReplicaHandlerAccessToState { return ReplicaHandlerAccessToState{state_}; }

 private:
  State state_;
  std::timed_mutex mtx_{};
  std::uint64_t timestamp_{};
};

}  // namespace memgraph::system
