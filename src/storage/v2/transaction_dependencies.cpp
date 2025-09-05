// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "transaction_dependencies.hpp"

namespace memgraph::storage {

static bool IsWaitingFor(std::unordered_map<uint64_t, uint64_t> const &deps, uint64_t from, uint64_t to) {
  auto const it = deps.find(from);
  if (it == deps.cend()) {
    return false;
  } else if (it->second == to) {
    return true;
  } else {
    return IsWaitingFor(deps, it->second, to);
  }
}

void TransactionDependencies::RegisterTransaction(uint64_t transaction_id) {
  std::unique_lock l{mutex_};
  active_transactions_.insert(transaction_id);
}

void TransactionDependencies::UnregisterTransaction(uint64_t transaction_id) {
  std::unique_lock l{mutex_};
  active_transactions_.erase(transaction_id);
  cv_.notify_all();
}

WaitResult TransactionDependencies::WaitFor(uint64_t dependent_transaction_id, uint64_t prerequisite_transaction_id) {
  std::unique_lock l{mutex_};

  if (active_transactions_.count(prerequisite_transaction_id) == 0) {
    return WaitResult::SUCCESS;
  }

  if (IsWaitingFor(dependencies_, prerequisite_transaction_id, dependent_transaction_id)) {
    return WaitResult::CIRCULAR_DEPENDENCY;
  }

  dependencies_[dependent_transaction_id] = prerequisite_transaction_id;

  cv_.wait(l, [&]() { return active_transactions_.count(prerequisite_transaction_id) == 0; });

  dependencies_.erase(dependent_transaction_id);
  return WaitResult::SUCCESS;
}

}  // namespace memgraph::storage
