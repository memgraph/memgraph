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
#include <iostream>

namespace memgraph::storage {

// @TODO add unit tests for this class.

// static bool IsWaitingFor(std::unordered_map<uint64_t, uint64_t> const& deps, uint64_t from, uint64_t to) {
//     auto const it = deps.find(from);
//     if (it == deps.cend()) {
//         return false;
//     } else if (it->second == to) {
//         return true;
//     } else {
//         return IsWaitingFor(it->second, to);
//     }
// }

void TransactionDependencies::RegisterTransaction(uint64_t transaction_id) {
  std::cout << "RegisterTransaction " << transaction_id << "\n";
  std::unique_lock l{mutex_};
  active_transactions_.insert(transaction_id);
}

void TransactionDependencies::UnregisterTransaction(uint64_t transaction_id) {
  std::cout << "UnregisterTransaction " << transaction_id << "\n";
  std::unique_lock l{mutex_};
  active_transactions_.erase(transaction_id);
  cv_.notify_all();
}

bool TransactionDependencies::WaitFor(uint64_t dependent_transaction_id, uint64_t prerequisite_transaction_id) {
  std::cout << "WaitFor " << dependent_transaction_id << " on " << prerequisite_transaction_id << "\n";
  // If there is no transitive dependency between prerequisite_transaction_id and depdendent_transaction_id,
  // we can just wait on a condition variable. @TODO just assume that for now, as that is all that I
  // am using in my test cases.
  std::unique_lock l{mutex_};

  while (true) {
    cv_.wait(l, [&]() {
      puts("CHECKING");
      return active_transactions_.count(prerequisite_transaction_id) == 0;
    });
  }

  return true;
}

}  // namespace memgraph::storage
