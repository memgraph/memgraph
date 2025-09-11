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

#pragma once

#include <condition_variable>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace memgraph::storage {

enum class WaitResult { SUCCESS, CIRCULAR_DEPENDENCY };

class TransactionDependencies {
 public:
  void RegisterTransaction(uint64_t transaction_id);
  void UnregisterTransaction(uint64_t transaction_id);

  WaitResult WaitFor(uint64_t dependent_transaction_id, uint64_t prerequisite_transaction_id);

 private:
  std::mutex mutex_;
  std::unordered_set<uint64_t> active_transactions_;
  std::unordered_map<uint64_t, uint64_t> dependencies_;
  std::condition_variable cv_;
};

}  // namespace memgraph::storage
