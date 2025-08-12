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

#include "utils/resource_monitoring.hpp"

namespace memgraph::utils {

bool TransactionsMemoryResource::Allocate(size_t size) {
  const auto result = Increment(size, MemoryTrackerCanThrow());
  if (!result.success) {
    // register our error data, we will pick this up on the other side of jemalloc
    utils::MemoryErrorStatus().set({static_cast<int64_t>(size), static_cast<int64_t>(result.current),
                                    static_cast<int64_t>(result.limit), utils::MemoryTrackerStatus::kUser});
  }
  return result.success;
}
void TransactionsMemoryResource::Deallocate(size_t size) { Decrement(size); }

}  // namespace memgraph::utils
