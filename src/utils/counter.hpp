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

#include <cstdint>

namespace memgraph::utils {

/// A resettable counter, every Nth call returns true
template <std::size_t N>
auto ResettableCounter() {
  return [counter = N]() mutable {
    --counter;
    if (counter != 0) return false;
    counter = N;
    return true;
  };
}

struct ResettableRuntimeCounter {
 public:
  explicit ResettableRuntimeCounter(uint64_t original_size) : original_size_(original_size), current_(original_size) {}

  bool operator()() {
    --current_;
    if (current_ != 0) return false;
    current_ = original_size_;
    return true;
  }

 private:
  uint64_t original_size_;
  uint64_t current_;
};

}  // namespace memgraph::utils
