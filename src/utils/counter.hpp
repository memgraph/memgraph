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

#include <atomic>
#include <cstdint>
#include <stdexcept>

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

struct ResettableAtomicCounter {
  explicit ResettableAtomicCounter(uint64_t const original_size) : original_size_(original_size) {
    if (original_size == 0) {
      throw std::invalid_argument("Counter needs to be initialized with a value larger than 0");
    }
  }

  bool operator()(uint32_t const update_factor) {
    auto old_value = current_.load(std::memory_order_acquire);
    while (true) {
      auto new_value = old_value + update_factor;
      bool const status = new_value >= original_size_;
      new_value %= original_size_;
      if (current_.compare_exchange_weak(old_value, new_value)) {
        return status;
      }
    }
  }

 private:
  uint64_t original_size_;
  std::atomic<uint64_t> current_;
};

}  // namespace memgraph::utils
