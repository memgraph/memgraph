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

#include <cstddef>

namespace memgraph::utils {

/// A resettable counter, every Nth call returns true

struct ResettableCounter {
  ResettableCounter(std::size_t N) : counter_{N}, orig_{N} {}

  bool operator()() const {
    --counter_;
    if (counter_ != 0) return false;
    counter_ = orig_;
    return true;
  }

 private:
  mutable std::size_t counter_;
  std::size_t orig_;
};

}  // namespace memgraph::utils
