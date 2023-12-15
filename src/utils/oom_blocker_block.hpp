// Copyright 2023 Memgraph Ltd.
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

#include <functional>
#include "utils/memory_tracker.hpp"

namespace memgraph::utils {

// Calls a function with out of memory exception blocker, checks memory allocation after block execution
template <typename Callable>
class [[nodiscard]] OOMBlockerBlock {
 public:
  explicit OOMBlockerBlock(Callable &&function) : function_{std::forward<Callable>(function)} {}
  OOMBlockerBlock(OOMBlockerBlock const &) = delete;
  OOMBlockerBlock(OOMBlockerBlock &&) = delete;
  OOMBlockerBlock &operator=(OOMBlockerBlock const &) = delete;
  OOMBlockerBlock &operator=(OOMBlockerBlock &&) = delete;
  ~OOMBlockerBlock() = default;

  void operator()() {
    {
      utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
      function_();
    }
    total_memory_tracker.DoCheck();
  }

 private:
  std::function<void()> function_;
};
template <typename Callable>
OOMBlockerBlock(Callable &&) -> OOMBlockerBlock<Callable>;

}  // namespace memgraph::utils
