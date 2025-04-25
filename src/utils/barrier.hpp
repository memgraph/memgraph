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

namespace memgraph::utils {
// std::barrier seems to have a bug which leads to missed notifications, so some threads block forever
class SimpleBarrier {
 public:
  explicit SimpleBarrier(size_t n) : phase1_{n}, phase2_{n} {}

  ~SimpleBarrier() { wait(); }

  SimpleBarrier(const SimpleBarrier &) = delete;
  SimpleBarrier &operator=(const SimpleBarrier &) = delete;
  SimpleBarrier(SimpleBarrier &&) = delete;
  SimpleBarrier &operator=(SimpleBarrier &&) = delete;

  void arrive_and_wait() {
    // Phase1 incoming threads decrement and wait for all to arrive
    if (--phase1_ == 0) {
      // All arrived at the barrier
      phase1_.notify_all();
    } else {
      // Wait for all to arrive
      while (phase1_ != 0) phase1_.wait(1);
    }
    // Phase2 decrement and return
    // This guards against the barrier's desctruction while threads are waiting
    if (--phase2_ == 0) {
      phase2_.notify_all();
    }
  }

  void wait() {
    while (phase2_ != 0) phase2_.wait(1);
  }

 private:
  std::atomic<size_t> phase1_;
  std::atomic<size_t> phase2_;
};

}  // namespace memgraph::utils
