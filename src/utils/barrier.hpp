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

#include <atomic>
#include <barrier>
#include <cstddef>

namespace memgraph::utils {

// EXPERIMENTAL: this delegates to std::barrier, which a hand-rolled implementation used to replace
// on the grounds that std::barrier missed notifications and left threads blocked forever. That
// claim was never tied to a compiler version or a reproducer, and Barrier.StdBarrierHammer exists
// to say whether it still holds. Restore the hand-rolled version from git history if it does.
//
// std::barrier does not offer the two guarantees callers here rely on, so they are added:
// a wait() that blocks until every thread has left the barrier, and a destructor that does the
// same, so the barrier outlives the threads waiting on it.
class SimpleBarrier {
 public:
  explicit SimpleBarrier(size_t n) : barrier_{static_cast<std::ptrdiff_t>(n)}, remaining_{n} {}

  ~SimpleBarrier() { wait(); }

  SimpleBarrier(const SimpleBarrier &) = delete;
  SimpleBarrier &operator=(const SimpleBarrier &) = delete;
  SimpleBarrier(SimpleBarrier &&) = delete;
  SimpleBarrier &operator=(SimpleBarrier &&) = delete;

  void arrive_and_wait() {
    barrier_.arrive_and_wait();
    // Counted down after the barrier releases, so wait() below returns only once every thread is
    // through and no thread still holds a reference to this object.
    if (--remaining_ == 0) {
      done_ = true;
      done_.notify_all();
    }
  }

  void wait() { done_.wait(false); }

 private:
  std::barrier<> barrier_;
  std::atomic<size_t> remaining_;
  std::atomic_bool done_{false};
};

}  // namespace memgraph::utils
