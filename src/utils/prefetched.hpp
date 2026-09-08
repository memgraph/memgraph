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
#include <exception>
#include <functional>
#include <thread>
#include <utility>

#include "utils/data_queue.hpp"

namespace memgraph::utils {

/// Runs a producer on its own thread and hands what it makes to a consumer, one item at a time.
///
/// For a source that pushes its work at whoever will take it while the code consuming it wants to
/// pull one item at a time. The queue is bounded, so a producer faster than its consumer waits
/// rather than running away with memory.
///
/// A consumer that stops early is the ordinary case, not an error: destroying this tells the
/// producer to stop, and the destructor does not return until it has. What the producer threw is
/// carried across and rethrown from `Next`, so a failure on the other thread reaches the caller
/// rather than being lost.
///
/// One producer, one consumer. `Next` is not safe to call from two threads.
///
/// As a member, declare this after everything the producer reads. Members are constructed in
/// declaration order and the thread starts here, so anything declared later is still uninitialized
/// when the producer first runs, whatever order the initializer list is written in.
template <typename T>
class Prefetched {
 public:
  /// Offers one item to the consumer. Returns false once the consumer has stopped, which is the
  /// producer's signal to return; it must not keep pushing after that.
  using Push = std::function<bool(T)>;

  /// Runs on the producer thread. Returning ends the source; throwing reports a failure to the
  /// consumer.
  using Produce = std::function<void(Push const &)>;

  /// `depth` is how many items may wait for the consumer, and so how far the producer may run ahead.
  Prefetched(std::size_t depth, Produce produce)
      : queue_{depth}, thread_{[this, produce = std::move(produce)]() { Run(produce); }} {}

  Prefetched(Prefetched const &) = delete;
  auto operator=(Prefetched const &) -> Prefetched & = delete;
  Prefetched(Prefetched &&) = delete;
  auto operator=(Prefetched &&) -> Prefetched & = delete;

  ~Prefetched() {
    // Refusing further items is what a blocked producer sees, so shutting the queue before the
    // thread is joined is what keeps that join from waiting on a consumer that has gone.
    queue_.finish();
  }

  /// Hands over an item only if one is already waiting. Never blocks, so a consumer that already has
  /// something to return can take what has arrived without committing to wait for more.
  auto TryNext(T &out) -> bool { return queue_.try_pop(out); }

  /// Hands over the next item. Returns false once the source is spent, and rethrows what the
  /// producer threw if it failed.
  auto Next(T &out) -> bool {
    if (queue_.pop(out)) {
      return true;
    }

    // finish() happens-before pop() reporting the queue drained, so a failure recorded before it is
    // visible here without further synchronization.
    if (failure_) {
      std::rethrow_exception(std::exchange(failure_, nullptr));
    }
    return false;
  }

 private:
  void Run(Produce const &produce) {
    try {
      produce([this](T item) { return queue_.push(std::move(item)); });
    } catch (...) {
      failure_ = std::current_exception();
    }
    queue_.finish();
  }

  DataQueue<T> queue_;
  std::exception_ptr failure_;
  // Declared last so the thread is joined before anything it touches is destroyed.
  std::jthread thread_;
};

}  // namespace memgraph::utils
