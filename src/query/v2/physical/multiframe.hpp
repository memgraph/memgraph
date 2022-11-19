// Copyright 2022 Memgraph Ltd.
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

#include <limits>
#include <mutex>
#include <vector>

namespace memgraph::query::v2::physical {

struct Frame {
  int64_t a;
  int64_t b;
};

/// Fixed in size during query execution.
/// NOTE/TODO(gitbuda): Accessing Multiframe might be tricky because of multi-threading.
/// As soon as one operator "gives" data to the other operator (in any direction), if operators
/// operate on different threads there has to be some way of synchronization.
///   OPTIONS:
///     1) Make Multiframe thread-safe.
///     2) Ensure returned Multiframe is not accessed until is not reclaimed (additional method).
///
/// At the moment OPTION 2 is implemented. Multiframe is not thread-safe, but
/// access to the Multiframe reference is "protected" by appropriate methods.
/// The intention is to make interactions with the Multiframe as fast as
/// possible, while minimizing the overhead of concurrent access while getting
/// correct Multiframe references.
///
/// TODO(gitbuda): Articulate how to actually do/pass aggregations in the Multiframe context.
///
/// Moving/copying Frames between operators is questionable because operators
/// mostly operate on a single Frame value.
///
class Multiframe {
 public:
  Multiframe() = delete;
  Multiframe(const Multiframe &) = default;
  Multiframe(Multiframe &&) = default;
  Multiframe &operator=(const Multiframe &) = default;
  Multiframe &operator=(Multiframe &&) = default;
  explicit Multiframe(const size_t capacity = 0) : data_(0) { data_.reserve(capacity); }
  ~Multiframe() = default;

  size_t Size() const { return data_.size(); }
  void Put(const Frame &frame) { data_.push_back(frame); }
  const std::vector<Frame> &Data() const { return data_; }

 private:
  std::vector<Frame> data_;
};

/// Preallocated memory for an operator results.
/// Responsible only for synchronizing access to a set of Multiframes.
/// Requires giving back Token after Multiframe is consumed/filled.
/// Equivalent to a queue of Multiframes with intention to minimize the time
/// spent in critical sections.
///
class MultiframePool {
 public:
  struct Token {
    int id;
    Multiframe *multiframe;
  };
  /// Critical because all filled multiframes should be consumed at some point.
  enum class MultiframeState {
    EMPTY,
    IN_USE,
    FULL,
  };
  struct InternalToken {
    int64_t priority;
    MultiframeState state;
  };
  // TODO(gitbuda): Decide how to know that there is no more data for a given operator.
  //   1) Probably better outside the pool because then the pool is more generic.
  //   2) Since Emit and Next are decoupled there has to be some source of truth.
  enum class PoolState {
    EMPTY,
    HAS_MORE,
    EXHAUSTED,
  };
  enum class Mode {
    // TODO(gitbuda): To implement FCFS ordering policy a queue of token is
    // required with round robin id assignment.
    //   * A queue seem very intuitive choice but there are a lot of issues.
    //   * Trivial queue doesn't work because returning token is not trivial.
    // FCFS/LIFO is a probably a wrong concept here, instead:
    //   * Single/Multiple Producer + Single/Multiple Consumer is probably the way to go.
    //
    SPSC,  // With 2 Multiframes SPSC already provides simultaneous operator execution.
    SPMC,
    MPSC,
    MPMC,
    // All the above 4 modes are meaningful in the overall operator stack. If
    // MultiframePool is used between the operators, each operator can specify
    // the actual mode because of the operator semantics.
    //
  };
  enum class Order {
    FCFS,
    RANDOM,
  };

  explicit MultiframePool(int pool_size, size_t multiframe_size) {
    for (int i = 0; i < pool_size; ++i) {
      frames_.emplace_back(Multiframe{multiframe_size});
    }
    for (int i = 0; i < pool_size; ++i) {
      priority_states_.emplace_back(InternalToken{.priority = 0, .state = MultiframeState::EMPTY});
    }
  }
  MultiframePool() = delete;
  MultiframePool(const MultiframePool &) = delete;
  MultiframePool(MultiframePool &&) = delete;
  MultiframePool &operator=(const MultiframePool &) = delete;
  MultiframePool &operator=(MultiframePool &&) = delete;
  ~MultiframePool() = default;

  /// if nullopt -> useful multiframe is not available.
  /// The below implementation should be a correct but probably suboptimal
  /// implementation of MPMC mode with FCFS ordering of multiframes.
  ///
  std::optional<Token> GetFull() {
    std::unique_lock lock{mutex};
    int64_t min_full = std::numeric_limits<int64_t>::max();
    MultiframeState state = MultiframeState::EMPTY;
    int64_t index = -1;
    for (int i = 0; i < priority_states_.size(); ++i) {
      if (priority_states_[i].priority < min_full && priority_states_[i].state == MultiframeState::FULL) {
        min_full = priority_states_[i].priority;
        state = MultiframeState::FULL;
        index = i;
      }
    }

    if (state == MultiframeState::FULL) {
      // we have to wait for the next multiframe by order, client has to retry
      if (last_taken_priority_ + 1 != min_full) {
        return std::nullopt;
      }
      last_taken_priority_ = min_full;
      priority_states_[index].state = MultiframeState::IN_USE;
      // TODO(gitbuda): An issue is that sometime out of order frame gets full first
      MG_ASSERT(min_full > order_check_, "has to grow monotonic");
      order_check_ = min_full;
      return Token{.id = static_cast<int>(index), .multiframe = &frames_.at(index)};
    }
    return std::nullopt;
  }

  std::optional<Token> GetEmpty() {
    std::unique_lock lock{mutex};
    ++priority_counter_;
    for (int index = 0; index < priority_states_.size(); ++index) {
      if (priority_states_[index].state == MultiframeState::EMPTY) {
        priority_states_[index].priority = priority_counter_;
        priority_states_[index].state = MultiframeState::IN_USE;
        return Token{.id = static_cast<int>(index), .multiframe = &frames_.at(index)};
      }
    }
    // in case we haven't found an empty frame the priority counter has to be
    // set on the previous value because it hasn't actually being used
    --priority_counter_;
    return std::nullopt;
  }

  void ReturnEmpty(int id) {
    std::unique_lock lock{mutex};
    MG_ASSERT(priority_states_[id].state == MultiframeState::IN_USE, "should be in use");
    priority_states_[id].state = MultiframeState::EMPTY;
  }

  void ReturnFull(int id) {
    std::unique_lock lock{mutex};
    MG_ASSERT(priority_states_[id].state == MultiframeState::IN_USE, "should be in use");
    priority_states_[id].state = MultiframeState::FULL;
  }

 private:
  std::vector<Multiframe> frames_;
  std::vector<InternalToken> priority_states_;
  int64_t priority_counter_{-1};
  int64_t order_check_{-1};
  int64_t last_taken_priority_{-1};
  std::mutex mutex;
};

}  // namespace memgraph::query::v2::physical
