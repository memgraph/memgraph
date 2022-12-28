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

#include <concepts>
#include <limits>
#include <mutex>
#include <optional>
#include <vector>

#include "query/v2/physical/mock/frame.hpp"
#include "utils/cast.hpp"
#include "utils/logging.hpp"

namespace memgraph::query::v2::physical::multiframe {

// TODO(gitbuda): Reset/Clear data bool state.

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
template <typename TFrame>
class Multiframe {
 public:
  Multiframe() = delete;
  Multiframe(const Multiframe &) = delete;
  Multiframe(Multiframe &&) noexcept = default;
  Multiframe &operator=(const Multiframe &) = delete;
  Multiframe &operator=(Multiframe &&) noexcept = default;
  explicit Multiframe(const size_t capacity = 0) : data_(0) { data_.reserve(capacity); }
  ~Multiframe() = default;

  size_t Capacity() const { return data_.capacity(); }
  size_t Size() const { return data_.size(); }
  bool IsFull() const { return Size() == Capacity(); }
  void PushBack(const TFrame &frame) { data_.push_back(frame); }
  void Put(const TFrame &frame, int at) { data_[at] = frame; }
  const std::vector<TFrame> &Data() const { return data_; }
  // The main issue with the Multiframe is that once we have to reuse a filled
  // frame, the cleanup cost might be high -> ensure Multiframe "reset"
  // (whatever that might be) is fast.
  //
  // If the stored elements have trivial destructors, then the implementation
  // can optimize the cost away and clear() becomes a cheap O(1) operation
  // (just resetting the size --end pointer).
  //
  // vector<vector<TypedValue>> (all with custom allocator), all have to be cleared
  //
  // https://stackoverflow.com/questions/15004517/moving-elements-from-stdvector-to-another-one
  //   -> v2.insert + v1.erase
  //   -> test how many times TypedValue destructor is called in case the Frame object is moved
  //
  // TODO(gitbuda): Implement fast Multiframe::Clear
  //   Clear implementation should be fast because the pool will call Clear to
  //   ensure each returned empty multiframe is a clear state. E.g. if
  //   Multiframe contains objects which are expensive to delete (e.g. lists),
  //   Clear will be slower.
  //
  void Clear() { data_.clear(); }

 private:
  // The multiframe don't have to be moved in our case, but since Frame has the
  // allocator -> move noexcept is not possible (Frame is a vector) -> moving
  // Frame is not possible
  //
  // Because the above -> don't mark mocked frame's move noexcept
  //
  // TODO(gitbuda): Make test on how to move individual Frame between vectors.
  //
  std::vector<TFrame> data_;
};

template <typename TPool>
concept MultiframePoolConcept = requires(TPool p, int id) {
  { p.GetEmpty() } -> std::same_as<std::optional<typename TPool::Token>>;
  { p.GetFull() } -> std::same_as<std::optional<typename TPool::Token>>;
  { p.ReturnEmpty(id) } -> std::same_as<void>;
  { p.ReturnFull(id) } -> std::same_as<void>;
};

/// Preallocated memory for an operator results.
/// Responsible only for synchronizing access to a set of Multiframes.
/// Requires giving back Token after Multiframe is consumed/filled.
/// Equivalent to a queue of Multiframes with intention to minimize the time
/// spent in critical sections.
class MPMCMultiframeFCFSPool {
 public:
  using TFrame = mock::Frame;
  using TMultiframe = Multiframe<TFrame>;

  // TODO(gitbuda): PoolState should be composable, because e.g. at the same
  // time it's possible be in both HAS_MORE and HINT_WRITER_DONE states.
  //
  enum class PoolState : uint32_t {
    EMPTY = 1U << 0U,
    HAS_MORE = 1U << 1U,
    HAS_SPACE = 1U << 2U,
    FULL = 1U << 3U,
    HINT_WRITER_DONE = 1U << 4U,
    EXHAUSTED = 1U << 5U,
  };
  struct Token {
    int id;
    Multiframe<TFrame> *multiframe;
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

  explicit MPMCMultiframeFCFSPool(int pool_size, size_t multiframe_size)
      : pool_state_(utils::UnderlyingCast(PoolState::EMPTY)) {
    for (int i = 0; i < pool_size; ++i) {
      frames_.emplace_back(Multiframe<TFrame>{multiframe_size});
    }
    for (int i = 0; i < pool_size; ++i) {
      priority_states_.emplace_back(InternalToken{.priority = 0, .state = MultiframeState::EMPTY});
    }
  }
  MPMCMultiframeFCFSPool() = delete;
  MPMCMultiframeFCFSPool(const MPMCMultiframeFCFSPool &) = delete;
  MPMCMultiframeFCFSPool(MPMCMultiframeFCFSPool &&) = delete;
  MPMCMultiframeFCFSPool &operator=(const MPMCMultiframeFCFSPool &) = delete;
  MPMCMultiframeFCFSPool &operator=(MPMCMultiframeFCFSPool &&) = delete;
  ~MPMCMultiframeFCFSPool() = default;

  /// if nullopt -> useful multiframe is not available.
  /// The below implementation should be a correct but probably suboptimal
  /// implementation of MPMC mode with FCFS ordering of multiframes.
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
      // We have to wait for the next multiframe by order, client has to retry
      if (last_taken_priority_ + 1 != min_full) {
        return std::nullopt;
      }
      last_taken_priority_ = min_full;
      priority_states_[index].state = MultiframeState::IN_USE;
      // An issue is that sometime out of order multiframe gets full first,
      // this is just a check to make sure returned multiframes are in order.
      MG_ASSERT(min_full > order_check_, "has to grow monotonic");
      order_check_ = min_full;
      return Token{.id = static_cast<int>(index), .multiframe = &frames_.at(index)};
    }

    pool_state_ |= utils::UnderlyingCast(PoolState::EMPTY);
    return std::nullopt;
  }

  std::optional<Token> GetEmpty() {
    std::unique_lock lock{mutex};
    ++priority_counter_;
    for (int index = 0; index < priority_states_.size(); ++index) {
      if (priority_states_[index].state == MultiframeState::EMPTY) {
        priority_states_[index].priority = priority_counter_;
        priority_states_[index].state = MultiframeState::IN_USE;
        frames_[index].Clear();
        return Token{.id = static_cast<int>(index), .multiframe = &frames_.at(index)};
      }
    }
    // in case we haven't found an empty frame the priority counter has to be
    // set on the previous value because it hasn't actually being used
    --priority_counter_;
    pool_state_ |= utils::UnderlyingCast(PoolState::FULL);
    return std::nullopt;
  }

  void ReturnEmpty(int id) {
    std::unique_lock lock{mutex};
    MG_ASSERT(priority_states_[id].state == MultiframeState::IN_USE, "should be in use");
    priority_states_[id].state = MultiframeState::EMPTY;
    // NOTE: At this point we can call Multiframe::Clear but the same call is
    // done inside GetEmpty method because we are delaying the cleanup as much
    // as possible. We might not even need to explicitly cleanup the frame
    // because the frame might not be reused at all.
    pool_state_ |= utils::UnderlyingCast(PoolState::HAS_SPACE);
  }

  void ReturnFull(int id) {
    std::unique_lock lock{mutex};
    MG_ASSERT(priority_states_[id].state == MultiframeState::IN_USE, "should be in use");
    priority_states_[id].state = MultiframeState::FULL;
    pool_state_ &= ~utils::UnderlyingCast(PoolState::EMPTY);
    pool_state_ |= utils::UnderlyingCast(PoolState::HAS_MORE);
  }

  void MarkWriterDone() {
    std::unique_lock lock{mutex};
    pool_state_ |= utils::UnderlyingCast(PoolState::HINT_WRITER_DONE);
  }
  bool IsWriterDone() {
    std::unique_lock lock{mutex};
    return pool_state_ & utils::UnderlyingCast(PoolState::HINT_WRITER_DONE);
  }

  // NOTE: There is a difference between exhaustion of a Multiframe and the entire pool.
  void MarkExhausted() {
    std::unique_lock lock{mutex};
    pool_state_ |= utils::UnderlyingCast(PoolState::EXHAUSTED);
  }
  bool IsExhausted() {
    std::unique_lock lock{mutex};
    return pool_state_ & utils::UnderlyingCast(PoolState::EXHAUSTED);
  }

  bool IsEmpty() {
    std::unique_lock lock{mutex};
    return pool_state_ & utils::UnderlyingCast(PoolState::EMPTY);
  }

 private:
  // NOTE: A few potential issue with this design
  //   * Copying/Moving/Destroying underlying values might be expensive
  //     * How to move the entire Multiframe/TResult between pools?
  //       Instead of move+destroy, pass the Multiframe further and create a new one here
  //         * Good as long as the total number of Multiframes is the same -> ONLY SWAP IS OK
  //           Go here only when move+destroy is a noticeable problem
  //
  std::vector<multiframe::Multiframe<TFrame>> frames_;
  std::vector<InternalToken> priority_states_;
  int64_t priority_counter_{-1};
  int64_t order_check_{-1};
  int64_t last_taken_priority_{-1};
  // TODO(gitbuda): An interesting metric would be to count calls which require taking the lock?
  std::mutex mutex;
  std::underlying_type<PoolState>::type pool_state_;
};

static_assert(MultiframePoolConcept<MPMCMultiframeFCFSPool>);

}  // namespace memgraph::query::v2::physical::multiframe
