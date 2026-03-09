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

#include <cstdint>
#include <utility>
#include <variant>

#include "storage/v2/edge.hpp"
#include "utils/epoch_tracker.hpp"
#include "utils/skip_list.hpp"

namespace memgraph::storage {

// RAII guard that acquires an epoch ID from an EpochTracker on construction and
// releases it on destruction.
//
// Edge-index Iterables store either a utils::SkipList<Edge>::ConstAccessor (normal-edge mode,
// prevents GC from freeing the edges_ skiplist nodes that hold the Edge structs) or one of
// these guards (light-edge mode, where edges_ is empty and edges are pool-allocated instead).
//
// The graveyard drain condition uses EpochTracker::IsSafeToFree(guard_epoch) to allow draining
// once all readers that existed before the graveyard entry was inserted have finished — more
// precise than the previous "active count == 0" check, which blocked even readers created after.
struct LightEdgeIterableGuard {
  LightEdgeIterableGuard() = default;

  explicit LightEdgeIterableGuard(utils::EpochTracker *tracker) : tracker_(tracker) {
    if (tracker_) id_ = tracker_->Acquire();
  }

  ~LightEdgeIterableGuard() {
    if (tracker_) tracker_->Release(id_);
  }

  LightEdgeIterableGuard(LightEdgeIterableGuard &&o) noexcept
      : tracker_(std::exchange(o.tracker_, nullptr)), id_(o.id_) {}

  LightEdgeIterableGuard &operator=(LightEdgeIterableGuard &&o) noexcept {
    if (this != &o) {
      if (tracker_) tracker_->Release(id_);
      tracker_ = std::exchange(o.tracker_, nullptr);
      id_ = o.id_;
    }
    return *this;
  }

  LightEdgeIterableGuard(LightEdgeIterableGuard const &) = delete;
  LightEdgeIterableGuard &operator=(LightEdgeIterableGuard const &) = delete;

 private:
  utils::EpochTracker *tracker_{nullptr};
  uint64_t id_{0};
};

// Pin type stored in every edge-index Iterable/ChunkedIterable.
// - ConstAccessor: normal edges — prevents edges_ skiplist GC from freeing Edge memory.
// - LightEdgeIterableGuard: light edges — acquires an epoch ID so the graveyard drain
//   only unblocks once all pre-existing readers have finished.
using EdgePin = std::variant<utils::SkipList<Edge>::ConstAccessor, LightEdgeIterableGuard>;

}  // namespace memgraph::storage
