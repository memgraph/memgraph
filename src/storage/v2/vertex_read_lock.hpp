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

#include <shared_mutex>

#include "storage/v2/vertex.hpp"
#include "utils/rw_spin_lock.hpp"

namespace memgraph::storage {

/// Holds a vertex's read lock for as long as a reader needs the record to stay still.
///
/// The lock is released when the guard goes out of scope, unless the vertex has uncommitted
/// non-sequential deltas: a reader that has to walk those needs the record held across the walk.
class VertexReadLock {
 public:
  explicit VertexReadLock(Vertex const *vertex) : vertex_{vertex}, lock_{vertex->lock, std::defer_lock} {}

  class SnapshotGuard {
   public:
    explicit SnapshotGuard(VertexReadLock *manager, bool has_uncommitted_non_sequential_deltas)
        : manager_{manager}, has_uncommitted_non_sequential_deltas_{has_uncommitted_non_sequential_deltas} {}

    ~SnapshotGuard() {
      if (!has_uncommitted_non_sequential_deltas_) {
        manager_->lock_.unlock();
      }
    }

    SnapshotGuard(SnapshotGuard const &) = delete;
    SnapshotGuard(SnapshotGuard &&) = delete;
    SnapshotGuard &operator=(SnapshotGuard const &) = delete;
    SnapshotGuard &operator=(SnapshotGuard &&) = delete;

   private:
    VertexReadLock *manager_;
    bool has_uncommitted_non_sequential_deltas_;
  };

  // `AcquireLock` can be called at most once per `VertexReadLock` instance.
  // Calling it again on a `VertexReadLock` for which you've already acquired
  // the lock could result in deadlock.
  SnapshotGuard AcquireLock() {
    lock_.lock();
    return SnapshotGuard{this, vertex_->has_uncommitted_non_sequential_deltas()};
  }

 private:
  Vertex const *vertex_;
  std::shared_lock<utils::RWSpinLock> lock_;
};

}  // namespace memgraph::storage
