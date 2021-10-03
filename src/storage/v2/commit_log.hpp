// Copyright 2021 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/// @file commit_log.hpp
#pragma once

#include <cstdint>
#include <mutex>

#include "utils/memory.hpp"
#include "utils/spin_lock.hpp"

namespace storage {

/// This class keeps track of finalized transactions to provide info on the
/// oldest active transaction (minimal transaction ID which could still be
/// active).
///
/// Basically, it is a set which, at the beginning, contains all transaction
/// IDs and supports two operations: remove an ID from the set (\ref
/// SetFinished) and retrieve the minimal ID still in the set (\ref
/// OldestActive).
///
/// This class is thread-safe.
class CommitLog final {
 public:
  // TODO(mtomic): use pool allocator for blocks
  CommitLog();
  /// Create a commit log which has the oldest active id set to
  /// oldest_active
  /// @param oldest_active the oldest active id
  explicit CommitLog(uint64_t oldest_active);

  CommitLog(const CommitLog &) = delete;
  CommitLog &operator=(const CommitLog &) = delete;
  CommitLog(CommitLog &&) = delete;
  CommitLog &operator=(CommitLog &&) = delete;

  ~CommitLog();

  /// Mark a transaction as finished.
  /// @throw std::bad_alloc
  void MarkFinished(uint64_t id);

  /// Retrieve the oldest transaction still not marked as finished.
  uint64_t OldestActive();

 private:
  static constexpr uint64_t kBlockSize = 8192;
  static constexpr uint64_t kIdsInField = sizeof(uint64_t) * 8;
  static constexpr uint64_t kIdsInBlock = kBlockSize * kIdsInField;

  struct Block {
    Block *next{nullptr};
    uint64_t field[kBlockSize]{};
  };

  void UpdateOldestActive();

  /// @throw std::bad_alloc
  Block *FindOrCreateBlock(uint64_t id);

  Block *head_{nullptr};
  uint64_t head_start_{0};
  uint64_t next_start_{0};
  uint64_t oldest_active_{0};
  utils::SpinLock lock_;
  utils::Allocator<Block> allocator_;
};

}  // namespace storage
