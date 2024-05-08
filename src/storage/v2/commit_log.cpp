// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/commit_log.hpp"
#include "utils/memory.hpp"

#include <mutex>

namespace memgraph::storage {
CommitLog::CommitLog() : allocator_(utils::NewDeleteResource()) {}

CommitLog::CommitLog(uint64_t oldest_active) : allocator_(utils::NewDeleteResource()) {
  head_ = allocator_.allocate(1);
  allocator_.construct(head_);
  head_start_ = oldest_active / kIdsInBlock * kIdsInBlock;
  next_start_ = head_start_ + kIdsInBlock;

  // set all the previous ids
  const auto field_idx = (oldest_active % kIdsInBlock) / kIdsInField;
  for (size_t i = 0; i < field_idx; ++i) {
    head_->field[i] = std::numeric_limits<uint64_t>::max();
  }

  const auto idx_in_field = oldest_active % kIdsInField;
  if (idx_in_field != 0) {
    head_->field[field_idx] = std::numeric_limits<uint64_t>::max();
    head_->field[field_idx] >>= kIdsInField - idx_in_field;
  }

  oldest_active_ = oldest_active;
}

CommitLog::~CommitLog() {
  while (head_) {
    Block *tmp = head_->next;
    head_->~Block();
    allocator_.deallocate(head_, 1);
    head_ = tmp;
  }
}

void CommitLog::MarkFinished(uint64_t id) {
  auto guard = std::lock_guard{lock_};

  Block *block = FindOrCreateBlock(id);
  block->field[(id % kIdsInBlock) / kIdsInField] |= 1ULL << (id % kIdsInField);
  if (id == oldest_active_) {
    UpdateOldestActive();
  }
}

uint64_t CommitLog::OldestActive() {
  auto guard = std::lock_guard{lock_};
  return oldest_active_;
}

void CommitLog::UpdateOldestActive() {
  while (head_) {
    // This is necessary for amortized constant complexity. If we always start
    // from the 0th field, the amount of steps we make through each block is
    // quadratic in kBlockSize.
    uint64_t start_field = oldest_active_ >= head_start_ ? (oldest_active_ - head_start_) / kIdsInField : 0;
    for (uint64_t i = start_field; i < kBlockSize; ++i) {
      if (head_->field[i] != std::numeric_limits<uint64_t>::max()) {
        oldest_active_ = head_start_ + i * kIdsInField + __builtin_ffsl(~head_->field[i]) - 1;
        return;
      }
    }

    // All IDs in this block are marked, we can delete it now.
    Block *tmp = head_->next;
    head_->~Block();
    allocator_.deallocate(head_, 1);
    head_ = tmp;
    head_start_ += kIdsInBlock;
  }

  oldest_active_ = next_start_;
}

CommitLog::Block *CommitLog::FindOrCreateBlock(const uint64_t id) {
  if (!head_) {
    head_ = allocator_.allocate(1);
    allocator_.construct(head_);
    head_start_ = next_start_;
    next_start_ += kIdsInBlock;
  }

  Block *current = head_;
  uint64_t current_start = head_start_;

  while (id >= current_start + kIdsInBlock) {
    if (!current->next) {
      current->next = allocator_.allocate(1);
      allocator_.construct(current->next);
      next_start_ += kIdsInBlock;
    }

    current = current->next;
    current_start += kIdsInBlock;
  }

  return current;
}
}  // namespace memgraph::storage
