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

#include "storage/v2/commit_log.hpp"
#include "utils/memory.hpp"

#include <mutex>

namespace memgraph::storage {
CommitLog::CommitLog() : allocator_(utils::NewDeleteResource()) {}

CommitLog::CommitLog(uint64_t const oldest_active) : allocator_(utils::NewDeleteResource()) {
  head_ = allocator_.allocate(1);
  allocator_.construct(head_);

  if (oldest_active == 0) {
    next_start_ = kIdsInBlock;
    return;
  }

  // We set -1 because we search for the block where we track finished txns
  auto const last_finished = oldest_active - 1;
  head_start_ = last_finished / kIdsInBlock * kIdsInBlock;
  next_start_ = head_start_ + kIdsInBlock;
  MarkFinishedInRange(0, last_finished);
}

CommitLog::~CommitLog() {
  while (head_) {
    Block *tmp = head_->next;
    head_->~Block();
    allocator_.deallocate(head_, 1);
    head_ = tmp;
  }
}

void CommitLog::MarkFinished(uint64_t const id) {
  auto guard = std::lock_guard{lock_};

  Block *block = FindOrCreateBlock(id);
  block->field[(id % kIdsInBlock) / kIdsInField] |= 1ULL << (id % kIdsInField);
  if (id == oldest_active_) {
    UpdateOldestActive();
  }
}

void CommitLog::MarkFinishedInRange(uint64_t const start_id, uint64_t const end_id) {
  auto guard = std::lock_guard{lock_};

  if (end_id < start_id) {
    return;
  }

  utils::OnScopeExit update_oldest_active{[this, start_id, end_id]() {
    if (start_id <= oldest_active_ && oldest_active_ <= end_id) {
      UpdateOldestActive();
    }
  }};

  // Start info
  auto const start_field_idx = (start_id % kIdsInBlock) / kIdsInField;
  auto const start_idx_in_field = start_id % kIdsInField;

  // End info
  auto const end_field_idx = (end_id % kIdsInBlock) / kIdsInField;
  auto const end_idx_in_field = end_id % kIdsInField;

  // This will also create all intermediate blocks if one of them doesn't exist
  Block *end_block = FindOrCreateBlock(end_id);
  Block *start_block = FindOrCreateBlock(start_id);

  if (start_block == end_block && start_field_idx == end_field_idx) {
    // Shifting by 64 bits means setting whole field as finished
    if (auto const bits_shift = end_idx_in_field - start_idx_in_field + 1; bits_shift == kIdsInField) {
      start_block->field[start_field_idx] = std::numeric_limits<uint64_t>::max();
    } else {
      uint64_t const combined_mask = ((1ULL << bits_shift) - 1) << start_idx_in_field;
      start_block->field[start_field_idx] |= combined_mask;
    }
    return;
  }

  // Apply start mask on start_field_idx
  uint64_t const start_mask = std::numeric_limits<uint64_t>::max() << start_idx_in_field;
  start_block->field[start_field_idx] |= start_mask;

  if (start_block == end_block) {
    // Mark all intermediate fields as finished
    for (auto i = start_field_idx + 1; i < end_field_idx; ++i) {
      start_block->field[i] = std::numeric_limits<uint64_t>::max();
    }
  } else {
    // Mark all fields > start_field_idx as finished
    for (size_t i = start_field_idx + 1; i < kBlockSize; ++i) {
      start_block->field[i] = std::numeric_limits<uint64_t>::max();
    }

    // find block after start
    uint64_t current_start = ((start_id / kIdsInBlock) + 1) * kIdsInBlock;
    Block *current = start_block->next;

    // mark intermediate blocks as finished
    while (current_start + kIdsInBlock < end_id) {
      // Mark whole block as finished
      for (auto i = 0; i < kBlockSize; ++i) {
        current->field[i] = std::numeric_limits<uint64_t>::max();
      }
      current = current->next;
      current_start += kIdsInBlock;
    }

    MG_ASSERT(current == end_block, "Same path leads to two different end blocks in commit log");

    // Mark all fields < end_field_idx as finished
    for (size_t i = 0; i < end_field_idx; ++i) {
      end_block->field[i] = std::numeric_limits<uint64_t>::max();
    }
  }

  // Apply the mask on the end field idx
  if (auto const bits_shift = end_idx_in_field + 1; bits_shift == kIdsInField) {
    end_block->field[end_field_idx] = std::numeric_limits<uint64_t>::max();
  } else {
    uint64_t const end_mask = (1ULL << bits_shift) - 1;
    end_block->field[end_field_idx] |= end_mask;
  }
}

uint64_t CommitLog::OldestActive() {
  auto guard = std::lock_guard{lock_};
  return oldest_active_;
}

bool CommitLog::IsFinished(uint64_t const id) const {
  if (!head_) {
    return false;
  }

  Block *current = head_;
  auto current_start = head_start_;

  while (current_start + kIdsInBlock <= id) {
    current = current->next;
    if (current == nullptr) {
      return false;
    }
    current_start += kIdsInBlock;
  }

  auto const field_idx = (id % kIdsInBlock) / kIdsInField;
  auto const bit_idx = id % kIdsInField;

  return current->field[field_idx] & (1ULL << bit_idx);
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
