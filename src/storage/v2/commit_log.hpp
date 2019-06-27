/// @file commit_log.hpp
#pragma once

#include <cstdint>
#include <mutex>

#include "utils/memory.hpp"
#include "utils/spin_lock.hpp"

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
  CommitLog()
      : head_(nullptr),
        head_start_(0),
        next_start_(0),
        oldest_active_(0),
        allocator_(utils::NewDeleteResource()) {}

  CommitLog(const CommitLog &) = delete;
  CommitLog &operator=(const CommitLog &) = delete;
  CommitLog(CommitLog &&) = delete;
  CommitLog &operator=(CommitLog &&) = delete;

  ~CommitLog() {
    while (head_) {
      Block *tmp = head_->next;
      head_->~Block();
      allocator_.deallocate(head_, 1);
      head_ = tmp;
    }
  }

  /// Mark a transaction as finished.
  void MarkFinished(uint64_t id) {
    std::lock_guard<utils::SpinLock> guard(lock_);

    Block *block = FindOrCreateBlock(id);
    block->field[(id % kIdsInBlock) / kIdsInField] |= 1ULL
                                                      << (id % kIdsInField);
    if (id == oldest_active_) {
      UpdateOldestActive();
    }
  }

  /// Retrieve the oldest transaction still not marked as finished.
  uint64_t OldestActive() {
    std::lock_guard<utils::SpinLock> guard(lock_);
    return oldest_active_;
  }

 private:
  static constexpr uint64_t kBlockSize = 8192;
  static constexpr uint64_t kIdsInField = sizeof(uint64_t) * 8;
  static constexpr uint64_t kIdsInBlock = kBlockSize * kIdsInField;

  struct Block {
    Block *next{nullptr};
    uint64_t field[kBlockSize]{};
  };

  void UpdateOldestActive() {
    while (head_) {
      // This is necessary for amortized constant complexity. If we always start
      // from the 0th field, the amount of steps we make through each block is
      // quadratic in kBlockSize.
      uint64_t start_field = oldest_active_ >= head_start_
                                 ? (oldest_active_ - head_start_) / kIdsInField
                                 : 0;
      for (uint64_t i = start_field; i < kBlockSize; ++i) {
        if (head_->field[i] != std::numeric_limits<uint64_t>::max()) {
          oldest_active_ = head_start_ + i * kIdsInField +
                           __builtin_ffsl(~head_->field[i]) - 1;
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

  Block *FindOrCreateBlock(uint64_t id) {
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

  Block *head_{nullptr};
  uint64_t head_start_{0};
  uint64_t next_start_{0};
  uint64_t oldest_active_{0};
  utils::SpinLock lock_;
  utils::Allocator<Block> allocator_;
};
