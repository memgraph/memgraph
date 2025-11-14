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

#pragma once

#include <algorithm>
#include <iterator>
#include <mutex>
#include <optional>
#include <type_traits>
#include <utility>

#include "utils/linux.hpp"
#include "utils/spin_lock.hpp"

namespace memgraph::utils {

/// This class implements a stack. It is primarily intended for
/// storing primitive types. Thread-safety can be enabled via the `ThreadSafe`
/// template parameter.
///
/// The stack stores all objects in memory blocks. That makes it really
/// efficient in terms of memory allocations. The memory block size is optimized
/// to be a multiple of the Linux memory page size so that only entire memory
/// pages are used. The constructor has a static_assert to warn you that you are
/// not using a valid `TSize` parameter. The `TSize` parameter should make the
/// `struct Block` a multiple in size of the Linux memory page size.
///
/// This can be calculated using:
/// sizeof(Block *) + sizeof(uint64_t) + sizeof(TObj) * TSize \
///     == k * kLinuxPageSize
///
/// Which translates to:
/// 8 + 8 + sizeof(TObj) * TSize == k * 4096
///
/// @tparam TObj primitive object that should be stored in the stack
/// @tparam TSize size of the memory block used
/// @tparam ThreadSafe if true, the stack will be thread-safe using a spin lock
template <typename TObj, uint64_t TSize, bool ThreadSafe = false>
class Stack {
 private:
  struct Block {
    Block *prev{nullptr};
    uint64_t used{0};
    TObj obj[TSize];
  };
  struct EmptyLock {
    void lock() {}
    void unlock() {}
  };
  struct NoOpDeleter {
    void operator()(const TObj & /*unused*/) const {}
  };

  void PushImpl(TObj obj) {
    if (head_ == nullptr) {
      // Allocate a new block.
      head_ = new Block();
    }
    while (true) {
      MG_ASSERT(head_->used <= TSize,
                "utils::Stack has more elements in a "
                "Block than the block has space!");
      if (head_->used == TSize) {
        // Allocate a new block.
        auto *block = new Block();
        block->prev = head_;
        head_ = block;
      } else {
        head_->obj[head_->used++] = obj;
        break;
      }
    }
  }

  std::optional<TObj> PopImpl() {
    while (true) {
      if (head_ == nullptr) return std::nullopt;
      MG_ASSERT(head_->used <= TSize,
                "utils::Stack has more elements in a "
                "Block than the block has space!");
      if (head_->used == 0) {
        Block *prev = head_->prev;
        delete head_;
        head_ = prev;
      } else {
        return head_->obj[--head_->used];
      }
    }
  }

  template <typename Predicate, typename Deleter = NoOpDeleter>
  void EraseIfImpl(Predicate &&pred, Deleter &&deleter = NoOpDeleter{}) {
    if (head_ == nullptr) return;
    auto partition_point = std::partition(begin(), end(), std::forward<Predicate>(pred));
    for (auto it = begin(); it != partition_point; ++it) {
      std::forward<Deleter>(deleter)(*it);
      PopImpl();
    }
  }

  Block *head_{nullptr};
  [[no_unique_address]] std::conditional_t<ThreadSafe, SpinLock, EmptyLock> lock_;

 public:
  Stack() {
    static_assert(sizeof(Block) % kLinuxPageSize == 0,
                  "It is recommended that you set the TSize constant so that "
                  "the size of Stack::Block is a multiple of the page size.");
  }

  Stack(Stack &&other) noexcept : head_(other.head_) { other.head_ = nullptr; }
  Stack &operator=(Stack &&other) noexcept {
    while (head_ != nullptr) {
      Block *prev = head_->prev;
      delete head_;
      head_ = prev;
    }
    head_ = other.head_;
    other.head_ = nullptr;
    return *this;
  }

  Stack(const Stack &) = delete;
  Stack &operator=(const Stack &) = delete;

  ~Stack() {
    while (head_ != nullptr) {
      Block *prev = head_->prev;
      delete head_;
      head_ = prev;
    }
  }

  void Push(TObj obj) {
    if constexpr (ThreadSafe) {
      auto guard = std::lock_guard{lock_};
      PushImpl(obj);
    } else {
      PushImpl(obj);
    }
  }

  std::optional<TObj> Pop() {
    if constexpr (ThreadSafe) {
      auto guard = std::lock_guard{lock_};
      return PopImpl();
    } else {
      return PopImpl();
    }
  }

  template <typename Predicate>
  auto Partition(Predicate &&pred) {
    return std::partition(begin(), end(), std::forward<Predicate>(pred));
  }

  template <typename Predicate, typename Deleter = NoOpDeleter>
  void EraseIf(Predicate &&pred, Deleter &&deleter = NoOpDeleter{}) {
    if constexpr (ThreadSafe) {
      auto guard = std::lock_guard{lock_};
      EraseIfImpl(std::forward<Predicate>(pred), std::forward<Deleter>(deleter));
    } else {
      EraseIfImpl(std::forward<Predicate>(pred), std::forward<Deleter>(deleter));
    }
  }

  class Iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = TObj;
    using difference_type = std::ptrdiff_t;
    using pointer = TObj *;
    using reference = TObj &;

    Iterator() : block_(nullptr), index_(0) {}

    explicit Iterator(Block *block) : block_(block) {
      while (block_ != nullptr && block_->used == 0) {
        block_ = block_->prev;
      }
      index_ = block_ != nullptr ? block_->used - 1 : 0;
    }

    reference operator*() {
      MG_ASSERT(block_ != nullptr && index_ <= block_->used, "Iterator dereference out of bounds!");
      return block_->obj[index_];
    }

    pointer operator->() { return &(operator*()); }

    Iterator &operator++() {
      if (block_ == nullptr) {
        return *this;
      }

      if (index_ > 0) {
        --index_;
      } else {
        block_ = block_->prev;
        index_ = block_ != nullptr ? block_->used - 1 : 0;
      }

      return *this;
    }

    Iterator operator++(int) {
      Iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    bool operator==(const Iterator &other) const { return block_ == other.block_ && index_ == other.index_; }

    bool operator!=(const Iterator &other) const { return !(*this == other); }

   private:
    Block *block_;
    uint64_t index_;
  };

  Iterator begin() { return Iterator(head_); }

  Iterator end() { return Iterator(); }
};

}  // namespace memgraph::utils
