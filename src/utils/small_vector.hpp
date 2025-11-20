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

#include "utils/memory_layout.hpp"

#include <cassert>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>
#include <vector>

namespace memgraph::utils {

// datastructure for a small vector, design goals:
// - 16B, so we are smaller than std::vector<T> (24B)
// - small buffer representation to avoid allocation
// - limitation only 2^32-1 capacity
// layout:
//  Heap allocation
//  ┌─────────┬─────────┐
//  │SIZE     │CAPACITY │
//  ├─────────┴─────────┤    ┌────┬────┬────┬─
//  │PTR                ├───►│    │    │    │...
//  └───────────────────┘    └────┴────┴────┴─
//  Small buffer representation
//   capacity is fixed size, while size <= that, we can use the small buffer
//  ┌─────────┬─────────┐
//  │<=2      │2        │
//  ├─────────┼─────────┤
//  │Val1     │Val2     │
//  └─────────┴─────────┘
template <typename T>
struct small_vector {
  using value_type = T;
  using reference = value_type &;
  using const_reference = value_type const &;
  using pointer = value_type *;
  using const_pointer = value_type const *;

  using iterator = value_type *;
  using const_iterator = value_type const *;

  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  small_vector() {
    // Small buffer optimization is turned off; define buffer_ as nullptr
    if constexpr (!usingSmallBuffer(kSmallCapacity)) {
      buffer_ = nullptr;
    }
  }

  small_vector(const small_vector &other) : size_{other.size_}, capacity_{std::max(other.size_, kSmallCapacity)} {
    // NOTE 1: smallest capacity is kSmallCapacity
    // NOTE 2: upon copy construct we only need enough capacity to satisfy size requirement
    if (!usingSmallBuffer(capacity_)) {
      buffer_ = reinterpret_cast<pointer>(operator new (capacity_ * sizeof(T), std::align_val_t{alignof(T)}));
    }
    std::ranges::uninitialized_copy(other, *this);
  }

  small_vector(small_vector &&other) noexcept : size_{other.size_}, capacity_{other.capacity_} {
    // NOTE 1: moved from vector, new capacity is kSmallCapacity
    // NOTE 2: either move the buffer or move into the small buffer
    if (usingSmallBuffer(other.capacity_)) {
      std::ranges::uninitialized_move(other, *this);
      std::ranges::destroy(other);
    } else {
      buffer_ = std::exchange(other.buffer_, nullptr);
    }
    other.size_ = 0;
    other.capacity_ = kSmallCapacity;
  }

  auto operator=(const small_vector &other) -> small_vector & {
    if (std::addressof(other) == this) [[unlikely]] {
      return *this;
    }
    // NOTE : ensure we have enough capacity
    if (capacity_ < other.size_) {
      auto *new_data = reinterpret_cast<pointer>(operator new (other.size_ * sizeof(T), std::align_val_t{alignof(T)}));
      // NOTE: move values to the new buffer
      std::ranges::uninitialized_move(begin(), end(), new_data, new_data + size_);
      std::destroy(begin(), end());
      if (!usingSmallBuffer(capacity_)) {
        operator delete (buffer_, std::align_val_t{alignof(T)});
      }
      buffer_ = new_data;
      capacity_ = other.size_;
    }
    assert(other.size_ <= capacity_);

    // NOTE: take care
    //       - copy assign over values that exist
    //       - copy construct into uninitialised memory
    //       - destroy unneeded values

    // copy assignment
    auto to_assign = std::min(size_, other.size_);
    auto src_assign_end = other.cbegin() + to_assign;
    auto dst_assign_end = begin() + to_assign;
    std::copy(other.cbegin(), src_assign_end, begin() /*, dst_assign_end*/);

    if (size_ < other.size_) {
      // copy construct
      auto dst_ctr_end = begin() + other.size_;
      std::ranges::uninitialized_copy(src_assign_end, other.cend(), dst_assign_end, dst_ctr_end);
      size_ = other.size_;
    } else if (other.size_ < size_) {
      // destroy
      std::destroy(dst_assign_end, end());
      size_ = other.size_;
    }
    return *this;
  }

  auto operator=(small_vector &&other) noexcept -> small_vector & {
    if (std::addressof(other) == this) [[unlikely]] {
      return *this;
    }

    if (usingSmallBuffer(other.capacity_)) {
      assert(other.capacity_ <= capacity_);
      // move assignment
      auto to_assign = std::min(size_, other.size_);
      auto src_assign_end = other.begin() + to_assign;
      auto dst_assign_end = begin() + to_assign;
      std::move(other.begin(), src_assign_end, begin() /*, dst_assign_end*/);

      if (size_ < other.size_) {
        // move construct
        auto dst_ctr_end = begin() + other.size_;
        std::ranges::uninitialized_move(src_assign_end, other.end(), dst_assign_end, dst_ctr_end);
        size_ = other.size_;
      } else if (other.size_ < size_) {
        // destroy
        std::destroy(dst_assign_end, end());
        size_ = other.size_;
      }
    } else {
      std::destroy(begin(), end());
      if (!usingSmallBuffer(capacity_)) {
        operator delete (buffer_, std::align_val_t{alignof(T)});
      }
      size_ = std::exchange(other.size_, 0);
      capacity_ = std::exchange(other.capacity_, kSmallCapacity);
      buffer_ = std::exchange(other.buffer_, nullptr);
    }
    return *this;
  }

  ~small_vector() {
    std::destroy(begin(), end());
    if (!usingSmallBuffer(capacity_)) {
      operator delete (buffer_, std::align_val_t{alignof(T)});
    }
  }

  explicit small_vector(std::initializer_list<T> other) : small_vector(other.begin(), other.end()) {}

  // TODO: change to range + add test
  explicit small_vector(std::span<T const> other) : small_vector(other.begin(), other.end()) {}
  // TODO: generalise to not just vector
  explicit small_vector(std::vector<T> &&other) : size_(other.size()), capacity_{std::max(size_, kSmallCapacity)} {
    if (!usingSmallBuffer(capacity_)) {
      buffer_ = reinterpret_cast<pointer>(operator new (capacity_ * sizeof(T), std::align_val_t{alignof(T)}));
    }
    std::ranges::uninitialized_move(other.begin(), other.end(), begin(), end());
  }

  template <typename It>
  explicit small_vector(It first, It last)
      : size_(std::distance(first, last)), capacity_{std::max(size_, kSmallCapacity)} {
    if (!usingSmallBuffer(capacity_)) {
      buffer_ = reinterpret_cast<pointer>(operator new (capacity_ * sizeof(T), std::align_val_t{alignof(T)}));
    }
    std::ranges::uninitialized_copy(first, last, begin(), end());
  }

  void push_back(const_reference value) {
    if (capacity_ == size_) {
      reserve(capacity_ == 0 ? 1 : 2 * capacity_);
    }
    std::construct_at(std::to_address(end()), value);
    ++size_;
  }

  void push_back(value_type &&value) {
    if (capacity_ == size_) {
      reserve(capacity_ == 0 ? 1 : 2 * capacity_);
    }
    std::construct_at(std::to_address(end()), std::move(value));
    ++size_;
  }

  template <typename... Args>
  void emplace_back(Args &&...args) {
    if (capacity_ == size_) {
      reserve(capacity_ == 0 ? 1 : 2 * capacity_);
    }
    std::construct_at(std::to_address(end()), std::forward<Args>(args)...);
    ++size_;
  }

  void pop_back() {
    if (empty()) [[unlikely]] {
      return;
    }
    std::destroy_at(std::addressof(back()));
    --size_;
  }

  auto operator[](uint32_t index) -> reference { return begin()[index]; }

  auto operator[](uint32_t index) const -> const_reference { return begin()[index]; }

  auto at(uint32_t index) -> reference {
    if (size_ <= index) {
      throw std::out_of_range("Index out of range");
    }
    return begin()[index];
  }

  [[nodiscard]] auto at(uint32_t index) const -> const_reference {
    if (size_ <= index) {
      throw std::out_of_range("Index out of range");
    }
    return begin()[index];
  }

  [[nodiscard]] auto size() const -> uint32_t { return size_; }

  [[nodiscard]] auto capacity() const -> uint32_t { return capacity_; }

  void shrink_to_fit() {
    if (usingSmallBuffer(capacity_)) return;

    if (size_ == 0) {
      // special case, reset to default small_vector
      operator delete (buffer_, std::align_val_t{alignof(T)});
      capacity_ = kSmallCapacity;
      if constexpr (!usingSmallBuffer(kSmallCapacity)) {
        buffer_ = nullptr;
      }
      return;
    }

    // policy check, is capacity in [size, size*2]
    if (capacity_ <= size_ * 2) return;

    // return to small buffer is possible
    if constexpr (usingSmallBuffer(kSmallCapacity)) {
      if (size_ <= kSmallCapacity) {
        // get information about old buffer
        auto buffer = buffer_;
        auto src_b = begin();
        auto src_e = end();
        // make into small buffer
        capacity_ = kSmallCapacity;
        auto dst_b = begin();
        // move, destroy, delete
        std::uninitialized_move(src_b, src_e, dst_b);
        std::destroy(src_b, src_e);
        operator delete (buffer, std::align_val_t{alignof(T)});
        return;
      }
    }

    pointer new_data;
    try {
      new_data = reinterpret_cast<pointer>(operator new (size_ * sizeof(T), std::align_val_t{alignof(T)}));
    } catch (...) {
      // couldn't allocate smaller buffer... do nothing
      return;
    }

    std::uninitialized_move(begin(), end(), new_data);
    std::destroy(begin(), end());
    operator delete (buffer_, std::align_val_t{alignof(T)});
    buffer_ = new_data;
    capacity_ = size_;
  }

  void reserve(uint32_t new_capacity) {
    if (new_capacity <= capacity_) {
      return;
    }

    auto *new_data = reinterpret_cast<pointer>(operator new (new_capacity * sizeof(T), std::align_val_t{alignof(T)}));
    std::uninitialized_move(begin(), end(), new_data);
    std::destroy(begin(), end());
    if (!usingSmallBuffer(capacity_)) {
      operator delete (buffer_, std::align_val_t{alignof(T)});
    }
    buffer_ = new_data;
    capacity_ = new_capacity;
  }

  void resize(uint32_t new_size) {
    if (size_ < new_size) {
      reserve(new_size);
      auto old_size = std::exchange(size_, new_size);
      for (auto it = begin() + old_size; it != end(); ++it) {
        std::construct_at(std::to_address(it));
      }
    } else if (new_size < size_) {
      auto old_size = std::exchange(size_, new_size);
      std::destroy(begin() + new_size, begin() + old_size);
    }
  }

  auto back() -> reference { return begin()[size_ - 1]; }

  [[nodiscard]] auto back() const -> const_reference { return begin()[size_ - 1]; }

  [[nodiscard]] auto empty() const -> bool { return size_ == 0; }

  void clear() {
    std::destroy(begin(), end());
    size_ = 0;
  }

  [[nodiscard]] auto data() -> pointer { return usingSmallBuffer(capacity_) ? small_buffer_->as() : buffer_; }

  [[nodiscard]] auto data() const -> const_pointer {
    return usingSmallBuffer(capacity_) ? small_buffer_->as() : buffer_;
  }

  auto begin() -> iterator { return data(); }

  auto end() -> iterator { return begin() + size_; }

  [[nodiscard]] auto begin() const -> const_iterator { return data(); }

  [[nodiscard]] auto end() const -> const_iterator { return begin() + size_; }

  [[nodiscard]] auto cbegin() const -> const_iterator { return begin(); }

  [[nodiscard]] auto cend() const -> const_iterator { return end(); }

  auto rbegin() -> reverse_iterator { return reverse_iterator{end()}; }

  auto rend() -> reverse_iterator { return reverse_iterator{begin()}; }

  [[nodiscard]] auto rbegin() const -> const_reverse_iterator { return const_reverse_iterator{cend()}; }

  [[nodiscard]] auto rend() const -> const_reverse_iterator { return const_reverse_iterator{cbegin()}; }

  [[nodiscard]] auto crbegin() const -> const_reverse_iterator { return rbegin(); }

  [[nodiscard]] auto crend() const -> const_reverse_iterator { return rend(); }

  auto erase(const_iterator pos) -> iterator {
    auto it = begin() + (pos - cbegin());
    if (it == end()) return it;
    auto tail = std::move(it + 1, end(), it);
    std::destroy_at(tail);
    --size_;
    return it;
  }

  auto erase(const_iterator beg_itr, const_iterator end_itr) -> iterator {
    auto dst = begin() + (beg_itr - cbegin());
    auto src = begin() + (end_itr - cbegin());
    if (dst == src) return dst;
    auto tail = std::move(src, end(), dst);
    std::destroy(tail, end());
    size_ -= static_cast<uint32_t>(src - dst);
    return dst;
  }

  friend bool operator==(small_vector const &lhs, small_vector const &rhs) { return std::ranges::equal(lhs, rhs); }

  // kSmallCapacity can be 0; in that case we disable the small buffer
  constexpr static std::uint32_t kSmallCapacity = sizeof(value_type *) / sizeof(value_type);

  constexpr bool usingSmallBuffer() { return kSmallCapacity != 0 && capacity_ == kSmallCapacity; }

 private:
  constexpr static bool usingSmallBuffer(uint32_t capacity) {
    return kSmallCapacity != 0 && capacity == kSmallCapacity;
  }

  uint32_t size_{};                    // max 4 billion
  uint32_t capacity_{kSmallCapacity};  // max 4 billion
  union {
    value_type *buffer_;
    uninitialised_storage<value_type, kSmallCapacity ? sizeof(value_type) : 1>
        small_buffer_[kSmallCapacity ? kSmallCapacity : 1];  // This mess is to avoid array with 0 length (ub in c++)
  };
};

static_assert(sizeof(small_vector<int>) == 16);

}  // namespace memgraph::utils
