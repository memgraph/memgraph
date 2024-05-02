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

#pragma once

#include "storage/v2/id_types.hpp"

#include <cassert>
#include <cstdint>
#include <span>

namespace memgraph::storage {

// custom datastructure for a small vector
// design goals:
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
  using size_type = uint32_t;

  using iterator = value_type *;
  using const_iterator = value_type const *;

  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  small_vector() = default;
  small_vector(small_vector const &other) : size_{other.size_}, capacity_{std::max(other.size_, kSmallCapacity)} {
    if (capacity_ != kSmallCapacity) [[unlikely]] {
      buffer_ = new value_type[capacity_];
    }
    std::ranges::copy(other, begin());
  }
  small_vector(small_vector &&other) noexcept
      : size_{std::exchange(other.size_, 0)}, capacity_{std::exchange(other.capacity_, kSmallCapacity)} {
    if (capacity_ == kSmallCapacity) {
      small_buffer_ = other.small_buffer_;
    } else {
      buffer_ = std::exchange(other.buffer_, nullptr);
    }
  }
  small_vector &operator=(small_vector const &other) {
    if (this == std::addressof(other)) return *this;
    if (other.size_ <= capacity_) {
      std::ranges::copy(other, begin());
    } else {
      auto new_buffer = new value_type[other.size_];
      std::ranges::copy(other, new_buffer);
      if (capacity_ != kSmallCapacity) [[unlikely]] {
        delete[] buffer_;
      }
      buffer_ = new_buffer;
      capacity_ = other.size_;
    }
    size_ = other.size_;
    return *this;
  }
  small_vector &operator=(small_vector &&other) noexcept {
    if (this == std::addressof(other)) return *this;
    if (capacity_ != kSmallCapacity) [[unlikely]] {
      delete[] buffer_;
    }
    size_ = std::exchange(other.size_, 0);
    capacity_ = std::exchange(other.capacity_, kSmallCapacity);
    if (capacity_ == kSmallCapacity) {
      small_buffer_ = other.small_buffer_;
    } else {
      buffer_ = std::exchange(other.buffer_, nullptr);
    }
    return *this;
  }
  ~small_vector() {
    if (capacity_ != kSmallCapacity) [[unlikely]] {
      delete[] buffer_;
    }
  }

  // construct from a span
  explicit small_vector(std::span<value_type const> other)
      : size_(other.size()), capacity_{std::max<size_type>(other.size(), kSmallCapacity)} {
    if (capacity_ != kSmallCapacity) [[unlikely]] {
      buffer_ = new value_type[capacity_];
    }
    std::ranges::copy(other, begin());
  }

  auto begin() -> iterator { return (capacity_ == kSmallCapacity) ? small_buffer_.begin() : buffer_; }
  auto end() -> iterator { return begin() + size_; }
  auto begin() const -> const_iterator { return (capacity_ == kSmallCapacity) ? small_buffer_.begin() : buffer_; }
  auto end() const -> const_iterator { return begin() + size_; }
  auto cbegin() const -> const_iterator { return begin(); }
  auto cend() const -> const_iterator { return end(); }

  auto rbegin() -> reverse_iterator { return reverse_iterator{end()}; }
  auto rend() -> reverse_iterator { return reverse_iterator{begin()}; }
  auto rbegin() const -> const_reverse_iterator { return const_reverse_iterator{end()}; }
  auto rend() const -> const_reverse_iterator { return const_reverse_iterator{begin()}; }
  auto crbegin() const -> const_reverse_iterator { return const_reverse_iterator{end()}; }
  auto crend() const -> const_reverse_iterator { return const_reverse_iterator{begin()}; }

  void push_back(value_type id) {
    if (size_ == capacity_) {
      auto new_cap = capacity_ * 2;
      auto new_buffer = new value_type[new_cap];
      std::ranges::copy(*this, new_buffer);
      if (capacity_ != kSmallCapacity) delete[] buffer_;
      buffer_ = new_buffer;
      capacity_ = new_cap;
    }
    begin()[size_] = id;
    ++size_;
  }
  void emplace_back(value_type id) { push_back(id); }
  auto back() -> reference { return *rbegin(); }
  auto back() const -> const_reference { return *crbegin(); }
  void pop_back() {
    // C++26 change for contract
    assert(!empty());
    if (size_) --size_;
  }

  iterator erase(const_iterator pos) {
    // C++26 change for contract
    assert(!empty());
    auto it = begin() + std::distance(cbegin(), pos);
    std::move(std::next(it), end(), it);
    pop_back();  // destroy last + reduce size
    return it;
  }
  iterator erase(const_iterator first, const_iterator last) {
    assert(!empty());
    auto it_first = begin() + std::distance(cbegin(), first);
    auto it_last = begin() + std::distance(cbegin(), last);
    auto e = end();
    auto n = std::distance(it_first, it_last);
    if (it_first != it_last) {
      std::move(it_last, e, it_first);
      std::destroy(e - n, e);
      size_ -= n;
    }
    return it_first;
  }

  void reserve(size_type new_cap) {
    if (capacity_ < new_cap) {
      auto new_buffer = new value_type[new_cap];
      std::ranges::copy(*this, new_buffer);
      if (capacity_ != kSmallCapacity) delete[] buffer_;
      buffer_ = new_buffer;
      capacity_ = new_cap;
    }
  }

  auto size() const noexcept -> std::size_t { return size_; }
  auto empty() const noexcept -> bool { return size_ == 0; }
  auto capacity() const noexcept -> std::size_t { return capacity_; }

  auto operator[](size_t idx) -> reference { return *(begin() + idx); }
  auto operator[](size_t idx) const -> const_reference { return *(begin() + idx); }

 private:
  static_assert(std::is_trivial_v<value_type>, "ATM code is implemented assuming trivial types");
  static_assert(sizeof(value_type) <= sizeof(value_type *),
                "Small buffer must be large enough for at least one element");
  static constexpr size_type kSmallCapacity = sizeof(value_type *) / sizeof(value_type);

  size_type size_ = 0;
  size_type capacity_ = kSmallCapacity;
  union {
    std::array<value_type, kSmallCapacity> small_buffer_;
    value_type *buffer_;
  };
};

using label_set = small_vector<LabelId>;
// TODO: extern template instatiation

static_assert(sizeof(label_set) == 16);
static_assert(sizeof(label_set) < sizeof(std::vector<LabelId>));

}  // namespace memgraph::storage
