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

#include <cstdint>
#include <span>

namespace memgraph::storage {

// custom datastructure to hold LabelIds
// design goals:
// - 16B, so we are smaller than std::vector<LabelId> (24B)
// - small representation to avoid allocation
// layout:
//  Heap allocation
//  ┌─────────┬─────────┐
//  │SIZE     │CAPACITY │
//  ├─────────┴─────────┤    ┌────┬────┬────┬─
//  │PTR                ├───►│    │    │    │...
//  └───────────────────┘    └────┴────┴────┴─
//  Small representation
//  ┌─────────┬─────────┐
//  │<=2      │2        │
//  ├─────────┼─────────┤
//  │Label1   │Label2   │
//  └─────────┴─────────┘
struct label_set {
  using value_type = LabelId;
  using reference = value_type &;
  using const_reference = value_type const &;
  using size_type = uint32_t;

  using iterator = LabelId *;
  using const_iterator = LabelId const *;

  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  label_set() = default;
  label_set(label_set const &other) : size_{other.size_}, capacity_{std::max(other.size_, kSmallCapacity)} {
    if (capacity_ != kSmallCapacity) [[unlikely]] {
      buffer_ = new LabelId[capacity_];
    }
    std::ranges::copy(other, begin());
  }
  label_set(label_set &&other) noexcept
      : size_{std::exchange(other.size_, 0)}, capacity_{std::exchange(other.capacity_, kSmallCapacity)} {
    if (capacity_ == kSmallCapacity) {
      small_buffer_ = other.small_buffer_;
    } else {
      buffer_ = std::exchange(other.buffer_, nullptr);
    }
  }
  label_set &operator=(label_set const &other) {
    if (this == std::addressof(other)) return *this;
    if (other.size_ <= capacity_) {
      std::ranges::copy(other, begin());
    } else {
      auto new_buffer = new LabelId[other.size_];
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
  label_set &operator=(label_set &&other) noexcept {
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
  ~label_set() {
    if (capacity_ != kSmallCapacity) [[unlikely]] {
      delete[] buffer_;
    }
  }

  explicit label_set(std::span<LabelId const> other)
      : size_(other.size()), capacity_{std::max<size_type>(other.size(), kSmallCapacity)} {
    if (capacity_ != kSmallCapacity) [[unlikely]] {
      buffer_ = new LabelId[capacity_];
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

  void push_back(LabelId id) {
    if (size_ == capacity_) {
      auto new_cap = capacity_ * 2;
      auto new_buffer = new LabelId[new_cap];
      std::ranges::copy(*this, new_buffer);
      if (capacity_ != kSmallCapacity) delete[] buffer_;
      buffer_ = new_buffer;
      capacity_ = new_cap;
    }
    begin()[size_] = id;
    ++size_;
  };
  void emplace_back(LabelId id) { push_back(id); };
  auto back() -> reference { return *rbegin(); };
  auto back() const -> const_reference { return *crbegin(); }
  void pop_back() {
    if (size_) --size_;
  }
  void reserve(size_type new_cap) {
    if (capacity_ < new_cap) {
      auto new_buffer = new LabelId[new_cap];
      std::ranges::copy(*this, new_buffer);
      if (capacity_ != kSmallCapacity) delete[] buffer_;
      buffer_ = new_buffer;
      capacity_ = new_cap;
    }
  }

  auto size() const noexcept -> std::size_t { return size_; };
  auto capacity() const noexcept -> std::size_t { return capacity_; };

  auto operator[](size_t idx) -> reference { return *(begin() + idx); }
  auto operator[](size_t idx) const -> const_reference { return *(begin() + idx); }

 private:
  static constexpr size_type kSmallCapacity = sizeof(LabelId *) / sizeof(LabelId);

  size_type size_ = 0;
  size_type capacity_ = kSmallCapacity;
  union {
    std::array<LabelId, kSmallCapacity> small_buffer_;
    LabelId *buffer_;
  };
};

static_assert(sizeof(label_set) == 16);
static_assert(sizeof(label_set) < sizeof(std::vector<LabelId>));

}  // namespace memgraph::storage
