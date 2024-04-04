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

#include <algorithm>
#include <compare>
#include <cstddef>
#include <memory>
#include <span>
#include <type_traits>

namespace memgraph::utils {

template <typename T>
struct static_vector_iterator {
  using iterator_category = std::contiguous_iterator_tag;
  using value_type = T;
  using reference = value_type &;
  using pointer = value_type *;
  using difference_type = std::ptrdiff_t;

  static_vector_iterator() = default;
  explicit static_vector_iterator(pointer ptr) : ptr_{ptr} {}
  static_vector_iterator(static_vector_iterator const &) = default;
  auto operator=(static_vector_iterator const &) -> static_vector_iterator & = default;

  auto operator*() const -> reference { return *ptr_; }
  auto operator->() const -> pointer { return ptr_; }
  auto operator[](difference_type n) const -> reference { return ptr_[n]; }

  auto operator++() -> static_vector_iterator & {
    ++ptr_;
    return *this;
  }
  auto operator++(int) -> static_vector_iterator {
    auto cpy = *this;
    ++(*this);
    return cpy;
  }
  auto operator--() -> static_vector_iterator & {
    --ptr_;
    return *this;
  }
  auto operator--(int) -> static_vector_iterator {
    auto cpy = *this;
    --(*this);
    return cpy;
  }
  auto operator+=(difference_type n) -> static_vector_iterator & {
    ptr_ += n;
    return *this;
  }
  auto operator-=(difference_type n) -> static_vector_iterator & {
    ptr_ -= n;
    return *this;
  }
  friend auto operator+(static_vector_iterator const &lhs, difference_type n) -> static_vector_iterator {
    return static_vector_iterator{lhs.ptr_ + n};
  }
  friend auto operator+(difference_type n, static_vector_iterator const &rhs) -> static_vector_iterator {
    return static_vector_iterator{rhs.ptr_ + n};
  }
  friend auto operator-(static_vector_iterator const &lhs, difference_type n) -> static_vector_iterator {
    return static_vector_iterator{lhs.ptr_ - n};
  }

  friend auto operator-(static_vector_iterator const &lhs, static_vector_iterator const &rhs) -> difference_type {
    return lhs.ptr_ - rhs.ptr_;
  }
  friend auto operator<=>(static_vector_iterator const &, static_vector_iterator const &) = default;

 private:
  pointer ptr_{};
};

template <typename T, std::size_t N>
struct static_vector {
  using value_type = T;
  using iterator = static_vector_iterator<T>;
  using const_iterator = static_vector_iterator<T const>;
  static_vector() = default;

  template <std::size_t U>
  explicit static_vector(std::array<T, U> &&arr) noexcept(std::is_nothrow_move_constructible_v<T>) : size_{U} {
    std::uninitialized_move(arr.begin(), arr.end(), begin());
  }

  template <std::size_t U>
  explicit static_vector(std::array<T, U> const &arr) : size_{U} {
    std::uninitialized_copy(arr.begin(), arr.end(), begin());
  }

  static_vector(static_vector const &other) requires(std::is_copy_constructible_v<T>) : size_{other.size_} {
    std::uninitialized_copy(other.begin(), other.end(), begin());
  }

  static_vector(static_vector const &other) requires(!std::is_copy_constructible_v<T>) = delete;

  static_vector &operator=(static_vector const &other) requires(std::is_copy_assignable_v<T>) {
    auto const b = begin();
    auto const ob = other.begin();
    if (other.size_ < size_) {
      std::destroy(b + other.size_, b + size_);
    }
    std::copy(ob, ob + size_, b);
    if (size_ < other.size_) {
      std::uninitialized_copy(ob + size_, ob + other.size_, b + size_);
    }
    size_ = other.size_;
    return *this;
  }

  static_vector &operator=(static_vector const &other) requires(!std::is_copy_assignable_v<T>) = delete;

  auto begin() -> iterator { return iterator{reinterpret_cast<T *>(reinterpret_cast<void *>(&buffer_[0]))}; }
  auto end() -> iterator { return begin() + size_; }

  auto begin() const -> const_iterator {
    return const_iterator{reinterpret_cast<T const *>(reinterpret_cast<void const *>(&buffer_[0]))};
  }
  auto end() const -> const_iterator { return begin() + size_; }

  friend bool operator==(static_vector const &lhs, static_vector const &rhs) { return std::ranges::equal(lhs, rhs); }
  friend bool operator==(std::span<T const> lhs, static_vector const &rhs) { return std::ranges::equal(lhs, rhs); }
  friend bool operator==(static_vector const &lhs, std::span<T const> rhs) { return std::ranges::equal(lhs, rhs); }

  ~static_vector() requires(!std::is_trivially_destructible_v<T>) { std::destroy(begin(), end()); }

  ~static_vector() requires(std::is_trivially_destructible_v<T>) = default;

  auto size() const -> std::size_t { return size_; }
  constexpr auto capacity() const -> std::size_t { return N; }
  auto empty() const -> bool { return size_ == 0; }

  auto operator[](std::size_t ix) -> T & { return *(begin() + ix); }
  auto operator[](std::size_t ix) const -> T const & { return *(begin() + ix); }

  bool is_full() const { return size_ == capacity(); };

  template <typename... Args>
  auto emplace(Args &&...args) -> T & {
    assert(!is_full());
    auto *new_item = &*(begin() + size_);
    std::construct_at(new_item, std::forward<Args>(args)...);
    ++size_;  // increment after sucessful construction
    return *std::launder(new_item);
  }

  static constexpr auto header_size() -> std::size_t { return sizeof(size_); }

 private:
  std::size_t size_ = 0;
  // deliberatly an uninitialised buffer
  alignas(alignof(T)) std::byte buffer_[N * sizeof(T)];
};

template <typename T, std::size_t N>
static_vector(std::array<T, N> arr) -> static_vector<T, N>;

}  // namespace memgraph::utils
