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

#include <algorithm>
#include <cstddef>
#include <memory>
#include <span>
#include <type_traits>
#include <utility>
#include <vector>

namespace r = std::ranges;
namespace memgraph::utils {

// datastructure for a fixed capacity vector, design goals:
// - local buffer representation, never allocates a storage buffer
// - limitation only 2^32-1 capacity
// layout:
//  Heap allocation
//  ┌──────────────┐
//  │SIZE          │
//  ├────┬────┬────┤
//  │    │    │... │
//  └────┴────┴────┘
template <typename T, std::size_t ByteLimit>
struct static_vector {
  using value_type = T;
  using reference = value_type &;
  using const_reference = value_type const &;
  using pointer = value_type *;
  using const_pointer = value_type const *;

  using size_type = uint32_t;

  static_assert(sizeof(size_type) + sizeof(T) <= ByteLimit, "Not enough bytes for a single element");

  static constexpr auto N = (ByteLimit - sizeof(size_type)) / sizeof(T);

  struct const_iterator;

  struct iterator {
    friend struct const_iterator;
    using value_type = T;
    using iterator_category = std::contiguous_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type *;
    using reference = value_type &;

    iterator() = default;

    explicit iterator(pointer p) : ptr{p} {}

    auto operator++() -> iterator & {
      ++ptr;
      return *this;
    }

    auto operator++(int) -> iterator {
      auto cpy = *this;
      ++(*this);
      return cpy;
    }

    auto operator--() -> iterator & {
      --ptr;
      return *this;
    }

    auto operator--(int) -> iterator {
      auto cpy = *this;
      --(*this);
      return cpy;
    }

    auto operator+=(difference_type n) -> iterator & {
      ptr += n;
      return *this;
    }

    auto operator-=(difference_type n) -> iterator & { return this->operator+=(-n); }

    auto operator+(const difference_type n) const -> iterator { return iterator(ptr + n); }
    friend auto operator+(const difference_type n, iterator other) -> iterator { return iterator(other.ptr + n); }

    auto operator*() const -> reference { return *ptr; }

    auto operator->() const -> pointer { return ptr; }

    auto operator[](difference_type n) const -> reference { return ptr[n]; }

    friend auto operator==(iterator const &lhs, iterator const &rhs) -> bool { return lhs.ptr == rhs.ptr; }
    friend auto operator<(iterator const &lhs, iterator const &rhs) -> bool { return lhs.ptr < rhs.ptr; }
    friend auto operator>(iterator const &lhs, iterator const &rhs) -> bool { return rhs < lhs; }
    friend auto operator>=(iterator const &lhs, iterator const &rhs) -> bool { return !(lhs < rhs); }
    friend auto operator<=(iterator const &lhs, iterator const &rhs) -> bool { return !(rhs < lhs); }

    auto operator-(const iterator &rv) const -> std::ptrdiff_t { return ptr - rv.ptr; }
    auto operator-(const difference_type n) const -> iterator { return iterator(ptr - n); }

   private:
    value_type *ptr{};
  };

  struct const_iterator {
    using value_type = T const;
    using iterator_category = std::contiguous_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type *;
    using reference = value_type &;

    const_iterator() = default;

    explicit(false) const_iterator(iterator p) : ptr{p.ptr} {}
    explicit const_iterator(pointer p) : ptr{p} {}

    auto operator++() -> const_iterator & {
      ++ptr;
      return *this;
    }

    auto operator++(int) -> const_iterator {
      const_iterator temp = *this;
      ++(*this);
      return temp;
    }

    auto operator--() -> const_iterator & {
      --ptr;
      return *this;
    }

    auto operator--(int) -> const_iterator {
      const_iterator temp = *this;
      --(*this);
      return temp;
    }

    auto operator+=(difference_type n) -> const_iterator & {
      ptr += n;
      return *this;
    }

    auto operator-=(difference_type n) -> const_iterator & { return this->operator+=(-n); }

    auto operator+(const difference_type n) const -> const_iterator { return const_iterator(ptr + n); }
    friend auto operator+(const difference_type n, const_iterator other) -> const_iterator {
      return const_iterator(other.ptr + n);
    }

    auto operator*() const -> reference { return *ptr; }

    auto operator->() const -> pointer { return ptr; }

    auto operator[](difference_type n) const -> reference { return ptr[n]; }

    friend auto operator==(const_iterator const &lhs, const_iterator const &rhs) -> bool { return lhs.ptr == rhs.ptr; }
    friend auto operator<(const_iterator const &lhs, const_iterator const &rhs) -> bool { return lhs.ptr < rhs.ptr; }
    friend auto operator>(const_iterator const &lhs, const_iterator const &rhs) -> bool { return rhs < lhs; }
    friend auto operator>=(const_iterator const &lhs, const_iterator const &rhs) -> bool { return !(lhs < rhs); }
    friend auto operator<=(const_iterator const &lhs, const_iterator const &rhs) -> bool { return !(rhs < lhs); }

    friend auto operator==(iterator const &lhs, const_iterator const &rhs) -> bool {
      return const_iterator{lhs} == rhs;
    }

    auto operator-(const const_iterator &rv) const -> std::ptrdiff_t { return ptr - rv.ptr; }
    auto operator-(const difference_type n) const -> const_iterator { return const_iterator(ptr - n); }

   private:
    value_type *ptr{};
  };

  using reverse_iterator = std::reverse_iterator<iterator>;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;

  static_vector() = default;

  static_vector(const static_vector &other) : size_{other.size_} { r::uninitialized_copy(other, *this); }

  static_vector(static_vector &&other) noexcept : size_{other.size_} {
    r::uninitialized_move(other, *this);
    r::destroy(other);
    other.size_ = 0;
  }

  auto operator=(const static_vector &other) -> static_vector & {
    if (std::addressof(other) == this) [[unlikely]] {
      return *this;
    }

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
      r::uninitialized_copy(src_assign_end, other.cend(), dst_assign_end, dst_ctr_end);
      size_ = other.size_;
    } else if (other.size_ < size_) {
      // destroy
      std::destroy(dst_assign_end, end());
      size_ = other.size_;
    }
    return *this;
  }

  auto operator=(static_vector &&other) noexcept -> static_vector & {
    if (std::addressof(other) == this) [[unlikely]] {
      return *this;
    }

    {
      // move assignment
      auto to_assign = std::min(size_, other.size_);
      auto src_assign_end = other.begin() + to_assign;
      auto dst_assign_end = begin() + to_assign;
      std::move(other.begin(), src_assign_end, begin() /*, dst_assign_end*/);

      if (size_ < other.size_) {
        // move construct
        auto dst_ctr_end = begin() + other.size_;
        r::uninitialized_move(src_assign_end, other.end(), dst_assign_end, dst_ctr_end);
        size_ = other.size_;
      } else if (other.size_ < size_) {
        // destroy
        std::destroy(dst_assign_end, end());
        size_ = other.size_;
      }
    }
    return *this;
  }

  ~static_vector() requires(!std::is_trivially_destructible_v<value_type>) { std::destroy(begin(), end()); }

  ~static_vector() requires(std::is_trivially_destructible_v<value_type>) = default;

  explicit static_vector(std::initializer_list<T> other) : static_vector(other.begin(), other.end()) {}

  // TODO: change to range + add test
  explicit static_vector(std::span<T const> other) : static_vector(other.begin(), other.end()) {}
  // TODO: generalise to not just vector
  explicit static_vector(std::vector<T> &&other) : size_(other.size()) {
    r::uninitialized_move(other.begin(), other.end(), begin(), end());
  }

  template <typename It>
  explicit static_vector(It first, It last) : size_(std::distance(first, last)) {
    r::uninitialized_copy(first, last, begin(), end());
  }

  bool is_full() const { return N == size_; }

  auto push_back(const_reference value) -> reference {
    if (is_full()) {
      throw std::logic_error{"Exceeds fixed capacity"};
    }
    auto *mem = std::to_address(end());
    std::construct_at(mem, value);
    ++size_;
    return *mem;
  }

  auto push_back(value_type &&value) -> reference {
    if (is_full()) {
      throw std::logic_error{"Exceeds fixed capacity"};
    }
    auto *mem = std::to_address(end());
    std::construct_at(mem, std::move(value));
    ++size_;
    return *mem;
  }

  template <typename... Args>
  auto emplace_back(Args &&...args) -> reference {
    if (is_full()) {
      throw std::logic_error{"Exceeds fixed capacity"};
    }
    auto *mem = std::to_address(end());
    std::construct_at(mem, std::forward<Args>(args)...);
    ++size_;
    return *mem;
  }

  void pop_back() {
    if (empty()) [[unlikely]] {
      return;
    }
    std::destroy_at(std::addressof(back()));
    --size_;
  }

  auto operator[](size_type index) -> reference { return begin()[index]; }

  auto operator[](size_type index) const -> const_reference { return begin()[index]; }

  auto at(size_type index) -> reference {
    if (size_ <= index) {
      throw std::out_of_range("Index out of range");
    }
    return begin()[index];
  }

  [[nodiscard]] auto at(size_type index) const -> const_reference {
    if (size_ <= index) {
      throw std::out_of_range("Index out of range");
    }
    return begin()[index];
  }

  [[nodiscard]] auto size() const -> size_type { return size_; }

  [[nodiscard]] static constexpr auto capacity() -> size_type { return N; }

  void resize(size_type new_size) {
    if (size_ < new_size) {
      if (N < new_size) throw std::logic_error{"Exceeds fixed capacity"};
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

  auto begin() -> iterator { return iterator{buffer_->as()}; }

  auto end() -> iterator { return begin() + size_; }

  [[nodiscard]] auto begin() const -> const_iterator { return const_iterator{buffer_->as()}; }

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
    auto it = begin() + std::distance(cbegin(), pos);
    if (it != end()) {
      std::move(std::next(it), end(), it);
      std::destroy_at(std::addressof(back()));
      --size_;
    }
    return it;
  }

  auto erase(const_iterator beg_itr, const_iterator end_itr) -> iterator {
    auto it_first = begin() + std::distance(cbegin(), beg_itr);
    auto it_last = begin() + std::distance(cbegin(), end_itr);

    if (it_first != it_last) {
      auto e = end();
      auto n = std::distance(it_first, it_last);
      std::move(it_last, e, it_first);
      std::destroy(e - n, e);
      size_ -= n;
    }
    return it_first;
  }

  friend bool operator==(static_vector const &lhs, static_vector const &rhs) { return r::equal(lhs, rhs); }
  friend bool operator==(std::span<T const> lhs, static_vector const &rhs) { return r::equal(lhs, rhs); }
  friend bool operator==(static_vector const &lhs, std::span<T const> rhs) { return r::equal(lhs, rhs); }

 private:
  size_type size_{};  // max 4 billion
  uninitialised_storage<value_type> buffer_[N];
};

template <typename T, std::size_t N>
static_vector(std::array<T, N> arr) -> static_vector<T, sizeof(T) * N + sizeof(uint32_t)>;

static_assert(sizeof(static_vector<int, 15>) == 12);

}  // namespace memgraph::utils
