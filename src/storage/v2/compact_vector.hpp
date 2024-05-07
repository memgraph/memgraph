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

#include <cassert>
#include <cstdint>
#include <memory>
#include <span>
#include <utility>
#include <vector>

namespace memgraph::storage {

namespace {

template <typename T>
struct uninitialised_storage {
  alignas(T) std::byte buf[sizeof(T)];
  auto as() -> T * { return reinterpret_cast<T *>(buf); }
  auto as() const -> T const * { return reinterpret_cast<T const *>(buf); }
};

}  // namespace

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
struct CompactVector {
  using value_type = T;
  using reference = value_type &;
  using const_reference = value_type const &;
  using pointer = value_type *;
  using const_pointer = value_type const *;

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

  CompactVector() = default;

  CompactVector(const CompactVector &other) : size_{other.size_}, capacity_{std::max(other.size_, kSmallCapacity)} {
    // NOTE 1: smallest capacity is kSmallCapacity
    // NOTE 2: upon copy construct we only need enough capacity to satisfy size requirement
    if (!usingSmallBuffer(capacity_)) {
      buffer_ = reinterpret_cast<pointer>(std::aligned_alloc(alignof(T), capacity_ * sizeof(T)));
    }
    std::ranges::uninitialized_copy(other, *this);
  }

  CompactVector(CompactVector &&other) noexcept : size_{other.size_}, capacity_{other.capacity_} {
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

  auto operator=(const CompactVector &other) -> CompactVector & {
    if (std::addressof(other) == this) [[unlikely]] {
      return *this;
    }
    // NOTE : ensure we have enough capacity
    if (capacity_ < other.size_) {
      auto *new_data = reinterpret_cast<pointer>(std::aligned_alloc(alignof(T), other.size_ * sizeof(T)));
      // NOTE: move values to the new buffer
      std::ranges::uninitialized_move(begin(), end(), new_data, new_data + size_);
      std::destroy(begin(), end());
      if (!usingSmallBuffer(capacity_)) {
        std::free(buffer_);
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

  auto operator=(CompactVector &&other) noexcept -> CompactVector & {
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
      if (usingSmallBuffer(capacity_)) {
        std::destroy(begin(), end());
        size_ = std::exchange(other.size_, 0);
        capacity_ = std::exchange(other.capacity_, kSmallCapacity);
        buffer_ = other.buffer_;
      } else {
        std::destroy(begin(), end());
        size_ = std::exchange(other.size_, 0);
        capacity_ = std::exchange(other.capacity_, kSmallCapacity);
        auto old_buffer = std::exchange(buffer_, other.buffer_);
        std::free(old_buffer);
      }
    }
    return *this;
  }

  ~CompactVector() {
    std::destroy(begin(), end());
    if (!usingSmallBuffer(capacity_)) {
      std::free(buffer_);
    }
  }

  explicit CompactVector(std::initializer_list<T> other) : CompactVector(other.begin(), other.end()) {}

  // TODO: change to range + add test
  explicit CompactVector(std::span<T const> other) : CompactVector(other.cbegin(), other.cend()) {}
  // TODO: generalise to not just vector
  explicit CompactVector(std::vector<T> &&other) : size_(other.size()), capacity_{std::max(size_, kSmallCapacity)} {
    if (!usingSmallBuffer(capacity_)) {
      buffer_ = reinterpret_cast<pointer>(std::aligned_alloc(alignof(T), capacity_ * sizeof(T)));
    }
    std::ranges::uninitialized_move(other.begin(), other.end(), begin(), end());
  }

  template <typename It>
  explicit CompactVector(It first, It last)
      : size_(std::distance(first, last)), capacity_{std::max(size_, kSmallCapacity)} {
    if (!usingSmallBuffer(capacity_)) {
      buffer_ = reinterpret_cast<pointer>(std::aligned_alloc(alignof(T), capacity_ * sizeof(T)));
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

  void reserve(uint32_t new_capacity) {
    if (new_capacity <= capacity_) {
      return;
    }

    auto *new_data = reinterpret_cast<pointer>(std::aligned_alloc(alignof(T), new_capacity * sizeof(T)));
    std::uninitialized_move(begin(), end(), new_data);
    std::destroy(begin(), end());
    if (!usingSmallBuffer(capacity_)) {
      std::free(buffer_);
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

  auto begin() -> iterator { return iterator{(usingSmallBuffer(capacity_)) ? small_buffer_->as() : buffer_}; }

  auto end() -> iterator { return begin() + size_; }

  [[nodiscard]] auto begin() const -> const_iterator {
    return const_iterator{(usingSmallBuffer(capacity_)) ? small_buffer_->as() : buffer_};
  }

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

  friend bool operator==(CompactVector const &lhs, CompactVector const &rhs) { return std::ranges::equal(lhs, rhs); }

  constexpr static std::uint32_t kSmallCapacity = sizeof(value_type *) / sizeof(value_type);

 private:
  constexpr static bool usingSmallBuffer(uint32_t capacity) { return capacity == kSmallCapacity; }

  uint32_t size_{};                    // max 4 billion
  uint32_t capacity_{kSmallCapacity};  // max 4 billion
  union {
    value_type *buffer_;
    uninitialised_storage<value_type> small_buffer_[kSmallCapacity];
  };
};

static_assert(sizeof(CompactVector<int>) == 16);

}  // namespace memgraph::storage
