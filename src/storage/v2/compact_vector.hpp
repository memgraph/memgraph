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

#include <cstdint>
#include <memory>
#include <span>
#include <utility>
#include <vector>

namespace memgraph::storage {

template <typename T>
struct CompactVector {
  using value_type = T;
  using reference = value_type &;
  using const_reference = value_type const &;
  using pointer = value_type *;
  using const_pointer = value_type const *;

  CompactVector() = default;

  CompactVector(const CompactVector &in) {
    reserve(in.size_);
    std::uninitialized_copy(in.begin(), in.end(), data_);
    size_ = in.size_;
  }

  CompactVector(CompactVector &&in) noexcept {
    if (std::addressof(in) != this) {
      data_ = std::exchange(in.data_, nullptr);
      size_ = std::exchange(in.size_, 0);
      capacity_ = std::exchange(in.capacity_, 0);
    }
  }

  explicit CompactVector(std::initializer_list<T> in) : CompactVector(in.begin(), in.end()) {}

  auto operator=(const CompactVector &in) -> CompactVector & {
    reserve(in.size_);  // TODO: this can cause an unessisary move from old to new buffer
    std::copy(in.cbegin(), in.cbegin() + size_, data_);
    std::uninitialized_copy(in.cbegin() + size_, in.cend(), data_ + size_);
    size_ = in.size_;
    return *this;
  }

  auto operator=(CompactVector &&in) noexcept -> CompactVector & {
    if (std::addressof(in) != this) {
      data_ = std::exchange(in.data_, nullptr);
      size_ = std::exchange(in.size_, 0);
      capacity_ = std::exchange(in.capacity_, 0);
    }
    return *this;
  }

  ~CompactVector() {
    if (data_) {
      std::destroy(begin(), end());
      std::free(reinterpret_cast<void *>(data_));
    }
  }

  // TODO: change to range + add test
  explicit CompactVector(std::span<T const> in) : CompactVector(in.cbegin(), in.cend()) {}
  // TODO: generalise to not just vector
  explicit CompactVector(std::vector<T> &&in)
      : CompactVector(std::move_iterator{in.begin()}, std::move_iterator{in.end()}) {}

  template <typename It>
  explicit CompactVector(It first, It last) {
    auto const size = std::distance(first, last);
    reserve(size);
    std::uninitialized_copy(first, last, data_);
    size_ = size;
  }

  void push_back(const_reference value) {
    if (capacity_ <= size_) {
      reserve(capacity_ == 0 ? 1 : 2 * capacity_);
    }
    std::construct_at(data_ + size_, value);
    size_++;
  }

  void push_back(value_type &&value) {
    if (capacity_ <= size_) {
      reserve(capacity_ == 0 ? 1 : 2 * capacity_);
    }
    std::construct_at(data_ + size_, std::move(value));
    size_++;
  }

  template <typename... Args>
  void emplace_back(Args &&...args) {
    if (capacity_ <= size_) {
      reserve(capacity_ == 0 ? 1 : 2 * capacity_);
    }
    std::construct_at(data_ + size_, std::forward<Args>(args)...);
    ++size_;
  }

  void pop_back() {
    if (!empty()) {
      std::destroy_at(std::addressof(back()));
      --size_;
    }
  }

  auto operator[](uint32_t index) -> reference { return data_[index]; }

  auto operator[](uint32_t index) const -> const_reference { return data_[index]; }

  auto at(uint32_t index) -> reference {
    if (index >= size_) {
      throw std::out_of_range("Index out of range");
    }
    return data_[index];
  }

  [[nodiscard]] auto at(uint32_t index) const -> const_reference {
    if (index >= size_) {
      throw std::out_of_range("Index out of range");
    }
    return data_[index];
  }

  [[nodiscard]] auto size() const -> uint32_t { return size_; }

  [[nodiscard]] auto capacity() const -> uint32_t { return capacity_; }

  void reserve(uint32_t new_capacity) {
    if (new_capacity <= capacity_) {
      return;
    }

    auto *new_data = reinterpret_cast<pointer>(std::aligned_alloc(alignof(T), new_capacity * sizeof(T)));
    if (data_) {
      std::uninitialized_move(begin(), end(), new_data);
      std::destroy(begin(), end());
      std::free(reinterpret_cast<void *>(data_));
    }
    data_ = new_data;
    capacity_ = new_capacity;
  }

  void resize(uint32_t new_size) {
    if (new_size == size_) [[unlikely]] {
      return;
    }
    if (size_ < new_size) {
      reserve(new_size);
      for (auto i = size_; i != new_size; ++i) {
        std::construct_at(data_ + i);
      }
    } else {
      std::destroy(data_ + new_size, data_ + size_);
    }
    size_ = new_size;
  }

  auto back() -> reference { return data_[size_ - 1]; }

  [[nodiscard]] auto back() const -> const_reference { return data_[size_ - 1]; }

  [[nodiscard]] auto empty() const -> bool { return size_ == 0; }

  void clear() {
    std::destroy(begin(), end());
    size_ = 0;
  }

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

  auto begin() -> iterator { return iterator(data_); }

  auto end() -> iterator { return iterator(data_ + size_); }

  [[nodiscard]] auto begin() const -> const_iterator { return const_iterator(data_); }

  [[nodiscard]] auto end() const -> const_iterator { return const_iterator(data_ + size_); }

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
      pop_back();
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

 private:
  T *data_{};
  uint32_t size_{};      // max 4 billion
  uint32_t capacity_{};  // max 4 billion
};

static_assert(sizeof(CompactVector<int>) == 16);

}  // namespace memgraph::storage
