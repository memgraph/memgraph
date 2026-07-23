// Copyright 2026 Memgraph Ltd.
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

#include <cstddef>
#include <iterator>

namespace memgraph::utils {

template <typename T>
struct contiguous_const_iterator;

template <typename T>
struct contiguous_iterator {
  friend struct contiguous_const_iterator<T>;
  using value_type = T;
  using iterator_category = std::contiguous_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using pointer = value_type *;
  using reference = value_type &;

  contiguous_iterator() = default;

  explicit contiguous_iterator(pointer p) : ptr{p} {}

  auto operator++() -> contiguous_iterator & {
    ++ptr;
    return *this;
  }

  auto operator++(int) -> contiguous_iterator {
    auto cpy = *this;
    ++(*this);
    return cpy;
  }

  auto operator--() -> contiguous_iterator & {
    --ptr;
    return *this;
  }

  auto operator--(int) -> contiguous_iterator {
    auto cpy = *this;
    --(*this);
    return cpy;
  }

  auto operator+=(difference_type n) -> contiguous_iterator & {
    ptr += n;
    return *this;
  }

  auto operator-=(difference_type n) -> contiguous_iterator & { return this->operator+=(-n); }

  auto operator+(const difference_type n) const -> contiguous_iterator { return contiguous_iterator(ptr + n); }

  friend auto operator+(const difference_type n, contiguous_iterator other) -> contiguous_iterator {
    return contiguous_iterator(other.ptr + n);
  }

  auto operator*() const -> reference { return *ptr; }

  auto operator->() const -> pointer { return ptr; }

  auto operator[](difference_type n) const -> reference { return ptr[n]; }

  friend auto operator==(contiguous_iterator const &lhs, contiguous_iterator const &rhs) -> bool {
    return lhs.ptr == rhs.ptr;
  }

  friend auto operator<(contiguous_iterator const &lhs, contiguous_iterator const &rhs) -> bool {
    return lhs.ptr < rhs.ptr;
  }

  friend auto operator>(contiguous_iterator const &lhs, contiguous_iterator const &rhs) -> bool { return rhs < lhs; }

  friend auto operator>=(contiguous_iterator const &lhs, contiguous_iterator const &rhs) -> bool {
    return !(lhs < rhs);
  }

  friend auto operator<=(contiguous_iterator const &lhs, contiguous_iterator const &rhs) -> bool {
    return !(rhs < lhs);
  }

  auto operator-(const contiguous_iterator &rv) const -> std::ptrdiff_t { return ptr - rv.ptr; }

  auto operator-(const difference_type n) const -> contiguous_iterator { return contiguous_iterator(ptr - n); }

 private:
  value_type *ptr{};
};

template <typename T>
struct contiguous_const_iterator {
  using value_type = T const;
  using iterator_category = std::contiguous_iterator_tag;
  using difference_type = std::ptrdiff_t;
  using pointer = value_type *;
  using reference = value_type &;

  contiguous_const_iterator() = default;

  explicit(false) contiguous_const_iterator(contiguous_iterator<T> p) : ptr{p.ptr} {}

  explicit contiguous_const_iterator(pointer p) : ptr{p} {}

  auto operator++() -> contiguous_const_iterator & {
    ++ptr;
    return *this;
  }

  auto operator++(int) -> contiguous_const_iterator {
    contiguous_const_iterator temp = *this;
    ++(*this);
    return temp;
  }

  auto operator--() -> contiguous_const_iterator & {
    --ptr;
    return *this;
  }

  auto operator--(int) -> contiguous_const_iterator {
    contiguous_const_iterator temp = *this;
    --(*this);
    return temp;
  }

  auto operator+=(difference_type n) -> contiguous_const_iterator & {
    ptr += n;
    return *this;
  }

  auto operator-=(difference_type n) -> contiguous_const_iterator & { return this->operator+=(-n); }

  auto operator+(const difference_type n) const -> contiguous_const_iterator {
    return contiguous_const_iterator(ptr + n);
  }

  friend auto operator+(const difference_type n, contiguous_const_iterator other) -> contiguous_const_iterator {
    return contiguous_const_iterator(other.ptr + n);
  }

  auto operator*() const -> reference { return *ptr; }

  auto operator->() const -> pointer { return ptr; }

  auto operator[](difference_type n) const -> reference { return ptr[n]; }

  friend auto operator==(contiguous_const_iterator const &lhs, contiguous_const_iterator const &rhs) -> bool {
    return lhs.ptr == rhs.ptr;
  }

  friend auto operator<(contiguous_const_iterator const &lhs, contiguous_const_iterator const &rhs) -> bool {
    return lhs.ptr < rhs.ptr;
  }

  friend auto operator>(contiguous_const_iterator const &lhs, contiguous_const_iterator const &rhs) -> bool {
    return rhs < lhs;
  }

  friend auto operator>=(contiguous_const_iterator const &lhs, contiguous_const_iterator const &rhs) -> bool {
    return !(lhs < rhs);
  }

  friend auto operator<=(contiguous_const_iterator const &lhs, contiguous_const_iterator const &rhs) -> bool {
    return !(rhs < lhs);
  }

  friend auto operator==(contiguous_iterator<T> const &lhs, contiguous_const_iterator const &rhs) -> bool {
    return contiguous_const_iterator{lhs} == rhs;
  }

  auto operator-(const contiguous_const_iterator &rv) const -> std::ptrdiff_t { return ptr - rv.ptr; }

  auto operator-(const difference_type n) const -> contiguous_const_iterator {
    return contiguous_const_iterator(ptr - n);
  }

 private:
  value_type *ptr{};
};

}  // namespace memgraph::utils
