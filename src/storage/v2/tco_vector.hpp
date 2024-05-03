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

#include <cstddef>
#include <cstdlib>
#include <iterator>
#include <memory>
#include <utility>
#include <vector>

namespace memgraph::storage {

#include <stdexcept>

template <typename T>
class TcoVector {
 private:
  T *data_;
  uint32_t size_;      // max 4 billion
  uint32_t capacity_;  // max 4 billion

 public:
  using value_type = T;

  TcoVector() : data_(nullptr), size_(0), capacity_(0) {}

  TcoVector(const TcoVector &in) : TcoVector() {
    reserve(in.size_);
    std::copy(in.begin(), in.end(), data_);
    size_ = in.size_;
  }

  TcoVector(TcoVector &&in) : data_{in.data_}, size_{in.size_}, capacity_{in.capacity_} {
    if (&in != this) {
      in.data_ = nullptr;
      in.size_ = 0;
      in.capacity_ = 0;
    }
  }

  TcoVector(std::initializer_list<T> in) : TcoVector() {
    reserve(in.size());
    for (auto &v : in) {
      push_back(v);
    }
    size_ = in.size();
  }

  TcoVector<T> &operator=(const TcoVector<T> &in) {
    reserve(in.size_);
    std::copy(in.begin(), in.end(), data_);
    size_ = in.size_;
    return *this;
  }

  TcoVector<T> &operator=(TcoVector<T> &&in) {
    if (&in != this) {
      data_ = std::exchange(in.data_, nullptr);
      size_ = std::exchange(in.size_, 0);
      capacity_ = std::exchange(in.capacity_, 0);
    }
    return *this;
  }

  ~TcoVector() {
    std::destroy(begin(), end());
    std::free(reinterpret_cast<void *>(data_));
  }

  explicit TcoVector(const std::vector<T> &in) {
    reserve(in.size());
    std::copy(in.begin(), in.end(), data_);
    size_ = in.size();
  }

  void push_back(const T &value) {
    if (size_ >= capacity_) {
      reserve(capacity_ == 0 ? 1 : 2 * capacity_);
    }
    std::construct_at(data_ + size_, value);
    size_++;
  }

  template <typename... Args>
  void emplace_back(Args &&...args) {
    if (size_ >= capacity_) {
      reserve(capacity_ == 0 ? 1 : 2 * capacity_);
    }
    new (&data_[size_++]) T{std::forward<Args>(args)...};
  }

  void pop_back() {
    if (size_) {
      // Explicitly ignore return value to avoid warning
      (void)std::move(data_[--size_]);
    }
  }

  T &operator[](unsigned int index) { return data_[index]; }

  const T &operator[](unsigned int index) const { return data_[index]; }

  T &at(unsigned int index) {
    if (index >= size_) {
      throw std::out_of_range("Index out of range");
    }
    return data_[index];
  }

  const T &at(unsigned int index) const {
    if (index >= size_) {
      throw std::out_of_range("Index out of range");
    }
    return data_[index];
  }

  unsigned int size() const { return size_; }

  unsigned int capacity() const { return capacity_; }

  void reserve(unsigned int new_capacity) {
    // TODO Test if >= max uin32_t
    if (new_capacity <= capacity_) {
      return;
    }

    T *new_data = reinterpret_cast<T *>(std::aligned_alloc(alignof(T), new_capacity * sizeof(T)));
    std::uninitialized_move(begin(), end(), new_data);

    std::destroy(begin(), end());
    std::free(reinterpret_cast<void *>(data_));
    data_ = new_data;
    capacity_ = new_capacity;
  }

  void resize(unsigned int new_size) {
    if (new_size > capacity_) {
      reserve(new_size);
    }
    if (new_size > size_) {
      for (unsigned int i = size_; i < new_size; ++i) {
        data_[i] = T();
      }
    }
    size_ = new_size;
  }

  T &back() { return data_[size_ - 1]; }

  const T &back() const { return data_[size_ - 1]; }

  bool empty() const { return size_ == 0; }

  void clear() {
    std::destroy(begin(), end());
    size_ = 0;
  }

  class Iterator {
   private:
    T *ptr;
    friend class TcoVector<T>;

   public:
    using iterator_category = std::contiguous_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = T *;
    using reference = T &;

    Iterator() : ptr(nullptr) {}

    Iterator(T *p) : ptr(p) {}

    Iterator &operator++() {
      ++ptr;
      return *this;
    }

    Iterator operator++(int) {
      Iterator temp = *this;
      ++(*this);
      return temp;
    }

    Iterator &operator--() {
      --ptr;
      return *this;
    }

    Iterator operator--(int) {
      Iterator temp = *this;
      --(*this);
      return temp;
    }

    Iterator operator+=(difference_type n) {
      ptr += n;
      return *this;
    }

    Iterator operator+(const difference_type n) const { return Iterator(ptr + n); }

    T &operator*() const { return *ptr; }

    T *operator->() const { return ptr; }

    bool operator<(const Iterator &other) const { return ptr < other.ptr; }
    bool operator>(const Iterator &other) const { return ptr > other.ptr; }
    bool operator==(const Iterator &other) const { return ptr == other.ptr; }
    bool operator<=(const Iterator &other) const { return ptr <= other.ptr; }
    bool operator>=(const Iterator &other) const { return ptr >= other.ptr; }
    bool operator!=(const Iterator &other) const { return ptr != other.ptr; }

    std::ptrdiff_t operator-(const Iterator &rv) const { return ptr - rv.ptr; }
    Iterator operator-(const difference_type n) const { return Iterator(ptr - n); }
  };

  class ReverseIterator {
   private:
    T *ptr;
    friend class TcoVector<T>;

   public:
    using iterator_category = std::contiguous_iterator_tag;
    using value_type = T;
    using difference_type = std::ptrdiff_t;
    using pointer = T *;
    using reference = T &;

    ReverseIterator(T *p) : ptr(p) {}

    ReverseIterator &operator++() {
      --ptr;
      return *this;
    }

    ReverseIterator operator++(int) {
      ReverseIterator temp = *this;
      ++(*this);
      return temp;
    }

    ReverseIterator &operator--() {
      ++ptr;
      return *this;
    }

    ReverseIterator operator--(int) {
      ReverseIterator temp = *this;
      --(*this);
      return temp;
    }

    T &operator*() const { return *ptr; }

    T *operator->() const { return ptr; }

    bool operator<(const ReverseIterator &other) const { return ptr < other.ptr; }
    bool operator>(const ReverseIterator &other) const { return ptr > other.ptr; }
    bool operator==(const ReverseIterator &other) const { return ptr == other.ptr; }
    bool operator<=(const ReverseIterator &other) const { return ptr <= other.ptr; }
    bool operator>=(const ReverseIterator &other) const { return ptr >= other.ptr; }
    bool operator!=(const ReverseIterator &other) const { return ptr != other.ptr; }

    std::ptrdiff_t operator-(const ReverseIterator &rv) const { return rv.ptr - ptr; }
  };

  Iterator begin() { return Iterator(data_); }

  Iterator end() { return Iterator(data_ + size_); }

  const Iterator begin() const { return Iterator(data_); }

  const Iterator end() const { return Iterator(data_ + size_); }

  const Iterator cbegin() const { return Iterator(data_); }

  const Iterator cend() const { return Iterator(data_ + size_); }

  ReverseIterator rbegin() { return ReverseIterator(data_ + size_ - 1); }

  ReverseIterator rend() { return ReverseIterator(data_ - 1); }

  const ReverseIterator rbegin() const { return ReverseIterator(data_ + size_ - 1); }

  const ReverseIterator rend() const { return ReverseIterator(data_ - 1); }

  Iterator erase(const Iterator pos) {
    if (pos < begin() || pos > end()) {
      throw std::out_of_range("Iterator out of range");
    }
    if (pos == end()) {
      return pos;
    }

    auto it = begin() + std::distance(cbegin(), pos);

    std::move(std::next(it), end(), it);
    pop_back();
    return it;
  }

  Iterator erase(const Iterator beg_itr, const Iterator end_itr) {
    if (beg_itr < begin() || beg_itr > end() || end_itr < beg_itr || end_itr > end()) {
      throw std::out_of_range("Iterator out of range");
    }
    auto it_first = begin() + std::distance(cbegin(), beg_itr);
    auto it_last = begin() + std::distance(cbegin(), end_itr);
    auto e = end();
    auto n = std::distance(it_first, it_last);

    if (it_first != it_last) {
      std::move(it_last, e, it_first);
      std::destroy(e - n, e);
      size_ -= n;
    }
    return it_first;
  }
};

static_assert(sizeof(TcoVector<int>) == 16);

}  // namespace memgraph::storage
