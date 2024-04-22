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

#include <iterator>
#include <vector>

namespace memgraph::storage {

#include <stdexcept>

template <typename T>
class TcoVector {
 private:
  T *data_;
  uint32_t size_;      // max 4 million
  uint32_t capacity_;  // max 4 million

 public:
  TcoVector() : data_(nullptr), size_(0), capacity_(0) {}

  TcoVector(const TcoVector &in) {
    reserve(in.size_);
    memcpy(data_, in.data_, in.size_ * sizeof(T));
    size_ = in.size_;
  }
  TcoVector(TcoVector &&in) : data_{in.data_}, size_{in.size_}, capacity_{in.capacity_} {
    if (&in != this) {
      in.data_ = nullptr;
      in.size_ = 0;
      in.capacity_ = 0;
    }
  }
  TcoVector<T> &operator=(TcoVector<T> &in) {
    reserve(in.size_);
    memcpy(data_, in.data_, in.size_ * sizeof(T));
    size_ = in.size_;
    return *this;
  }
  TcoVector<T> &operator=(TcoVector<T> &&in) {
    if (&in != this) {
      data_ = in.data_;
      size_ = in.size_;
      capacity_ = in.capacity_;
      in.data_ = nullptr;
      in.size_ = 0;
      in.capacity_ = 0;
    }
    return *this;
  }
  ~TcoVector() { delete[] data_; }

  TcoVector(const std::vector<T> &in) {
    reserve(in.size());
    memcpy(data_, in.data(), in.size() * sizeof(T));
    size_ = in.size();
  }

  void push_back(const T &value) {
    if (size_ >= capacity_) {
      reserve(capacity_ == 0 ? 1 : 2 * capacity_);
    }
    data_[size_++] = value;
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
      auto back = std::move(data_[--size_]);
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

    // T *new_data = new T[new_capacity];
    const auto size = ((sizeof(T) + 7) / 8) * 8;
    T *new_data = reinterpret_cast<T *>(new uint8_t[new_capacity * size]);
    memcpy(new_data, data_, size_ * sizeof(T));

    delete[] data_;
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

    T &operator*() const { return *ptr; }

    T *operator->() const { return ptr; }

    bool operator<(const Iterator &other) const { return ptr < other.ptr; }
    bool operator>(const Iterator &other) const { return ptr > other.ptr; }
    bool operator==(const Iterator &other) const { return ptr == other.ptr; }
    bool operator<=(const Iterator &other) const { return ptr <= other.ptr; }
    bool operator>=(const Iterator &other) const { return ptr >= other.ptr; }
    bool operator!=(const Iterator &other) const { return ptr != other.ptr; }

    std::ptrdiff_t operator-(const Iterator &rv) const { return ptr - rv.ptr; }
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

  void erase(Iterator pos) {
    if (pos >= begin() && pos < end()) {
      Iterator next = pos;
      ++next;
      while (next != end()) {
        *pos.ptr = std::move(*next);
        ++pos;
        ++next;
      }
      --size_;
    } else {
      throw std::out_of_range("Iterator out of range");
    }
  }

  void erase(Iterator pos, Iterator end_itr) {
    if (pos < begin() || pos >= end() || end_itr < pos || end_itr > end()) {
      throw std::out_of_range("Iterator out of range");
    }
    for (auto itr = pos; itr < end_itr; ++itr, --end_itr) {
      erase(itr);
    }
  }
};

static_assert(sizeof(TcoVector<int>) == 16);

}  // namespace memgraph::storage
