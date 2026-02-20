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

#include <algorithm>
#include <cstdint>
#include <cstring>

#include "utils/memory_layout.hpp"

namespace memgraph::utils {

class PackedVarintVector {
 public:
  PackedVarintVector() : size_(0), capacity_(kSmallCapacity) {
    if constexpr (kSmallCapacity == 0) buffer_ = nullptr;  // union: avoid indeterminate buffer_
  }

  PackedVarintVector(const PackedVarintVector &other) : size_(other.size_), capacity_(other.capacity_) {
    if (usingSmallBuffer(capacity_)) {
      std::copy(other.data(), other.data() + other.size_, data());
    } else {
      buffer_ = reinterpret_cast<uint8_t *>(operator new(capacity_, std::align_val_t{alignof(uint8_t)}));
      std::copy(other.data(), other.data() + other.size_, buffer_);
    }
  }

  PackedVarintVector &operator=(const PackedVarintVector &other) {
    if (this == &other) return *this;
    if (!usingSmallBuffer(capacity_) && other.size_ <= capacity_) {
      // Reuse existing heap buffer
      std::copy(other.data(), other.data() + other.size_, buffer_);
      size_ = other.size_;
    } else {
      if (!usingSmallBuffer(capacity_)) {
        operator delete(buffer_, std::align_val_t{alignof(uint8_t)});
      }
      size_ = other.size_;
      capacity_ = other.capacity_;
      if (usingSmallBuffer(other.capacity_)) {
        std::copy(other.data(), other.data() + other.size_, data());
      } else {
        buffer_ = reinterpret_cast<uint8_t *>(operator new(capacity_, std::align_val_t{alignof(uint8_t)}));
        std::copy(other.data(), other.data() + other.size_, buffer_);
      }
    }
    return *this;
  }

  PackedVarintVector(PackedVarintVector &&other) noexcept : size_(other.size_), capacity_(other.capacity_) {
    if (usingSmallBuffer(capacity_)) {
      std::copy(other.data(), other.data() + other.size_, data());
    } else {
      buffer_ = other.buffer_;
    }
    other.size_ = 0;
    other.capacity_ = kSmallCapacity;
    if constexpr (kSmallCapacity == 0) other.buffer_ = nullptr;
  }

  PackedVarintVector &operator=(PackedVarintVector &&other) noexcept {
    if (this == &other) return *this;
    if (!usingSmallBuffer(capacity_)) {
      operator delete(buffer_, std::align_val_t{alignof(uint8_t)});
    }
    size_ = other.size_;
    capacity_ = other.capacity_;
    if (usingSmallBuffer(capacity_)) {
      std::copy(other.data(), other.data() + other.size_, data());
    } else {
      buffer_ = other.buffer_;
    }
    other.size_ = 0;
    other.capacity_ = kSmallCapacity;
    if constexpr (kSmallCapacity == 0) other.buffer_ = nullptr;
    return *this;
  }

  ~PackedVarintVector() {
    if (!usingSmallBuffer(capacity_)) {
      operator delete(buffer_, std::align_val_t{alignof(uint8_t)});
    }
  }

  // Push a 32-bit int, packed into 1-5 bytes
  void push_back(uint32_t value) {
    // 1. Determine how many bytes this value needs
    uint8_t needed = 0;
    uint32_t temp = value;
    do {
      temp >>= 7;
      needed++;
    } while (temp > 0);

    // 2. Ensure capacity (handling raw byte capacity)
    if (size_ + needed > capacity_) {
      reserve(std::max(size_ + needed, capacity_ * 2));
    }

    // 3. Encode LEB128
    uint8_t *dst = data() + size_;
    while (value >= 0x80) {
      *dst++ = static_cast<uint8_t>((value & 0x7F) | 0x80);
      value >>= 7;
    }
    *dst = static_cast<uint8_t>(value);

    size_ += needed;
  }

  // To read, we must use an iterator or a visitor because of variable widths
  template <typename Func>
  void for_each(Func &&func) const {
    const uint8_t *ptr = data();
    const uint8_t *end_ptr = data() + size_;
    while (ptr < end_ptr) {
      uint32_t value = 0;
      uint32_t shift = 0;
      uint8_t byte;
      do {
        if (ptr == end_ptr) return;  // malformed: avoid reading past end
        byte = *ptr++;
        value |= (static_cast<uint32_t>(byte & 0x7F) << shift);
        shift += 7;
      } while (byte & 0x80);
      func(value);
    }
  }

  [[nodiscard]] size_t byte_size() const { return size_; }

  [[nodiscard]] bool empty() const { return size_ == 0; }

  void clear() { size_ = 0; }

  [[nodiscard]] uint32_t count() const {
    uint32_t num = 0;
    for_each([&num](uint32_t /* val */) { num++; });
    return num;
  }

  // Forward iterator over decoded uint32_t elements (byte offset-based).
  class iterator;
  class const_iterator;

  class iterator {
   public:
    using value_type = uint32_t;
    using difference_type = std::ptrdiff_t;
    using reference = uint32_t;
    using pointer = void;
    using iterator_category = std::forward_iterator_tag;

    iterator() : vec_(nullptr), offset_(0) {}

    reference operator*() const {
      uint32_t value;
      decode_one(vec_->data() + offset_, vec_->data() + vec_->size_, &value);
      return value;
    }

    iterator &operator++() {
      uint32_t dummy;
      const uint8_t *next = decode_one(vec_->data() + offset_, vec_->data() + vec_->size_, &dummy);
      offset_ = static_cast<uint32_t>(next - vec_->data());
      return *this;
    }

    iterator operator++(int) {
      iterator copy = *this;
      ++*this;
      return copy;
    }

    bool operator==(const iterator &other) const { return vec_ == other.vec_ && offset_ == other.offset_; }

    bool operator!=(const iterator &other) const { return !(*this == other); }

   private:
    friend class PackedVarintVector;

    iterator(PackedVarintVector *vec, uint32_t offset) : vec_(vec), offset_(offset) {}

    PackedVarintVector *vec_;
    uint32_t offset_;
  };

  class const_iterator {
   public:
    using value_type = uint32_t;
    using difference_type = std::ptrdiff_t;
    using reference = uint32_t;
    using pointer = void;
    using iterator_category = std::forward_iterator_tag;

    const_iterator() : vec_(nullptr), offset_(0) {}

    reference operator*() const {
      uint32_t value;
      decode_one(vec_->data() + offset_, vec_->data() + vec_->size_, &value);
      return value;
    }

    const_iterator &operator++() {
      uint32_t dummy;
      const uint8_t *next = decode_one(vec_->data() + offset_, vec_->data() + vec_->size_, &dummy);
      offset_ = static_cast<uint32_t>(next - vec_->data());
      return *this;
    }

    const_iterator operator++(int) {
      const_iterator copy = *this;
      ++*this;
      return copy;
    }

    bool operator==(const const_iterator &other) const { return vec_ == other.vec_ && offset_ == other.offset_; }

    bool operator!=(const const_iterator &other) const { return !(*this == other); }

   private:
    friend class PackedVarintVector;

    const_iterator(const PackedVarintVector *vec, uint32_t offset) : vec_(vec), offset_(offset) {}

    const PackedVarintVector *vec_;
    uint32_t offset_;
  };

  iterator begin() { return iterator(this, 0); }

  iterator end() { return iterator(this, size_); }

  const_iterator begin() const { return const_iterator(this, 0); }

  const_iterator end() const { return const_iterator(this, size_); }

  // Erase the element at the given iterator (must be valid and not end()).
  iterator erase(iterator pos) {
    const uint8_t *ptr = data() + pos.offset_;
    const uint8_t *end_ptr = data() + size_;
    uint32_t value;
    const uint8_t *next = decode_one(ptr, end_ptr, &value);
    auto num_bytes = static_cast<uint32_t>(next - ptr);
    std::memmove(data() + pos.offset_, next, static_cast<size_t>(end_ptr - next));
    size_ -= num_bytes;
    return pos;
  }

 private:
  // Decode one LEB128 value; returns pointer past the decoded value, or end if malformed.
  static const uint8_t *decode_one(const uint8_t *ptr, const uint8_t *end, uint32_t *out_value) {
    *out_value = 0;
    uint32_t shift = 0;
    uint8_t byte;
    do {
      if (ptr == end) return end;
      byte = *ptr++;
      *out_value |= (static_cast<uint32_t>(byte & 0x7F) << shift);
      shift += 7;
    } while (byte & 0x80);
    return ptr;
  }

  auto data() -> uint8_t * { return (capacity_ <= kSmallCapacity) ? small_buffer_.as() : buffer_; }

  auto data() const -> const uint8_t * { return (capacity_ <= kSmallCapacity) ? small_buffer_.as() : buffer_; }

  void reserve(uint32_t new_cap) {
    if (new_cap <= capacity_) return;
    auto *new_data = reinterpret_cast<uint8_t *>(operator new(new_cap, std::align_val_t{alignof(uint8_t)}));
    std::copy(data(), data() + size_, new_data);
    if (capacity_ > kSmallCapacity) {
      operator delete(buffer_, std::align_val_t{alignof(uint8_t)});
    }
    buffer_ = new_data;
    capacity_ = new_cap;
  }

  static constexpr bool usingSmallBuffer(uint32_t cap) { return cap <= kSmallCapacity; }

  static constexpr size_t kSmallCapacity = sizeof(uint8_t *);

  uint32_t size_{0};                   // Current bytes used
  uint32_t capacity_{kSmallCapacity};  // Total bytes available

  union {
    uint8_t *buffer_;
    uninitialised_storage<uint8_t, kSmallCapacity> small_buffer_;
  };

  static_assert(sizeof(small_buffer_) == kSmallCapacity, "kSmallCapacity must be the size of the small buffer");

  friend class iterator;
  friend class const_iterator;
};

}  // namespace memgraph::utils
