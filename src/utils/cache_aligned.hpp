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

#include <array>
#include <atomic>
#include <cstddef>

namespace memgraph::utils {

// Cache line size is typically 64 bytes on x86-64
constexpr size_t CACHE_LINE_SIZE = 64;

// Align to cache line boundary to prevent false sharing
template <typename T>
struct alignas(CACHE_LINE_SIZE) CacheAligned {
  T value;

  CacheAligned() = default;
  explicit CacheAligned(const T &v) : value(v) {}
  explicit CacheAligned(T &&v) : value(std::move(v)) {}

  T &operator*() { return value; }
  const T &operator*() const { return value; }
  T *operator->() { return &value; }
  const T *operator->() const { return &value; }

  T &operator[](size_t index) { return value[index]; }
  const T &operator[](size_t index) const { return value[index]; }

  explicit operator T &() { return value; }
  explicit operator const T &() const { return value; }
};

// Cache-aligned atomic counter to reduce false sharing
struct alignas(CACHE_LINE_SIZE) CacheAlignedAtomic {
  std::atomic<int> value{0};

  int fetch_add(int delta) { return value.fetch_add(delta, std::memory_order_acq_rel); }

  int load() const { return value.load(std::memory_order_acquire); }

  void store(int val) { value.store(val, std::memory_order_release); }

  explicit operator int() const { return load(); }
};

// Cache-aligned array for thread-local data
template <typename T, size_t N>
struct alignas(CACHE_LINE_SIZE) CacheAlignedArray {
  std::array<CacheAligned<T>, N> data;

  T &operator[](size_t index) { return *data[index]; }
  const T &operator[](size_t index) const { return *data[index]; }

  size_t size() const { return N; }
};

// Padding to ensure cache line separation
template <size_t Size = CACHE_LINE_SIZE>
struct alignas(CACHE_LINE_SIZE) CacheLinePadding {
  char padding[Size];
};

}  // namespace memgraph::utils
