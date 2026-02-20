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

#include <cstdint>

#include "utils/logging.hpp"

namespace memgraph::utils {

/// Stores a pointer of type @p T together with @p NumFlagBits flag bits packed
/// into the low bits of the pointer value. The pointed-to type must be aligned
/// so that the low @p NumFlagBits are naturally zero (e.g. 8-byte alignment
/// gives 3 free bits).
template <typename T, int NumFlagBits>
class PointerPack {
  static_assert(NumFlagBits > 0 && NumFlagBits <= static_cast<int>(8 * sizeof(uintptr_t)));
  static constexpr uintptr_t kFlagsMask = (1UL << NumFlagBits) - 1;
  static constexpr uintptr_t kPtrMask = ~kFlagsMask;

  uintptr_t storage_{0};

 public:
  PointerPack() = default;

  explicit PointerPack(T *ptr, uintptr_t flags = 0)
      : storage_(reinterpret_cast<uintptr_t>(ptr) | (flags & kFlagsMask)) {
    MG_ASSERT((reinterpret_cast<uintptr_t>(ptr) & kFlagsMask) == 0, "Pointer must be aligned!");
  }

  T *get_ptr() const { return reinterpret_cast<T *>(storage_ & kPtrMask); }

  void set_ptr(T *ptr) {
    MG_ASSERT((reinterpret_cast<uintptr_t>(ptr) & kFlagsMask) == 0, "Pointer must be aligned!");
    storage_ = reinterpret_cast<uintptr_t>(ptr) | (storage_ & kFlagsMask);
  }

  /// Extracts the bit field at position @p Pos with @p Size bits.
  template <int Pos, int Size = 1>
  uintptr_t get() const {
    static_assert(Pos >= 0 && Size > 0 && Pos + Size <= NumFlagBits);
    return (storage_ >> Pos) & ((1UL << Size) - 1);
  }

  /// Sets the bit field at position @p Pos with @p Size bits to @p value.
  template <int Pos, int Size = 1>
  void set(uintptr_t value) {
    static_assert(Pos >= 0 && Size > 0 && Pos + Size <= NumFlagBits);
    const uintptr_t field_mask = ((1UL << Size) - 1) << Pos;
    storage_ = (storage_ & ~field_mask) | ((value << Pos) & field_mask);
  }

  operator T *() const { return get_ptr(); }  // NOLINT(google-explicit-constructor)

  PointerPack &operator=(T *ptr) {
    set_ptr(ptr);
    return *this;
  }
};

}  // namespace memgraph::utils
