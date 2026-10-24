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

#include <atomic>
#include <cstdint>

namespace memgraph::utils {

/// Stores a pointer of type @p T together with @p NumFlagBits flag bits packed
/// into the low bits of the pointer value. The pointed-to type must be aligned
/// so that the low @p NumFlagBits are naturally zero (e.g. 8-byte alignment
/// gives 3 free bits).
template <typename T, int NumFlagBits>
  requires(alignof(T) >= (1U << NumFlagBits))
class PointerPack {
  static_assert(NumFlagBits > 0 && NumFlagBits <= static_cast<int>(8 * sizeof(uintptr_t)));
  static constexpr uintptr_t kFlagsMask = (1UL << NumFlagBits) - 1;
  static constexpr uintptr_t kPtrMask = ~kFlagsMask;

  uintptr_t storage_{0};

 public:
  PointerPack() = default;

  explicit PointerPack(T *ptr, uintptr_t flags = 0)
      : storage_(reinterpret_cast<uintptr_t>(ptr) | (flags & kFlagsMask)) {}

  T *GetPtr() const { return reinterpret_cast<T *>(storage_ & kPtrMask); }

  /// Writers must hold the caller's lock; `atomic_ref` (relaxed) only keeps the store well-defined
  /// against a lock-free `GetRelaxed` reader.
  void SetPtr(T *ptr) {
    auto ref = std::atomic_ref<uintptr_t>(storage_);
    uintptr_t const flags = ref.load(std::memory_order_relaxed) & kFlagsMask;
    ref.store(reinterpret_cast<uintptr_t>(ptr) | flags, std::memory_order_relaxed);
  }

  /// Extracts the bit field at position @p Pos with @p Size bits.
  /// Plain load: the caller must hold the lock that serialises writers.
  template <int Pos, int Size = 1>
  uintptr_t Get() const {
    static_assert(Pos >= 0 && Size > 0 && Pos + Size <= NumFlagBits);
    return (storage_ >> Pos) & ((1UL << Size) - 1);
  }

  /// Sets the bit field at position @p Pos with @p Size bits to @p value. Writers must hold the caller's lock;
  /// `atomic_ref` is only for lock-free `GetRelaxed` readers, so multi-bit fields use load+store, not RMW.
  template <int Pos, int Size = 1>
  void Set(uintptr_t value) {
    static_assert(Pos >= 0 && Size > 0 && Pos + Size <= NumFlagBits);
    const uintptr_t field_mask = ((1UL << Size) - 1) << Pos;
    auto ref = std::atomic_ref<uintptr_t>(storage_);
    if constexpr (Size == 1) {
      if ((value & 1UL) != 0UL) {
        ref.fetch_or(field_mask, std::memory_order_relaxed);
      } else {
        ref.fetch_and(~field_mask, std::memory_order_relaxed);
      }
    } else {
      uintptr_t const old = ref.load(std::memory_order_relaxed);
      ref.store((old & ~field_mask) | ((value << Pos) & field_mask), std::memory_order_relaxed);
    }
  }

  /// Lock-free read of the bit at @p Pos. Relaxed: a non-torn flag only, no ordering with prior writes.
  /// Safe only because every write to `storage_` (`Set`, `SetPtr`, `SetRelaxed`) goes through `atomic_ref`.
  template <int Pos>
  bool GetRelaxed() const {
    static_assert(Pos >= 0 && Pos < NumFlagBits);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast) -- atomic_ref needs a non-const T&; the object is mutable.
    auto const bits = std::atomic_ref<uintptr_t>(const_cast<uintptr_t &>(storage_)).load(std::memory_order_relaxed);
    return ((bits >> Pos) & 1UL) != 0;
  }

  /// Atomic RMW (`fetch_or`/`fetch_and`) of the bit at @p Pos; pairs with lock-free `GetRelaxed`.
  /// Writer/writer exclusion is still the caller's lock.
  template <int Pos>
  void SetRelaxed(bool value) {
    static_assert(Pos >= 0 && Pos < NumFlagBits);
    constexpr uintptr_t bit_mask = 1UL << Pos;
    auto ref = std::atomic_ref<uintptr_t>(storage_);
    if (value) {
      ref.fetch_or(bit_mask, std::memory_order_relaxed);
    } else {
      ref.fetch_and(~bit_mask, std::memory_order_relaxed);
    }
  }

  operator T *() const { return GetPtr(); }  // NOLINT(google-explicit-constructor)

  PointerPack &operator=(T *ptr) {
    SetPtr(ptr);
    return *this;
  }
};

}  // namespace memgraph::utils
