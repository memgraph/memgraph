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

  void SetPtr(T *ptr) { storage_ = reinterpret_cast<uintptr_t>(ptr) | (storage_ & kFlagsMask); }

  /// Extracts the bit field at position @p Pos with @p Size bits.
  template <int Pos, int Size = 1>
  uintptr_t Get() const {
    static_assert(Pos >= 0 && Size > 0 && Pos + Size <= NumFlagBits);
    return (storage_ >> Pos) & ((1UL << Size) - 1);
  }

  /// Sets the bit field at position @p Pos with @p Size bits to @p value.
  template <int Pos, int Size = 1>
  void Set(uintptr_t value) {
    static_assert(Pos >= 0 && Size > 0 && Pos + Size <= NumFlagBits);
    const uintptr_t field_mask = ((1UL << Size) - 1) << Pos;
    storage_ = (storage_ & ~field_mask) | ((value << Pos) & field_mask);
  }

  /// Relaxed-atomic read of the single bit at position @p Pos. Intended for a caller that reads
  /// the flag WITHOUT holding whatever lock guards `Set<Pos>`/`SetPtr` (e.g. a lock-free read
  /// fast-path) -- a plain (non-atomic) load here would be a data race against a concurrent
  /// `Set`/`SetPtr` on another thread. `storage_` itself stays a plain `uintptr_t` (all writers
  /// are still ordinary, lock-serialized stores, see the owning bit's own accessor doc-comment for
  /// which lock); this only changes how THIS read observes it, via `std::atomic_ref` over the same
  /// memory. `memory_order_relaxed` is deliberate: this only needs a non-torn snapshot of the flag
  /// bit itself, not a synchronizes-with relationship to whatever the writer did before setting it
  /// -- callers needing the latter must establish it through their own, separate synchronization.
  template <int Pos>
  bool GetRelaxed() const {
    static_assert(Pos >= 0 && Pos < NumFlagBits);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast) -- std::atomic_ref<T> requires a
    // non-const T&; `storage_` is only const here because this accessor is `const`, not because
    // the referenced object actually is (a sibling `Set<Pos>` on the same, non-const `*this` may
    // run concurrently on another thread -- that possibility is exactly why this function exists).
    auto const bits = std::atomic_ref<uintptr_t>(const_cast<uintptr_t &>(storage_)).load(std::memory_order_relaxed);
    return ((bits >> Pos) & 1UL) != 0;
  }

  operator T *() const { return GetPtr(); }  // NOLINT(google-explicit-constructor)

  PointerPack &operator=(T *ptr) {
    SetPtr(ptr);
    return *this;
  }
};

}  // namespace memgraph::utils
