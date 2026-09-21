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

  /// Atomically replaces the pointer portion of the packed word while preserving all tag bits.
  /// Goes through `std::atomic_ref` (relaxed) so it composes safely with any concurrent
  /// `GetRelaxed` reader that observes the same `storage_` word without holding the object lock.
  /// All writers of `storage_` must still be lock-serialised by the caller; the two-instruction
  /// load+store is not a CAS loop because no concurrent writer can interleave between them.
  void SetPtr(T *ptr) {
    auto ref = std::atomic_ref<uintptr_t>(storage_);
    // Preserve all packed tag bits: read them atomically, splice in the new pointer.
    uintptr_t const flags = ref.load(std::memory_order_relaxed) & kFlagsMask;
    ref.store(reinterpret_cast<uintptr_t>(ptr) | flags, std::memory_order_relaxed);
  }

  /// Extracts the bit field at position @p Pos with @p Size bits.
  /// Always called under the object's own lock (only locked readers use this path), so a plain
  /// non-atomic load is correct: concurrent `GetRelaxed` is also a read, and two reads do not
  /// conflict regardless of whether one is via `atomic_ref`.
  template <int Pos, int Size = 1>
  uintptr_t Get() const {
    static_assert(Pos >= 0 && Size > 0 && Pos + Size <= NumFlagBits);
    return (storage_ >> Pos) & ((1UL << Size) - 1);
  }

  /// Sets the bit field at position @p Pos with @p Size bits to @p value.
  /// Goes through `std::atomic_ref` (relaxed) for the same reason as `SetPtr`: this write to
  /// `storage_` must be expressed atomically so it does not race with a concurrent `GetRelaxed`
  /// load on the same word from a lock-free reader. For single-bit fields (Size == 1), a true
  /// fetch_or/fetch_and RMW is used; for multi-bit fields, a relaxed load+store suffices because
  /// all writers are still lock-serialised by the caller (writer/writer exclusion is the lock's
  /// job, not `atomic_ref`'s). `memory_order_relaxed` mirrors `GetRelaxed`/`SetRelaxed`.
  template <int Pos, int Size = 1>
  void Set(uintptr_t value) {
    static_assert(Pos >= 0 && Size > 0 && Pos + Size <= NumFlagBits);
    const uintptr_t field_mask = ((1UL << Size) - 1) << Pos;
    auto ref = std::atomic_ref<uintptr_t>(storage_);
    if constexpr (Size == 1) {
      // True atomic RMW: preserves every other bit in the word without a CAS loop.
      if ((value & 1UL) != 0UL) {
        ref.fetch_or(field_mask, std::memory_order_relaxed);
      } else {
        ref.fetch_and(~field_mask, std::memory_order_relaxed);
      }
    } else {
      // Multi-bit: lock-serialised writers make a plain load+store equivalent to a CAS loop here.
      uintptr_t const old = ref.load(std::memory_order_relaxed);
      ref.store((old & ~field_mask) | ((value << Pos) & field_mask), std::memory_order_relaxed);
    }
  }

  /// Relaxed-atomic read of the single bit at position @p Pos. Intended for a caller that reads
  /// the flag WITHOUT holding whatever lock guards `Set<Pos>`/`SetPtr` (e.g. a lock-free read
  /// fast-path). `storage_` stays a plain `uintptr_t` (no layout change); ALL writes to it --
  /// `Set`, `SetPtr`, and `SetRelaxed` -- also go through `std::atomic_ref` (relaxed) over the
  /// same `storage_` word, so every access is via an atomic operation and there is no
  /// plain-store-vs-atomic-load UB anywhere on this word. `memory_order_relaxed` is deliberate:
  /// this only needs a non-torn snapshot of the flag bit itself, not a synchronizes-with
  /// relationship to whatever the writer did before setting it -- callers needing the latter must
  /// establish it through their own, separate synchronization.
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

  /// Relaxed-atomic set/clear of the single bit at position @p Pos. Counterpart to `GetRelaxed<Pos>()`:
  /// use this to write a bit that `GetRelaxed` reads without the caller's lock, so BOTH sides go
  /// through `std::atomic_ref` over the same `storage_` word -- a plain (non-atomic) store here,
  /// racing against a concurrent `GetRelaxed` load on another thread, would be the same UB that
  /// `GetRelaxed` exists to avoid on the read side. Implemented as a true read-modify-write
  /// (`fetch_or`/`fetch_and`) so it touches only this bit -- the pointer and every sibling bit in
  /// the packed word are preserved verbatim, never clobbered. `memory_order_relaxed` is deliberate,
  /// mirroring `GetRelaxed`: this only needs a non-torn RMW of the flag bit, not a
  /// synchronizes-with relationship -- writer/writer mutual exclusion (e.g. so two `SetRelaxed`
  /// calls don't race each other) is still the CALLER's responsibility via whatever lock already
  /// serializes writers of this field; this helper only changes how the write is *expressed*
  /// (atomically, on the shared word) so it composes safely with a lock-free `GetRelaxed` reader.
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
