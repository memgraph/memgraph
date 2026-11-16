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

#ifdef MG_ENTERPRISE
#include <atomic>
#endif
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

#ifdef MG_ENTERPRISE
  static_assert(std::atomic_ref<uintptr_t>::is_always_lock_free,
                "lock-free PointerPack access requires lock-free atomics");
#endif

  // Enterprise: keeps writes well-defined against the lock-free GetRelaxed reader.
  void Store(uintptr_t value) {
#ifdef MG_ENTERPRISE
    std::atomic_ref<uintptr_t>(storage_).store(value, std::memory_order_relaxed);
#else
    storage_ = value;
#endif
  }

  uintptr_t Load() const {
#ifdef MG_ENTERPRISE
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-const-cast) -- atomic_ref needs a non-const T&; the object is mutable.
    return std::atomic_ref<uintptr_t>(const_cast<uintptr_t &>(storage_)).load(std::memory_order_relaxed);
#else
    return storage_;
#endif
  }

 public:
  PointerPack() = default;

  explicit PointerPack(T *ptr, uintptr_t flags = 0)
      : storage_(reinterpret_cast<uintptr_t>(ptr) | (flags & kFlagsMask)) {}

  T *GetPtr() const { return reinterpret_cast<T *>(storage_ & kPtrMask); }

  /// Write contract (`SetPtr`, `Set`, `operator=(T *)`): the caller must hold the owning object's lock. The only
  /// lock-free writers are the enterprise `Atomic*` helpers (fast-discard cleanup, which runs without the object
  /// lock); the only lock-free reader is `GetRelaxed`.
  void SetPtr(T *ptr) { Store(reinterpret_cast<uintptr_t>(ptr) | (Load() & kFlagsMask)); }

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
    Store((Load() & ~field_mask) | ((value << Pos) & field_mask));
  }

#ifdef MG_ENTERPRISE
  /// Lock-free RMW: sets the bit at @p Pos without losing concurrent updates to other bits or the pointer.
  template <int Pos>
  void AtomicSetBit() {
    static_assert(Pos >= 0 && Pos < NumFlagBits);
    std::atomic_ref<uintptr_t>(storage_).fetch_or(1UL << Pos, std::memory_order_relaxed);
  }

  /// Lock-free RMW: clears the bit at @p Pos without losing concurrent updates to other bits or the pointer.
  template <int Pos>
  void AtomicClearBit() {
    static_assert(Pos >= 0 && Pos < NumFlagBits);
    std::atomic_ref<uintptr_t>(storage_).fetch_and(~(1UL << Pos), std::memory_order_relaxed);
  }

  /// Lock-free replace of the pointer, preserving flag bits set concurrently.
  void AtomicSetPtr(T *ptr) {
    std::atomic_ref<uintptr_t> ref(storage_);
    auto cur = ref.load(std::memory_order_relaxed);
    while (!ref.compare_exchange_weak(
        cur, reinterpret_cast<uintptr_t>(ptr) | (cur & kFlagsMask), std::memory_order_relaxed)) {
    }
  }
#endif

  /// Lock-free read of the bit at @p Pos. Relaxed: a non-torn flag only, no ordering with prior writes.
  /// Safe only because every mutator goes through `atomic_ref` in enterprise builds; community has no lock-free
  /// readers. The other readers (`GetPtr`, `Get<>`) are plain and rely on the owning object's lock; fast-discard's
  /// unlocked clear runs only when no reader can exist.
  template <int Pos>
  bool GetRelaxed() const {
    static_assert(Pos >= 0 && Pos < NumFlagBits);
    return ((Load() >> Pos) & 1UL) != 0;
  }

  operator T *() const { return GetPtr(); }  // NOLINT(google-explicit-constructor)

  PointerPack &operator=(T *ptr) {
    SetPtr(ptr);
    return *this;
  }
};

}  // namespace memgraph::utils
