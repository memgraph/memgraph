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
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "storage/v2/id_types.hpp"

namespace memgraph::storage {

/// A read-only view of a lock-free register-blocked bloom filter of gids (k=2 bits in one 64-bit word, Fibonacci hash).
/// The owner of the words (LazyGidFilter) inserts through `Locate`, so both sides share the layout.
struct GidBloomProbe {
  struct Slot {
    size_t idx;
    uint64_t bits;
  };

  // Each bit position is a 6-bit field.
  static constexpr unsigned kBitFieldWidth = 6U;
  // Smallest valid index_shift: the two bit fields must fit below the word index.
  static constexpr unsigned kMinIndexShift = 2U * kBitFieldWidth;

  // Fibonacci hash; the word index is the top bits of the product and the two bit positions are the two 6-bit fields
  // directly below it, so the three ranges are disjoint at every size with index_shift >= kMinIndexShift.
  [[nodiscard]] static Slot Locate(uint64_t gid, unsigned index_shift) noexcept {
    uint64_t const h = gid * 0x9E3779B97F4A7C15ULL;
    size_t const idx = h >> index_shift;
    uint64_t const bit_a = 1ULL << ((h >> (index_shift - kBitFieldWidth)) & 63U);
    uint64_t const bit_b = 1ULL << ((h >> (index_shift - kMinIndexShift)) & 63U);
    return {.idx = idx, .bits = bit_a | bit_b};
  }

  [[nodiscard]] static unsigned IndexShift(size_t num_words) noexcept {
    unsigned const shift = 64U - static_cast<unsigned>(std::bit_width(num_words) - 1);
    assert(shift >= kMinIndexShift);
    return shift;
  }

  // A probe of a LazyGidFilter that had no words yet carries `slot`: it re-reads the filter's word pointer, so a scan
  // that began before the first insert still sees later ones.
  [[nodiscard]] bool MayContain(Gid gid) const noexcept {
    auto const *w = words;
    if (w == nullptr) [[unlikely]] {
      if (slot == nullptr) return false;
      w = slot->load(std::memory_order_acquire);
      if (w == nullptr) return false;
    }
    auto const [idx, bits] = Locate(gid.AsUint(), index_shift);
    return (w[idx].load(std::memory_order_acquire) & bits) == bits;
  }

  std::atomic<uint64_t> const *words{nullptr};
  unsigned index_shift{0};
  std::atomic<std::atomic<uint64_t> *> const *slot{nullptr};
};

/// The owner of a GidBloomProbe's words: allocated on the first Insert, so a filter nothing was inserted into holds no
/// memory and answers "absent" after one pointer load. The size is fixed at construction, so readers never see a
/// reallocation. Insert-only and lock-free.
class LazyGidFilter {
 public:
  explicit LazyGidFilter(size_t num_words)
      : num_words_(num_words), index_shift_(GidBloomProbe::IndexShift(num_words)) {}

  LazyGidFilter(const LazyGidFilter &) = delete;
  LazyGidFilter &operator=(const LazyGidFilter &) = delete;

  ~LazyGidFilter() { delete[] words_.load(std::memory_order_relaxed); }

  /// Throws std::bad_alloc from the first Insert, with nothing recorded.
  void Insert(Gid gid) {
    auto *words = words_.load(std::memory_order_acquire);
    if (words == nullptr) words = Allocate();
    auto const [idx, bits] = GidBloomProbe::Locate(gid.AsUint(), index_shift_);
    words[idx].fetch_or(bits, std::memory_order_release);
  }

  [[nodiscard]] bool MayContain(Gid gid) const noexcept {
    auto const *words = words_.load(std::memory_order_acquire);
    if (words == nullptr) return false;
    return GidBloomProbe{.words = words, .index_shift = index_shift_}.MayContain(gid);
  }

  /// The same filter as a read-only view an index scan can test inline.
  [[nodiscard]] GidBloomProbe Probe() const noexcept {
    return {.words = words_.load(std::memory_order_acquire), .index_shift = index_shift_, .slot = &words_};
  }

 private:
  std::atomic<uint64_t> *Allocate() {
    auto fresh = std::make_unique<std::atomic<uint64_t>[]>(num_words_);
    std::atomic<uint64_t> *expected = nullptr;
    if (words_.compare_exchange_strong(expected, fresh.get(), std::memory_order_release, std::memory_order_acquire)) {
      return fresh.release();
    }
    return expected;
  }

  size_t num_words_;
  unsigned index_shift_;
  std::atomic<std::atomic<uint64_t> *> words_{nullptr};
};

}  // namespace memgraph::storage

#endif
