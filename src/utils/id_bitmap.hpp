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
#include <concepts>
#include <cstdint>
#include <vector>

namespace memgraph::utils {

template <typename TId>
concept DenseId = requires(TId const id) {
  { id.AsUint() } -> std::convertible_to<uint64_t>;
};

/// Membership of a set of ids that are dense and allocated in sequence, so that the whole id
/// space costs a word per sixty-four ids rather than a node per member. Adding and asking are
/// constant time with no hashing, which is what a caller adding ids in a hot loop needs.
///
/// Typed on the id so that two bitmaps over different kinds of id cannot be confused for each
/// other. Named after std::bitset, which it is the growable counterpart of.
template <DenseId TId>
class IdBitmap {
 public:
  void set(TId id) {
    auto const [word, bit] = Position(id);
    if (word >= words_.size()) words_.resize(word + 1, 0);
    words_[word] |= bit;
  }

  bool test(TId id) const {
    auto const [word, bit] = Position(id);
    return word < words_.size() && (words_[word] & bit) != 0;
  }

  bool any() const {
    return std::ranges::any_of(words_, [](uint64_t const word) { return word != 0; });
  }

  /// Empties the set without giving up the memory. The same ids tend to recur, so a caller that
  /// empties and refills repeatedly would otherwise pay to grow back to the same size each time.
  void reset() { std::ranges::fill(words_, 0); }

  IdBitmap &operator|=(IdBitmap const &other) {
    if (words_.size() < other.words_.size()) words_.resize(other.words_.size(), 0);
    for (size_t i = 0; i != other.words_.size(); ++i) words_[i] |= other.words_[i];
    return *this;
  }

 private:
  static constexpr auto kBitsPerWord = 64U;

  static auto Position(TId id) {
    auto const raw = static_cast<uint64_t>(id.AsUint());

    struct {
      size_t word;
      uint64_t bit;
    } position{.word = static_cast<size_t>(raw / kBitsPerWord), .bit = uint64_t{1} << (raw % kBitsPerWord)};

    return position;
  }

  std::vector<uint64_t> words_;
};

}  // namespace memgraph::utils
