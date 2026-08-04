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

/// Set membership for ids that are dense and allocated in sequence: the whole id space costs a
/// word per sixty-four ids rather than a node per member, and set/test are constant time with no
/// hashing, which is what a caller adding ids in a hot loop needs.
template <DenseId TId>
class IdBitmap {
 public:
  void set(TId id) {
    auto const [word, bit] = Position(id);
    if (word >= num_words_) {
      words_.resize(word + 1, 0);
      num_words_ = word + 1;
    }
    words_[word] |= bit;
  }

  bool test(TId id) const {
    auto const [word, bit] = Position(id);
    return word < num_words_ && (words_[word] & bit) != 0;
  }

  bool any() const {
    return std::ranges::any_of(words_, [](uint64_t const word) { return word != 0; });
  }

  /// Empties without giving up the memory: a caller refills with much the same ids each time.
  void reset() { std::ranges::fill(words_, 0); }

  IdBitmap &operator|=(IdBitmap const &other) {
    if (num_words_ < other.num_words_) {
      words_.resize(other.num_words_, 0);
      num_words_ = other.num_words_;
    }
    for (size_t i = 0; i != other.num_words_; ++i) words_[i] |= other.words_[i];
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
  /// Held alongside so the bounds check costs a load rather than a subtraction, which set()
  /// pays once per delta. Every write to words_ updates it.
  size_t num_words_{0};
};

}  // namespace memgraph::utils
