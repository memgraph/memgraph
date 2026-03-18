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

#include <bit>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <utility>

#include <boost/container/small_vector.hpp>
#include <boost/container_hash/hash.hpp>

namespace memgraph::query::plan::v2 {

/// Bitset over a per-extraction dense variable index.
///
/// Carrier: `small_vector<uint64_t, 2>` (128 bits inline). Bit-position based,
/// EClassId-agnostic; translation lives in `VariableIndex`.
class VariableSet {
 public:
  using block_type = uint64_t;
  static constexpr std::size_t kBitsPerBlock = 64;

  VariableSet() = default;

  // ---- Capacity / queries (context-free) ----

  [[nodiscard]] auto empty() const noexcept -> bool {
    for (auto b : bits_)
      if (b != 0) return false;
    return true;
  }

  [[nodiscard]] auto size() const noexcept -> std::size_t {
    std::size_t n = 0;
    for (auto b : bits_) n += static_cast<std::size_t>(std::popcount(b));
    return n;
  }

  [[nodiscard]] auto test(uint16_t bit) const noexcept -> bool {
    auto const word = static_cast<std::size_t>(bit) / kBitsPerBlock;
    if (word >= bits_.size()) return false;
    return (bits_[word] >> (bit % kBitsPerBlock)) & 1U;
  }

  [[nodiscard]] auto operator==(VariableSet const &other) const noexcept -> bool {
    auto const n = std::max(bits_.size(), other.bits_.size());
    for (std::size_t i = 0; i < n; ++i) {
      auto a = i < bits_.size() ? bits_[i] : 0;
      auto b = i < other.bits_.size() ? other.bits_[i] : 0;
      if (a != b) return false;
    }
    return true;
  }

  [[nodiscard]] auto is_subset_of(VariableSet const &other) const noexcept -> bool {
    for (std::size_t i = 0; i < bits_.size(); ++i) {
      auto const b = i < other.bits_.size() ? other.bits_[i] : 0;
      if (bits_[i] & ~b) return false;
    }
    return true;
  }

  /// Pareto-friendly subset comparison: returns greater if `*this ⊂ other`
  /// (i.e. *this is "smaller" hence dominates under SmallerSubsetIsBetter),
  /// less if `other ⊂ *this`, equivalent if equal, unordered otherwise.
  /// Walks word-by-word with early exit once both subset directions fail.
  [[nodiscard]] auto subset_compare(VariableSet const &other) const noexcept -> std::partial_ordering {
    bool a_sub_b = true;
    bool b_sub_a = true;
    auto const n = std::max(bits_.size(), other.bits_.size());
    for (std::size_t i = 0; i < n; ++i) {
      auto const a = i < bits_.size() ? bits_[i] : 0;
      auto const b = i < other.bits_.size() ? other.bits_[i] : 0;
      if (a & ~b) a_sub_b = false;
      if (b & ~a) b_sub_a = false;
      if (!a_sub_b && !b_sub_a) return std::partial_ordering::unordered;
    }
    if (a_sub_b && b_sub_a) return std::partial_ordering::equivalent;
    if (a_sub_b) return std::partial_ordering::greater;
    return std::partial_ordering::less;
  }

  // ---- Mutation (bit-indexed) ----

  void set(uint16_t bit) {
    auto const word = static_cast<std::size_t>(bit) / kBitsPerBlock;
    if (word >= bits_.size()) bits_.resize(word + 1, 0);
    bits_[word] |= block_type{1} << (bit % kBitsPerBlock);
  }

  void unset(uint16_t bit) noexcept {
    auto const word = static_cast<std::size_t>(bit) / kBitsPerBlock;
    if (word >= bits_.size()) return;
    bits_[word] &= ~(block_type{1} << (bit % kBitsPerBlock));
  }

  // ---- Set algebra (return new VariableSet, *this unchanged) ----

  [[nodiscard]] auto set_union(VariableSet const &other) const -> VariableSet {
    VariableSet out;
    auto const n = std::max(bits_.size(), other.bits_.size());
    out.bits_.resize(n, 0);
    for (std::size_t i = 0; i < n; ++i) {
      auto const a = i < bits_.size() ? bits_[i] : 0;
      auto const b = i < other.bits_.size() ? other.bits_[i] : 0;
      out.bits_[i] = a | b;
    }
    return out;
  }

  [[nodiscard]] auto difference(VariableSet const &other) const -> VariableSet {
    VariableSet out;
    out.bits_.resize(bits_.size(), 0);
    for (std::size_t i = 0; i < bits_.size(); ++i) {
      auto const b = i < other.bits_.size() ? other.bits_[i] : 0;
      out.bits_[i] = bits_[i] & ~b;
    }
    return out;
  }

  [[nodiscard]] auto difference_bit(uint16_t bit) const -> VariableSet {
    VariableSet out = *this;
    out.unset(bit);
    return out;
  }

  [[nodiscard]] auto union_bit(uint16_t bit) const -> VariableSet {
    VariableSet out = *this;
    out.set(bit);
    return out;
  }

  // ---- Iteration: yields bit positions (uint16_t), ascending ----
  //
  // Iterator-of-the-set-bits.  Walks blocks; per-block extracts lowest set bit
  // via std::countr_zero and clears it.  No EClassIds; translation lives in
  // VariableIndex.

  class const_iterator {
   public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = uint16_t;
    using difference_type = std::ptrdiff_t;
    using pointer = void;
    using reference = uint16_t;

    const_iterator() = default;

    const_iterator(VariableSet const *owner, std::size_t word)
        : owner_(owner), word_(word), residual_(word < owner->bits_.size() ? owner->bits_[word] : 0) {
      advance_to_set();
    }

    auto operator*() const -> uint16_t {
      return static_cast<uint16_t>(word_ * kBitsPerBlock + static_cast<std::size_t>(std::countr_zero(residual_)));
    }

    auto operator++() -> const_iterator & {
      residual_ &= residual_ - 1;
      advance_to_set();
      return *this;
    }

    auto operator++(int) -> const_iterator {
      auto it = *this;
      ++*this;
      return it;
    }

    auto operator==(const_iterator const &other) const noexcept -> bool {
      return owner_ == other.owner_ && word_ == other.word_ && residual_ == other.residual_;
    }

    static auto make_end(VariableSet const *owner, std::size_t word) -> const_iterator {
      const_iterator it;
      it.owner_ = owner;
      it.word_ = word;
      it.residual_ = 0;
      return it;
    }

   private:
    void advance_to_set() {
      // Walk forward through zero words until we find a set bit or hit the
      // end.  Constructor and operator++ both call this after seeding /
      // clearing the current word's residual.  At end-of-range we settle on
      // word_ == bits_.size() and residual_ == 0, matching make_end().
      while (residual_ == 0) {
        if (word_ >= owner_->bits_.size()) return;
        ++word_;
        if (word_ >= owner_->bits_.size()) return;
        residual_ = owner_->bits_[word_];
      }
    }

    VariableSet const *owner_ = nullptr;
    std::size_t word_ = 0;
    block_type residual_ = 0;
  };

  [[nodiscard]] auto begin() const noexcept -> const_iterator { return const_iterator{this, 0}; }

  [[nodiscard]] auto end() const noexcept -> const_iterator { return const_iterator::make_end(this, bits_.size()); }

  /// Block-wise hash, ignoring trailing zero blocks so the equality contract
  /// holds: VariableSets with the same set of set bits hash the same regardless
  /// of `bits_` length (e.g. `{0, 0}` and `{0}` are equal and hash equal).
  [[nodiscard]] auto hash() const noexcept -> std::size_t {
    std::size_t last = bits_.size();
    while (last > 0 && bits_[last - 1] == 0) --last;
    std::size_t h = 0;
    for (std::size_t i = 0; i < last; ++i) boost::hash_combine(h, bits_[i]);
    return h;
  }

 private:
  boost::container::small_vector<block_type, 2> bits_;
};

}  // namespace memgraph::query::plan::v2
