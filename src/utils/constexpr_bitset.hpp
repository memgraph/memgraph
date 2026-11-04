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

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string_view>

namespace memgraph::utils {

/// A set of the integers below N, fixed at compile time and written as one
/// character per integer, '1' for a member.
///
/// It exists because libstdc++ does not make `std::bitset`'s constructor from a
/// string of bits `constexpr`, which is the only thing it offers over that
/// type. Replacing it with `std::bitset` runs into exactly that.
///
/// It reads a string the way `std::bitset` does: the leading character is the
/// highest index. A table written for `std::bitset` therefore carries over
/// unchanged, and reading one the other way round would silently change which
/// integers are members, so a caller holding a table of its own wants a test
/// over members it knows.
template <std::size_t N>
  requires(N % std::numeric_limits<std::uint64_t>::digits == 0)
class ConstexprBitset {
  using word_t = std::uint64_t;
  static constexpr std::size_t kWordBits = std::numeric_limits<word_t>::digits;

  /// Packs one word from the characters naming its members, most significant
  /// bit first, which is the order a bit string is written in.
  ///
  /// The word is built in a local and stored once by the caller. Setting bits
  /// straight into the array instead costs enough constant-evaluation steps for
  /// a wide set to exceed the compiler's limit.
  static constexpr word_t PackWord(std::span<char const, kWordBits> bits) {
    word_t word = 0;
    for (char const bit : bits) {
      if (bit != '0' && bit != '1') {
        throw std::invalid_argument{"a bit set takes only '0' and '1'"};
      }
      word = (word << 1U) | static_cast<word_t>(bit == '1');
    }
    return word;
  }

 public:
  /// Anything other than exactly N characters of '0' and '1' is rejected, which
  /// during constant initialization makes it a build failure.
  explicit constexpr ConstexprBitset(std::string_view bits) {
    if (bits.size() != N) {
      throw std::invalid_argument{"a bit set needs one character per bit"};
    }
    // The leading characters name the highest indices, which the last word
    // holds, so the words fill in reverse as the string is read forwards.
    //
    // Chunking the string with a view would say this more directly and costs
    // more constant-evaluation steps than the limit allows.
    std::size_t read = 0;
    for (word_t &word : words_ | std::views::reverse) {
      word = PackWord(std::span<char const, kWordBits>{bits.data() + read, kWordBits});
      read += kWordBits;
    }
  }

  bool operator==(ConstexprBitset const &) const = default;

  /// An index at or above N is guaranteed not to be a member, so a caller may
  /// ask about any value rather than bounding it first.
  [[nodiscard]] constexpr bool Contains(std::size_t index) const {
    return index < N && ((words_[index / kWordBits] >> (index % kWordBits)) & word_t{1}) != 0;
  }

 private:
  std::array<word_t, N / kWordBits> words_{};
};

}  // namespace memgraph::utils
