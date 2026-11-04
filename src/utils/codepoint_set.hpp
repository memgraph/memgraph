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

namespace detail {

using codepoint_word_t = std::uint64_t;
inline constexpr std::size_t kCodepointWordBits = std::numeric_limits<codepoint_word_t>::digits;

/// Packs one word from the characters naming its codepoints, most significant
/// bit first, which is the order a bit string is written in.
///
/// The word is built in a local and stored once by the caller. Setting bits
/// straight into the array instead costs enough constant-evaluation steps for a
/// wide set to exceed the compiler's limit.
constexpr codepoint_word_t PackCodepointWord(std::span<char const, kCodepointWordBits> bits) {
  codepoint_word_t word = 0;
  for (char const bit : bits) {
    if (bit != '0' && bit != '1') {
      throw std::invalid_argument{"a codepoint set takes only '0' and '1'"};
    }
    word = (word << 1U) | static_cast<codepoint_word_t>(bit == '1');
  }
  return word;
}

}  // namespace detail

/// A set of the Unicode codepoints below N, fixed at compile time and written
/// as one character per codepoint, '1' for a member.
///
/// It stands in for `std::bitset`, whose constructor from a string of bits
/// libstdc++ does not make `constexpr`, and it reads a string the same way:
/// the leading character is the highest codepoint. A table written for
/// `std::bitset` therefore carries over unchanged, and reading one the other
/// way round would silently change which codepoints are members, so a caller
/// holding a table of its own wants a test over codepoints it knows.
template <std::size_t N>
  requires(N % detail::kCodepointWordBits == 0)
class CodepointSet {
  using word_t = detail::codepoint_word_t;
  static constexpr std::size_t kWordBits = detail::kCodepointWordBits;

 public:
  /// Anything other than exactly N characters of '0' and '1' is rejected, which
  /// during constant initialization makes it a build failure.
  explicit constexpr CodepointSet(std::string_view bits) {
    if (bits.size() != N) {
      throw std::invalid_argument{"a codepoint set needs one character per codepoint"};
    }
    // The leading characters name the highest codepoints, which the last word
    // holds, so the words fill in reverse as the string is read forwards.
    //
    // Chunking the string with a view would say this more directly and costs
    // more constant-evaluation steps than the limit allows.
    std::size_t read = 0;
    for (word_t &word : words_ | std::views::reverse) {
      word = detail::PackCodepointWord(std::span<char const, kWordBits>{bits.data() + read, kWordBits});
      read += kWordBits;
    }
  }

  bool operator==(CodepointSet const &) const = default;

  /// A codepoint at or above N is not a member, so a caller may ask about any
  /// codepoint rather than bounding it first.
  [[nodiscard]] constexpr bool Contains(char32_t codepoint) const {
    auto const pos = static_cast<std::size_t>(codepoint);
    return pos < N && ((words_[pos / kWordBits] >> (pos % kWordBits)) & word_t{1}) != 0;
  }

 private:
  std::array<word_t, N / kWordBits> words_{};
};

}  // namespace memgraph::utils
