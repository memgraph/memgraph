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

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace memgraph::dbms {

namespace detail {

// Decodes one strictly well-formed UTF-8 code point (no overlongs, surrogates, or values above U+10FFFF) starting at
// name[pos] and advances pos. Returns false on malformed input.
constexpr bool DecodeUtf8(std::string_view name, std::size_t &pos, char32_t &cp) {
  const auto byte = [&](std::size_t i) { return static_cast<std::uint8_t>(name[i]); };
  const std::uint8_t b0 = byte(pos);
  std::size_t len = 0;
  std::uint8_t lo = 0x80;  // allowed range of the second byte [lo, hi]
  std::uint8_t hi = 0xBF;
  if (b0 < 0x80) {
    cp = b0;
    ++pos;
    return true;
  }
  if (b0 >= 0xC2 && b0 <= 0xDF) {
    len = 2;
    cp = b0 & 0x1FU;
  } else if (b0 >= 0xE0 && b0 <= 0xEF) {
    len = 3;
    cp = b0 & 0x0FU;
    if (b0 == 0xE0) lo = 0xA0;  // overlong
    if (b0 == 0xED) hi = 0x9F;  // surrogates
  } else if (b0 >= 0xF0 && b0 <= 0xF4) {
    len = 4;
    cp = b0 & 0x07U;
    if (b0 == 0xF0) lo = 0x90;  // overlong
    if (b0 == 0xF4) hi = 0x8F;  // > U+10FFFF
  } else {
    return false;
  }
  if (name.size() - pos < len) return false;
  const std::uint8_t b1 = byte(pos + 1);
  if (b1 < lo || b1 > hi) return false;
  cp = (cp << 6) | (b1 & 0x3FU);
  for (std::size_t i = 2; i < len; ++i) {
    const std::uint8_t b = byte(pos + i);
    if ((b & 0xC0U) != 0x80U) return false;
    cp = (cp << 6) | (b & 0x3FU);
  }
  pos += len;
  return true;
}

constexpr bool IsControl(char32_t cp) { return cp <= 0x1F || (cp >= 0x7F && cp <= 0x9F); }

// Whitespace not already covered by IsControl (U+0009-U+000D and U+0085 are controls).
constexpr bool IsSpace(char32_t cp) {
  return cp == 0x20 || cp == 0xA0 || cp == 0x1680 || (cp >= 0x2000 && cp <= 0x200A) || cp == 0x2028 || cp == 0x2029 ||
         cp == 0x202F || cp == 0x205F || cp == 0x3000;
}

}  // namespace detail

inline constexpr std::string_view kInvalidDatabaseNameMessage =
    "Invalid database name: a name must be 1-63 bytes of valid UTF-8, with no control characters and no leading or "
    "trailing whitespace.";

// A name minted locally (CREATE/RENAME) must be 1-63 bytes of valid UTF-8 with no control characters (Unicode Cc) and
// no leading or trailing whitespace. Names an older release persisted are adopted even if they fail this rule.
inline bool DatabaseNameIsWellFormed(std::string_view name) {
  if (name.empty() || name.size() > 63) return false;
  char32_t first = 0;
  char32_t last = 0;
  for (std::size_t pos = 0; pos < name.size();) {
    char32_t cp = 0;
    if (!detail::DecodeUtf8(name, pos, cp) || detail::IsControl(cp)) return false;
    if (first == 0) first = cp;  // NUL is rejected above, so 0 means "not yet set"
    last = cp;
  }
  return !detail::IsSpace(first) && !detail::IsSpace(last);
}

}  // namespace memgraph::dbms
