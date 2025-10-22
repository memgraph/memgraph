// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "query/frontend/parsing.hpp"

#include <cctype>
#include <charconv>
#include <stdexcept>

#include "query/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"

namespace memgraph::query::frontend {

int64_t ParseIntegerLiteral(const std::string &s) {
  try {
    // Not really correct since long long can have a bigger range than int64_t.
    return static_cast<int64_t>(std::stoll(s, 0, 0));
  } catch (const std::out_of_range &) {
    throw SemanticException("Integer literal exceeds 64 bits.");
  }
}

std::string ParseStringLiteral(const std::string &s) {
  // These functions is declared as lambda since its semantics is highly
  // specific for this conxtext and shouldn't be used elsewhere.
  // Encode a Unicode codepoint to UTF-8 bytes
  // Assumes the codepoint is valid (caller must validate, especially for UTF-16 surrogates)
  auto EncodeCodepointToUtf8 = [](uint32_t cp) -> std::string {
    std::string result;
    if (cp <= 0x7F) {
      // 1-byte sequence: 0xxxxxxx
      result += static_cast<char>(cp);
    } else if (cp <= 0x7FF) {
      // 2-byte sequence: 110xxxxx 10xxxxxx
      result += static_cast<char>(0xC0 | ((cp >> 6) & 0x1F));
      result += static_cast<char>(0x80 | (cp & 0x3F));
    } else if (cp <= 0xFFFF) {
      // 3-byte sequence: 1110xxxx 10xxxxxx 10xxxxxx
      result += static_cast<char>(0xE0 | ((cp >> 12) & 0x0F));
      result += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
      result += static_cast<char>(0x80 | (cp & 0x3F));
    } else if (cp <= 0x10FFFF) {
      // 4-byte sequence: 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
      result += static_cast<char>(0xF0 | ((cp >> 18) & 0x07));
      result += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
      result += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
      result += static_cast<char>(0x80 | (cp & 0x3F));
    }
    return result;
  };

  auto EncodeEscapedUnicodeCodepointUtf32 = [&EncodeCodepointToUtf8](const std::string &s, int &i) {
    constexpr int kLongUnicodeLength = 8;
    int j = i + 1;
    while (j < static_cast<int>(s.size()) - 1 && j < i + kLongUnicodeLength + 1 && isxdigit(s[j])) {
      ++j;
    }
    if (j - i == kLongUnicodeLength + 1) {
      uint32_t codepoint = 0;
      const char *start = s.data() + i + 1;
      const char *end = start + kLongUnicodeLength;
      auto [ptr, ec] = std::from_chars(start, end, codepoint, 16);
      if (ec != std::errc{} || ptr != end) {
        throw SemanticException("Invalid UTF codepoint.");
      }
      i += kLongUnicodeLength;
      return EncodeCodepointToUtf8(codepoint);
    }
    throw SyntaxException(
        "Expected 8 hex digits as unicode codepoint started with \\U. "
        "Use \\u for 4 hex digits format.");
  };
  auto EncodeEscapedUnicodeCodepointUtf16 = [&EncodeCodepointToUtf8](const std::string &s, int &i) {
    constexpr int kShortUnicodeLength = 4;

    // Check if a UTF-16 code unit is a high surrogate (0xD800-0xDBFF)
    auto IsHighSurrogate = [](uint16_t unit) { return unit >= 0xD800 && unit <= 0xDBFF; };

    // Check if a UTF-16 code unit is a low surrogate (0xDC00-0xDFFF)
    auto IsLowSurrogate = [](uint16_t unit) { return unit >= 0xDC00 && unit <= 0xDFFF; };

    // Parse a UTF-16 code unit (4 hex digits) from the string at the given position
    auto ParseUtf16Unit = [&s](int pos) -> uint16_t {
      uint16_t unit = 0;
      const char *start = s.data() + pos;
      const char *end = start + kShortUnicodeLength;
      auto [ptr, ec] = std::from_chars(start, end, unit, 16);
      if (ec != std::errc{} || ptr != end) {
        throw SemanticException("Invalid UTF codepoint.");
      }
      return unit;
    };

    int j = i + 1;
    while (j < static_cast<int>(s.size()) - 1 && j < i + kShortUnicodeLength + 1 && isxdigit(s[j])) {
      ++j;
    }
    if (j - i >= kShortUnicodeLength + 1) {
      const uint16_t first_unit = ParseUtf16Unit(i + 1);

      if (IsHighSurrogate(first_unit)) {
        // High surrogate - expect a low surrogate to follow
        j = i + kShortUnicodeLength + 1;
        if (j >= static_cast<int>(s.size()) - 1 || s[j] != '\\') {
          throw SemanticException("Invalid UTF codepoint.");
        }
        ++j;
        if (j >= static_cast<int>(s.size()) - 1 || (s[j] != 'u' && s[j] != 'U')) {
          throw SemanticException("Invalid UTF codepoint.");
        }
        ++j;
        int k = j;
        while (k < static_cast<int>(s.size()) - 1 && k < j + kShortUnicodeLength && isxdigit(s[k])) {
          ++k;
        }
        if (k != j + kShortUnicodeLength) {
          throw SemanticException("Invalid UTF codepoint.");
        }

        const uint16_t second_unit = ParseUtf16Unit(j);

        // Convert UTF-16 surrogate pair to Unicode codepoint
        // Formula: 0x10000 + ((high & 0x3FF) << 10) | (low & 0x3FF)
        const uint32_t codepoint = 0x10000 + (((first_unit & 0x3FF) << 10) | (second_unit & 0x3FF));
        i += kShortUnicodeLength + 2 + kShortUnicodeLength;
        return EncodeCodepointToUtf8(codepoint);
      } else if (IsLowSurrogate(first_unit)) {
        // Low surrogate appearing alone - invalid
        throw SemanticException("Invalid UTF codepoint.");
      } else {
        // Single UTF-16 code unit (not a surrogate), encode directly to UTF-8
        i += kShortUnicodeLength;
        return EncodeCodepointToUtf8(first_unit);
      }
    }
    throw SyntaxException(
        "Expected 4 hex digits as unicode codepoint started with \\u. "
        "Use \\U for 8 hex digits format.");
  };

  std::string unescaped;
  bool escape = false;

  // First and last char is quote, we don't need to look at them.
  for (int i = 1; i < static_cast<int>(s.size()) - 1; ++i) {
    if (escape) {
      switch (s[i]) {
        case '\\':
          unescaped += '\\';
          break;
        case '\'':
          unescaped += '\'';
          break;
        case '"':
          unescaped += '"';
          break;
        case 'B':
        case 'b':
          unescaped += '\b';
          break;
        case 'F':
        case 'f':
          unescaped += '\f';
          break;
        case 'N':
        case 'n':
          unescaped += '\n';
          break;
        case 'R':
        case 'r':
          unescaped += '\r';
          break;
        case 'T':
        case 't':
          unescaped += '\t';
          break;
        case 'U':
          try {
            unescaped += EncodeEscapedUnicodeCodepointUtf32(s, i);
          } catch (const std::range_error &) {
            throw SemanticException("Invalid UTF codepoint.");
          }
          break;
        case 'u':
          try {
            unescaped += EncodeEscapedUnicodeCodepointUtf16(s, i);
          } catch (const std::range_error &) {
            throw SemanticException("Invalid UTF codepoint.");
          }
          break;
        default:
          // This should never happen, except grammar changes and we don't
          // notice change in this production.
          DLOG_FATAL("can't happen");
          throw std::exception();
      }
      escape = false;
    } else if (s[i] == '\\') {
      escape = true;
    } else {
      unescaped += s[i];
    }
  }
  return unescaped;
}

double ParseDoubleLiteral(const std::string &s) {
  try {
    return utils::ParseDouble(s);
  } catch (const utils::BasicException &) {
    throw SemanticException("Couldn't parse string to double.");
  }
}

std::string ParseParameter(const std::string &s) {
  DMG_ASSERT(s[0] == '$', "Invalid string passed as parameter name");
  if (s[1] != '`') return s.substr(1);
  // If parameter name is escaped symbolic name then symbolic name should be
  // unescaped and leading and trailing backquote should be removed.
  DMG_ASSERT(s.size() > 3U && s.back() == '`', "Invalid string passed as parameter name");
  std::string out;
  for (int i = 2; i < static_cast<int>(s.size()) - 1; ++i) {
    if (s[i] == '`') {
      ++i;
    }
    out.push_back(s[i]);
  }
  return out;
}

}  // namespace memgraph::query::frontend
