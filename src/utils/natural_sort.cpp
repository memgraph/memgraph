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

#include "utils/natural_sort.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <iterator>
#include <string>
#include <utility>

#include "utils/logging.hpp"
#include "utils/natural_sort_table.hpp"

namespace memgraph::utils {

namespace {

// Key bytes are chosen so that byte order ranks the parts of a name the way the
// collator does: marks (whitespace, punctuation, symbols) before numbers before
// letters before everything else. A mark is two bytes, kMarkPrefix and its rank.
// A number is kNumberPrefix plus the count of digits in its length, then its
// length, then its digits without leading zeros: self-delimiting, and ordered
// by value whatever its length. Letters are their lower-case ASCII form, and
// characters outside the table keep their UTF-8 bytes (0xC2 and up).
constexpr char kMarkPrefix = 0x01;
constexpr char kNumberPrefix = 0x02;
static_assert(std::size(natural_sort::kMarks) < 0x80, "a rank byte must be valid UTF-8 on its own");

// Not a Unicode code point, so never in the table: a malformed byte is copied as is.
constexpr char32_t kMalformed = 0x110000;

// The code point of the UTF-8 sequence starting at s[pos], and its length in bytes.
std::pair<char32_t, size_t> DecodeUtf8(std::string_view s, size_t pos) {
  auto const lead = static_cast<unsigned char>(s[pos]);
  if (lead < 0x80U) return {lead, 1};
  size_t const length = lead >= 0xF0U ? 4 : lead >= 0xE0U ? 3 : lead >= 0xC0U ? 2 : 1;
  if (length == 1 || pos + length > s.size()) return {kMalformed, 1};
  char32_t code_point = lead & (0x7FU >> length);
  for (size_t i = 1; i < length; ++i) {
    auto const next = static_cast<unsigned char>(s[pos + i]);
    if ((next & 0xC0U) != 0x80U) return {kMalformed, 1};
    code_point = (code_point << 6U) | (next & 0x3FU);
  }
  return {code_point, length};
}

enum class Kind : uint8_t { kKeep, kIgnore, kLetter, kMark };

struct Class {
  Kind kind = Kind::kKeep;
  uint8_t value = 0;  // kMark: rank, from 1. kLetter: index into kFolds.
};

// The class of every code point the table covers, in two dense blocks
// (U+0000-U+036F and General Punctuation), built once from the table.
class Classes {
 public:
  Classes() {
    for (auto const [first, last] : natural_sort::kIgnorable) {
      for (auto code_point = first; code_point <= last; ++code_point) Set(code_point, {.kind = Kind::kIgnore});
    }
    for (size_t i = 0; i < std::size(natural_sort::kMarks); ++i) {
      ForEachCodePoint(natural_sort::kMarks[i], [&](char32_t code_point) {
        Set(code_point, {.kind = Kind::kMark, .value = static_cast<uint8_t>(i + 1)});
      });
    }
    for (size_t i = 0; i < std::size(natural_sort::kFolds); ++i) {
      ForEachCodePoint(natural_sort::kFolds[i].second, [&](char32_t code_point) {
        Set(code_point, {.kind = Kind::kLetter, .value = static_cast<uint8_t>(i)});
      });
    }
  }

  Class Of(char32_t code_point) const {
    if (code_point < kLowEnd) return low_[code_point];
    if (code_point >= kPunctuationFirst && code_point < kPunctuationEnd) {
      return punctuation_[code_point - kPunctuationFirst];
    }
    return {};
  }

 private:
  static constexpr char32_t kLowEnd = 0x370;
  static constexpr char32_t kPunctuationFirst = 0x2000;
  static constexpr char32_t kPunctuationEnd = 0x2070;

  void Set(char32_t code_point, Class value) {
    MG_ASSERT(code_point < kLowEnd || (code_point >= kPunctuationFirst && code_point < kPunctuationEnd),
              "Natural sort table lists U+{:04X} outside its blocks",
              static_cast<uint32_t>(code_point));
    auto &slot = code_point < kLowEnd ? low_[code_point] : punctuation_[code_point - kPunctuationFirst];
    MG_ASSERT(slot.kind == Kind::kKeep, "Natural sort table lists U+{:04X} twice", static_cast<uint32_t>(code_point));
    slot = value;
  }

  template <class Visit>
  static void ForEachCodePoint(std::string_view s, Visit visit) {
    for (size_t pos = 0; pos < s.size();) {
      auto const [code_point, length] = DecodeUtf8(s, pos);
      visit(code_point);
      pos += length;
    }
  }

  std::array<Class, kLowEnd> low_{};
  std::array<Class, kPunctuationEnd - kPunctuationFirst> punctuation_{};
};

Class ClassOf(char32_t code_point) {
  static Classes const classes;
  return classes.Of(code_point);
}

template <class TString>
void AppendNumber(TString *out, std::string_view digits) {
  auto const first = digits.find_first_not_of('0');
  auto const value = first == std::string_view::npos ? std::string_view{"0"} : digits.substr(first);
  auto const length = std::to_string(value.size());
  out->push_back(static_cast<char>(kNumberPrefix + length.size()));
  out->append(length);
  out->append(value);
}

}  // namespace

template <class TAllocator>
std::basic_string<char, std::char_traits<char>, TAllocator> *NaturalSortKey(
    std::basic_string<char, std::char_traits<char>, TAllocator> *out, std::string_view s) {
  out->clear();
  out->reserve(s.size() + 4);
  for (size_t pos = 0; pos < s.size();) {
    auto const c = s[pos];
    if (c >= '0' && c <= '9') {
      auto const end = std::min(s.find_first_not_of("0123456789", pos), s.size());
      AppendNumber(out, s.substr(pos, end - pos));
      pos = end;
    } else if (c >= 'a' && c <= 'z') {
      out->push_back(c);
      ++pos;
    } else if (c >= 'A' && c <= 'Z') {
      out->push_back(static_cast<char>(c - 'A' + 'a'));
      ++pos;
    } else {
      auto const [code_point, length] = DecodeUtf8(s, pos);
      switch (auto const cls = ClassOf(code_point); cls.kind) {
        case Kind::kKeep:
          out->append(s.substr(pos, length));
          break;
        case Kind::kIgnore:
          break;
        case Kind::kLetter:
          out->append(natural_sort::kFolds[cls.value].first);
          break;
        case Kind::kMark:
          out->push_back(kMarkPrefix);
          out->push_back(static_cast<char>(cls.value));
          break;
      }
      pos += length;
    }
  }
  return out;
}

template std::string *NaturalSortKey(std::string *out, std::string_view s);
template pmr::string *NaturalSortKey(pmr::string *out, std::string_view s);

}  // namespace memgraph::utils
