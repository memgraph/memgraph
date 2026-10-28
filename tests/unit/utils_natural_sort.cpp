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

#include <algorithm>
#include <random>
#include <string>
#include <vector>

#include "gtest/gtest.h"

#include "utils/natural_sort.hpp"
#include "utils/natural_sort_table.hpp"
#include "utils/string.hpp"

namespace {

std::string Key(std::string_view s) {
  std::string out;
  memgraph::utils::NaturalSortKey(&out, s);
  return out;
}

std::string Utf8(char32_t code_point) {
  std::string out;
  if (code_point < 0x80) {
    out += static_cast<char>(code_point);
  } else if (code_point < 0x800) {
    out += static_cast<char>(0xC0 | (code_point >> 6));
    out += static_cast<char>(0x80 | (code_point & 0x3F));
  } else {
    out += static_cast<char>(0xE0 | (code_point >> 12));
    out += static_cast<char>(0x80 | ((code_point >> 6) & 0x3F));
    out += static_cast<char>(0x80 | (code_point & 0x3F));
  }
  return out;
}

}  // namespace

TEST(NaturalSortKey, ComparesDigitRunsAsNumbers) {
  EXPECT_LT(Key("T2"), Key("T10"));
  EXPECT_LT(Key("T9 Stage 10"), Key("T10 Stage 2"));
  EXPECT_LT(Key("x99999999999999999999"), Key("x100000000000000000000"));
  EXPECT_LT(Key("x1234567890123456789012345"), Key("x12345678901234567890123456"));
  // Leading zeros do not change the value.
  EXPECT_EQ(Key("T01"), Key("T1"));
  EXPECT_EQ(Key("x0"), Key("x000"));
  EXPECT_LT(Key("x0"), Key("x1"));
}

TEST(NaturalSortKey, IgnoresCaseAndLatinDiacritics) {
  EXPECT_EQ(Key("Temp"), Key("temp"));
  EXPECT_EQ(Key("Caf\u00E9"), Key("cafe"));
  EXPECT_EQ(Key("L\u00CDNEA"), Key("linea"));
  EXPECT_EQ(Key("Stra\u00DFe"), Key("strasse"));
  EXPECT_EQ(Key("\u00C6r\u00F8"), Key("aero"));
}

TEST(NaturalSortKey, RanksWhitespaceAndPunctuationBeforeDigitsAndLetters) {
  EXPECT_LT(Key("T 1"), Key("T_1"));
  EXPECT_LT(Key("T_1"), Key("T-1"));
  EXPECT_LT(Key("T-1"), Key("T.1"));
  EXPECT_LT(Key("T.1"), Key("T1"));
  EXPECT_LT(Key("T1"), Key("TA"));
  EXPECT_LT(Key("a\tb"), Key("a b"));
  EXPECT_LT(Key("a-b"), Key("a\u2013b"));
}

TEST(NaturalSortKey, SkipsIgnorableCharacters) {
  EXPECT_EQ(Key("a\001b"), Key("ab"));
  EXPECT_EQ(Key("Cafe\u0301"), Key("Caf\u00E9"));
  EXPECT_EQ(Key("co\u00ADop"), Key("coop"));
}

TEST(NaturalSortKey, EmptyStringHasEmptyKey) { EXPECT_EQ(Key(""), ""); }

TEST(NaturalSortKey, KeyOfValidUtf8IsValidUtf8) {
  // Keys travel back to clients, so they must stay valid UTF-8.
  for (auto const *s : {"WTG12.Gen_345-TT7", "\u00BFQu\u00E9?", "\u0141\u00F3d\u017A 2", "\u4E2D\u6587 10"}) {
    auto const key = Key(s);
    for (size_t i = 0; i < key.size();) {
      auto const lead = static_cast<unsigned char>(key[i]);
      size_t const len = lead < 0x80 ? 1 : lead >= 0xF0 ? 4 : lead >= 0xE0 ? 3 : 2;
      ASSERT_TRUE(lead < 0x80 || lead >= 0xC2) << s;
      for (size_t j = 1; j < len; ++j) {
        ASSERT_LT(i + j, key.size()) << s;
        ASSERT_EQ(static_cast<unsigned char>(key[i + j]) & 0xC0U, 0x80U) << s;
      }
      i += len;
    }
  }
}

TEST(NaturalSortKey, AppliesTheTable) {
  using memgraph::utils::CountUtf8CodePoints;
  namespace table = memgraph::utils::natural_sort;
  for (auto const &[base, letters] : table::kFolds) {
    std::string expected;
    for (size_t i = 0; i < CountUtf8CodePoints(letters); ++i) expected += base;
    EXPECT_EQ(Key(letters), expected) << letters;
  }
  // Every character of a group has the group's mark, and groups rank in order.
  std::string previous;
  for (auto const group : table::kMarks) {
    auto const key = Key(group);
    ASSERT_EQ(key.size(), 2 * CountUtf8CodePoints(group)) << group;
    auto const mark = key.substr(0, 2);
    for (size_t i = 0; i < key.size(); i += 2) EXPECT_EQ(key.substr(i, 2), mark) << group;
    EXPECT_LT(previous, mark) << group;
    previous = mark;
  }
  for (auto const &[first, last] : table::kIgnorable) {
    for (auto const code_point : {first, last})
      EXPECT_EQ(Key("a" + Utf8(code_point) + "b"), "ab") << static_cast<uint32_t>(code_point);
  }
}

// Expected order produced by Intl.Collator(undefined, {numeric: true,
// sensitivity: "base"}) in Node 22 (ICU 77.1, en-US), exact ties broken by the
// raw string, which is how a client that sorts with it orders these names.
TEST(NaturalSortKey, OrdersLikeIcuNumericBaseCollation) {
  std::vector<std::string> const expected = {
      "",
      "\u00BFQue?",
      "\u201Cquoted\u201D",
      "(spare)",
      "[old]",
      "50 C",
      "50\u00B0C",
      "a\tb",
      "a b",
      "a\u00A0b",
      "a-b",
      "a\u2013b",
      "A&B",
      "A#B",
      "a\001b",
      "ab",
      "aero 1",
      "\u00C6r\u00F8 1",
      "Angstrom",
      "\u00C5ngstr\u00F6m",
      "back\\slash 1",
      "Cafe\u0301",
      "Caf\u00E9",
      "cafe",
      "Linea 1",
      "L\u00EDnea 2",
      "L\u00CDNEA 3",
      "Lodz",
      "\u0141\u00F3d\u017A",
      "Lote",
      "Nandu",
      "\u00D1and\u00FA",
      "O'Brien 2",
      "Pump 9 Stage 10",
      "Pump 10 Stage 2",
      "Pump 10 Stage 10",
      "Strasse",
      "Stra\u00DFe",
      "T",
      "T 1",
      "T_1",
      "T-1",
      "T.1",
      "T01",
      "T1",
      "t1",
      "TA",
      "WTG9",
      "WTG10",
      "WTG12.Gen_345-TT7",
      "WTG12.Gen_345-TT10",
      "x0",
      "x00",
      "x000001",
      "x1",
      "x12345678901234567890",
      "x99999999999999999999",
      "x0100000000000000000000",
      "x100000000000000000000",
  };
  auto names = expected;
  std::shuffle(names.begin(), names.end(), std::mt19937{1080});
  std::ranges::sort(names, [](std::string const &a, std::string const &b) {
    auto const ka = Key(a);
    auto const kb = Key(b);
    return ka != kb ? ka < kb : a < b;
  });
  EXPECT_EQ(names, expected);
}
