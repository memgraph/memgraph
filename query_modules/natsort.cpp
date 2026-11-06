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

// natsort.key(string) returns a key whose plain string order is natural order,
// the order ICU's root collation gives at base strength with numeric ordering
// (what `Intl.Collator(locale, {numeric: true, sensitivity: "base"})` gives a
// client):
//   - a run of ASCII digits compares by its value, at any length, so "T2" sorts
//     before "T10" and "T01" equals "T1";
//   - letters compare case-insensitively, and the Latin letters that ICU equates
//     with a-z letters as those letters, so "Café" equals "cafe", "Straße" equals
//     "strasse" and "Ǆ" equals "dz";
//   - whitespace, punctuation and symbols sort before digits, in ICU's order,
//     and "…" equals "...";
//   - control characters and combining marks are ignored, so a decomposed "é"
//     equals the precomposed one.
// Other characters compare by code point, after the ASCII letters: other
// scripts, and the Latin letters and number forms that ICU sorts on their own,
// such as "ŋ", "þ" and "½". Strings with equal keys can be ordered by the
// original string to break the tie:
//   ORDER BY natsort.key(n.name), n.name
// The key is valid UTF-8 whenever the string is, and null for null.

#include <algorithm>
#include <array>
#include <cstdint>
#include <exception>
#include <iostream>
#include <iterator>
#include <string>
#include <string_view>
#include <utility>

#include <mgp.hpp>

namespace {

constexpr const char *kFunctionKey = "key";
constexpr const char *kParameterString = "string";

// How natsort.key() treats characters other than ASCII letters and digits,
// derived from ICU 77.1 root collation at base strength with numeric
// ordering, over ASCII, Latin-1 Supplement, Latin Extended-A/B, combining
// diacritics and General Punctuation. Characters not listed keep their code
// point order, after the ASCII letters.

// Ignored entirely, as ICU ignores them: control and formatting characters,
// combining marks.
constexpr std::pair<char32_t, char32_t> kIgnorable[] = {
    {0x0000, 0x0008},
    {0x000E, 0x001F},
    {0x007F, 0x0084},
    {0x0086, 0x009F},
    {0x00AD, 0x00AD},
    {0x0300, 0x0362},
    {0x200B, 0x200F},
    {0x202A, 0x202E},
    {0x2060, 0x2064},
    {0x2066, 0x206F},
};

// Whitespace, punctuation and symbols, which sort before digits, in collation
// order. The characters of one string compare equal.
// clang-format off
constexpr std::string_view kMarks[] = {
    "\t", "\n", "\v", "\f", "\r", "\u0085", "\u2028", "\u2029",
    " \u00A0\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F", "\u203E", "_", "\u2017",
    "-", "\u2010\u2011", "\u2012", "\u2013", "\u2014", "\u2015", "\u2053", ",", ";", "\u204F", ":", "!", "\u00A1", "?",
    "\u00BF", "\u203D", ".\u2024", "\u00B7", "\u2055", "\u2056", "\u2058", "\u2059", "\u205A", "\u205B", "\u205C",
    "\u205D", "\u205E", "'\u2018\u2019\u201A\u201B", "\u2039", "\u203A", "\"\u201C\u201D\u201E\u201F", "\u00AB",
    "\u00BB", "(", ")", "[", "]", "{", "}", "\u2045", "\u2046", "\u2016", "\u00A7", "\u00B6", "\u204B", "@", "*",
    "\u204E", "\u2051", "/", "\\", "&", "\u204A", "#", "%", "\u2030", "\u2031", "\u2020", "\u2021", "\u2022", "\u2023",
    "\u2027", "\u2043", "\u204C", "\u204D", "\u2032", "\u2035", "\u2038", "\u203B", "\u203F", "\u2054", "\u2040",
    "\u2050", "\u2041", "\u2042", "`", "\u00B4", "^", "\u00AF", "\u00A8", "\u00B8", "\u00B0", "\u00A9", "\u00AE", "+",
    "\u00B1", "\u00F7", "\u00D7", "<", "=", ">", "\u00AC", "|", "\u00A6", "~", "\u2052", "\u2044", "\u00A4", "\u00A2",
    "$", "\u00A3", "\u00A5",
};
// clang-format on

// Characters compared as a string of a-z letters and marks: accented letters as
// their base letters, ligatures and repeated punctuation as their parts.
constexpr std::pair<std::string_view, std::string_view> kExpansions[] = {
    {"!!", "\u203C"},
    {"!?", "\u2049"},
    {"..", "\u2025"},
    {"...", "\u2026"},
    {"?!", "\u2048"},
    {"??", "\u2047"},
    {"a", "ªÀÁÂÃÄÅàáâãäåĀāĂăĄąǍǎǞǟǠǡǺǻȀȁȂȃȦȧ\u0363"},
    {"ae", "ÆæǢǣǼǽ"},
    {"c", "ÇçĆćĈĉĊċČč\u0368"},
    {"d", "ÐðĎďĐđ\u0369"},
    {"dz", "ǄǅǆǱǲǳ"},
    {"e", "ÈÉÊËèéêëĒēĔĕĖėĘęĚěȄȅȆȇȨȩ\u0364"},
    {"g", "ĜĝĞğĠġĢģǦǧǴǵ"},
    {"h", "ĤĥĦħȞȟ\u036A"},
    {"i", "ÌÍÎÏìíîïĨĩĪīĬĭĮįİǏǐȈȉȊȋ\u0365"},
    {"ij", "Ĳĳ"},
    {"j", "Ĵĵǰ"},
    {"k", "ĶķǨǩ"},
    {"l", "ĹĺĻļĽľĿŀŁł"},
    {"lj", "Ǉǈǉ"},
    {"m", "\u036B"},
    {"n", "ÑñŃńŅņŇňǸǹ"},
    {"nj", "Ǌǋǌ"},
    {"o", "ºÒÓÔÕÖØòóôõöøŌōŎŏŐőƠơǑǒǪǫǬǭǾǿȌȍȎȏȪȫȬȭȮȯȰȱ\u0366"},
    {"oe", "Œœ"},
    {"r", "ŔŕŖŗŘřȐȑȒȓ\u036C"},
    {"s", "ŚśŜŝŞşŠšſȘș"},
    {"ss", "ß"},
    {"t", "ŢţŤťȚț\u036D"},
    {"u", "ÙÚÛÜùúûüŨũŪūŬŭŮůŰűŲųƯưǓǔǕǖǗǘǙǚǛǜȔȕȖȗ\u0367"},
    {"v", "\u036E"},
    {"w", "Ŵŵ"},
    {"x", "\u036F"},
    {"y", "ÝýÿŶŷŸȲȳ"},
    {"z", "ŹźŻżŽž"},
    {"\u2032\u2032", "\u2033"},
    {"\u2032\u2032\u2032", "\u2034"},
    {"\u2032\u2032\u2032\u2032", "\u2057"},
    {"\u2035\u2035", "\u2036"},
    {"\u2035\u2035\u2035", "\u2037"},
};

// Key bytes are chosen so that byte order ranks the parts of a string the way
// the collator does: marks (whitespace, punctuation, symbols) before numbers
// before letters before everything else. A mark is two bytes, kMarkPrefix and
// its rank. A number is kNumberPrefix plus the count of digits in its length,
// then its length, then its digits without leading zeros: self-delimiting, and
// ordered by value whatever its length. Letters are their lower-case ASCII
// form, and characters outside the table keep their UTF-8 bytes (0xC2 and up).
constexpr char kMarkPrefix = 0x01;
constexpr char kNumberPrefix = 0x02;
static_assert(std::size(kMarks) < 0x80, "a rank byte must be valid UTF-8 on its own");
static_assert(std::size(kExpansions) <= 0x100, "an expansion is indexed by one byte");

// Not a Unicode code point, so never in the table: a malformed byte is copied as is.
constexpr char32_t kMalformed = 0x110000;

// The code point of the UTF-8 sequence starting at s[pos], and its length in bytes.
constexpr std::pair<char32_t, size_t> DecodeUtf8(std::string_view s, size_t pos) {
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

template <class Visit>
constexpr void ForEachCodePoint(std::string_view s, Visit visit) {
  for (size_t pos = 0; pos < s.size();) {
    auto const [code_point, length] = DecodeUtf8(s, pos);
    visit(code_point);
    pos += length;
  }
}

enum class Kind : uint8_t { kKeep, kIgnore, kExpand, kMark };

struct Class {
  Kind kind = Kind::kKeep;
  uint8_t value = 0;  // kMark: rank, from 1. kExpand: index into kExpansions.
};

// The class of every code point the table covers, in two dense blocks
// (U+0000-U+036F and General Punctuation).
struct Classes {
  static constexpr char32_t kLowEnd = 0x370;
  static constexpr char32_t kPunctuationFirst = 0x2000;
  static constexpr char32_t kPunctuationEnd = 0x2070;

  constexpr Class Of(char32_t code_point) const {
    if (code_point < kLowEnd) return low[code_point];
    if (code_point >= kPunctuationFirst && code_point < kPunctuationEnd) {
      return punctuation[code_point - kPunctuationFirst];
    }
    return {};
  }

  std::array<Class, kLowEnd> low{};
  std::array<Class, kPunctuationEnd - kPunctuationFirst> punctuation{};
};

// Expands the table at compile time, so a table that lists a code point twice
// or outside the two blocks does not build. Nor does an expansion to anything
// but a-z letters and marks, so keying one never expands further.
consteval Classes BuildClasses() {
  Classes classes;
  auto set = [&classes](char32_t code_point, Class value) {
    if (code_point >= Classes::kLowEnd &&
        (code_point < Classes::kPunctuationFirst || code_point >= Classes::kPunctuationEnd)) {
      throw "the natural sort table lists a code point outside its blocks";
    }
    auto &slot = code_point < Classes::kLowEnd ? classes.low[code_point]
                                               : classes.punctuation[code_point - Classes::kPunctuationFirst];
    if (slot.kind != Kind::kKeep) throw "the natural sort table lists a code point twice";
    slot = value;
  };
  for (auto const [first, last] : kIgnorable) {
    for (auto code_point = first; code_point <= last; ++code_point) set(code_point, {.kind = Kind::kIgnore});
  }
  for (size_t i = 0; i < std::size(kMarks); ++i) {
    ForEachCodePoint(kMarks[i], [&](char32_t code_point) {
      set(code_point, {.kind = Kind::kMark, .value = static_cast<uint8_t>(i + 1)});
    });
  }
  for (size_t i = 0; i < std::size(kExpansions); ++i) {
    ForEachCodePoint(kExpansions[i].second, [&](char32_t code_point) {
      set(code_point, {.kind = Kind::kExpand, .value = static_cast<uint8_t>(i)});
    });
  }
  for (auto const &expansion : kExpansions) {
    ForEachCodePoint(expansion.first, [&](char32_t code_point) {
      if ((code_point < 'a' || code_point > 'z') && classes.Of(code_point).kind != Kind::kMark) {
        throw "a natural sort expansion holds more than a-z letters and marks";
      }
    });
  }
  return classes;
}

constexpr Classes kClasses = BuildClasses();

void AppendNumber(std::string *out, std::string_view digits) {
  auto const first = digits.find_first_not_of('0');
  auto const value = first == std::string_view::npos ? std::string_view{"0"} : digits.substr(first);
  auto const length = std::to_string(value.size());
  out->push_back(static_cast<char>(kNumberPrefix + length.size()));
  out->append(length);
  out->append(value);
}

void AppendKey(std::string *out, std::string_view s) {
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
      auto cls = kClasses.Of(code_point);
      // ICU ignores a middle dot (U+00B7) right after "l", as in Catalan "l·l".
      if (code_point == 0xB7 && pos > 0 && (s[pos - 1] == 'l' || s[pos - 1] == 'L')) cls = {.kind = Kind::kIgnore};
      switch (cls.kind) {
        case Kind::kKeep:
          out->append(s.substr(pos, length));
          break;
        case Kind::kIgnore:
          break;
        case Kind::kExpand:
          AppendKey(out, kExpansions[cls.value].first);
          break;
        case Kind::kMark:
          out->push_back(kMarkPrefix);
          out->push_back(static_cast<char>(cls.value));
          break;
      }
      pos += length;
    }
  }
}

void Key(mgp_list *args, mgp_func_context * /*ctx*/, mgp_func_result *res, mgp_memory *memory) {
  const mgp::MemoryDispatcherGuard guard(memory);
  auto result = mgp::Result(res);
  try {
    const auto arguments = mgp::List(args);
    if (arguments[0].IsNull()) {
      result.SetValue();
      return;
    }
    auto const string = arguments[0].ValueString();
    std::string key;
    key.reserve(string.size() + 4);
    AppendKey(&key, string);
    result.SetValue(std::string_view{key});
  } catch (const std::exception &e) {
    result.SetErrorMessage(e.what());
  }
}

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard(memory);
    // Accepts a null argument, which mgp::AddFunction cannot express, so register it directly.
    auto *key = mgp::module_add_function(module, kFunctionKey, Key);
    mgp::func_add_arg(key, kParameterString, mgp::type_nullable(mgp::type_string()));
  } catch (const std::exception &e) {
    std::cerr << "Error while initializing query module: " << e.what() << '\n';
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
