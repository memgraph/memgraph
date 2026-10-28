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

// How naturalSortKey() treats characters other than ASCII letters and digits,
// derived from ICU 77.1 root collation at base strength with numeric
// ordering, over ASCII, Latin-1 Supplement, Latin Extended-A/B, combining
// diacritics and General Punctuation. Characters not listed keep their code
// point order, after the ASCII letters.

#pragma once

#include <string_view>
#include <utility>

namespace memgraph::utils::natural_sort {

// Ignored entirely, as ICU ignores them: control and formatting characters,
// combining marks.
inline constexpr std::pair<char32_t, char32_t> kIgnorable[] = {
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
inline constexpr std::string_view kMarks[] = {
    "\t", "\n", "\v", "\f", "\r", "\u0085", "\u2028", "\u2029",
    " \u00A0\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202F\u205F", "\u203E", "_", "\u2017",
    "-", "\u2010\u2011", "\u2012", "\u2013", "\u2014", "\u2015", "\u2053", ",", ";", "\u204F", ":", "!", "\u203C",
    "\u2049", "\u00A1", "?", "\u2048", "\u2047", "\u00BF", "\u203D", ".\u2024", "\u2025", "\u2026", "\u00B7", "\u2055",
    "\u2056", "\u2058", "\u2059", "\u205A", "\u205B", "\u205C", "\u205D", "\u205E", "'\u2018\u2019\u201A\u201B",
    "\u2039", "\u203A", "\"\u201C\u201D\u201E\u201F", "\u00AB", "\u00BB", "(", ")", "[", "]", "{", "}", "\u2045",
    "\u2046", "\u2016", "\u00A7", "\u00B6", "\u204B", "@", "*", "\u204E", "\u2051", "/", "\\", "&", "\u204A", "#", "%",
    "\u2030", "\u2031", "\u2020", "\u2021", "\u2022", "\u2023", "\u2027", "\u2043", "\u204C", "\u204D", "\u2032",
    "\u2033", "\u2034", "\u2057", "\u2035", "\u2036", "\u2037", "\u2038", "\u203B", "\u203F", "\u2054", "\u2040",
    "\u2050", "\u2041", "\u2042", "`", "\u00B4", "^", "\u00AF", "\u00A8", "\u00B8", "\u00B0", "\u00A9", "\u00AE", "+",
    "\u00B1", "\u00F7", "\u00D7", "<", "=", ">", "\u00AC", "|", "\u00A6", "~", "\u2052", "\u2044", "\u00A4", "\u00A2",
    "$", "\u00A3", "\u00A5",
};
// clang-format on

// Letters compared as the a-z letters they equal.
inline constexpr std::pair<std::string_view, std::string_view> kFolds[] = {
    {"a", "ÀÁÂÃÄÅàáâãäåĀāĂăĄąǍǎǞǟǠǡǺǻȀȁȂȃȦȧ"},
    {"ae", "Ææ"},
    {"c", "ÇçĆćĈĉĊċČč"},
    {"d", "ĎďĐđ"},
    {"e", "ÈÉÊËèéêëĒēĔĕĖėĘęĚěȄȅȆȇȨȩ"},
    {"g", "ĜĝĞğĠġĢģǦǧǴǵ"},
    {"h", "ĤĥĦħȞȟ"},
    {"i", "ÌÍÎÏìíîïĨĩĪīĬĭĮįİǏǐȈȉȊȋ"},
    {"j", "Ĵĵǰ"},
    {"k", "ĶķǨǩ"},
    {"l", "ĹĺĻļĽľŁł"},
    {"n", "ÑñŃńŅņŇňǸǹ"},
    {"o", "ÒÓÔÕÖØòóôõöøŌōŎŏŐőƠơǑǒǪǫǬǭȌȍȎȏȪȫȬȭȮȯȰȱ"},
    {"oe", "Œœ"},
    {"r", "ŔŕŖŗŘřȐȑȒȓ"},
    {"s", "ŚśŜŝŞşŠšȘș"},
    {"ss", "ß"},
    {"t", "ŢţŤťȚț"},
    {"u", "ÙÚÛÜùúûüŨũŪūŬŭŮůŰűŲųƯưǓǔǕǖǗǘǙǚǛǜȔȕȖȗ"},
    {"w", "Ŵŵ"},
    {"y", "ÝýÿŶŷŸȲȳ"},
    {"z", "ŹźŻżŽž"},
};

}  // namespace memgraph::utils::natural_sort
