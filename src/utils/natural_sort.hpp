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

#include <string>
#include <string_view>

#include "utils/pmr/string.hpp"

namespace memgraph::utils {

/**
 * Store in `out` a sort key for `s`: comparing two keys as plain strings orders
 * their sources in natural order, the order ICU's root collation gives at base
 * strength with numeric ordering (what `Intl.Collator(locale, {numeric: true,
 * sensitivity: "base"})` gives a client):
 *   - a run of ASCII digits compares by its value, at any length, so "T2" sorts
 *     before "T10" and "T01" equals "T1";
 *   - letters compare case-insensitively, and Latin letters (U+00C0-U+024F) as
 *     their base letters, so "Café" equals "cafe" and "Straße" equals "strasse";
 *   - whitespace, punctuation and symbols sort before digits, in ICU's order;
 *   - control characters and combining marks are ignored, so a decomposed "é"
 *     equals the precomposed one.
 * Other characters compare by code point, after the ASCII letters. Strings with
 * equal keys can be ordered by the original string to break the tie. The key is
 * valid UTF-8 whenever `s` is.
 * @return pointer to `out`.
 */
template <class TAllocator>
std::basic_string<char, std::char_traits<char>, TAllocator> *NaturalSortKey(
    std::basic_string<char, std::char_traits<char>, TAllocator> *out, std::string_view s);

extern template std::string *NaturalSortKey(std::string *out, std::string_view s);
extern template pmr::string *NaturalSortKey(pmr::string *out, std::string_view s);

}  // namespace memgraph::utils
