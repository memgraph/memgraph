// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "utils/string.hpp"

#include <regex>

std::vector<std::string> memgraph::utils::Split(std::string_view const src) {
  if (src.empty()) return {};
  // TODO: Investigate how much regex allocate and perhaps replace with custom
  // solution doing no allocations.
  static std::regex const not_whitespace("[^\\s]+");
  auto matches_begin = std::cregex_iterator(src.data(), src.data() + src.size(), not_whitespace);
  auto matches_end = std::cregex_iterator();
  std::vector<std::string> res;
  res.reserve(std::distance(matches_begin, matches_end));
  for (auto match = matches_begin; match != matches_end; ++match) {
    std::string_view const match_view(&src[match->position()], match->length());
    res.emplace_back(match_view);
  }
  return res;
}
