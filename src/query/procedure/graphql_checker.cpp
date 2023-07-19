// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "graphql_checker.hpp"

#include <algorithm>
#include <array>

namespace {

constexpr std::array<std::pair<std::string_view, std::string_view>, 2> kNameMapping = {{

    {"dbms.components", "mgps.components"}, {"apoc.util.validate", "mgps.validate"}

}};

}  // namespace

namespace memgraph::query::procedure {

std::optional<std::string_view> FindApocReplacement(std::string_view apoc_name) {
  const auto *maybe_found = std::find_if(kNameMapping.begin(), kNameMapping.end(),
                                         [apoc_name](const auto &pair) { return pair.first == apoc_name; });

  if (maybe_found != kNameMapping.end()) {
    return maybe_found->second;
  }
  return std::nullopt;
}

}  // namespace memgraph::query::procedure
