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

namespace {

void replace_and_shrink(std::string &query, std::size_t pos, std::string_view old_name, std::string_view new_name) {
  const auto size = old_name.length();
  query.replace(pos, size, new_name);
  if (size - pos > new_name.length()) {
    query.shrink_to_fit();
  }
}

}  // namespace

namespace memgraph::query {

void GraphqlChecker::operator()(std::string &query) const {
  for (auto [original, mg_specific] : mapping_) {
    while (true) {
      const auto pos = query.find(original);
      if (std::string::npos == pos) {
        break;
      }
      replace_and_shrink(query, pos, original, mg_specific);
    }
  }
}

}  // namespace memgraph::query
