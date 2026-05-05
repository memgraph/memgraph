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

#include <algorithm>
#include <map>
#include <sstream>

import memgraph.planner.core.egraph;

namespace memgraph::planner::core::test {

using pattern::PatternVar;

struct Bindings : std::map<PatternVar, EClassId> {
  using std::map<PatternVar, EClassId>::map;

  /// Check if a Match satisfies all expected variable bindings.
  [[nodiscard]] auto satisfied_by(pattern::Match const &match) const -> bool {
    return std::ranges::all_of(*this, [&](auto const &kv) { return match[kv.first] == kv.second; });
  }

  /// Format for diagnostic output.
  [[nodiscard]] auto format() const -> std::string {
    std::ostringstream os;
    os << "{";
    bool first = true;
    for (auto const &[var, id] : *this) {
      if (!first) os << ", ";
      os << "?" << static_cast<int>(var.id) << "=" << id;
      first = false;
    }
    os << "}";
    return os.str();
  }
};

}  // namespace memgraph::planner::core::test
