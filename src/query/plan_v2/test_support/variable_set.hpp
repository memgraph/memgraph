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

#include <cstdint>
#include <initializer_list>

#include "query/plan_v2/resolve/variable_set.hpp"

namespace memgraph::query::plan::v2 {

/// Construct a VariableSet from a brace-initialiser list of bit positions.
/// Shared across plan_v2 unit tests that need lightweight VariableSet fixtures
/// without standing up a VariableIndex; tests treat the integers as abstract
/// variable identifiers (the role the per-extraction VariableIndex fills in a
/// real query).
inline auto MakeSet(std::initializer_list<uint16_t> bits) -> VariableSet {
  VariableSet s;
  for (auto b : bits) s.set(b);
  return s;
}

}  // namespace memgraph::query::plan::v2
