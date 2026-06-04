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

#include "query/plan_v2/resolve/analysis.hpp"

#include "query/exceptions.hpp"
#include "query/plan_v2/resolve/constant_identity.hpp"

namespace memgraph::query::plan::v2 {

void ExpressionAnalysis::merge(ExpressionAnalysis const &other) {
  if (!other.known_constant_value) return;
  if (!known_constant_value) {
    known_constant_value = other.known_constant_value;
    return;
  }
  if (!ConstantIdentityEq{}(*known_constant_value, *other.known_constant_value)) {
    throw PlannerBug{
        "analysis merge: two e-classes proven equivalent carry different constant values; this is a planner "
        "bug - please report it at https://github.com/memgraph/memgraph/issues"};
  }
}

void analysis::merge(analysis const &other) {
  if (index() != other.index()) {
    throw PlannerBug{
        "analysis merge: e-classes of different kinds were equated; this violates the kind dichotomy and is a "
        "planner bug - please report it at https://github.com/memgraph/memgraph/issues"};
  }
  if (auto *expr = std::get_if<ExpressionAnalysis>(this)) {
    expr->merge(std::get<ExpressionAnalysis>(other));
  }
  // OperatorAnalysis / SymbolAnalysis carry no facts to merge yet.
}

}  // namespace memgraph::query::plan::v2
