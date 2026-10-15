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

#include <functional>
#include <optional>

#include "query/exceptions.hpp"
#include "query/plan_v2/resolve/constant_identity.hpp"

namespace memgraph::query::plan::v2 {

namespace {
/// Merge one optional fact field with the agree-or-bug rule: a one-sided fact
/// is taken, an agreeing fact is kept, and a genuine disagreement is a planner
/// bug (sound rewrites cannot equate two e-classes whose facts contradict).
template <typename T, typename Eq>
void MergeField(std::optional<T> &lhs, std::optional<T> const &rhs, Eq eq) {
  if (!rhs) return;
  if (!lhs) {
    lhs = rhs;
    return;
  }
  if (!eq(*lhs, *rhs)) {
    throw PlannerBug{
        "analysis merge: two e-classes proven equivalent carry contradictory facts; this is a planner "
        "bug - please report it at https://github.com/memgraph/memgraph/issues"};
  }
}
}  // namespace

void ExpressionAnalysis::merge(ExpressionAnalysis const &other) {
  MergeField(known_constant_value, other.known_constant_value, ConstantIdentityEq{});
  MergeField(known_list_length, other.known_list_length, std::equal_to<>{});
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
