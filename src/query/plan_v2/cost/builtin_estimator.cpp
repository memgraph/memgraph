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

#include "query/plan_v2/cost/builtin_estimator.hpp"

#include <cmath>
#include <cstddef>
#include <optional>

#include "query/plan_v2/egraph/egraph_internal.hpp"

namespace memgraph::query::plan::v2 {

using enum symbol;

namespace {

/// Best-effort: if `eclass_id` is statically known to hold an int constant,
/// return it. Reads `known_constant_value` from the e-class's analysis, so it
/// fires for any constant-valued e-class (a literal, or a folded expression),
/// not just one still carrying a `Literal` e-node.
auto TryReadIntLiteral(EGraph const &eg, planner::core::EClassId eclass_id) -> std::optional<int64_t> {
  auto const *expr = eg.eclass(eg.find(eclass_id)).analysis().expression();
  if (expr == nullptr || !expr->known_constant_value) return std::nullopt;

  auto const &val = *expr->known_constant_value;
  if (val.IsInt()) return val.ValueInt();
  // Tolerate doubles that exactly represent an integer (e.g. range(0.0,
  // 5.0) coming from a parser that promoted ints to doubles).  Reject
  // non-integral doubles - they're not what range expects anyway.
  if (val.IsDouble()) {
    auto const d = val.ValueDouble();
    if (std::isfinite(d) && std::trunc(d) == d) return static_cast<int64_t>(d);
  }
  return std::nullopt;
}

auto EstimateFunction(EGraph const &eg, egraph const &facade, planner::core::ENode<symbol> const &enode,
                      std::span<planner::core::EClassId const> arg_eclasses) -> double {
  auto const *info = facade.FunctionInfoById(enode.disambiguator());
  if (info == nullptr) return kDefaultListSize;

  switch (info->kind) {
    case BuiltinKind::Range:
      // The estimate is the produced list's size; an unprovable range falls
      // back to the default guess.
      if (auto const len = ProvableRangeLength(eg, arg_eclasses)) return static_cast<double>(*len);
      return kDefaultListSize;
    case BuiltinKind::Size:
      // size() produces a scalar, not a list, so it has no list-size estimate.
    case BuiltinKind::Unknown:
      return kDefaultListSize;
  }
  return kDefaultListSize;
}

}  // namespace

/// Only two-arg int-literal start/end with implicit step=1 is modelled;
/// non-literal, non-int, or wrong-arity args are not provable.
auto ProvableRangeLength(EGraph const &eg, std::span<planner::core::EClassId const> args)
    -> std::optional<std::size_t> {
  if (args.size() != 2) return std::nullopt;
  auto const a = TryReadIntLiteral(eg, args[0]);
  auto const b = TryReadIntLiteral(eg, args[1]);
  if (!a || !b) return std::nullopt;
  if (*b < *a) return std::size_t{0};
  return static_cast<std::size_t>(*b - *a) + 1;
}

BuiltinEstimator::BuiltinEstimator(egraph const &f) : CardinalityEstimator(impl_of(f).graph.core()), facade(f) {}

auto BuiltinEstimator::Estimate(planner::core::ENode<symbol> const &enode,
                                std::span<planner::core::EClassId const> children) const -> double {
  switch (enode.symbol()) {
    case Function:
      return EstimateFunction(eg_, facade, enode, children);
    default:
      return kDefaultCardinalityEstimate;
  }
}

}  // namespace memgraph::query::plan::v2
