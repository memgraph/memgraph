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

// ============================================================================
// Bind semantics — shared algebra for the three pipeline sites.
// ============================================================================
//
// A `Bind(input, sym, expr)` enode represents "compute `expr`, bind it to
// `sym`, then run `input` in that scope."  The same algebra surfaces in three
// pipeline sites and used to be repeated three times:
//
//   1. PlanCostModel::Bind       — bottom-up frontier construction.
//   2. BestBindBranchCostsForResolve — top-down resolver tie-break.
//   3. PlanResolver::Impl::visit_bind_children — Bind-aware child visitation.
//
// Plus the Builder's dead-Bind detection in ConvertToLogicalOperator, which is
// *not* part of this algebra: it observes the resolver's decision via
// build_cache membership rather than re-deciding alive vs dead.  Bind alive vs
// dead is a *resolution* decision; the build phase consumes its outcome.
//
// All five values live here so a future scoped-binding form (Let, scoped
// projection) can reuse the algebra by writing one new caller.

#include <algorithm>
#include <ranges>

#include <boost/container/flat_set.hpp>
#include <boost/container/small_vector.hpp>

import memgraph.planner.core.egraph;

namespace memgraph::query::plan::v2::bind {

/// Set of EClassIds — used for "demand" sets (`required`) and "provided"
/// contexts in the resolver.  flat_set + small_vector keeps small sets
/// (typical Cypher demand depth ≤ 8) heap-free.
using SymbolSet = boost::container::flat_set<planner::core::EClassId, std::less<>,
                                             boost::container::small_vector<planner::core::EClassId, 8>>;

/// Cost of a Symbol leaf alternative.
///
/// Invariant — every Symbol eclass has exactly one alternative
/// `{cost = kSymbolCost, required = ∅}`.  Three sites depend on this:
///   * PlanCostModel emits Symbol leaves with this shape.
///   * PlanCostModel::Bind collapses the sym child frontier to a scalar via
///     min_cost (correct only because the frontier has a single alt).
///   * PlanResolver::Impl::visit_bind_children asserts the leaf shape on
///     entry, surfacing the violation if a future rewrite produces multiple
///     Symbol alternatives.
///
/// If Symbol ever gains alternatives, sym_cost must become a frontier and the
/// alive-branch cost in BestBindBranchCostsForResolve / PlanCostModel::Bind
/// must enumerate it alongside expr.  The assert in visit_bind_children is the
/// loud canary.
inline constexpr double kSymbolCost = 1.0;

/// Predicate — is the Bind alive given the input alt's demand set?
///
/// A Bind is *alive* when the input demands the symbol it would bind: the
/// expr must run, sym must be evaluated, the binding has work to do.
/// A Bind is *dead* when the input doesn't demand the symbol: expr is unused,
/// sym is unused, the Bind is a no-op pass-through over input.
inline auto IsAlive(SymbolSet const &input_required, planner::core::EClassId sym) -> bool {
  return input_required.contains(sym);
}

/// Cost of the alive branch.  Pay for input, sym evaluation, and expr.
inline auto AliveCost(double input_cost, double sym_cost, double expr_cost) -> double {
  return input_cost + sym_cost + expr_cost;
}

/// Cost of the dead branch.  Only the input runs; sym and expr are skipped.
inline auto DeadCost(double input_cost) -> double { return input_cost; }

/// Required-set algebra for the alive branch — `(input.required \ {sym}) ∪ expr.required`.
///
/// Removing `sym` reflects that the Bind itself supplies that symbol; whatever
/// `expr` demands flows up because the binding does not satisfy expr's needs.
///
/// `scratch` is a caller-supplied buffer reused across calls to avoid per-call
/// heap traffic in the cartesian-product loop in PlanCostModel::Bind.  The two
/// inputs are sorted (flat_set invariant); the merged output is sorted-unique
/// so the resulting flat_set is constructed via `ordered_unique_range`,
/// skipping the redundant sort.
inline auto AliveRequired(SymbolSet const &input_required, planner::core::EClassId sym, SymbolSet const &expr_required,
                          boost::container::small_vector<planner::core::EClassId, 16> &scratch) -> SymbolSet {
  scratch.clear();
  scratch.reserve(input_required.size() + expr_required.size());
  auto input_minus_sym = input_required | std::views::filter([sym](planner::core::EClassId id) { return id != sym; });
  std::ranges::set_union(input_minus_sym, expr_required, std::back_inserter(scratch));
  return SymbolSet(boost::container::ordered_unique_range, scratch.begin(), scratch.end());
}

}  // namespace memgraph::query::plan::v2::bind
