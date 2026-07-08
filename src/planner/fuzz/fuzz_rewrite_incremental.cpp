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

// Differential fuzzer for incremental arming: a random e-graph is saturated twice
// with the same rules, once in Full mode (every rule every pass - the
// reference) and once in Incremental mode (the optimisation). The two must agree.
// A divergence means incremental arming skipped a rule that should have run - the exact
// failure incremental arming must never cause.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "fuzz_common.hpp"
#include "planner/rewrite/rewriter.hpp"

namespace {

using namespace memgraph::planner::core;
using memgraph::planner::core::fuzz::FuzzAnalysis;
using memgraph::planner::core::fuzz::FuzzSymbol;
using pattern::Match;
using pattern::Pattern;
using pattern::PatternVar;
using pattern::dsl::Sym;
using pattern::dsl::Var;
using rewrite::ArmingMode;
using rewrite::RewriteConfig;
using rewrite::Rewriter;
using rewrite::RewriteRule;
using rewrite::RuleContext;
using rewrite::RuleSet;

using FuzzGraph = EGraph<FuzzSymbol, FuzzAnalysis>;
using FuzzRule = RewriteRule<FuzzGraph>;

[[noreturn]] void fail(char const *what) {
  std::fprintf(stderr, "incremental arming divergence: %s\n", what);
  std::abort();
}

// Rules that merge and compose so saturation takes several passes:
//   Plus(x, x) -> x, Mul(x, x) -> x  (binary, fire once children become equal)
//   F(F(x))    -> x                  (nested unary, depth 2 - exercises
//                                      parent-closure beyond one hop)
constexpr PatternVar kX{0};
constexpr PatternVar kRoot{1};

auto make_rules() -> RuleSet<FuzzGraph> {
  auto merge_root_x = [](RuleContext<FuzzGraph> &ctx, Match const &m) { ctx.merge(m[kRoot], m[kX]); };

  auto plus = FuzzRule::Builder{"plus_idem"}
                  .pattern(Pattern<FuzzSymbol>::build(kRoot, FuzzSymbol::Plus, {Var{kX}, Var{kX}}))
                  .apply(merge_root_x);
  auto mul = FuzzRule::Builder{"mul_idem"}
                 .pattern(Pattern<FuzzSymbol>::build(kRoot, FuzzSymbol::Mul, {Var{kX}, Var{kX}}))
                 .apply(merge_root_x);
  auto double_f = FuzzRule::Builder{"double_f"}
                      .pattern(Pattern<FuzzSymbol>::build(kRoot, FuzzSymbol::F, {Sym(FuzzSymbol::F, Var{kX})}))
                      .apply(merge_root_x);
  return RuleSet<FuzzGraph>::Build(std::move(plus), std::move(mul), std::move(double_f));
}

// Build a random e-graph from the fuzz bytes. Deterministic: the same bytes
// always produce the same graph, so the two saturations start identical.
auto build_graph(uint8_t const *data, size_t size) -> FuzzGraph {
  FuzzGraph eg;
  ProcessingContext<FuzzSymbol> ctx;
  std::vector<EClassId> pool;
  pool.push_back(eg.emplace(FuzzSymbol::A, 0).eclass_id);
  pool.push_back(eg.emplace(FuzzSymbol::A, 1).eclass_id);

  size_t cursor = 0;
  auto next = [&]() -> uint8_t { return cursor < size ? data[cursor++] : 0; };
  auto pick = [&]() -> EClassId { return pool[next() % pool.size()]; };

  // Bound the graph so the fuzzer stays fast.
  while (cursor < size && pool.size() < 512) {
    switch (next() % 4) {
      case 0:
        pool.push_back(eg.emplace(FuzzSymbol::Plus, {pick(), pick()}).eclass_id);
        break;
      case 1:
        pool.push_back(eg.emplace(FuzzSymbol::Mul, {pick(), pick()}).eclass_id);
        break;
      case 2:
        pool.push_back(eg.emplace(FuzzSymbol::F, {pick()}).eclass_id);
        break;
      case 3:
        eg.merge(pick(), pick());  // create equalities so the rules can fire
        break;
    }
  }
  if (eg.needs_rebuild()) eg.rebuild(ctx);
  return eg;
}

}  // namespace

extern "C" auto LLVMFuzzerTestOneInput(uint8_t const *data, size_t size) -> int {
  auto const rules = make_rules();

  // Reference: every rule every pass.
  auto arm_all_eg = build_graph(data, size);
  Rewriter arm_all{arm_all_eg, rules};
  arm_all.saturate(RewriteConfig::Unlimited(), ArmingMode::Full);

  // Optimisation: only the rules a pass could re-enable.
  auto incremental_eg = build_graph(data, size);
  Rewriter incremental{incremental_eg, rules};
  incremental.saturate(RewriteConfig::Unlimited(), ArmingMode::Incremental);

  // Same fixpoint = same final shape. Rewrite COUNTS can differ by schedule (a
  // merge redundant in one order is a no-op in another), so compare the graph.
  // Incremental only skips rules/prunes candidates, so its merges are a subset of
  // Full's; equal counts plus the sharp fixpoint oracle below pin them equal.
  if (incremental_eg.num_classes() != arm_all_eg.num_classes()) fail("e-class count differs");
  if (incremental_eg.num_live_nodes() != arm_all_eg.num_live_nodes()) fail("live-node count differs");

  // Sharp oracle: one all-rules pass on the incremental result must find nothing,
  // i.e. incremental arming did not stop short of the real fixpoint.
  if (incremental.iterate_once() != 0) fail("incremental result is not a true fixpoint");

  return 0;
}
