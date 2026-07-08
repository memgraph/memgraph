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
//
// Coverage: the rule set includes both merge-only rules and a node-INSERTING
// rule (commutativity), so the insert-during-rewrite path - touched-set insert +
// incremental matcher reindex, the path production's constant-fold exercises - is
// fuzzed, not just merges. The oracle checks e-class/live-node counts, the sharp
// fixpoint oracle (iterate_once() == 0), and merge-PARTITION equivalence over the
// seeded nodes (which nodes ended up equal must match between the two modes) - a
// stronger structural check than counts alone.

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include <boost/unordered/unordered_flat_map.hpp>

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
//   Plus(x, y) -> Plus(y, x)         (commutativity: INSERTS a new e-node then
//                                      merges - the insert-during-rewrite path)
constexpr PatternVar kX{0};
constexpr PatternVar kRoot{1};
constexpr PatternVar kY{2};

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
  // Commutativity mints Plus(y, x) (if absent) and merges it with the matched
  // Plus(x, y). Terminating: the swap hash-conses and merges into one class, so
  // re-firing is a no-op. This is the only rule here that creates e-nodes, so it
  // is what exercises touched-set insert recording and the incremental matcher
  // reindex for new classes.
  auto commute_plus =
      FuzzRule::Builder{"plus_comm"}
          .pattern(Pattern<FuzzSymbol>::build(kRoot, FuzzSymbol::Plus, {Var{kX}, Var{kY}}))
          .apply([](RuleContext<FuzzGraph> &ctx, Match const &m) {
            auto const swapped =
                ctx.emplace(FuzzSymbol::Plus, memgraph::utils::small_vector<EClassId>{m[kY], m[kX]}).eclass_id;
            ctx.merge(m[kRoot], swapped);
          });
  return RuleSet<FuzzGraph>::Build(std::move(plus), std::move(mul), std::move(double_f), std::move(commute_plus));
}

// Build a random e-graph from the fuzz bytes. Deterministic: the same bytes
// always produce the same graph, so the two saturations start identical. `pool`
// is filled with the seeded e-class ids (the probe set for the partition check);
// because construction is deterministic, both runs get identical pool ids.
auto build_graph(uint8_t const *data, size_t size, std::vector<EClassId> &pool) -> FuzzGraph {
  FuzzGraph eg;
  ProcessingContext<FuzzSymbol> ctx;
  pool.clear();
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

// The partition the probe set falls into under `eg`, normalized to a canonical
// labeling (group index by first appearance). Two runs induce equal partitions
// iff these label vectors are equal - independent of how each graph numbers its
// representatives.
auto partition_labels(FuzzGraph const &eg, std::vector<EClassId> const &pool) -> std::vector<int> {
  std::vector<int> labels;
  labels.reserve(pool.size());
  boost::unordered_flat_map<EClassId, int> rep_to_label;
  for (auto const id : pool) {
    auto const [it, _] = rep_to_label.try_emplace(eg.find(id), static_cast<int>(rep_to_label.size()));
    labels.push_back(it->second);
  }
  return labels;
}

}  // namespace

extern "C" auto LLVMFuzzerTestOneInput(uint8_t const *data, size_t size) -> int {
  auto const rules = make_rules();

  // Reference: every rule every pass.
  std::vector<EClassId> arm_all_pool;
  auto arm_all_eg = build_graph(data, size, arm_all_pool);
  Rewriter arm_all{arm_all_eg, rules};
  arm_all.saturate(RewriteConfig::Unlimited(), ArmingMode::Full);

  // Optimisation: only the rules a pass could re-enable.
  std::vector<EClassId> incremental_pool;
  auto incremental_eg = build_graph(data, size, incremental_pool);
  Rewriter incremental{incremental_eg, rules};
  incremental.saturate(RewriteConfig::Unlimited(), ArmingMode::Incremental);

  // Same fixpoint = same final shape. Rewrite COUNTS can differ by schedule (a
  // merge redundant in one order is a no-op in another), so compare the graph.
  // Incremental only skips rules/prunes candidates, so its merges are a subset of
  // Full's; equal counts plus the sharp fixpoint oracle below pin them equal.
  if (incremental_eg.num_classes() != arm_all_eg.num_classes()) fail("e-class count differs");
  if (incremental_eg.num_live_nodes() != arm_all_eg.num_live_nodes()) fail("live-node count differs");

  // Structural check beyond counts: the two runs must merge the seeded nodes into
  // the exact same equivalence classes. Catches a divergence that happened to
  // preserve both counts.
  if (partition_labels(arm_all_eg, arm_all_pool) != partition_labels(incremental_eg, incremental_pool)) {
    fail("merge partition over seeded nodes differs between full and incremental");
  }

  // Sharp oracle: one all-rules pass on the incremental result must find nothing,
  // i.e. incremental arming did not stop short of the real fixpoint.
  if (incremental.iterate_once() != 0) fail("incremental result is not a true fixpoint");

  return 0;
}
