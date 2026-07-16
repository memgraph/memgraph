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
using pattern::dsl::Var;
using rewrite::ArmingMode;
using rewrite::RewriteConfig;
using rewrite::Rewriter;
using rewrite::RewriteRule;
using rewrite::RuleContext;
using rewrite::RuleSet;

using FuzzGraph = EGraph<FuzzSymbol, FuzzAnalysis>;  // core graph: seeded/queried directly
using memgraph::planner::core::fuzz::FuzzTypedEGraph;
using FuzzRule = RewriteRule<FuzzTypedEGraph>;  // the rewrite engine drives a TypedEGraph

[[noreturn]] void fail(char const *what) {
  std::fprintf(stderr, "incremental arming divergence: %s\n", what);
  std::abort();
}

constexpr PatternVar kX{0};
constexpr PatternVar kRoot{1};
constexpr PatternVar kY{2};
constexpr PatternVar kRoot2{3};  // second root of the multi-pattern rule

// A collapse-tower pattern F^depth(x) with the outermost F bound to kRoot and the
// leaf bound to kX. Its depth is exactly `depth`, so selecting different depths
// drives the arming gate (min_hop(S) <= d_P) at hops 1..depth. Merge-only, so
// terminating: it fuses the tower root with its leaf and re-firing is a no-op.
auto tower_pattern(std::size_t depth) -> Pattern<FuzzSymbol> {
  Pattern<FuzzSymbol>::Builder pb;
  auto node = pb.var(kX);
  for (std::size_t i = 0; i + 1 < depth; ++i) node = pb.sym(FuzzSymbol::F, {node});
  pb.sym(FuzzSymbol::F, {node}, kRoot);  // outermost binds the tower root
  return std::move(pb).build();
}

// The pool is fuzz-selected: `selection` bits choose which optional rules are
// active, `tower_depth` (1..4) sizes the collapse tower. Every rule is either
// merge-only or a permutation-Make (commutativity), so any selected subset
// terminates - the property the differential relies on to reach a real fixpoint.
//
//   plus_idem/mul_idem  Plus(x,x)->x, Mul(x,x)->x   depth 1, merge-only
//   plus_comm           Plus(x,y)->Plus(y,x)        depth 1, permutation-Make;
//                       the only rule that mints an e-node, exercising touched-set
//                       insert recording and the incremental matcher reindex
//   collapse_tower      F^n(x)->x                   depth n, merge-only; the gate
//   multi_root_idem     {Plus(a,a), Mul(a,a)}       one rule, two root symbols
//                       joined on the shared leaf a, exercising the arming index's
//                       multi-root path; merges the two roots
//
// noop_always_armed (?x -> merge(x,x)) is ALWAYS present. Its root is a bare
// variable, so it is symbol-less and always armed - the one rule that exercises
// the always-armed path and the "symbol-less rule fires under an empty active
// set" branch every pass. A self-merge is not a rewrite and records no touch, so
// it is a true no-op that never perturbs the oracle.
auto make_rules(std::uint8_t selection, std::uint8_t tower_depth_raw) -> RuleSet<FuzzTypedEGraph> {
  auto const merge_root_x = [](RuleContext<FuzzTypedEGraph> &ctx, Match const &m) { ctx.merge(m[kRoot], m[kX]); };

  RuleSet<FuzzTypedEGraph>::Builder builder;

  {
    Pattern<FuzzSymbol>::Builder pb;
    pb.var(kX);  // bare-variable root: symbol-less, hence always armed
    builder.add_rule(FuzzRule::Builder{"noop_always_armed"}
                         .pattern(std::move(pb).build())
                         .apply([](RuleContext<FuzzTypedEGraph> &ctx, Match const &m) { ctx.merge(m[kX], m[kX]); }));
  }

  if (selection & 0x01U)
    builder.add_rule(FuzzRule::Builder{"plus_idem"}
                         .pattern(Pattern<FuzzSymbol>::build(kRoot, FuzzSymbol::Plus, {Var{kX}, Var{kX}}))
                         .apply(merge_root_x));
  if (selection & 0x02U)
    builder.add_rule(FuzzRule::Builder{"mul_idem"}
                         .pattern(Pattern<FuzzSymbol>::build(kRoot, FuzzSymbol::Mul, {Var{kX}, Var{kX}}))
                         .apply(merge_root_x));
  if (selection & 0x04U)
    builder.add_rule(FuzzRule::Builder{"plus_comm"}
                         .pattern(Pattern<FuzzSymbol>::build(kRoot, FuzzSymbol::Plus, {Var{kX}, Var{kY}}))
                         .apply([](RuleContext<FuzzTypedEGraph> &ctx, Match const &m) {
                           auto const swapped = ctx.Make<FuzzSymbol::Plus>(m[kY], m[kX]);
                           ctx.merge(m[kRoot], swapped);
                         }));
  if (selection & 0x08U)
    builder.add_rule(
        FuzzRule::Builder{"collapse_tower"}.pattern(tower_pattern(1U + (tower_depth_raw % 4U))).apply(merge_root_x));
  if (selection & 0x10U)
    builder.add_rule(
        FuzzRule::Builder{"multi_root_idem"}
            .pattern(Pattern<FuzzSymbol>::build(kRoot, FuzzSymbol::Plus, {Var{kX}, Var{kX}}))
            .pattern(Pattern<FuzzSymbol>::build(kRoot2, FuzzSymbol::Mul, {Var{kX}, Var{kX}}))
            .apply([](RuleContext<FuzzTypedEGraph> &ctx, Match const &m) { ctx.merge(m[kRoot], m[kRoot2]); }));

  return builder.build();
}

// Build a random e-graph from the fuzz bytes. Deterministic: the same bytes
// always produce the same graph, so the two saturations start identical. `pool`
// is filled with the seeded e-class ids (the probe set for the partition check);
// because construction is deterministic, both runs get identical pool ids.
auto build_graph(uint8_t const *data, size_t size, std::vector<EClassId> &pool) -> FuzzTypedEGraph {
  FuzzTypedEGraph typed;
  auto &eg = typed.core();
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
  return typed;
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
  // First two bytes pick the active rule subset and the collapse-tower depth; the
  // remainder builds the graph. Both runs share the identical rules and graph.
  std::uint8_t const selection = size > 0 ? data[0] : 0;
  std::uint8_t const tower_depth = size > 1 ? data[1] : 0;
  uint8_t const *graph_data = size > 2 ? data + 2 : data;
  size_t const graph_size = size > 2 ? size - 2 : 0;
  auto const rules = make_rules(selection, tower_depth);

  // Reference: every rule every pass.
  std::vector<EClassId> arm_all_pool;
  auto arm_all_eg = build_graph(graph_data, graph_size, arm_all_pool);
  Rewriter arm_all{arm_all_eg, rules};
  arm_all.saturate(RewriteConfig::Unlimited(), ArmingMode::Full);

  // Optimisation: only the rules a pass could re-enable.
  std::vector<EClassId> incremental_pool;
  auto incremental_eg = build_graph(graph_data, graph_size, incremental_pool);
  Rewriter incremental{incremental_eg, rules};
  incremental.saturate(RewriteConfig::Unlimited(), ArmingMode::Incremental);

  // Same fixpoint = same final shape. Rewrite COUNTS can differ by schedule (a
  // merge redundant in one order is a no-op in another), so compare the graph.
  // Incremental only skips rules/prunes candidates, so its merges are a subset of
  // Full's; equal counts plus the sharp fixpoint oracle below pin them equal.
  if (incremental_eg.core().num_classes() != arm_all_eg.core().num_classes()) fail("e-class count differs");
  if (incremental_eg.core().num_live_nodes() != arm_all_eg.core().num_live_nodes()) fail("live-node count differs");

  // Structural check beyond counts: the two runs must merge the seeded nodes into
  // the exact same equivalence classes. Catches a divergence that happened to
  // preserve both counts.
  if (partition_labels(arm_all_eg.core(), arm_all_pool) != partition_labels(incremental_eg.core(), incremental_pool)) {
    fail("merge partition over seeded nodes differs between full and incremental");
  }

  // Sharp oracle: one all-rules pass on the incremental result must find nothing,
  // i.e. incremental arming did not stop short of the real fixpoint.
  if (incremental.iterate_once() != 0) fail("incremental result is not a true fixpoint");

  return 0;
}
