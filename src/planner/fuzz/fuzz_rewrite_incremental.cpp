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

// Differential fuzzer for incremental arming: two identical e-graphs are driven
// through the same rounds, one saturated in Full mode (every rule every pass -
// the reference) and one in Incremental mode (the optimisation). The two must
// agree. A divergence means incremental arming skipped a rule that should have
// run - the exact failure incremental arming must never cause.
//
// Multi-round: after the initial build, each round mutates both graphs
// identically and re-saturates on a PERSISTENT rewriter. The incremental
// rewriter folds a round's mutations into the next arm() through its surviving
// latch - the re-saturation path a single saturate never reaches, and the
// capability latched arming exists to provide.
//
// Oracles per round: equal e-class/live-node counts; merge-PARTITION equivalence
// over the seeded probe set (which nodes ended up equal must match, a stronger
// check than counts); the sharp fixpoint oracle (a full pass on the incremental
// result finds nothing); and convergence within a finite cap (both runs must
// reach a fixpoint, so hitting the cap is a non-termination or skip bug).
//
// The rule pool includes a node-INSERTING rule (commutativity), so the
// insert-during-rewrite path - touched-set insert + incremental matcher reindex -
// is fuzzed, not just merges.

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

// Seed both graphs with the same distinct leaves. Distinct leaves so the
// partition oracle discriminates finely: a wrong merge is likelier to move some
// label when there are more starting classes. Both graphs assign ids identically
// (same op sequence, no saturation yet), so a single shared `pool` indexes both.
void seed_leaves(FuzzTypedEGraph &ref_eg, FuzzTypedEGraph &inc_eg, std::vector<EClassId> &pool) {
  pool.clear();
  for (auto const leaf : {FuzzSymbol::A, FuzzSymbol::B, FuzzSymbol::C, FuzzSymbol::D, FuzzSymbol::E}) {
    for (std::uint64_t d = 0; d < 2; ++d) {
      auto const id = ref_eg.core().emplace(leaf, d).eclass_id;
      inc_eg.core().emplace(leaf, d);  // same id by construction
      pool.push_back(id);
    }
  }
}

// Apply up to `budget` build opcodes to BOTH graphs in lockstep from the shared
// byte cursor, reading each operand index once and using it for both graphs, so
// the two receive the identical construction. `grow` adds new e-classes to the
// pickable pool (true for the initial build); a mutation batch runs with grow
// off, keeping the probe pool frozen and identical across the two graphs even
// after their internal ids diverge under saturation.
void apply_ops(FuzzTypedEGraph &ref_eg, FuzzTypedEGraph &inc_eg, std::vector<EClassId> &pool, uint8_t const *data,
               size_t size, size_t &cursor, bool grow, size_t budget) {
  auto &rc = ref_eg.core();
  auto &ic = inc_eg.core();
  auto next = [&]() -> uint8_t { return cursor < size ? data[cursor++] : 0; };
  auto idx = [&]() -> std::size_t { return next() % pool.size(); };

  for (size_t n = 0; n < budget && cursor < size && pool.size() < 512; ++n) {
    switch (next() % 5) {
      case 0: {
        auto const i = idx(), j = idx();
        auto const id = rc.emplace(FuzzSymbol::Plus, {pool[i], pool[j]}).eclass_id;
        ic.emplace(FuzzSymbol::Plus, {pool[i], pool[j]});
        if (grow) pool.push_back(id);
        break;
      }
      case 1: {
        auto const i = idx(), j = idx();
        auto const id = rc.emplace(FuzzSymbol::Mul, {pool[i], pool[j]}).eclass_id;
        ic.emplace(FuzzSymbol::Mul, {pool[i], pool[j]});
        if (grow) pool.push_back(id);
        break;
      }
      case 2: {
        auto const i = idx();
        auto const id = rc.emplace(FuzzSymbol::F, {pool[i]}).eclass_id;
        ic.emplace(FuzzSymbol::F, {pool[i]});
        if (grow) pool.push_back(id);
        break;
      }
      case 3: {
        auto const i = idx(), j = idx();
        rc.merge(pool[i], pool[j]);  // create equalities so the rules can fire
        ic.merge(pool[i], pool[j]);
        break;
      }
      case 4: {
        // An F-tower: apply F 1..5 times onto a picked class. Uniform construction
        // almost never chains F deep enough by chance, so the collapse-tower rule
        // and the hop-2+ arming gate would otherwise be starved of matches.
        auto const i = idx();
        auto const height = 1U + (next() % 5U);
        auto ref_id = pool[i];
        auto inc_id = pool[i];
        for (unsigned h = 0; h < height; ++h) {
          ref_id = rc.emplace(FuzzSymbol::F, {ref_id}).eclass_id;
          inc_id = ic.emplace(FuzzSymbol::F, {inc_id}).eclass_id;
        }
        if (grow) pool.push_back(ref_id);
        break;
      }
    }
  }
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
  // remainder drives the graph. Both graphs share the identical rules and ops.
  std::uint8_t const selection = size > 0 ? data[0] : 0;
  std::uint8_t const tower_depth = size > 1 ? data[1] : 0;
  uint8_t const *graph_data = size > 2 ? data + 2 : data;
  size_t const graph_size = size > 2 ? size - 2 : 0;
  auto const rules = make_rules(selection, tower_depth);

  FuzzTypedEGraph ref_eg;  // reference: every rule every pass
  FuzzTypedEGraph inc_eg;  // optimisation: only the rules a pass could re-enable
  std::vector<EClassId> pool;
  ProcessingContext<FuzzSymbol> proc;

  size_t cursor = 0;
  seed_leaves(ref_eg, inc_eg, pool);
  apply_ops(ref_eg, inc_eg, pool, graph_data, graph_size, cursor, /*grow=*/true, /*budget=*/40);
  if (ref_eg.core().needs_rebuild()) ref_eg.core().rebuild(proc);
  if (inc_eg.core().needs_rebuild()) inc_eg.core().rebuild(proc);

  // Persistent rewriters: the incremental one folds each round's mutations into
  // the next arm() through the surviving latch - the path a single saturate never
  // reaches. The reference re-saturates with Full, which ignores the latch.
  Rewriter ref{ref_eg, rules};
  Rewriter inc{inc_eg, rules};

  auto run_round = [&]() {
    // Finite, size-scaled cap: under the pool's termination discipline both runs
    // must reach a fixpoint, so hitting the cap is always a bug (non-termination
    // or a skip that prevents convergence), not a legitimately slow input.
    auto cfg = RewriteConfig::Unlimited();
    cfg.max_iterations = 2 * inc_eg.core().num_classes() + 64;
    auto const ref_result = ref.saturate(cfg, ArmingMode::Full);
    auto const inc_result = inc.saturate(cfg, ArmingMode::Incremental);
    if (!ref_result.saturated() || !inc_result.saturated()) {
      fail("did not converge within bound: termination discipline violated or a skip prevents fixpoint");
    }

    // Rewrite COUNTS can differ by schedule (a merge redundant in one order is a
    // no-op in another), so compare the graph. Incremental only skips rules or
    // prunes candidates, so its merges are a subset of Full's; equal counts plus
    // the partition and the sharp fixpoint oracle below pin the two equal.
    if (inc_eg.core().num_classes() != ref_eg.core().num_classes()) fail("e-class count differs");
    if (inc_eg.core().num_live_nodes() != ref_eg.core().num_live_nodes()) fail("live-node count differs");

    // Structural check beyond counts: the two runs must merge the seeded nodes
    // into the exact same equivalence classes.
    if (partition_labels(ref_eg.core(), pool) != partition_labels(inc_eg.core(), pool)) {
      fail("merge partition over seeded nodes differs between full and incremental");
    }

    // Sharp oracle: one all-rules pass on the incremental result must find
    // nothing, i.e. incremental arming did not stop short of the real fixpoint.
    if (inc.iterate_once() != 0) fail("incremental result is not a true fixpoint");
  };

  run_round();  // initial build

  // Re-saturation rounds: mutate both graphs identically, then re-saturate. This
  // is where the persistent latch arms from a touched-set left by a prior pass -
  // the branch's headline capability. A frozen pool keeps the two probe sets
  // identical even as saturation renumbers each graph's classes differently.
  for (int rounds = 0; cursor < graph_size && rounds < 8; ++rounds) {
    apply_ops(ref_eg, inc_eg, pool, graph_data, graph_size, cursor, /*grow=*/false, /*budget=*/10);
    if (ref_eg.core().needs_rebuild()) ref_eg.core().rebuild(proc);
    if (inc_eg.core().needs_rebuild()) inc_eg.core().rebuild(proc);
    ref.rebuild_index();  // the graphs were mutated outside the rewriter
    inc.rebuild_index();
    run_round();
  }

  return 0;
}
