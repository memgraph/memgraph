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

#include <array>
#include <cstdint>
#include <vector>

#include "test_support/types.hpp"

namespace memgraph::planner::core::test {

// ============================================================================
// E-Graph Builders
// ============================================================================
// Reusable graph constructions for tests and benchmarks. Keep this header
// independent of gtest/google-benchmark so either harness can include it.

// N independent Add(Var, Var) expressions - no shared structure.
inline void BuildIndependentAdds(TestEGraph &g, int64_t n) {
  for (int64_t i = 0; i < n; ++i) {
    auto x = g.emplace(Op::Var, static_cast<uint64_t>(i * 2)).eclass_id;
    auto y = g.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1)).eclass_id;
    g.emplace(Op::Add, {x, y});
  }
}

// Single chain: Neg(Neg(Neg(...Var(0)...))).
inline auto BuildNegChain(TestEGraph &g, int64_t depth) -> EClassId {
  auto cur = g.emplace(Op::Var, 0).eclass_id;
  for (int64_t i = 0; i < depth; ++i) {
    cur = g.emplace(Op::Neg, {cur}).eclass_id;
  }
  return cur;
}

// Deep linear F-chain: F(F(...F(Const(0))...)).  Returns root eclass id.
inline auto BuildDeepChain(TestEGraph &g, int64_t depth) -> EClassId {
  EClassId cur = g.emplace(Op::Const, uint64_t{0}).eclass_id;
  for (int64_t i = 0; i < depth; ++i) {
    cur = g.emplace(Op::F, {cur}).eclass_id;
  }
  return cur;
}

// Wide multi-enode eclass: `fanout` F(Const_i) nodes merged into one eclass.
// Caller is responsible for `egraph.rebuild()` and re-canonicalising via
// `egraph.find(root)` afterwards.
inline auto BuildWideMerge(TestEGraph &g, int64_t fanout) -> EClassId {
  EClassId root = g.emplace(Op::F, {g.emplace(Op::Const, uint64_t{0}).eclass_id}).eclass_id;
  for (int64_t i = 1; i < fanout; ++i) {
    EClassId leaf = g.emplace(Op::Const, static_cast<uint64_t>(i)).eclass_id;
    EClassId sibling = g.emplace(Op::F, {leaf}).eclass_id;
    root = g.merge(root, sibling).eclass_id;
  }
  return root;
}

// Multiple independent Neg chains of given depth.
inline void BuildNegChains(TestEGraph &g, int64_t num_chains, int64_t depth) {
  for (int64_t i = 0; i < num_chains; ++i) {
    auto cur = g.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id;
    for (int64_t j = 0; j < depth; ++j) {
      cur = g.emplace(Op::Neg, {cur}).eclass_id;
    }
  }
}

// Half Add(x,x), half Add(x,y) - useful for same-variable-pattern tests.
inline void BuildMixedAdds(TestEGraph &g, int64_t n) {
  std::vector<EClassId> vars;
  for (int64_t i = 0; i < n; ++i) {
    vars.push_back(g.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id);
  }
  for (int64_t i = 0; i < n / 2; ++i) {
    g.emplace(Op::Add, {vars[static_cast<size_t>(i)], vars[static_cast<size_t>(i)]});
  }
  for (int64_t i = 0; i < n / 2; ++i) {
    auto j = (i + 1) % (n / 2);
    if (i != j) g.emplace(Op::Add, {vars[static_cast<size_t>(i)], vars[static_cast<size_t>(j)]});
  }
}

// Add(x,y) and Mul(x,y) sharing operands.
inline void BuildAddMulPairs(TestEGraph &g, int64_t n) {
  for (int64_t i = 0; i < n; ++i) {
    auto x = g.emplace(Op::Var, static_cast<uint64_t>(i * 2)).eclass_id;
    auto y = g.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1)).eclass_id;
    g.emplace(Op::Add, {x, y});
    g.emplace(Op::Mul, {x, y});
  }
}

// Add and Mul nodes merged together (multiple e-nodes per class).
inline void BuildMergedAddMul(TestEGraph &g, int64_t n) {
  TestProcessingContext pctx;
  std::vector<EClassId> vars;
  for (int64_t i = 0; i < n; ++i) {
    vars.push_back(g.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id);
  }
  std::vector<EClassId> adds, muls;
  for (int64_t i = 0; i < n - 1; ++i) {
    adds.push_back(g.emplace(Op::Add, {vars[static_cast<size_t>(i)], vars[static_cast<size_t>(i + 1)]}).eclass_id);
    muls.push_back(g.emplace(Op::Mul, {vars[static_cast<size_t>(i)], vars[static_cast<size_t>(i + 1)]}).eclass_id);
  }
  for (size_t i = 0; i < adds.size(); ++i) {
    g.merge(adds[i], muls[i]);
  }
  g.rebuild(pctx);
}

// Many Adds but only ONE Neg - for testing selective patterns.
inline void BuildAddsWithOneNeg(TestEGraph &g, int64_t n) {
  std::vector<EClassId> vars;
  for (int64_t i = 0; i < n; ++i) {
    vars.push_back(g.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id);
  }
  for (int64_t i = 0; i < n - 1; ++i) {
    g.emplace(Op::Add, {vars[static_cast<size_t>(i)], vars[static_cast<size_t>(i + 1)]});
  }
  auto neg = g.emplace(Op::Neg, {vars[0]}).eclass_id;
  g.emplace(Op::Add, {neg, vars[1]});
}

// Add(x,y), Mul(x,z), Neg(x) for each x.
inline void BuildAddMulNegTriples(TestEGraph &g, int64_t n) {
  for (int64_t i = 0; i < n; ++i) {
    auto x = g.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id;
    auto y = g.emplace(Op::Var, static_cast<uint64_t>(10000 + i)).eclass_id;
    auto z = g.emplace(Op::Var, static_cast<uint64_t>(20000 + i)).eclass_id;
    g.emplace(Op::Add, {x, y});
    g.emplace(Op::Mul, {x, z});
    g.emplace(Op::Neg, {x});
  }
}

// Add(x,y) and Mul(x,z) sharing a small pool of x variables.
// Creates O(n²/k) join matches where k = num_shared_x.
inline void BuildAddMulFewSharedX(TestEGraph &g, int64_t n, int64_t num_shared_x) {
  std::vector<EClassId> shared_x_vars;
  shared_x_vars.reserve(static_cast<std::size_t>(num_shared_x));
  for (int64_t i = 0; i < num_shared_x; ++i) {
    shared_x_vars.push_back(g.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id);
  }
  for (int64_t i = 0; i < n; ++i) {
    auto x = shared_x_vars[static_cast<std::size_t>(i % num_shared_x)];
    auto y = g.emplace(Op::Var, static_cast<uint64_t>(1000 + i)).eclass_id;
    g.emplace(Op::Add, {x, y});
  }
  for (int64_t i = 0; i < n; ++i) {
    auto x = shared_x_vars[static_cast<std::size_t>(i % num_shared_x)];
    auto z = g.emplace(Op::Var, static_cast<uint64_t>(2000 + i)).eclass_id;
    g.emplace(Op::Mul, {x, z});
  }
}

// Add(x,y) and Neg(x) with no shared variables between patterns.
// Creates O(n²) Cartesian product join matches.
inline void BuildAddNegDisjoint(TestEGraph &g, int64_t n) {
  for (int64_t i = 0; i < n; ++i) {
    auto x = g.emplace(Op::Var, static_cast<uint64_t>(i * 2)).eclass_id;
    auto y = g.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1)).eclass_id;
    g.emplace(Op::Add, {x, y});
    g.emplace(Op::Neg, {x});
  }
}

// One "hub" leaf with many parents of different symbols. Tests symbol-index
// filtering: 1 hub + parents_f F(hub, other) + parents_neg Neg(hub).
inline void BuildHighParentHub(TestEGraph &g, int64_t parents_f, int64_t parents_neg) {
  auto hub = g.emplace(Op::Const, 0).eclass_id;
  for (int64_t i = 0; i < parents_f; ++i) {
    auto other = g.emplace(Op::Const, static_cast<uint64_t>(i + 1)).eclass_id;
    g.emplace(Op::F, {hub, other});
  }
  for (int64_t i = 0; i < parents_neg; ++i) {
    g.emplace(Op::Neg, {hub});
  }
}

// Self-referential e-class via merge.
// n0 = Const(seed), n1 = F(n0), n2 = F(n1), merge(n1, n2) => EC1 = {F(n0), F(EC1)}.
inline auto BuildSelfReferential(TestEGraph &g, uint64_t seed = 42) -> EClassId {
  TestProcessingContext pctx;
  auto n0 = g.emplace(Op::Const, seed).eclass_id;
  auto n1 = g.emplace(Op::F, {n0}).eclass_id;
  auto n2 = g.emplace(Op::F, {n1}).eclass_id;
  g.merge(n1, n2);
  g.rebuild(pctx);
  return g.find(n1);
}

// (F ?v0) JOIN (F (F (F (F ?v0)))) scenario. For each leaf, creates a shallow
// F(leaf) and a deep F(F(F(F(leaf)))). num_leaves matches, ?v0 binds to each leaf.
inline void BuildNestedJoinGraph(TestEGraph &g, int64_t num_leaves) {
  for (int64_t i = 0; i < num_leaves; ++i) {
    auto leaf = g.emplace(Op::Const, static_cast<uint64_t>(i)).eclass_id;
    g.emplace(Op::F, {leaf});
    auto f1 = g.emplace(Op::F, {leaf}).eclass_id;
    auto f2 = g.emplace(Op::F, {f1}).eclass_id;
    auto f3 = g.emplace(Op::F, {f2}).eclass_id;
    g.emplace(Op::F, {f3});
  }
}

// Many leaves, each with diverse parents distributed among Add, Mul, Neg, F.
inline void BuildParentDiversity(TestEGraph &g, int64_t num_leaves, int64_t parents_per_leaf, uint64_t seed = 42) {
  std::vector<EClassId> leaves;
  leaves.reserve(static_cast<std::size_t>(num_leaves));

  for (int64_t i = 0; i < num_leaves; ++i) {
    leaves.push_back(g.emplace(Op::Const, static_cast<uint64_t>(i)).eclass_id);
  }

  std::array<Op, 4> symbols = {Op::Add, Op::Mul, Op::Neg, Op::F};
  uint64_t rng = seed;
  auto next_rng = [&rng]() {
    rng = rng * 6364136223846793005ULL + 1442695040888963407ULL;
    return rng;
  };

  for (auto leaf_id : leaves) {
    for (int64_t p = 0; p < parents_per_leaf; ++p) {
      auto sym = symbols[next_rng() % 4];
      if (sym == Op::Neg) {
        g.emplace(Op::Neg, {leaf_id});
      } else {
        auto other_idx = next_rng() % static_cast<uint64_t>(leaves.size());
        auto other_leaf = leaves[other_idx];
        g.emplace(sym, {leaf_id, other_leaf});
      }
    }
  }
}

// Realistic e-graph: 180 Const leaves, 60 Var leaves, 320 layers of
// Add/Neg/Mul/F/F2 nodes with periodic unions to create non-trivial e-classes.
inline void BuildComplexGraph(TestEGraph &g) {
  TestProcessingContext pctx;
  constexpr std::size_t kNumConsts = 180;
  constexpr std::size_t kNumVars = 60;
  constexpr std::size_t kNumLayers = 320;

  std::vector<EClassId> consts;
  std::vector<EClassId> vars;
  consts.reserve(kNumConsts);
  vars.reserve(kNumVars);

  for (std::size_t i = 0; i < kNumConsts; ++i) {
    consts.push_back(g.emplace(Op::Const, static_cast<uint64_t>(i)).eclass_id);
  }
  for (std::size_t i = 0; i < kNumVars; ++i) {
    vars.push_back(g.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id);
  }

  for (std::size_t i = 0; i < kNumLayers; ++i) {
    auto const c1 = consts[i % consts.size()];
    auto const c2 = consts[(i * 7 + 11) % consts.size()];
    auto const v1 = vars[(i * 13 + 3) % vars.size()];

    auto const add = g.emplace(Op::Add, {c1, c2}).eclass_id;
    auto const neg = g.emplace(Op::Neg, {c2}).eclass_id;
    auto const mul = g.emplace(Op::Mul, {add, neg}).eclass_id;
    auto const f = g.emplace(Op::F, {mul, v1}).eclass_id;
    g.emplace(Op::F2, {f});

    if ((i % 5) == 0) {
      auto const add_swapped = g.emplace(Op::Add, {c2, c1}).eclass_id;
      g.merge(add, add_swapped);
    }
    if ((i % 9) == 0) {
      auto const neg2 = g.emplace(Op::Neg, {c2}).eclass_id;
      g.merge(neg, neg2);
    }
  }
  g.rebuild(pctx);
}

// N Bind nodes, each with M Ident nodes referencing the same symbol.
// Yields N * M join matches.
inline void BuildBindIdentGraph(TestEGraph &g, int64_t num_binds, int64_t idents_per_sym) {
  for (int64_t i = 0; i < num_binds; ++i) {
    auto placeholder = g.emplace(Op::Const, static_cast<uint64_t>(i * 1000)).eclass_id;
    auto sym_val = g.emplace(Op::Const, static_cast<uint64_t>(i)).eclass_id;
    auto expr_val = g.emplace(Op::Const, static_cast<uint64_t>(i + 10000)).eclass_id;
    g.emplace(Op::Bind, {placeholder, sym_val, expr_val});

    for (int64_t j = 0; j < idents_per_sym; ++j) {
      g.emplace(Op::Ident, {sym_val});
    }
  }
}

// Eclass-level hoisting fixture.
//
// Eclasses with multiple F enodes (via merges) and Mul parents.
// Pattern: ?r=F(?x) + Mul(?r, _).
//
// Mul(?r, _) depends only on ?r (the eclass register), not on which F enode
// is selected. With hoisting, Mul's parent traversal runs once per eclass.
// Without hoisting, it runs once per F enode - redundantly.
//
// Uses Mul(f_eclass, unique_leaf) so each parent is structurally distinct,
// avoiding e-graph deduplication. This lets us control the parent count.
//
// Expected matches: num_eclasses * enodes_per_class * parents.
inline void BuildEclassHoistGraph(TestEGraph &g, int64_t num_eclasses, int64_t enodes_per_class, int64_t parents) {
  TestProcessingContext pctx;

  for (int64_t ec = 0; ec < num_eclasses; ++ec) {
    auto leaf0 = g.emplace(Op::Const, static_cast<uint64_t>(ec * 1000)).eclass_id;
    auto first_f = g.emplace(Op::F, {leaf0}).eclass_id;

    for (int64_t e = 1; e < enodes_per_class; ++e) {
      auto leaf = g.emplace(Op::Const, static_cast<uint64_t>(ec * 1000 + e)).eclass_id;
      auto f = g.emplace(Op::F, {leaf}).eclass_id;
      g.merge(first_f, f);
    }

    for (int64_t p = 0; p < parents; ++p) {
      auto unique = g.emplace(Op::Const, static_cast<uint64_t>(100000 + ec * 1000 + p)).eclass_id;
      g.emplace(Op::Mul, {first_f, unique});
    }
  }

  g.rebuild(pctx);
}

}  // namespace memgraph::planner::core::test
