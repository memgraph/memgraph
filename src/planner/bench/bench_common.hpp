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

#include <benchmark/benchmark.h>

#include <cstdint>
#include <vector>

#include "planner/rewrite/rewriter.hpp"

namespace memgraph::planner::bench {

// ============================================================================
// Common Types
// ============================================================================

enum class Op : uint8_t { Add, Mul, Neg, Var, Const, F, Bind, Ident };

struct NoAnalysis {};

using namespace memgraph::planner::core;

using TestEGraph = EGraph<Op, NoAnalysis>;
using TestPattern = Pattern<Op>;
using TestEMatcher = EMatcher<Op, NoAnalysis>;
using TestMatches = std::vector<PatternMatch>;
using TestRewriteRule = RewriteRule<Op, NoAnalysis>;
using TestRuleSet = RuleSet<Op, NoAnalysis>;
using TestRewriter = Rewriter<Op, NoAnalysis>;
using TestRuleContext = RuleContext<Op, NoAnalysis>;
using TestRewriteContext = RewriteContext;

// ============================================================================
// Pattern Variables
// ============================================================================

constexpr PatternVar kX{0};
constexpr PatternVar kY{1};
constexpr PatternVar kZ{2};
constexpr PatternVar kRootDoubleNeg{10};
constexpr PatternVar kRootAdd{11};
constexpr PatternVar kRootMul{12};
constexpr PatternVar kRootNeg{13};

// ============================================================================
// Benchmark Ranges
// ============================================================================

namespace ranges {
constexpr int64_t kSmall = 10;
constexpr int64_t kMedium = 100;
constexpr int64_t kLarge = 1000;
constexpr int64_t kXLarge = 5000;
constexpr int64_t kHuge = 10000;
constexpr int64_t kMassive = 50000;

constexpr int64_t kFreshCtx = 0;
constexpr int64_t kReusedCtx = 1;
}  // namespace ranges

// ============================================================================
// E-Graph Builders
// ============================================================================

// N independent Add(Var, Var) expressions - no shared structure
inline void BuildIndependentAdds(TestEGraph &g, int64_t n) {
  for (int64_t i = 0; i < n; ++i) {
    auto x = g.emplace(Op::Var, static_cast<uint64_t>(i * 2)).eclass_id;
    auto y = g.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1)).eclass_id;
    g.emplace(Op::Add, {x, y});
  }
}

// NegChain: Neg(Neg(Neg(...Var(0)...))) - single chain of Neg nodes
inline auto BuildNegChain(TestEGraph &g, int64_t depth) -> EClassId {
  auto cur = g.emplace(Op::Var, 0).eclass_id;
  for (int64_t i = 0; i < depth; ++i) {
    cur = g.emplace(Op::Neg, {cur}).eclass_id;
  }
  return cur;
}

// NegChains: Multiple independent Neg chains of given depth
inline void BuildNegChains(TestEGraph &g, int64_t num_chains, int64_t depth) {
  for (int64_t i = 0; i < num_chains; ++i) {
    auto cur = g.emplace(Op::Var, static_cast<uint64_t>(i)).eclass_id;
    for (int64_t j = 0; j < depth; ++j) {
      cur = g.emplace(Op::Neg, {cur}).eclass_id;
    }
  }
}

// MixedAdds: Half Add(x,x), half Add(x,y) for testing same-variable patterns
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

// AddMulPairs: Add(x,y) and Mul(x,y) sharing operands
inline void BuildAddMulPairs(TestEGraph &g, int64_t n) {
  for (int64_t i = 0; i < n; ++i) {
    auto x = g.emplace(Op::Var, static_cast<uint64_t>(i * 2)).eclass_id;
    auto y = g.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1)).eclass_id;
    g.emplace(Op::Add, {x, y});
    g.emplace(Op::Mul, {x, y});
  }
}

// MergedAddMul: Add and Mul nodes merged together (multiple e-nodes per class)
inline void BuildMergedAddMul(TestEGraph &g, int64_t n) {
  ProcessingContext<Op> pctx;
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

// AddsWithOneNeg: Many Adds but only ONE Neg - for testing selective patterns
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

// AddMulNegTriples: Add(x,y), Mul(x,z), Neg(x) for each x
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

// AddMulFewSharedX: Add(x,y) and Mul(x,z) sharing a small pool of x variables.
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

// AddNegDisjoint: Add(x,y) and Neg(x) with no shared variables between patterns.
// Creates O(n²) Cartesian product join matches.
inline void BuildAddNegDisjoint(TestEGraph &g, int64_t n) {
  for (int64_t i = 0; i < n; ++i) {
    auto x = g.emplace(Op::Var, static_cast<uint64_t>(i * 2)).eclass_id;
    auto y = g.emplace(Op::Var, static_cast<uint64_t>(i * 2 + 1)).eclass_id;
    g.emplace(Op::Add, {x, y});
    g.emplace(Op::Neg, {x});
  }
}

// HighParentCount: A "hub" leaf with many parents of different symbols.
// Tests symbol index filtering effectiveness.
// Creates 1 hub + parents_f F(hub, other) nodes + parents_neg Neg(hub) nodes.
inline void BuildHighParentHub(TestEGraph &g, int64_t parents_f, int64_t parents_neg) {
  auto hub = g.emplace(Op::Const, 0).eclass_id;

  // Create F parents (binary op using hub and unique leaves)
  for (int64_t i = 0; i < parents_f; ++i) {
    auto other = g.emplace(Op::Const, static_cast<uint64_t>(i + 1)).eclass_id;
    g.emplace(Op::F, {hub, other});
  }

  // Create Neg parents (unary op using hub)
  for (int64_t i = 0; i < parents_neg; ++i) {
    g.emplace(Op::Neg, {hub});
  }
}

// SelfReferentialEClass: Creates a self-referential e-class via merge.
// n0 = Const(seed), n1 = F(n0), n2 = F(n1), merge(n1, n2) => EC1 = {F(n0), F(EC1)}
inline auto BuildSelfReferential(TestEGraph &g, uint64_t seed = 42) -> EClassId {
  ProcessingContext<Op> pctx;
  auto n0 = g.emplace(Op::Const, seed).eclass_id;
  auto n1 = g.emplace(Op::F, {n0}).eclass_id;
  auto n2 = g.emplace(Op::F, {n1}).eclass_id;
  g.merge(n1, n2);
  g.rebuild(pctx);
  return g.find(n1);  // Return canonical ID of self-referential class
}

// NestedJoinGraph: Creates scenario for (F ?v0) JOIN (F (F (F (F ?v0)))) pattern.
// For each of num_leaves leaves, creates:
//   - A shallow F node: F(leaf)
//   - A deep F chain: F(F(F(F(leaf))))
// This creates num_leaves matches where ?v0 binds to each leaf.
inline void BuildNestedJoinGraph(TestEGraph &g, int64_t num_leaves) {
  for (int64_t i = 0; i < num_leaves; ++i) {
    auto leaf = g.emplace(Op::Const, static_cast<uint64_t>(i)).eclass_id;

    // Create shallow F(leaf)
    g.emplace(Op::F, {leaf});

    // Create deep chain F(F(F(F(leaf))))
    auto f1 = g.emplace(Op::F, {leaf}).eclass_id;
    auto f2 = g.emplace(Op::F, {f1}).eclass_id;
    auto f3 = g.emplace(Op::F, {f2}).eclass_id;
    g.emplace(Op::F, {f3});
  }
}

// ParentDiversity: Many leaves each with diverse parents of various symbols.
// num_leaves leaves, parents_per_leaf parents randomly distributed among Add, Mul, Neg, F.
inline void BuildParentDiversity(TestEGraph &g, int64_t num_leaves, int64_t parents_per_leaf, uint64_t seed = 42) {
  std::vector<EClassId> leaves;
  leaves.reserve(static_cast<std::size_t>(num_leaves));

  for (int64_t i = 0; i < num_leaves; ++i) {
    leaves.push_back(g.emplace(Op::Const, static_cast<uint64_t>(i)).eclass_id);
  }

  // Simple deterministic "random" distribution
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
        // Binary ops need two children
        auto other_idx = next_rng() % static_cast<uint64_t>(leaves.size());
        auto other_leaf = leaves[other_idx];
        g.emplace(sym, {leaf_id, other_leaf});
      }
    }
  }
}

// ============================================================================
// Pattern Builders
// ============================================================================

inline auto PatternAdd() { return TestPattern::build(Op::Add, {Var{kX}, Var{kY}}); }

inline auto PatternAddSameVar() { return TestPattern::build(Op::Add, {Var{kX}, Var{kX}}); }

inline auto PatternDoubleNeg() { return TestPattern::build(Op::Neg, {Sym(Op::Neg, Var{kX})}, kRootDoubleNeg); }

inline auto PatternSelective() { return TestPattern::build(Op::Add, {Sym(Op::Neg, Var{kX}), Var{kY}}); }

inline auto PatternNestedNeg(int depth) -> TestPattern {
  auto b = TestPattern::Builder{};
  auto cur = b.var(kX);
  for (int i = 0; i < depth; ++i) cur = b.sym(Op::Neg, {cur});
  return std::move(b).build();
}

inline auto PatternNeg() { return TestPattern::build(Op::Neg, {Var{kX}}); }

inline auto PatternNestedF() { return TestPattern::build(Op::F, {Sym(Op::F, Var{kX})}); }

// Pattern for shallow F: F(?x)
inline auto PatternShallowF() { return TestPattern::build(Op::F, {Var{kX}}); }

// Pattern for deep nested F: F(F(F(F(?x)))) - 4 levels deep
inline auto PatternDeepNestedF() { return TestPattern::build(Op::F, {Sym(Op::F, Sym(Op::F, Sym(Op::F, Var{kX})))}); }

// ============================================================================
// Rule Builders
// ============================================================================

inline auto RuleDoubleNeg() {
  return TestRewriteRule::Builder{"double_neg"}
      .pattern(PatternDoubleNeg())
      .apply([](TestRuleContext &ctx, Match const &m) { ctx.merge(m[kRootDoubleNeg], m[kX]); });
}

inline auto RuleNoOp() {
  return TestRewriteRule::Builder{"noop"}.pattern(PatternAdd()).apply([](TestRuleContext &, Match const &) {});
}

inline auto RuleMergeAddMul() {
  return TestRewriteRule::Builder{"merge_add_mul"}
      .pattern(TestPattern::build(Op::Add, {Var{kX}, Var{kY}}, kRootAdd), "add")
      .pattern(TestPattern::build(Op::Mul, {Var{kX}, Var{kY}}, kRootMul), "mul")
      .apply([](TestRuleContext &ctx, Match const &m) { ctx.merge(m[kRootAdd], m[kRootMul]); });
}

inline auto RuleThreePattern() {
  return TestRewriteRule::Builder{"three"}
      .pattern(TestPattern::build(Op::Add, {Var{kX}, Var{kY}}, kRootAdd), "add")
      .pattern(TestPattern::build(Op::Mul, {Var{kX}, Var{kZ}}, kRootMul), "mul")
      .pattern(TestPattern::build(Op::Neg, {Var{kX}}, kRootNeg), "neg")
      .apply([](TestRuleContext &, Match const &) {});
}

inline auto RuleCartesian() {
  return TestRewriteRule::Builder{"cartesian"}
      .pattern(TestPattern::build(Op::Add, {Var{kX}, Var{kY}}, kRootAdd), "add")
      .pattern(TestPattern::build(Op::Neg, {Var{kZ}}), "neg")
      .apply([](TestRuleContext &, Match const &) {});
}

inline auto RuleWideJoin() {
  return TestRewriteRule::Builder{"wide"}
      .pattern(TestPattern::build(Op::Add, {Var{kX}, Var{kY}}, kRootAdd), "add")
      .pattern(TestPattern::build(Op::Mul, {Var{kX}, Var{kZ}}, kRootMul), "mul")
      .apply([](TestRuleContext &, Match const &) {});
}

// ============================================================================
// Benchmark Abstractions
// ============================================================================

// Run a matching benchmark with fresh or reused EMatchContext.
// ApplyFn signature: void(EMatchContext&)
template <typename ApplyFn>
void BenchmarkWithMatchContext(benchmark::State &state, int64_t context_mode, EMatchContext &reusable_context,
                               ApplyFn &&apply_fn) {
  if (context_mode == ranges::kReusedCtx) {
    for (auto _ : state) {
      reusable_context.clear();
      apply_fn(reusable_context);
    }
  } else {
    for (auto _ : state) {
      EMatchContext fresh_context;
      apply_fn(fresh_context);
    }
  }
}

// Run a rewrite benchmark with fresh or reused RewriteContext.
// ApplyFn signature: void(TestRewriteContext&)
template <typename ApplyFn>
void BenchmarkWithRewriteContext(benchmark::State &state, int64_t context_mode, TestRewriteContext &reusable_context,
                                 ApplyFn &&apply_fn) {
  if (context_mode == ranges::kReusedCtx) {
    for (auto _ : state) {
      reusable_context.clear_new_eclasses();
      apply_fn(reusable_context);
    }
  } else {
    for (auto _ : state) {
      TestRewriteContext fresh_context;
      apply_fn(fresh_context);
    }
  }
}

// Base fixture providing common e-graph and matcher setup.
class MatcherFixtureBase : public benchmark::Fixture {
 protected:
  TestEGraph egraph_;
  std::unique_ptr<TestEMatcher> matcher_;
  EMatchContext match_context_;
  TestMatches matches_;

  void ResetEGraph() { egraph_ = TestEGraph{}; }

  void CreateMatcher() { matcher_ = std::make_unique<TestEMatcher>(egraph_); }

  template <typename BuilderFn>
  void SetupGraphAndMatcher(BuilderFn &&build_fn) {
    ResetEGraph();
    build_fn(egraph_);
    CreateMatcher();
  }
};

// Base fixture for rewrite benchmarks.
class RewriterFixtureBase : public benchmark::Fixture {
 protected:
  TestEGraph egraph_;
  std::unique_ptr<TestEMatcher> matcher_;
  std::unique_ptr<TestRewriter> rewriter_;
  TestRewriteContext rewrite_context_;

  void ResetEGraph() { egraph_ = TestEGraph{}; }

  void CreateMatcher() { matcher_ = std::make_unique<TestEMatcher>(egraph_); }

  void CreateRewriter(TestRuleSet const &rules) { rewriter_ = std::make_unique<TestRewriter>(egraph_, rules); }

  template <typename BuilderFn>
  void SetupGraphAndMatcher(BuilderFn &&build_fn) {
    ResetEGraph();
    build_fn(egraph_);
    CreateMatcher();
  }

  template <typename BuilderFn>
  void SetupGraphAndRewriter(BuilderFn &&build_fn, TestRuleSet const &rules) {
    ResetEGraph();
    build_fn(egraph_);
    CreateRewriter(rules);
  }
};

}  // namespace memgraph::planner::bench
