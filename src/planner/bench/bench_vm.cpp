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

#include "bench_common.hpp"

#include <array>
#include <optional>

#include "planner/pattern/vm/compiler.hpp"
#include "planner/pattern/vm/executor.hpp"

using namespace memgraph::planner::bench;
using namespace memgraph::planner::bench::ranges;
using namespace memgraph::planner::core::vm;

// ============================================================================
// VM Executor Fixture Base
// ============================================================================

class VMFixtureBase : public benchmark::Fixture {
 protected:
  TestEGraph egraph_;
  EMatchContext match_context_;
  std::vector<PatternMatch> matches_;
  PatternCompiler<Op> compiler_;

  void ResetEGraph() { egraph_ = TestEGraph{}; }

  template <typename BuilderFn>
  void SetupGraph(BuilderFn &&build_fn) {
    ResetEGraph();
    build_fn(egraph_);
  }

  auto GetAllCandidates() -> std::vector<EClassId> {
    std::vector<EClassId> candidates;
    for (auto id : egraph_.canonical_class_ids()) {
      candidates.push_back(id);
    }
    return candidates;
  }
};

// ============================================================================
// Simple Pattern: Add(?x, ?y) - VM Version
// ============================================================================

class VMSimplePatternFixture : public VMFixtureBase {
 protected:
  std::optional<CompiledPattern<Op>> pattern_;
  std::vector<EClassId> candidates_;
  int64_t graph_size_ = 0;
  int64_t context_mode_ = 0;

  void SetUp(const benchmark::State &state) override {
    graph_size_ = state.range(0);
    context_mode_ = state.range(1);
    SetupGraph([this](TestEGraph &g) { BuildIndependentAdds(g, graph_size_); });
    pattern_ = compiler_.compile(PatternAdd());
    candidates_ = GetAllCandidates();
  }
};

BENCHMARK_DEFINE_F(VMSimplePatternFixture, Match)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  if (context_mode_ == kReusedCtx) {
    for (auto _ : state) {
      match_context_.clear();
      matches_.clear();
      executor.execute(*pattern_, candidates_, match_context_, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  } else {
    for (auto _ : state) {
      EMatchContext fresh_context;
      matches_.clear();
      executor.execute(*pattern_, candidates_, fresh_context, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  }
  state.SetItemsProcessed(state.iterations() * graph_size_);
}

BENCHMARK_REGISTER_F(VMSimplePatternFixture, Match)
    ->ArgsProduct({{kSmall, kMedium, kLarge, kMassive}, {kFreshCtx, kReusedCtx}})
    ->ArgNames({"size", "ctx"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Deep Pattern: Neg(Neg(...Neg(?x)...)) - VM Version
// ============================================================================

class VMDeepPatternFixture : public VMFixtureBase {
 protected:
  std::optional<CompiledPattern<Op>> pattern_;
  std::vector<EClassId> candidates_;
  int64_t pattern_depth_ = 0;
  int64_t context_mode_ = 0;

  void SetUp(const benchmark::State &state) override {
    pattern_depth_ = state.range(0);
    context_mode_ = state.range(1);
    SetupGraph([this](TestEGraph &g) { BuildNegChain(g, pattern_depth_); });
    pattern_ = compiler_.compile(PatternNestedNeg(static_cast<int>(pattern_depth_)));
    candidates_ = GetAllCandidates();
  }
};

BENCHMARK_DEFINE_F(VMDeepPatternFixture, Match)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  if (context_mode_ == kReusedCtx) {
    for (auto _ : state) {
      match_context_.clear();
      matches_.clear();
      executor.execute(*pattern_, candidates_, match_context_, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  } else {
    for (auto _ : state) {
      EMatchContext fresh_context;
      matches_.clear();
      executor.execute(*pattern_, candidates_, fresh_context, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  }
  state.SetItemsProcessed(state.iterations());
}

// Note: VM has 64 registers, deep patterns need 2*depth+1 registers, so max depth ~31
BENCHMARK_REGISTER_F(VMDeepPatternFixture, Match)
    ->ArgsProduct({{1, 5, 10, 20, 30}, {kFreshCtx, kReusedCtx}})
    ->ArgNames({"depth", "ctx"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Same Variable: Add(?x, ?x) - VM Version
// ============================================================================

class VMSameVariableFixture : public VMFixtureBase {
 protected:
  std::optional<CompiledPattern<Op>> pattern_;
  std::vector<EClassId> candidates_;
  int64_t graph_size_ = 0;
  int64_t context_mode_ = 0;

  void SetUp(const benchmark::State &state) override {
    graph_size_ = state.range(0);
    context_mode_ = state.range(1);
    SetupGraph([this](TestEGraph &g) { BuildMixedAdds(g, graph_size_); });
    pattern_ = compiler_.compile(PatternAddSameVar());
    candidates_ = GetAllCandidates();
  }
};

BENCHMARK_DEFINE_F(VMSameVariableFixture, Match)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  if (context_mode_ == kReusedCtx) {
    for (auto _ : state) {
      match_context_.clear();
      matches_.clear();
      executor.execute(*pattern_, candidates_, match_context_, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  } else {
    for (auto _ : state) {
      EMatchContext fresh_context;
      matches_.clear();
      executor.execute(*pattern_, candidates_, fresh_context, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  }
  state.SetItemsProcessed(state.iterations() * graph_size_);
}

BENCHMARK_REGISTER_F(VMSameVariableFixture, Match)
    ->ArgsProduct({{kSmall, kMedium, kLarge}, {kFreshCtx, kReusedCtx}})
    ->ArgNames({"size", "ctx"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Merged E-Graph (multiple e-nodes per class) - VM Version
// ============================================================================

class VMMergedEGraphFixture : public VMFixtureBase {
 protected:
  std::optional<CompiledPattern<Op>> pattern_;
  std::vector<EClassId> candidates_;
  int64_t graph_size_ = 0;
  int64_t context_mode_ = 0;

  void SetUp(const benchmark::State &state) override {
    graph_size_ = state.range(0);
    context_mode_ = state.range(1);
    SetupGraph([this](TestEGraph &g) { BuildMergedAddMul(g, graph_size_); });
    pattern_ = compiler_.compile(PatternAdd());
    candidates_ = GetAllCandidates();
  }
};

BENCHMARK_DEFINE_F(VMMergedEGraphFixture, Match)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  if (context_mode_ == kReusedCtx) {
    for (auto _ : state) {
      match_context_.clear();
      matches_.clear();
      executor.execute(*pattern_, candidates_, match_context_, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  } else {
    for (auto _ : state) {
      EMatchContext fresh_context;
      matches_.clear();
      executor.execute(*pattern_, candidates_, fresh_context, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  }
  state.SetItemsProcessed(state.iterations() * graph_size_);
}

BENCHMARK_REGISTER_F(VMMergedEGraphFixture, Match)
    ->ArgsProduct({{kSmall, kMedium, kLarge}, {kFreshCtx, kReusedCtx}})
    ->ArgNames({"size", "ctx"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Selective Pattern: Add(Neg(?x), ?y) - VM Version
// ============================================================================

class VMSelectivePatternFixture : public VMFixtureBase {
 protected:
  std::optional<CompiledPattern<Op>> pattern_;
  std::vector<EClassId> candidates_;
  int64_t graph_size_ = 0;
  int64_t context_mode_ = 0;

  void SetUp(const benchmark::State &state) override {
    graph_size_ = state.range(0);
    context_mode_ = state.range(1);
    SetupGraph([this](TestEGraph &g) { BuildAddsWithOneNeg(g, graph_size_); });
    pattern_ = compiler_.compile(PatternSelective());
    candidates_ = GetAllCandidates();
  }
};

BENCHMARK_DEFINE_F(VMSelectivePatternFixture, Match)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  if (context_mode_ == kReusedCtx) {
    for (auto _ : state) {
      match_context_.clear();
      matches_.clear();
      executor.execute(*pattern_, candidates_, match_context_, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  } else {
    for (auto _ : state) {
      EMatchContext fresh_context;
      matches_.clear();
      executor.execute(*pattern_, candidates_, fresh_context, matches_);
      benchmark::DoNotOptimize(matches_);
    }
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(VMSelectivePatternFixture, Match)
    ->ArgsProduct({{kSmall, kMedium, kLarge}, {kFreshCtx, kReusedCtx}})
    ->ArgNames({"size", "ctx"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Multi-Pattern Join: Bind(_, ?sym, ?expr) + ?id = Ident(?sym)
// Compares EMatcher hash-join vs VM fused parent traversal
// ============================================================================

constexpr PatternVar kVarSym{1};
constexpr PatternVar kVarExpr{2};
constexpr PatternVar kVarId{3};
constexpr PatternVar kBindRoot{10};
constexpr PatternVar kIdentRoot{11};

// Build e-graph with N Bind nodes, each referencing a unique symbol.
// For each symbol, create M Ident nodes referencing it.
// This creates N * M join matches.
inline void BuildBindIdentGraph(TestEGraph &g, int64_t num_binds, int64_t idents_per_sym) {
  for (int64_t i = 0; i < num_binds; ++i) {
    auto placeholder = g.emplace(Op::Const, static_cast<uint64_t>(i * 1000)).eclass_id;
    auto sym_val = g.emplace(Op::Const, static_cast<uint64_t>(i)).eclass_id;
    auto expr_val = g.emplace(Op::Const, static_cast<uint64_t>(i + 10000)).eclass_id;
    g.emplace(Op::Bind, {placeholder, sym_val, expr_val});

    // Create multiple Ident nodes for this symbol
    for (int64_t j = 0; j < idents_per_sym; ++j) {
      g.emplace(Op::Ident, {sym_val});
    }
  }
}

// EMatcher approach: Use hash-join via RewriteRule
class BindIdentEMatcherFixture : public RewriterFixtureBase {
 protected:
  int64_t num_binds_ = 0;
  int64_t idents_per_sym_ = 0;
  std::unique_ptr<TestRewriteRule> rule_;

  void SetUp(const benchmark::State &state) override {
    num_binds_ = state.range(0);
    idents_per_sym_ = state.range(1);
    ResetEGraph();
    BuildBindIdentGraph(egraph_, num_binds_, idents_per_sym_);
    CreateMatcher();

    // Create rule: Bind(_, ?sym, ?expr) joined with Ident(?sym)
    rule_ = std::make_unique<TestRewriteRule>(
        TestRewriteRule::Builder{"bind_ident"}
            .pattern(TestPattern::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}, kBindRoot), "bind")
            .pattern(TestPattern::build(Op::Ident, {Var{kVarSym}}, kIdentRoot), "ident")
            .apply([](TestRuleContext &, Match const &) {}));
  }
};

BENCHMARK_DEFINE_F(BindIdentEMatcherFixture, HashJoin)(benchmark::State &state) {
  for (auto _ : state) {
    rewrite_context_.clear_new_eclasses();
    auto rewrites = rule_->apply(egraph_, *matcher_, rewrite_context_);
    benchmark::DoNotOptimize(rewrites);
  }
  state.SetItemsProcessed(state.iterations() * num_binds_ * idents_per_sym_);
}

BENCHMARK_REGISTER_F(BindIdentEMatcherFixture, HashJoin)
    ->Args({10, 1})    // 10 binds, 1 ident each = 10 matches
    ->Args({100, 1})   // 100 binds, 1 ident each = 100 matches
    ->Args({10, 10})   // 10 binds, 10 idents each = 100 matches
    ->Args({100, 10})  // 100 binds, 10 idents each = 1000 matches
    ->Args({1000, 1})  // 1000 binds, 1 ident each = 1000 matches
    ->ArgNames({"binds", "idents_per"})
    ->Unit(benchmark::kMicrosecond);

// VM Fused approach: Use PatternCompiler with parent traversal
class BindIdentVMFusedFixture : public VMFixtureBase {
 protected:
  int64_t num_binds_ = 0;
  int64_t idents_per_sym_ = 0;
  std::optional<CompiledPattern<Op>> compiled_;
  std::vector<EClassId> bind_candidates_;

  void SetUp(const benchmark::State &state) override {
    num_binds_ = state.range(0);
    idents_per_sym_ = state.range(1);
    SetupGraph([this](TestEGraph &g) { BuildBindIdentGraph(g, num_binds_, idents_per_sym_); });

    // Create patterns - compiler will figure out anchor and join order
    auto bind_pattern = TestPattern::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}, kBindRoot);
    auto ident_pattern = TestPattern::build(Op::Ident, {Var{kVarSym}}, kIdentRoot);

    // Compile with pattern compiler
    PatternCompiler<Op> fused_compiler;
    std::array patterns = {bind_pattern, ident_pattern};
    compiled_ = fused_compiler.compile(patterns);

    // Get all Bind e-classes as candidates
    bind_candidates_.clear();
    for (auto id : egraph_.canonical_class_ids()) {
      auto const &eclass = egraph_.eclass(id);
      for (auto enode_id : eclass.nodes()) {
        auto const &enode = egraph_.get_enode(enode_id);
        if (enode.symbol() == Op::Bind) {
          bind_candidates_.push_back(id);
          break;
        }
      }
    }
  }
};

BENCHMARK_DEFINE_F(BindIdentVMFusedFixture, ParentTraversal)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    executor.execute(*compiled_, bind_candidates_, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.SetItemsProcessed(state.iterations() * num_binds_ * idents_per_sym_);
}

BENCHMARK_REGISTER_F(BindIdentVMFusedFixture, ParentTraversal)
    ->Args({10, 1})    // 10 binds, 1 ident each = 10 matches
    ->Args({100, 1})   // 100 binds, 1 ident each = 100 matches
    ->Args({10, 10})   // 10 binds, 10 idents each = 100 matches
    ->Args({100, 10})  // 100 binds, 10 idents each = 1000 matches
    ->Args({1000, 1})  // 1000 binds, 1 ident each = 1000 matches
    ->ArgNames({"binds", "idents_per"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// High Parent Count: Hub node with many parents of different symbols
// ============================================================================
//
// Measures: Symbol filtering effectiveness when a node has many parents.
// Why it matters: Both modes use IterParents + CheckSymbol, iterating all parents
//   and filtering by symbol. Clean mode may use parent index for optimization.
// Variables: parents_f (F parents), parents_neg (Neg parents to match).

class VMHighParentFixture : public VMFixtureBase {
 protected:
  std::optional<CompiledPattern<Op>> pattern_;
  std::vector<EClassId> candidates_;
  int64_t parents_f_ = 0;
  int64_t parents_neg_ = 0;

  void SetUp(const benchmark::State &state) override {
    parents_f_ = state.range(0);
    parents_neg_ = state.range(1);
    SetupGraph([this](TestEGraph &g) { BuildHighParentHub(g, parents_f_, parents_neg_); });
    pattern_ = compiler_.compile(PatternNeg());
    candidates_ = GetAllCandidates();
  }
};

BENCHMARK_DEFINE_F(VMHighParentFixture, Match)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    executor.execute(*pattern_, candidates_, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.SetItemsProcessed(state.iterations() * parents_neg_);
}

BENCHMARK_REGISTER_F(VMHighParentFixture, Match)
    ->Args({1000, 100})   // 1000 F parents, 100 Neg parents
    ->Args({5000, 100})   // 5000 F parents, 100 Neg parents
    ->Args({10000, 100})  // 10000 F parents, 100 Neg parents
    ->ArgNames({"F_parents", "Neg_parents"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Self-Referential E-Class: Pattern matching on cyclic structures
// ============================================================================
//
// Measures: Matching patterns on e-classes that reference themselves.
// Why it matters: Self-referential e-classes (from merges) create unique
//   matching scenarios where the same e-class appears at multiple depths.
// Setup: EC1 = {F(Const), F(EC1)} - e-class contains F node pointing to itself.

class VMSelfRefFixture : public VMFixtureBase {
 protected:
  std::optional<CompiledPattern<Op>> pattern_;
  std::vector<EClassId> candidates_;

  void SetUp(const benchmark::State &) override {
    SetupGraph([](TestEGraph &g) { BuildSelfReferential(g, 42); });
    pattern_ = compiler_.compile(PatternNestedF());
    candidates_ = GetAllCandidates();
  }
};

BENCHMARK_DEFINE_F(VMSelfRefFixture, Match)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    executor.execute(*pattern_, candidates_, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(VMSelfRefFixture, Match)->Unit(benchmark::kNanosecond);

// ============================================================================
// Parent Diversity: Many nodes with diverse parent symbols
// ============================================================================
//
// Measures: Matching when parents are distributed across many symbols.
// Why it matters: Tests how well the symbol index filters vs linear scan.
// Variables: num_leaves, parents_per_leaf (parents randomly distributed).

class VMParentDiversityFixture : public VMFixtureBase {
 protected:
  std::optional<CompiledPattern<Op>> pattern_;
  std::vector<EClassId> candidates_;
  int64_t num_leaves_ = 0;
  int64_t parents_per_leaf_ = 0;

  void SetUp(const benchmark::State &state) override {
    num_leaves_ = state.range(0);
    parents_per_leaf_ = state.range(1);
    SetupGraph([this](TestEGraph &g) { BuildParentDiversity(g, num_leaves_, parents_per_leaf_, 42); });
    pattern_ = compiler_.compile(PatternNeg());
    candidates_ = GetAllCandidates();
  }
};

BENCHMARK_DEFINE_F(VMParentDiversityFixture, Match)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    executor.execute(*pattern_, candidates_, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.SetItemsProcessed(state.iterations());
}

BENCHMARK_REGISTER_F(VMParentDiversityFixture, Match)
    ->Args({100, 20})  // 100 leaves, 20 parents each
    ->Args({500, 20})  // 500 leaves, 20 parents each
    ->Args({100, 50})  // 100 leaves, 50 parents each
    ->ArgNames({"leaves", "parents_per"})
    ->Unit(benchmark::kMicrosecond);

// ============================================================================
// Nested Join Pattern: (F ?v0) JOIN (F (F (F (F ?v0))))
// ============================================================================
//
// This pattern was identified by fuzzer as a VM bottleneck case (only 2-5x
// speedup vs EMatcher, compared to 10-30x for simpler patterns).
//
// Measures: Performance of deep nested patterns with shared variables.
// Why it's slow: Pattern 2 needs 4x LoadChild + CheckSymbol ops, and the
//   shared variable forces join across patterns with deep backtracking.

class VMNestedJoinFixture : public VMFixtureBase {
 protected:
  std::optional<CompiledPattern<Op>> pattern_;
  std::vector<EClassId> candidates_;
  int64_t num_leaves_ = 0;

  void SetUp(const benchmark::State &state) override {
    num_leaves_ = state.range(0);
    SetupGraph([this](TestEGraph &g) { BuildNestedJoinGraph(g, num_leaves_); });

    // Compile the two-pattern join: (F ?v0) JOIN (F (F (F (F ?v0))))
    std::array patterns = {PatternShallowF(), PatternDeepNestedF()};
    pattern_ = compiler_.compile(patterns);
    candidates_ = GetAllCandidates();
  }
};

BENCHMARK_DEFINE_F(VMNestedJoinFixture, Match)(benchmark::State &state) {
  VMExecutor<Op, NoAnalysis> executor(egraph_);
  for (auto _ : state) {
    match_context_.clear();
    matches_.clear();
    executor.execute(*pattern_, candidates_, match_context_, matches_);
    benchmark::DoNotOptimize(matches_);
  }
  state.SetItemsProcessed(state.iterations() * num_leaves_);
}

BENCHMARK_REGISTER_F(VMNestedJoinFixture, Match)
    ->Args({10})   // 10 leaves = 10 matches
    ->Args({50})   // 50 leaves = 50 matches
    ->Args({100})  // 100 leaves = 100 matches
    ->Args({500})  // 500 leaves = 500 matches
    ->ArgNames({"leaves"})
    ->Unit(benchmark::kMicrosecond);
