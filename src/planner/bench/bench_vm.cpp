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
#include "planner/pattern/vm/parent_index.hpp"

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
  VMExecutorVerify<Op, NoAnalysis> executor(egraph_);
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
  VMExecutorVerify<Op, NoAnalysis> executor(egraph_);
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
  VMExecutorVerify<Op, NoAnalysis> executor(egraph_);
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
  VMExecutorVerify<Op, NoAnalysis> executor(egraph_);
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
  VMExecutorVerify<Op, NoAnalysis> executor(egraph_);
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

// VM Fused approach: Use FusedPatternCompiler with parent traversal
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

    // Create anchor and joined patterns
    auto anchor = TestPattern::build(Op::Bind, {Wildcard{}, Var{kVarSym}, Var{kVarExpr}}, kBindRoot);
    auto joined = TestPattern::build(Op::Ident, {Var{kVarSym}}, kIdentRoot);

    // Compile with fused compiler
    FusedPatternCompiler<Op> fused_compiler;
    compiled_ = fused_compiler.compile(anchor, {joined}, {kVarSym});

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
  VMExecutorVerify<Op, NoAnalysis> executor(egraph_);
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
