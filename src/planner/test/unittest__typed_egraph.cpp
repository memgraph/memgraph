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

// Tests for TypedEGraph, the typed Make<S>() wrapper over the core e-graph.
// Structural hash-consing (children, order, symbol distinctness) belongs to the
// core EGraph and is covered in unittest__egraph.cpp; here we test only what the
// typed layer itself owns:
//   - Make lowers to the same node a hand-written core emplace would produce.
//   - Make's disambiguator branch, reached via a stateful interning trait.
//   - storage<S>() recovers the per-symbol side-data Make wrote.
//   - SymbolMakeTraits rejects ill-formed traits at the constraint.

#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>
#include <boost/unordered/unordered_flat_set.hpp>

#include "planner/rewrite/rewriter.hpp"
#include "planner/rewrite/rule_context.hpp"
#include "test_support/op_make_traits.hpp"
#include "utils/small_vector.hpp"

namespace memgraph::planner::core {
namespace {

// A minimal symbol set whose only trait interns user data: make() records the
// name in per-symbol storage and returns the interned id as the disambiguator.
// This reaches the half of Make's dispatch that the production-like `Op` set
// (no storage, no disambiguator) never exercises.
enum class ToyOp : std::uint8_t { Symbol };

struct ToyAnalysis {
  void merge(ToyAnalysis const & /*other*/) {}
};

template <ToyOp S>
struct toy_traits;

template <>
struct toy_traits<ToyOp::Symbol> {
  struct storage_type {
    std::map<std::string, std::uint64_t> by_name;
    std::uint64_t next_id = 0;
  };

  static auto make(storage_type &s, std::string_view name) -> MakeResult<ToyAnalysis> {
    auto [it, inserted] = s.by_name.try_emplace(std::string{name}, s.next_id);
    if (inserted) {
      ++s.next_id;
    }
    return {.lowered = {.children = {}, .disambiguator = it->second}};
  }
};

using ToyEGraph = TypedEGraph<ToyOp, ToyAnalysis, SymbolSequence<ToyOp, ToyOp::Symbol>, toy_traits>;

// A trait whose make() returns the wrong type, for the negative concept check.
struct BadTraits {
  struct storage_type {};

  static auto make(storage_type &) -> int { return 0; }
};

// A symbol set that carries a real fact, for the analysis-refresh tests. `Leaf`
// knows its own value; `Opaque` knows nothing; `Sum` derives its value from
// both operands, so its fact depends on facts held by other e-classes.
enum class FactOp : std::uint8_t { Leaf, Opaque, Sum };

struct FactAnalysis {
  std::optional<int> known;

  void merge(FactAnalysis const &other) {
    if (!known) known = other.known;
  }

  auto operator==(FactAnalysis const &) const -> bool = default;
};

template <FactOp S>
struct fact_traits;

template <>
struct fact_traits<FactOp::Leaf> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, int value) -> MakeResult<FactAnalysis> {
    return {.lowered = {.children = {}, .disambiguator = static_cast<std::uint64_t>(value)},
            .seed = FactAnalysis{.known = value}};
  }
};

template <>
struct fact_traits<FactOp::Opaque> {
  struct storage_type {};

  static auto make(storage_type & /*s*/) -> MakeResult<FactAnalysis> {
    return {.lowered = {.children = {}, .disambiguator = std::nullopt}};
  }
};

template <>
struct fact_traits<FactOp::Sum> {
  struct storage_type {};

  static auto make(storage_type & /*s*/, EClassId lhs, EClassId rhs) -> MakeResult<FactAnalysis> {
    return {.lowered = {.children = utils::small_vector<EClassId>{lhs, rhs}, .disambiguator = std::nullopt}};
  }

  /// The value a Sum's own children establish. Answering false is the normal
  /// case: an operand whose value is unknown leaves the sum unknown, and a sum
  /// already carrying its value has nothing to add.
  static auto update(EGraph<FactOp, FactAnalysis> const &eg, storage_type const & /*s*/,
                     std::span<EClassId const> children, std::uint64_t /*disambiguator*/, FactAnalysis &out) -> bool {
    if (out.known || children.size() != 2) return false;
    auto const lhs = eg.analysis_of(children[0]).known;
    auto const rhs = eg.analysis_of(children[1]).known;
    if (!lhs || !rhs) return false;
    out.known = *lhs + *rhs;
    return true;
  }
};

using FactEGraph =
    TypedEGraph<FactOp, FactAnalysis, SymbolSequence<FactOp, FactOp::Leaf, FactOp::Opaque, FactOp::Sum>, fact_traits>;

using FactCore = EGraph<FactOp, FactAnalysis>;

// Traits whose update() is a near miss, each wrong in one way a trait author
// could plausibly write by accident.
struct UpdateReturnsVoid {
  struct storage_type {};

  static void update(FactCore const & /*eg*/, storage_type const & /*s*/, std::span<EClassId const> /*children*/,
                     std::uint64_t /*disambiguator*/, FactAnalysis & /*out*/) {}
};

struct UpdateTakesAnalysisByValue {
  struct storage_type {};

  static auto update(FactCore const & /*eg*/, storage_type const & /*s*/, std::span<EClassId const> /*children*/,
                     std::uint64_t /*disambiguator*/, FactAnalysis /*out*/) -> bool {
    return false;
  }
};

struct UpdateOmitsTheChildren {
  struct storage_type {};

  static auto update(FactCore const & /*eg*/, storage_type const & /*s*/, std::uint64_t /*disambiguator*/,
                     FactAnalysis & /*out*/) -> bool {
    return false;
  }
};

/// A trait carrying the children-only shape, which reads as complete on its own
/// and cannot recover which e-node is asking.
struct UpdateOmitsTheNodeIdentity {
  struct storage_type {};

  static auto update(FactCore const & /*eg*/, std::span<EClassId const> /*children*/, FactAnalysis & /*out*/) -> bool {
    return false;
  }
};

using FactPattern = pattern::Pattern<FactOp>;
using FactRule = rewrite::RewriteRule<FactEGraph>;
using FactRuleCtx = rewrite::RuleContext<FactEGraph>;
using FactRuleSet = rewrite::RuleSet<FactEGraph>;
using FactRewriter = rewrite::Rewriter<FactEGraph>;

constexpr pattern::PatternVar kRoot{0};

/// Replaces a Sum with the leaf of its value. Gated on the fact rather than on
/// the operands' shape, the way a constant fold is: it matches every Sum and
/// acts only once the value is known. Declines once it has, so saturation
/// settles.
auto fold_known_sum() -> FactRule {
  return FactRule::Builder{"fold_known_sum"}
      .pattern(FactPattern::build(kRoot, FactOp::Sum, {pattern::Wildcard{}, pattern::Wildcard{}}))
      .apply([](FactRuleCtx &ctx, pattern::Match const &match) {
        auto const known = ctx.analysis(match[kRoot]).known;
        if (!known) return;
        auto const folded = ctx.Make<FactOp::Leaf>(*known);
        if (ctx.find(match[kRoot]) == ctx.find(folded)) return;
        ctx.merge(match[kRoot], folded);
      });
}

}  // namespace

// === Make: lowering parity with the core e-graph (S1) ===

TEST(TypedEGraph, MakeMatchesRawEmplace) {
  test::TypedTestEGraph eg;
  auto const x = eg.Make<test::Op::Var>().eclass_id;
  auto const y = eg.Make<test::Op::Const>().eclass_id;

  // Make<S>(args...) lowers to exactly the node a caller would emplace by hand.
  auto const sum = eg.Make<test::Op::Add>(x, y).eclass_id;
  auto const sum_raw = eg.core().emplace(test::Op::Add, utils::small_vector<EClassId>{x, y}).eclass_id;
  EXPECT_EQ(sum, sum_raw);

  // Children are positional: the wrapper does not canonicalise their order.
  EXPECT_NE(sum, eg.Make<test::Op::Add>(y, x).eclass_id);

  // A vector-argument trait forwards its children the same way, so identical
  // n-ary nodes hash-cons to one class.
  EXPECT_EQ(eg.Make<test::Op::F>(std::vector<EClassId>{x, y}).eclass_id,
            eg.Make<test::Op::F>(std::vector<EClassId>{x, y}).eclass_id);
}

// === Make: the disambiguator branch via interning (S2, S3) ===

TEST(TypedEGraph, InterningReusesClassForSameKey) {
  ToyEGraph eg;
  auto const x1 = eg.Make<ToyOp::Symbol>(std::string_view{"x"});
  auto const x2 = eg.Make<ToyOp::Symbol>(std::string_view{"x"});
  // Make surfaces the hash-cons outcome: a reused key is a hit, not an insert.
  EXPECT_TRUE(x1.did_insert);
  EXPECT_FALSE(x2.did_insert);
  EXPECT_EQ(x1.eclass_id, x2.eclass_id);
  EXPECT_EQ(eg.core().num_classes(), 1u);
}

TEST(TypedEGraph, InterningSeparatesDistinctKeys) {
  ToyEGraph eg;
  auto const x = eg.Make<ToyOp::Symbol>(std::string_view{"x"});
  auto const y = eg.Make<ToyOp::Symbol>(std::string_view{"y"});
  EXPECT_NE(x.eclass_id, y.eclass_id);
  EXPECT_EQ(eg.core().num_classes(), 2u);
}

// === storage<S>(): per-symbol side-data recovery (S4) ===

TEST(TypedEGraph, StorageReflectsInternedEntries) {
  ToyEGraph eg;
  eg.Make<ToyOp::Symbol>(std::string_view{"alice"});
  eg.Make<ToyOp::Symbol>(std::string_view{"bob"});

  auto const &store = eg.storage<ToyOp::Symbol>();
  EXPECT_EQ(store.next_id, 2u);
  EXPECT_TRUE(store.by_name.contains("alice"));
  EXPECT_TRUE(store.by_name.contains("bob"));
}

// === SymbolMakeTraits: trait protocol enforcement (S5) ===

static_assert(SymbolMakeTraits<toy_traits<ToyOp::Symbol>, ToyAnalysis, std::string_view>,
              "a well-formed interning trait satisfies the protocol");
static_assert(!SymbolMakeTraits<BadTraits, ToyAnalysis>, "make() not returning a MakeResult is rejected");

// === SymbolUpdatesAnalysis: the optional half of the protocol ===

// An update the concept declines is replaced by a no-op, so a shape below that
// failed to be rejected would be a symbol whose facts silently stop being
// recomputed, with no error to read.
static_assert(SymbolUpdatesAnalysis<fact_traits<FactOp::Sum>, FactAnalysis, FactCore>,
              "a well-formed update satisfies the protocol");
static_assert(!SymbolUpdatesAnalysis<fact_traits<FactOp::Leaf>, FactAnalysis, FactCore>,
              "a symbol offering no update is not required to have one");
static_assert(!SymbolUpdatesAnalysis<UpdateReturnsVoid, FactAnalysis, FactCore>,
              "an update that reports no change is rejected");
static_assert(!SymbolUpdatesAnalysis<UpdateTakesAnalysisByValue, FactAnalysis, FactCore>,
              "an update that widens a copy of the facts is rejected");
static_assert(!SymbolUpdatesAnalysis<UpdateOmitsTheChildren, FactAnalysis, FactCore>,
              "an update that cannot read its children is rejected");
static_assert(!SymbolUpdatesAnalysis<UpdateOmitsTheNodeIdentity, FactAnalysis, FactCore>,
              "an update that cannot tell which e-node is asking is rejected");

static_assert(!AnalysisUpdate<FactAnalysis, FactCore, UpdateReturnsVoid>::updates);
static_assert(AnalysisUpdate<FactAnalysis, FactCore, fact_traits<FactOp::Sum>>::updates);

// === RewritableGraph: only the interning wrapper is rewritable ===

// The rewrite engine drives a TypedEGraph: it exposes symbol/analysis types and
// a core() the engine mutates. A bare EGraph is not directly rewritable - it has
// no core() - so new nodes are always minted through the typed Make<S>, which
// seeds analysis.
static_assert(rewrite::RewritableGraph<test::TypedTestEGraph>);
static_assert(!rewrite::RewritableGraph<EGraph<test::Op, test::NoAnalysis>>);

template <typename Ctx>
concept CtxCanMakeVar = requires(Ctx ctx) { ctx.template Make<test::Op::Var>(); };

// A rule's context over a TypedEGraph offers Make<S> to intern new nodes.
static_assert(CtxCanMakeVar<rewrite::RuleContext<test::TypedTestEGraph>>);

TEST(TypedEGraph, RefreshDerivesAFactForAFreshlyMadeNode) {
  FactEGraph eg;
  auto refresh_ctx = BaseProcessingContext{};

  auto const one = eg.Make<FactOp::Leaf>(1).eclass_id;
  auto const five = eg.Make<FactOp::Leaf>(5).eclass_id;

  // No merge is involved, and `make` seeds a Sum with nothing, so only
  // `update` can say 1 + 5: a new e-class has to be visited on its own
  // account.
  auto const sum = eg.Make<FactOp::Sum>(one, five).eclass_id;
  ASSERT_EQ(eg.refresh_analysis(refresh_ctx), 1U);
  ASSERT_TRUE(eg.core().analysis_of(sum).known.has_value());
  EXPECT_EQ(*eg.core().analysis_of(sum).known, 6);
}

// === refresh_analysis: the phase costs nothing when no symbol updates ===

// The walk itself, not just the per-symbol body, is dropped for a symbol set
// with no update. Without this the refresh would visit every e-class and
// every e-node of every graph each pass to establish that it has no work.
static_assert(!test::TypedTestEGraph::updates_analysis);
static_assert(FactEGraph::updates_analysis);

TEST(TypedEGraph, RefreshIsANoOpWithoutAnUpdate) {
  test::TypedTestEGraph eg;
  auto refresh_ctx = BaseProcessingContext{};
  auto const x = eg.Make<test::Op::Var>().eclass_id;
  eg.Make<test::Op::Add>(x, eg.Make<test::Op::Const>().eclass_id);
  EXPECT_EQ(eg.refresh_analysis(refresh_ctx), 0U);
}

// === refresh_analysis: a fact that only becomes derivable later ===

TEST(TypedEGraph, RefreshRederivesAFactAMergeEnabled) {
  FactEGraph eg;
  auto refresh_ctx = BaseProcessingContext{};

  auto const one = eg.Make<FactOp::Leaf>(1).eclass_id;
  auto const opaque = eg.Make<FactOp::Opaque>().eclass_id;

  // Sum derives from both operands, and one of them knows nothing, so the
  // make-time seed declines.
  auto const sum = eg.Make<FactOp::Sum>(one, opaque).eclass_id;
  EXPECT_FALSE(eg.core().analysis_of(sum).known.has_value());

  // Proving the opaque operand equal to a known leaf gives that e-class a
  // fact. Merging leaves needs no rebuild: congruence is not what is under
  // test, only whose analysis the merge reaches.
  auto const five = eg.Make<FactOp::Leaf>(5).eclass_id;
  eg.core().merge(opaque, five);
  ASSERT_EQ(eg.core().analysis_of(opaque).known, 5);

  // The merge refined the analysis of the class it merged, and nothing
  // revisited the parent whose own fact was derived from it.
  EXPECT_FALSE(eg.core().analysis_of(sum).known.has_value());

  eg.refresh_analysis(refresh_ctx);
  ASSERT_TRUE(eg.core().analysis_of(sum).known.has_value());
  EXPECT_EQ(*eg.core().analysis_of(sum).known, 6);
}

TEST(TypedEGraph, RefreshPropagatesThroughTwoLevelsInOneCall) {
  FactEGraph eg;
  auto refresh_ctx = BaseProcessingContext{};

  auto const one = eg.Make<FactOp::Leaf>(1).eclass_id;
  auto const opaque = eg.Make<FactOp::Opaque>().eclass_id;
  auto const inner = eg.Make<FactOp::Sum>(one, opaque).eclass_id;
  auto const outer = eg.Make<FactOp::Sum>(inner, one).eclass_id;

  auto const five = eg.Make<FactOp::Leaf>(5).eclass_id;
  eg.core().merge(opaque, five);

  // Only the inner sum reads the merged class. The outer sum is reached because
  // the inner sum's own facts moved during the walk, so a single call settles
  // both levels instead of leaving the outer fact to a later pass.
  ASSERT_EQ(eg.refresh_analysis(refresh_ctx), 2U);
  ASSERT_TRUE(eg.core().analysis_of(outer).known.has_value());
  EXPECT_EQ(*eg.core().analysis_of(outer).known, 7);
}

TEST(TypedEGraph, RefreshMarksTheClassesItChangedTouched) {
  FactEGraph eg;
  auto refresh_ctx = BaseProcessingContext{};

  auto const one = eg.Make<FactOp::Leaf>(1).eclass_id;
  auto const opaque = eg.Make<FactOp::Opaque>().eclass_id;
  auto const sum = eg.Make<FactOp::Sum>(one, opaque).eclass_id;
  auto const five = eg.Make<FactOp::Leaf>(5).eclass_id;
  eg.core().merge(opaque, five);

  // Building each e-class and merging announced those classes already, so the
  // set has to be drained for what it holds next to be the refresh's own doing.
  // The refresh propagates from its separate change set, which draining leaves
  // alone - the reason the two sets are not one.
  eg.core().clear_touched();
  ASSERT_EQ(eg.refresh_analysis(refresh_ctx), 1U);

  // A fact that moves without being announced leaves every rule gated on it
  // unarmed. The sum is the one class whose facts the refresh widened, so it is
  // exactly what the set should now hold.
  auto touched = boost::unordered_flat_set<EClassId>{};
  eg.core().touched_eclasses_into(touched);
  EXPECT_TRUE(touched.contains(eg.core().find(sum)));
  EXPECT_EQ(touched.size(), 1U);
}

// === clear(): pending work does not outlive the graph it belonged to ===

TEST(TypedEGraph, ClearDropsThePendingAnalysisWork) {
  FactEGraph eg;
  auto const one = eg.Make<FactOp::Leaf>(1).eclass_id;
  auto const opaque = eg.Make<FactOp::Opaque>().eclass_id;
  eg.Make<FactOp::Sum>(one, opaque);

  eg.core().clear();

  // Any e-class id a refresh still had queued now names nothing, and the
  // union-find it would canonicalise through has been emptied as well, so
  // carrying one over reads a vacated slot rather than finding no work.
  auto refresh_ctx = BaseProcessingContext{};
  EXPECT_EQ(eg.refresh_analysis(refresh_ctx), 0U);

  // Ids restart from zero after a clear, so a carried-over id would not merely
  // dangle: it would name one of these fresh e-classes.
  auto const two = eg.Make<FactOp::Leaf>(2).eclass_id;
  auto const three = eg.Make<FactOp::Leaf>(3).eclass_id;
  auto const sum = eg.Make<FactOp::Sum>(two, three).eclass_id;
  EXPECT_EQ(eg.refresh_analysis(refresh_ctx), 1U);
  ASSERT_TRUE(eg.core().analysis_of(sum).known.has_value());
  EXPECT_EQ(*eg.core().analysis_of(sum).known, 5);
}

TEST(TypedEGraph, SaturationFiresARuleTheRefreshEnabled) {
  FactEGraph eg;
  auto const one = eg.Make<FactOp::Leaf>(1).eclass_id;
  auto const opaque = eg.Make<FactOp::Opaque>().eclass_id;
  auto const sum = eg.Make<FactOp::Sum>(one, opaque).eclass_id;
  ASSERT_FALSE(eg.core().analysis_of(sum).known.has_value());

  // Proves the opaque operand equal to a known leaf, which is what makes the
  // sum's own fact derivable. Declines once it has, so saturation settles.
  auto reveal = FactRule::Builder{"reveal_opaque"}
                    .pattern(FactPattern::build(kRoot, FactOp::Opaque))
                    .apply([](FactRuleCtx &ctx, pattern::Match const &match) {
                      if (ctx.analysis(match[kRoot]).known) return;
                      ctx.merge(match[kRoot], ctx.Make<FactOp::Leaf>(5));
                    });

  FactRewriter rewriter{eg, FactRuleSet::Build(std::move(reveal), fold_known_sum())};
  rewriter.saturate_incremental(rewrite::RewriteConfig::Unlimited());

  // On the pass that revealed the operand, the sum's fact was still unknown and
  // the fold declined. It can only have fired if the refresh ran inside the
  // pass loop and announced the new fact in time to arm it.
  ASSERT_TRUE(eg.core().analysis_of(sum).known.has_value());
  EXPECT_EQ(*eg.core().analysis_of(sum).known, 6);
  EXPECT_EQ(eg.core().find(sum), eg.core().find(eg.Make<FactOp::Leaf>(6).eclass_id));
}

// A fact nothing in the rewrite had to establish: both operands were known
// before the rewriter was handed the graph, so the fold's gate is shut only
// because no refresh has run yet. The first pass therefore rewrites nothing,
// and a loop that reads that as a fixpoint strands the fold behind a fact the
// very same pass went on to derive. Driven under both drivers, since the stop
// condition belongs to the loop they share.
TEST(TypedEGraph, SaturationFoldsAFactNoRewriteHadToDerive) {
  for (auto const incremental : {false, true}) {
    FactEGraph eg;
    auto const one = eg.Make<FactOp::Leaf>(1).eclass_id;
    auto const five = eg.Make<FactOp::Leaf>(5).eclass_id;
    auto const sum = eg.Make<FactOp::Sum>(one, five).eclass_id;
    ASSERT_FALSE(eg.core().analysis_of(sum).known.has_value());

    FactRewriter rewriter{eg, FactRuleSet::Build(fold_known_sum())};
    auto const config = rewrite::RewriteConfig::Unlimited();
    auto const result = incremental ? rewriter.saturate_incremental(config) : rewriter.saturate_full(config);

    EXPECT_TRUE(result.saturated()) << "incremental=" << incremental;
    ASSERT_TRUE(eg.core().analysis_of(sum).known.has_value()) << "incremental=" << incremental;
    EXPECT_EQ(*eg.core().analysis_of(sum).known, 6) << "incremental=" << incremental;
    EXPECT_EQ(eg.core().find(sum), eg.core().find(eg.Make<FactOp::Leaf>(6).eclass_id))
        << "the fold never ran, incremental=" << incremental;
  }
}

// The same facts under a one-pass budget, which a caller capping iterations
// gets. A rule gated on a fact the graph arrived with has to be armed and fired
// within that pass, so the facts have to be settled before the pass opens
// rather than at its end.
TEST(TypedEGraph, OnePassFiresARuleGatedOnAFactTheGraphArrivedWith) {
  FactEGraph eg;
  auto const one = eg.Make<FactOp::Leaf>(1).eclass_id;
  auto const five = eg.Make<FactOp::Leaf>(5).eclass_id;
  auto const sum = eg.Make<FactOp::Sum>(one, five).eclass_id;

  FactRewriter rewriter{eg, FactRuleSet::Build(fold_known_sum())};
  auto config = rewrite::RewriteConfig::Unlimited();
  config.max_iterations = 1;
  auto const result = rewriter.saturate_incremental(config);

  EXPECT_EQ(result.rewrites_applied, 1U);
  EXPECT_EQ(eg.core().find(sum), eg.core().find(eg.Make<FactOp::Leaf>(6).eclass_id));
}

TEST(TypedEGraph, SaturationClaimsAFactWhenNoMergePrompted) {
  FactEGraph eg;
  auto const one = eg.Make<FactOp::Leaf>(1).eclass_id;
  auto const five = eg.Make<FactOp::Leaf>(5).eclass_id;
  eg.Make<FactOp::Opaque>();

  // Builds a term and merges nothing, which leaves congruence no work at all.
  // Fires once, so that saturation settles whatever the refresh does with it.
  auto fired = false;
  auto build = FactRule::Builder{"build_a_sum"}
                   .pattern(FactPattern::build(kRoot, FactOp::Opaque))
                   .apply([&fired, one, five](FactRuleCtx &ctx, pattern::Match const & /*match*/) {
                     if (fired) return;
                     fired = true;
                     ctx.Make<FactOp::Sum>(one, five);
                   });

  FactRewriter rewriter{eg, FactRuleSet::Build(std::move(build))};
  rewriter.saturate_incremental(rewrite::RewriteConfig::Unlimited());
  ASSERT_TRUE(fired);

  // Both operands were known when the sum was built, so only `update` can say
  // 1 + 5. Conditioning the refresh on a pending rebuild would strand this
  // fact: a pass that only interns leaves nothing for congruence to do, and
  // the rewrite loop requires a clean graph to begin one.
  auto const sum = eg.Make<FactOp::Sum>(one, five).eclass_id;
  ASSERT_TRUE(eg.core().analysis_of(sum).known.has_value());
  EXPECT_EQ(*eg.core().analysis_of(sum).known, 6);

  // Saturation reporting itself done means no fact is still derivable.
  auto refresh_ctx = BaseProcessingContext{};
  EXPECT_EQ(eg.refresh_analysis(refresh_ctx), 0U);
}

// The same pass that mints content without merging, with a rule gated on the
// fact that content yields. Minting is not counted as a rewrite, so the pass
// reports nothing done while having moved a fact, and only a fixpoint test that
// reads both leaves the gated rule a pass to run in.
TEST(TypedEGraph, SaturationFiresARuleGatedOnAFactAMintedTermYielded) {
  FactEGraph eg;
  auto const one = eg.Make<FactOp::Leaf>(1).eclass_id;
  auto const five = eg.Make<FactOp::Leaf>(5).eclass_id;
  eg.Make<FactOp::Opaque>();

  auto fired = false;
  auto build = FactRule::Builder{"build_a_sum"}
                   .pattern(FactPattern::build(kRoot, FactOp::Opaque))
                   .apply([&fired, one, five](FactRuleCtx &ctx, pattern::Match const & /*match*/) {
                     if (fired) return;
                     fired = true;
                     ctx.Make<FactOp::Sum>(one, five);
                   });

  FactRewriter rewriter{eg, FactRuleSet::Build(std::move(build), fold_known_sum())};
  rewriter.saturate_incremental(rewrite::RewriteConfig::Unlimited());
  ASSERT_TRUE(fired);

  auto const sum = eg.Make<FactOp::Sum>(one, five).eclass_id;
  EXPECT_EQ(eg.core().find(sum), eg.core().find(eg.Make<FactOp::Leaf>(6).eclass_id));
}

}  // namespace memgraph::planner::core
