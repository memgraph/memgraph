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

// Demand-flow tests for plan_v2 cost traits.
//
// Inclusion criterion: a test here pins what `required` / `introduces` /
// `cardinality` a per-symbol cost trait emits for a given operator, asserted
// against the frontier the production cost pass computes - not against a formula
// re-typed into the test. These build the egraph directly and run only the cost
// pass through CostHarness, so a malformed alt fails here, at the trait, rather
// than surfacing downstream as a wrong v1 plan.

#include <gtest/gtest.h>

#include <algorithm>

#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/test_support/cost.hpp"
#include "query/plan_v2/test_support/literals.hpp"

namespace memgraph::query::plan::v2 {
namespace {

// range(1, 3) -> a list of statically-known length 3.
auto Range1To3(egraph &eg) -> eclass { return eg.MakeFunction("range", {IntLit(eg, 1), IntLit(eg, 3)}); }

auto HasIntroducedBit(std::span<Alternative const> alts, uint16_t bit) -> bool {
  return std::ranges::any_of(alts, [bit](Alternative const &a) { return a.introduces.test(bit); });
}

auto WithoutIntroducedBit(std::span<Alternative const> alts, uint16_t bit) -> Alternative const * {
  auto it = std::ranges::find_if(alts, [bit](Alternative const &a) { return !a.introduces.test(bit); });
  return it == alts.end() ? nullptr : &*it;
}

auto WithIntroducedBit(std::span<Alternative const> alts, uint16_t bit) -> Alternative const * {
  auto it = std::ranges::find_if(alts, [bit](Alternative const &a) { return a.introduces.test(bit); });
  return it == alts.end() ? nullptr : &*it;
}

// ===========================================================================
// Bind: one-shot binder. Cardinality passes through; alive introduces the sym
// only when it is referenced, dead is always available.
// ===========================================================================

TEST(BindDemand, ReferencedSymEmitsAliveIntroducingSym) {
  egraph eg;
  auto once = eg.MakeOnce();
  auto x = eg.MakeSymbol(0, "x");
  eg.MakeIdentifier(x);  // x is referenced somewhere -> alive alt is emitted
  auto bind = eg.MakeBind(once, x, IntLit(eg, 42));

  test::CostHarness h{eg, bind};
  auto const alts = h.alts(bind);
  auto const x_bit = h.bit_of(x);

  auto const *alive = WithIntroducedBit(alts, x_bit);
  ASSERT_NE(alive, nullptr) << "referenced Bind must emit an alive alt that introduces its sym";
  EXPECT_DOUBLE_EQ(alive->cardinality, 1.0) << "Bind is one-shot: cardinality passes through";
  EXPECT_TRUE(alive->required.empty()) << "operator Alt has empty required";
}

TEST(BindDemand, UnreferencedSymEmitsOnlyDead) {
  egraph eg;
  auto once = eg.MakeOnce();
  auto x = eg.MakeSymbol(0, "x");  // never referenced
  auto bind = eg.MakeBind(once, x, IntLit(eg, 42));

  test::CostHarness h{eg, bind};
  auto const alts = h.alts(bind);
  auto const x_bit = h.bit_of(x);

  EXPECT_FALSE(HasIntroducedBit(alts, x_bit)) << "unreferenced Bind must not introduce its sym";
  EXPECT_NE(WithoutIntroducedBit(alts, x_bit), nullptr) << "a dead alt is always available";
}

// ===========================================================================
// Unwind: row-generative binder. Always emits an alive alt scaling rows by the
// list length; adds a strictly-cheaper dead alt only when the sym is
// unreferenced and the length is statically known.
// ===========================================================================

TEST(UnwindDemand, ReferencedSymOverKnownLengthScalesRows) {
  egraph eg;
  auto once = eg.MakeOnce();  // cardinality 1
  auto x = eg.MakeSymbol(0, "x");
  eg.MakeIdentifier(x);  // referenced -> no dead alt
  auto uw = eg.MakeUnwind(once, x, Range1To3(eg));

  test::CostHarness h{eg, uw};
  auto const alts = h.alts(uw);
  auto const x_bit = h.bit_of(x);

  auto const *alive = WithIntroducedBit(alts, x_bit);
  ASSERT_NE(alive, nullptr);
  EXPECT_DOUBLE_EQ(alive->cardinality, 3.0) << "row count = input(1) x list length(3)";
  EXPECT_EQ(WithoutIntroducedBit(alts, x_bit), nullptr) << "referenced sym: no dead alt";
}

TEST(UnwindDemand, UnreferencedSymOverKnownLengthAddsCheaperDeadAlt) {
  egraph eg;
  auto once = eg.MakeOnce();
  auto x = eg.MakeSymbol(0, "x");  // unreferenced
  auto uw = eg.MakeUnwind(once, x, Range1To3(eg));

  test::CostHarness h{eg, uw};
  auto const alts = h.alts(uw);
  auto const x_bit = h.bit_of(x);

  auto const *alive = WithIntroducedBit(alts, x_bit);
  auto const *dead = WithoutIntroducedBit(alts, x_bit);
  ASSERT_NE(alive, nullptr);
  ASSERT_NE(dead, nullptr) << "unreferenced sym + known length: dead alt elides the binding";
  EXPECT_DOUBLE_EQ(alive->cardinality, 3.0);
  EXPECT_DOUBLE_EQ(dead->cardinality, 3.0) << "dead Unwind still scales rows by the known length";
  EXPECT_LT(dead->cost, alive->cost) << "dropping the binding work makes the dead alt strictly cheaper";
}

// ===========================================================================
// Output: harvests each NamedOutput's sym into `introduces` (own_syms rule).
// ===========================================================================

TEST(OutputDemand, IntroducesEachNamedOutputSym) {
  egraph eg;
  auto once = eg.MakeOnce();
  auto r = eg.MakeSymbol(0, "r");
  auto named = eg.MakeNamedOutput("r", r, IntLit(eg, 1));
  auto out = eg.MakeOutput(once, {named});

  test::CostHarness h{eg, out};
  auto const alts = h.alts(out);
  auto const r_bit = h.bit_of(r);

  ASSERT_FALSE(alts.empty());
  EXPECT_TRUE(std::ranges::all_of(alts, [r_bit](Alternative const &a) { return a.introduces.test(r_bit); }))
      << "Output introduces every sym its NamedOutput children bind";
}

// ===========================================================================
// Subquery: scope barrier. Only the exposed syms cross into the outer scope;
// the inner pipeline's introductions are dropped.
// ===========================================================================

TEST(SubqueryDemand, ExposesOnlyExposedSymsAcrossBoundary) {
  egraph eg;
  // inner: UNWIND-free pipeline binding x, returning y = x.
  auto inner_once = eg.MakeOnce();
  auto x = eg.MakeSymbol(0, "x");
  auto inner_bind = eg.MakeBind(inner_once, x, IntLit(eg, 1));
  auto y = eg.MakeSymbol(1, "y");
  auto inner_named = eg.MakeNamedOutput("y", y, eg.MakeIdentifier(x));
  auto inner_root = eg.MakeOutput(inner_bind, {inner_named});

  auto outer_once = eg.MakeOnce();
  auto subq = eg.MakeSubquery(outer_once, inner_root, {y});

  test::CostHarness h{eg, subq};
  auto const alts = h.alts(subq);
  auto const x_bit = h.bit_of(x);
  auto const y_bit = h.bit_of(y);

  ASSERT_FALSE(alts.empty());
  EXPECT_TRUE(std::ranges::all_of(alts, [y_bit](Alternative const &a) { return a.introduces.test(y_bit); }))
      << "the exposed sym crosses the boundary";
  EXPECT_FALSE(HasIntroducedBit(alts, x_bit)) << "inner-only bindings are dropped at the boundary";
}

}  // namespace
}  // namespace memgraph::query::plan::v2
