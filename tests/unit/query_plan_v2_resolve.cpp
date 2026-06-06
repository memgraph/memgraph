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

// Resolver-contract tests for plan_v2.
//
// Inclusion criterion: a test here pins what the resolver *selects* and how it
// *threads scope* for a given operator - which alt (alive vs dead binder) wins,
// and the in_scope / must_introduce each child is resolved under - asserted
// against the production resolve pass via ResolveHarness. The cost-emit half
// (which alts exist) lives in the demand suite; the v1-build half lives in the
// pipeline suite. A wrong selection or threading fails here, at the resolver,
// rather than surfacing downstream as a wrong v1 plan.

#include <gtest/gtest.h>

#include <algorithm>

#include "query/plan_v2/egraph/egraph.hpp"
#include "query/plan_v2/egraph/symbol.hpp"
#include "query/plan_v2/test_support/literals.hpp"
#include "query/plan_v2/test_support/resolve.hpp"

namespace memgraph::query::plan::v2 {
namespace {

// DEFERRED COVERAGE: two resolver behaviours are not exercised here yet.
//   - Subquery inner-scope isolation (inner resolved with in_scope = {}). A
//     valid Subquery e-graph that resolves end-to-end is fiddly to build by
//     hand; until a builder helper exists, MinimalCallReturn in the pipeline
//     suite covers it end-to-end.
//   - The assert-unique guard firing when one e-class resolves under more than
//     one ResolverKey (and a key-keyed accessor to disambiguate it). No query
//     reaches a single e-class under divergent scope today - the same T2 gap
//     the AliveBindChains suite documents.

using test::ResolveHarness;

TEST(PlannerV2Resolve, RootResolvesToOutput) {
  // The simplest whole-query resolve: WITH 1 AS a RETURN a, built directly so
  // both Bind alts are present. The root is an Output, and resolving from it
  // must choose the Output e-node.
  egraph eg;
  auto sym_a = eg.MakeSymbol(0, "a");
  auto sym_r = eg.MakeSymbol(1, "r");
  auto bind = eg.MakeBind(eg.MakeOnce(), sym_a, IntLit(eg, 1));
  auto named_out = eg.MakeNamedOutput("r", sym_r, eg.MakeIdentifier(sym_a));
  auto root = eg.MakeOutput(bind, {named_out});

  ResolveHarness h{eg, root};

  EXPECT_EQ(h.chosen_symbol(root), symbol::Output);
}

TEST(PlannerV2Resolve, UnusedBindResolvesDead) {
  // WITH 1 AS unused RETURN 2: the bound sym is referenced nowhere, so the
  // resolver picks the dead Bind alt - the binding is elided and the input
  // pipe forwarded.
  egraph eg;
  auto sym_unused = eg.MakeSymbol(0, "unused");
  auto sym_r = eg.MakeSymbol(1, "r");
  auto bind = eg.MakeBind(eg.MakeOnce(), sym_unused, IntLit(eg, 1));
  auto named_out = eg.MakeNamedOutput("r", sym_r, IntLit(eg, 2));
  auto root = eg.MakeOutput(bind, {named_out});

  ResolveHarness h{eg, root};

  EXPECT_EQ(h.binder_state(bind), ResolveHarness::BinderState::Dead);
}

TEST(PlannerV2Resolve, ReferencedBindResolvesAlive) {
  // WITH 1 AS a RETURN a: a is read by Identifier(a) in the output, so the
  // resolver keeps the binding - the alive Bind alt.
  egraph eg;
  auto sym_a = eg.MakeSymbol(0, "a");
  auto sym_r = eg.MakeSymbol(1, "r");
  auto bind = eg.MakeBind(eg.MakeOnce(), sym_a, IntLit(eg, 1));
  auto named_out = eg.MakeNamedOutput("r", sym_r, eg.MakeIdentifier(sym_a));
  auto root = eg.MakeOutput(bind, {named_out});

  ResolveHarness h{eg, root};

  EXPECT_EQ(h.binder_state(bind), ResolveHarness::BinderState::Alive);
}

TEST(PlannerV2Resolve, AliveBindThreadsBoundSymIntoExprScope) {
  // WITH 1 AS a WITH a+1 AS b RETURN b, built directly so both binders stay
  // alive. For the alive Bind of b, the expr child (a+1) must be resolved with
  // a in scope, and the input pipe must be told to introduce a.
  egraph eg;
  auto sym_a = eg.MakeSymbol(0, "a");
  auto sym_b = eg.MakeSymbol(1, "b");
  auto sym_r = eg.MakeSymbol(2, "r");
  auto bind_a = eg.MakeBind(eg.MakeOnce(), sym_a, IntLit(eg, 1));
  auto expr_b = eg.MakeAdd(eg.MakeIdentifier(sym_a), IntLit(eg, 1));
  auto bind_b = eg.MakeBind(bind_a, sym_b, expr_b);
  auto named_out = eg.MakeNamedOutput("r", sym_r, eg.MakeIdentifier(sym_b));
  auto root = eg.MakeOutput(bind_b, {named_out});

  ResolveHarness h{eg, root};
  ASSERT_EQ(h.binder_state(bind_b), ResolveHarness::BinderState::Alive);

  auto const bit_a = h.bit_of(sym_a);
  auto const threads = h.threading(bind_b);

  auto const thread_to = [&](eclass child) -> ResolverKey {
    for (auto const &k : threads)
      if (k.eclass == to_core(child)) return k;
    ADD_FAILURE() << "no child thread for the requested e-class";
    return ResolverKey{};
  };

  // The expr child sees a; the input pipe is asked to introduce a.
  EXPECT_TRUE(thread_to(expr_b).in_scope.test(bit_a));
  EXPECT_TRUE(thread_to(bind_a).must_introduce.test(bit_a));
}

TEST(PlannerV2Resolve, UnusedUnwindOverKnownLengthResolvesDead) {
  // UNWIND range(0, 5) AS x RETURN 1: x is unused and range's length is
  // statically known, so the resolver elides the binding (dead Unwind) while
  // keeping the list to drive the row count.
  egraph eg;
  auto sym_x = eg.MakeSymbol(0, "x");
  auto sym_r = eg.MakeSymbol(1, "r");
  auto range = eg.MakeFunction("range", {IntLit(eg, 0), IntLit(eg, 5)}, /*is_pure=*/true);
  auto unwind = eg.MakeUnwind(eg.MakeOnce(), sym_x, range);
  auto named_out = eg.MakeNamedOutput("r", sym_r, IntLit(eg, 1));
  auto root = eg.MakeOutput(unwind, {named_out});

  ResolveHarness h{eg, root};
  EXPECT_EQ(h.binder_state(unwind), ResolveHarness::BinderState::Dead);

  // The list child is still resolved (its length drives the scale); the sym is not.
  auto const threads = h.threading(unwind);
  EXPECT_TRUE(std::ranges::any_of(threads, [&](auto const &k) { return k.eclass == to_core(range); }));
  EXPECT_TRUE(std::ranges::none_of(threads, [&](auto const &k) { return k.eclass == to_core(sym_x); }));
}

TEST(PlannerV2Resolve, ReferencedUnwindResolvesAlive) {
  // UNWIND range(0, 5) AS x RETURN x: x is read downstream, so the row-generating
  // binding is kept - the alive Unwind alt.
  egraph eg;
  auto sym_x = eg.MakeSymbol(0, "x");
  auto sym_r = eg.MakeSymbol(1, "r");
  auto range = eg.MakeFunction("range", {IntLit(eg, 0), IntLit(eg, 5)}, /*is_pure=*/true);
  auto unwind = eg.MakeUnwind(eg.MakeOnce(), sym_x, range);
  auto named_out = eg.MakeNamedOutput("r", sym_r, eg.MakeIdentifier(sym_x));
  auto root = eg.MakeOutput(unwind, {named_out});

  ResolveHarness h{eg, root};
  EXPECT_EQ(h.binder_state(unwind), ResolveHarness::BinderState::Alive);
}

TEST(PlannerV2Resolve, OutputDoesNotDemandItsOwnSymFromInputPipe) {
  // WITH 1 AS a RETURN a: the Output projects r and reads a. Its input pipe is
  // asked to introduce a (the downstream-needed sym) but not r, which the
  // Output itself binds - the own_syms exclusion.
  egraph eg;
  auto sym_a = eg.MakeSymbol(0, "a");
  auto sym_r = eg.MakeSymbol(1, "r");
  auto bind = eg.MakeBind(eg.MakeOnce(), sym_a, IntLit(eg, 1));
  auto named_out = eg.MakeNamedOutput("r", sym_r, eg.MakeIdentifier(sym_a));
  auto root = eg.MakeOutput(bind, {named_out});

  ResolveHarness h{eg, root};
  auto const threads = h.threading(root);

  ResolverKey pipe{};
  for (auto const &k : threads)
    if (k.eclass == to_core(bind)) pipe = k;

  EXPECT_TRUE(pipe.must_introduce.test(h.bit_of(sym_a)));
  EXPECT_FALSE(pipe.must_introduce.test(h.bit_of(sym_r)));
}

}  // namespace
}  // namespace memgraph::query::plan::v2
