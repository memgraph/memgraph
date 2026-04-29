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

// Pure-algebra tests for the Bind semantics module.  These do not construct
// an egraph or invoke the planner — they exercise the alive/dead predicate,
// cost formulas, and required-set algebra directly.  Behaviour changes to any
// of them would shift the cost-model and resolver simultaneously and benefit
// from being caught at the algebra level rather than via the full pipeline.

#include <gtest/gtest.h>

#include <boost/container/small_vector.hpp>

#include "query/plan_v2/bind_semantics.hpp"

import memgraph.planner.core.egraph;

namespace memgraph::query::plan::v2::bind {
namespace {

using EClassId = planner::core::EClassId;

// Helper: construct a SymbolSet from an initializer list.
auto MakeSet(std::initializer_list<uint32_t> ids) -> SymbolSet {
  SymbolSet s;
  for (auto id : ids) s.insert(EClassId{id});
  return s;
}

TEST(BindAlgebra_IsAlive, SymInRequiredYieldsAlive) {
  auto required = MakeSet({1, 2, 3});
  EXPECT_TRUE(IsAlive(required, EClassId{2}));
}

TEST(BindAlgebra_IsAlive, SymNotInRequiredYieldsDead) {
  auto required = MakeSet({1, 2, 3});
  EXPECT_FALSE(IsAlive(required, EClassId{4}));
}

TEST(BindAlgebra_IsAlive, EmptyRequiredIsAlwaysDead) {
  auto required = MakeSet({});
  EXPECT_FALSE(IsAlive(required, EClassId{1}));
}

TEST(BindAlgebra_Cost, AliveSumsAllThree) {
  EXPECT_DOUBLE_EQ(AliveCost(2.5, kSymbolCost, 3.5), 2.5 + kSymbolCost + 3.5);
}

TEST(BindAlgebra_Cost, DeadIgnoresSymAndExpr) { EXPECT_DOUBLE_EQ(DeadCost(7.5), 7.5); }

TEST(BindAlgebra_Cost, kSymbolCostIsOne) {
  // Documenting the chosen scalar; if this changes, all three Bind sites move
  // together via this constant rather than via three separate edits.
  EXPECT_DOUBLE_EQ(kSymbolCost, 1.0);
}

TEST(BindAlgebra_AliveRequired, RemovesSymFromInput) {
  auto input = MakeSet({1, 2, 3});
  auto expr = MakeSet({});
  boost::container::small_vector<EClassId, 16> scratch;
  auto result = AliveRequired(input, EClassId{2}, expr, scratch);
  EXPECT_EQ(result, MakeSet({1, 3}));
}

TEST(BindAlgebra_AliveRequired, AddsExprDemandsToOutput) {
  auto input = MakeSet({1, 2});
  auto expr = MakeSet({4, 5});
  boost::container::small_vector<EClassId, 16> scratch;
  auto result = AliveRequired(input, EClassId{2}, expr, scratch);
  EXPECT_EQ(result, MakeSet({1, 4, 5}));
}

TEST(BindAlgebra_AliveRequired, ExprDemandOverlapsInputProducesUnion) {
  auto input = MakeSet({1, 2, 3});
  auto expr = MakeSet({3, 4});
  boost::container::small_vector<EClassId, 16> scratch;
  auto result = AliveRequired(input, EClassId{2}, expr, scratch);
  // (input \ {2}) ∪ expr = {1, 3} ∪ {3, 4} = {1, 3, 4}
  EXPECT_EQ(result, MakeSet({1, 3, 4}));
}

TEST(BindAlgebra_AliveRequired, ExprDemandReintroducesSym) {
  // Edge case: expr demands the symbol that this Bind itself binds.  This
  // happens when expr is e.g. Identifier(sym) referencing the same sym.
  // Result: sym ends up in the required set after all (because expr demands
  // it, even though input no longer does post-removal).
  auto input = MakeSet({1, 2});
  auto expr = MakeSet({2});
  boost::container::small_vector<EClassId, 16> scratch;
  auto result = AliveRequired(input, EClassId{2}, expr, scratch);
  // (input \ {2}) ∪ expr = {1} ∪ {2} = {1, 2}
  EXPECT_EQ(result, MakeSet({1, 2}));
}

TEST(BindAlgebra_AliveRequired, EmptyInputAndExprYieldsEmpty) {
  auto input = MakeSet({});
  auto expr = MakeSet({});
  boost::container::small_vector<EClassId, 16> scratch;
  auto result = AliveRequired(input, EClassId{2}, expr, scratch);
  EXPECT_EQ(result, MakeSet({}));
}

TEST(BindAlgebra_AliveRequired, ScratchReusedAcrossCalls) {
  // The scratch buffer is reused across calls; each call clears it.
  // This test ensures repeated calls produce correct results without
  // contamination from prior iterations.
  boost::container::small_vector<EClassId, 16> scratch;

  auto r1 = AliveRequired(MakeSet({1, 2, 3}), EClassId{2}, MakeSet({4}), scratch);
  EXPECT_EQ(r1, MakeSet({1, 3, 4}));

  auto r2 = AliveRequired(MakeSet({5}), EClassId{5}, MakeSet({}), scratch);
  EXPECT_EQ(r2, MakeSet({}));

  auto r3 = AliveRequired(MakeSet({1, 2}), EClassId{1}, MakeSet({3, 4}), scratch);
  EXPECT_EQ(r3, MakeSet({2, 3, 4}));
}

}  // namespace
}  // namespace memgraph::query::plan::v2::bind
