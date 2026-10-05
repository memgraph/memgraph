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

#include <gtest/gtest.h>

#include <set>

#include "query/frontend/ast/ast.hpp"

using memgraph::query::AdditionOperator;
using memgraph::query::AstStorage;
using memgraph::query::NamedExpression;
using memgraph::query::PrimitiveLiteral;

// An evaluation slot is where an expression's value lives while the expression
// above it is still being worked out. Two expressions sharing one slot would
// have the inner value overwritten before the outer one read it, so the whole
// scheme rests on each expression owning a slot no other expression owns.
TEST(EvalSlots, EveryExpressionGetsItsOwnSlot) {
  AstStorage storage;
  auto *lhs = storage.Create<PrimitiveLiteral>(1);
  auto *rhs = storage.Create<PrimitiveLiteral>(2);
  auto *sum = storage.Create<AdditionOperator>(lhs, rhs);

  EXPECT_NE(lhs->eval_slot_, rhs->eval_slot_);
  EXPECT_NE(lhs->eval_slot_, sum->eval_slot_);
  EXPECT_NE(rhs->eval_slot_, sum->eval_slot_);
  EXPECT_EQ(storage.EvalSlotCount(), 3U);
}

// A named expression is not an Expression, but the same visitor evaluates it
// and it yields a value, so it needs a slot for that value on the same terms.
TEST(EvalSlots, ANamedExpressionGetsASlotOfItsOwn) {
  AstStorage storage;
  auto *inner = storage.Create<PrimitiveLiteral>(1);
  auto *named = storage.Create<NamedExpression>();
  named->expression_ = inner;

  EXPECT_NE(inner->eval_slot_, named->eval_slot_);
  EXPECT_EQ(storage.EvalSlotCount(), 2U);
}

// A cached plan is cloned before it is run, so the copy has to be as sound as
// the original: its slots distinct, and every one of them inside the count the
// evaluator sizes its scratch to. A clone that carried the source's slots
// across would hand two nodes the same one.
TEST(EvalSlots, CloningGivesTheCopyItsOwnSlotsWithinItsOwnCount) {
  AstStorage source;
  auto *lhs = source.Create<PrimitiveLiteral>(1);
  auto *rhs = source.Create<PrimitiveLiteral>(2);
  auto *sum = source.Create<AdditionOperator>(lhs, rhs);

  AstStorage target;
  auto *copy = static_cast<AdditionOperator *>(sum->Clone(&target));

  std::set<uint32_t> const slots{copy->eval_slot_, copy->expression1_->eval_slot_, copy->expression2_->eval_slot_};
  EXPECT_EQ(slots.size(), 3U) << "two nodes in the clone share a slot";
  for (auto const slot : slots) {
    EXPECT_LT(slot, target.EvalSlotCount()) << "a slot sits outside the scratch the evaluator would allocate";
  }
}
