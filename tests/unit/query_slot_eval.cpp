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

#include <memory>

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "storage/v2/inmemory/storage.hpp"

using memgraph::query::AstStorage;
using memgraph::query::Expression;
using memgraph::query::TypedValue;

namespace {

class SlotEval : public ::testing::Test {
 protected:
  std::unique_ptr<memgraph::storage::Storage> db_ =
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{});
  std::unique_ptr<memgraph::storage::Storage::Accessor> accessor_ = db_->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba_{accessor_.get()};

  AstStorage storage_;
  memgraph::query::SymbolTable symbol_table_;
  memgraph::query::Frame frame_{128};
  memgraph::query::ExecutionContext context_;

  memgraph::query::ExpressionEvaluator MakeEvaluator() {
    context_.db_accessor = &dba_;
    context_.symbol_table = symbol_table_;
    return memgraph::query::ExpressionEvaluator{&frame_, context_, memgraph::storage::View::OLD};
  }

  // Literal int, so a test can write an expression without a graph behind it.
  Expression *Int(int64_t value) { return storage_.Create<memgraph::query::PrimitiveLiteral>(value); }
};

}  // namespace

// D1 in the plan. The slot path has to leave the same value as the path that
// returns it, for the same expression, or every query that takes it is wrong.
TEST_F(SlotEval, TheSlotPathLeavesWhatAcceptReturns) {
  auto *equal = storage_.Create<memgraph::query::EqualOperator>(
      storage_.Create<memgraph::query::AdditionOperator>(Int(1), Int(2)), Int(3));
  auto *conjunction = storage_.Create<memgraph::query::AndOperator>(
      equal, storage_.Create<memgraph::query::EqualOperator>(Int(4), Int(4)));

  auto evaluator = MakeEvaluator();

  TypedValue const by_accept = conjunction->Accept(evaluator);
  TypedValue const &by_slot = evaluator.EvalIntoSlot(conjunction);

  EXPECT_TRUE(TypedValue::BoolEqual{}(by_accept, by_slot));
  EXPECT_TRUE(by_slot.IsBool());
  EXPECT_TRUE(by_slot.ValueBool());
}

// The slot a node owns is written afresh each time, so running the same
// expression twice over a changed frame cannot leave the first answer behind.
TEST_F(SlotEval, TheSlotPathDoesNotCarryTheLastAnswerOver) {
  auto *equal = storage_.Create<memgraph::query::EqualOperator>(Int(1), Int(2));
  auto evaluator = MakeEvaluator();

  EXPECT_FALSE(evaluator.EvalIntoSlot(equal).ValueBool());

  auto *same = storage_.Create<memgraph::query::EqualOperator>(Int(7), Int(7));
  EXPECT_TRUE(evaluator.EvalIntoSlot(same).ValueBool());
  EXPECT_FALSE(evaluator.EvalIntoSlot(equal).ValueBool());
}
