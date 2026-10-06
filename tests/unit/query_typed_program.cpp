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

#include <string>

#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "query/interpret/typed_program.hpp"
#include "storage/v2/inmemory/storage.hpp"

using memgraph::query::AstStorage;
using memgraph::query::Expression;
using memgraph::query::Frame;
using memgraph::query::TypedProgram;
using memgraph::query::TypedValue;

namespace {

class TypedProgramTest : public ::testing::Test {
 protected:
  AstStorage storage_;
  Frame frame_{8};

  void Set(int position, TypedValue value) {
    auto writer = frame_.GetFrameWriter(nullptr, memgraph::utils::NewDeleteResource());
    memgraph::query::Symbol const symbol{"v" + std::to_string(position), position, false};
    writer.Modify(symbol, [&](TypedValue &slot) { slot = std::move(value); });
  }

  Expression *Ident(int position) {
    auto *identifier = storage_.Create<memgraph::query::Identifier>("v");
    identifier->symbol_pos_ = position;
    return identifier;
  }
};

}  // namespace

// The tracer bullet: a comparison of two frame values the compiler guesses are
// integers, run without building a TypedValue for either operand.
TEST_F(TypedProgramTest, AnIntegerComparisonCompilesAndAnswers) {
  Set(0, TypedValue(int64_t{3}));
  Set(1, TypedValue(int64_t{3}));

  auto program = TypedProgram::Compile(storage_.Create<memgraph::query::EqualOperator>(Ident(0), Ident(1)));
  ASSERT_TRUE(program.has_value());
  EXPECT_EQ(program->Run(frame_), TypedProgram::Answer::True);

  Set(1, TypedValue(int64_t{4}));
  EXPECT_EQ(program->Run(frame_), TypedProgram::Answer::False);
}

// A filter over a property is the shape that actually runs per row, so the
// program has to take it. Reading the property is left to the evaluator, which
// already knows about views, permissions and deleted objects; what is new here
// is that the answer never becomes a TypedValue.
TEST_F(TypedProgramTest, APropertyComparisonCompilesAndAnswers) {
  std::unique_ptr<memgraph::storage::Storage> db =
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{});
  auto accessor = db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba{accessor.get()};

  auto vertex = dba.InsertVertex();
  auto const age = dba.NameToProperty("age");
  ASSERT_TRUE(vertex.SetProperty(age, memgraph::storage::PropertyValue(int64_t{30})).has_value());
  dba.AdvanceCommand();
  Set(0, TypedValue(vertex));

  auto *lookup = storage_.Create<memgraph::query::PropertyLookup>(Ident(0), storage_.GetPropertyIx("age"));
  auto *expr = storage_.Create<memgraph::query::GreaterOperator>(
      lookup, storage_.Create<memgraph::query::PrimitiveLiteral>(int64_t{20}));

  auto program = TypedProgram::Compile(expr);
  ASSERT_TRUE(program.has_value()) << "a property compared with a literal should compile";

  memgraph::query::ExecutionContext context;
  context.db_accessor = &dba;
  context.evaluation_context.properties = memgraph::query::NamesToProperties(storage_.properties_, &dba);
  memgraph::query::ExpressionEvaluator evaluator{&frame_, context, memgraph::storage::View::OLD};

  EXPECT_EQ(program->Run(frame_, &evaluator), TypedProgram::Answer::True);
}
