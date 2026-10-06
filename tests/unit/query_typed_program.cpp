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

#include "query/frontend/ast/ast.hpp"
#include "query/interpret/frame.hpp"
#include "query/interpret/typed_program.hpp"

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
