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
#include "query/parameters.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/temporal.hpp"

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

// The planner folds the label a pattern names into the filter expression, so a
// filter over a labelled node is a conjunction with a label test in it. Without
// this the label test refuses the whole conjunction, which is almost every
// filter that runs per row.
TEST_F(TypedProgramTest, ALabelledNodeFilterCompilesAndAnswers) {
  std::unique_ptr<memgraph::storage::Storage> db =
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{});
  auto accessor = db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba{accessor.get()};

  auto vertex = dba.InsertVertex();
  ASSERT_TRUE(vertex.AddLabel(dba.NameToLabel("L1")).has_value());
  auto const age = dba.NameToProperty("age");
  ASSERT_TRUE(vertex.SetProperty(age, memgraph::storage::PropertyValue(int64_t{30})).has_value());
  dba.AdvanceCommand();
  Set(0, TypedValue(vertex));

  auto *labelled = storage_.Create<memgraph::query::LabelsTest>(
      Ident(0), std::vector<memgraph::query::LabelIx>{storage_.GetLabelIx("L1")});
  auto *expr = storage_.Create<memgraph::query::AndOperator>(
      labelled,
      storage_.Create<memgraph::query::GreaterOperator>(
          storage_.Create<memgraph::query::PropertyLookup>(Ident(0), storage_.GetPropertyIx("age")),
          storage_.Create<memgraph::query::PrimitiveLiteral>(int64_t{20})));

  auto program = TypedProgram::Compile(expr);
  ASSERT_TRUE(program.has_value()) << "a label test and a property comparison should compile";

  memgraph::query::ExecutionContext context;
  context.db_accessor = &dba;
  context.evaluation_context.properties = memgraph::query::NamesToProperties(storage_.properties_, &dba);
  context.evaluation_context.labels = memgraph::query::NamesToLabels(storage_.labels_, &dba);
  memgraph::query::ExpressionEvaluator evaluator{&frame_, context, memgraph::storage::View::OLD};

  EXPECT_EQ(program->Run(frame_, &evaluator), TypedProgram::Answer::True);
}

// What a Produce writes per row is a value rather than an answer, so a program
// has to be able to carry one out. Only the value that leaves is boxed; the
// arithmetic on the way to it is not.
TEST_F(TypedProgramTest, AValueExpressionCompilesAndYieldsOne) {
  Set(0, TypedValue(int64_t{3}));

  auto *expr = storage_.Create<memgraph::query::AdditionOperator>(
      Ident(0), storage_.Create<memgraph::query::PrimitiveLiteral>(int64_t{4}));

  auto program = TypedProgram::CompileValue(expr);
  ASSERT_TRUE(program.has_value()) << "arithmetic over integers should compile to a value";

  TypedValue out;
  ASSERT_TRUE(program->RunInto(frame_, out));
  ASSERT_TRUE(out.IsInt());
  EXPECT_EQ(out.ValueInt(), 7);

  // A value that is not the type the program was built for refuses, and leaves
  // the caller to evaluate the expression the ordinary way.
  Set(0, TypedValue("three"));
  TypedValue untouched{int64_t{99}};
  EXPECT_FALSE(program->RunInto(frame_, untouched));
  EXPECT_EQ(untouched.ValueInt(), 99);
}

// A missing operand makes the value null rather than refusing, since null is a
// value a Produce can perfectly well write.
TEST_F(TypedProgramTest, AValueExpressionYieldsNullForAMissingOperand) {
  Set(0, TypedValue());

  auto *expr = storage_.Create<memgraph::query::AdditionOperator>(
      Ident(0), storage_.Create<memgraph::query::PrimitiveLiteral>(int64_t{4}));

  auto program = TypedProgram::CompileValue(expr);
  ASSERT_TRUE(program.has_value());

  TypedValue out{int64_t{99}};
  ASSERT_TRUE(program->RunInto(frame_, out));
  EXPECT_TRUE(out.IsNull());
}

// One conjunct this does not cover used to refuse the whole conjunction, which
// in a real query mix is what most refusals were. An expression that answers
// true, false or null is handed to the evaluator instead, and everything around
// it still compiles.
TEST_F(TypedProgramTest, AnUncoveredConjunctIsHandedToTheEvaluator) {
  Set(0, TypedValue(int64_t{5}));
  Set(1, TypedValue(int64_t{1}));
  Set(2, TypedValue(int64_t{3}));

  auto const in_list = [&](int position) {
    std::vector<Expression *> elements;
    for (int64_t value : {int64_t{1}, int64_t{2}, int64_t{3}}) {
      elements.push_back(storage_.Create<memgraph::query::PrimitiveLiteral>(value));
    }
    return storage_.Create<memgraph::query::InListOperator>(
        Ident(position), storage_.Create<memgraph::query::ListLiteral>(std::move(elements)));
  };

  auto *expr = storage_.Create<memgraph::query::AndOperator>(
      storage_.Create<memgraph::query::GreaterOperator>(Ident(0), Ident(1)), in_list(2));

  auto program = TypedProgram::Compile(expr);
  ASSERT_TRUE(program.has_value()) << "a conjunction holding an IN should still compile";

  memgraph::query::ExecutionContext context;
  context.db_accessor = nullptr;
  memgraph::query::ExpressionEvaluator evaluator{&frame_, context, memgraph::storage::View::OLD};

  EXPECT_EQ(program->Run(frame_, &evaluator), TypedProgram::Answer::True);

  Set(2, TypedValue(int64_t{9}));
  EXPECT_EQ(program->Run(frame_, &evaluator), TypedProgram::Answer::False);
}

// The shape LDBC filters on: a time a record carries, against a time the query
// names. Both are compared as microseconds, which is what the ordering on a
// local date time is, so the comparison needs no value built around either.
TEST_F(TypedProgramTest, ATemporalComparisonCompilesAndAnswers) {
  std::unique_ptr<memgraph::storage::Storage> db =
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{});
  auto accessor = db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba{accessor.get()};

  // Parsed the same way the query's own constructor parses its argument, so
  // the two sides cannot disagree for a reason that is not the comparison.
  auto const [day, time] = memgraph::utils::ParseLocalDateTimeParameters("2020-06-01T12:00:00");
  auto const when = memgraph::utils::LocalDateTime(day, time);
  auto vertex = dba.InsertVertex();
  ASSERT_TRUE(vertex
                  .SetProperty(dba.NameToProperty("created"),
                               memgraph::storage::PropertyValue(memgraph::storage::TemporalData(
                                   memgraph::storage::TemporalType::LocalDateTime, when.SysMicrosecondsSinceEpoch())))
                  .has_value());
  dba.AdvanceCommand();
  Set(0, TypedValue(vertex));

  auto const compare_against = [&](int year) {
    auto *literal = storage_.Create<memgraph::query::PrimitiveLiteral>(
        memgraph::storage::ExternalPropertyValue(std::to_string(year) + "-06-01T12:00:00"));
    auto *call = storage_.Create<memgraph::query::Function>("LOCALDATETIME", std::vector<Expression *>{literal});
    return storage_.Create<memgraph::query::GreaterOperator>(
        storage_.Create<memgraph::query::PropertyLookup>(Ident(0), storage_.GetPropertyIx("created")), call);
  };

  auto *earlier = compare_against(2019);
  auto *later = compare_against(2021);

  memgraph::query::ExecutionContext context;
  context.db_accessor = &dba;
  context.evaluation_context.properties = memgraph::query::NamesToProperties(storage_.properties_, &dba);
  memgraph::query::ExpressionEvaluator evaluator{&frame_, context, memgraph::storage::View::OLD};

  auto after = TypedProgram::Compile(earlier);
  ASSERT_TRUE(after.has_value()) << "a time compared with one the query names should compile";
  EXPECT_EQ(after->Run(frame_, &evaluator), TypedProgram::Answer::True);

  auto before = TypedProgram::Compile(later);
  ASSERT_TRUE(before.has_value());
  EXPECT_EQ(before->Run(frame_, &evaluator), TypedProgram::Answer::False);
}

// A chained comparison is a conjunction that evaluates both sides whatever the
// first says, which is what separates it from an AND and why it compiles to
// one without the jump over the second.
TEST_F(TypedProgramTest, AChainedComparisonCompilesAndAnswers) {
  Set(0, TypedValue(int64_t{5}));

  auto *range = storage_.Create<memgraph::query::RangeOperator>();
  range->expression1_ = storage_.Create<memgraph::query::GreaterOperator>(
      Ident(0), storage_.Create<memgraph::query::PrimitiveLiteral>(int64_t{1}));
  range->expression2_ = storage_.Create<memgraph::query::LessOperator>(
      Ident(0), storage_.Create<memgraph::query::PrimitiveLiteral>(int64_t{10}));

  auto program = TypedProgram::Compile(range);
  ASSERT_TRUE(program.has_value()) << "a chained comparison should compile";
  EXPECT_EQ(program->Run(frame_), TypedProgram::Answer::True);

  Set(0, TypedValue(int64_t{20}));
  EXPECT_EQ(program->Run(frame_), TypedProgram::Answer::False);
}

// Asking whether something is null always answers, where a comparison against
// it would not: a missing property makes a comparison null and this false.
TEST_F(TypedProgramTest, AnIsNullTestCompilesAndAnswers) {
  std::unique_ptr<memgraph::storage::Storage> db =
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{});
  auto accessor = db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba{accessor.get()};

  auto vertex = dba.InsertVertex();
  ASSERT_TRUE(vertex.SetProperty(dba.NameToProperty("age"), memgraph::storage::PropertyValue(int64_t{30})).has_value());
  dba.AdvanceCommand();
  Set(0, TypedValue(vertex));

  auto const is_null = [&](char const *property) {
    auto *test = storage_.Create<memgraph::query::IsNullOperator>(
        storage_.Create<memgraph::query::PropertyLookup>(Ident(0), storage_.GetPropertyIx(property)));
    return TypedProgram::Compile(test);
  };

  // Both names are registered before the mapping is built, since it is indexed
  // by the order they were registered in and a later one sits past its end.
  auto present = is_null("age");
  auto absent = is_null("absent");

  memgraph::query::ExecutionContext context;
  context.db_accessor = &dba;
  context.evaluation_context.properties = memgraph::query::NamesToProperties(storage_.properties_, &dba);
  memgraph::query::ExpressionEvaluator evaluator{&frame_, context, memgraph::storage::View::OLD};

  ASSERT_TRUE(present.has_value()) << "a null test over a property should compile";
  EXPECT_EQ(present->Run(frame_, &evaluator), TypedProgram::Answer::False);

  ASSERT_TRUE(absent.has_value());
  EXPECT_EQ(absent->Run(frame_, &evaluator), TypedProgram::Answer::True);
}

// A conjunction whose left side is false never evaluates its right side, and
// the difference shows when the right side would throw. Reading a property off
// a deleted record is the case that arises: the evaluator never reaches it, so
// neither may a compiled program.
TEST_F(TypedProgramTest, AFalseConjunctionDoesNotReachItsRightSide) {
  std::unique_ptr<memgraph::storage::Storage> db =
      std::make_unique<memgraph::storage::InMemoryStorage>(memgraph::storage::Config{});
  auto accessor = db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba{accessor.get()};

  auto vertex = dba.InsertVertex();
  auto const age = dba.NameToProperty("age");
  ASSERT_TRUE(vertex.SetProperty(age, memgraph::storage::PropertyValue(int64_t{30})).has_value());
  dba.AdvanceCommand();
  ASSERT_TRUE(dba.RemoveVertex(&vertex).has_value());
  dba.AdvanceCommand();

  Set(0, TypedValue(int64_t{1}));
  Set(1, TypedValue(int64_t{2}));
  Set(2, TypedValue(vertex));

  auto *record = storage_.Create<memgraph::query::Identifier>("v");
  record->symbol_pos_ = 2;
  auto *reads_a_deleted_record = storage_.Create<memgraph::query::GreaterOperator>(
      storage_.Create<memgraph::query::PropertyLookup>(record, storage_.GetPropertyIx("age")),
      storage_.Create<memgraph::query::PrimitiveLiteral>(int64_t{1}));
  // 1 > 2 is false, so the right side is never asked for.
  auto *expr = storage_.Create<memgraph::query::AndOperator>(
      storage_.Create<memgraph::query::GreaterOperator>(Ident(0), Ident(1)), reads_a_deleted_record);

  auto program = TypedProgram::Compile(expr);
  ASSERT_TRUE(program.has_value());

  memgraph::query::ExecutionContext context;
  context.db_accessor = &dba;
  context.evaluation_context.properties = memgraph::query::NamesToProperties(storage_.properties_, &dba);
  memgraph::query::ExpressionEvaluator evaluator{&frame_, context, memgraph::storage::View::OLD};

  EXPECT_EQ(program->Run(frame_, &evaluator), TypedProgram::Answer::False);
}

// A cached query has had its literals stripped into parameters, so this is the
// shape a filter actually has by the time it runs per row. A parameter holds
// the same value for every row, but its type is only known once the query is
// run, so it is guessed and checked like anything read from the frame.
TEST_F(TypedProgramTest, AParameterComparisonCompilesAndAnswers) {
  Set(0, TypedValue(int64_t{30}));

  auto *expr =
      storage_.Create<memgraph::query::GreaterOperator>(Ident(0), storage_.Create<memgraph::query::ParameterLookup>(7));

  auto program = TypedProgram::Compile(expr);
  ASSERT_TRUE(program.has_value()) << "a value compared with a parameter should compile";

  memgraph::query::Parameters parameters;
  parameters.Add(7, memgraph::storage::ExternalPropertyValue(int64_t{20}));
  EXPECT_EQ(program->Run(frame_, nullptr, &parameters), TypedProgram::Answer::True);

  memgraph::query::Parameters bigger;
  bigger.Add(7, memgraph::storage::ExternalPropertyValue(int64_t{40}));
  EXPECT_EQ(program->Run(frame_, nullptr, &bigger), TypedProgram::Answer::False);
}

// The same program, run with a parameter the guess did not expect, has to
// refuse rather than answer. A parameter is bound per execution, so this is
// the one guard that fires for a whole run rather than a row.
TEST_F(TypedProgramTest, AParameterOfAnotherTypeIsRefused) {
  Set(0, TypedValue(int64_t{30}));

  auto *expr =
      storage_.Create<memgraph::query::GreaterOperator>(Ident(0), storage_.Create<memgraph::query::ParameterLookup>(7));

  auto program = TypedProgram::Compile(expr);
  ASSERT_TRUE(program.has_value());

  memgraph::query::Parameters parameters;
  parameters.Add(7, memgraph::storage::ExternalPropertyValue("twenty"));
  EXPECT_EQ(program->Run(frame_, nullptr, &parameters), TypedProgram::Answer::Refused);
}
