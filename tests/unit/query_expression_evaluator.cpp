//
// Copyright 2017 Memgraph
// Created by Mislav Bradac on 27.03.17.
//

#include <iterator>
#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "query/frontend/ast/ast.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/interpret/eval.hpp"

using namespace query;

struct NoContextExpressionEvaluator {
  NoContextExpressionEvaluator() {}
  Frame frame{0};
  SymbolTable symbol_table;
  ExpressionEvaluator eval{frame, symbol_table};
};

TEST(ExpressionEvaluator, OrOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<OrOperator>(storage.Create<Literal>(true),
                                        storage.Create<Literal>(false));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<OrOperator>(storage.Create<Literal>(true),
                                  storage.Create<Literal>(true));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
}

TEST(ExpressionEvaluator, XorOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<XorOperator>(storage.Create<Literal>(true),
                                         storage.Create<Literal>(false));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<XorOperator>(storage.Create<Literal>(true),
                                   storage.Create<Literal>(true));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
}

TEST(ExpressionEvaluator, AndOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<AndOperator>(storage.Create<Literal>(true),
                                         storage.Create<Literal>(true));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<AndOperator>(storage.Create<Literal>(false),
                                   storage.Create<Literal>(true));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
}

TEST(ExpressionEvaluator, AdditionOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<AdditionOperator>(storage.Create<Literal>(2),
                                              storage.Create<Literal>(3));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, SubtractionOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<SubtractionOperator>(storage.Create<Literal>(2),
                                                 storage.Create<Literal>(3));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), -1);
}

TEST(ExpressionEvaluator, MultiplicationOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<MultiplicationOperator>(storage.Create<Literal>(2),
                                                    storage.Create<Literal>(3));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), 6);
}

TEST(ExpressionEvaluator, DivisionOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<DivisionOperator>(storage.Create<Literal>(50),
                                              storage.Create<Literal>(10));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, ModOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<ModOperator>(storage.Create<Literal>(65),
                                         storage.Create<Literal>(10));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, EqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<EqualOperator>(storage.Create<Literal>(10),
                                           storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<EqualOperator>(storage.Create<Literal>(15),
                                     storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<EqualOperator>(storage.Create<Literal>(20),
                                     storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
}

TEST(ExpressionEvaluator, NotEqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<NotEqualOperator>(storage.Create<Literal>(10),
                                              storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<NotEqualOperator>(storage.Create<Literal>(15),
                                        storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<NotEqualOperator>(storage.Create<Literal>(20),
                                        storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
}

TEST(ExpressionEvaluator, LessOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<LessOperator>(storage.Create<Literal>(10),
                                          storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<LessOperator>(storage.Create<Literal>(15),
                                    storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<LessOperator>(storage.Create<Literal>(20),
                                    storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
}

TEST(ExpressionEvaluator, GreaterOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<GreaterOperator>(storage.Create<Literal>(10),
                                             storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<GreaterOperator>(storage.Create<Literal>(15),
                                       storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<GreaterOperator>(storage.Create<Literal>(20),
                                       storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
}

TEST(ExpressionEvaluator, LessEqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<LessEqualOperator>(storage.Create<Literal>(10),
                                               storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<LessEqualOperator>(storage.Create<Literal>(15),
                                         storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<LessEqualOperator>(storage.Create<Literal>(20),
                                         storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
}

TEST(ExpressionEvaluator, GreaterEqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<GreaterEqualOperator>(storage.Create<Literal>(10),
                                                  storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<GreaterEqualOperator>(storage.Create<Literal>(15),
                                            storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<GreaterEqualOperator>(storage.Create<Literal>(20),
                                            storage.Create<Literal>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
}

TEST(ExpressionEvaluator, NotOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<NotOperator>(storage.Create<Literal>(false));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
}

TEST(ExpressionEvaluator, UnaryPlusOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<UnaryPlusOperator>(storage.Create<Literal>(5));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, UnaryMinusOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<UnaryMinusOperator>(storage.Create<Literal>(5));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), -5);
}

TEST(ExpressionEvaluator, IsNullOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<IsNullOperator>(storage.Create<Literal>(1));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op =
      storage.Create<IsNullOperator>(storage.Create<Literal>(TypedValue::Null));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
}

TEST(ExpressionEvaluator, AbsFunction) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  {
    std::vector<Expression *> arguments = {
        storage.Create<Literal>(TypedValue::Null)};
    auto *op = storage.Create<Function>(NameToFunction("ABS"), arguments);
    op->Accept(eval.eval);
    ASSERT_EQ(eval.eval.PopBack().type(), TypedValue::Type::Null);
  }
  {
    std::vector<Expression *> arguments = {storage.Create<Literal>(-2)};
    auto *op = storage.Create<Function>(NameToFunction("ABS"), arguments);
    op->Accept(eval.eval);
    ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), 2);
  }
  {
    std::vector<Expression *> arguments = {storage.Create<Literal>(-2.5)};
    auto *op = storage.Create<Function>(NameToFunction("ABS"), arguments);
    op->Accept(eval.eval);
    ASSERT_EQ(eval.eval.PopBack().Value<double>(), 2.5);
  }
  {
    std::vector<Expression *> arguments = {storage.Create<Literal>(true)};
    auto *op = storage.Create<Function>(NameToFunction("ABS"), arguments);
    ASSERT_THROW(op->Accept(eval.eval), QueryRuntimeException);
  }
  {
    std::vector<Expression *> arguments = {
        storage.Create<Literal>(std::vector<TypedValue>(5))};
    auto *op = storage.Create<Function>(NameToFunction("ABS"), arguments);
    ASSERT_THROW(op->Accept(eval.eval), QueryRuntimeException);
  }
}

TEST(ExpressionEvaluator, Aggregation) {
  AstTreeStorage storage;
  auto aggr = storage.Create<Aggregation>(storage.Create<Literal>(42),
                                          Aggregation::Op::COUNT);
  SymbolTable symbol_table;
  auto aggr_sym = symbol_table.CreateSymbol("aggr");
  symbol_table[*aggr] = aggr_sym;
  Frame frame{symbol_table.max_position()};
  frame[aggr_sym] = TypedValue(1);
  ExpressionEvaluator eval{frame, symbol_table};
  aggr->Accept(eval);
  EXPECT_EQ(eval.PopBack().Value<int64_t>(), 1);
}
