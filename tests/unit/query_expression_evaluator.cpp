#include <cmath>
#include <iterator>
#include <memory>
#include <unordered_map>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "database/graph_db_accessor.hpp"
#include "database/graph_db_datatypes.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "query/path.hpp"
#include "utils/string.hpp"

#include "query_common.hpp"

using namespace query;
using query::test_common::ToList;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

namespace {

struct NoContextExpressionEvaluator {
  NoContextExpressionEvaluator() {}
  Frame frame{128};
  SymbolTable symbol_table;
  GraphDb db;
  GraphDbAccessor dba{db};
  Parameters parameters;
  ExpressionEvaluator eval{frame, parameters, symbol_table, dba};
};

TypedValue EvaluateFunction(const std::string &function_name,
                            const std::vector<TypedValue> &args, GraphDb &db) {
  AstTreeStorage storage;
  SymbolTable symbol_table;
  GraphDbAccessor dba(db);
  Frame frame{128};
  Parameters parameters;
  ExpressionEvaluator eval{frame, parameters, symbol_table, dba};

  std::vector<Expression *> expressions;
  for (const auto &arg : args) {
    expressions.push_back(storage.Create<PrimitiveLiteral>(arg));
  }
  auto *op =
      storage.Create<Function>(NameToFunction(function_name), expressions);
  return op->Accept(eval);
}

TypedValue EvaluateFunction(const std::string &function_name,
                            const std::vector<TypedValue> &args) {
  GraphDb db;
  return EvaluateFunction(function_name, args, db);
}

TEST(ExpressionEvaluator, OrOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<OrOperator>(storage.Create<PrimitiveLiteral>(true),
                                 storage.Create<PrimitiveLiteral>(false));
  auto val1 = op->Accept(eval.eval);
  ASSERT_EQ(val1.Value<bool>(), true);
  op = storage.Create<OrOperator>(storage.Create<PrimitiveLiteral>(true),
                                  storage.Create<PrimitiveLiteral>(true));
  auto val2 = op->Accept(eval.eval);
  ASSERT_EQ(val2.Value<bool>(), true);
}

TEST(ExpressionEvaluator, XorOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<XorOperator>(storage.Create<PrimitiveLiteral>(true),
                                  storage.Create<PrimitiveLiteral>(false));
  auto val1 = op->Accept(eval.eval);
  ASSERT_EQ(val1.Value<bool>(), true);
  op = storage.Create<XorOperator>(storage.Create<PrimitiveLiteral>(true),
                                   storage.Create<PrimitiveLiteral>(true));
  auto val2 = op->Accept(eval.eval);
  ASSERT_EQ(val2.Value<bool>(), false);
}

TEST(ExpressionEvaluator, AndOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(true),
                                  storage.Create<PrimitiveLiteral>(true));
  auto val1 = op->Accept(eval.eval);
  ASSERT_EQ(val1.Value<bool>(), true);
  op = storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(false),
                                   storage.Create<PrimitiveLiteral>(true));
  auto val2 = op->Accept(eval.eval);
  ASSERT_EQ(val2.Value<bool>(), false);
}

TEST(ExpressionEvaluator, AndOperatorShortCircuit) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  {
    auto *op =
        storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(false),
                                    storage.Create<PrimitiveLiteral>(5));
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.Value<bool>(), false);
  }
  {
    auto *op =
        storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(5),
                                    storage.Create<PrimitiveLiteral>(false));
    // We are evaluating left to right, so we don't short circuit here and raise
    // due to `5`. This differs from neo4j, where they evaluate both sides and
    // return `false` without checking for type of the first expression.
    EXPECT_THROW(op->Accept(eval.eval), QueryRuntimeException);
  }
}

TEST(ExpressionEvaluator, AndOperatorNull) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  {
    // Null doesn't short circuit
    auto *op = storage.Create<AndOperator>(
        storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>(5));
    EXPECT_THROW(op->Accept(eval.eval), QueryRuntimeException);
  }
  {
    auto *op = storage.Create<AndOperator>(
        storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>(true));
    auto value = op->Accept(eval.eval);
    EXPECT_TRUE(value.IsNull());
  }
  {
    auto *op = storage.Create<AndOperator>(
        storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>(false));
    auto value = op->Accept(eval.eval);
    ASSERT_TRUE(value.IsBool());
    EXPECT_EQ(value.Value<bool>(), false);
  }
}

TEST(ExpressionEvaluator, AdditionOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<AdditionOperator>(
      storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(3));
  auto value = op->Accept(eval.eval);
  ASSERT_EQ(value.Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, SubtractionOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<SubtractionOperator>(
      storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(3));
  auto value = op->Accept(eval.eval);
  ASSERT_EQ(value.Value<int64_t>(), -1);
}

TEST(ExpressionEvaluator, MultiplicationOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<MultiplicationOperator>(
      storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(3));
  auto value = op->Accept(eval.eval);
  ASSERT_EQ(value.Value<int64_t>(), 6);
}

TEST(ExpressionEvaluator, DivisionOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<DivisionOperator>(storage.Create<PrimitiveLiteral>(50),
                                       storage.Create<PrimitiveLiteral>(10));
  auto value = op->Accept(eval.eval);
  ASSERT_EQ(value.Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, ModOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<ModOperator>(storage.Create<PrimitiveLiteral>(65),
                                         storage.Create<PrimitiveLiteral>(10));
  auto value = op->Accept(eval.eval);
  ASSERT_EQ(value.Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, EqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(10),
                                    storage.Create<PrimitiveLiteral>(15));
  auto val1 = op->Accept(eval.eval);
  ASSERT_EQ(val1.Value<bool>(), false);
  op = storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(15),
                                     storage.Create<PrimitiveLiteral>(15));
  auto val2 = op->Accept(eval.eval);
  ASSERT_EQ(val2.Value<bool>(), true);
  op = storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(20),
                                     storage.Create<PrimitiveLiteral>(15));
  auto val3 = op->Accept(eval.eval);
  ASSERT_EQ(val3.Value<bool>(), false);
}

TEST(ExpressionEvaluator, NotEqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<NotEqualOperator>(storage.Create<PrimitiveLiteral>(10),
                                       storage.Create<PrimitiveLiteral>(15));
  auto val1 = op->Accept(eval.eval);
  ASSERT_EQ(val1.Value<bool>(), true);
  op = storage.Create<NotEqualOperator>(storage.Create<PrimitiveLiteral>(15),
                                        storage.Create<PrimitiveLiteral>(15));
  auto val2 = op->Accept(eval.eval);
  ASSERT_EQ(val2.Value<bool>(), false);
  op = storage.Create<NotEqualOperator>(storage.Create<PrimitiveLiteral>(20),
                                        storage.Create<PrimitiveLiteral>(15));
  auto val3 = op->Accept(eval.eval);
  ASSERT_EQ(val3.Value<bool>(), true);
}

TEST(ExpressionEvaluator, LessOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<LessOperator>(storage.Create<PrimitiveLiteral>(10),
                                          storage.Create<PrimitiveLiteral>(15));
  auto val1 = op->Accept(eval.eval);
  ASSERT_EQ(val1.Value<bool>(), true);
  op = storage.Create<LessOperator>(storage.Create<PrimitiveLiteral>(15),
                                    storage.Create<PrimitiveLiteral>(15));
  auto val2 = op->Accept(eval.eval);
  ASSERT_EQ(val2.Value<bool>(), false);
  op = storage.Create<LessOperator>(storage.Create<PrimitiveLiteral>(20),
                                    storage.Create<PrimitiveLiteral>(15));
  auto val3 = op->Accept(eval.eval);
  ASSERT_EQ(val3.Value<bool>(), false);
}

TEST(ExpressionEvaluator, GreaterOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<GreaterOperator>(storage.Create<PrimitiveLiteral>(10),
                                      storage.Create<PrimitiveLiteral>(15));
  auto val1 = op->Accept(eval.eval);
  ASSERT_EQ(val1.Value<bool>(), false);
  op = storage.Create<GreaterOperator>(storage.Create<PrimitiveLiteral>(15),
                                       storage.Create<PrimitiveLiteral>(15));
  auto val2 = op->Accept(eval.eval);
  ASSERT_EQ(val2.Value<bool>(), false);
  op = storage.Create<GreaterOperator>(storage.Create<PrimitiveLiteral>(20),
                                       storage.Create<PrimitiveLiteral>(15));
  auto val3 = op->Accept(eval.eval);
  ASSERT_EQ(val3.Value<bool>(), true);
}

TEST(ExpressionEvaluator, LessEqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<LessEqualOperator>(storage.Create<PrimitiveLiteral>(10),
                                        storage.Create<PrimitiveLiteral>(15));
  auto val1 = op->Accept(eval.eval);
  ASSERT_EQ(val1.Value<bool>(), true);
  op = storage.Create<LessEqualOperator>(storage.Create<PrimitiveLiteral>(15),
                                         storage.Create<PrimitiveLiteral>(15));
  auto val2 = op->Accept(eval.eval);
  ASSERT_EQ(val2.Value<bool>(), true);
  op = storage.Create<LessEqualOperator>(storage.Create<PrimitiveLiteral>(20),
                                         storage.Create<PrimitiveLiteral>(15));
  auto val3 = op->Accept(eval.eval);
  ASSERT_EQ(val3.Value<bool>(), false);
}

TEST(ExpressionEvaluator, GreaterEqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<GreaterEqualOperator>(
      storage.Create<PrimitiveLiteral>(10),
      storage.Create<PrimitiveLiteral>(15));
  auto val1 = op->Accept(eval.eval);
  ASSERT_EQ(val1.Value<bool>(), false);
  op = storage.Create<GreaterEqualOperator>(
      storage.Create<PrimitiveLiteral>(15),
      storage.Create<PrimitiveLiteral>(15));
  auto val2 = op->Accept(eval.eval);
  ASSERT_EQ(val2.Value<bool>(), true);
  op = storage.Create<GreaterEqualOperator>(
      storage.Create<PrimitiveLiteral>(20),
      storage.Create<PrimitiveLiteral>(15));
  auto val3 = op->Accept(eval.eval);
  ASSERT_EQ(val3.Value<bool>(), true);
}

TEST(ExpressionEvaluator, InListOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *list_literal = storage.Create<ListLiteral>(std::vector<Expression *>{
      storage.Create<PrimitiveLiteral>(1), storage.Create<PrimitiveLiteral>(2),
      storage.Create<PrimitiveLiteral>("a")});
  {
    // Element exists in list.
    auto *op = storage.Create<InListOperator>(
        storage.Create<PrimitiveLiteral>(2), list_literal);
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.Value<bool>(), true);
  }
  {
    // Element doesn't exist in list.
    auto *op = storage.Create<InListOperator>(
        storage.Create<PrimitiveLiteral>("x"), list_literal);
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.Value<bool>(), false);
  }
  {
    auto *list_literal = storage.Create<ListLiteral>(std::vector<Expression *>{
        storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>(2),
        storage.Create<PrimitiveLiteral>("a")});
    // Element doesn't exist in list with null element.
    auto *op = storage.Create<InListOperator>(
        storage.Create<PrimitiveLiteral>("x"), list_literal);
    auto value = op->Accept(eval.eval);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Null list.
    auto *op = storage.Create<InListOperator>(
        storage.Create<PrimitiveLiteral>("x"),
        storage.Create<PrimitiveLiteral>(TypedValue::Null));
    auto value = op->Accept(eval.eval);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Null literal.
    auto *op = storage.Create<InListOperator>(
        storage.Create<PrimitiveLiteral>(TypedValue::Null), list_literal);
    auto value = op->Accept(eval.eval);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Null literal, empty list.
    auto *op = storage.Create<InListOperator>(
        storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<ListLiteral>(std::vector<Expression *>()));
    auto value = op->Accept(eval.eval);
    EXPECT_FALSE(value.ValueBool());
  }
}

TEST(ExpressionEvaluator, ListMapIndexingOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *list_literal = storage.Create<ListLiteral>(std::vector<Expression *>{
      storage.Create<PrimitiveLiteral>(1), storage.Create<PrimitiveLiteral>(2),
      storage.Create<PrimitiveLiteral>(3),
      storage.Create<PrimitiveLiteral>(4)});
  {
    // Legal indexing.
    auto *op = storage.Create<ListMapIndexingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(2));
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.Value<int64_t>(), 3);
  }
  {
    // Out of bounds indexing.
    auto *op = storage.Create<ListMapIndexingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(4));
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.type(), TypedValue::Type::Null);
  }
  {
    // Out of bounds indexing with negative bound.
    auto *op = storage.Create<ListMapIndexingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(-100));
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.type(), TypedValue::Type::Null);
  }
  {
    // Legal indexing with negative index.
    auto *op = storage.Create<ListMapIndexingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(-2));
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.Value<int64_t>(), 3);
  }
  {
    // Indexing with one operator being null.
    auto *op = storage.Create<ListMapIndexingOperator>(
        storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>(-2));
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.type(), TypedValue::Type::Null);
  }
  {
    // Indexing with incompatible type.
    auto *op = storage.Create<ListMapIndexingOperator>(
        storage.Create<PrimitiveLiteral>(2),
        storage.Create<PrimitiveLiteral>(TypedValue::Null));
    EXPECT_THROW(op->Accept(eval.eval), QueryRuntimeException);
  }
}

TEST(ExpressionEvaluator, MapIndexing) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  GraphDb db;
  GraphDbAccessor dba(db);
  auto *map_literal = storage.Create<MapLiteral>(
      std::unordered_map<std::pair<std::string, GraphDbTypes::Property>,
                         Expression *>{
          {PROPERTY_PAIR("a"), storage.Create<PrimitiveLiteral>(1)},
          {PROPERTY_PAIR("b"), storage.Create<PrimitiveLiteral>(2)},
          {PROPERTY_PAIR("c"), storage.Create<PrimitiveLiteral>(3)}});
  {
    // Legal indexing.
    auto *op = storage.Create<ListMapIndexingOperator>(
        map_literal, storage.Create<PrimitiveLiteral>("b"));
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.Value<int64_t>(), 2);
  }
  {
    // Legal indexing, non-existing key.
    auto *op = storage.Create<ListMapIndexingOperator>(
        map_literal, storage.Create<PrimitiveLiteral>("z"));
    auto value = op->Accept(eval.eval);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Wrong key type.
    auto *op = storage.Create<ListMapIndexingOperator>(
        map_literal, storage.Create<PrimitiveLiteral>(42));
    EXPECT_THROW(op->Accept(eval.eval), QueryRuntimeException);
  }
  {
    // Indexing with Null.
    auto *op = storage.Create<ListMapIndexingOperator>(
        map_literal, storage.Create<PrimitiveLiteral>(TypedValue::Null));
    auto value = op->Accept(eval.eval);
    EXPECT_TRUE(value.IsNull());
  }
}

TEST(ExpressionEvaluator, ListSlicingOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *list_literal = storage.Create<ListLiteral>(std::vector<Expression *>{
      storage.Create<PrimitiveLiteral>(1), storage.Create<PrimitiveLiteral>(2),
      storage.Create<PrimitiveLiteral>(3),
      storage.Create<PrimitiveLiteral>(4)});

  auto extract_ints = [](TypedValue list) {
    std::vector<int64_t> int_list;
    for (auto x : list.Value<std::vector<TypedValue>>()) {
      int_list.push_back(x.Value<int64_t>());
    }
    return int_list;
  };
  {
    // Legal slicing with both bounds defined.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(2),
        storage.Create<PrimitiveLiteral>(4));
    auto value = op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(value), ElementsAre(3, 4));
  }
  {
    // Legal slicing with negative bound.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(2),
        storage.Create<PrimitiveLiteral>(-1));
    auto value = op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(value), ElementsAre(3));
  }
  {
    // Lower bound larger than upper bound.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(2),
        storage.Create<PrimitiveLiteral>(-4));
    auto value = op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(value), ElementsAre());
  }
  {
    // Bounds ouf or range.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(-100),
        storage.Create<PrimitiveLiteral>(10));
    auto value = op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(value), ElementsAre(1, 2, 3, 4));
  }
  {
    // Lower bound undefined.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, nullptr, storage.Create<PrimitiveLiteral>(3));
    auto value = op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(value), ElementsAre(1, 2, 3));
  }
  {
    // Upper bound undefined.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(-2), nullptr);
    auto value = op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(value), ElementsAre(3, 4));
  }
  {
    // Bound of illegal type and null value bound.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>("mirko"));
    EXPECT_THROW(op->Accept(eval.eval), QueryRuntimeException);
  }
  {
    // List of illegal type.
    auto *op = storage.Create<ListSlicingOperator>(
        storage.Create<PrimitiveLiteral>("a"),
        storage.Create<PrimitiveLiteral>(-2), nullptr);
    EXPECT_THROW(op->Accept(eval.eval), QueryRuntimeException);
  }
  {
    // Null value list with undefined upper bound.
    auto *op = storage.Create<ListSlicingOperator>(
        storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>(-2), nullptr);
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.type(), TypedValue::Type::Null);
  }
  {
    // Null value index.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(-2),
        storage.Create<PrimitiveLiteral>(TypedValue::Null));
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.type(), TypedValue::Type::Null);
  }
}

TEST(ExpressionEvaluator, IfOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *then_expression = storage.Create<PrimitiveLiteral>(10);
  auto *else_expression = storage.Create<PrimitiveLiteral>(20);
  {
    auto *condition_true =
        storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(2),
                                      storage.Create<PrimitiveLiteral>(2));
    auto *op = storage.Create<IfOperator>(condition_true, then_expression,
                                          else_expression);
    auto value = op->Accept(eval.eval);
    ASSERT_EQ(value.Value<int64_t>(), 10);
  }
  {
    auto *condition_false =
        storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(2),
                                      storage.Create<PrimitiveLiteral>(3));
    auto *op = storage.Create<IfOperator>(condition_false, then_expression,
                                          else_expression);
    auto value = op->Accept(eval.eval);
    ASSERT_EQ(value.Value<int64_t>(), 20);
  }
  {
    auto *condition_exception =
        storage.Create<AdditionOperator>(storage.Create<PrimitiveLiteral>(2),
                                         storage.Create<PrimitiveLiteral>(3));
    auto *op = storage.Create<IfOperator>(condition_exception, then_expression,
                                          else_expression);
    ASSERT_THROW(op->Accept(eval.eval), QueryRuntimeException);
  }
}

TEST(ExpressionEvaluator, NotOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<NotOperator>(storage.Create<PrimitiveLiteral>(false));
  auto value = op->Accept(eval.eval);
  ASSERT_EQ(value.Value<bool>(), true);
}

TEST(ExpressionEvaluator, UnaryPlusOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<UnaryPlusOperator>(storage.Create<PrimitiveLiteral>(5));
  auto value = op->Accept(eval.eval);
  ASSERT_EQ(value.Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, UnaryMinusOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<UnaryMinusOperator>(storage.Create<PrimitiveLiteral>(5));
  auto value = op->Accept(eval.eval);
  ASSERT_EQ(value.Value<int64_t>(), -5);
}

TEST(ExpressionEvaluator, IsNullOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<IsNullOperator>(storage.Create<PrimitiveLiteral>(1));
  auto val1 = op->Accept(eval.eval);
  ASSERT_EQ(val1.Value<bool>(), false);
  op = storage.Create<IsNullOperator>(
      storage.Create<PrimitiveLiteral>(TypedValue::Null));
  auto val2 = op->Accept(eval.eval);
  ASSERT_EQ(val2.Value<bool>(), true);
}

class ExpressionEvaluatorPropertyLookup : public testing::Test {
 protected:
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  GraphDb db;
  GraphDbAccessor dba{db};
  std::pair<std::string, GraphDbTypes::Property> prop_age =
      PROPERTY_PAIR("age");
  std::pair<std::string, GraphDbTypes::Property> prop_height =
      PROPERTY_PAIR("height");
  Expression *identifier = storage.Create<Identifier>("element");
  Symbol symbol = eval.symbol_table.CreateSymbol("element", true);

  void SetUp() { eval.symbol_table[*identifier] = symbol; }

  auto Value(std::pair<std::string, GraphDbTypes::Property> property) {
    auto *op = storage.Create<PropertyLookup>(identifier, property);
    return op->Accept(eval.eval);
  }
};

TEST_F(ExpressionEvaluatorPropertyLookup, Vertex) {
  auto v1 = dba.InsertVertex();
  v1.PropsSet(prop_age.second, 10);
  eval.frame[symbol] = v1;
  EXPECT_EQ(Value(prop_age).Value<int64_t>(), 10);
  EXPECT_TRUE(Value(prop_height).IsNull());
}

TEST_F(ExpressionEvaluatorPropertyLookup, Edge) {
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto e12 = dba.InsertEdge(v1, v2, dba.EdgeType("edge_type"));
  e12.PropsSet(prop_age.second, 10);
  eval.frame[symbol] = e12;
  EXPECT_EQ(Value(prop_age).Value<int64_t>(), 10);
  EXPECT_TRUE(Value(prop_height).IsNull());
}

TEST_F(ExpressionEvaluatorPropertyLookup, Null) {
  eval.frame[symbol] = TypedValue::Null;
  EXPECT_TRUE(Value(prop_age).IsNull());
}

TEST_F(ExpressionEvaluatorPropertyLookup, MapLiteral) {
  eval.frame[symbol] = std::map<std::string, TypedValue>{{prop_age.first, 10}};
  EXPECT_EQ(Value(prop_age).Value<int64_t>(), 10);
  EXPECT_TRUE(Value(prop_height).IsNull());
}

TEST(ExpressionEvaluator, LabelsTest) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  GraphDb db;
  GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  v1.add_label(dba.Label("ANIMAL"));
  v1.add_label(dba.Label("DOG"));
  v1.add_label(dba.Label("NICE_DOG"));
  auto *identifier = storage.Create<Identifier>("n");
  auto node_symbol = eval.symbol_table.CreateSymbol("n", true);
  eval.symbol_table[*identifier] = node_symbol;
  eval.frame[node_symbol] = v1;
  {
    auto *op = storage.Create<LabelsTest>(
        identifier, std::vector<GraphDbTypes::Label>{dba.Label("DOG"),
                                                     dba.Label("ANIMAL")});
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.Value<bool>(), true);
  }
  {
    auto *op = storage.Create<LabelsTest>(
        identifier,
        std::vector<GraphDbTypes::Label>{dba.Label("DOG"), dba.Label("BAD_DOG"),
                                         dba.Label("ANIMAL")});
    auto value = op->Accept(eval.eval);
    EXPECT_EQ(value.Value<bool>(), false);
  }
  {
    eval.frame[node_symbol] = TypedValue::Null;
    auto *op = storage.Create<LabelsTest>(
        identifier,
        std::vector<GraphDbTypes::Label>{dba.Label("DOG"), dba.Label("BAD_DOG"),
                                         dba.Label("ANIMAL")});
    auto value = op->Accept(eval.eval);
    EXPECT_TRUE(value.IsNull());
  }
}

TEST(ExpressionEvaluator, Aggregation) {
  AstTreeStorage storage;
  auto aggr = storage.Create<Aggregation>(storage.Create<PrimitiveLiteral>(42),
                                          nullptr, Aggregation::Op::COUNT);
  SymbolTable symbol_table;
  auto aggr_sym = symbol_table.CreateSymbol("aggr", true);
  symbol_table[*aggr] = aggr_sym;
  Frame frame{symbol_table.max_position()};
  frame[aggr_sym] = TypedValue(1);
  GraphDb db;
  GraphDbAccessor dba(db);
  Parameters parameters;
  ExpressionEvaluator eval{frame, parameters, symbol_table, dba};
  auto value = aggr->Accept(eval);
  EXPECT_EQ(value.Value<int64_t>(), 1);
}

TEST(ExpressionEvaluator, ListLiteral) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *list_literal = storage.Create<ListLiteral>(
      std::vector<Expression *>{storage.Create<PrimitiveLiteral>(1),
                                storage.Create<PrimitiveLiteral>("bla"),
                                storage.Create<PrimitiveLiteral>(true)});
  TypedValue result = list_literal->Accept(eval.eval);
  ASSERT_EQ(result.type(), TypedValue::Type::List);
  auto &result_elems = result.Value<std::vector<TypedValue>>();
  ASSERT_EQ(3, result_elems.size());
  EXPECT_EQ(result_elems[0].type(), TypedValue::Type::Int);
  EXPECT_EQ(result_elems[1].type(), TypedValue::Type::String);
  EXPECT_EQ(result_elems[2].type(), TypedValue::Type::Bool);
}

TEST(ExpressionEvaluator, FunctionCoalesce) {
  ASSERT_THROW(EvaluateFunction("COALESCE", {}), QueryRuntimeException);
  ASSERT_EQ(
      EvaluateFunction("COALESCE", {TypedValue::Null, TypedValue::Null}).type(),
      TypedValue::Type::Null);
  ASSERT_EQ(
      EvaluateFunction("COALESCE", {TypedValue::Null, 2, 3}).Value<int64_t>(),
      2);
}

TEST(ExpressionEvaluator, FunctionEndNode) {
  ASSERT_THROW(EvaluateFunction("ENDNODE", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("ENDNODE", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  GraphDb db;
  GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  v1.add_label(dba.Label("label1"));
  auto v2 = dba.InsertVertex();
  v2.add_label(dba.Label("label2"));
  auto e = dba.InsertEdge(v1, v2, dba.EdgeType("t"));
  ASSERT_TRUE(EvaluateFunction("ENDNODE", {e})
                  .Value<VertexAccessor>()
                  .has_label(dba.Label("label2")));
  ASSERT_THROW(EvaluateFunction("ENDNODE", {2}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionHead) {
  ASSERT_THROW(EvaluateFunction("HEAD", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("HEAD", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  std::vector<TypedValue> arguments;
  arguments.push_back(std::vector<TypedValue>{3, 4, 5});
  ASSERT_EQ(EvaluateFunction("HEAD", arguments).Value<int64_t>(), 3);
  arguments[0].Value<std::vector<TypedValue>>().clear();
  ASSERT_EQ(EvaluateFunction("HEAD", arguments).type(), TypedValue::Type::Null);
  ASSERT_THROW(EvaluateFunction("HEAD", {2}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionProperties) {
  ASSERT_THROW(EvaluateFunction("PROPERTIES", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("PROPERTIES", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  GraphDb db;
  GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  v1.PropsSet(dba.Property("height"), 5);
  v1.PropsSet(dba.Property("age"), 10);
  auto v2 = dba.InsertVertex();
  auto e = dba.InsertEdge(v1, v2, dba.EdgeType("type1"));
  e.PropsSet(dba.Property("height"), 3);
  e.PropsSet(dba.Property("age"), 15);

  auto prop_values_to_int = [](TypedValue t) {
    std::unordered_map<std::string, int> properties;
    for (auto property : t.Value<std::map<std::string, TypedValue>>()) {
      properties[property.first] = property.second.Value<int64_t>();
    }
    return properties;
  };

  ASSERT_THAT(prop_values_to_int(EvaluateFunction("PROPERTIES", {v1}, db)),
              UnorderedElementsAre(testing::Pair("height", 5),
                                   testing::Pair("age", 10)));
  ASSERT_THAT(prop_values_to_int(EvaluateFunction("PROPERTIES", {e}, db)),
              UnorderedElementsAre(testing::Pair("height", 3),
                                   testing::Pair("age", 15)));
  ASSERT_THROW(EvaluateFunction("PROPERTIES", {2}, db), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionLast) {
  ASSERT_THROW(EvaluateFunction("LAST", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("LAST", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  std::vector<TypedValue> arguments;
  arguments.push_back(std::vector<TypedValue>{3, 4, 5});
  ASSERT_EQ(EvaluateFunction("LAST", arguments).Value<int64_t>(), 5);
  arguments[0].Value<std::vector<TypedValue>>().clear();
  ASSERT_EQ(EvaluateFunction("LAST", arguments).type(), TypedValue::Type::Null);
  ASSERT_THROW(EvaluateFunction("LAST", {5}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionSize) {
  ASSERT_THROW(EvaluateFunction("SIZE", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("SIZE", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  std::vector<TypedValue> arguments;
  arguments.push_back(std::vector<TypedValue>{3, 4, 5});
  ASSERT_EQ(EvaluateFunction("SIZE", arguments).Value<int64_t>(), 3);
  ASSERT_EQ(EvaluateFunction("SIZE", {"john"}).Value<int64_t>(), 4);
  ASSERT_EQ(EvaluateFunction("SIZE", {std::map<std::string, TypedValue>{
                                         {"a", 5}, {"b", true}, {"c", "123"}}})
                .Value<int64_t>(),
            3);
  ASSERT_THROW(EvaluateFunction("SIZE", {5}), QueryRuntimeException);

  GraphDb db;
  GraphDbAccessor dba(db);
  auto v0 = dba.InsertVertex();
  query::Path path(v0);
  EXPECT_EQ(EvaluateFunction("SIZE", {path}).ValueInt(), 0);
  auto v1 = dba.InsertVertex();
  path.Expand(dba.InsertEdge(v0, v1, dba.EdgeType("type")));
  path.Expand(v1);
  EXPECT_EQ(EvaluateFunction("SIZE", {path}).ValueInt(), 1);
}

TEST(ExpressionEvaluator, FunctionStartNode) {
  ASSERT_THROW(EvaluateFunction("STARTNODE", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("STARTNODE", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  GraphDb db;
  GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  v1.add_label(dba.Label("label1"));
  auto v2 = dba.InsertVertex();
  v2.add_label(dba.Label("label2"));
  auto e = dba.InsertEdge(v1, v2, dba.EdgeType("t"));
  ASSERT_TRUE(EvaluateFunction("STARTNODE", {e})
                  .Value<VertexAccessor>()
                  .has_label(dba.Label("label1")));
  ASSERT_THROW(EvaluateFunction("STARTNODE", {2}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionDegree) {
  ASSERT_THROW(EvaluateFunction("DEGREE", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("DEGREE", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  GraphDb db;
  GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto e12 = dba.InsertEdge(v1, v2, dba.EdgeType("t"));
  dba.InsertEdge(v3, v2, dba.EdgeType("t"));
  ASSERT_EQ(EvaluateFunction("DEGREE", {v1}).Value<int64_t>(), 1);
  ASSERT_EQ(EvaluateFunction("DEGREE", {v2}).Value<int64_t>(), 2);
  ASSERT_EQ(EvaluateFunction("DEGREE", {v3}).Value<int64_t>(), 1);
  ASSERT_THROW(EvaluateFunction("DEGREE", {2}), QueryRuntimeException);
  ASSERT_THROW(EvaluateFunction("DEGREE", {e12}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionToBoolean) {
  ASSERT_THROW(EvaluateFunction("TOBOOLEAN", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {123}).ValueBool(), true);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {-213}).ValueBool(), true);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {0}).ValueBool(), false);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {" trUE \n\t"}).Value<bool>(), true);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {"\n\tFalsE "}).Value<bool>(), false);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {"\n\tFALSEA "}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {true}).Value<bool>(), true);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {false}).Value<bool>(), false);
}

TEST(ExpressionEvaluator, FunctionToFloat) {
  ASSERT_THROW(EvaluateFunction("TOFLOAT", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("TOFLOAT", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("TOFLOAT", {" -3.5 \n\t"}).Value<double>(), -3.5);
  ASSERT_EQ(EvaluateFunction("TOFLOAT", {"\n\t0.5e-1"}).Value<double>(), 0.05);
  ASSERT_EQ(EvaluateFunction("TOFLOAT", {"\n\t3.4e-3X "}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("TOFLOAT", {-3.5}).Value<double>(), -3.5);
  ASSERT_EQ(EvaluateFunction("TOFLOAT", {-3}).Value<double>(), -3.0);
  ASSERT_THROW(EvaluateFunction("TOFLOAT", {true}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionToInteger) {
  ASSERT_THROW(EvaluateFunction("TOINTEGER", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {false}).Value<int64_t>(), 0);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {true}).Value<int64_t>(), 1);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {"\n\t3"}).Value<int64_t>(), 3);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {" -3.5 \n\t"}).Value<int64_t>(), -3);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {"\n\t3X "}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {-3.5}).Value<int64_t>(), -3);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {3.5}).Value<int64_t>(), 3);
}

TEST(ExpressionEvaluator, FunctionType) {
  ASSERT_THROW(EvaluateFunction("TYPE", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("TYPE", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  GraphDb db;
  GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  v1.add_label(dba.Label("label1"));
  auto v2 = dba.InsertVertex();
  v2.add_label(dba.Label("label2"));
  auto e = dba.InsertEdge(v1, v2, dba.EdgeType("type1"));
  ASSERT_EQ(EvaluateFunction("TYPE", {e}, db).Value<std::string>(), "type1");
  ASSERT_THROW(EvaluateFunction("TYPE", {2}, db), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionLabels) {
  ASSERT_THROW(EvaluateFunction("LABELS", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("LABELS", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  GraphDb db;
  GraphDbAccessor dba(db);
  auto v = dba.InsertVertex();
  v.add_label(dba.Label("label1"));
  v.add_label(dba.Label("label2"));
  std::vector<std::string> labels;
  auto _labels =
      EvaluateFunction("LABELS", {v}, db).Value<std::vector<TypedValue>>();
  for (auto label : _labels) {
    labels.push_back(label.Value<std::string>());
  }
  ASSERT_THAT(labels, UnorderedElementsAre("label1", "label2"));
  ASSERT_THROW(EvaluateFunction("LABELS", {2}, db), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionNodesRelationships) {
  EXPECT_THROW(EvaluateFunction("NODES", {}), QueryRuntimeException);
  EXPECT_THROW(EvaluateFunction("RELATIONSHIPS", {}), QueryRuntimeException);
  EXPECT_TRUE(EvaluateFunction("NODES", {TypedValue::Null}).IsNull());
  EXPECT_TRUE(EvaluateFunction("RELATIONSHIPS", {TypedValue::Null}).IsNull());

  {
    GraphDb db;
    GraphDbAccessor dba(db);
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    auto v3 = dba.InsertVertex();
    auto e1 = dba.InsertEdge(v1, v2, dba.EdgeType("Type"));
    auto e2 = dba.InsertEdge(v2, v3, dba.EdgeType("Type"));
    query::Path path(v1, e1, v2, e2, v3);

    auto _nodes = EvaluateFunction("NODES", {path}).ValueList();
    std::vector<VertexAccessor> nodes;
    for (const auto &node : _nodes) {
      nodes.push_back(node.ValueVertex());
    }
    EXPECT_THAT(nodes, ElementsAre(v1, v2, v3));

    auto _edges = EvaluateFunction("RELATIONSHIPS", {path}).ValueList();
    std::vector<EdgeAccessor> edges;
    for (const auto &edge : _edges) {
      edges.push_back(edge.ValueEdge());
    }
    EXPECT_THAT(edges, ElementsAre(e1, e2));
  }

  EXPECT_THROW(EvaluateFunction("NODES", {2}), QueryRuntimeException);
  EXPECT_THROW(EvaluateFunction("RELATIONSHIPS", {2}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionRange) {
  EXPECT_THROW(EvaluateFunction("RANGE", {}), QueryRuntimeException);
  EXPECT_TRUE(EvaluateFunction("RANGE", {1, 2, TypedValue::Null}).IsNull());
  EXPECT_THROW(EvaluateFunction("RANGE", {1, TypedValue::Null, 1.3}),
               QueryRuntimeException);
  EXPECT_THROW(EvaluateFunction("RANGE", {1, 2, 0}), QueryRuntimeException);
  EXPECT_THAT(ToList<int64_t>(EvaluateFunction("RANGE", {1, 3})),
              ElementsAre(1, 2, 3));
  EXPECT_THAT(ToList<int64_t>(EvaluateFunction("RANGE", {-1, 5, 2})),
              ElementsAre(-1, 1, 3, 5));
  EXPECT_THAT(ToList<int64_t>(EvaluateFunction("RANGE", {2, 10, 3})),
              ElementsAre(2, 5, 8));
  EXPECT_THAT(ToList<int64_t>(EvaluateFunction("RANGE", {2, 2, 2})),
              ElementsAre(2));
  EXPECT_THAT(ToList<int64_t>(EvaluateFunction("RANGE", {3, 0, 5})),
              ElementsAre());
  EXPECT_THAT(ToList<int64_t>(EvaluateFunction("RANGE", {5, 1, -2})),
              ElementsAre(5, 3, 1));
  EXPECT_THAT(ToList<int64_t>(EvaluateFunction("RANGE", {6, 1, -2})),
              ElementsAre(6, 4, 2));
  EXPECT_THAT(ToList<int64_t>(EvaluateFunction("RANGE", {2, 2, -3})),
              ElementsAre(2));
  EXPECT_THAT(ToList<int64_t>(EvaluateFunction("RANGE", {-2, 4, -1})),
              ElementsAre());
}

TEST(ExpressionEvaluator, FunctionKeys) {
  ASSERT_THROW(EvaluateFunction("KEYS", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("KEYS", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  GraphDb db;
  GraphDbAccessor dba(db);
  auto v1 = dba.InsertVertex();
  v1.PropsSet(dba.Property("height"), 5);
  v1.PropsSet(dba.Property("age"), 10);
  auto v2 = dba.InsertVertex();
  auto e = dba.InsertEdge(v1, v2, dba.EdgeType("type1"));
  e.PropsSet(dba.Property("width"), 3);
  e.PropsSet(dba.Property("age"), 15);

  auto prop_keys_to_string = [](TypedValue t) {
    std::vector<std::string> keys;
    for (auto property : t.Value<std::vector<TypedValue>>()) {
      keys.push_back(property.Value<std::string>());
    }
    return keys;
  };
  ASSERT_THAT(prop_keys_to_string(EvaluateFunction("KEYS", {v1}, db)),
              UnorderedElementsAre("height", "age"));
  ASSERT_THAT(prop_keys_to_string(EvaluateFunction("KEYS", {e}, db)),
              UnorderedElementsAre("width", "age"));
  ASSERT_THROW(EvaluateFunction("KEYS", {2}, db), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionTail) {
  ASSERT_THROW(EvaluateFunction("TAIL", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("TAIL", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  std::vector<TypedValue> arguments;
  arguments.push_back(std::vector<TypedValue>{});
  ASSERT_EQ(EvaluateFunction("TAIL", arguments)
                .Value<std::vector<TypedValue>>()
                .size(),
            0U);
  arguments[0] = std::vector<TypedValue>{3, 4, true, "john"};
  auto list =
      EvaluateFunction("TAIL", arguments).Value<std::vector<TypedValue>>();
  ASSERT_EQ(list.size(), 3U);
  ASSERT_EQ(list[0].Value<int64_t>(), 4);
  ASSERT_EQ(list[1].Value<bool>(), true);
  ASSERT_EQ(list[2].Value<std::string>(), "john");
  ASSERT_THROW(EvaluateFunction("TAIL", {2}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionAbs) {
  ASSERT_THROW(EvaluateFunction("ABS", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("ABS", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("ABS", {-2}).Value<int64_t>(), 2);
  ASSERT_EQ(EvaluateFunction("ABS", {-2.5}).Value<double>(), 2.5);
  ASSERT_THROW(EvaluateFunction("ABS", {true}), QueryRuntimeException);
}

// Test if log works. If it does then all functions wrapped with
// WRAP_CMATH_FLOAT_FUNCTION macro should work and are not gonna be tested for
// correctnes..
TEST(ExpressionEvaluator, FunctionLog) {
  ASSERT_THROW(EvaluateFunction("LOG", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("LOG", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  ASSERT_DOUBLE_EQ(EvaluateFunction("LOG", {2}).Value<double>(), log(2));
  ASSERT_DOUBLE_EQ(EvaluateFunction("LOG", {1.5}).Value<double>(), log(1.5));
  // Not portable, but should work on most platforms.
  ASSERT_TRUE(std::isnan(EvaluateFunction("LOG", {-1.5}).Value<double>()));
  ASSERT_THROW(EvaluateFunction("LOG", {true}), QueryRuntimeException);
}

// Function Round wraps round from cmath and will work if FunctionLog test
// passes. This test is used to show behavior of round since it differs from
// neo4j's round.
TEST(ExpressionEvaluator, FunctionRound) {
  ASSERT_THROW(EvaluateFunction("ROUND", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("ROUND", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("ROUND", {-2}).Value<double>(), -2);
  ASSERT_EQ(EvaluateFunction("ROUND", {-2.4}).Value<double>(), -2);
  ASSERT_EQ(EvaluateFunction("ROUND", {-2.5}).Value<double>(), -3);
  ASSERT_EQ(EvaluateFunction("ROUND", {-2.6}).Value<double>(), -3);
  ASSERT_EQ(EvaluateFunction("ROUND", {2.4}).Value<double>(), 2);
  ASSERT_EQ(EvaluateFunction("ROUND", {2.5}).Value<double>(), 3);
  ASSERT_EQ(EvaluateFunction("ROUND", {2.6}).Value<double>(), 3);
  ASSERT_THROW(EvaluateFunction("ROUND", {true}), QueryRuntimeException);
}

// Check if wrapped functions are callable (check if everything was spelled
// correctly...). Wrapper correctnes is checked in FunctionLog test.
TEST(ExpressionEvaluator, FunctionWrappedMathFunctions) {
  for (auto function_name :
       {"FLOOR", "CEIL", "ROUND", "EXP", "LOG", "LOG10", "SQRT", "ACOS", "ASIN",
        "ATAN", "COS", "SIN", "TAN"}) {
    EvaluateFunction(function_name, {0.5});
  }
}

TEST(ExpressionEvaluator, FunctionAtan2) {
  ASSERT_THROW(EvaluateFunction("ATAN2", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("ATAN2", {TypedValue::Null, 1}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("ATAN2", {1, TypedValue::Null}).type(),
            TypedValue::Type::Null);
  ASSERT_DOUBLE_EQ(EvaluateFunction("ATAN2", {2, -1.0}).Value<double>(),
                   atan2(2, -1));
  ASSERT_THROW(EvaluateFunction("ATAN2", {3.0, true}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionSign) {
  ASSERT_THROW(EvaluateFunction("SIGN", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("SIGN", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("SIGN", {-2}).Value<int64_t>(), -1);
  ASSERT_EQ(EvaluateFunction("SIGN", {-0.2}).Value<int64_t>(), -1);
  ASSERT_EQ(EvaluateFunction("SIGN", {0.0}).Value<int64_t>(), 0);
  ASSERT_EQ(EvaluateFunction("SIGN", {2.5}).Value<int64_t>(), 1);
  ASSERT_THROW(EvaluateFunction("SIGN", {true}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionE) {
  ASSERT_THROW(EvaluateFunction("E", {1}), QueryRuntimeException);
  ASSERT_DOUBLE_EQ(EvaluateFunction("E", {}).Value<double>(), M_E);
}

TEST(ExpressionEvaluator, FunctionPi) {
  ASSERT_THROW(EvaluateFunction("PI", {1}), QueryRuntimeException);
  ASSERT_DOUBLE_EQ(EvaluateFunction("PI", {}).Value<double>(), M_PI);
}

TEST(ExpressionEvaluator, FunctionRand) {
  ASSERT_THROW(EvaluateFunction("RAND", {1}), QueryRuntimeException);
  ASSERT_GE(EvaluateFunction("RAND", {}).Value<double>(), 0.0);
  ASSERT_LT(EvaluateFunction("RAND", {}).Value<double>(), 1.0);
}

TEST(ExpressionEvaluator, FunctionStartsWith) {
  EXPECT_THROW(EvaluateFunction(kStartsWith, {}), QueryRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kStartsWith, {"a", TypedValue::Null}).IsNull());
  EXPECT_THROW(EvaluateFunction(kStartsWith, {TypedValue::Null, 1.3}),
               QueryRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kStartsWith, {"abc", "abc"}).Value<bool>());
  EXPECT_TRUE(EvaluateFunction(kStartsWith, {"abcdef", "abc"}).Value<bool>());
  EXPECT_FALSE(EvaluateFunction(kStartsWith, {"abcdef", "aBc"}).Value<bool>());
  EXPECT_FALSE(EvaluateFunction(kStartsWith, {"abc", "abcd"}).Value<bool>());
}

TEST(ExpressionEvaluator, FunctionEndsWith) {
  EXPECT_THROW(EvaluateFunction(kEndsWith, {}), QueryRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kEndsWith, {"a", TypedValue::Null}).IsNull());
  EXPECT_THROW(EvaluateFunction(kEndsWith, {TypedValue::Null, 1.3}),
               QueryRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kEndsWith, {"abc", "abc"}).Value<bool>());
  EXPECT_TRUE(EvaluateFunction(kEndsWith, {"abcdef", "def"}).Value<bool>());
  EXPECT_FALSE(EvaluateFunction(kEndsWith, {"abcdef", "dEf"}).Value<bool>());
  EXPECT_FALSE(EvaluateFunction(kEndsWith, {"bcd", "abcd"}).Value<bool>());
}

TEST(ExpressionEvaluator, FunctionContains) {
  EXPECT_THROW(EvaluateFunction(kContains, {}), QueryRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kContains, {"a", TypedValue::Null}).IsNull());
  EXPECT_THROW(EvaluateFunction(kContains, {TypedValue::Null, 1.3}),
               QueryRuntimeException);
  EXPECT_TRUE(EvaluateFunction(kContains, {"abc", "abc"}).Value<bool>());
  EXPECT_TRUE(EvaluateFunction(kContains, {"abcde", "bcd"}).Value<bool>());
  EXPECT_FALSE(EvaluateFunction(kContains, {"cde", "abcdef"}).Value<bool>());
  EXPECT_FALSE(EvaluateFunction(kContains, {"abcdef", "dEf"}).Value<bool>());
}

TEST(ExpressionEvaluator, FunctionAll) {
  AstTreeStorage storage;
  auto *ident_x = IDENT("x");
  auto *all =
      ALL("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(1))));
  NoContextExpressionEvaluator eval;
  const auto x_sym = eval.symbol_table.CreateSymbol("x", true);
  eval.symbol_table[*all->identifier_] = x_sym;
  eval.symbol_table[*ident_x] = x_sym;
  auto value = all->Accept(eval.eval);
  ASSERT_EQ(value.type(), TypedValue::Type::Bool);
  EXPECT_FALSE(value.Value<bool>());
}

TEST(ExpressionEvaluator, FunctionAllNullList) {
  AstTreeStorage storage;
  auto *all = ALL("x", LITERAL(TypedValue::Null), WHERE(LITERAL(true)));
  NoContextExpressionEvaluator eval;
  const auto x_sym = eval.symbol_table.CreateSymbol("x", true);
  eval.symbol_table[*all->identifier_] = x_sym;
  auto value = all->Accept(eval.eval);
  EXPECT_TRUE(value.IsNull());
}

TEST(ExpressionEvaluator, FunctionAllWhereWrongType) {
  AstTreeStorage storage;
  auto *all = ALL("x", LIST(LITERAL(1)), WHERE(LITERAL(2)));
  NoContextExpressionEvaluator eval;
  const auto x_sym = eval.symbol_table.CreateSymbol("x", true);
  eval.symbol_table[*all->identifier_] = x_sym;
  EXPECT_THROW(all->Accept(eval.eval), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionAssert) {
  // Invalid calls.
  ASSERT_THROW(EvaluateFunction("ASSERT", {}), QueryRuntimeException);
  ASSERT_THROW(EvaluateFunction("ASSERT", {false, false}),
               QueryRuntimeException);
  ASSERT_THROW(EvaluateFunction("ASSERT", {"string", false}),
               QueryRuntimeException);
  ASSERT_THROW(EvaluateFunction("ASSERT", {false, "reason", true}),
               QueryRuntimeException);

  // Valid calls, assertion fails.
  ASSERT_THROW(EvaluateFunction("ASSERT", {false}), QueryRuntimeException);
  ASSERT_THROW(EvaluateFunction("ASSERT", {false, "message"}),
               QueryRuntimeException);
  try {
    EvaluateFunction("ASSERT", {false, "bbgba"});
  } catch (QueryRuntimeException &e) {
    ASSERT_TRUE(utils::EndsWith(e.what(), "bbgba"));
  }

  // Valid calls, assertion passes.
  ASSERT_TRUE(EvaluateFunction("ASSERT", {true}).ValueBool());
  ASSERT_TRUE(EvaluateFunction("ASSERT", {true, "message"}).ValueBool());
}

TEST(ExpressionEvaluator, ParameterLookup) {
  NoContextExpressionEvaluator eval;
  eval.parameters.Add(0, 42);
  AstTreeStorage storage;
  auto *param_lookup = storage.Create<ParameterLookup>(0);
  auto value = param_lookup->Accept(eval.eval);
  ASSERT_EQ(value.type(), TypedValue::Type::Int);
  EXPECT_EQ(value.Value<int64_t>(), 42);
}

TEST(ExpressionEvaluator, FunctionCounter) {
  GraphDb db;
  EXPECT_THROW(EvaluateFunction("COUNTER", {}, db), QueryRuntimeException);
  EXPECT_THROW(EvaluateFunction("COUNTER", {"a", "b"}, db),
               QueryRuntimeException);
  EXPECT_EQ(EvaluateFunction("COUNTER", {"c1"}, db).ValueInt(), 0);
  EXPECT_EQ(EvaluateFunction("COUNTER", {"c1"}, db).ValueInt(), 1);
  EXPECT_EQ(EvaluateFunction("COUNTER", {"c2"}, db).ValueInt(), 0);
  EXPECT_EQ(EvaluateFunction("COUNTER", {"c1"}, db).ValueInt(), 2);
  EXPECT_EQ(EvaluateFunction("COUNTER", {"c2"}, db).ValueInt(), 1);
}

TEST(ExpressionEvaluator, FunctionCounterSet) {
  GraphDb db;
  EXPECT_THROW(EvaluateFunction("COUNTERSET", {}, db), QueryRuntimeException);
  EXPECT_THROW(EvaluateFunction("COUNTERSET", {"a"}, db),
               QueryRuntimeException);
  EXPECT_THROW(EvaluateFunction("COUNTERSET", {"a", "b"}, db),
               QueryRuntimeException);
  EXPECT_THROW(EvaluateFunction("COUNTERSET", {"a", 11, 12}, db),
               QueryRuntimeException);
  EXPECT_EQ(EvaluateFunction("COUNTER", {"c1"}, db).ValueInt(), 0);
  EvaluateFunction("COUNTERSET", {"c1", 12}, db);
  EXPECT_EQ(EvaluateFunction("COUNTER", {"c1"}, db).ValueInt(), 12);
  EvaluateFunction("COUNTERSET", {"c2", 42}, db);
  EXPECT_EQ(EvaluateFunction("COUNTER", {"c2"}, db).ValueInt(), 42);
  EXPECT_EQ(EvaluateFunction("COUNTER", {"c1"}, db).ValueInt(), 13);
  EXPECT_EQ(EvaluateFunction("COUNTER", {"c2"}, db).ValueInt(), 43);
}

TEST(ExpressionEvaluator, FunctionIndexInfo) {
  GraphDb db;
  EXPECT_THROW(EvaluateFunction("INDEXINFO", {1}, db), QueryRuntimeException);
  EXPECT_EQ(EvaluateFunction("INDEXINFO", {}, db).ValueList().size(), 0);
  GraphDbAccessor dba(db);
  dba.InsertVertex().add_label(dba.Label("l1"));
  {
    auto info = ToList<std::string>(EvaluateFunction("INDEXINFO", {}, db));
    EXPECT_EQ(info.size(), 1);
    EXPECT_EQ(info[0], ":l1");
  }
  {
    dba.BuildIndex(dba.Label("l1"), dba.Property("prop"));
    auto info = ToList<std::string>(EvaluateFunction("INDEXINFO", {}, db));
    EXPECT_EQ(info.size(), 2);
    EXPECT_THAT(info, testing::UnorderedElementsAre(":l1", ":l1(prop)"));
  }
}
}  // namespace
