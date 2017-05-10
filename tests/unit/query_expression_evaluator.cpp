#include <cmath>
#include <iterator>
#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "database/graph_db_accessor.hpp"
#include "database/graph_db_datatypes.hpp"
#include "dbms/dbms.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"

using namespace query;
using testing::Pair;
using testing::UnorderedElementsAre;
using testing::ElementsAre;

namespace {

struct NoContextExpressionEvaluator {
  NoContextExpressionEvaluator() {}
  Frame frame{128};
  SymbolTable symbol_table;
  Dbms dbms;
  std::unique_ptr<GraphDbAccessor> dba = dbms.active();
  ExpressionEvaluator eval{frame, symbol_table, *dba};
};

TypedValue EvaluateFunction(const std::string &function_name,
                            const std::vector<TypedValue> &args) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  Dbms dbms;
  auto dba = dbms.active();

  std::vector<Expression *> expressions;
  for (const auto &arg : args) {
    expressions.push_back(storage.Create<PrimitiveLiteral>(arg));
  }
  auto *op =
      storage.Create<Function>(NameToFunction(function_name), expressions);
  op->Accept(eval.eval);
  return eval.eval.PopBack();
}

TEST(ExpressionEvaluator, OrOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<OrOperator>(storage.Create<PrimitiveLiteral>(true),
                                 storage.Create<PrimitiveLiteral>(false));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<OrOperator>(storage.Create<PrimitiveLiteral>(true),
                                  storage.Create<PrimitiveLiteral>(true));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
}

TEST(ExpressionEvaluator, XorOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<XorOperator>(storage.Create<PrimitiveLiteral>(true),
                                  storage.Create<PrimitiveLiteral>(false));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<XorOperator>(storage.Create<PrimitiveLiteral>(true),
                                   storage.Create<PrimitiveLiteral>(true));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
}

TEST(ExpressionEvaluator, AndOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(true),
                                  storage.Create<PrimitiveLiteral>(true));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<AndOperator>(storage.Create<PrimitiveLiteral>(false),
                                   storage.Create<PrimitiveLiteral>(true));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
}

TEST(ExpressionEvaluator, FilterAndOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  {
    auto *op = storage.Create<FilterAndOperator>(
        storage.Create<PrimitiveLiteral>(true),
        storage.Create<PrimitiveLiteral>(true));
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().Value<bool>(), true);
  }
  {
    auto *op = storage.Create<FilterAndOperator>(
        storage.Create<PrimitiveLiteral>(false),
        storage.Create<PrimitiveLiteral>(5));
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().Value<bool>(), false);
  }
  {
    auto *op = storage.Create<FilterAndOperator>(
        storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>(5));
    op->Accept(eval.eval);
    EXPECT_TRUE(eval.eval.PopBack().IsNull());
  }
}

TEST(ExpressionEvaluator, AdditionOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<AdditionOperator>(
      storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(3));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, SubtractionOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<SubtractionOperator>(
      storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(3));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), -1);
}

TEST(ExpressionEvaluator, MultiplicationOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<MultiplicationOperator>(
      storage.Create<PrimitiveLiteral>(2), storage.Create<PrimitiveLiteral>(3));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), 6);
}

TEST(ExpressionEvaluator, DivisionOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<DivisionOperator>(storage.Create<PrimitiveLiteral>(50),
                                       storage.Create<PrimitiveLiteral>(10));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, ModOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<ModOperator>(storage.Create<PrimitiveLiteral>(65),
                                         storage.Create<PrimitiveLiteral>(10));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, EqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(10),
                                    storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(15),
                                     storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<EqualOperator>(storage.Create<PrimitiveLiteral>(20),
                                     storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
}

TEST(ExpressionEvaluator, NotEqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<NotEqualOperator>(storage.Create<PrimitiveLiteral>(10),
                                       storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<NotEqualOperator>(storage.Create<PrimitiveLiteral>(15),
                                        storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<NotEqualOperator>(storage.Create<PrimitiveLiteral>(20),
                                        storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
}

TEST(ExpressionEvaluator, LessOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<LessOperator>(storage.Create<PrimitiveLiteral>(10),
                                          storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<LessOperator>(storage.Create<PrimitiveLiteral>(15),
                                    storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<LessOperator>(storage.Create<PrimitiveLiteral>(20),
                                    storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
}

TEST(ExpressionEvaluator, GreaterOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<GreaterOperator>(storage.Create<PrimitiveLiteral>(10),
                                      storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<GreaterOperator>(storage.Create<PrimitiveLiteral>(15),
                                       storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<GreaterOperator>(storage.Create<PrimitiveLiteral>(20),
                                       storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
}

TEST(ExpressionEvaluator, LessEqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<LessEqualOperator>(storage.Create<PrimitiveLiteral>(10),
                                        storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<LessEqualOperator>(storage.Create<PrimitiveLiteral>(15),
                                         storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<LessEqualOperator>(storage.Create<PrimitiveLiteral>(20),
                                         storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
}

TEST(ExpressionEvaluator, GreaterEqualOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op = storage.Create<GreaterEqualOperator>(
      storage.Create<PrimitiveLiteral>(10),
      storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<GreaterEqualOperator>(
      storage.Create<PrimitiveLiteral>(15),
      storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
  op = storage.Create<GreaterEqualOperator>(
      storage.Create<PrimitiveLiteral>(20),
      storage.Create<PrimitiveLiteral>(15));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
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
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().Value<bool>(), true);
  }
  {
    // Element doesn't exist in list.
    auto *op = storage.Create<InListOperator>(
        storage.Create<PrimitiveLiteral>("x"), list_literal);
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().Value<bool>(), false);
  }
  {
    auto *list_literal = storage.Create<ListLiteral>(std::vector<Expression *>{
        storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>(2),
        storage.Create<PrimitiveLiteral>("a")});
    // Element doesn't exist in list with null element.
    auto *op = storage.Create<InListOperator>(
        storage.Create<PrimitiveLiteral>("x"), list_literal);
    op->Accept(eval.eval);
    EXPECT_TRUE(eval.eval.PopBack().IsNull());
  }
  {
    // Null list.
    auto *op = storage.Create<InListOperator>(
        storage.Create<PrimitiveLiteral>("x"),
        storage.Create<PrimitiveLiteral>(TypedValue::Null));
    op->Accept(eval.eval);
    EXPECT_TRUE(eval.eval.PopBack().IsNull());
  }
}

TEST(ExpressionEvaluator, ListIndexingOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *list_literal = storage.Create<ListLiteral>(std::vector<Expression *>{
      storage.Create<PrimitiveLiteral>(1), storage.Create<PrimitiveLiteral>(2),
      storage.Create<PrimitiveLiteral>(3),
      storage.Create<PrimitiveLiteral>(4)});
  {
    // Legal indexing.
    auto *op = storage.Create<ListIndexingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(2));
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().Value<int64_t>(), 3);
  }
  {
    // Out of bounds indexing.
    auto *op = storage.Create<ListIndexingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(4));
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().type(), TypedValue::Type::Null);
  }
  {
    // Out of bounds indexing with negative bound.
    auto *op = storage.Create<ListIndexingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(-100));
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().type(), TypedValue::Type::Null);
  }
  {
    // Legal indexing with negative index.
    auto *op = storage.Create<ListIndexingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(-2));
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().Value<int64_t>(), 3);
  }
  {
    // Indexing with one operator being null.
    auto *op = storage.Create<ListIndexingOperator>(
        storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>(-2));
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().type(), TypedValue::Type::Null);
  }
  {
    // Indexing with incompatible type.
    auto *op = storage.Create<ListIndexingOperator>(
        storage.Create<PrimitiveLiteral>(2),
        storage.Create<PrimitiveLiteral>(TypedValue::Null));
    EXPECT_THROW(op->Accept(eval.eval), TypedValueException);
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
    op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(eval.eval.PopBack()), ElementsAre(3, 4));
  }
  {
    // Legal slicing with negative bound.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(2),
        storage.Create<PrimitiveLiteral>(-1));
    op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(eval.eval.PopBack()), ElementsAre(3));
  }
  {
    // Lower bound larger than upper bound.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(2),
        storage.Create<PrimitiveLiteral>(-4));
    op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(eval.eval.PopBack()), ElementsAre());
  }
  {
    // Bounds ouf or range.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(-100),
        storage.Create<PrimitiveLiteral>(10));
    op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(eval.eval.PopBack()), ElementsAre(1, 2, 3, 4));
  }
  {
    // Lower bound undefined.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, nullptr, storage.Create<PrimitiveLiteral>(3));
    op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(eval.eval.PopBack()), ElementsAre(1, 2, 3));
  }
  {
    // Upper bound undefined.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(-2), nullptr);
    op->Accept(eval.eval);
    EXPECT_THAT(extract_ints(eval.eval.PopBack()), ElementsAre(3, 4));
  }
  {
    // Bound of illegal type and null value bound.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>("mirko"));
    EXPECT_THROW(op->Accept(eval.eval), TypedValueException);
  }
  {
    // List of illegal type.
    auto *op = storage.Create<ListSlicingOperator>(
        storage.Create<PrimitiveLiteral>("a"),
        storage.Create<PrimitiveLiteral>(-2), nullptr);
    EXPECT_THROW(op->Accept(eval.eval), TypedValueException);
  }
  {
    // Null value list with undefined upper bound.
    auto *op = storage.Create<ListSlicingOperator>(
        storage.Create<PrimitiveLiteral>(TypedValue::Null),
        storage.Create<PrimitiveLiteral>(-2), nullptr);
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().type(), TypedValue::Type::Null);
  }
  {
    // Null value index.
    auto *op = storage.Create<ListSlicingOperator>(
        list_literal, storage.Create<PrimitiveLiteral>(-2),
        storage.Create<PrimitiveLiteral>(TypedValue::Null));
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().type(), TypedValue::Type::Null);
  }
}

TEST(ExpressionEvaluator, NotOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<NotOperator>(storage.Create<PrimitiveLiteral>(false));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
}

TEST(ExpressionEvaluator, UnaryPlusOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<UnaryPlusOperator>(storage.Create<PrimitiveLiteral>(5));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), 5);
}

TEST(ExpressionEvaluator, UnaryMinusOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<UnaryMinusOperator>(storage.Create<PrimitiveLiteral>(5));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<int64_t>(), -5);
}

TEST(ExpressionEvaluator, IsNullOperator) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *op =
      storage.Create<IsNullOperator>(storage.Create<PrimitiveLiteral>(1));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), false);
  op = storage.Create<IsNullOperator>(
      storage.Create<PrimitiveLiteral>(TypedValue::Null));
  op->Accept(eval.eval);
  ASSERT_EQ(eval.eval.PopBack().Value<bool>(), true);
}

TEST(ExpressionEvaluator, PropertyLookup) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->insert_vertex();
  v1.PropsSet(dba->property("age"), 10);
  auto *identifier = storage.Create<Identifier>("n");
  auto node_symbol = eval.symbol_table.CreateSymbol("n");
  eval.symbol_table[*identifier] = node_symbol;
  eval.frame[node_symbol] = v1;
  {
    auto *op = storage.Create<PropertyLookup>(identifier, dba->property("age"));
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().Value<int64_t>(), 10);
  }
  {
    auto *op =
        storage.Create<PropertyLookup>(identifier, dba->property("height"));
    op->Accept(eval.eval);
    EXPECT_TRUE(eval.eval.PopBack().IsNull());
  }
  {
    eval.frame[node_symbol] = TypedValue::Null;
    auto *op = storage.Create<PropertyLookup>(identifier, dba->property("age"));
    op->Accept(eval.eval);
    EXPECT_TRUE(eval.eval.PopBack().IsNull());
  }
}

TEST(ExpressionEvaluator, LabelsTest) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->insert_vertex();
  v1.add_label(dba->label("ANIMAL"));
  v1.add_label(dba->label("DOG"));
  v1.add_label(dba->label("NICE_DOG"));
  auto *identifier = storage.Create<Identifier>("n");
  auto node_symbol = eval.symbol_table.CreateSymbol("n");
  eval.symbol_table[*identifier] = node_symbol;
  eval.frame[node_symbol] = v1;
  {
    auto *op = storage.Create<LabelsTest>(
        identifier, std::vector<GraphDbTypes::Label>{dba->label("DOG"),
                                                     dba->label("ANIMAL")});
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().Value<bool>(), true);
  }
  {
    auto *op = storage.Create<LabelsTest>(
        identifier,
        std::vector<GraphDbTypes::Label>{
            dba->label("DOG"), dba->label("BAD_DOG"), dba->label("ANIMAL")});
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().Value<bool>(), false);
  }
  {
    eval.frame[node_symbol] = TypedValue::Null;
    auto *op = storage.Create<LabelsTest>(
        identifier,
        std::vector<GraphDbTypes::Label>{
            dba->label("DOG"), dba->label("BAD_DOG"), dba->label("ANIMAL")});
    op->Accept(eval.eval);
    EXPECT_TRUE(eval.eval.PopBack().IsNull());
  }
}

TEST(ExpressionEvaluator, EdgeTypeTest) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  auto e = dba->insert_edge(v1, v2, dba->edge_type("TYPE1"));
  auto *identifier = storage.Create<Identifier>("e");
  auto edge_symbol = eval.symbol_table.CreateSymbol("e");
  eval.symbol_table[*identifier] = edge_symbol;
  eval.frame[edge_symbol] = e;
  {
    auto *op = storage.Create<EdgeTypeTest>(
        identifier, std::vector<GraphDbTypes::EdgeType>{
                        dba->edge_type("TYPE0"), dba->edge_type("TYPE1"),
                        dba->edge_type("TYPE2")});
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().Value<bool>(), true);
  }
  {
    auto *op = storage.Create<EdgeTypeTest>(
        identifier, std::vector<GraphDbTypes::EdgeType>{
                        dba->edge_type("TYPE0"), dba->edge_type("TYPE2")});
    op->Accept(eval.eval);
    EXPECT_EQ(eval.eval.PopBack().Value<bool>(), false);
  }
  {
    eval.frame[edge_symbol] = TypedValue::Null;
    auto *op = storage.Create<EdgeTypeTest>(
        identifier, std::vector<GraphDbTypes::EdgeType>{
                        dba->edge_type("TYPE0"), dba->edge_type("TYPE2")});
    op->Accept(eval.eval);
    EXPECT_TRUE(eval.eval.PopBack().IsNull());
  }
}

TEST(ExpressionEvaluator, Aggregation) {
  AstTreeStorage storage;
  auto aggr = storage.Create<Aggregation>(storage.Create<PrimitiveLiteral>(42),
                                          Aggregation::Op::COUNT);
  SymbolTable symbol_table;
  auto aggr_sym = symbol_table.CreateSymbol("aggr");
  symbol_table[*aggr] = aggr_sym;
  Frame frame{symbol_table.max_position()};
  frame[aggr_sym] = TypedValue(1);
  Dbms dbms;
  auto dba = dbms.active();
  ExpressionEvaluator eval{frame, symbol_table, *dba};
  aggr->Accept(eval);
  EXPECT_EQ(eval.PopBack().Value<int64_t>(), 1);
}

TEST(ExpressionEvaluator, ListLiteral) {
  AstTreeStorage storage;
  NoContextExpressionEvaluator eval;
  auto *list_literal = storage.Create<ListLiteral>(
      std::vector<Expression *>{storage.Create<PrimitiveLiteral>(1),
                                storage.Create<PrimitiveLiteral>("bla"),
                                storage.Create<PrimitiveLiteral>(true)});
  list_literal->Accept(eval.eval);
  TypedValue result = eval.eval.PopBack();
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
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->insert_vertex();
  v1.add_label(dba->label("label1"));
  auto v2 = dba->insert_vertex();
  v2.add_label(dba->label("label2"));
  auto e = dba->insert_edge(v1, v2, dba->edge_type("t"));
  ASSERT_TRUE(EvaluateFunction("ENDNODE", {e})
                  .Value<VertexAccessor>()
                  .has_label(dba->label("label2")));
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
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->insert_vertex();
  v1.PropsSet(dba->property("height"), 5);
  v1.PropsSet(dba->property("age"), 10);
  auto v2 = dba->insert_vertex();
  auto e = dba->insert_edge(v1, v2, dba->edge_type("type1"));
  e.PropsSet(dba->property("height"), 3);
  e.PropsSet(dba->property("age"), 15);

  auto prop_values_to_int = [](TypedValue t) {
    std::unordered_map<std::string, int> properties;
    for (auto property : t.Value<std::map<std::string, TypedValue>>()) {
      properties[property.first] = property.second.Value<int64_t>();
    }
    return properties;
  };
  ASSERT_THAT(prop_values_to_int(EvaluateFunction("PROPERTIES", {v1})),
              UnorderedElementsAre(Pair("height", 5), Pair("age", 10)));
  ASSERT_THAT(prop_values_to_int(EvaluateFunction("PROPERTIES", {e})),
              UnorderedElementsAre(Pair("height", 3), Pair("age", 15)));
  ASSERT_THROW(EvaluateFunction("PROPERTIES", {2}), QueryRuntimeException);
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
}

TEST(ExpressionEvaluator, FunctionStartNode) {
  ASSERT_THROW(EvaluateFunction("STARTNODE", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("STARTNODE", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->insert_vertex();
  v1.add_label(dba->label("label1"));
  auto v2 = dba->insert_vertex();
  v2.add_label(dba->label("label2"));
  auto e = dba->insert_edge(v1, v2, dba->edge_type("t"));
  ASSERT_TRUE(EvaluateFunction("STARTNODE", {e})
                  .Value<VertexAccessor>()
                  .has_label(dba->label("label1")));
  ASSERT_THROW(EvaluateFunction("STARTNODE", {2}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionToBoolean) {
  ASSERT_THROW(EvaluateFunction("TOBOOLEAN", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {" trUE \n\t"}).Value<bool>(), true);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {"\n\tFalsE "}).Value<bool>(), false);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {"\n\tFALSEA "}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {true}).Value<bool>(), true);
  ASSERT_EQ(EvaluateFunction("TOBOOLEAN", {false}).Value<bool>(), false);
  ASSERT_THROW(EvaluateFunction("TOBOOLEAN", {2}), QueryRuntimeException);
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
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {"\n\t3"}).Value<int64_t>(), 3);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {" -3.5 \n\t"}).Value<int64_t>(), -3);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {"\n\t3X "}).type(),
            TypedValue::Type::Null);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {-3.5}).Value<int64_t>(), -3);
  ASSERT_EQ(EvaluateFunction("TOINTEGER", {3.5}).Value<int64_t>(), 3);
  ASSERT_THROW(EvaluateFunction("TOINTEGER", {true}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionType) {
  ASSERT_THROW(EvaluateFunction("TYPE", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("TYPE", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->insert_vertex();
  v1.add_label(dba->label("label1"));
  auto v2 = dba->insert_vertex();
  v2.add_label(dba->label("label2"));
  auto e = dba->insert_edge(v1, v2, dba->edge_type("type1"));
  ASSERT_EQ(EvaluateFunction("TYPE", {e}).Value<std::string>(), "type1");
  ASSERT_THROW(EvaluateFunction("TYPE", {2}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionLabels) {
  ASSERT_THROW(EvaluateFunction("LABELS", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("LABELS", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  Dbms dbms;
  auto dba = dbms.active();
  auto v = dba->insert_vertex();
  v.add_label(dba->label("label1"));
  v.add_label(dba->label("label2"));
  std::vector<std::string> labels;
  auto _labels =
      EvaluateFunction("LABELS", {v}).Value<std::vector<TypedValue>>();
  for (auto label : _labels) {
    labels.push_back(label.Value<std::string>());
  }
  ASSERT_THAT(labels, UnorderedElementsAre("label1", "label2"));
  ASSERT_THROW(EvaluateFunction("LABELS", {2}), QueryRuntimeException);
}

TEST(ExpressionEvaluator, FunctionRange) {
  EXPECT_THROW(EvaluateFunction("RANGE", {}), QueryRuntimeException);
  EXPECT_TRUE(EvaluateFunction("RANGE", {1, 2, TypedValue::Null}).IsNull());
  EXPECT_THROW(EvaluateFunction("RANGE", {1, TypedValue::Null, 1.3}),
               QueryRuntimeException);
  auto to_int_list = [](const TypedValue &t) {
    std::vector<int64_t> list;
    for (auto x : t.Value<std::vector<TypedValue>>()) {
      list.push_back(x.Value<int64_t>());
    }
    return list;
  };
  EXPECT_THROW(EvaluateFunction("RANGE", {1, 2, 0}), QueryRuntimeException);
  EXPECT_THAT(to_int_list(EvaluateFunction("RANGE", {1, 3})),
              ElementsAre(1, 2, 3));
  EXPECT_THAT(to_int_list(EvaluateFunction("RANGE", {-1, 5, 2})),
              ElementsAre(-1, 1, 3, 5));
  EXPECT_THAT(to_int_list(EvaluateFunction("RANGE", {2, 10, 3})),
              ElementsAre(2, 5, 8));
  EXPECT_THAT(to_int_list(EvaluateFunction("RANGE", {2, 2, 2})),
              ElementsAre(2));
  EXPECT_THAT(to_int_list(EvaluateFunction("RANGE", {3, 0, 5})), ElementsAre());
  EXPECT_THAT(to_int_list(EvaluateFunction("RANGE", {5, 1, -2})),
              ElementsAre(5, 3, 1));
  EXPECT_THAT(to_int_list(EvaluateFunction("RANGE", {6, 1, -2})),
              ElementsAre(6, 4, 2));
  EXPECT_THAT(to_int_list(EvaluateFunction("RANGE", {2, 2, -3})),
              ElementsAre(2));
  EXPECT_THAT(to_int_list(EvaluateFunction("RANGE", {-2, 4, -1})),
              ElementsAre());
}

TEST(ExpressionEvaluator, FunctionKeys) {
  ASSERT_THROW(EvaluateFunction("KEYS", {}), QueryRuntimeException);
  ASSERT_EQ(EvaluateFunction("KEYS", {TypedValue::Null}).type(),
            TypedValue::Type::Null);
  Dbms dbms;
  auto dba = dbms.active();
  auto v1 = dba->insert_vertex();
  v1.PropsSet(dba->property("height"), 5);
  v1.PropsSet(dba->property("age"), 10);
  auto v2 = dba->insert_vertex();
  auto e = dba->insert_edge(v1, v2, dba->edge_type("type1"));
  e.PropsSet(dba->property("width"), 3);
  e.PropsSet(dba->property("age"), 15);

  auto prop_keys_to_string = [](TypedValue t) {
    std::vector<std::string> keys;
    for (auto property : t.Value<std::vector<TypedValue>>()) {
      keys.push_back(property.Value<std::string>());
    }
    return keys;
  };
  ASSERT_THAT(prop_keys_to_string(EvaluateFunction("KEYS", {v1})),
              UnorderedElementsAre("height", "age"));
  ASSERT_THAT(prop_keys_to_string(EvaluateFunction("KEYS", {e})),
              UnorderedElementsAre("width", "age"));
  ASSERT_THROW(EvaluateFunction("KEYS", {2}), QueryRuntimeException);
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
}
