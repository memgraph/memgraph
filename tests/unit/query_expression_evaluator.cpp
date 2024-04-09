// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>
#include <cmath>
#include <iterator>
#include <memory>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "disk_test_utils.hpp"
#include "query/context.hpp"
#include "query/db_accessor.hpp"
#include "query/frontend/ast/ast.hpp"
#include "query/frontend/opencypher/parser.hpp"
#include "query/interpret/awesome_memgraph_functions.hpp"
#include "query/interpret/eval.hpp"
#include "query/interpret/frame.hpp"
#include "query/path.hpp"
#include "query/typed_value.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/storage.hpp"
#include "utils/exceptions.hpp"
#include "utils/string.hpp"

#include "query_common.hpp"
#include "utils/temporal.hpp"

using namespace memgraph::query;
using memgraph::query::test_common::ToIntList;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

namespace {

template <typename StorageType>
class ExpressionEvaluatorTest : public ::testing::Test {
 protected:
  const std::string testSuite = "expression_evaluator";

  memgraph::storage::Config config;
  std::unique_ptr<memgraph::storage::Storage> db;
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba;
  memgraph::query::DbAccessor dba;

  AstStorage storage;
  memgraph::utils::MonotonicBufferResource mem{1024};
  EvaluationContext ctx{.memory = &mem, .timestamp = memgraph::query::QueryTimestamp()};
  SymbolTable symbol_table;

  Frame frame{128};
  ExpressionEvaluator eval{&frame, symbol_table, ctx, &dba, memgraph::storage::View::OLD};

  ExpressionEvaluatorTest()
      : config(disk_test_utils::GenerateOnDiskConfig(testSuite)),
        db(new StorageType(config)),
        storage_dba(db->Access(memgraph::replication_coordination_glue::ReplicationRole::MAIN)),
        dba(storage_dba.get()) {}

  ~ExpressionEvaluatorTest() override {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  Identifier *CreateIdentifierWithValue(std::string name, const TypedValue &value) {
    auto id = storage.template Create<Identifier>(name, true);
    auto symbol = symbol_table.CreateSymbol(name, true);
    id->MapTo(symbol);
    frame[symbol] = value;
    return id;
  }

  Exists *CreateExistsWithValue(std::string name, TypedValue &&value) {
    auto id = storage.template Create<Exists>();
    auto symbol = symbol_table.CreateSymbol(name, true);
    id->MapTo(symbol);
    frame[symbol] = std::move(value);
    return id;
  }

  template <class TExpression>
  auto Eval(TExpression *expr) {
    ctx.properties = NamesToProperties(storage.properties_, &dba);
    ctx.labels = NamesToLabels(storage.labels_, &dba);
    auto value = expr->Accept(eval);
    EXPECT_EQ(value.GetMemoryResource(), &mem) << "ExpressionEvaluator must use the MemoryResource from "
                                                  "EvaluationContext for allocations!";
    return value;
  }
};

// using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;
using StorageTypes = ::testing::Types<memgraph::storage::DiskStorage>;
TYPED_TEST_SUITE(ExpressionEvaluatorTest, StorageTypes);

TYPED_TEST(ExpressionEvaluatorTest, OrOperator) {
  auto *op = this->storage.template Create<OrOperator>(this->storage.template Create<PrimitiveLiteral>(true),
                                                       this->storage.template Create<PrimitiveLiteral>(false));
  auto val1 = this->Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = this->storage.template Create<OrOperator>(this->storage.template Create<PrimitiveLiteral>(true),
                                                 this->storage.template Create<PrimitiveLiteral>(true));
  auto val2 = this->Eval(op);
  ASSERT_EQ(val2.ValueBool(), true);
}

TYPED_TEST(ExpressionEvaluatorTest, XorOperator) {
  auto *op = this->storage.template Create<XorOperator>(this->storage.template Create<PrimitiveLiteral>(true),
                                                        this->storage.template Create<PrimitiveLiteral>(false));
  auto val1 = this->Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = this->storage.template Create<XorOperator>(this->storage.template Create<PrimitiveLiteral>(true),
                                                  this->storage.template Create<PrimitiveLiteral>(true));
  auto val2 = this->Eval(op);
  ASSERT_EQ(val2.ValueBool(), false);
}

TYPED_TEST(ExpressionEvaluatorTest, AndOperator) {
  auto *op = this->storage.template Create<AndOperator>(this->storage.template Create<PrimitiveLiteral>(true),
                                                        this->storage.template Create<PrimitiveLiteral>(true));
  auto val1 = this->Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = this->storage.template Create<AndOperator>(this->storage.template Create<PrimitiveLiteral>(false),
                                                  this->storage.template Create<PrimitiveLiteral>(true));
  auto val2 = this->Eval(op);
  ASSERT_EQ(val2.ValueBool(), false);
}

TYPED_TEST(ExpressionEvaluatorTest, AndOperatorShortCircuit) {
  {
    auto *op = this->storage.template Create<AndOperator>(this->storage.template Create<PrimitiveLiteral>(false),
                                                          this->storage.template Create<PrimitiveLiteral>(5));
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueBool(), false);
  }
  {
    auto *op = this->storage.template Create<AndOperator>(this->storage.template Create<PrimitiveLiteral>(5),
                                                          this->storage.template Create<PrimitiveLiteral>(false));
    // We are evaluating left to right, so we don't short circuit here and
    // raise due to `5`. This differs from neo4j, where they evaluate both
    // sides and return `false` without checking for type of the first
    // expression.
    EXPECT_THROW(this->Eval(op), QueryRuntimeException);
  }
}

TYPED_TEST(ExpressionEvaluatorTest, AndExistsOperatorShortCircuit) {
  {
    std::function<void(TypedValue *)> my_func = [](TypedValue * /*return_value*/) {
      throw QueryRuntimeException("This should not be evaluated");
    };
    TypedValue func_should_not_evaluate{std::move(my_func)};

    auto *op = this->storage.template Create<AndOperator>(
        this->storage.template Create<PrimitiveLiteral>(false),
        this->CreateExistsWithValue("anon1", std::move(func_should_not_evaluate)));
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueBool(), false);
  }
  {
    std::function<void(TypedValue *)> my_func = [memory = this->ctx.memory](TypedValue *return_value) {
      *return_value = TypedValue(false, memory);
    };
    TypedValue should_evaluate{std::move(my_func)};

    auto *op =
        this->storage.template Create<AndOperator>(this->storage.template Create<PrimitiveLiteral>(true),
                                                   this->CreateExistsWithValue("anon1", std::move(should_evaluate)));
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueBool(), false);
  }
}

TYPED_TEST(ExpressionEvaluatorTest, AndOperatorNull) {
  {
    // Null doesn't short circuit
    auto *op = this->storage.template Create<AndOperator>(
        this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()),
        this->storage.template Create<PrimitiveLiteral>(5));
    EXPECT_THROW(this->Eval(op), QueryRuntimeException);
  }
  {
    auto *op = this->storage.template Create<AndOperator>(
        this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()),
        this->storage.template Create<PrimitiveLiteral>(true));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    auto *op = this->storage.template Create<AndOperator>(
        this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()),
        this->storage.template Create<PrimitiveLiteral>(false));
    auto value = this->Eval(op);
    ASSERT_TRUE(value.IsBool());
    EXPECT_EQ(value.ValueBool(), false);
  }
}

TYPED_TEST(ExpressionEvaluatorTest, AdditionOperator) {
  auto *op = this->storage.template Create<AdditionOperator>(this->storage.template Create<PrimitiveLiteral>(2),
                                                             this->storage.template Create<PrimitiveLiteral>(3));
  auto value = this->Eval(op);
  ASSERT_EQ(value.ValueInt(), 5);
}

TYPED_TEST(ExpressionEvaluatorTest, SubtractionOperator) {
  auto *op = this->storage.template Create<SubtractionOperator>(this->storage.template Create<PrimitiveLiteral>(2),
                                                                this->storage.template Create<PrimitiveLiteral>(3));
  auto value = this->Eval(op);
  ASSERT_EQ(value.ValueInt(), -1);
}

TYPED_TEST(ExpressionEvaluatorTest, MultiplicationOperator) {
  auto *op = this->storage.template Create<MultiplicationOperator>(this->storage.template Create<PrimitiveLiteral>(2),
                                                                   this->storage.template Create<PrimitiveLiteral>(3));
  auto value = this->Eval(op);
  ASSERT_EQ(value.ValueInt(), 6);
}

TYPED_TEST(ExpressionEvaluatorTest, DivisionOperator) {
  auto *op = this->storage.template Create<DivisionOperator>(this->storage.template Create<PrimitiveLiteral>(50),
                                                             this->storage.template Create<PrimitiveLiteral>(10));
  auto value = this->Eval(op);
  ASSERT_EQ(value.ValueInt(), 5);
}

TYPED_TEST(ExpressionEvaluatorTest, ModOperator) {
  auto *op = this->storage.template Create<ModOperator>(this->storage.template Create<PrimitiveLiteral>(65),
                                                        this->storage.template Create<PrimitiveLiteral>(10));
  auto value = this->Eval(op);
  ASSERT_EQ(value.ValueInt(), 5);
}

TYPED_TEST(ExpressionEvaluatorTest, EqualOperator) {
  auto *op = this->storage.template Create<EqualOperator>(this->storage.template Create<PrimitiveLiteral>(10),
                                                          this->storage.template Create<PrimitiveLiteral>(15));
  auto val1 = this->Eval(op);
  ASSERT_EQ(val1.ValueBool(), false);
  op = this->storage.template Create<EqualOperator>(this->storage.template Create<PrimitiveLiteral>(15),
                                                    this->storage.template Create<PrimitiveLiteral>(15));
  auto val2 = this->Eval(op);
  ASSERT_EQ(val2.ValueBool(), true);
  op = this->storage.template Create<EqualOperator>(this->storage.template Create<PrimitiveLiteral>(20),
                                                    this->storage.template Create<PrimitiveLiteral>(15));
  auto val3 = this->Eval(op);
  ASSERT_EQ(val3.ValueBool(), false);
}

TYPED_TEST(ExpressionEvaluatorTest, NotEqualOperator) {
  auto *op = this->storage.template Create<NotEqualOperator>(this->storage.template Create<PrimitiveLiteral>(10),
                                                             this->storage.template Create<PrimitiveLiteral>(15));
  auto val1 = this->Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = this->storage.template Create<NotEqualOperator>(this->storage.template Create<PrimitiveLiteral>(15),
                                                       this->storage.template Create<PrimitiveLiteral>(15));
  auto val2 = this->Eval(op);
  ASSERT_EQ(val2.ValueBool(), false);
  op = this->storage.template Create<NotEqualOperator>(this->storage.template Create<PrimitiveLiteral>(20),
                                                       this->storage.template Create<PrimitiveLiteral>(15));
  auto val3 = this->Eval(op);
  ASSERT_EQ(val3.ValueBool(), true);
}

TYPED_TEST(ExpressionEvaluatorTest, LessOperator) {
  auto *op = this->storage.template Create<LessOperator>(this->storage.template Create<PrimitiveLiteral>(10),
                                                         this->storage.template Create<PrimitiveLiteral>(15));
  auto val1 = this->Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = this->storage.template Create<LessOperator>(this->storage.template Create<PrimitiveLiteral>(15),
                                                   this->storage.template Create<PrimitiveLiteral>(15));
  auto val2 = this->Eval(op);
  ASSERT_EQ(val2.ValueBool(), false);
  op = this->storage.template Create<LessOperator>(this->storage.template Create<PrimitiveLiteral>(20),
                                                   this->storage.template Create<PrimitiveLiteral>(15));
  auto val3 = this->Eval(op);
  ASSERT_EQ(val3.ValueBool(), false);
}

TYPED_TEST(ExpressionEvaluatorTest, GreaterOperator) {
  auto *op = this->storage.template Create<GreaterOperator>(this->storage.template Create<PrimitiveLiteral>(10),
                                                            this->storage.template Create<PrimitiveLiteral>(15));
  auto val1 = this->Eval(op);
  ASSERT_EQ(val1.ValueBool(), false);
  op = this->storage.template Create<GreaterOperator>(this->storage.template Create<PrimitiveLiteral>(15),
                                                      this->storage.template Create<PrimitiveLiteral>(15));
  auto val2 = this->Eval(op);
  ASSERT_EQ(val2.ValueBool(), false);
  op = this->storage.template Create<GreaterOperator>(this->storage.template Create<PrimitiveLiteral>(20),
                                                      this->storage.template Create<PrimitiveLiteral>(15));
  auto val3 = this->Eval(op);
  ASSERT_EQ(val3.ValueBool(), true);
}

TYPED_TEST(ExpressionEvaluatorTest, LessEqualOperator) {
  auto *op = this->storage.template Create<LessEqualOperator>(this->storage.template Create<PrimitiveLiteral>(10),
                                                              this->storage.template Create<PrimitiveLiteral>(15));
  auto val1 = this->Eval(op);
  ASSERT_EQ(val1.ValueBool(), true);
  op = this->storage.template Create<LessEqualOperator>(this->storage.template Create<PrimitiveLiteral>(15),
                                                        this->storage.template Create<PrimitiveLiteral>(15));
  auto val2 = this->Eval(op);
  ASSERT_EQ(val2.ValueBool(), true);
  op = this->storage.template Create<LessEqualOperator>(this->storage.template Create<PrimitiveLiteral>(20),
                                                        this->storage.template Create<PrimitiveLiteral>(15));
  auto val3 = this->Eval(op);
  ASSERT_EQ(val3.ValueBool(), false);
}

TYPED_TEST(ExpressionEvaluatorTest, GreaterEqualOperator) {
  auto *op = this->storage.template Create<GreaterEqualOperator>(this->storage.template Create<PrimitiveLiteral>(10),
                                                                 this->storage.template Create<PrimitiveLiteral>(15));
  auto val1 = this->Eval(op);
  ASSERT_EQ(val1.ValueBool(), false);
  op = this->storage.template Create<GreaterEqualOperator>(this->storage.template Create<PrimitiveLiteral>(15),
                                                           this->storage.template Create<PrimitiveLiteral>(15));
  auto val2 = this->Eval(op);
  ASSERT_EQ(val2.ValueBool(), true);
  op = this->storage.template Create<GreaterEqualOperator>(this->storage.template Create<PrimitiveLiteral>(20),
                                                           this->storage.template Create<PrimitiveLiteral>(15));
  auto val3 = this->Eval(op);
  ASSERT_EQ(val3.ValueBool(), true);
}

TYPED_TEST(ExpressionEvaluatorTest, InListOperator) {
  auto *list_literal = this->storage.template Create<ListLiteral>(std::vector<Expression *>{
      this->storage.template Create<PrimitiveLiteral>(1), this->storage.template Create<PrimitiveLiteral>(2),
      this->storage.template Create<PrimitiveLiteral>("a")});
  {
    // Element exists in list.
    auto *op =
        this->storage.template Create<InListOperator>(this->storage.template Create<PrimitiveLiteral>(2), list_literal);
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueBool(), true);
  }
  {
    // Element doesn't exist in list.
    auto *op = this->storage.template Create<InListOperator>(this->storage.template Create<PrimitiveLiteral>("x"),
                                                             list_literal);
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueBool(), false);
  }
  {
    auto *list_literal = this->storage.template Create<ListLiteral>(std::vector<Expression *>{
        this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()),
        this->storage.template Create<PrimitiveLiteral>(2), this->storage.template Create<PrimitiveLiteral>("a")});
    // Element doesn't exist in list with null element.
    auto *op = this->storage.template Create<InListOperator>(this->storage.template Create<PrimitiveLiteral>("x"),
                                                             list_literal);
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Null list.
    auto *op = this->storage.template Create<InListOperator>(
        this->storage.template Create<PrimitiveLiteral>("x"),
        this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Null literal.
    auto *op = this->storage.template Create<InListOperator>(
        this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()), list_literal);
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Null literal, empty list.
    auto *op = this->storage.template Create<InListOperator>(
        this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()),
        this->storage.template Create<ListLiteral>(std::vector<Expression *>()));
    auto value = this->Eval(op);
    EXPECT_FALSE(value.ValueBool());
  }
}

TYPED_TEST(ExpressionEvaluatorTest, ListIndexing) {
  auto *list_literal = this->storage.template Create<ListLiteral>(std::vector<Expression *>{
      this->storage.template Create<PrimitiveLiteral>(1), this->storage.template Create<PrimitiveLiteral>(2),
      this->storage.template Create<PrimitiveLiteral>(3), this->storage.template Create<PrimitiveLiteral>(4)});
  {
    // Legal indexing.
    auto *op = this->storage.template Create<SubscriptOperator>(list_literal,
                                                                this->storage.template Create<PrimitiveLiteral>(2));
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueInt(), 3);
  }
  {
    // Out of bounds indexing.
    auto *op = this->storage.template Create<SubscriptOperator>(list_literal,
                                                                this->storage.template Create<PrimitiveLiteral>(4));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Out of bounds indexing with negative bound.
    auto *op = this->storage.template Create<SubscriptOperator>(list_literal,
                                                                this->storage.template Create<PrimitiveLiteral>(-100));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Legal indexing with negative index.
    auto *op = this->storage.template Create<SubscriptOperator>(list_literal,
                                                                this->storage.template Create<PrimitiveLiteral>(-2));
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueInt(), 3);
  }
  {
    // Indexing with one operator being null.
    auto *op = this->storage.template Create<SubscriptOperator>(
        this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()),
        this->storage.template Create<PrimitiveLiteral>(-2));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Indexing with incompatible type.
    auto *op = this->storage.template Create<SubscriptOperator>(list_literal,
                                                                this->storage.template Create<PrimitiveLiteral>("bla"));
    EXPECT_THROW(this->Eval(op), QueryRuntimeException);
  }
}

TYPED_TEST(ExpressionEvaluatorTest, MapIndexing) {
  auto *map_literal = this->storage.template Create<MapLiteral>(std::unordered_map<PropertyIx, Expression *>{
      {this->storage.GetPropertyIx("a"), this->storage.template Create<PrimitiveLiteral>(1)},
      {this->storage.GetPropertyIx("b"), this->storage.template Create<PrimitiveLiteral>(2)},
      {this->storage.GetPropertyIx("c"), this->storage.template Create<PrimitiveLiteral>(3)}});
  {
    // Legal indexing.
    auto *op = this->storage.template Create<SubscriptOperator>(map_literal,
                                                                this->storage.template Create<PrimitiveLiteral>("b"));
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueInt(), 2);
  }
  {
    // Legal indexing, non-existing key.
    auto *op = this->storage.template Create<SubscriptOperator>(map_literal,
                                                                this->storage.template Create<PrimitiveLiteral>("z"));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Wrong key type.
    auto *op = this->storage.template Create<SubscriptOperator>(map_literal,
                                                                this->storage.template Create<PrimitiveLiteral>(42));
    EXPECT_THROW(this->Eval(op), QueryRuntimeException);
  }
  {
    // Indexing with Null.
    auto *op = this->storage.template Create<SubscriptOperator>(
        map_literal, this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
}

TYPED_TEST(ExpressionEvaluatorTest, MapProjectionIndexing) {
  auto *map_variable = this->storage.template Create<MapLiteral>(std::unordered_map<PropertyIx, Expression *>{
      {this->storage.GetPropertyIx("x"), this->storage.template Create<PrimitiveLiteral>(0)}});
  auto *map_projection_literal = this->storage.template Create<MapProjectionLiteral>(
      map_variable, std::unordered_map<PropertyIx, Expression *>{
                        {this->storage.GetPropertyIx("a"), this->storage.template Create<PrimitiveLiteral>(1)},
                        {this->storage.GetPropertyIx("y"), this->storage.template Create<PropertyLookup>(
                                                               map_variable, this->storage.GetPropertyIx("y"))}});

  {
    // Legal indexing.
    auto *op = this->storage.template Create<SubscriptOperator>(map_projection_literal,
                                                                this->storage.template Create<PrimitiveLiteral>("a"));
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueInt(), 1);
  }
  {
    // Legal indexing; property created by PropertyLookup of a non-existent map variable key
    auto *op = this->storage.template Create<SubscriptOperator>(map_projection_literal,
                                                                this->storage.template Create<PrimitiveLiteral>("y"));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Legal indexing, non-existing property.
    auto *op = this->storage.template Create<SubscriptOperator>(map_projection_literal,
                                                                this->storage.template Create<PrimitiveLiteral>("z"));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Wrong key type.
    auto *op = this->storage.template Create<SubscriptOperator>(map_projection_literal,
                                                                this->storage.template Create<PrimitiveLiteral>(42));
    EXPECT_THROW(this->Eval(op), QueryRuntimeException);
  }
  {
    // Indexing with Null.
    auto *op = this->storage.template Create<SubscriptOperator>(
        map_projection_literal, this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
}

TYPED_TEST(ExpressionEvaluatorTest, MapProjectionAllPropertiesLookupBefore) {
  // AllPropertiesLookup (.*) may contain properties whose names also occur in MapProjectionLiteral
  // The ones in MapProjectionLiteral are explicitly given and thus take precedence over those in AllPropertiesLookup
  // Test case: AllPropertiesLookup comes before the identically-named properties

  auto *map_variable = this->storage.template Create<MapLiteral>(std::unordered_map<PropertyIx, Expression *>{
      {this->storage.GetPropertyIx("x"), this->storage.template Create<PrimitiveLiteral>(0)}});
  auto *map_projection_literal = this->storage.template Create<MapProjectionLiteral>(
      map_variable,
      std::unordered_map<PropertyIx, Expression *>{
          {this->storage.GetPropertyIx("*"), this->storage.template Create<AllPropertiesLookup>(map_variable)},
          {this->storage.GetPropertyIx("x"), this->storage.template Create<PrimitiveLiteral>(1)}});

  auto *op = this->storage.template Create<SubscriptOperator>(map_projection_literal,
                                                              this->storage.template Create<PrimitiveLiteral>("x"));
  auto value = this->Eval(op);
  EXPECT_EQ(value.ValueInt(), 1);
}

TYPED_TEST(ExpressionEvaluatorTest, MapProjectionAllPropertiesLookupAfter) {
  // AllPropertiesLookup (.*) may contain properties whose names also occur in MapProjectionLiteral
  // The ones in MapProjectionLiteral are explicitly given and thus take precedence over those in AllPropertiesLookup
  // Test case: AllPropertiesLookup comes after the identically-named properties

  auto *map_variable = this->storage.template Create<MapLiteral>(std::unordered_map<PropertyIx, Expression *>{
      {this->storage.GetPropertyIx("x"), this->storage.template Create<PrimitiveLiteral>(0)}});
  auto *map_projection_literal = this->storage.template Create<MapProjectionLiteral>(
      map_variable,
      std::unordered_map<PropertyIx, Expression *>{
          {this->storage.GetPropertyIx("x"), this->storage.template Create<PrimitiveLiteral>(1)},
          {this->storage.GetPropertyIx("*"), this->storage.template Create<AllPropertiesLookup>(map_variable)}});

  auto *op = this->storage.template Create<SubscriptOperator>(map_projection_literal,
                                                              this->storage.template Create<PrimitiveLiteral>("x"));
  auto value = this->Eval(op);
  EXPECT_EQ(value.ValueInt(), 1);
}

TYPED_TEST(ExpressionEvaluatorTest, VertexAndEdgeIndexing) {
  auto edge_type = this->dba.NameToEdgeType("edge_type");
  auto prop = this->dba.NameToProperty("prop");
  auto v1 = this->dba.InsertVertex();
  auto e11 = this->dba.InsertEdge(&v1, &v1, edge_type);
  ASSERT_TRUE(e11.HasValue());
  ASSERT_TRUE(v1.SetProperty(prop, memgraph::storage::PropertyValue(42)).HasValue());
  ASSERT_TRUE(e11->SetProperty(prop, memgraph::storage::PropertyValue(43)).HasValue());
  this->dba.AdvanceCommand();

  auto *vertex_id = this->CreateIdentifierWithValue("v1", TypedValue(v1));
  auto *edge_id = this->CreateIdentifierWithValue("e11", TypedValue(*e11));
  {
    // Legal indexing.
    auto *op1 = this->storage.template Create<SubscriptOperator>(
        vertex_id, this->storage.template Create<PrimitiveLiteral>("prop"));
    auto value1 = this->Eval(op1);
    EXPECT_EQ(value1.ValueInt(), 42);

    auto *op2 = this->storage.template Create<SubscriptOperator>(
        edge_id, this->storage.template Create<PrimitiveLiteral>("prop"));
    auto value2 = this->Eval(op2);
    EXPECT_EQ(value2.ValueInt(), 43);
  }
  {
    // Legal indexing, non-existing key.
    auto *op1 = this->storage.template Create<SubscriptOperator>(
        vertex_id, this->storage.template Create<PrimitiveLiteral>("blah"));
    auto value1 = this->Eval(op1);
    EXPECT_TRUE(value1.IsNull());

    auto *op2 = this->storage.template Create<SubscriptOperator>(
        edge_id, this->storage.template Create<PrimitiveLiteral>("blah"));
    auto value2 = this->Eval(op2);
    EXPECT_TRUE(value2.IsNull());
  }
  {
    // Wrong key type.
    auto *op1 =
        this->storage.template Create<SubscriptOperator>(vertex_id, this->storage.template Create<PrimitiveLiteral>(1));
    EXPECT_THROW(this->Eval(op1), QueryRuntimeException);

    auto *op2 =
        this->storage.template Create<SubscriptOperator>(edge_id, this->storage.template Create<PrimitiveLiteral>(1));
    EXPECT_THROW(this->Eval(op2), QueryRuntimeException);
  }
  {
    // Indexing with Null.
    auto *op1 = this->storage.template Create<SubscriptOperator>(
        vertex_id, this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()));
    auto value1 = this->Eval(op1);
    EXPECT_TRUE(value1.IsNull());

    auto *op2 = this->storage.template Create<SubscriptOperator>(
        edge_id, this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()));
    auto value2 = this->Eval(op2);
    EXPECT_TRUE(value2.IsNull());
  }
}

TYPED_TEST(ExpressionEvaluatorTest, TypedValueListIndexing) {
  auto list_vector = memgraph::utils::pmr::vector<TypedValue>(this->ctx.memory);
  list_vector.emplace_back("string1");
  list_vector.emplace_back("string2");

  auto *identifier = this->storage.template Create<Identifier>("n");
  auto node_symbol = this->symbol_table.CreateSymbol("n", true);
  identifier->MapTo(node_symbol);
  this->frame[node_symbol] = TypedValue(list_vector, this->ctx.memory);

  {
    // Legal indexing.
    auto *op = this->storage.template Create<SubscriptOperator>(identifier,
                                                                this->storage.template Create<PrimitiveLiteral>(0));
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueString(), "string1");
  }
  {
    // Out of bounds indexing
    auto *op = this->storage.template Create<SubscriptOperator>(identifier,
                                                                this->storage.template Create<PrimitiveLiteral>(3));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Out of bounds indexing with negative bound.
    auto *op = this->storage.template Create<SubscriptOperator>(identifier,
                                                                this->storage.template Create<PrimitiveLiteral>(-100));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
  {
    // Legal indexing with negative index.
    auto *op = this->storage.template Create<SubscriptOperator>(identifier,
                                                                this->storage.template Create<PrimitiveLiteral>(-2));
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueString(), "string1");
  }
  {
    // Indexing with incompatible type.
    auto *op = this->storage.template Create<SubscriptOperator>(identifier,
                                                                this->storage.template Create<PrimitiveLiteral>("bla"));
    EXPECT_THROW(this->Eval(op), QueryRuntimeException);
  }
}

TYPED_TEST(ExpressionEvaluatorTest, ListSlicingOperator) {
  auto *list_literal = this->storage.template Create<ListLiteral>(std::vector<Expression *>{
      this->storage.template Create<PrimitiveLiteral>(1), this->storage.template Create<PrimitiveLiteral>(2),
      this->storage.template Create<PrimitiveLiteral>(3), this->storage.template Create<PrimitiveLiteral>(4)});

  auto extract_ints = [](TypedValue list) {
    std::vector<int64_t> int_list;
    for (auto x : list.ValueList()) {
      int_list.push_back(x.ValueInt());
    }
    return int_list;
  };
  {
    // Legal slicing with both bounds defined.
    auto *op = this->storage.template Create<ListSlicingOperator>(list_literal,
                                                                  this->storage.template Create<PrimitiveLiteral>(2),
                                                                  this->storage.template Create<PrimitiveLiteral>(4));
    auto value = this->Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre(3, 4));
  }
  {
    // Legal slicing with negative bound.
    auto *op = this->storage.template Create<ListSlicingOperator>(list_literal,
                                                                  this->storage.template Create<PrimitiveLiteral>(2),
                                                                  this->storage.template Create<PrimitiveLiteral>(-1));
    auto value = this->Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre(3));
  }
  {
    // Lower bound larger than upper bound.
    auto *op = this->storage.template Create<ListSlicingOperator>(list_literal,
                                                                  this->storage.template Create<PrimitiveLiteral>(2),
                                                                  this->storage.template Create<PrimitiveLiteral>(-4));
    auto value = this->Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre());
  }
  {
    // Bounds ouf or range.
    auto *op = this->storage.template Create<ListSlicingOperator>(list_literal,
                                                                  this->storage.template Create<PrimitiveLiteral>(-100),
                                                                  this->storage.template Create<PrimitiveLiteral>(10));
    auto value = this->Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre(1, 2, 3, 4));
  }
  {
    // Lower bound undefined.
    auto *op = this->storage.template Create<ListSlicingOperator>(list_literal, nullptr,
                                                                  this->storage.template Create<PrimitiveLiteral>(3));
    auto value = this->Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre(1, 2, 3));
  }
  {
    // Upper bound undefined.
    auto *op = this->storage.template Create<ListSlicingOperator>(
        list_literal, this->storage.template Create<PrimitiveLiteral>(-2), nullptr);
    auto value = this->Eval(op);
    EXPECT_THAT(extract_ints(value), ElementsAre(3, 4));
  }
  {
    // Bound of illegal type and null value bound.
    auto *op = this->storage.template Create<ListSlicingOperator>(
        list_literal, this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()),
        this->storage.template Create<PrimitiveLiteral>("mirko"));
    EXPECT_THROW(this->Eval(op), QueryRuntimeException);
  }
  {
    // List of illegal type.
    auto *op = this->storage.template Create<ListSlicingOperator>(this->storage.template Create<PrimitiveLiteral>("a"),
                                                                  this->storage.template Create<PrimitiveLiteral>(-2),
                                                                  nullptr);
    EXPECT_THROW(this->Eval(op), QueryRuntimeException);
  }
  {
    // Null value list with undefined upper bound.
    auto *op = this->storage.template Create<ListSlicingOperator>(
        this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()),
        this->storage.template Create<PrimitiveLiteral>(-2), nullptr);
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
    ;
  }
  {
    // Null value index.
    auto *op = this->storage.template Create<ListSlicingOperator>(
        list_literal, this->storage.template Create<PrimitiveLiteral>(-2),
        this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()));
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
    ;
  }
}

TYPED_TEST(ExpressionEvaluatorTest, IfOperator) {
  auto *then_expression = this->storage.template Create<PrimitiveLiteral>(10);
  auto *else_expression = this->storage.template Create<PrimitiveLiteral>(20);
  {
    auto *condition_true = this->storage.template Create<EqualOperator>(
        this->storage.template Create<PrimitiveLiteral>(2), this->storage.template Create<PrimitiveLiteral>(2));
    auto *op = this->storage.template Create<IfOperator>(condition_true, then_expression, else_expression);
    auto value = this->Eval(op);
    ASSERT_EQ(value.ValueInt(), 10);
  }
  {
    auto *condition_false = this->storage.template Create<EqualOperator>(
        this->storage.template Create<PrimitiveLiteral>(2), this->storage.template Create<PrimitiveLiteral>(3));
    auto *op = this->storage.template Create<IfOperator>(condition_false, then_expression, else_expression);
    auto value = this->Eval(op);
    ASSERT_EQ(value.ValueInt(), 20);
  }
  {
    auto *condition_exception = this->storage.template Create<AdditionOperator>(
        this->storage.template Create<PrimitiveLiteral>(2), this->storage.template Create<PrimitiveLiteral>(3));
    auto *op = this->storage.template Create<IfOperator>(condition_exception, then_expression, else_expression);
    ASSERT_THROW(this->Eval(op), QueryRuntimeException);
  }
}

TYPED_TEST(ExpressionEvaluatorTest, NotOperator) {
  auto *op = this->storage.template Create<NotOperator>(this->storage.template Create<PrimitiveLiteral>(false));
  auto value = this->Eval(op);
  ASSERT_EQ(value.ValueBool(), true);
}

TYPED_TEST(ExpressionEvaluatorTest, UnaryPlusOperator) {
  auto *op = this->storage.template Create<UnaryPlusOperator>(this->storage.template Create<PrimitiveLiteral>(5));
  auto value = this->Eval(op);
  ASSERT_EQ(value.ValueInt(), 5);
}

TYPED_TEST(ExpressionEvaluatorTest, UnaryMinusOperator) {
  auto *op = this->storage.template Create<UnaryMinusOperator>(this->storage.template Create<PrimitiveLiteral>(5));
  auto value = this->Eval(op);
  ASSERT_EQ(value.ValueInt(), -5);
}

TYPED_TEST(ExpressionEvaluatorTest, IsNullOperator) {
  auto *op = this->storage.template Create<IsNullOperator>(this->storage.template Create<PrimitiveLiteral>(1));
  auto val1 = this->Eval(op);
  ASSERT_EQ(val1.ValueBool(), false);
  op = this->storage.template Create<IsNullOperator>(
      this->storage.template Create<PrimitiveLiteral>(memgraph::storage::PropertyValue()));
  auto val2 = this->Eval(op);
  ASSERT_EQ(val2.ValueBool(), true);
}

TYPED_TEST(ExpressionEvaluatorTest, LabelsTest) {
  auto v1 = this->dba.InsertVertex();
  ASSERT_TRUE(v1.AddLabel(this->dba.NameToLabel("ANIMAL")).HasValue());
  ASSERT_TRUE(v1.AddLabel(this->dba.NameToLabel("DOG")).HasValue());
  ASSERT_TRUE(v1.AddLabel(this->dba.NameToLabel("NICE_DOG")).HasValue());
  this->dba.AdvanceCommand();
  auto *identifier = this->storage.template Create<Identifier>("n");
  auto node_symbol = this->symbol_table.CreateSymbol("n", true);
  identifier->MapTo(node_symbol);
  this->frame[node_symbol] = TypedValue(v1);
  {
    auto *op = this->storage.template Create<LabelsTest>(
        identifier, std::vector<LabelIx>{this->storage.GetLabelIx("DOG"), this->storage.GetLabelIx("ANIMAL")});
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueBool(), true);
  }
  {
    auto *op = this->storage.template Create<LabelsTest>(
        identifier, std::vector<LabelIx>{this->storage.GetLabelIx("DOG"), this->storage.GetLabelIx("BAD_DOG"),
                                         this->storage.GetLabelIx("ANIMAL")});
    auto value = this->Eval(op);
    EXPECT_EQ(value.ValueBool(), false);
  }
  {
    this->frame[node_symbol] = TypedValue();
    auto *op = this->storage.template Create<LabelsTest>(
        identifier, std::vector<LabelIx>{this->storage.GetLabelIx("DOG"), this->storage.GetLabelIx("BAD_DOG"),
                                         this->storage.GetLabelIx("ANIMAL")});
    auto value = this->Eval(op);
    EXPECT_TRUE(value.IsNull());
  }
}

TYPED_TEST(ExpressionEvaluatorTest, Aggregation) {
  auto aggr = this->storage.template Create<Aggregation>(this->storage.template Create<PrimitiveLiteral>(42), nullptr,
                                                         Aggregation::Op::COUNT, false);
  auto aggr_sym = this->symbol_table.CreateSymbol("aggr", true);
  aggr->MapTo(aggr_sym);
  this->frame[aggr_sym] = TypedValue(1);
  auto value = this->Eval(aggr);
  EXPECT_EQ(value.ValueInt(), 1);
}

TYPED_TEST(ExpressionEvaluatorTest, ListLiteral) {
  auto *list_literal = this->storage.template Create<ListLiteral>(std::vector<Expression *>{
      this->storage.template Create<PrimitiveLiteral>(1), this->storage.template Create<PrimitiveLiteral>("bla"),
      this->storage.template Create<PrimitiveLiteral>(true)});
  TypedValue result = this->Eval(list_literal);
  ASSERT_TRUE(result.IsList());
  auto &result_elems = result.ValueList();
  ASSERT_EQ(3, result_elems.size());
  EXPECT_TRUE(result_elems[0].IsInt());
  ;
  EXPECT_TRUE(result_elems[1].IsString());
  ;
  EXPECT_TRUE(result_elems[2].IsBool());
  ;
}

TYPED_TEST(ExpressionEvaluatorTest, ParameterLookup) {
  this->ctx.parameters.Add(0, memgraph::storage::PropertyValue(42));
  auto *param_lookup = this->storage.template Create<ParameterLookup>(0);
  auto value = this->Eval(param_lookup);
  ASSERT_TRUE(value.IsInt());
  EXPECT_EQ(value.ValueInt(), 42);
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAll1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *all = ALL("x", LIST(LITERAL(1), LITERAL(1)), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(all);
  ASSERT_TRUE(value.IsBool());
  EXPECT_TRUE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAll2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *all = ALL("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(all);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAllNullList) {
  AstStorage storage;
  auto *all = ALL("x", LITERAL(memgraph::storage::PropertyValue()), WHERE(LITERAL(true)));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  auto value = this->Eval(all);
  EXPECT_TRUE(value.IsNull());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAllNullElementInList1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *all = ALL("x", LIST(LITERAL(1), LITERAL(memgraph::storage::PropertyValue())), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(all);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAllNullElementInList2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *all = ALL("x", LIST(LITERAL(2), LITERAL(memgraph::storage::PropertyValue())), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(all);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAllWhereWrongType) {
  AstStorage storage;
  auto *all = ALL("x", LIST(LITERAL(1)), WHERE(LITERAL(2)));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  all->identifier_->MapTo(x_sym);
  EXPECT_THROW(this->Eval(all), QueryRuntimeException);
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionSingle1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *single = SINGLE("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  single->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(single);
  ASSERT_TRUE(value.IsBool());
  EXPECT_TRUE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionSingle2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *single = SINGLE("x", LIST(LITERAL(1), LITERAL(2)), WHERE(GREATER(ident_x, LITERAL(0))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  single->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(single);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionSingleNullList) {
  AstStorage storage;
  auto *single = SINGLE("x", LITERAL(memgraph::storage::PropertyValue()), WHERE(LITERAL(true)));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  single->identifier_->MapTo(x_sym);
  auto value = this->Eval(single);
  EXPECT_TRUE(value.IsNull());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionSingleNullElementInList1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *single =
      SINGLE("x", LIST(LITERAL(1), LITERAL(memgraph::storage::PropertyValue())), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  single->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(single);
  ASSERT_TRUE(value.IsBool());
  EXPECT_TRUE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionSingleNullElementInList2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *single =
      SINGLE("x", LIST(LITERAL(2), LITERAL(memgraph::storage::PropertyValue())), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  single->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(single);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAny1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *any = ANY("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(any);
  ASSERT_TRUE(value.IsBool());
  EXPECT_TRUE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAny2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *any = ANY("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(any);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAnyNullList) {
  AstStorage storage;
  auto *any = ANY("x", LITERAL(memgraph::storage::PropertyValue()), WHERE(LITERAL(true)));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  auto value = this->Eval(any);
  EXPECT_TRUE(value.IsNull());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAnyNullElementInList1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *any = ANY("x", LIST(LITERAL(0), LITERAL(memgraph::storage::PropertyValue())), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(any);
  EXPECT_TRUE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAnyNullElementInList2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *any = ANY("x", LIST(LITERAL(1), LITERAL(memgraph::storage::PropertyValue())), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(any);
  EXPECT_FALSE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionAnyWhereWrongType) {
  AstStorage storage;
  auto *any = ANY("x", LIST(LITERAL(1)), WHERE(LITERAL(2)));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  EXPECT_THROW(this->Eval(any), QueryRuntimeException);
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionNone1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *none = NONE("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  none->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(none);
  ASSERT_TRUE(value.IsBool());
  EXPECT_TRUE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionNone2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *none = NONE("x", LIST(LITERAL(1), LITERAL(2)), WHERE(EQ(ident_x, LITERAL(1))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  none->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(none);
  ASSERT_TRUE(value.IsBool());
  EXPECT_FALSE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionNoneNullList) {
  AstStorage storage;
  auto *none = NONE("x", LITERAL(memgraph::storage::PropertyValue()), WHERE(LITERAL(true)));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  none->identifier_->MapTo(x_sym);
  auto value = this->Eval(none);
  EXPECT_TRUE(value.IsNull());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionNoneNullElementInList1) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *any = NONE("x", LIST(LITERAL(1), LITERAL(memgraph::storage::PropertyValue())), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  any->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(any);
  EXPECT_TRUE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionNoneNullElementInList2) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *none = NONE("x", LIST(LITERAL(0), LITERAL(memgraph::storage::PropertyValue())), WHERE(EQ(ident_x, LITERAL(0))));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  none->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(none);
  EXPECT_FALSE(value.ValueBool());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionNoneWhereWrongType) {
  AstStorage storage;
  auto *none = NONE("x", LIST(LITERAL(1)), WHERE(LITERAL(2)));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  none->identifier_->MapTo(x_sym);
  EXPECT_THROW(this->Eval(none), QueryRuntimeException);
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionReduce) {
  AstStorage storage;
  auto *ident_sum = IDENT("sum");
  auto *ident_x = IDENT("x");
  auto *reduce = REDUCE("sum", LITERAL(0), "x", LIST(LITERAL(1), LITERAL(2)), ADD(ident_sum, ident_x));
  const auto sum_sym = this->symbol_table.CreateSymbol("sum", true);
  reduce->accumulator_->MapTo(sum_sym);
  ident_sum->MapTo(sum_sym);
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  reduce->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(reduce);
  ASSERT_TRUE(value.IsInt());
  EXPECT_EQ(value.ValueInt(), 3);
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionExtract) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *extract =
      EXTRACT("x", LIST(LITERAL(1), LITERAL(2), LITERAL(memgraph::storage::PropertyValue())), ADD(ident_x, LITERAL(1)));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  extract->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(extract);
  EXPECT_TRUE(value.IsList());
  ;
  auto result = value.ValueList();
  EXPECT_EQ(result[0].ValueInt(), 2);
  EXPECT_EQ(result[1].ValueInt(), 3);
  EXPECT_TRUE(result[2].IsNull());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionExtractNull) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *extract = EXTRACT("x", LITERAL(memgraph::storage::PropertyValue()), ADD(ident_x, LITERAL(1)));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  extract->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  auto value = this->Eval(extract);
  EXPECT_TRUE(value.IsNull());
}

TYPED_TEST(ExpressionEvaluatorTest, FunctionExtractExceptions) {
  AstStorage storage;
  auto *ident_x = IDENT("x");
  auto *extract = EXTRACT("x", LITERAL("bla"), ADD(ident_x, LITERAL(1)));
  const auto x_sym = this->symbol_table.CreateSymbol("x", true);
  extract->identifier_->MapTo(x_sym);
  ident_x->MapTo(x_sym);
  EXPECT_THROW(this->Eval(extract), QueryRuntimeException);
}

TYPED_TEST(ExpressionEvaluatorTest, Coalesce) {
  // coalesce()
  EXPECT_THROW(this->Eval(COALESCE()), QueryRuntimeException);

  // coalesce(null, null)
  EXPECT_TRUE(this->Eval(COALESCE(LITERAL(TypedValue()), LITERAL(TypedValue()))).IsNull());

  // coalesce(null, 2, 3)
  EXPECT_EQ(this->Eval(COALESCE(LITERAL(TypedValue()), LITERAL(2), LITERAL(3))).ValueInt(), 2);

  // coalesce(null, 2, assert(false), 3)
  EXPECT_EQ(
      this->Eval(COALESCE(LITERAL(TypedValue()), LITERAL(2), FN("ASSERT", LITERAL(false)), LITERAL(3))).ValueInt(), 2);

  // (null, assert(false))
  EXPECT_THROW(this->Eval(COALESCE(LITERAL(TypedValue()), FN("ASSERT", LITERAL(false)))), QueryRuntimeException);

  // coalesce([null, null])
  EXPECT_FALSE(this->Eval(COALESCE(LITERAL(TypedValue(std::vector<TypedValue>{TypedValue(), TypedValue()})))).IsNull());
}

TYPED_TEST(ExpressionEvaluatorTest, RegexMatchInvalidArguments) {
  EXPECT_TRUE(this->Eval(this->storage.template Create<RegexMatch>(LITERAL(TypedValue()), LITERAL("regex"))).IsNull());
  EXPECT_TRUE(this->Eval(this->storage.template Create<RegexMatch>(LITERAL(3), LITERAL("regex"))).IsNull());
  EXPECT_TRUE(
      this->Eval(this->storage.template Create<RegexMatch>(LIST(LITERAL("string")), LITERAL("regex"))).IsNull());
  EXPECT_TRUE(this->Eval(this->storage.template Create<RegexMatch>(LITERAL("string"), LITERAL(TypedValue()))).IsNull());
  EXPECT_THROW(this->Eval(this->storage.template Create<RegexMatch>(LITERAL("string"), LITERAL(42))),
               QueryRuntimeException);
  EXPECT_THROW(this->Eval(this->storage.template Create<RegexMatch>(LITERAL("string"), LIST(LITERAL("regex")))),
               QueryRuntimeException);
}

TYPED_TEST(ExpressionEvaluatorTest, RegexMatchInvalidRegex) {
  EXPECT_THROW(this->Eval(this->storage.template Create<RegexMatch>(LITERAL("text"), LITERAL("*ext"))),
               QueryRuntimeException);
  EXPECT_THROW(this->Eval(this->storage.template Create<RegexMatch>(LITERAL("text"), LITERAL("[ext"))),
               QueryRuntimeException);
}

TYPED_TEST(ExpressionEvaluatorTest, RegexMatch) {
  EXPECT_FALSE(this->Eval(this->storage.template Create<RegexMatch>(LITERAL("text"), LITERAL(".*ex"))).ValueBool());
  EXPECT_TRUE(this->Eval(this->storage.template Create<RegexMatch>(LITERAL("text"), LITERAL(".*ext"))).ValueBool());
  EXPECT_FALSE(this->Eval(this->storage.template Create<RegexMatch>(LITERAL("text"), LITERAL("[ext]"))).ValueBool());
  EXPECT_TRUE(this->Eval(this->storage.template Create<RegexMatch>(LITERAL("text"), LITERAL(".+[ext]"))).ValueBool());
}

template <typename StorageType>
class ExpressionEvaluatorPropertyLookup : public ExpressionEvaluatorTest<StorageType> {
 protected:
  std::pair<std::string, memgraph::storage::PropertyId> prop_age =
      std::make_pair("age", this->dba.NameToProperty("age"));
  std::pair<std::string, memgraph::storage::PropertyId> prop_height =
      std::make_pair("height", this->dba.NameToProperty("height"));
  Identifier *identifier = this->storage.template Create<Identifier>("element");
  Symbol symbol = this->symbol_table.CreateSymbol("element", true);

  void SetUp() override { identifier->MapTo(symbol); }

  auto Value(std::pair<std::string, memgraph::storage::PropertyId> property) {
    auto *op = this->storage.template Create<PropertyLookup>(identifier, this->storage.GetPropertyIx(property.first));
    return this->Eval(op);
  }
};

TYPED_TEST_SUITE(ExpressionEvaluatorPropertyLookup, StorageTypes);

TYPED_TEST(ExpressionEvaluatorPropertyLookup, Vertex) {
  auto v1 = this->dba.InsertVertex();
  ASSERT_TRUE(v1.SetProperty(this->prop_age.second, memgraph::storage::PropertyValue(10)).HasValue());
  this->dba.AdvanceCommand();
  this->frame[this->symbol] = TypedValue(v1);
  EXPECT_EQ(this->Value(this->prop_age).ValueInt(), 10);
  EXPECT_TRUE(this->Value(this->prop_height).IsNull());
}

TYPED_TEST(ExpressionEvaluatorPropertyLookup, Duration) {
  const memgraph::utils::Duration dur({10, 1, 30, 2, 22, 45});
  this->frame[this->symbol] = TypedValue(dur);

  const std::pair day = std::make_pair("day", this->dba.NameToProperty("day"));
  const auto total_days = this->Value(day);
  EXPECT_TRUE(total_days.IsInt());
  EXPECT_EQ(total_days.ValueInt(), 10);

  const std::pair hour = std::make_pair("hour", this->dba.NameToProperty("hour"));
  const auto total_hours = this->Value(hour);
  EXPECT_TRUE(total_hours.IsInt());
  EXPECT_EQ(total_hours.ValueInt(), 1);

  const std::pair minute = std::make_pair("minute", this->dba.NameToProperty("minute"));
  const auto total_mins = this->Value(minute);
  EXPECT_TRUE(total_mins.IsInt());

  EXPECT_EQ(total_mins.ValueInt(), 1 * 60 + 30);

  const std::pair sec = std::make_pair("second", this->dba.NameToProperty("second"));
  const auto total_secs = this->Value(sec);
  EXPECT_TRUE(total_secs.IsInt());
  const auto expected_secs = total_mins.ValueInt() * 60 + 2;
  EXPECT_EQ(total_secs.ValueInt(), expected_secs);

  const std::pair milli = std::make_pair("millisecond", this->dba.NameToProperty("millisecond"));
  const auto total_milli = this->Value(milli);
  EXPECT_TRUE(total_milli.IsInt());
  const auto expected_milli = total_secs.ValueInt() * 1000 + 22;
  EXPECT_EQ(total_milli.ValueInt(), expected_milli);

  const std::pair micro = std::make_pair("microsecond", this->dba.NameToProperty("microsecond"));
  const auto total_micros = this->Value(micro);
  EXPECT_TRUE(total_micros.IsInt());
  const auto expected_micros = expected_milli * 1000 + 45;
  EXPECT_EQ(total_micros.ValueInt(), expected_micros);

  const std::pair nano = std::make_pair("nanosecond", this->dba.NameToProperty("nanosecond"));
  const auto total_nano = this->Value(nano);
  EXPECT_TRUE(total_nano.IsInt());
  const auto expected_nano = expected_micros * 1000;
  EXPECT_EQ(total_nano.ValueInt(), expected_nano);
}

TYPED_TEST(ExpressionEvaluatorPropertyLookup, Date) {
  const memgraph::utils::Date date({1996, 11, 22});
  this->frame[this->symbol] = TypedValue(date);

  const std::pair year = std::make_pair("year", this->dba.NameToProperty("year"));
  const auto y = this->Value(year);
  EXPECT_TRUE(y.IsInt());
  EXPECT_EQ(y.ValueInt(), 1996);

  const std::pair month = std::make_pair("month", this->dba.NameToProperty("month"));
  const auto m = this->Value(month);
  EXPECT_TRUE(m.IsInt());
  EXPECT_EQ(m.ValueInt(), 11);

  const std::pair day = std::make_pair("day", this->dba.NameToProperty("day"));
  const auto d = this->Value(day);
  EXPECT_TRUE(d.IsInt());
  EXPECT_EQ(d.ValueInt(), 22);
}

TYPED_TEST(ExpressionEvaluatorPropertyLookup, LocalTime) {
  const memgraph::utils::LocalTime lt({1, 2, 3, 11, 22});
  this->frame[this->symbol] = TypedValue(lt);

  const std::pair hour = std::make_pair("hour", this->dba.NameToProperty("hour"));
  const auto h = this->Value(hour);
  EXPECT_TRUE(h.IsInt());
  EXPECT_EQ(h.ValueInt(), 1);

  const std::pair minute = std::make_pair("minute", this->dba.NameToProperty("minute"));
  const auto min = this->Value(minute);
  EXPECT_TRUE(min.IsInt());
  EXPECT_EQ(min.ValueInt(), 2);

  const std::pair second = std::make_pair("second", this->dba.NameToProperty("second"));
  const auto sec = this->Value(second);
  EXPECT_TRUE(sec.IsInt());
  EXPECT_EQ(sec.ValueInt(), 3);

  const std::pair millis = std::make_pair("millisecond", this->dba.NameToProperty("millisecond"));
  const auto mil = this->Value(millis);
  EXPECT_TRUE(mil.IsInt());
  EXPECT_EQ(mil.ValueInt(), 11);

  const std::pair micros = std::make_pair("microsecond", this->dba.NameToProperty("microsecond"));
  const auto mic = this->Value(micros);
  EXPECT_TRUE(mic.IsInt());
  EXPECT_EQ(mic.ValueInt(), 22);
}

TYPED_TEST(ExpressionEvaluatorPropertyLookup, LocalDateTime) {
  const memgraph::utils::LocalDateTime ldt({1993, 8, 6}, {2, 3, 4, 55, 40});
  this->frame[this->symbol] = TypedValue(ldt);

  const std::pair year = std::make_pair("year", this->dba.NameToProperty("year"));
  const auto y = this->Value(year);
  EXPECT_TRUE(y.IsInt());
  EXPECT_EQ(y.ValueInt(), 1993);

  const std::pair month = std::make_pair("month", this->dba.NameToProperty("month"));
  const auto m = this->Value(month);
  EXPECT_TRUE(m.IsInt());
  EXPECT_EQ(m.ValueInt(), 8);

  const std::pair day = std::make_pair("day", this->dba.NameToProperty("day"));
  const auto d = this->Value(day);
  EXPECT_TRUE(d.IsInt());
  EXPECT_EQ(d.ValueInt(), 6);

  const std::pair hour = std::make_pair("hour", this->dba.NameToProperty("hour"));
  const auto h = this->Value(hour);
  EXPECT_TRUE(h.IsInt());
  EXPECT_EQ(h.ValueInt(), 2);

  const std::pair minute = std::make_pair("minute", this->dba.NameToProperty("minute"));
  const auto min = this->Value(minute);
  EXPECT_TRUE(min.IsInt());
  EXPECT_EQ(min.ValueInt(), 3);

  const std::pair second = std::make_pair("second", this->dba.NameToProperty("second"));
  const auto sec = this->Value(second);
  EXPECT_TRUE(sec.IsInt());
  EXPECT_EQ(sec.ValueInt(), 4);

  const std::pair millis = std::make_pair("millisecond", this->dba.NameToProperty("millisecond"));
  const auto mil = this->Value(millis);
  EXPECT_TRUE(mil.IsInt());
  EXPECT_EQ(mil.ValueInt(), 55);

  const std::pair micros = std::make_pair("microsecond", this->dba.NameToProperty("microsecond"));
  const auto mic = this->Value(micros);
  EXPECT_TRUE(mic.IsInt());
  EXPECT_EQ(mic.ValueInt(), 40);
}

TYPED_TEST(ExpressionEvaluatorPropertyLookup, ZonedDateTime) {
  const auto zdt = memgraph::utils::ZonedDateTime(
      {{2024, 3, 25}, {14, 18, 13, 206, 22}, memgraph::utils::Timezone("Europe/Zagreb")});
  this->frame[this->symbol] = TypedValue(zdt);

  const std::pair year = std::make_pair("year", this->dba.NameToProperty("year"));
  const auto y = this->Value(year);
  EXPECT_TRUE(y.IsInt());
  EXPECT_EQ(y.ValueInt(), 2024);

  const std::pair month = std::make_pair("month", this->dba.NameToProperty("month"));
  const auto m = this->Value(month);
  EXPECT_TRUE(m.IsInt());
  EXPECT_EQ(m.ValueInt(), 3);

  const std::pair day = std::make_pair("day", this->dba.NameToProperty("day"));
  const auto d = this->Value(day);
  EXPECT_TRUE(d.IsInt());
  EXPECT_EQ(d.ValueInt(), 25);

  const std::pair hour = std::make_pair("hour", this->dba.NameToProperty("hour"));
  const auto h = this->Value(hour);
  EXPECT_TRUE(h.IsInt());
  EXPECT_EQ(h.ValueInt(), 14);

  const std::pair minute = std::make_pair("minute", this->dba.NameToProperty("minute"));
  const auto min = this->Value(minute);
  EXPECT_TRUE(min.IsInt());
  EXPECT_EQ(min.ValueInt(), 18);

  const std::pair second = std::make_pair("second", this->dba.NameToProperty("second"));
  const auto sec = this->Value(second);
  EXPECT_TRUE(sec.IsInt());
  EXPECT_EQ(sec.ValueInt(), 13);

  const std::pair millis = std::make_pair("millisecond", this->dba.NameToProperty("millisecond"));
  const auto mil = this->Value(millis);
  EXPECT_TRUE(mil.IsInt());
  EXPECT_EQ(mil.ValueInt(), 206);

  const std::pair micros = std::make_pair("microsecond", this->dba.NameToProperty("microsecond"));
  const auto mic = this->Value(micros);
  EXPECT_TRUE(mic.IsInt());
  EXPECT_EQ(mic.ValueInt(), 22);
}

TYPED_TEST(ExpressionEvaluatorPropertyLookup, Edge) {
  auto v1 = this->dba.InsertVertex();
  auto v2 = this->dba.InsertVertex();
  auto e12 = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("edge_type"));
  ASSERT_TRUE(e12.HasValue());
  ASSERT_TRUE(e12->SetProperty(this->prop_age.second, memgraph::storage::PropertyValue(10)).HasValue());
  this->dba.AdvanceCommand();
  this->frame[this->symbol] = TypedValue(*e12);
  EXPECT_EQ(this->Value(this->prop_age).ValueInt(), 10);
  EXPECT_TRUE(this->Value(this->prop_height).IsNull());
}

TYPED_TEST(ExpressionEvaluatorPropertyLookup, Null) {
  this->frame[this->symbol] = TypedValue();
  EXPECT_TRUE(this->Value(this->prop_age).IsNull());
}

TYPED_TEST(ExpressionEvaluatorPropertyLookup, Map) {
  this->frame[this->symbol] = TypedValue(std::map<std::string, TypedValue>{{this->prop_age.first, TypedValue(10)}});
  EXPECT_EQ(this->Value(this->prop_age).ValueInt(), 10);
  EXPECT_TRUE(this->Value(this->prop_height).IsNull());
}

template <typename StorageType>
class ExpressionEvaluatorAllPropertiesLookup : public ExpressionEvaluatorTest<StorageType> {
 protected:
  std::pair<std::string, memgraph::storage::PropertyId> prop_age =
      std::make_pair("age", this->dba.NameToProperty("age"));
  std::pair<std::string, memgraph::storage::PropertyId> prop_height =
      std::make_pair("height", this->dba.NameToProperty("height"));
  Identifier *identifier = this->storage.template Create<Identifier>("element");
  Symbol symbol = this->symbol_table.CreateSymbol("element", true);

  void SetUp() override { identifier->MapTo(symbol); }

  auto Value() {
    auto *op = this->storage.template Create<AllPropertiesLookup>(identifier);
    return this->Eval(op);
  }
};

TYPED_TEST_SUITE(ExpressionEvaluatorAllPropertiesLookup, StorageTypes);

TYPED_TEST(ExpressionEvaluatorAllPropertiesLookup, Vertex) {
  auto v1 = this->dba.InsertVertex();
  ASSERT_TRUE(v1.SetProperty(this->prop_age.second, memgraph::storage::PropertyValue(10)).HasValue());
  this->dba.AdvanceCommand();
  this->frame[this->symbol] = TypedValue(v1);
  auto all_properties = this->Value();
  EXPECT_TRUE(all_properties.IsMap());
}

TYPED_TEST(ExpressionEvaluatorAllPropertiesLookup, Edge) {
  auto v1 = this->dba.InsertVertex();
  auto v2 = this->dba.InsertVertex();
  auto e12 = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("edge_type"));
  ASSERT_TRUE(e12.HasValue());
  ASSERT_TRUE(e12->SetProperty(this->prop_age.second, memgraph::storage::PropertyValue(10)).HasValue());
  this->dba.AdvanceCommand();
  this->frame[this->symbol] = TypedValue(*e12);
  auto all_properties = this->Value();
  EXPECT_TRUE(all_properties.IsMap());
}

TYPED_TEST(ExpressionEvaluatorAllPropertiesLookup, Duration) {
  const memgraph::utils::Duration dur({10, 1, 30, 2, 22, 45});
  this->frame[this->symbol] = TypedValue(dur);
  auto all_properties = this->Value();
  EXPECT_TRUE(all_properties.IsMap());
}

TYPED_TEST(ExpressionEvaluatorAllPropertiesLookup, Date) {
  const memgraph::utils::Date date({1996, 11, 22});
  this->frame[this->symbol] = TypedValue(date);
  auto all_properties = this->Value();
  EXPECT_TRUE(all_properties.IsMap());
}

TYPED_TEST(ExpressionEvaluatorAllPropertiesLookup, LocalTime) {
  const memgraph::utils::LocalTime lt({1, 2, 3, 11, 22});
  this->frame[this->symbol] = TypedValue(lt);
  auto all_properties = this->Value();
  EXPECT_TRUE(all_properties.IsMap());
}

TYPED_TEST(ExpressionEvaluatorAllPropertiesLookup, LocalDateTime) {
  const memgraph::utils::LocalDateTime ldt({1993, 8, 6}, {2, 3, 4, 55, 40});
  this->frame[this->symbol] = TypedValue(ldt);
  auto all_properties = this->Value();
  EXPECT_TRUE(all_properties.IsMap());
}

TYPED_TEST(ExpressionEvaluatorAllPropertiesLookup, ZonedDateTime) {
  const auto zdt = memgraph::utils::ZonedDateTime(
      {{2024, 3, 25}, {14, 18, 13, 206, 22}, memgraph::utils::Timezone("Europe/Zagreb")});
  this->frame[this->symbol] = TypedValue(zdt);
  ASSERT_THROW(this->Value(), QueryRuntimeException);
}

TYPED_TEST(ExpressionEvaluatorAllPropertiesLookup, Null) {
  this->frame[this->symbol] = TypedValue();
  auto all_properties = this->Value();
  EXPECT_TRUE(all_properties.IsNull());
}

TYPED_TEST(ExpressionEvaluatorAllPropertiesLookup, Map) {
  this->frame[this->symbol] = TypedValue(std::map<std::string, TypedValue>{{this->prop_age.first, TypedValue(10)}});
  auto all_properties = this->Value();
  EXPECT_TRUE(all_properties.IsMap());
}

template <typename StorageType>
class FunctionTest : public ExpressionEvaluatorTest<StorageType> {
 protected:
  std::vector<Expression *> ExpressionsFromTypedValues(const std::vector<TypedValue> &tvs) {
    std::vector<Expression *> expressions;
    expressions.reserve(tvs.size());

    for (size_t i = 0; i < tvs.size(); ++i) {
      auto *ident = this->storage.template Create<Identifier>("arg_" + std::to_string(i), true);
      auto sym = this->symbol_table.CreateSymbol("arg_" + std::to_string(i), true);
      ident->MapTo(sym);
      this->frame[sym] = tvs[i];
      expressions.push_back(ident);
    }

    return expressions;
  }

  TypedValue EvaluateFunctionWithExprs(const std::string &function_name, const std::vector<Expression *> &expressions) {
    auto *op = this->storage.template Create<Function>(function_name, expressions);
    return this->Eval(op);
  }

  template <class... TArgs>
  TypedValue EvaluateFunction(const std::string &function_name, std::tuple<TArgs...> args) {
    std::vector<TypedValue> tv_args;
    tv_args.reserve(args.size());
    for (auto &arg : args) tv_args.emplace_back(std::move(arg));
    return EvaluateFunctionWithExprs(function_name, ExpressionsFromTypedValues(tv_args));
  }

  template <class... TArgs>
  TypedValue EvaluateFunction(const std::string &function_name, TArgs &&...args) {
    return EvaluateFunctionWithExprs(function_name,
                                     ExpressionsFromTypedValues(std::vector<TypedValue>{TypedValue(args)...}));
  }
};

TYPED_TEST_SUITE(FunctionTest, StorageTypes);

template <class... TArgs>
static TypedValue MakeTypedValueList(TArgs &&...args) {
  return TypedValue(std::vector<TypedValue>{TypedValue(args)...});
}

TYPED_TEST(FunctionTest, EndNode) {
  ASSERT_THROW(this->EvaluateFunction("ENDNODE"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("ENDNODE", TypedValue()).IsNull());
  auto v1 = this->dba.InsertVertex();
  ASSERT_TRUE(v1.AddLabel(this->dba.NameToLabel("label1")).HasValue());
  auto v2 = this->dba.InsertVertex();
  ASSERT_TRUE(v2.AddLabel(this->dba.NameToLabel("label2")).HasValue());
  auto e = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("t"));
  ASSERT_TRUE(e.HasValue());
  ASSERT_TRUE(*this->EvaluateFunction("ENDNODE", *e)
                   .ValueVertex()
                   .HasLabel(memgraph::storage::View::NEW, this->dba.NameToLabel("label2")));
  ASSERT_THROW(this->EvaluateFunction("ENDNODE", 2), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Head) {
  ASSERT_THROW(this->EvaluateFunction("HEAD"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("HEAD", TypedValue()).IsNull());
  auto argument = MakeTypedValueList(3, 4, 5);
  ASSERT_EQ(this->EvaluateFunction("HEAD", argument).ValueInt(), 3);
  argument.ValueList().clear();
  ASSERT_TRUE(this->EvaluateFunction("HEAD", argument).IsNull());
  ASSERT_THROW(this->EvaluateFunction("HEAD", 2), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Properties) {
  ASSERT_THROW(this->EvaluateFunction("PROPERTIES"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("PROPERTIES", TypedValue()).IsNull());
  auto v1 = this->dba.InsertVertex();
  ASSERT_TRUE(v1.SetProperty(this->dba.NameToProperty("height"), memgraph::storage::PropertyValue(5)).HasValue());
  ASSERT_TRUE(v1.SetProperty(this->dba.NameToProperty("age"), memgraph::storage::PropertyValue(10)).HasValue());
  auto v2 = this->dba.InsertVertex();
  auto e = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("type1"));
  ASSERT_TRUE(e.HasValue());
  ASSERT_TRUE(e->SetProperty(this->dba.NameToProperty("height"), memgraph::storage::PropertyValue(3)).HasValue());
  ASSERT_TRUE(e->SetProperty(this->dba.NameToProperty("age"), memgraph::storage::PropertyValue(15)).HasValue());
  this->dba.AdvanceCommand();

  auto prop_values_to_int = [](TypedValue t) {
    std::unordered_map<std::string, int> properties;
    for (auto property : t.ValueMap()) {
      properties[std::string(property.first)] = property.second.ValueInt();
    }
    return properties;
  };

  ASSERT_THAT(prop_values_to_int(this->EvaluateFunction("PROPERTIES", v1)),
              UnorderedElementsAre(testing::Pair("height", 5), testing::Pair("age", 10)));
  ASSERT_THAT(prop_values_to_int(this->EvaluateFunction("PROPERTIES", *e)),
              UnorderedElementsAre(testing::Pair("height", 3), testing::Pair("age", 15)));
  ASSERT_THROW(this->EvaluateFunction("PROPERTIES", 2), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Last) {
  ASSERT_THROW(this->EvaluateFunction("LAST"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("LAST", TypedValue()).IsNull());
  auto argument = MakeTypedValueList(3, 4, 5);
  ASSERT_EQ(this->EvaluateFunction("LAST", argument).ValueInt(), 5);
  argument.ValueList().clear();
  ASSERT_TRUE(this->EvaluateFunction("LAST", argument).IsNull());
  ASSERT_THROW(this->EvaluateFunction("LAST", 5), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Size) {
  ASSERT_THROW(this->EvaluateFunction("SIZE"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("SIZE", TypedValue()).IsNull());
  auto argument = MakeTypedValueList(3, 4, 5);
  ASSERT_EQ(this->EvaluateFunction("SIZE", argument).ValueInt(), 3);
  ASSERT_EQ(this->EvaluateFunction("SIZE", "john").ValueInt(), 4);
  ASSERT_EQ(this->EvaluateFunction("SIZE",
                                   std::map<std::string, TypedValue>{
                                       {"a", TypedValue(5)}, {"b", TypedValue(true)}, {"c", TypedValue("123")}})
                .ValueInt(),
            3);
  ASSERT_THROW(this->EvaluateFunction("SIZE", 5), QueryRuntimeException);

  auto v0 = this->dba.InsertVertex();
  memgraph::query::Path path(v0);
  EXPECT_EQ(this->EvaluateFunction("SIZE", path).ValueInt(), 0);
  auto v1 = this->dba.InsertVertex();
  auto edge = this->dba.InsertEdge(&v0, &v1, this->dba.NameToEdgeType("type"));
  ASSERT_TRUE(edge.HasValue());
  path.Expand(*edge);
  path.Expand(v1);
  EXPECT_EQ(this->EvaluateFunction("SIZE", path).ValueInt(), 1);
}

TYPED_TEST(FunctionTest, StartNode) {
  ASSERT_THROW(this->EvaluateFunction("STARTNODE"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("STARTNODE", TypedValue()).IsNull());
  auto v1 = this->dba.InsertVertex();
  ASSERT_TRUE(v1.AddLabel(this->dba.NameToLabel("label1")).HasValue());
  auto v2 = this->dba.InsertVertex();
  ASSERT_TRUE(v2.AddLabel(this->dba.NameToLabel("label2")).HasValue());
  auto e = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("t"));
  ASSERT_TRUE(e.HasValue());
  ASSERT_TRUE(*this->EvaluateFunction("STARTNODE", *e)
                   .ValueVertex()
                   .HasLabel(memgraph::storage::View::NEW, this->dba.NameToLabel("label1")));
  ASSERT_THROW(this->EvaluateFunction("STARTNODE", 2), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Degree) {
  ASSERT_THROW(this->EvaluateFunction("DEGREE"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("DEGREE", TypedValue()).IsNull());
  auto v1 = this->dba.InsertVertex();
  auto v2 = this->dba.InsertVertex();
  auto v3 = this->dba.InsertVertex();
  auto e12 = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("t"));
  ASSERT_TRUE(e12.HasValue());
  ASSERT_TRUE(this->dba.InsertEdge(&v3, &v2, this->dba.NameToEdgeType("t")).HasValue());
  this->dba.AdvanceCommand();
  ASSERT_EQ(this->EvaluateFunction("DEGREE", v1).ValueInt(), 1);
  ASSERT_EQ(this->EvaluateFunction("DEGREE", v2).ValueInt(), 2);
  ASSERT_EQ(this->EvaluateFunction("DEGREE", v3).ValueInt(), 1);
  ASSERT_THROW(this->EvaluateFunction("DEGREE", 2), QueryRuntimeException);
  ASSERT_THROW(this->EvaluateFunction("DEGREE", *e12), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, InDegree) {
  ASSERT_THROW(this->EvaluateFunction("INDEGREE"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("INDEGREE", TypedValue()).IsNull());
  auto v1 = this->dba.InsertVertex();
  auto v2 = this->dba.InsertVertex();
  auto v3 = this->dba.InsertVertex();
  auto e12 = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("t"));
  ASSERT_TRUE(e12.HasValue());
  ASSERT_TRUE(this->dba.InsertEdge(&v3, &v2, this->dba.NameToEdgeType("t")).HasValue());
  this->dba.AdvanceCommand();
  ASSERT_EQ(this->EvaluateFunction("INDEGREE", v1).ValueInt(), 0);
  ASSERT_EQ(this->EvaluateFunction("INDEGREE", v2).ValueInt(), 2);
  ASSERT_EQ(this->EvaluateFunction("INDEGREE", v3).ValueInt(), 0);
  ASSERT_THROW(this->EvaluateFunction("INDEGREE", 2), QueryRuntimeException);
  ASSERT_THROW(this->EvaluateFunction("INDEGREE", *e12), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, OutDegree) {
  ASSERT_THROW(this->EvaluateFunction("OUTDEGREE"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("OUTDEGREE", TypedValue()).IsNull());
  auto v1 = this->dba.InsertVertex();
  auto v2 = this->dba.InsertVertex();
  auto v3 = this->dba.InsertVertex();
  auto e12 = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("t"));
  ASSERT_TRUE(e12.HasValue());
  ASSERT_TRUE(this->dba.InsertEdge(&v3, &v2, this->dba.NameToEdgeType("t")).HasValue());
  this->dba.AdvanceCommand();
  ASSERT_EQ(this->EvaluateFunction("OUTDEGREE", v1).ValueInt(), 1);
  ASSERT_EQ(this->EvaluateFunction("OUTDEGREE", v2).ValueInt(), 0);
  ASSERT_EQ(this->EvaluateFunction("OUTDEGREE", v3).ValueInt(), 1);
  ASSERT_THROW(this->EvaluateFunction("OUTDEGREE", 2), QueryRuntimeException);
  ASSERT_THROW(this->EvaluateFunction("OUTDEGREE", *e12), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, ToBoolean) {
  ASSERT_THROW(this->EvaluateFunction("TOBOOLEAN"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("TOBOOLEAN", TypedValue()).IsNull());
  ASSERT_EQ(this->EvaluateFunction("TOBOOLEAN", 123).ValueBool(), true);
  ASSERT_EQ(this->EvaluateFunction("TOBOOLEAN", -213).ValueBool(), true);
  ASSERT_EQ(this->EvaluateFunction("TOBOOLEAN", 0).ValueBool(), false);
  ASSERT_EQ(this->EvaluateFunction("TOBOOLEAN", " trUE \n\t").ValueBool(), true);
  ASSERT_EQ(this->EvaluateFunction("TOBOOLEAN", "\n\tFalsE").ValueBool(), false);
  ASSERT_TRUE(this->EvaluateFunction("TOBOOLEAN", "\n\tFALSEA ").IsNull());
  ASSERT_EQ(this->EvaluateFunction("TOBOOLEAN", true).ValueBool(), true);
  ASSERT_EQ(this->EvaluateFunction("TOBOOLEAN", false).ValueBool(), false);
}

TYPED_TEST(FunctionTest, ToFloat) {
  ASSERT_THROW(this->EvaluateFunction("TOFLOAT"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("TOFLOAT", TypedValue()).IsNull());
  ASSERT_EQ(this->EvaluateFunction("TOFLOAT", " -3.5 \n\t").ValueDouble(), -3.5);
  ASSERT_EQ(this->EvaluateFunction("TOFLOAT", "\n\t0.5e-1").ValueDouble(), 0.05);
  ASSERT_TRUE(this->EvaluateFunction("TOFLOAT", "\n\t3.4e-3X ").IsNull());
  ASSERT_EQ(this->EvaluateFunction("TOFLOAT", -3.5).ValueDouble(), -3.5);
  ASSERT_EQ(this->EvaluateFunction("TOFLOAT", -3).ValueDouble(), -3.0);
  ASSERT_THROW(this->EvaluateFunction("TOFLOAT", true), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, ToInteger) {
  ASSERT_THROW(this->EvaluateFunction("TOINTEGER"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("TOINTEGER", TypedValue()).IsNull());
  ASSERT_EQ(this->EvaluateFunction("TOINTEGER", false).ValueInt(), 0);
  ASSERT_EQ(this->EvaluateFunction("TOINTEGER", true).ValueInt(), 1);
  ASSERT_EQ(this->EvaluateFunction("TOINTEGER", "\n\t3").ValueInt(), 3);
  ASSERT_EQ(this->EvaluateFunction("TOINTEGER", " -3.5 \n\t").ValueInt(), -3);
  ASSERT_TRUE(this->EvaluateFunction("TOINTEGER", "\n\t3X ").IsNull());
  ASSERT_EQ(this->EvaluateFunction("TOINTEGER", -3.5).ValueInt(), -3);
  ASSERT_EQ(this->EvaluateFunction("TOINTEGER", 3.5).ValueInt(), 3);
}

TYPED_TEST(FunctionTest, Type) {
  ASSERT_THROW(this->EvaluateFunction("TYPE"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("TYPE", TypedValue()).IsNull());
  auto v1 = this->dba.InsertVertex();
  ASSERT_TRUE(v1.AddLabel(this->dba.NameToLabel("label1")).HasValue());
  auto v2 = this->dba.InsertVertex();
  ASSERT_TRUE(v2.AddLabel(this->dba.NameToLabel("label2")).HasValue());
  auto e = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("type1"));
  ASSERT_TRUE(e.HasValue());
  ASSERT_EQ(this->EvaluateFunction("TYPE", *e).ValueString(), "type1");
  ASSERT_THROW(this->EvaluateFunction("TYPE", 2), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, ValueType) {
  ASSERT_THROW(this->EvaluateFunction("VALUETYPE"), QueryRuntimeException);
  ASSERT_THROW(this->EvaluateFunction("VALUETYPE", TypedValue(), TypedValue()), QueryRuntimeException);
  ASSERT_EQ(this->EvaluateFunction("VALUETYPE", TypedValue()).ValueString(), "NULL");
  ASSERT_EQ(this->EvaluateFunction("VALUETYPE", TypedValue(true)).ValueString(), "BOOLEAN");
  ASSERT_EQ(this->EvaluateFunction("VALUETYPE", TypedValue(1)).ValueString(), "INTEGER");
  ASSERT_EQ(this->EvaluateFunction("VALUETYPE", TypedValue(1.1)).ValueString(), "FLOAT");
  ASSERT_EQ(this->EvaluateFunction("VALUETYPE", TypedValue("test")).ValueString(), "STRING");
  ASSERT_EQ(this->EvaluateFunction("VALUETYPE", TypedValue(std::vector<TypedValue>{TypedValue(1), TypedValue(2)}))
                .ValueString(),
            "LIST");
  ASSERT_EQ(this->EvaluateFunction("VALUETYPE", TypedValue(std::map<std::string, TypedValue>{{"test", TypedValue(1)}}))
                .ValueString(),
            "MAP");
  auto v1 = this->dba.InsertVertex();
  auto v2 = this->dba.InsertVertex();
  ASSERT_EQ(this->EvaluateFunction("VALUETYPE", v1).ValueString(), "NODE");
  auto e = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("type1"));
  ASSERT_TRUE(e.HasValue());
  ASSERT_EQ(this->EvaluateFunction("VALUETYPE", *e).ValueString(), "RELATIONSHIP");
  Path p(v1, *e, v2);
  ASSERT_EQ(this->EvaluateFunction("VALUETYPE", p).ValueString(), "PATH");
}

TYPED_TEST(FunctionTest, Labels) {
  ASSERT_THROW(this->EvaluateFunction("LABELS"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("LABELS", TypedValue()).IsNull());
  auto v = this->dba.InsertVertex();
  ASSERT_TRUE(v.AddLabel(this->dba.NameToLabel("label1")).HasValue());
  ASSERT_TRUE(v.AddLabel(this->dba.NameToLabel("label2")).HasValue());
  this->dba.AdvanceCommand();
  std::vector<std::string> labels;
  auto _labels = this->EvaluateFunction("LABELS", v).ValueList();
  labels.reserve(_labels.size());
  for (auto label : _labels) {
    labels.emplace_back(label.ValueString());
  }
  ASSERT_THAT(labels, UnorderedElementsAre("label1", "label2"));
  ASSERT_THROW(this->EvaluateFunction("LABELS", 2), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, NodesRelationships) {
  EXPECT_THROW(this->EvaluateFunction("NODES"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("RELATIONSHIPS"), QueryRuntimeException);
  EXPECT_TRUE(this->EvaluateFunction("NODES", TypedValue()).IsNull());
  EXPECT_TRUE(this->EvaluateFunction("RELATIONSHIPS", TypedValue()).IsNull());

  {
    auto v1 = this->dba.InsertVertex();
    auto v2 = this->dba.InsertVertex();
    auto v3 = this->dba.InsertVertex();
    auto e1 = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("Type"));
    ASSERT_TRUE(e1.HasValue());
    auto e2 = this->dba.InsertEdge(&v2, &v3, this->dba.NameToEdgeType("Type"));
    ASSERT_TRUE(e2.HasValue());
    memgraph::query::Path path{v1, *e1, v2, *e2, v3};
    this->dba.AdvanceCommand();

    auto _nodes = this->EvaluateFunction("NODES", path).ValueList();
    std::vector<memgraph::query::VertexAccessor> nodes;
    for (const auto &node : _nodes) {
      nodes.push_back(node.ValueVertex());
    }
    EXPECT_THAT(nodes, ElementsAre(v1, v2, v3));

    auto _edges = this->EvaluateFunction("RELATIONSHIPS", path).ValueList();
    std::vector<memgraph::query::EdgeAccessor> edges;
    for (const auto &edge : _edges) {
      edges.push_back(edge.ValueEdge());
    }
    EXPECT_THAT(edges, ElementsAre(*e1, *e2));
  }

  EXPECT_THROW(this->EvaluateFunction("NODES", 2), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("RELATIONSHIPS", 2), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Range) {
  EXPECT_THROW(this->EvaluateFunction("RANGE"), QueryRuntimeException);
  EXPECT_TRUE(this->EvaluateFunction("RANGE", 1, 2, TypedValue()).IsNull());
  EXPECT_THROW(this->EvaluateFunction("RANGE", 1, TypedValue(), 1.3), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("RANGE", 1, 2, 0), QueryRuntimeException);
  EXPECT_THAT(ToIntList(this->EvaluateFunction("RANGE", 1, 3)), ElementsAre(1, 2, 3));
  EXPECT_THAT(ToIntList(this->EvaluateFunction("RANGE", -1, 5, 2)), ElementsAre(-1, 1, 3, 5));
  EXPECT_THAT(ToIntList(this->EvaluateFunction("RANGE", 2, 10, 3)), ElementsAre(2, 5, 8));
  EXPECT_THAT(ToIntList(this->EvaluateFunction("RANGE", 2, 2, 2)), ElementsAre(2));
  EXPECT_THAT(ToIntList(this->EvaluateFunction("RANGE", 3, 0, 5)), ElementsAre());
  EXPECT_THAT(ToIntList(this->EvaluateFunction("RANGE", 5, 1, -2)), ElementsAre(5, 3, 1));
  EXPECT_THAT(ToIntList(this->EvaluateFunction("RANGE", 6, 1, -2)), ElementsAre(6, 4, 2));
  EXPECT_THAT(ToIntList(this->EvaluateFunction("RANGE", 2, 2, -3)), ElementsAre(2));
  EXPECT_THAT(ToIntList(this->EvaluateFunction("RANGE", -2, 4, -1)), ElementsAre());
}

TYPED_TEST(FunctionTest, Keys) {
  ASSERT_THROW(this->EvaluateFunction("KEYS"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("KEYS", TypedValue()).IsNull());
  auto v1 = this->dba.InsertVertex();
  ASSERT_TRUE(v1.SetProperty(this->dba.NameToProperty("height"), memgraph::storage::PropertyValue(5)).HasValue());
  ASSERT_TRUE(v1.SetProperty(this->dba.NameToProperty("age"), memgraph::storage::PropertyValue(10)).HasValue());
  auto v2 = this->dba.InsertVertex();
  auto e = this->dba.InsertEdge(&v1, &v2, this->dba.NameToEdgeType("type1"));
  ASSERT_TRUE(e.HasValue());
  ASSERT_TRUE(e->SetProperty(this->dba.NameToProperty("width"), memgraph::storage::PropertyValue(3)).HasValue());
  ASSERT_TRUE(e->SetProperty(this->dba.NameToProperty("age"), memgraph::storage::PropertyValue(15)).HasValue());
  this->dba.AdvanceCommand();

  auto prop_keys_to_string = [](TypedValue t) {
    std::vector<std::string> keys;
    for (auto property : t.ValueList()) {
      keys.emplace_back(property.ValueString());
    }
    return keys;
  };
  ASSERT_THAT(prop_keys_to_string(this->EvaluateFunction("KEYS", v1)), UnorderedElementsAre("height", "age"));
  ASSERT_THAT(prop_keys_to_string(this->EvaluateFunction("KEYS", *e)), UnorderedElementsAre("width", "age"));
  ASSERT_THROW(this->EvaluateFunction("KEYS", 2), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Tail) {
  ASSERT_THROW(this->EvaluateFunction("TAIL"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("TAIL", TypedValue()).IsNull());
  auto argument = MakeTypedValueList();
  ASSERT_EQ(this->EvaluateFunction("TAIL", argument).ValueList().size(), 0U);
  argument = MakeTypedValueList(3, 4, true, "john");
  auto list = this->EvaluateFunction("TAIL", argument).ValueList();
  ASSERT_EQ(list.size(), 3U);
  ASSERT_EQ(list[0].ValueInt(), 4);
  ASSERT_EQ(list[1].ValueBool(), true);
  ASSERT_EQ(list[2].ValueString(), "john");
  ASSERT_THROW(this->EvaluateFunction("TAIL", 2), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, UniformSample) {
  ASSERT_THROW(this->EvaluateFunction("UNIFORMSAMPLE"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("UNIFORMSAMPLE", TypedValue(), TypedValue()).IsNull());
  ASSERT_TRUE(this->EvaluateFunction("UNIFORMSAMPLE", TypedValue(), 1).IsNull());
  ASSERT_TRUE(this->EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(), TypedValue()).IsNull());
  ASSERT_TRUE(this->EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(), 1).IsNull());
  ASSERT_THROW(this->EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(1, 2, 3), -1), QueryRuntimeException);
  ASSERT_EQ(this->EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(1, 2, 3), 0).ValueList().size(), 0);
  ASSERT_EQ(this->EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(1, 2, 3), 2).ValueList().size(), 2);
  ASSERT_EQ(this->EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(1, 2, 3), 3).ValueList().size(), 3);
  ASSERT_EQ(this->EvaluateFunction("UNIFORMSAMPLE", MakeTypedValueList(1, 2, 3), 5).ValueList().size(), 5);
}

TYPED_TEST(FunctionTest, Abs) {
  ASSERT_THROW(this->EvaluateFunction("ABS"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("ABS", TypedValue()).IsNull());
  ASSERT_EQ(this->EvaluateFunction("ABS", -2).ValueInt(), 2);
  ASSERT_EQ(this->EvaluateFunction("ABS", -2.5).ValueDouble(), 2.5);
  ASSERT_THROW(this->EvaluateFunction("ABS", true), QueryRuntimeException);
}

// Test if log works. If it does then all functions wrapped with
// WRAP_CMATH_FLOAT_FUNCTION macro should work and are not gonna be tested for
// correctnes..
TYPED_TEST(FunctionTest, Log) {
  ASSERT_THROW(this->EvaluateFunction("LOG"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("LOG", TypedValue()).IsNull());
  ASSERT_DOUBLE_EQ(this->EvaluateFunction("LOG", 2).ValueDouble(), log(2));
  ASSERT_DOUBLE_EQ(this->EvaluateFunction("LOG", 1.5).ValueDouble(), log(1.5));
  // Not portable, but should work on most platforms.
  ASSERT_TRUE(std::isnan(this->EvaluateFunction("LOG", -1.5).ValueDouble()));
  ASSERT_THROW(this->EvaluateFunction("LOG", true), QueryRuntimeException);
}

// Function Round wraps round from cmath and will work if FunctionTest.Log test
// passes. This test is used to show behavior of round since it differs from
// neo4j's round.
TYPED_TEST(FunctionTest, Round) {
  ASSERT_THROW(this->EvaluateFunction("ROUND"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("ROUND", TypedValue()).IsNull());
  ASSERT_EQ(this->EvaluateFunction("ROUND", -2).ValueDouble(), -2);
  ASSERT_EQ(this->EvaluateFunction("ROUND", -2.4).ValueDouble(), -2);
  ASSERT_EQ(this->EvaluateFunction("ROUND", -2.5).ValueDouble(), -3);
  ASSERT_EQ(this->EvaluateFunction("ROUND", -2.6).ValueDouble(), -3);
  ASSERT_EQ(this->EvaluateFunction("ROUND", 2.4).ValueDouble(), 2);
  ASSERT_EQ(this->EvaluateFunction("ROUND", 2.5).ValueDouble(), 3);
  ASSERT_EQ(this->EvaluateFunction("ROUND", 2.6).ValueDouble(), 3);
  ASSERT_THROW(this->EvaluateFunction("ROUND", true), QueryRuntimeException);
}

// Check if wrapped functions are callable (check if everything was spelled
// correctly...). Wrapper correctnes is checked in FunctionTest.Log function
// test.
TYPED_TEST(FunctionTest, WrappedMathFunctions) {
  for (auto function_name :
       {"FLOOR", "CEIL", "ROUND", "EXP", "LOG", "LOG10", "SQRT", "ACOS", "ASIN", "ATAN", "COS", "SIN", "TAN"}) {
    this->EvaluateFunction(function_name, 0.5);
  }
}

TYPED_TEST(FunctionTest, Atan2) {
  ASSERT_THROW(this->EvaluateFunction("ATAN2"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("ATAN2", TypedValue(), 1).IsNull());
  ASSERT_TRUE(this->EvaluateFunction("ATAN2", 1, TypedValue()).IsNull());
  ASSERT_DOUBLE_EQ(this->EvaluateFunction("ATAN2", 2, -1.0).ValueDouble(), atan2(2, -1));
  ASSERT_THROW(this->EvaluateFunction("ATAN2", 3.0, true), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Sign) {
  ASSERT_THROW(this->EvaluateFunction("SIGN"), QueryRuntimeException);
  ASSERT_TRUE(this->EvaluateFunction("SIGN", TypedValue()).IsNull());
  ASSERT_EQ(this->EvaluateFunction("SIGN", -2).ValueInt(), -1);
  ASSERT_EQ(this->EvaluateFunction("SIGN", -0.2).ValueInt(), -1);
  ASSERT_EQ(this->EvaluateFunction("SIGN", 0.0).ValueInt(), 0);
  ASSERT_EQ(this->EvaluateFunction("SIGN", 2.5).ValueInt(), 1);
  ASSERT_THROW(this->EvaluateFunction("SIGN", true), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, E) {
  ASSERT_THROW(this->EvaluateFunction("E", 1), QueryRuntimeException);
  ASSERT_DOUBLE_EQ(this->EvaluateFunction("E").ValueDouble(), M_E);
}

TYPED_TEST(FunctionTest, Pi) {
  ASSERT_THROW(this->EvaluateFunction("PI", 1), QueryRuntimeException);
  ASSERT_DOUBLE_EQ(this->EvaluateFunction("PI").ValueDouble(), M_PI);
}

TYPED_TEST(FunctionTest, Rand) {
  ASSERT_THROW(this->EvaluateFunction("RAND", 1), QueryRuntimeException);
  ASSERT_GE(this->EvaluateFunction("RAND").ValueDouble(), 0.0);
  ASSERT_LT(this->EvaluateFunction("RAND").ValueDouble(), 1.0);
}

TYPED_TEST(FunctionTest, StartsWith) {
  EXPECT_THROW(this->EvaluateFunction(kStartsWith), QueryRuntimeException);
  EXPECT_TRUE(this->EvaluateFunction(kStartsWith, "a", TypedValue()).IsNull());
  EXPECT_THROW(this->EvaluateFunction(kStartsWith, TypedValue(), 1.3), QueryRuntimeException);
  EXPECT_TRUE(this->EvaluateFunction(kStartsWith, "abc", "abc").ValueBool());
  EXPECT_TRUE(this->EvaluateFunction(kStartsWith, "abcdef", "abc").ValueBool());
  EXPECT_FALSE(this->EvaluateFunction(kStartsWith, "abcdef", "aBc").ValueBool());
  EXPECT_FALSE(this->EvaluateFunction(kStartsWith, "abc", "abcd").ValueBool());
}

TYPED_TEST(FunctionTest, EndsWith) {
  EXPECT_THROW(this->EvaluateFunction(kEndsWith), QueryRuntimeException);
  EXPECT_TRUE(this->EvaluateFunction(kEndsWith, "a", TypedValue()).IsNull());
  EXPECT_THROW(this->EvaluateFunction(kEndsWith, TypedValue(), 1.3), QueryRuntimeException);
  EXPECT_TRUE(this->EvaluateFunction(kEndsWith, "abc", "abc").ValueBool());
  EXPECT_TRUE(this->EvaluateFunction(kEndsWith, "abcdef", "def").ValueBool());
  EXPECT_FALSE(this->EvaluateFunction(kEndsWith, "abcdef", "dEf").ValueBool());
  EXPECT_FALSE(this->EvaluateFunction(kEndsWith, "bcd", "abcd").ValueBool());
}

TYPED_TEST(FunctionTest, Contains) {
  EXPECT_THROW(this->EvaluateFunction(kContains), QueryRuntimeException);
  EXPECT_TRUE(this->EvaluateFunction(kContains, "a", TypedValue()).IsNull());
  EXPECT_THROW(this->EvaluateFunction(kContains, TypedValue(), 1.3), QueryRuntimeException);
  EXPECT_TRUE(this->EvaluateFunction(kContains, "abc", "abc").ValueBool());
  EXPECT_TRUE(this->EvaluateFunction(kContains, "abcde", "bcd").ValueBool());
  EXPECT_FALSE(this->EvaluateFunction(kContains, "cde", "abcdef").ValueBool());
  EXPECT_FALSE(this->EvaluateFunction(kContains, "abcdef", "dEf").ValueBool());
}

TYPED_TEST(FunctionTest, Assert) {
  // Invalid calls.
  ASSERT_THROW(this->EvaluateFunction("ASSERT"), QueryRuntimeException);
  ASSERT_THROW(this->EvaluateFunction("ASSERT", false, false), QueryRuntimeException);
  ASSERT_THROW(this->EvaluateFunction("ASSERT", "string", false), QueryRuntimeException);
  ASSERT_THROW(this->EvaluateFunction("ASSERT", false, "reason", true), QueryRuntimeException);

  // Valid calls, assertion fails.
  ASSERT_THROW(this->EvaluateFunction("ASSERT", false), QueryRuntimeException);
  ASSERT_THROW(this->EvaluateFunction("ASSERT", false, "message"), QueryRuntimeException);
  try {
    this->EvaluateFunction("ASSERT", false, "bbgba");
  } catch (QueryRuntimeException &e) {
    ASSERT_TRUE(std::string(e.what()).find("bbgba") != std::string::npos);
  }

  // Valid calls, assertion passes.
  ASSERT_TRUE(this->EvaluateFunction("ASSERT", true).ValueBool());
  ASSERT_TRUE(this->EvaluateFunction("ASSERT", true, "message").ValueBool());
}

TYPED_TEST(FunctionTest, Counter) {
  EXPECT_THROW(this->EvaluateFunction("COUNTER"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("COUNTER", "a"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("COUNTER", "a", "b"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("COUNTER", "a", "b", "c"), QueryRuntimeException);

  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c1", 0).ValueInt(), 0);
  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c1", 0).ValueInt(), 1);
  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c2", 0).ValueInt(), 0);
  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c1", 0).ValueInt(), 2);
  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c2", 0).ValueInt(), 1);

  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c3", -1).ValueInt(), -1);
  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c3", -1).ValueInt(), 0);
  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c3", -1).ValueInt(), 1);

  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c4", 0, 5).ValueInt(), 0);
  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c4", 0, 5).ValueInt(), 5);
  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c4", 0, 5).ValueInt(), 10);

  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c5", 0, -5).ValueInt(), 0);
  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c5", 0, -5).ValueInt(), -5);
  EXPECT_EQ(this->EvaluateFunction("COUNTER", "c5", 0, -5).ValueInt(), -10);

  EXPECT_THROW(this->EvaluateFunction("COUNTER", "c6", 0, 0), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Id) {
  auto va = this->dba.InsertVertex();
  auto ea = this->dba.InsertEdge(&va, &va, this->dba.NameToEdgeType("edge"));
  ASSERT_TRUE(ea.HasValue());
  auto vb = this->dba.InsertVertex();
  this->dba.AdvanceCommand();
  EXPECT_TRUE(this->EvaluateFunction("ID", TypedValue()).IsNull());
  EXPECT_EQ(this->EvaluateFunction("ID", va).ValueInt(), 0);
  EXPECT_EQ(this->EvaluateFunction("ID", *ea).ValueInt(), 0);
  EXPECT_EQ(this->EvaluateFunction("ID", vb).ValueInt(), 1);
  EXPECT_THROW(this->EvaluateFunction("ID"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("ID", 0), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("ID", va, *ea), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, ToStringNull) { EXPECT_TRUE(this->EvaluateFunction("TOSTRING", TypedValue()).IsNull()); }

TYPED_TEST(FunctionTest, ToStringString) {
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", "").ValueString(), "");
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", "this is a string").ValueString(), "this is a string");
}

TYPED_TEST(FunctionTest, ToStringInteger) {
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", -23321312).ValueString(), "-23321312");
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", 0).ValueString(), "0");
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", 42).ValueString(), "42");
}

TYPED_TEST(FunctionTest, ToStringDouble) {
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", -42.42).ValueString(), "-42.420000000000002");
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", 0.0).ValueString(), "0");
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", 238910.2313217).ValueString(), "238910.231321700004628");
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", 238910.23132171234).ValueString(), "238910.231321712344652");
}

TYPED_TEST(FunctionTest, ToStringBool) {
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", true).ValueString(), "true");
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", false).ValueString(), "false");
}

TYPED_TEST(FunctionTest, ToStringDate) {
  const auto date = memgraph::utils::Date({1970, 1, 2});
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", date).ValueString(), "1970-01-02");
}

TYPED_TEST(FunctionTest, ToStringLocalTime) {
  const auto lt = memgraph::utils::LocalTime({13, 2, 40, 100, 50});
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", lt).ValueString(), "13:02:40.100050");
}

TYPED_TEST(FunctionTest, ToStringLocalDateTime) {
  const auto ldt = memgraph::utils::LocalDateTime({1970, 1, 2}, {23, 02, 59});
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", ldt).ValueString(), "1970-01-02T23:02:59.000000");
}

TYPED_TEST(FunctionTest, ToStringDuration) {
  memgraph::utils::Duration duration{{.minute = 2, .second = 2, .microsecond = 33}};
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", duration).ValueString(), "P0DT0H2M2.000033S");
}

TYPED_TEST(FunctionTest, ToStringZonedDateTime) {
  const auto zdt = memgraph::utils::ZonedDateTime(
      {{2024, 3, 25}, {14, 18, 13, 206, 22}, memgraph::utils::Timezone("Europe/Zagreb")});
  EXPECT_EQ(this->EvaluateFunction("TOSTRING", zdt).ValueString(), "2024-03-25T14:18:13.206022+01:00[Europe/Zagreb]");
}

TYPED_TEST(FunctionTest, ToStringExceptions) {
  EXPECT_THROW(this->EvaluateFunction("TOSTRING", 1, 2, 3), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, TimestampVoid) {
  this->ctx.timestamp = 42;
  EXPECT_EQ(this->EvaluateFunction("TIMESTAMP").ValueInt(), 42);
}

TYPED_TEST(FunctionTest, TimestampDate) {
  this->ctx.timestamp = 42;
  EXPECT_EQ(this->EvaluateFunction("TIMESTAMP", memgraph::utils::Date({1970, 1, 1})).ValueInt(), 0);
  EXPECT_EQ(this->EvaluateFunction("TIMESTAMP", memgraph::utils::Date({1971, 1, 1})).ValueInt(), 31536000000000);
}

TYPED_TEST(FunctionTest, TimestampLocalTime) {
  this->ctx.timestamp = 42;
  const memgraph::utils::LocalTime time(10000);
  EXPECT_EQ(this->EvaluateFunction("TIMESTAMP", time).ValueInt(), 10000);
}

TYPED_TEST(FunctionTest, TimestampLocalDateTime) {
  this->ctx.timestamp = 42;
  const memgraph::utils::LocalDateTime time(20000);
  EXPECT_EQ(this->EvaluateFunction("TIMESTAMP", time).ValueInt(), 20000);
}

TYPED_TEST(FunctionTest, TimestampDuration) {
  this->ctx.timestamp = 42;
  const memgraph::utils::Duration time(20000);
  EXPECT_EQ(this->EvaluateFunction("TIMESTAMP", time).ValueInt(), 20000);
}

TYPED_TEST(FunctionTest, TimestampZonedDateTime) {
  this->ctx.timestamp = 42;

  const int64_t microseconds = 20000;
  const auto zdt = memgraph::utils::ZonedDateTime(memgraph::utils::AsSysTime(microseconds),
                                                  memgraph::utils::Timezone("America/Los_Angeles"));
  EXPECT_EQ(this->EvaluateFunction("TIMESTAMP", zdt).ValueInt(), microseconds);
}

TYPED_TEST(FunctionTest, TimestampExceptions) {
  this->ctx.timestamp = 42;
  EXPECT_THROW(this->EvaluateFunction("TIMESTAMP", 1).ValueInt(), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Left) {
  EXPECT_THROW(this->EvaluateFunction("LEFT"), QueryRuntimeException);

  EXPECT_TRUE(this->EvaluateFunction("LEFT", TypedValue(), TypedValue()).IsNull());
  EXPECT_TRUE(this->EvaluateFunction("LEFT", TypedValue(), 10).IsNull());
  EXPECT_THROW(this->EvaluateFunction("LEFT", TypedValue(), -10), QueryRuntimeException);

  EXPECT_EQ(this->EvaluateFunction("LEFT", "memgraph", 0).ValueString(), "");
  EXPECT_EQ(this->EvaluateFunction("LEFT", "memgraph", 3).ValueString(), "mem");
  EXPECT_EQ(this->EvaluateFunction("LEFT", "memgraph", 1000).ValueString(), "memgraph");
  EXPECT_THROW(this->EvaluateFunction("LEFT", "memgraph", -10), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("LEFT", "memgraph", "graph"), QueryRuntimeException);

  EXPECT_THROW(this->EvaluateFunction("LEFT", 132, 10), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Right) {
  EXPECT_THROW(this->EvaluateFunction("RIGHT"), QueryRuntimeException);

  EXPECT_TRUE(this->EvaluateFunction("RIGHT", TypedValue(), TypedValue()).IsNull());
  EXPECT_TRUE(this->EvaluateFunction("RIGHT", TypedValue(), 10).IsNull());
  EXPECT_THROW(this->EvaluateFunction("RIGHT", TypedValue(), -10), QueryRuntimeException);

  EXPECT_EQ(this->EvaluateFunction("RIGHT", "memgraph", 0).ValueString(), "");
  EXPECT_EQ(this->EvaluateFunction("RIGHT", "memgraph", 3).ValueString(), "aph");
  EXPECT_EQ(this->EvaluateFunction("RIGHT", "memgraph", 1000).ValueString(), "memgraph");
  EXPECT_THROW(this->EvaluateFunction("RIGHT", "memgraph", -10), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("RIGHT", "memgraph", "graph"), QueryRuntimeException);

  EXPECT_THROW(this->EvaluateFunction("RIGHT", 132, 10), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Trimming) {
  EXPECT_TRUE(this->EvaluateFunction("LTRIM", TypedValue()).IsNull());
  EXPECT_TRUE(this->EvaluateFunction("RTRIM", TypedValue()).IsNull());
  EXPECT_TRUE(this->EvaluateFunction("TRIM", TypedValue()).IsNull());

  EXPECT_EQ(this->EvaluateFunction("LTRIM", "  abc    ").ValueString(), "abc    ");
  EXPECT_EQ(this->EvaluateFunction("RTRIM", " abc ").ValueString(), " abc");
  EXPECT_EQ(this->EvaluateFunction("TRIM", "abc").ValueString(), "abc");

  EXPECT_THROW(this->EvaluateFunction("LTRIM", "x", "y"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("RTRIM", "x", "y"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("TRIM", "x", "y"), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Reverse) {
  EXPECT_TRUE(this->EvaluateFunction("REVERSE", TypedValue()).IsNull());
  EXPECT_EQ(this->EvaluateFunction("REVERSE", "abc").ValueString(), "cba");
  EXPECT_THROW(this->EvaluateFunction("REVERSE", "x", "y"), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Replace) {
  EXPECT_THROW(this->EvaluateFunction("REPLACE"), QueryRuntimeException);
  EXPECT_TRUE(this->EvaluateFunction("REPLACE", TypedValue(), "l", "w").IsNull());
  EXPECT_TRUE(this->EvaluateFunction("REPLACE", "hello", TypedValue(), "w").IsNull());
  EXPECT_TRUE(this->EvaluateFunction("REPLACE", "hello", "l", TypedValue()).IsNull());
  EXPECT_EQ(this->EvaluateFunction("REPLACE", "hello", "l", "w").ValueString(), "hewwo");

  EXPECT_THROW(this->EvaluateFunction("REPLACE", 1, "l", "w"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("REPLACE", "hello", 1, "w"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("REPLACE", "hello", "l", 1), QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Split) {
  EXPECT_THROW(this->EvaluateFunction("SPLIT"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("SPLIT", "one,two", 1), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("SPLIT", 1, "one,two"), QueryRuntimeException);

  EXPECT_TRUE(this->EvaluateFunction("SPLIT", TypedValue(), TypedValue()).IsNull());
  EXPECT_TRUE(this->EvaluateFunction("SPLIT", "one,two", TypedValue()).IsNull());
  EXPECT_TRUE(this->EvaluateFunction("SPLIT", TypedValue(), ",").IsNull());

  auto result = this->EvaluateFunction("SPLIT", "one,two", ",");
  EXPECT_TRUE(result.IsList());
  EXPECT_EQ(result.ValueList()[0].ValueString(), "one");
  EXPECT_EQ(result.ValueList()[1].ValueString(), "two");
}

TYPED_TEST(FunctionTest, Substring) {
  EXPECT_THROW(this->EvaluateFunction("SUBSTRING"), QueryRuntimeException);

  EXPECT_TRUE(this->EvaluateFunction("SUBSTRING", TypedValue(), 0, 10).IsNull());
  EXPECT_THROW(this->EvaluateFunction("SUBSTRING", TypedValue(), TypedValue()), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("SUBSTRING", TypedValue(), -10), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("SUBSTRING", TypedValue(), 0, TypedValue()), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("SUBSTRING", TypedValue(), 0, -10), QueryRuntimeException);

  EXPECT_EQ(this->EvaluateFunction("SUBSTRING", "hello", 2).ValueString(), "llo");
  EXPECT_EQ(this->EvaluateFunction("SUBSTRING", "hello", 10).ValueString(), "");
  EXPECT_EQ(this->EvaluateFunction("SUBSTRING", "hello", 2, 0).ValueString(), "");
  EXPECT_EQ(this->EvaluateFunction("SUBSTRING", "hello", 1, 3).ValueString(), "ell");
  EXPECT_EQ(this->EvaluateFunction("SUBSTRING", "hello", 1, 4).ValueString(), "ello");
  EXPECT_EQ(this->EvaluateFunction("SUBSTRING", "hello", 1, 10).ValueString(), "ello");
}

TYPED_TEST(FunctionTest, ToLower) {
  EXPECT_THROW(this->EvaluateFunction("TOLOWER"), QueryRuntimeException);
  EXPECT_TRUE(this->EvaluateFunction("TOLOWER", TypedValue()).IsNull());
  EXPECT_EQ(this->EvaluateFunction("TOLOWER", "Ab__C").ValueString(), "ab__c");
}

TYPED_TEST(FunctionTest, ToUpper) {
  EXPECT_THROW(this->EvaluateFunction("TOUPPER"), QueryRuntimeException);
  EXPECT_TRUE(this->EvaluateFunction("TOUPPER", TypedValue()).IsNull());
  EXPECT_EQ(this->EvaluateFunction("TOUPPER", "Ab__C").ValueString(), "AB__C");
}

TYPED_TEST(FunctionTest, ToByteString) {
  EXPECT_THROW(this->EvaluateFunction("TOBYTESTRING"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("TOBYTESTRING", 42), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("TOBYTESTRING", TypedValue()), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("TOBYTESTRING", "", 42), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("TOBYTESTRING", "ff"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("TOBYTESTRING", "00"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("TOBYTESTRING", "0xG"), QueryRuntimeException);
  EXPECT_EQ(this->EvaluateFunction("TOBYTESTRING", "").ValueString(), "");
  EXPECT_EQ(this->EvaluateFunction("TOBYTESTRING", "0x").ValueString(), "");
  EXPECT_EQ(this->EvaluateFunction("TOBYTESTRING", "0X").ValueString(), "");
  EXPECT_EQ(this->EvaluateFunction("TOBYTESTRING", "0x0123456789aAbBcCdDeEfF").ValueString(),
            "\x01\x23\x45\x67\x89\xAA\xBB\xCC\xDD\xEE\xFF");
  EXPECT_EQ(this->EvaluateFunction("TOBYTESTRING", "0x042").ValueString().size(), 2);
  EXPECT_EQ(this->EvaluateFunction("TOBYTESTRING", "0x042").ValueString(),
            memgraph::utils::pmr::string("\x00\x42", 2, memgraph::utils::NewDeleteResource()));
}

TYPED_TEST(FunctionTest, FromByteString) {
  EXPECT_THROW(this->EvaluateFunction("FROMBYTESTRING"), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("FROMBYTESTRING", 42), QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("FROMBYTESTRING", TypedValue()), QueryRuntimeException);
  EXPECT_EQ(this->EvaluateFunction("FROMBYTESTRING", "").ValueString(), "");
  auto bytestring = this->EvaluateFunction("TOBYTESTRING", "0x123456789aAbBcCdDeEfF");
  EXPECT_EQ(this->EvaluateFunction("FROMBYTESTRING", bytestring).ValueString(), "0x0123456789aabbccddeeff");
  EXPECT_EQ(this->EvaluateFunction("FROMBYTESTRING", std::string("\x00\x42", 2)).ValueString(), "0x0042");
}

TYPED_TEST(FunctionTest, Date) {
  const auto unix_epoch = memgraph::utils::Date({1970, 1, 1});
  EXPECT_EQ(this->EvaluateFunction("DATE", "1970-01-01").ValueDate(), unix_epoch);
  const auto map_param = TypedValue(
      std::map<std::string, TypedValue>{{"year", TypedValue(1970)}, {"month", TypedValue(1)}, {"day", TypedValue(1)}});
  EXPECT_EQ(this->EvaluateFunction("DATE", map_param).ValueDate(), unix_epoch);
  const auto today = memgraph::utils::CurrentDate();
  EXPECT_EQ(this->EvaluateFunction("DATE").ValueDate(), today);

  EXPECT_THROW(this->EvaluateFunction("DATE", "{}"), memgraph::utils::BasicException);
  EXPECT_THROW(this->EvaluateFunction("DATE", std::map<std::string, TypedValue>{{"years", TypedValue(1970)}}),
               QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("DATE", std::map<std::string, TypedValue>{{"mnths", TypedValue(1970)}}),
               QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("DATE", std::map<std::string, TypedValue>{{"dayz", TypedValue(1970)}}),
               QueryRuntimeException);
}

TYPED_TEST(FunctionTest, LocalTime) {
  const auto local_time = memgraph::utils::LocalTime({13, 3, 2, 0, 0});
  EXPECT_EQ(this->EvaluateFunction("LOCALTIME", "130302").ValueLocalTime(), local_time);
  const auto one_sec_in_microseconds = 1000000;
  const auto map_param = TypedValue(std::map<std::string, TypedValue>{{"hour", TypedValue(1)},
                                                                      {"minute", TypedValue(2)},
                                                                      {"second", TypedValue(3)},
                                                                      {"millisecond", TypedValue(4)},
                                                                      {"microsecond", TypedValue(5)}});
  EXPECT_EQ(this->EvaluateFunction("LOCALTIME", map_param).ValueLocalTime(),
            memgraph::utils::LocalTime({1, 2, 3, 4, 5}));
  const auto today = memgraph::utils::CurrentLocalTime();
  EXPECT_NEAR(this->EvaluateFunction("LOCALTIME").ValueLocalTime().MicrosecondsSinceEpoch(),
              today.MicrosecondsSinceEpoch(), one_sec_in_microseconds);

  EXPECT_THROW(this->EvaluateFunction("LOCALTIME", "{}"), memgraph::utils::BasicException);
  EXPECT_THROW(
      this->EvaluateFunction("LOCALTIME", TypedValue(std::map<std::string, TypedValue>{{"hous", TypedValue(1970)}})),
      QueryRuntimeException);
  EXPECT_THROW(
      this->EvaluateFunction("LOCALTIME", TypedValue(std::map<std::string, TypedValue>{{"minut", TypedValue(1970)}})),
      QueryRuntimeException);
  EXPECT_THROW(
      this->EvaluateFunction("LOCALTIME", TypedValue(std::map<std::string, TypedValue>{{"seconds", TypedValue(1970)}})),
      QueryRuntimeException);
}

TYPED_TEST(FunctionTest, LocalDateTime) {
  const auto local_date_time = memgraph::utils::LocalDateTime({1970, 1, 1}, {13, 3, 2, 0, 0});
  EXPECT_EQ(this->EvaluateFunction("LOCALDATETIME", "1970-01-01T13:03:02").ValueLocalDateTime(), local_date_time);
  const auto today = memgraph::utils::CurrentLocalDateTime();
  const auto one_sec_in_microseconds = 1000000;
  const auto map_param = TypedValue(std::map<std::string, TypedValue>{{"year", TypedValue(1972)},
                                                                      {"month", TypedValue(2)},
                                                                      {"day", TypedValue(3)},
                                                                      {"hour", TypedValue(4)},
                                                                      {"minute", TypedValue(5)},
                                                                      {"second", TypedValue(6)},
                                                                      {"millisecond", TypedValue(7)},
                                                                      {"microsecond", TypedValue(8)}});

  EXPECT_EQ(this->EvaluateFunction("LOCALDATETIME", map_param).ValueLocalDateTime(),
            memgraph::utils::LocalDateTime({1972, 2, 3}, {4, 5, 6, 7, 8}));
  EXPECT_NEAR(this->EvaluateFunction("LOCALDATETIME").ValueLocalDateTime().MicrosecondsSinceEpoch(),
              today.MicrosecondsSinceEpoch(), one_sec_in_microseconds);
  EXPECT_THROW(this->EvaluateFunction("LOCALDATETIME", "{}"), memgraph::utils::BasicException);
  EXPECT_THROW(this->EvaluateFunction("LOCALDATETIME",
                                      TypedValue(std::map<std::string, TypedValue>{{"hours", TypedValue(1970)}})),
               QueryRuntimeException);
  EXPECT_THROW(this->EvaluateFunction("LOCALDATETIME",
                                      TypedValue(std::map<std::string, TypedValue>{{"seconds", TypedValue(1970)}})),
               QueryRuntimeException);
}

TYPED_TEST(FunctionTest, Duration) {
  const auto map_param = TypedValue(std::map<std::string, TypedValue>{{"day", TypedValue(3)},
                                                                      {"hour", TypedValue(4)},
                                                                      {"minute", TypedValue(5)},
                                                                      {"second", TypedValue(6)},
                                                                      {"millisecond", TypedValue(7)},
                                                                      {"microsecond", TypedValue(8)}});

  EXPECT_EQ(this->EvaluateFunction("DURATION", map_param).ValueDuration(),
            memgraph::utils::Duration({3, 4, 5, 6, 7, 8}));
  EXPECT_THROW(this->EvaluateFunction("DURATION", "{}"), memgraph::utils::BasicException);
  EXPECT_THROW(
      this->EvaluateFunction("DURATION", TypedValue(std::map<std::string, TypedValue>{{"hours", TypedValue(1970)}})),
      QueryRuntimeException);
  EXPECT_THROW(
      this->EvaluateFunction("DURATION", TypedValue(std::map<std::string, TypedValue>{{"seconds", TypedValue(1970)}})),
      QueryRuntimeException);

  const auto map_param_negative = TypedValue(std::map<std::string, TypedValue>{{"day", TypedValue(-3)},
                                                                               {"hour", TypedValue(-4)},
                                                                               {"minute", TypedValue(-5)},
                                                                               {"second", TypedValue(-6)},
                                                                               {"millisecond", TypedValue(-7)},
                                                                               {"microsecond", TypedValue(-8)}});
  EXPECT_EQ(this->EvaluateFunction("DURATION", map_param_negative).ValueDuration(),
            memgraph::utils::Duration({-3, -4, -5, -6, -7, -8}));

  EXPECT_EQ(this->EvaluateFunction("DURATION", "P4DT4H5M6.2S").ValueDuration(),
            memgraph::utils::Duration({4, 4, 5, 6, 0, 200000}));
  EXPECT_EQ(this->EvaluateFunction("DURATION", "P3DT4H5M6.100S").ValueDuration(),
            memgraph::utils::Duration({3, 4, 5, 6, 0, 100000}));
  EXPECT_EQ(this->EvaluateFunction("DURATION", "P3DT4H5M6.100110S").ValueDuration(),
            memgraph::utils::Duration({3, 4, 5, 6, 100, 110}));
}

TYPED_TEST(FunctionTest, ZonedDateTime) {
  const auto date_parameters = memgraph::utils::DateParameters{2024, 6, 22};
  const auto local_time_parameters = memgraph::utils::LocalTimeParameters{12, 6, 3, 0, 0};
  const auto zdt = memgraph::utils::ZonedDateTime(
      {date_parameters, local_time_parameters, memgraph::utils::Timezone("America/Los_Angeles")});
  EXPECT_EQ(this->EvaluateFunction("DATETIME", "2024-06-22T12:06:03[America/Los_Angeles]").ValueZonedDateTime(), zdt);

  const auto map_param = TypedValue(std::map<std::string, TypedValue>{{"year", TypedValue(2024)},
                                                                      {"month", TypedValue(6)},
                                                                      {"day", TypedValue(22)},
                                                                      {"hour", TypedValue(12)},
                                                                      {"minute", TypedValue(6)},
                                                                      {"second", TypedValue(3)},
                                                                      {"millisecond", TypedValue(0)},
                                                                      {"microsecond", TypedValue(0)},
                                                                      {"timezone", TypedValue("America/Los_Angeles")}});
  EXPECT_EQ(this->EvaluateFunction("DATETIME", map_param).ValueZonedDateTime(), zdt);

  const auto one_sec_in_microseconds = 1000000;
  const auto today = memgraph::utils::CurrentZonedDateTime();
  EXPECT_NEAR(this->EvaluateFunction("DATETIME").ValueZonedDateTime().SysMicrosecondsSinceEpoch(),
              today.SysMicrosecondsSinceEpoch(), one_sec_in_microseconds);
  EXPECT_EQ(this->EvaluateFunction("DATETIME", TypedValue(std::map<std::string, TypedValue>{})).ValueZonedDateTime(),
            memgraph::utils::ZonedDateTime({{}, {}, memgraph::utils::DefaultTimezone()}));

  EXPECT_THROW(this->EvaluateFunction("DATETIME", "{}"), memgraph::utils::BasicException);
  EXPECT_THROW(
      this->EvaluateFunction("DATETIME", TypedValue(std::map<std::string, TypedValue>{{"hours", TypedValue(1970)}})),
      QueryRuntimeException);
  EXPECT_THROW(
      this->EvaluateFunction("DATETIME", TypedValue(std::map<std::string, TypedValue>{{"seconds", TypedValue(1970)}})),
      QueryRuntimeException);
}
}  // namespace
