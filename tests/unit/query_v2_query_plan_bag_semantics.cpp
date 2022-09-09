// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <algorithm>
#include <iterator>
#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "query/v2/context.hpp"
#include "query/v2/exceptions.hpp"
#include "query/v2/frontend/ast/ast.hpp"
#include "query/v2/plan/operator.hpp"

#include "query_v2_query_plan_common.hpp"
#include "storage/v3/conversions.hpp"
#include "storage/v3/property_value.hpp"

using namespace memgraph::query::v2;
using namespace memgraph::query::v2::plan;

namespace memgraph::query::v2::tests {

class QueryPlanBagSemanticsTest : public testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(db.CreateSchema(label, {storage::v3::SchemaProperty{property, common::SchemaType::INT}}));
  }

  storage::v3::Storage db;
  const storage::v3::LabelId label{db.NameToLabel("label")};
  const storage::v3::PropertyId property{db.NameToProperty("property")};
};

TEST_F(QueryPlanBagSemanticsTest, Skip) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n1");
  auto skip = std::make_shared<plan::Skip>(n.op_, LITERAL(2));

  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(0, PullAll(*skip, &context));

  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(1)}}).HasValue());
  dba.AdvanceCommand();
  EXPECT_EQ(0, PullAll(*skip, &context));

  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(2)}}).HasValue());
  dba.AdvanceCommand();
  EXPECT_EQ(0, PullAll(*skip, &context));

  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(3)}}).HasValue());
  dba.AdvanceCommand();
  EXPECT_EQ(1, PullAll(*skip, &context));

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(i + 3)}}).HasValue());
  }
  dba.AdvanceCommand();
  EXPECT_EQ(11, PullAll(*skip, &context));
}

TEST_F(QueryPlanBagSemanticsTest, Limit) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n1");
  auto skip = std::make_shared<plan::Limit>(n.op_, LITERAL(2));

  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(0, PullAll(*skip, &context));

  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(1)}}).HasValue());
  dba.AdvanceCommand();
  EXPECT_EQ(1, PullAll(*skip, &context));

  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(2)}}).HasValue());
  dba.AdvanceCommand();
  EXPECT_EQ(2, PullAll(*skip, &context));

  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(3)}}).HasValue());
  dba.AdvanceCommand();
  EXPECT_EQ(2, PullAll(*skip, &context));

  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(i + 3)}}).HasValue());
  }
  dba.AdvanceCommand();
  EXPECT_EQ(2, PullAll(*skip, &context));
}

TEST_F(QueryPlanBagSemanticsTest, CreateLimit) {
  // CREATE (n), (m)
  // MATCH (n) CREATE (m) LIMIT 1
  // in the end we need to have 3 vertices in the db
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);
  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(1)}}).HasValue());
  ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(2)}}).HasValue());
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n1");
  NodeCreationInfo m;
  m.symbol = symbol_table.CreateSymbol("m", true);
  m.labels = {label};
  std::get<std::vector<std::pair<storage::v3::PropertyId, Expression *>>>(m.properties)
      .emplace_back(property, LITERAL(3));
  auto c = std::make_shared<CreateNode>(n.op_, m);
  auto skip = std::make_shared<plan::Limit>(c, LITERAL(1));

  auto context = MakeContext(storage, symbol_table, &dba);
  EXPECT_EQ(1, PullAll(*skip, &context));
  dba.AdvanceCommand();
  EXPECT_EQ(3, CountIterable(dba.Vertices(storage::v3::View::OLD)));
}

TEST_F(QueryPlanBagSemanticsTest, OrderBy) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;
  auto prop = dba.NameToProperty("prop");

  // contains a series of tests
  // each test defines the ordering a vector of values in the desired order
  auto Null = storage::v3::PropertyValue();
  std::vector<std::pair<Ordering, std::vector<storage::v3::PropertyValue>>> orderable{
      {Ordering::ASC,
       {storage::v3::PropertyValue(0), storage::v3::PropertyValue(0), storage::v3::PropertyValue(0.5),
        storage::v3::PropertyValue(1), storage::v3::PropertyValue(2), storage::v3::PropertyValue(12.6),
        storage::v3::PropertyValue(42), Null, Null}},
      {Ordering::ASC,
       {storage::v3::PropertyValue(false), storage::v3::PropertyValue(false), storage::v3::PropertyValue(true),
        storage::v3::PropertyValue(true), Null, Null}},
      {Ordering::ASC,
       {storage::v3::PropertyValue("A"), storage::v3::PropertyValue("B"), storage::v3::PropertyValue("a"),
        storage::v3::PropertyValue("a"), storage::v3::PropertyValue("aa"), storage::v3::PropertyValue("ab"),
        storage::v3::PropertyValue("aba"), Null, Null}},
      {Ordering::DESC,
       {Null, Null, storage::v3::PropertyValue(33), storage::v3::PropertyValue(33), storage::v3::PropertyValue(32.5),
        storage::v3::PropertyValue(32), storage::v3::PropertyValue(2.2), storage::v3::PropertyValue(2.1),
        storage::v3::PropertyValue(0)}},
      {Ordering::DESC, {Null, storage::v3::PropertyValue(true), storage::v3::PropertyValue(false)}},
      {Ordering::DESC, {Null, storage::v3::PropertyValue("zorro"), storage::v3::PropertyValue("borro")}}};

  for (const auto &order_value_pair : orderable) {
    std::vector<TypedValue> values;
    values.reserve(order_value_pair.second.size());
    for (const auto &v : order_value_pair.second) values.emplace_back(storage::v3::PropertyToTypedValue<TypedValue>(v));
    // empty database
    for (auto vertex : dba.Vertices(storage::v3::View::OLD)) ASSERT_TRUE(dba.DetachRemoveVertex(&vertex).HasValue());
    dba.AdvanceCommand();
    ASSERT_EQ(0, CountIterable(dba.Vertices(storage::v3::View::OLD)));

    // take some effort to shuffle the values
    // because we are testing that something not ordered gets ordered
    // and need to take care it does not happen by accident
    auto shuffled = values;
    auto order_equal = [&values, &shuffled]() {
      return std::equal(values.begin(), values.end(), shuffled.begin(), TypedValue::BoolEqual{});
    };
    for (int i = 0; i < 50 && order_equal(); ++i) {
      std::random_shuffle(shuffled.begin(), shuffled.end());
    }
    ASSERT_FALSE(order_equal());

    // create the vertices
    for (const auto &value : shuffled) {
      ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(1)}})
                      ->SetProperty(prop, storage::v3::TypedToPropertyValue(value))
                      .HasValue());
    }
    dba.AdvanceCommand();

    // order by and collect results
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_p = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop);
    auto order_by = std::make_shared<plan::OrderBy>(n.op_, std::vector<SortItem>{{order_value_pair.first, n_p}},
                                                    std::vector<Symbol>{n.sym_});
    auto n_p_ne = NEXPR("n.p", n_p)->MapTo(symbol_table.CreateSymbol("n.p", true));
    auto produce = MakeProduce(order_by, n_p_ne);
    auto context = MakeContext(storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    ASSERT_EQ(values.size(), results.size());
    for (int j = 0; j < results.size(); ++j) EXPECT_TRUE(TypedValue::BoolEqual{}(results[j][0], values[j]));
  }
}

TEST_F(QueryPlanBagSemanticsTest, OrderByMultiple) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  auto p1 = dba.NameToProperty("p1");
  auto p2 = dba.NameToProperty("p2");

  // create a bunch of vertices that in two properties
  // have all the variations (with repetition) of N values.
  // ensure that those vertices are not created in the
  // "right" sequence, but randomized
  const int N = 20;
  std::vector<std::pair<int, int>> prop_values;
  for (int i = 0; i < N * N; ++i) prop_values.emplace_back(i % N, i / N);
  std::random_shuffle(prop_values.begin(), prop_values.end());
  for (const auto &pair : prop_values) {
    auto v = *dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(1)}});
    ASSERT_TRUE(v.SetProperty(p1, storage::v3::PropertyValue(pair.first)).HasValue());
    ASSERT_TRUE(v.SetProperty(p2, storage::v3::PropertyValue(pair.second)).HasValue());
  }
  dba.AdvanceCommand();

  // order by and collect results
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_p1 = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), p1);
  auto n_p2 = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), p2);
  // order the results so we get
  // (p1: 0, p2: N-1)
  // (p1: 0, p2: N-2)
  // ...
  // (p1: N-1, p2:0)
  auto order_by = std::make_shared<plan::OrderBy>(n.op_,
                                                  std::vector<SortItem>{
                                                      {Ordering::ASC, n_p1},
                                                      {Ordering::DESC, n_p2},
                                                  },
                                                  std::vector<Symbol>{n.sym_});
  auto n_p1_ne = NEXPR("n.p1", n_p1)->MapTo(symbol_table.CreateSymbol("n.p1", true));
  auto n_p2_ne = NEXPR("n.p2", n_p2)->MapTo(symbol_table.CreateSymbol("n.p2", true));
  auto produce = MakeProduce(order_by, n_p1_ne, n_p2_ne);
  auto context = MakeContext(storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(N * N, results.size());
  for (int j = 0; j < N * N; ++j) {
    ASSERT_EQ(results[j][0].type(), TypedValue::Type::Int);
    EXPECT_EQ(results[j][0].ValueInt(), j / N);
    ASSERT_EQ(results[j][1].type(), TypedValue::Type::Int);
    EXPECT_EQ(results[j][1].ValueInt(), N - 1 - j % N);
  }
}

TEST_F(QueryPlanBagSemanticsTest, OrderByExceptions) {
  auto storage_dba = db.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;
  auto prop = dba.NameToProperty("prop");

  // a vector of pairs of typed values that should result
  // in an exception when trying to order on them
  std::vector<std::pair<storage::v3::PropertyValue, storage::v3::PropertyValue>> exception_pairs{
      {storage::v3::PropertyValue(42), storage::v3::PropertyValue(true)},
      {storage::v3::PropertyValue(42), storage::v3::PropertyValue("bla")},
      {storage::v3::PropertyValue(42),
       storage::v3::PropertyValue(std::vector<storage::v3::PropertyValue>{storage::v3::PropertyValue(42)})},
      {storage::v3::PropertyValue(true), storage::v3::PropertyValue("bla")},
      {storage::v3::PropertyValue(true),
       storage::v3::PropertyValue(std::vector<storage::v3::PropertyValue>{storage::v3::PropertyValue(true)})},
      {storage::v3::PropertyValue("bla"),
       storage::v3::PropertyValue(std::vector<storage::v3::PropertyValue>{storage::v3::PropertyValue("bla")})},
      // illegal comparisons of same-type values
      {storage::v3::PropertyValue(std::vector<storage::v3::PropertyValue>{storage::v3::PropertyValue(42)}),
       storage::v3::PropertyValue(std::vector<storage::v3::PropertyValue>{storage::v3::PropertyValue(42)})}};

  for (const auto &pair : exception_pairs) {
    // empty database
    for (auto vertex : dba.Vertices(storage::v3::View::OLD)) ASSERT_TRUE(dba.DetachRemoveVertex(&vertex).HasValue());
    dba.AdvanceCommand();
    ASSERT_EQ(0, CountIterable(dba.Vertices(storage::v3::View::OLD)));

    // make two vertices, and set values
    ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(1)}})
                    ->SetProperty(prop, pair.first)
                    .HasValue());
    ASSERT_TRUE(dba.InsertVertexAndValidate(label, {}, {{property, storage::v3::PropertyValue(2)}})
                    ->SetProperty(prop, pair.second)
                    .HasValue());
    dba.AdvanceCommand();
    ASSERT_EQ(2, CountIterable(dba.Vertices(storage::v3::View::OLD)));
    for (const auto &va : dba.Vertices(storage::v3::View::OLD))
      ASSERT_NE(va.GetProperty(storage::v3::View::OLD, prop).GetValue().type(), storage::v3::PropertyValue::Type::Null);

    // order by and expect an exception
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_p = PROPERTY_LOOKUP(IDENT("n")->MapTo(n.sym_), prop);
    auto order_by =
        std::make_shared<plan::OrderBy>(n.op_, std::vector<SortItem>{{Ordering::ASC, n_p}}, std::vector<Symbol>{});
    auto context = MakeContext(storage, symbol_table, &dba);
    EXPECT_THROW(PullAll(*order_by, &context), QueryRuntimeException);
  }
}
}  // namespace memgraph::query::v2::tests
