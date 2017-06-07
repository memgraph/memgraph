//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 14.03.17.
//

#include <algorithm>
#include <iterator>
#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "dbms/dbms.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/plan/operator.hpp"

#include "query_plan_common.hpp"

using namespace query;
using namespace query::plan;

TEST(QueryPlan, Skip) {
  Dbms dbms;
  auto dba = dbms.active();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n1");
  auto skip = std::make_shared<plan::Skip>(n.op_, LITERAL(2));

  EXPECT_EQ(0, PullAll(skip, *dba, symbol_table));

  dba->insert_vertex();
  dba->advance_command();
  EXPECT_EQ(0, PullAll(skip, *dba, symbol_table));

  dba->insert_vertex();
  dba->advance_command();
  EXPECT_EQ(0, PullAll(skip, *dba, symbol_table));

  dba->insert_vertex();
  dba->advance_command();
  EXPECT_EQ(1, PullAll(skip, *dba, symbol_table));

  for (int i = 0; i < 10; ++i) dba->insert_vertex();
  dba->advance_command();
  EXPECT_EQ(11, PullAll(skip, *dba, symbol_table));
}

TEST(QueryPlan, Limit) {
  Dbms dbms;
  auto dba = dbms.active();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n1");
  auto skip = std::make_shared<plan::Limit>(n.op_, LITERAL(2));

  EXPECT_EQ(0, PullAll(skip, *dba, symbol_table));

  dba->insert_vertex();
  dba->advance_command();
  EXPECT_EQ(1, PullAll(skip, *dba, symbol_table));

  dba->insert_vertex();
  dba->advance_command();
  EXPECT_EQ(2, PullAll(skip, *dba, symbol_table));

  dba->insert_vertex();
  dba->advance_command();
  EXPECT_EQ(2, PullAll(skip, *dba, symbol_table));

  for (int i = 0; i < 10; ++i) dba->insert_vertex();
  dba->advance_command();
  EXPECT_EQ(2, PullAll(skip, *dba, symbol_table));
}

TEST(QueryPlan, CreateLimit) {
  // CREATE (n), (m)
  // MATCH (n) CREATE (m) LIMIT 1
  // in the end we need to have 3 vertices in the db
  Dbms dbms;
  auto dba = dbms.active();
  dba->insert_vertex();
  dba->insert_vertex();
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n1");
  auto m = NODE("m");
  symbol_table[*m->identifier_] = symbol_table.CreateSymbol("m", true);
  auto c = std::make_shared<CreateNode>(m, n.op_);
  auto skip = std::make_shared<plan::Limit>(c, LITERAL(1));

  EXPECT_EQ(1, PullAll(skip, *dba, symbol_table));
  dba->advance_command();
  EXPECT_EQ(3, CountIterable(dba->vertices(false)));
}

TEST(QueryPlan, OrderBy) {
  Dbms dbms;
  auto dba = dbms.active();
  AstTreeStorage storage;
  SymbolTable symbol_table;
  auto prop = dba->property("prop");

  // contains a series of tests
  // each test defines the ordering a vector of values in the desired order
  auto Null = TypedValue::Null;
  std::vector<std::pair<Ordering, std::vector<TypedValue>>> orderable{
      {Ordering::ASC, {0, 0, 0.5, 1, 2, 12.6, 42, Null, Null}},
      {Ordering::ASC, {false, false, true, true, Null, Null}},
      {Ordering::ASC, {"A", "B", "a", "a", "aa", "ab", "aba", Null, Null}},
      {Ordering::DESC, {Null, Null, 33, 33, 32.5, 32, 2.2, 2.1, 0}},
      {Ordering::DESC, {Null, true, false}},
      {Ordering::DESC, {Null, "zorro", "borro"}}};

  for (const auto &order_value_pair : orderable) {
    const auto &values = order_value_pair.second;
    // empty database
    for (auto &vertex : dba->vertices(false)) dba->detach_remove_vertex(vertex);
    dba->advance_command();
    ASSERT_EQ(0, CountIterable(dba->vertices(false)));

    // take some effort to shuffle the values
    // because we are testing that something not ordered gets ordered
    // and need to take care it does not happen by accident
    std::vector<TypedValue> shuffled(values.begin(), values.end());
    auto order_equal = [&values, &shuffled]() {
      return std::equal(values.begin(), values.end(), shuffled.begin(),
                        TypedValue::BoolEqual{});
    };
    for (int i = 0; i < 50 && order_equal(); ++i) {
      std::random_shuffle(shuffled.begin(), shuffled.end());
    }
    ASSERT_FALSE(order_equal());

    // create the vertices
    for (const auto &value : shuffled)
      dba->insert_vertex().PropsSet(prop, value);
    dba->advance_command();

    // order by and collect results
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_p = PROPERTY_LOOKUP("n", prop);
    symbol_table[*n_p->expression_] = n.sym_;
    auto order_by = std::make_shared<plan::OrderBy>(
        n.op_,
        std::vector<std::pair<Ordering, Expression *>>{
            {order_value_pair.first, n_p}},
        std::vector<Symbol>{n.sym_});
    auto n_p_ne = NEXPR("n.p", n_p);
    symbol_table[*n_p_ne] = symbol_table.CreateSymbol("n.p", true);
    auto produce = MakeProduce(order_by, n_p_ne);
    auto results = CollectProduce(produce.get(), symbol_table, *dba);
    ASSERT_EQ(values.size(), results.size());
    for (int j = 0; j < results.size(); ++j)
      EXPECT_TRUE(TypedValue::BoolEqual{}(results[j][0], values[j]));
  }
}

TEST(QueryPlan, OrderByMultiple) {
  Dbms dbms;
  auto dba = dbms.active();
  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto p1 = dba->property("p1");
  auto p2 = dba->property("p2");

  // create a bunch of vertices that in two properties
  // have all the variations (with repetition) of N values.
  // ensure that those vertices are not created in the
  // "right" sequence, but randomized
  const int N = 20;
  std::vector<std::pair<int, int>> prop_values;
  for (int i = 0; i < N * N; ++i) prop_values.emplace_back(i % N, i / N);
  std::random_shuffle(prop_values.begin(), prop_values.end());
  for (const auto &pair : prop_values) {
    auto v = dba->insert_vertex();
    v.PropsSet(p1, pair.first);
    v.PropsSet(p2, pair.second);
  }
  dba->advance_command();

  // order by and collect results
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_p1 = PROPERTY_LOOKUP("n", p1);
  symbol_table[*n_p1->expression_] = n.sym_;
  auto n_p2 = PROPERTY_LOOKUP("n", p2);
  symbol_table[*n_p2->expression_] = n.sym_;
  // order the results so we get
  // (p1: 0, p2: N-1)
  // (p1: 0, p2: N-2)
  // ...
  // (p1: N-1, p2:0)
  auto order_by = std::make_shared<plan::OrderBy>(
      n.op_,
      std::vector<std::pair<Ordering, Expression *>>{
          {Ordering::ASC, n_p1}, {Ordering::DESC, n_p2},
      },
      std::vector<Symbol>{n.sym_});
  auto n_p1_ne = NEXPR("n.p1", n_p1);
  symbol_table[*n_p1_ne] = symbol_table.CreateSymbol("n.p1", true);
  auto n_p2_ne = NEXPR("n.p2", n_p2);
  symbol_table[*n_p2_ne] = symbol_table.CreateSymbol("n.p2", true);
  auto produce = MakeProduce(order_by, n_p1_ne, n_p2_ne);
  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  ASSERT_EQ(N * N, results.size());
  for (int j = 0; j < N * N; ++j) {
    ASSERT_EQ(results[j][0].type(), TypedValue::Type::Int);
    EXPECT_EQ(results[j][0].Value<int64_t>(), j / N);
    ASSERT_EQ(results[j][1].type(), TypedValue::Type::Int);
    EXPECT_EQ(results[j][1].Value<int64_t>(), N - 1 - j % N);
  }
}

TEST(QueryPlan, OrderByExceptions) {
  Dbms dbms;
  auto dba = dbms.active();
  AstTreeStorage storage;
  SymbolTable symbol_table;
  auto prop = dba->property("prop");

  // a vector of pairs of typed values that should result
  // in an exception when trying to order on them
  std::vector<std::pair<TypedValue, TypedValue>> exception_pairs{
      {42, true},
      {42, "bla"},
      {42, std::vector<TypedValue>{42}},
      {true, "bla"},
      {true, std::vector<TypedValue>{true}},
      {"bla", std::vector<TypedValue>{"bla"}},
      // illegal comparisons of same-type values
      {std::vector<TypedValue>{42}, std::vector<TypedValue>{42}}};

  for (const auto &pair : exception_pairs) {
    // empty database
    for (auto &vertex : dba->vertices(false)) dba->detach_remove_vertex(vertex);
    dba->advance_command();
    ASSERT_EQ(0, CountIterable(dba->vertices(false)));

    // make two vertices, and set values
    dba->insert_vertex().PropsSet(prop, pair.first);
    dba->insert_vertex().PropsSet(prop, pair.second);
    dba->advance_command();
    ASSERT_EQ(2, CountIterable(dba->vertices(false)));
    for (const auto &va : dba->vertices(false))
      ASSERT_NE(va.PropsAt(prop).type(), PropertyValue::Type::Null);

    // order by and expect an exception
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_p = PROPERTY_LOOKUP("n", prop);
    symbol_table[*n_p->expression_] = n.sym_;
    auto order_by = std::make_shared<plan::OrderBy>(
        n.op_,
        std::vector<std::pair<Ordering, Expression *>>{{Ordering::ASC, n_p}},
        std::vector<Symbol>{});
    EXPECT_THROW(PullAll(order_by, *dba, symbol_table), QueryRuntimeException);
  }
}
