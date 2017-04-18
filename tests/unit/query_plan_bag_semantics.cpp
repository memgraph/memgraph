//
// Copyright 2017 Memgraph
// Created by Florijan Stamenkovic on 14.03.17.
//

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

  for (int i = 0; i < 10; ++i)
    dba->insert_vertex();
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

  for (int i = 0; i < 10; ++i)
    dba->insert_vertex();
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
  symbol_table[*m->identifier_] = symbol_table.CreateSymbol("m");
  auto c = std::make_shared<CreateNode>(m, n.op_);
  auto skip = std::make_shared<plan::Limit>(c, LITERAL(1));

  EXPECT_EQ(1, PullAll(skip, *dba, symbol_table));
  dba->advance_command();
  EXPECT_EQ(3, CountIterable(dba->vertices()));
}
