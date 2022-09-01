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
#include <random>
#include <set>
#include <vector>

#include "common/types.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "io/simulator/simulator.hpp"
#include "query/v2/context.hpp"
#include "query/v2/exceptions.hpp"
#include "query/v2/plan/operator_distributed.hpp"
#include "query_v2_query_plan_common.hpp"
#include "storage/v3/property_value.hpp"
#include "storage/v3/schemas.hpp"

#include <random>

#include <chrono>

using namespace memgraph::query::v2;
using namespace memgraph::query::v2::plan;
using test_common::ToIntList;
using test_common::ToIntMap;
using testing::UnorderedElementsAre;

namespace memgraph::query::v2::tests {

class QueryPlanHardCodedQueriesTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_TRUE(
        db_v3.CreateSchema(schema_label, {storage::v3::SchemaProperty{schema_property, common::SchemaType::INT}}));
  }

  storage::v3::Storage db_v3;
  const storage::v3::LabelId schema_label{db_v3.NameToLabel("label")};
  const storage::v3::PropertyId schema_property{db_v3.NameToProperty("property")};
};

TEST_F(QueryPlanHardCodedQueriesTest, ScallAllScanAllScanAllWhileBatching_multiframe) {
  /*
    QUERY:
      MATCH (n)
      MATCH (p)
      MATCH (q)
      RETURN n,p,q;

    QUERY PLAN:
      Produce {n, p, q}
      ScanAll (n)
      ScanAll (p)
      ScanAll (q)
      Once
  */
  auto tuples_gids_of_expected_vertices = std::set<std::tuple<storage::v3::Gid, storage::v3::Gid, storage::v3::Gid>>{};
  auto gid_of_expected_vertices = std::set<storage::v3::Gid>{};

  const auto number_of_vertices = 200;
  const auto number_of_frames_per_batch = 20;
  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto vertex_node = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});
      auto [it, inserted] = gid_of_expected_vertices.insert(vertex_node.Gid());
      ASSERT_TRUE(inserted);
    }

    // Generate the expected result from the double ScanAll
    for (auto &outer_gid : gid_of_expected_vertices) {
      for (auto &middle_gid : gid_of_expected_vertices) {
        for (auto &inner_gid : gid_of_expected_vertices) {
          auto [it, inserted] =
              tuples_gids_of_expected_vertices.insert(std::make_tuple(outer_gid, middle_gid, inner_gid));
          ASSERT_TRUE(inserted);
        }
      }
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // MATCH (q)
  auto scan_all_1 = MakeScanAllDistributed(storage, symbol_table, "q");
  // MATCH (p)
  auto scan_all_2 = MakeScanAllDistributed(storage, symbol_table, "p", scan_all_1.op_);
  // MATCH (n)
  auto scan_all_3 = MakeScanAllDistributed(storage, symbol_table, "n", scan_all_2.op_);

  auto output_q =
      NEXPR("q", IDENT("q")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_q", true));
  auto output_p =
      NEXPR("p", IDENT("p")->MapTo(scan_all_2.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_p", true));
  auto output_n =
      NEXPR("n", IDENT("n")->MapTo(scan_all_3.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_n", true));

  auto produce = MakeProduceDistributed(scan_all_3.op_, output_n, output_p, output_q);
  auto context = MakeContextDistributed(storage, symbol_table, &dba);

  // Collecting results
  memgraph::query::v2::MultiFrame multiframe(&context, number_of_frames_per_batch);

  // top level node in the operator tree is a produce (return)
  // so stream out results

  // collect the symbols from the return clause
  std::vector<Symbol> symbols;
  for (auto named_expression : produce->named_expressions_) {
    symbols.emplace_back(context.symbol_table.at(*named_expression));
  }

  // stream out results
  auto cursor = produce->MakeCursor(memgraph::utils::NewDeleteResource());
  std::vector<std::vector<TypedValue>> results;
  // START COUNTING
  auto start = std::chrono::steady_clock::now();
  while (cursor->Pull(multiframe, context)) {
    for (auto *frame : multiframe.GetValidFrames()) {
      auto is_ok = true;
      std::vector<TypedValue> values;

      for (auto &symbol : symbols) {
        values.emplace_back((*frame)[symbol]);
      }

      if (is_ok) {
        results.emplace_back(values);
      }
    }

    multiframe.ResetAll();
  }
  auto end = std::chrono::steady_clock::now();
  // STOP COUNTING
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  std::cout << "MULTIFRAME: DURATION OF PULL: " << duration << " ms" << std::endl;

  // End of collect result

  ASSERT_EQ(results.size(), tuples_gids_of_expected_vertices.size());
}

TEST_F(QueryPlanHardCodedQueriesTest, ScallAllScanAllScanAllWhileBatching_singleframe) {
  /*
    QUERY:
      MATCH (n)
      MATCH (p)
      MATCH (q)
      RETURN n,p,q;

    QUERY PLAN:
      Produce {n, p, q}
      ScanAll (n)
      ScanAll (p)
      ScanAll (q)
      Once
  */
  auto tuples_gids_of_expected_vertices = std::set<std::tuple<storage::v3::Gid, storage::v3::Gid, storage::v3::Gid>>{};
  auto gid_of_expected_vertices = std::set<storage::v3::Gid>{};

  const auto number_of_vertices = 200;
  {  // Inserting data
    auto storage_dba = db_v3.Access();
    DbAccessor dba(&storage_dba);

    auto property_index = 0;
    for (auto idx = 0; idx < number_of_vertices; ++idx) {
      auto vertex_node = *dba.InsertVertexAndValidate(
          schema_label, {}, {{schema_property, storage::v3::PropertyValue(++property_index)}});
      auto [it, inserted] = gid_of_expected_vertices.insert(vertex_node.Gid());
      ASSERT_TRUE(inserted);
    }

    // Generate the expected result from the double ScanAll
    for (auto &outer_gid : gid_of_expected_vertices) {
      for (auto &middle_gid : gid_of_expected_vertices) {
        for (auto &inner_gid : gid_of_expected_vertices) {
          auto [it, inserted] =
              tuples_gids_of_expected_vertices.insert(std::make_tuple(outer_gid, middle_gid, inner_gid));
          ASSERT_TRUE(inserted);
        }
      }
    }

    ASSERT_FALSE(dba.Commit().HasError());
  }

  auto storage_dba = db_v3.Access();
  DbAccessor dba(&storage_dba);
  AstStorage storage;
  SymbolTable symbol_table;

  // MATCH (q)
  auto scan_all_1 = MakeScanAll(storage, symbol_table, "q");
  // MATCH (p)
  auto scan_all_2 = MakeScanAll(storage, symbol_table, "p", scan_all_1.op_);
  // MATCH (n)
  auto scan_all_3 = MakeScanAll(storage, symbol_table, "n", scan_all_2.op_);

  auto output_q =
      NEXPR("q", IDENT("q")->MapTo(scan_all_1.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_q", true));
  auto output_p =
      NEXPR("p", IDENT("p")->MapTo(scan_all_2.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_p", true));
  auto output_n =
      NEXPR("n", IDENT("n")->MapTo(scan_all_3.sym_))->MapTo(symbol_table.CreateSymbol("named_expression_n", true));

  auto produce = MakeProduce(scan_all_3.op_, output_n, output_p, output_q);
  auto context = MakeContext(storage, symbol_table, &dba);

  // Collecting results
  Frame frame(context.symbol_table.max_position());

  // top level node in the operator tree is a produce (return)
  // so stream out results

  // collect the symbols from the return clause
  std::vector<Symbol> symbols;
  for (auto named_expression : produce->named_expressions_)
    symbols.emplace_back(context.symbol_table.at(*named_expression));

  // stream out results
  auto cursor = produce->MakeCursor(memgraph::utils::NewDeleteResource());
  std::vector<std::vector<TypedValue>> results;
  // START COUNTING
  auto start = std::chrono::steady_clock::now();
  while (cursor->Pull(frame, context)) {
    std::vector<TypedValue> values;
    for (auto &symbol : symbols) values.emplace_back(frame[symbol]);
    results.emplace_back(values);
  }

  auto end = std::chrono::steady_clock::now();
  // STOP COUNTING
  const auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  std::cout << "SINGLE FRAME: DURATION OF PULL: " << duration << " ms" << std::endl;

  // End of collect result
  ASSERT_EQ(results.size(), tuples_gids_of_expected_vertices.size());
}

}  // namespace memgraph::query::v2::tests
