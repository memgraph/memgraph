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

#include <algorithm>
#include <cmath>
#include <iterator>
#include <memory>
#include <vector>

#include "disk_test_utils.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "helpers/stub_property_fga_checker.hpp"

#include "license/license.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/plan/operator.hpp"
#include "query_plan_common.hpp"
#include "storage/v2/disk/storage.hpp"
#include "storage/v2/inmemory/storage.hpp"

using namespace memgraph::query;
using namespace memgraph::query::plan;
using memgraph::query::test_common::ToIntList;
using memgraph::query::test_common::ToIntMap;
using testing::UnorderedElementsAre;

template <typename StorageType>
class QueryPlanTest : public testing::Test {
 public:
  const std::string testSuite = "query_plan_accumulate_aggregate";
  memgraph::storage::Config config = disk_test_utils::GenerateOnDiskConfig(testSuite);
  std::unique_ptr<memgraph::storage::Storage> db = std::make_unique<StorageType>(config);
  AstStorage storage;

  void TearDown() override { CleanStorageDirs(); }

  void CleanStorageDirs() {
    if (std::is_same<StorageType, memgraph::storage::DiskStorage>::value) {
      disk_test_utils::RemoveRocksDbDirs(testSuite);
    }
  }

  std::shared_ptr<Produce> MakeAggregationProduce(std::shared_ptr<LogicalOperator> input, SymbolTable &symbol_table,
                                                  const std::vector<Expression *> aggr_inputs,
                                                  const std::vector<Aggregation::Op> aggr_ops,
                                                  const std::vector<Expression *> group_by_exprs,
                                                  const std::vector<Symbol> remember, const bool distinct) {
    // prepare all the aggregations
    std::vector<Aggregate::Element> aggregates;
    std::vector<NamedExpression *> named_expressions;

    auto aggr_inputs_it = aggr_inputs.begin();
    for (auto aggr_op : aggr_ops) {
      // TODO change this from using IDENT to using AGGREGATION
      // once AGGREGATION is handled properly in ExpressionEvaluation
      auto aggr_sym = symbol_table.CreateSymbol("aggregation", true);
      auto named_expr =
          NEXPR("", IDENT("aggregation")->MapTo(aggr_sym))->MapTo(symbol_table.CreateSymbol("named_expression", true));
      named_expressions.push_back(named_expr);
      // the key expression is only used in COLLECT_MAP
      Expression *key_expr_ptr = aggr_op == Aggregation::Op::COLLECT_MAP ? LITERAL("key") : nullptr;
      aggregates.emplace_back(Aggregate::Element{*aggr_inputs_it++, key_expr_ptr, aggr_op, aggr_sym, distinct});
    }

    // Produce will also evaluate group_by expressions and return them after the
    // aggregations.
    for (auto group_by_expr : group_by_exprs) {
      auto named_expr = NEXPR("", group_by_expr)->MapTo(symbol_table.CreateSymbol("named_expression", true));
      named_expressions.push_back(named_expr);
    }
    auto aggregation = std::make_shared<Aggregate>(input, aggregates, group_by_exprs, remember);
    return std::make_shared<Produce>(aggregation, named_expressions);
  }
};

using StorageTypes = ::testing::Types<memgraph::storage::InMemoryStorage, memgraph::storage::DiskStorage>;

TYPED_TEST_SUITE(QueryPlanTest, StorageTypes);

TYPED_TEST(QueryPlanTest, Accumulate) {
  // simulate the following two query execution on an empty db
  // CREATE ({x:0})-[:T]->({x:0})
  // MATCH (n)--(m) SET n.x = n.x + 1, m.x = m.x + 1 RETURN n.x, m.x
  // without accumulation we expected results to be [[1, 1], [2, 2]]
  // with accumulation we expect them to be [[2, 2], [2, 2]]

  auto check = [&](bool accumulate) {
    this->db.reset(nullptr);
    this->CleanStorageDirs();
    this->db = std::make_unique<TypeParam>(this->config);
    auto storage_dba = this->db->Access(memgraph::storage::WRITE);
    memgraph::query::DbAccessor dba(storage_dba.get());
    auto prop = dba.NameToProperty("x");

    auto v1 = dba.InsertVertex();
    ASSERT_TRUE(v1.SetProperty(prop, memgraph::storage::PropertyValue(0)).has_value());
    auto v2 = dba.InsertVertex();
    ASSERT_TRUE(v2.SetProperty(prop, memgraph::storage::PropertyValue(0)).has_value());
    ASSERT_TRUE(dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("T")).has_value());
    dba.AdvanceCommand();

    SymbolTable symbol_table;

    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto r_m = MakeExpand(this->storage,
                          symbol_table,
                          n.op_,
                          n.sym_,
                          "r",
                          EdgeAtom::Direction::BOTH,
                          {},
                          "m",
                          false,
                          memgraph::storage::View::OLD);

    auto one = LITERAL(1);
    auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
    auto set_n_p = std::make_shared<plan::SetProperty>(r_m.op_, prop, n_p, ADD(n_p, one));
    auto m_p = PROPERTY_LOOKUP(dba, IDENT("m")->MapTo(r_m.node_sym_), prop);
    auto set_m_p = std::make_shared<plan::SetProperty>(set_n_p, prop, m_p, ADD(m_p, one));

    std::shared_ptr<LogicalOperator> last_op = set_m_p;
    if (accumulate) {
      last_op = std::make_shared<Accumulate>(last_op, std::vector<Symbol>{n.sym_, r_m.node_sym_});
    }

    auto n_p_ne = NEXPR("n.p", n_p)->MapTo(symbol_table.CreateSymbol("n_p_ne", true));
    auto m_p_ne = NEXPR("m.p", m_p)->MapTo(symbol_table.CreateSymbol("m_p_ne", true));
    auto produce = MakeProduce(last_op, n_p_ne, m_p_ne);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    std::vector<int> results_data;
    for (const auto &row : results)
      for (const auto &column : row) results_data.emplace_back(column.ValueInt());
    if (accumulate)
      EXPECT_THAT(results_data, testing::ElementsAre(2, 2, 2, 2));
    else
      EXPECT_THAT(results_data, testing::ElementsAre(1, 1, 2, 2));
  };

  check(false);
  check(true);
}

TYPED_TEST(QueryPlanTest, AccumulateAdvance) {
  // we simulate 'CREATE (n) WITH n AS n MATCH (m) RETURN m'
  // to get correct results we need to advance the command
  auto check = [&](bool advance) {
    this->db.reset();
    this->CleanStorageDirs();
    this->db = std::make_unique<TypeParam>(this->config);
    auto storage_dba = this->db->Access(memgraph::storage::WRITE);
    memgraph::query::DbAccessor dba(storage_dba.get());
    SymbolTable symbol_table;
    NodeCreationInfo node;
    node.symbol = symbol_table.CreateSymbol("n", true);
    auto create = std::make_shared<CreateNode>(nullptr, node);
    auto accumulate = std::make_shared<Accumulate>(create, std::vector<Symbol>{node.symbol}, advance);
    auto match = MakeScanAll(this->storage, symbol_table, "m", accumulate);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    EXPECT_EQ(advance ? 1 : 0, PullAll(*match.op_, &context));
  };
  check(false);
  check(true);
}

/** Test fixture for all the aggregation ops in one return. */
template <typename StorageType>
class QueryPlanAggregateOps : public QueryPlanTest<StorageType> {
 protected:
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{this->db->Access(memgraph::storage::WRITE)};
  memgraph::query::DbAccessor dba{storage_dba.get()};
  memgraph::storage::PropertyId prop = this->db->NameToProperty("prop");

  SymbolTable symbol_table;

  void AddData() {
    // setup is several nodes most of which have an int property set
    // we will take the sum, avg, min, max and count
    // we won't group by anything
    ASSERT_TRUE(dba.InsertVertex().SetProperty(prop, memgraph::storage::PropertyValue(5)).has_value());
    ASSERT_TRUE(dba.InsertVertex().SetProperty(prop, memgraph::storage::PropertyValue(7)).has_value());
    ASSERT_TRUE(dba.InsertVertex().SetProperty(prop, memgraph::storage::PropertyValue(12)).has_value());
    ASSERT_TRUE(dba.InsertVertex().SetProperty(prop, memgraph::storage::PropertyValue(5)).has_value());
    ASSERT_TRUE(dba.InsertVertex().SetProperty(prop, memgraph::storage::PropertyValue(5)).has_value());
    ASSERT_TRUE(dba.InsertVertex().SetProperty(prop, memgraph::storage::PropertyValue(12)).has_value());

    // a missing property (null) gets ignored by all aggregations except
    // COUNT(*)
    dba.InsertVertex();
    dba.AdvanceCommand();
  }

  auto AggregationResults(bool with_group_by, bool distinct,
                          std::vector<Aggregation::Op> ops = {Aggregation::Op::COUNT,
                                                              Aggregation::Op::COUNT,
                                                              Aggregation::Op::MIN,
                                                              Aggregation::Op::MAX,
                                                              Aggregation::Op::SUM,
                                                              Aggregation::Op::AVG,
                                                              Aggregation::Op::COLLECT_LIST,
                                                              Aggregation::Op::COLLECT_MAP}) {
    // match all nodes and perform aggregations
    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);

    std::vector<Expression *> aggregation_expressions(ops.size(), n_p);
    std::vector<Expression *> group_bys;
    if (with_group_by) group_bys.push_back(n_p);
    aggregation_expressions[0] = nullptr;
    auto produce =
        this->MakeAggregationProduce(n.op_, symbol_table, aggregation_expressions, ops, group_bys, {}, distinct);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    return CollectProduce(*produce, &context);
  }
};

TYPED_TEST_SUITE(QueryPlanAggregateOps, StorageTypes);

TYPED_TEST(QueryPlanAggregateOps, PutsANaNAtTheEndTheSortPutsItAt) {
  // A NaN is the largest number a sort reads, so a column holding one has it
  // last and MAX reports it. A fold that asked whether one number is greater
  // than another would be told no in both directions, and would keep whichever
  // row the scan happened to reach first.
  ASSERT_TRUE(this->dba.InsertVertex().SetProperty(this->prop, memgraph::storage::PropertyValue(5.0)).has_value());
  ASSERT_TRUE(
      this->dba.InsertVertex().SetProperty(this->prop, memgraph::storage::PropertyValue(std::nan(""))).has_value());
  ASSERT_TRUE(this->dba.InsertVertex().SetProperty(this->prop, memgraph::storage::PropertyValue(1.0)).has_value());
  this->dba.AdvanceCommand();

  // The helper reads the first aggregation as `count(*)`, so the two this test
  // is about are the second and the third.
  auto results =
      this->AggregationResults(false, false, {Aggregation::Op::COUNT, Aggregation::Op::MIN, Aggregation::Op::MAX});
  ASSERT_EQ(results.size(), 1);

  EXPECT_EQ(results[0][1].ValueDouble(), 1.0);
  EXPECT_TRUE(std::isnan(results[0][2].ValueDouble()));
}

TYPED_TEST(QueryPlanAggregateOps, ReadsAColumnOfUnlikeTypesInTheOrderASortWould) {
  // Every pair of unlike types has a position, so a column mixing them has a
  // first and a last like any other. A boolean sorts below every number.
  ASSERT_TRUE(this->dba.InsertVertex().SetProperty(this->prop, memgraph::storage::PropertyValue(1)).has_value());
  ASSERT_TRUE(this->dba.InsertVertex().SetProperty(this->prop, memgraph::storage::PropertyValue(true)).has_value());
  this->dba.AdvanceCommand();

  // The helper reads the first aggregation as `count(*)`, so the two this test
  // is about are the second and the third.
  auto results =
      this->AggregationResults(false, false, {Aggregation::Op::COUNT, Aggregation::Op::MIN, Aggregation::Op::MAX});
  ASSERT_EQ(results.size(), 1);

  ASSERT_EQ(results[0][1].type(), TypedValue::Type::Bool);
  EXPECT_TRUE(results[0][1].ValueBool());
  ASSERT_EQ(results[0][2].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][2].ValueInt(), 1);
}

TYPED_TEST(QueryPlanAggregateOps, AnswersAColumnOfListsTheSameWhateverItsLength) {
  // A list is ordered by what it holds, so a list holding a value a sort
  // refuses leaves the pair unplaced and MIN has no answer for the column. The
  // refusal has to come from the column rather than from a pair, because a fold
  // reaching a pair only from the second row would answer a one-row column and
  // refuse a longer one holding the same value.
  auto a_list_holding_a_map = memgraph::storage::PropertyValue{std::vector{
      memgraph::storage::PropertyValue{memgraph::storage::PropertyValue::map_t{}},
  }};

  ASSERT_TRUE(this->dba.InsertVertex().SetProperty(this->prop, a_list_holding_a_map).has_value());
  this->dba.AdvanceCommand();
  EXPECT_THROW(this->AggregationResults(false, false, {Aggregation::Op::COUNT, Aggregation::Op::MIN}),
               QueryRuntimeException);

  ASSERT_TRUE(this->dba.InsertVertex().SetProperty(this->prop, a_list_holding_a_map).has_value());
  this->dba.AdvanceCommand();
  EXPECT_THROW(this->AggregationResults(false, false, {Aggregation::Op::COUNT, Aggregation::Op::MIN}),
               QueryRuntimeException);
}

TYPED_TEST(QueryPlanAggregateOps, WithData) {
  this->AddData();
  auto results = this->AggregationResults(false, false);

  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 8);
  // count(*)
  ASSERT_EQ(results[0][0].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][0].ValueInt(), 7);
  // count
  ASSERT_EQ(results[0][1].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][1].ValueInt(), 6);
  // min
  ASSERT_EQ(results[0][2].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][2].ValueInt(), 5);
  // max
  ASSERT_EQ(results[0][3].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][3].ValueInt(), 12);
  // sum
  ASSERT_EQ(results[0][4].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][4].ValueInt(), 46);
  // avg
  ASSERT_EQ(results[0][5].type(), TypedValue::Type::Double);
  EXPECT_FLOAT_EQ(results[0][5].ValueDouble(), 46 / 6.0);
  // collect list
  ASSERT_EQ(results[0][6].type(), TypedValue::Type::List);
  EXPECT_THAT(ToIntList(results[0][6]), UnorderedElementsAre(5, 7, 12, 5, 5, 12));
  // collect map
  ASSERT_EQ(results[0][7].type(), TypedValue::Type::Map);
  auto map = ToIntMap(results[0][7]);
  ASSERT_EQ(map.size(), 1);
  EXPECT_EQ(map.begin()->first, "key");
  EXPECT_FALSE(std::set<int>({5, 7, 12}).insert(map.begin()->second).second);
}

TYPED_TEST(QueryPlanAggregateOps, WithoutDataWithGroupBy) {
  {
    auto results = this->AggregationResults(true, false, {Aggregation::Op::COUNT});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, false, {Aggregation::Op::SUM});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, false, {Aggregation::Op::AVG});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, false, {Aggregation::Op::MIN});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, false, {Aggregation::Op::MAX});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, false, {Aggregation::Op::COLLECT_LIST});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, false, {Aggregation::Op::COLLECT_MAP});
    EXPECT_EQ(results.size(), 0);
  }
}

TYPED_TEST(QueryPlanAggregateOps, WithoutDataWithoutGroupBy) {
  auto results = this->AggregationResults(false, false);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 8);
  // count(*)
  ASSERT_EQ(results[0][0].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][0].ValueInt(), 0);
  // count
  ASSERT_EQ(results[0][1].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][1].ValueInt(), 0);
  // min
  EXPECT_TRUE(results[0][2].IsNull());
  // max
  EXPECT_TRUE(results[0][3].IsNull());
  // sum
  EXPECT_EQ(results[0][4].ValueInt(), 0);
  // avg
  EXPECT_TRUE(results[0][5].IsNull());
  // collect list
  ASSERT_EQ(results[0][6].type(), TypedValue::Type::List);
  EXPECT_EQ(ToIntList(results[0][6]).size(), 0);
  // collect map
  ASSERT_EQ(results[0][7].type(), TypedValue::Type::Map);
  EXPECT_EQ(ToIntMap(results[0][7]).size(), 0);
}

TYPED_TEST(QueryPlanTest, AggregateGroupByValues) {
  // Tests that distinct groups are aggregated properly for values of all types.
  // Also test the "remember" part of the Aggregation API as final results are
  // obtained via a property lookup of a remembered node.
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());

  // a vector of memgraph::storage::PropertyValue to be set as property values on vertices
  // most of them should result in a distinct group (commented where not)
  std::vector<memgraph::storage::PropertyValue> group_by_vals;
  group_by_vals.emplace_back(4);
  group_by_vals.emplace_back(7);
  group_by_vals.emplace_back(7.3);
  group_by_vals.emplace_back(7.2);
  group_by_vals.emplace_back("Johhny");
  group_by_vals.emplace_back("Jane");
  group_by_vals.emplace_back("1");
  group_by_vals.emplace_back(true);
  group_by_vals.emplace_back(false);
  group_by_vals.emplace_back(std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(1)});
  group_by_vals.emplace_back(std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(1),
                                                                           memgraph::storage::PropertyValue(2)});
  group_by_vals.emplace_back(std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(2),
                                                                           memgraph::storage::PropertyValue(1)});
  group_by_vals.emplace_back(memgraph::storage::PropertyValue());
  // should NOT result in another group because 7.0 == 7
  group_by_vals.emplace_back(7.0);
  // should NOT result in another group
  group_by_vals.emplace_back(std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(1),
                                                                           memgraph::storage::PropertyValue(2.0)});

  // generate a lot of vertices and set props on them
  auto prop = dba.NameToProperty("prop");
  for (int i = 0; i < 1000; ++i)
    ASSERT_TRUE(dba.InsertVertex().SetProperty(prop, group_by_vals[i % group_by_vals.size()]).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;

  // match all nodes and perform aggregations
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);

  auto produce =
      this->MakeAggregationProduce(n.op_, symbol_table, {n_p}, {Aggregation::Op::COUNT}, {n_p}, {n.sym_}, false);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), group_by_vals.size() - 2);
  std::unordered_set<TypedValue, TypedValue::Hash, TypedValue::BoolEqual> result_group_bys;
  for (const auto &row : results) {
    ASSERT_EQ(2, row.size());
    result_group_bys.insert(row[1]);
  }
  ASSERT_EQ(result_group_bys.size(), group_by_vals.size() - 2);
  std::vector<TypedValue> group_by_tvals;
  group_by_tvals.reserve(group_by_vals.size());
  for (const auto &v : group_by_vals) group_by_tvals.emplace_back(v, storage_dba->GetNameIdMapper());
  EXPECT_TRUE(std::is_permutation(
      group_by_tvals.begin(), group_by_tvals.end() - 2, result_group_bys.begin(), TypedValue::BoolEqual{}));
}

TYPED_TEST(QueryPlanTest, AggregateMultipleGroupBy) {
  // in this test we have 3 different properties that have different values
  // for different records and assert that we get the correct combination
  // of values in our groups
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());

  auto prop1 = dba.NameToProperty("prop1");
  auto prop2 = dba.NameToProperty("prop2");
  auto prop3 = dba.NameToProperty("prop3");
  for (int i = 0; i < 2 * 3 * 5; ++i) {
    auto v = dba.InsertVertex();
    ASSERT_TRUE(v.SetProperty(prop1, memgraph::storage::PropertyValue(static_cast<bool>(i % 2))).has_value());
    ASSERT_TRUE(v.SetProperty(prop2, memgraph::storage::PropertyValue(i % 3)).has_value());
    ASSERT_TRUE(v.SetProperty(prop3, memgraph::storage::PropertyValue("value" + std::to_string(i % 5))).has_value());
  }
  dba.AdvanceCommand();

  SymbolTable symbol_table;

  // match all nodes and perform aggregations
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_p1 = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop1);
  auto n_p2 = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop2);
  auto n_p3 = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop3);

  auto produce = this->MakeAggregationProduce(
      n.op_, symbol_table, {n_p1}, {Aggregation::Op::COUNT}, {n_p1, n_p2, n_p3}, {n.sym_}, false);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(results.size(), 2 * 3 * 5);
}

TYPED_TEST(QueryPlanTest, AggregateNoInput) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;

  auto two = LITERAL(2);
  auto produce = this->MakeAggregationProduce(nullptr, symbol_table, {two}, {Aggregation::Op::COUNT}, {}, {}, false);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1, results[0].size());
  EXPECT_EQ(TypedValue::Type::Int, results[0][0].type());
  EXPECT_EQ(1, results[0][0].ValueInt());
}

TYPED_TEST(QueryPlanTest, AggregateCountEdgeCases) {
  // tests for detected bugs in the COUNT aggregation behavior
  // ensure that COUNT returns correctly for
  //  - 0 vertices in database
  //  - 1 vertex in database, property not set
  //  - 1 vertex in database, property set
  //  - 2 vertices in database, property set on one
  //  - 2 vertices in database, property set on both

  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto prop = dba.NameToProperty("prop");

  SymbolTable symbol_table;

  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);

  // returns -1 when there are no results
  // otherwise returns MATCH (n) RETURN count(n.prop)
  auto count = [&]() {
    auto produce = this->MakeAggregationProduce(n.op_, symbol_table, {n_p}, {Aggregation::Op::COUNT}, {}, {}, false);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    if (results.size() == 0) return -1L;
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(1, results[0].size());
    EXPECT_EQ(TypedValue::Type::Int, results[0][0].type());
    return results[0][0].ValueInt();
  };

  // no vertices yet in database
  EXPECT_EQ(0, count());

  // one vertex, no property set
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(0, count());

  // one vertex, property set
  for (auto va : dba.Vertices(memgraph::storage::View::OLD))
    ASSERT_TRUE(va.SetProperty(prop, memgraph::storage::PropertyValue(42)).has_value());
  dba.AdvanceCommand();
  EXPECT_EQ(1, count());

  // two vertices, one with property set
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(1, count());

  // two vertices, both with property set
  for (auto va : dba.Vertices(memgraph::storage::View::OLD))
    ASSERT_TRUE(va.SetProperty(prop, memgraph::storage::PropertyValue(42)).has_value());
  dba.AdvanceCommand();
  EXPECT_EQ(2, count());
}

// COUNT over a bare identifier, where the identifier is bound to Null for some rows. Null is
// skipped by every aggregation, so a row whose identifier holds Null is not counted, and two
// rows holding the same value are two counts unless DISTINCT says otherwise.
TYPED_TEST(QueryPlanTest, AggregateCountIdentifierSkipsNull) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;

  // UNWIND [1, Null, 2, Null, 2] AS x RETURN count(x), count(DISTINCT x)
  auto input_expr = this->storage.template Create<PrimitiveLiteral>(
      std::vector<memgraph::storage::ExternalPropertyValue>{memgraph::storage::ExternalPropertyValue(1),
                                                            memgraph::storage::ExternalPropertyValue(),
                                                            memgraph::storage::ExternalPropertyValue(2),
                                                            memgraph::storage::ExternalPropertyValue(),
                                                            memgraph::storage::ExternalPropertyValue(2)});

  auto x = symbol_table.CreateSymbol("x", true);
  auto unwind = std::make_shared<plan::Unwind>(nullptr, input_expr, x);

  auto count = [&](bool distinct) {
    auto produce = this->MakeAggregationProduce(
        unwind, symbol_table, {IDENT("x")->MapTo(x)}, {Aggregation::Op::COUNT}, {}, {}, distinct);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(TypedValue::Type::Int, results[0][0].type());
    return results[0][0].ValueInt();
  };

  EXPECT_EQ(3, count(false)) << "the two Nulls must not be counted";
  EXPECT_EQ(2, count(true)) << "Nulls skipped before dedup, and the repeated 2 counted once";
}

// An aggregation with no grouping key holds on to its single accumulator rather than looking it
// up per row. A reset clears the map that accumulator lives in, so the next pull has to build a
// fresh one: accumulating through the old one would count the second pass on top of the first.
TYPED_TEST(QueryPlanTest, AggregateNoGroupKeyStartsOverAfterReset) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;

  // UNWIND [1, 2, 3] AS x RETURN count(x)
  auto input_expr = this->storage.template Create<PrimitiveLiteral>(
      std::vector<memgraph::storage::ExternalPropertyValue>{memgraph::storage::ExternalPropertyValue(1),
                                                            memgraph::storage::ExternalPropertyValue(2),
                                                            memgraph::storage::ExternalPropertyValue(3)});

  auto x = symbol_table.CreateSymbol("x", true);
  auto unwind = std::make_shared<plan::Unwind>(nullptr, input_expr, x);
  auto produce = this->MakeAggregationProduce(
      unwind, symbol_table, {IDENT("x")->MapTo(x)}, {Aggregation::Op::COUNT}, {}, {}, false);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto const result_symbol = context.symbol_table.at(*produce->named_expressions_[0]);

  Frame frame(context.symbol_table.max_position());
  auto cursor = produce->MakeCursor(memgraph::utils::NewDeleteResource(), TestMetricHandles());

  auto count_once = [&]() -> int64_t {
    EXPECT_TRUE(cursor->Pull(frame, context));
    auto const value = frame[result_symbol];
    EXPECT_FALSE(cursor->Pull(frame, context)) << "no grouping key means exactly one group";
    return value.ValueInt();
  };

  EXPECT_EQ(3, count_once());
  cursor->Reset();
  EXPECT_EQ(3, count_once()) << "the accumulator from before the reset must not be added to";
}

// SUM and AVG accumulate into the value they already hold, so the type of the running total has
// to follow the same rules addition does: integers stay integers until a double joins them, and
// from then on the total is a double whatever arrives after it.
TYPED_TEST(QueryPlanTest, AggregateSumKeepsAdditionsTypeRules) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;

  auto sum_of = [&](std::vector<memgraph::storage::ExternalPropertyValue> values, Aggregation::Op op) {
    auto input_expr = this->storage.template Create<PrimitiveLiteral>(std::move(values));
    auto x = symbol_table.CreateSymbol("x", true);
    auto unwind = std::make_shared<plan::Unwind>(nullptr, input_expr, x);
    auto produce = this->MakeAggregationProduce(unwind, symbol_table, {IDENT("x")->MapTo(x)}, {op}, {}, {}, false);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    EXPECT_EQ(1, results.size());
    return results[0][0];
  };
  using PV = memgraph::storage::ExternalPropertyValue;

  auto all_ints = sum_of({PV(1), PV(2), PV(3)}, Aggregation::Op::SUM);
  EXPECT_EQ(all_ints.type(), TypedValue::Type::Int) << "integers alone keep an integer total";
  EXPECT_EQ(all_ints.ValueInt(), 6);

  auto int_then_double = sum_of({PV(1), PV(2), PV(0.5)}, Aggregation::Op::SUM);
  ASSERT_EQ(int_then_double.type(), TypedValue::Type::Double) << "a double arriving promotes the total";
  EXPECT_DOUBLE_EQ(int_then_double.ValueDouble(), 3.5);

  auto double_then_int = sum_of({PV(0.5), PV(1), PV(2)}, Aggregation::Op::SUM);
  ASSERT_EQ(double_then_int.type(), TypedValue::Type::Double) << "the total stays a double after that";
  EXPECT_DOUBLE_EQ(double_then_int.ValueDouble(), 3.5);

  auto with_nulls = sum_of({PV(1), PV(), PV(2)}, Aggregation::Op::SUM);
  ASSERT_EQ(with_nulls.type(), TypedValue::Type::Int);
  EXPECT_EQ(with_nulls.ValueInt(), 3) << "Null is skipped rather than making the total Null";

  auto mean = sum_of({PV(1), PV(2)}, Aggregation::Op::AVG);
  ASSERT_EQ(mean.type(), TypedValue::Type::Double) << "an average is a double even over integers";
  EXPECT_DOUBLE_EQ(mean.ValueDouble(), 1.5);
}

TYPED_TEST(QueryPlanTest, AggregateSumRejectsNonNumeric) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;

  auto input_expr =
      this->storage.template Create<PrimitiveLiteral>(std::vector<memgraph::storage::ExternalPropertyValue>{
          memgraph::storage::ExternalPropertyValue(1), memgraph::storage::ExternalPropertyValue("not a number")});
  auto x = symbol_table.CreateSymbol("x", true);
  auto unwind = std::make_shared<plan::Unwind>(nullptr, input_expr, x);
  auto produce =
      this->MakeAggregationProduce(unwind, symbol_table, {IDENT("x")->MapTo(x)}, {Aggregation::Op::SUM}, {}, {}, false);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  EXPECT_THROW(CollectProduce(*produce, &context), QueryRuntimeException);
}

TYPED_TEST(QueryPlanTest, AggregateFirstValueTypes) {
  // testing exceptions that get emitted by the first-value
  // type check

  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());

  auto v1 = dba.InsertVertex();
  auto prop_string = dba.NameToProperty("string");
  ASSERT_TRUE(v1.SetProperty(prop_string, memgraph::storage::PropertyValue("johhny")).has_value());
  auto prop_int = dba.NameToProperty("int");
  ASSERT_TRUE(v1.SetProperty(prop_int, memgraph::storage::PropertyValue(12)).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;

  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_prop_string = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop_string);
  auto n_prop_int = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop_int);
  auto n_id = n_prop_string->expression_;

  auto aggregate = [&](Expression *expression, Aggregation::Op aggr_op) {
    auto produce = this->MakeAggregationProduce(n.op_, symbol_table, {expression}, {aggr_op}, {}, {}, false);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    CollectProduce(*produce, &context);
  };

  // everything except for COUNT and COLLECT fails on a Vertex
  aggregate(n_id, Aggregation::Op::COUNT);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::MIN), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::MAX), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::AVG), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::SUM), QueryRuntimeException);

  // on strings AVG and SUM fail
  aggregate(n_prop_string, Aggregation::Op::COUNT);
  aggregate(n_prop_string, Aggregation::Op::MIN);
  aggregate(n_prop_string, Aggregation::Op::MAX);
  EXPECT_THROW(aggregate(n_prop_string, Aggregation::Op::AVG), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_prop_string, Aggregation::Op::SUM), QueryRuntimeException);

  // on ints nothing fails
  aggregate(n_prop_int, Aggregation::Op::COUNT);
  aggregate(n_prop_int, Aggregation::Op::MIN);
  aggregate(n_prop_int, Aggregation::Op::MAX);
  aggregate(n_prop_int, Aggregation::Op::AVG);
  aggregate(n_prop_int, Aggregation::Op::SUM);
  aggregate(n_prop_int, Aggregation::Op::COLLECT_LIST);
  aggregate(n_prop_int, Aggregation::Op::COLLECT_MAP);
}

TYPED_TEST(QueryPlanTest, AggregateTypes) {
  // testing exceptions that can get emitted by an aggregation
  // does not check all combinations that can result in an exception
  // (that logic is defined and tested by TypedValue)

  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());

  auto p1 = dba.NameToProperty("p1");  // has only string props
  ASSERT_TRUE(dba.InsertVertex().SetProperty(p1, memgraph::storage::PropertyValue("string")).has_value());
  ASSERT_TRUE(dba.InsertVertex().SetProperty(p1, memgraph::storage::PropertyValue("str2")).has_value());
  auto p2 = dba.NameToProperty("p2");  // combines int and bool
  ASSERT_TRUE(dba.InsertVertex().SetProperty(p2, memgraph::storage::PropertyValue(42)).has_value());
  ASSERT_TRUE(dba.InsertVertex().SetProperty(p2, memgraph::storage::PropertyValue(true)).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;

  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_p1 = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), p1);
  auto n_p2 = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), p2);

  auto aggregate = [&](Expression *expression, Aggregation::Op aggr_op) {
    auto produce = this->MakeAggregationProduce(n.op_, symbol_table, {expression}, {aggr_op}, {}, {}, false);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    CollectProduce(*produce, &context);
  };

  // everything except for COUNT and COLLECT fails on a Vertex
  auto n_id = n_p1->expression_;
  aggregate(n_id, Aggregation::Op::COUNT);
  aggregate(n_id, Aggregation::Op::COLLECT_LIST);
  aggregate(n_id, Aggregation::Op::COLLECT_MAP);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::MIN), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::MAX), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::AVG), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::SUM), QueryRuntimeException);

  // on strings AVG and SUM fail
  aggregate(n_p1, Aggregation::Op::COUNT);
  aggregate(n_p1, Aggregation::Op::COLLECT_LIST);
  aggregate(n_p1, Aggregation::Op::COLLECT_MAP);
  aggregate(n_p1, Aggregation::Op::MIN);
  aggregate(n_p1, Aggregation::Op::MAX);
  EXPECT_THROW(aggregate(n_p1, Aggregation::Op::AVG), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_p1, Aggregation::Op::SUM), QueryRuntimeException);

  // a column of unlike types is one MIN and MAX answer for, because a sort
  // places every pair of them; AVG and SUM still need numbers
  aggregate(n_p2, Aggregation::Op::COUNT);
  aggregate(n_p2, Aggregation::Op::COLLECT_LIST);
  aggregate(n_p2, Aggregation::Op::COLLECT_MAP);
  aggregate(n_p2, Aggregation::Op::MIN);
  aggregate(n_p2, Aggregation::Op::MAX);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::AVG), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::SUM), QueryRuntimeException);
}

#ifdef MG_ENTERPRISE
/// Reading one restricted property by the two routes a query can take to it.
template <typename StorageType>
class QueryPlanPropertyPermission : public QueryPlanTest<StorageType> {
 protected:
  std::unique_ptr<memgraph::storage::Storage::Accessor> storage_dba{this->db->Access(memgraph::storage::WRITE)};
  memgraph::query::DbAccessor dba{storage_dba.get()};
  memgraph::storage::LabelId label = this->db->NameToLabel("Item");
  memgraph::storage::PropertyId prop = this->db->NameToProperty("value");

  // A property restriction is only consulted under an enterprise licence; without one every read
  // is allowed, and these tests would pass while asserting nothing.
  void SetUp() override { memgraph::license::global_license_checker.EnableTesting(); }

  void TearDown() override {
    memgraph::license::global_license_checker.DisableTesting();
    QueryPlanTest<StorageType>::TearDown();
  }

  /// Every vertex carries the label the restriction is keyed on, which is what lets the queries
  /// below scan without one and still expect the restriction to apply to everything they read.
  void AddData() {
    for (int64_t value : {1, 2, 3}) {
      auto vertex = dba.InsertVertex();
      ASSERT_TRUE(vertex.AddLabel(label).has_value());
      ASSERT_TRUE(vertex.SetProperty(prop, memgraph::storage::PropertyValue(value)).has_value());
    }
    dba.AdvanceCommand();
  }

  /// A user who may read everything. Two things have to be said explicitly: property permissions
  /// are resolved per database, so the one under test is granted, and unrestricted access means a
  /// global grant over every property rather than the absence of any rule.
  memgraph::auth::User Unrestricted() const {
    auto user = memgraph::auth::User{"unrestricted"};
    user.db_access().Grant(dba.DatabaseName());
    user.fine_grained_access_handler().label_permissions().GrantGlobal(memgraph::auth::kAllLabelPermissions);
    user.property_access_handler().label_properties().GrantGlobal("*", memgraph::auth::kAllPropertyPermissionTypes);
    user.property_access_handler().edge_type_properties().GrantGlobal("*", memgraph::auth::kAllPropertyPermissionTypes);
    return user;
  }

  /// The same user, with the one property denied on the label carrying it.
  memgraph::auth::User Restricted() const {
    auto user = Unrestricted();
    user.property_access_handler().label_properties().Deny(
        {"Item"}, "value", memgraph::auth::kAllPropertyPermissionTypes);
    return user;
  }

  /// `MATCH (n) RETURN count(n.value), sum(n.value), max(n.value)`, which reads the property
  /// inside an aggregation. The three answer differently over no values, so together they say
  /// whether the aggregation saw any.
  std::vector<TypedValue> AggregatesThroughAggregation(memgraph::auth::User user) {
    SymbolTable symbol_table;
    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
    auto produce = this->MakeAggregationProduce(n.op_,
                                                symbol_table,
                                                {n_p, n_p, n_p},
                                                {Aggregation::Op::COUNT, Aggregation::Op::SUM, Aggregation::Op::MAX},
                                                {},
                                                {},
                                                false);

    memgraph::glue::FineGrainedAuthChecker auth_checker{std::move(user), &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);
    auto results = CollectProduce(*produce, &context);
    EXPECT_EQ(results.size(), 1);
    return results[0];
  }

  /// `MATCH (n) RETURN n.value`, which reads the same property through a projection.
  std::vector<TypedValue> ValuesThroughProjection(memgraph::auth::User user) {
    SymbolTable symbol_table;
    auto n = MakeScanAll(this->storage, symbol_table, "n");
    auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);
    auto output = NEXPR("value", n_p)->MapTo(symbol_table.CreateSymbol("value", true));
    auto produce = std::make_shared<Produce>(n.op_, std::vector<NamedExpression *>{output});

    memgraph::glue::FineGrainedAuthChecker auth_checker{std::move(user), &dba};
    auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &dba, &auth_checker);
    auto results = CollectProduce(*produce, &context);
    auto values = std::vector<TypedValue>{};
    for (auto const &row : results) values.emplace_back(row[0]);
    return values;
  }
};

TYPED_TEST_SUITE(QueryPlanPropertyPermission, StorageTypes);

TYPED_TEST(QueryPlanPropertyPermission, MasksARestrictedPropertyReadThroughAnAggregation) {
  // A denied property reads as Null, so an aggregation over it sees no values at all and answers
  // as it would for a property no vertex carries: counting none of them, summing to zero, and
  // having no maximum. An aggregation reaches the property by a different path from a projection,
  // which is why the same restriction is asserted through both.
  this->AddData();

  auto allowed = this->AggregatesThroughAggregation(this->Unrestricted());
  ASSERT_EQ(allowed.size(), 3);
  EXPECT_EQ(allowed[0].ValueInt(), 3);
  EXPECT_EQ(allowed[1].ValueInt(), 6);
  EXPECT_EQ(allowed[2].ValueInt(), 3);

  auto denied = this->AggregatesThroughAggregation(this->Restricted());
  ASSERT_EQ(denied.size(), 3);
  EXPECT_EQ(denied[0].ValueInt(), 0);
  EXPECT_EQ(denied[1].ValueInt(), 0);
  EXPECT_TRUE(denied[2].IsNull());
}

TYPED_TEST(QueryPlanPropertyPermission, MasksARestrictedPropertyReadThroughAProjection) {
  // The same property by the route the aggregation is being compared against, so that a failure
  // says which of the two paths lost the restriction.
  this->AddData();

  auto allowed = this->ValuesThroughProjection(this->Unrestricted());
  ASSERT_EQ(allowed.size(), 3);
  EXPECT_THAT((std::vector{allowed[0].ValueInt(), allowed[1].ValueInt(), allowed[2].ValueInt()}),
              UnorderedElementsAre(1, 2, 3));

  auto denied = this->ValuesThroughProjection(this->Restricted());
  ASSERT_EQ(denied.size(), 3);
  EXPECT_TRUE(denied[0].IsNull());
  EXPECT_TRUE(denied[1].IsNull());
  EXPECT_TRUE(denied[2].IsNull());
}

TYPED_TEST(QueryPlanPropertyPermission, CountsTheRowsItRefusesToShowTheValuesOf) {
  // Masking a property hides the value, not the row. `count(*)` is answered without reading the
  // property at all, so it reports every row whether or not the reader may see what is in it.
  this->AddData();

  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto produce = this->MakeAggregationProduce(n.op_, symbol_table, {nullptr}, {Aggregation::Op::COUNT}, {}, {}, false);

  memgraph::glue::FineGrainedAuthChecker auth_checker{this->Restricted(), &this->dba};
  auto context = MakeContextWithFineGrainedChecker(this->storage, symbol_table, &this->dba, &auth_checker);
  auto results = CollectProduce(*produce, &context);

  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(results[0][0].ValueInt(), 3);
}
#endif

TYPED_TEST(QueryPlanTest, Unwind) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;

  // UNWIND [ [1, true, "x"], [], ["bla"] ] AS x UNWIND x as y RETURN x, y
  auto input_expr =
      this->storage.template Create<PrimitiveLiteral>(std::vector<memgraph::storage::ExternalPropertyValue>{
          memgraph::storage::ExternalPropertyValue(
              std::vector<memgraph::storage::ExternalPropertyValue>{memgraph::storage::ExternalPropertyValue(1),
                                                                    memgraph::storage::ExternalPropertyValue(true),
                                                                    memgraph::storage::ExternalPropertyValue("x")}),
          memgraph::storage::ExternalPropertyValue(std::vector<memgraph::storage::ExternalPropertyValue>{}),
          memgraph::storage::ExternalPropertyValue(
              std::vector<memgraph::storage::ExternalPropertyValue>{memgraph::storage::ExternalPropertyValue("bla")})});

  auto x = symbol_table.CreateSymbol("x", true);
  auto unwind_0 = std::make_shared<plan::Unwind>(nullptr, input_expr, x);
  auto x_expr = IDENT("x")->MapTo(x);
  auto y = symbol_table.CreateSymbol("y", true);
  auto unwind_1 = std::make_shared<plan::Unwind>(unwind_0, x_expr, y);

  auto x_ne = NEXPR("x", x_expr)->MapTo(symbol_table.CreateSymbol("x_ne", true));
  auto y_ne = NEXPR("y", IDENT("y")->MapTo(y))->MapTo(symbol_table.CreateSymbol("y_ne", true));
  auto produce = MakeProduce(unwind_1, x_ne, y_ne);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(4, results.size());
  const std::vector<int> expected_x_card{3, 3, 3, 1};
  auto expected_x_card_it = expected_x_card.begin();
  const std::vector<TypedValue> expected_y{TypedValue(1), TypedValue(true), TypedValue("x"), TypedValue("bla")};
  auto expected_y_it = expected_y.begin();
  for (const auto &row : results) {
    ASSERT_EQ(2, row.size());
    ASSERT_EQ(row[0].type(), TypedValue::Type::List);
    EXPECT_EQ(row[0].ValueList().size(), *expected_x_card_it);
    EXPECT_EQ(row[1].type(), expected_y_it->type());
    expected_x_card_it++;
    expected_y_it++;
  }
}

TYPED_TEST(QueryPlanAggregateOps, WithDataDistinct) {
  this->AddData();
  auto results = this->AggregationResults(false, true);

  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 8);
  // count(*)
  ASSERT_EQ(results[0][0].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][0].ValueInt(), 7);
  // count
  ASSERT_EQ(results[0][1].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][1].ValueInt(), 3);
  // min
  ASSERT_EQ(results[0][2].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][2].ValueInt(), 5);
  // max
  ASSERT_EQ(results[0][3].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][3].ValueInt(), 12);
  // sum
  ASSERT_EQ(results[0][4].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][4].ValueInt(), 24);
  // avg
  ASSERT_EQ(results[0][5].type(), TypedValue::Type::Double);
  EXPECT_FLOAT_EQ(results[0][5].ValueDouble(), 24 / 3.0);
  // collect list
  ASSERT_EQ(results[0][6].type(), TypedValue::Type::List);
  EXPECT_THAT(ToIntList(results[0][6]), UnorderedElementsAre(5, 7, 12));
  // collect map
  ASSERT_EQ(results[0][7].type(), TypedValue::Type::Map);
  auto map = ToIntMap(results[0][7]);
  ASSERT_EQ(map.size(), 1);
  EXPECT_EQ(map.begin()->first, "key");
  EXPECT_FALSE(std::set<int>({5, 7, 12}).insert(map.begin()->second).second);
}

TYPED_TEST(QueryPlanAggregateOps, WithoutDataWithDistinctAndWithGroupBy) {
  {
    auto results = this->AggregationResults(true, true, {Aggregation::Op::COUNT});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, true, {Aggregation::Op::SUM});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, true, {Aggregation::Op::AVG});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, true, {Aggregation::Op::MIN});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, true, {Aggregation::Op::MAX});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, true, {Aggregation::Op::COLLECT_LIST});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = this->AggregationResults(true, true, {Aggregation::Op::COLLECT_MAP});
    EXPECT_EQ(results.size(), 0);
  }
}

TYPED_TEST(QueryPlanAggregateOps, WithoutDataWithDistinctAndWithoutGroupBy) {
  auto results = this->AggregationResults(false, true);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 8);
  // count(*)
  ASSERT_EQ(results[0][0].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][0].ValueInt(), 0);
  // count
  ASSERT_EQ(results[0][1].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][1].ValueInt(), 0);
  // min
  EXPECT_TRUE(results[0][2].IsNull());
  // max
  EXPECT_TRUE(results[0][3].IsNull());
  // sum
  EXPECT_EQ(results[0][4].ValueInt(), 0);
  // avg
  EXPECT_TRUE(results[0][5].IsNull());
  // collect list
  ASSERT_EQ(results[0][6].type(), TypedValue::Type::List);
  EXPECT_EQ(ToIntList(results[0][6]).size(), 0);
  // collect map
  ASSERT_EQ(results[0][7].type(), TypedValue::Type::Map);
  EXPECT_EQ(ToIntMap(results[0][7]).size(), 0);
}

TYPED_TEST(QueryPlanTest, AggregateGroupByValuesWithDistinct) {
  // Tests that distinct groups are aggregated properly for values of all types.
  // Also test the "remember" part of the Aggregation API as final results are
  // obtained via a property lookup of a remembered node.
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());

  // a vector of memgraph::storage::PropertyValue to be set as property values on vertices
  // most of them should result in a distinct group (commented where not)
  std::vector<memgraph::storage::PropertyValue> group_by_vals;
  group_by_vals.emplace_back(4);
  group_by_vals.emplace_back(7);
  group_by_vals.emplace_back(7.3);
  group_by_vals.emplace_back(7.2);
  group_by_vals.emplace_back("Johhny");
  group_by_vals.emplace_back("Jane");
  group_by_vals.emplace_back("1");
  group_by_vals.emplace_back(true);
  group_by_vals.emplace_back(false);
  group_by_vals.emplace_back(std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(1)});
  group_by_vals.emplace_back(std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(1),
                                                                           memgraph::storage::PropertyValue(2)});
  group_by_vals.emplace_back(std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(2),
                                                                           memgraph::storage::PropertyValue(1)});
  group_by_vals.emplace_back(memgraph::storage::PropertyValue());
  // should NOT result in another group because 7.0 == 7
  group_by_vals.emplace_back(7.0);
  // should NOT result in another group
  group_by_vals.emplace_back(std::vector<memgraph::storage::PropertyValue>{memgraph::storage::PropertyValue(1),
                                                                           memgraph::storage::PropertyValue(2.0)});

  // generate a lot of vertices and set props on them
  auto prop = dba.NameToProperty("prop");
  for (int i = 0; i < 1000; ++i)
    ASSERT_TRUE(dba.InsertVertex().SetProperty(prop, group_by_vals[i % group_by_vals.size()]).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;

  // match all nodes and perform aggregations
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);

  auto produce =
      this->MakeAggregationProduce(n.op_, symbol_table, {n_p}, {Aggregation::Op::COUNT}, {n_p}, {n.sym_}, true);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), group_by_vals.size() - 2);
  std::unordered_set<TypedValue, TypedValue::Hash, TypedValue::BoolEqual> result_group_bys;
  for (const auto &row : results) {
    ASSERT_EQ(2, row.size());
    if (!row[1].IsNull()) {
      ASSERT_EQ(1, row[0].ValueInt());
    }
    result_group_bys.insert(row[1]);
  }
  ASSERT_EQ(result_group_bys.size(), group_by_vals.size() - 2);
  std::vector<TypedValue> group_by_tvals;
  group_by_tvals.reserve(group_by_vals.size());
  for (const auto &v : group_by_vals) group_by_tvals.emplace_back(v, storage_dba->GetNameIdMapper());
  EXPECT_TRUE(std::is_permutation(
      group_by_tvals.begin(), group_by_tvals.end() - 2, result_group_bys.begin(), TypedValue::BoolEqual{}));
}

TYPED_TEST(QueryPlanTest, AggregateMultipleGroupByWithDistinct) {
  // in this test we have 3 different properties that have different values
  // for different records and assert that we get the correct combination
  // of values in our groups
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());

  auto prop1 = dba.NameToProperty("prop1");
  auto prop2 = dba.NameToProperty("prop2");
  auto prop3 = dba.NameToProperty("prop3");
  for (int i = 0; i < 2 * 3 * 5; ++i) {
    auto v = dba.InsertVertex();
    ASSERT_TRUE(v.SetProperty(prop1, memgraph::storage::PropertyValue(static_cast<bool>(i % 2))).has_value());
    ASSERT_TRUE(v.SetProperty(prop2, memgraph::storage::PropertyValue(i % 3)).has_value());
    ASSERT_TRUE(v.SetProperty(prop3, memgraph::storage::PropertyValue("value" + std::to_string(i % 5))).has_value());
  }
  dba.AdvanceCommand();

  SymbolTable symbol_table;

  // match all nodes and perform aggregations
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_p1 = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop1);
  auto n_p2 = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop2);

  auto produce =
      this->MakeAggregationProduce(n.op_, symbol_table, {n_p1}, {Aggregation::Op::COUNT}, {n_p1, n_p2}, {n.sym_}, true);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  for (const auto &row : results) {
    ASSERT_EQ(1, row[0].ValueInt());
  }
}

TYPED_TEST(QueryPlanTest, AggregateNoInputWithDistinct) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  SymbolTable symbol_table;

  auto two = LITERAL(2);
  auto produce = this->MakeAggregationProduce(nullptr, symbol_table, {two}, {Aggregation::Op::COUNT}, {}, {}, true);
  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1, results[0].size());
  EXPECT_EQ(TypedValue::Type::Int, results[0][0].type());
  EXPECT_EQ(1, results[0][0].ValueInt());
}

TYPED_TEST(QueryPlanTest, AggregateCountEdgeCasesWithDistinct) {
  // tests for detected bugs in the COUNT aggregation behavior
  // ensure that COUNT returns correctly for
  //  - 0 vertices in database
  //  - 1 vertex in database, property not set
  //  - 1 vertex in database, property set
  //  - 2 vertices in database, property set on one
  //  - 2 vertices in database, property set on both

  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto prop = dba.NameToProperty("prop");

  SymbolTable symbol_table;

  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_p = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop);

  // returns -1 when there are no results
  // otherwise returns MATCH (n) RETURN count(n.prop)
  auto count = [&]() {
    auto produce = this->MakeAggregationProduce(n.op_, symbol_table, {n_p}, {Aggregation::Op::COUNT}, {}, {}, true);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    auto results = CollectProduce(*produce, &context);
    if (results.size() == 0) return -1L;
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(1, results[0].size());
    EXPECT_EQ(TypedValue::Type::Int, results[0][0].type());
    return results[0][0].ValueInt();
  };

  // no vertices yet in database
  EXPECT_EQ(0, count());

  // one vertex, no property set
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(0, count());

  // one vertex, property set
  for (auto va : dba.Vertices(memgraph::storage::View::OLD))
    ASSERT_TRUE(va.SetProperty(prop, memgraph::storage::PropertyValue(42)).has_value());
  dba.AdvanceCommand();
  EXPECT_EQ(1, count());

  // two vertices, one with property set
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(1, count());

  // two vertices, both with property set
  for (auto va : dba.Vertices(memgraph::storage::View::OLD))
    ASSERT_TRUE(va.SetProperty(prop, memgraph::storage::PropertyValue(42)).has_value());
  dba.AdvanceCommand();
  EXPECT_EQ(1, count());
}

TYPED_TEST(QueryPlanTest, AggregateFirstValueTypesWithDistinct) {
  // testing exceptions that get emitted by the first-value
  // type check

  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());

  auto v1 = dba.InsertVertex();
  auto prop_string = dba.NameToProperty("string");
  ASSERT_TRUE(v1.SetProperty(prop_string, memgraph::storage::PropertyValue("johhny")).has_value());
  auto prop_int = dba.NameToProperty("int");
  ASSERT_TRUE(v1.SetProperty(prop_int, memgraph::storage::PropertyValue(12)).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;

  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_prop_string = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop_string);
  auto n_prop_int = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), prop_int);
  auto n_id = n_prop_string->expression_;

  auto aggregate = [&](Expression *expression, Aggregation::Op aggr_op) {
    auto produce = this->MakeAggregationProduce(n.op_, symbol_table, {expression}, {aggr_op}, {}, {}, true);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    CollectProduce(*produce, &context);
  };

  // everything except for COUNT and COLLECT fails on a Vertex
  aggregate(n_id, Aggregation::Op::COUNT);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::MIN), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::MAX), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::AVG), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::SUM), QueryRuntimeException);

  // on strings AVG and SUM fail
  aggregate(n_prop_string, Aggregation::Op::COUNT);
  aggregate(n_prop_string, Aggregation::Op::MIN);
  aggregate(n_prop_string, Aggregation::Op::MAX);
  EXPECT_THROW(aggregate(n_prop_string, Aggregation::Op::AVG), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_prop_string, Aggregation::Op::SUM), QueryRuntimeException);

  // on ints nothing fails
  aggregate(n_prop_int, Aggregation::Op::COUNT);
  aggregate(n_prop_int, Aggregation::Op::MIN);
  aggregate(n_prop_int, Aggregation::Op::MAX);
  aggregate(n_prop_int, Aggregation::Op::AVG);
  aggregate(n_prop_int, Aggregation::Op::SUM);
  aggregate(n_prop_int, Aggregation::Op::COLLECT_LIST);
  aggregate(n_prop_int, Aggregation::Op::COLLECT_MAP);
}

TYPED_TEST(QueryPlanTest, AggregateTypesWithDistinct) {
  // testing exceptions that can get emitted by an aggregation
  // does not check all combinations that can result in an exception
  // (that logic is defined and tested by TypedValue)

  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());

  auto p1 = dba.NameToProperty("p1");  // has only string props
  ASSERT_TRUE(dba.InsertVertex().SetProperty(p1, memgraph::storage::PropertyValue("string")).has_value());
  ASSERT_TRUE(dba.InsertVertex().SetProperty(p1, memgraph::storage::PropertyValue("str2")).has_value());
  auto p2 = dba.NameToProperty("p2");  // combines int and bool
  ASSERT_TRUE(dba.InsertVertex().SetProperty(p2, memgraph::storage::PropertyValue(42)).has_value());
  ASSERT_TRUE(dba.InsertVertex().SetProperty(p2, memgraph::storage::PropertyValue(true)).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;

  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_p1 = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), p1);
  auto n_p2 = PROPERTY_LOOKUP(dba, IDENT("n")->MapTo(n.sym_), p2);

  auto aggregate = [&](Expression *expression, Aggregation::Op aggr_op) {
    auto produce = this->MakeAggregationProduce(n.op_, symbol_table, {expression}, {aggr_op}, {}, {}, true);
    auto context = MakeContext(this->storage, symbol_table, &dba);
    CollectProduce(*produce, &context);
  };

  // everything except for COUNT and COLLECT fails on a Vertex
  auto n_id = n_p1->expression_;
  aggregate(n_id, Aggregation::Op::COUNT);
  aggregate(n_id, Aggregation::Op::COLLECT_LIST);
  aggregate(n_id, Aggregation::Op::COLLECT_MAP);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::MIN), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::MAX), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::AVG), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::SUM), QueryRuntimeException);

  // on strings AVG and SUM fail
  aggregate(n_p1, Aggregation::Op::COUNT);
  aggregate(n_p1, Aggregation::Op::COLLECT_LIST);
  aggregate(n_p1, Aggregation::Op::COLLECT_MAP);
  aggregate(n_p1, Aggregation::Op::MIN);
  aggregate(n_p1, Aggregation::Op::MAX);
  EXPECT_THROW(aggregate(n_p1, Aggregation::Op::AVG), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_p1, Aggregation::Op::SUM), QueryRuntimeException);

  // a column of unlike types is one MIN and MAX answer for, because a sort
  // places every pair of them; AVG and SUM still need numbers
  aggregate(n_p2, Aggregation::Op::COUNT);
  aggregate(n_p2, Aggregation::Op::COLLECT_LIST);
  aggregate(n_p2, Aggregation::Op::COLLECT_MAP);
  aggregate(n_p2, Aggregation::Op::MIN);
  aggregate(n_p2, Aggregation::Op::MAX);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::AVG), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::SUM), QueryRuntimeException);
}

TYPED_TEST(QueryPlanTest, CachePropertiesFillsSlotsInOnePass) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto prop_a = dba.NameToProperty("a");
  auto prop_b = dba.NameToProperty("b");

  auto v = dba.InsertVertex();
  ASSERT_TRUE(v.SetProperty(prop_a, memgraph::storage::PropertyValue(42)).has_value());
  ASSERT_TRUE(v.SetProperty(prop_b, memgraph::storage::PropertyValue("hello")).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto cached_a_sym = symbol_table.CreateSymbol("cached_a", true);
  auto cached_b_sym = symbol_table.CreateSymbol("cached_b", true);
  auto cache = std::make_shared<CacheProperties>(
      n.op_, n.sym_, std::vector<CachedProperty>{{prop_a, cached_a_sym}, {prop_b, cached_b_sym}});

  auto a_ne = NEXPR("a", IDENT("cached_a")->MapTo(cached_a_sym))->MapTo(symbol_table.CreateSymbol("a_ne", true));
  auto b_ne = NEXPR("b", IDENT("cached_b")->MapTo(cached_b_sym))->MapTo(symbol_table.CreateSymbol("b_ne", true));
  auto produce = MakeProduce(cache, a_ne, b_ne);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 2);
  EXPECT_EQ(results[0][0].ValueInt(), 42);
  EXPECT_EQ(results[0][1].ValueString(), "hello");
}

// The single pass reads a record in the order the record holds its properties, which is the order
// their names were first interned. A caller groups its properties by name, and the two orders
// disagree whenever a name interned earlier sorts later, so the operator cannot assume it is handed
// them in the order the read wants.
TYPED_TEST(QueryPlanTest, CachePropertiesFillsSlotsWhateverOrderTheyArriveIn) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  // Interned first, so it holds the lower id while sorting after the other.
  auto prop_zebra = dba.NameToProperty("zebra");
  auto prop_apple = dba.NameToProperty("apple");
  ASSERT_LT(prop_zebra.AsUint(), prop_apple.AsUint());

  auto v = dba.InsertVertex();
  ASSERT_TRUE(v.SetProperty(prop_zebra, memgraph::storage::PropertyValue(1)).has_value());
  ASSERT_TRUE(v.SetProperty(prop_apple, memgraph::storage::PropertyValue(2)).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto apple_sym = symbol_table.CreateSymbol("cached_apple", true);
  auto zebra_sym = symbol_table.CreateSymbol("cached_zebra", true);
  // Handed over in the order their names sort, which here is the reverse of the order the record
  // holds them in.
  auto cache = std::make_shared<CacheProperties>(
      n.op_, n.sym_, std::vector<CachedProperty>{{prop_apple, apple_sym}, {prop_zebra, zebra_sym}});

  auto apple_ne =
      NEXPR("apple", IDENT("cached_apple")->MapTo(apple_sym))->MapTo(symbol_table.CreateSymbol("apple_ne", true));
  auto zebra_ne =
      NEXPR("zebra", IDENT("cached_zebra")->MapTo(zebra_sym))->MapTo(symbol_table.CreateSymbol("zebra_ne", true));
  auto produce = MakeProduce(cache, apple_ne, zebra_ne);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 2);
  EXPECT_EQ(results[0][0].ValueInt(), 2);
  EXPECT_EQ(results[0][1].ValueInt(), 1);
}

#ifdef MG_ENTERPRISE
TYPED_TEST(QueryPlanTest, CachePropertiesDeniedPropertyIsNull) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto label = dba.NameToLabel("Person");
  auto prop_name = dba.NameToProperty("name");
  auto prop_ssn = dba.NameToProperty("ssn");

  auto v = dba.InsertVertex();
  ASSERT_TRUE(v.AddLabel(label).has_value());
  ASSERT_TRUE(v.SetProperty(prop_name, memgraph::storage::PropertyValue("Alice")).has_value());
  ASSERT_TRUE(v.SetProperty(prop_ssn, memgraph::storage::PropertyValue("secret")).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto cached_name_sym = symbol_table.CreateSymbol("cached_name", true);
  auto cached_ssn_sym = symbol_table.CreateSymbol("cached_ssn", true);
  auto cache = std::make_shared<CacheProperties>(
      n.op_, n.sym_, std::vector<CachedProperty>{{prop_name, cached_name_sym}, {prop_ssn, cached_ssn_sym}});

  auto name_ne =
      NEXPR("name", IDENT("cached_name")->MapTo(cached_name_sym))->MapTo(symbol_table.CreateSymbol("name_ne", true));
  auto ssn_ne =
      NEXPR("ssn", IDENT("cached_ssn")->MapTo(cached_ssn_sym))->MapTo(symbol_table.CreateSymbol("ssn_ne", true));
  auto produce = MakeProduce(cache, name_ne, ssn_ne);

  // A property lookup the caller may not read evaluates to Null, so caching it must not hand the
  // real value back instead.
  memgraph::tests::StubPropertyFGAChecker<memgraph::query::DbAccessor> checker(&dba, {{"Person", "ssn"}});
  auto context = MakeContext(this->storage, symbol_table, &dba);
  context.auth_checker = &checker;
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 2);
  EXPECT_EQ(results[0][0].ValueString(), "Alice");
  EXPECT_TRUE(results[0][1].IsNull());
}

TYPED_TEST(QueryPlanTest, CachePropertiesDeniedEdgePropertyIsNull) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto edge_type = dba.NameToEdgeType("Pays");
  auto prop_amount = dba.NameToProperty("amount");
  auto prop_account = dba.NameToProperty("account");

  auto from = dba.InsertVertex();
  auto to = dba.InsertVertex();
  auto edge = dba.InsertEdge(&from, &to, edge_type).value();
  ASSERT_TRUE(edge.SetProperty(prop_amount, memgraph::storage::PropertyValue(5)).has_value());
  ASSERT_TRUE(edge.SetProperty(prop_account, memgraph::storage::PropertyValue("secret")).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto r_m = MakeExpand(this->storage,
                        symbol_table,
                        n.op_,
                        n.sym_,
                        "r",
                        EdgeAtom::Direction::OUT,
                        {},
                        "m",
                        false,
                        memgraph::storage::View::OLD);

  auto cached_amount_sym = symbol_table.CreateSymbol("cached_amount", true);
  auto cached_account_sym = symbol_table.CreateSymbol("cached_account", true);
  auto cache = std::make_shared<CacheProperties>(
      r_m.op_,
      r_m.edge_sym_,
      std::vector<CachedProperty>{{prop_amount, cached_amount_sym}, {prop_account, cached_account_sym}});

  auto amount_ne = NEXPR("amount", IDENT("cached_amount")->MapTo(cached_amount_sym))
                       ->MapTo(symbol_table.CreateSymbol("amount_ne", true));
  auto account_ne = NEXPR("account", IDENT("cached_account")->MapTo(cached_account_sym))
                        ->MapTo(symbol_table.CreateSymbol("account_ne", true));
  auto produce = MakeProduce(cache, amount_ne, account_ne);

  // The same rule for a relationship's property: denied reads as Null, cached or not.
  memgraph::tests::StubPropertyFGAChecker<memgraph::query::DbAccessor> checker(&dba, {{"Pays", "account"}});
  auto context = MakeContext(this->storage, symbol_table, &dba);
  context.auth_checker = &checker;
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 2);
  EXPECT_EQ(results[0][0].ValueInt(), 5);
  EXPECT_TRUE(results[0][1].IsNull());
}
#endif

TYPED_TEST(QueryPlanTest, CachePropertiesAbsentPropertyIsNull) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto prop_a = dba.NameToProperty("a");
  auto prop_missing = dba.NameToProperty("missing");

  auto v = dba.InsertVertex();
  ASSERT_TRUE(v.SetProperty(prop_a, memgraph::storage::PropertyValue(1)).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto cached_missing_sym = symbol_table.CreateSymbol("cached_missing", true);
  auto cache =
      std::make_shared<CacheProperties>(n.op_, n.sym_, std::vector<CachedProperty>{{prop_missing, cached_missing_sym}});

  auto ne = NEXPR("missing", IDENT("cached_missing")->MapTo(cached_missing_sym))
                ->MapTo(symbol_table.CreateSymbol("missing_ne", true));
  auto produce = MakeProduce(cache, ne);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 1);
  EXPECT_TRUE(results[0][0].IsNull());
}

TYPED_TEST(QueryPlanTest, CachePropertiesNullInputSymbolYieldsNullSlots) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto prop_a = dba.NameToProperty("a");

  SymbolTable symbol_table;
  // Stands in for an OPTIONAL MATCH that found nothing: the symbol is bound to Null.
  auto unwind = MakeUnwind(symbol_table, "n", nullptr, LIST(LITERAL(TypedValue())));
  auto cached_a_sym = symbol_table.CreateSymbol("cached_a", true);
  auto cache =
      std::make_shared<CacheProperties>(unwind.op_, unwind.sym_, std::vector<CachedProperty>{{prop_a, cached_a_sym}});

  auto ne = NEXPR("a", IDENT("cached_a")->MapTo(cached_a_sym))->MapTo(symbol_table.CreateSymbol("a_ne", true));
  auto produce = MakeProduce(cache, ne);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 1);
  EXPECT_TRUE(results[0][0].IsNull());
}

// An edge keeps its properties in the same store a vertex does, so the operator reads a set of them
// the same way and the symbol it is given may be bound to either.
TYPED_TEST(QueryPlanTest, CachePropertiesReadsAnEdgesProperties) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto prop_a = dba.NameToProperty("a");
  auto prop_b = dba.NameToProperty("b");

  auto from = dba.InsertVertex();
  auto to = dba.InsertVertex();
  auto edge = dba.InsertEdge(&from, &to, dba.NameToEdgeType("et")).value();
  ASSERT_TRUE(edge.SetProperty(prop_a, memgraph::storage::PropertyValue(11)).has_value());
  ASSERT_TRUE(edge.SetProperty(prop_b, memgraph::storage::PropertyValue("eleven")).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto r_m = MakeExpand(this->storage,
                        symbol_table,
                        n.op_,
                        n.sym_,
                        "r",
                        EdgeAtom::Direction::OUT,
                        {},
                        "m",
                        false,
                        memgraph::storage::View::OLD);

  auto cached_a_sym = symbol_table.CreateSymbol("cached_a", true);
  auto cached_b_sym = symbol_table.CreateSymbol("cached_b", true);
  auto cache = std::make_shared<CacheProperties>(
      r_m.op_, r_m.edge_sym_, std::vector<CachedProperty>{{prop_a, cached_a_sym}, {prop_b, cached_b_sym}});

  auto a_ne = NEXPR("a", IDENT("cached_a")->MapTo(cached_a_sym))->MapTo(symbol_table.CreateSymbol("a_ne", true));
  auto b_ne = NEXPR("b", IDENT("cached_b")->MapTo(cached_b_sym))->MapTo(symbol_table.CreateSymbol("b_ne", true));
  auto produce = MakeProduce(cache, a_ne, b_ne);

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 2);
  EXPECT_EQ(results[0][0].ValueInt(), 11);
  EXPECT_EQ(results[0][1].ValueString(), "eleven");
}

// Lookups spread over two symbols are two records, so they are two reads whatever the plan looks
// like. One operator holding both keeps that to one pull and one buffer for the row.
TYPED_TEST(QueryPlanTest, CachePropertiesReadsTwoSymbolsInOneOperator) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto prop_a = dba.NameToProperty("a");
  auto prop_b = dba.NameToProperty("b");

  auto from = dba.InsertVertex();
  ASSERT_TRUE(from.SetProperty(prop_a, memgraph::storage::PropertyValue(1)).has_value());
  ASSERT_TRUE(from.SetProperty(prop_b, memgraph::storage::PropertyValue(2)).has_value());
  auto to = dba.InsertVertex();
  ASSERT_TRUE(to.SetProperty(prop_a, memgraph::storage::PropertyValue(3)).has_value());
  ASSERT_TRUE(to.SetProperty(prop_b, memgraph::storage::PropertyValue(4)).has_value());
  ASSERT_TRUE(dba.InsertEdge(&from, &to, dba.NameToEdgeType("et")).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto r_m = MakeExpand(this->storage,
                        symbol_table,
                        n.op_,
                        n.sym_,
                        "r",
                        EdgeAtom::Direction::OUT,
                        {},
                        "m",
                        false,
                        memgraph::storage::View::OLD);

  auto n_a = symbol_table.CreateSymbol("n_a", true);
  auto n_b = symbol_table.CreateSymbol("n_b", true);
  auto m_a = symbol_table.CreateSymbol("m_a", true);
  auto m_b = symbol_table.CreateSymbol("m_b", true);

  auto cache = std::make_shared<CacheProperties>(r_m.op_,
                                                 std::vector<CachedPropertyGroup>{
                                                     {n.sym_, {{prop_a, n_a}, {prop_b, n_b}}},
                                                     {r_m.node_sym_, {{prop_a, m_a}, {prop_b, m_b}}},
                                                 });

  auto produce = MakeProduce(cache,
                             NEXPR("na", IDENT("n_a")->MapTo(n_a))->MapTo(symbol_table.CreateSymbol("na", true)),
                             NEXPR("nb", IDENT("n_b")->MapTo(n_b))->MapTo(symbol_table.CreateSymbol("nb", true)),
                             NEXPR("ma", IDENT("m_a")->MapTo(m_a))->MapTo(symbol_table.CreateSymbol("ma", true)),
                             NEXPR("mb", IDENT("m_b")->MapTo(m_b))->MapTo(symbol_table.CreateSymbol("mb", true)));

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 4);
  EXPECT_EQ(results[0][0].ValueInt(), 1);
  EXPECT_EQ(results[0][1].ValueInt(), 2);
  EXPECT_EQ(results[0][2].ValueInt(), 3);
  EXPECT_EQ(results[0][3].ValueInt(), 4);
}

// A row where one symbol found nothing says nothing about the others, so a group that cannot be
// read nulls its own slots and the rest of the row is read as it would have been.
TYPED_TEST(QueryPlanTest, CachePropertiesOneGroupWithoutARecordLeavesTheOthers) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto prop_a = dba.NameToProperty("a");
  auto prop_b = dba.NameToProperty("b");

  auto v = dba.InsertVertex();
  ASSERT_TRUE(v.SetProperty(prop_a, memgraph::storage::PropertyValue(8)).has_value());
  ASSERT_TRUE(v.SetProperty(prop_b, memgraph::storage::PropertyValue(9)).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  // Stands in for an OPTIONAL MATCH that found nothing: the symbol is bound to Null.
  auto missing = MakeUnwind(symbol_table, "x", n.op_, LIST(LITERAL(TypedValue())));

  auto x_a = symbol_table.CreateSymbol("x_a", true);
  auto n_a = symbol_table.CreateSymbol("n_a", true);
  auto n_b = symbol_table.CreateSymbol("n_b", true);

  auto cache = std::make_shared<CacheProperties>(missing.op_,
                                                 std::vector<CachedPropertyGroup>{
                                                     {missing.sym_, {{prop_a, x_a}}},
                                                     {n.sym_, {{prop_a, n_a}, {prop_b, n_b}}},
                                                 });

  auto produce = MakeProduce(cache,
                             NEXPR("xa", IDENT("x_a")->MapTo(x_a))->MapTo(symbol_table.CreateSymbol("xa", true)),
                             NEXPR("na", IDENT("n_a")->MapTo(n_a))->MapTo(symbol_table.CreateSymbol("na", true)),
                             NEXPR("nb", IDENT("n_b")->MapTo(n_b))->MapTo(symbol_table.CreateSymbol("nb", true)));

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 3);
  EXPECT_TRUE(results[0][0].IsNull());
  EXPECT_EQ(results[0][1].ValueInt(), 8);
  EXPECT_EQ(results[0][2].ValueInt(), 9);
}

// A plan is cloned before it is run, so whatever establishes the order a record must be read in has
// to survive being copied. The group here names its properties against the order their ids sort, so
// a clone that lost the order would read Null for the one it had already passed.
TYPED_TEST(QueryPlanTest, CachePropertiesKeepsItsReadOrderThroughAClone) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());

  // Interned in this order, so "zebra" holds the lower id while being named second below.
  auto prop_zebra = dba.NameToProperty("zebra");
  auto prop_apple = dba.NameToProperty("apple");
  ASSERT_LT(prop_zebra.AsUint(), prop_apple.AsUint());

  auto v = dba.InsertVertex();
  ASSERT_TRUE(v.SetProperty(prop_zebra, memgraph::storage::PropertyValue(1)).has_value());
  ASSERT_TRUE(v.SetProperty(prop_apple, memgraph::storage::PropertyValue(2)).has_value());
  dba.AdvanceCommand();

  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto n_apple = symbol_table.CreateSymbol("n_apple", true);
  auto n_zebra = symbol_table.CreateSymbol("n_zebra", true);

  auto cache = std::make_shared<CacheProperties>(
      n.op_, std::vector<CachedPropertyGroup>{{n.sym_, {{prop_apple, n_apple}, {prop_zebra, n_zebra}}}});

  auto cloned = std::shared_ptr<LogicalOperator>{cache->Clone(&this->storage)};
  auto produce =
      MakeProduce(cloned,
                  NEXPR("apple", IDENT("n_apple")->MapTo(n_apple))->MapTo(symbol_table.CreateSymbol("apple", true)),
                  NEXPR("zebra", IDENT("n_zebra")->MapTo(n_zebra))->MapTo(symbol_table.CreateSymbol("zebra", true)));

  auto context = MakeContext(this->storage, symbol_table, &dba);
  auto results = CollectProduce(*produce, &context);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 2);
  EXPECT_EQ(results[0][0].ValueInt(), 2);
  EXPECT_EQ(results[0][1].ValueInt(), 1);
}

TYPED_TEST(QueryPlanTest, CachePropertiesModifiedSymbols) {
  auto storage_dba = this->db->Access(memgraph::storage::WRITE);
  memgraph::query::DbAccessor dba(storage_dba.get());
  auto prop_a = dba.NameToProperty("a");
  auto prop_b = dba.NameToProperty("b");

  SymbolTable symbol_table;
  auto n = MakeScanAll(this->storage, symbol_table, "n");
  auto cached_a_sym = symbol_table.CreateSymbol("cached_a", true);
  auto cached_b_sym = symbol_table.CreateSymbol("cached_b", true);
  auto cache = std::make_shared<CacheProperties>(
      n.op_, n.sym_, std::vector<CachedProperty>{{prop_a, cached_a_sym}, {prop_b, cached_b_sym}});

  EXPECT_THAT(cache->ModifiedSymbols(symbol_table), UnorderedElementsAre(n.sym_, cached_a_sym, cached_b_sym));
  // Not a column-defining operator, so it just forwards its input's output symbols.
  EXPECT_EQ(cache->OutputSymbols(symbol_table), n.op_->OutputSymbols(symbol_table));
}
