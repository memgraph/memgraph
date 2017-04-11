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
#include "query/frontend/interpret/interpret.hpp"
#include "query/frontend/logical/operator.hpp"

#include "query_plan_common.hpp"

using namespace query;
using namespace query::plan;

TEST(QueryPlan, Accumulate) {
  // simulate the following two query execution on an empty db
  // CREATE ({x:0})-[:T]->({x:0})
  // MATCH (n)--(m) SET n.x = n.x + 1, m.x = m.x + 1 RETURN n.x, m.x
  // without accumulation we expected results to be [[1, 1], [2, 2]]
  // with accumulation we expect them to be [[2, 2], [2, 2]]

  auto check = [&](bool accumulate) {
    Dbms dbms;
    auto dba = dbms.active();
    auto prop = dba->property("x");

    auto v1 = dba->insert_vertex();
    v1.PropsSet(prop, 0);
    auto v2 = dba->insert_vertex();
    v2.PropsSet(prop, 0);
    dba->insert_edge(v1, v2, dba->edge_type("T"));
    dba->advance_command();

    AstTreeStorage storage;
    SymbolTable symbol_table;

    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m = MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                          EdgeAtom::Direction::BOTH, false, "m", false);

    auto one = LITERAL(1);
    auto n_p = PROPERTY_LOOKUP("n", prop);
    symbol_table[*n_p->expression_] = n.sym_;
    auto set_n_p =
        std::make_shared<plan::SetProperty>(r_m.op_, n_p, ADD(n_p, one));
    auto m_p = PROPERTY_LOOKUP("m", prop);
    symbol_table[*m_p->expression_] = r_m.node_sym_;
    auto set_m_p =
        std::make_shared<plan::SetProperty>(set_n_p, m_p, ADD(m_p, one));

    std::shared_ptr<LogicalOperator> last_op = set_m_p;
    if (accumulate) {
      last_op = std::make_shared<Accumulate>(
          last_op, std::vector<Symbol>{n.sym_, r_m.node_sym_});
    }

    auto n_p_ne = NEXPR("n.p", n_p);
    symbol_table[*n_p_ne] = symbol_table.CreateSymbol("n_p_ne");
    auto m_p_ne = NEXPR("m.p", m_p);
    symbol_table[*m_p_ne] = symbol_table.CreateSymbol("m_p_ne");
    auto produce = MakeProduce(last_op, n_p_ne, m_p_ne);
    ResultStreamFaker results = CollectProduce(produce, symbol_table, *dba);
    std::vector<int> results_data;
    for (const auto &row : results.GetResults())
      for (const auto &column : row)
        results_data.emplace_back(column.Value<int64_t>());
    if (accumulate)
      EXPECT_THAT(results_data, testing::ElementsAre(2, 2, 2, 2));
    else
      EXPECT_THAT(results_data, testing::ElementsAre(1, 1, 2, 2));
  };

  check(false);
  check(true);
}

TEST(QueryPlan, AccumulateAdvance) {
  // we simulate 'CREATE (n) WITH n AS n MATCH (m) RETURN m'
  // to get correct results we need to advance the command

  auto check = [&](bool advance) {
    Dbms dbms;
    auto dba = dbms.active();
    AstTreeStorage storage;
    SymbolTable symbol_table;

    auto node = NODE("n");
    auto sym_n = symbol_table.CreateSymbol("n");
    symbol_table[*node->identifier_] = sym_n;
    auto create = std::make_shared<CreateNode>(node, nullptr);
    auto accumulate = std::make_shared<Accumulate>(
        create, std::vector<Symbol>{sym_n}, advance);
    auto match = MakeScanAll(storage, symbol_table, "m", accumulate);
    EXPECT_EQ(advance ? 1 : 0, PullAll(match.op_, *dba, symbol_table));
  };
  check(false);
  check(true);
}

std::shared_ptr<Produce> MakeAggregationProduce(
    std::shared_ptr<LogicalOperator> input, SymbolTable &symbol_table,
    AstTreeStorage &storage, const std::vector<Expression *> aggr_inputs,
    const std::vector<Aggregation::Op> aggr_ops,
    const std::vector<Expression *> group_by_exprs,
    const std::vector<Symbol> remember) {
  permanent_assert(aggr_inputs.size() == aggr_ops.size(),
                   "Provide as many aggr inputs as aggr ops");
  // prepare all the aggregations
  std::vector<Aggregate::Element> aggregates;
  std::vector<NamedExpression *> named_expressions;

  auto aggr_inputs_it = aggr_inputs.begin();
  for (auto aggr_op : aggr_ops) {
    // TODO change this from using IDENT to using AGGREGATION
    // once AGGREGATION is handled properly in ExpressionEvaluation
    auto named_expr = NEXPR("", IDENT("aggregation"));
    named_expressions.push_back(named_expr);
    symbol_table[*named_expr->expression_] =
        symbol_table.CreateSymbol("aggregation");
    symbol_table[*named_expr] = symbol_table.CreateSymbol("named_expression");
    aggregates.emplace_back(*aggr_inputs_it++, aggr_op,
                            symbol_table[*named_expr->expression_]);
  }

  // Produce will also evaluate group_by expressions
  // and return them after the aggregations
  for (auto group_by_expr : group_by_exprs) {
    auto named_expr = NEXPR("", group_by_expr);
    named_expressions.push_back(named_expr);
    symbol_table[*named_expr] = symbol_table.CreateSymbol("named_expression");
  }
  auto aggregation =
      std::make_shared<Aggregate>(input, aggregates, group_by_exprs, remember);
  return std::make_shared<Produce>(aggregation, named_expressions);
}

TEST(QueryPlan, AggregateOps) {
  Dbms dbms;
  auto dba = dbms.active();

  // setup is several nodes most of which have an int property set
  // we will take the sum, avg, min, max and count
  // we won't group by anything
  auto prop = dba->property("prop");
  dba->insert_vertex().PropsSet(prop, 4);
  dba->insert_vertex().PropsSet(prop, 7);
  dba->insert_vertex().PropsSet(prop, 12);
  // a missing property (null) gets ignored by all aggregations
  dba->insert_vertex();
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // match all nodes and perform aggregations
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_p = PROPERTY_LOOKUP("n", prop);
  symbol_table[*n_p->expression_] = n.sym_;

  auto produce = MakeAggregationProduce(
      n.op_, symbol_table, storage, std::vector<Expression *>(5, n_p),
      {Aggregation::Op::COUNT, Aggregation::Op::MIN, Aggregation::Op::MAX,
       Aggregation::Op::SUM, Aggregation::Op::AVG},
      {}, {});

  // checks
  auto results = CollectProduce(produce, symbol_table, *dba).GetResults();
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 5);
  // count
  ASSERT_EQ(results[0][0].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][0].Value<int64_t>(), 3);
  // min
  ASSERT_EQ(results[0][1].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][1].Value<int64_t>(), 4);
  // max
  ASSERT_EQ(results[0][2].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][2].Value<int64_t>(), 12);
  // sum
  ASSERT_EQ(results[0][3].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][3].Value<int64_t>(), 23);
  // avg
  ASSERT_EQ(results[0][4].type(), TypedValue::Type::Double);
  EXPECT_FLOAT_EQ(results[0][4].Value<double>(), 23 / 3.0);
}

TEST(QueryPlan, AggregateGroupByValues) {
  // tests that distinct groups are aggregated properly
  // for values of all types
  // also test the "remember" part of the Aggregation API
  // as final results are obtained via a property lookup of
  // a remembered node
  Dbms dbms;
  auto dba = dbms.active();

  // a vector of TypedValue to be set as property values on vertices
  // most of them should result in a distinct group (commented where not)
  std::vector<TypedValue> group_by_vals;
  group_by_vals.emplace_back(4);
  group_by_vals.emplace_back(7);
  group_by_vals.emplace_back(7.3);
  group_by_vals.emplace_back(7.2);
  group_by_vals.emplace_back("Johhny");
  group_by_vals.emplace_back("Jane");
  group_by_vals.emplace_back("1");
  group_by_vals.emplace_back(true);
  group_by_vals.emplace_back(false);
  group_by_vals.emplace_back(std::vector<TypedValue>{1});
  group_by_vals.emplace_back(std::vector<TypedValue>{1, 2});
  group_by_vals.emplace_back(std::vector<TypedValue>{2, 1});
  group_by_vals.emplace_back(TypedValue::Null);
  // should NOT result in another group because 7.0 == 7
  group_by_vals.emplace_back(7.0);
  // should NOT result in another group
  group_by_vals.emplace_back(std::vector<TypedValue>{1, 2.0});

  // generate a lot of vertices and set props on them
  auto prop = dba->property("prop");
  for (int i = 0; i < 1000; ++i)
    dba->insert_vertex().PropsSet(prop,
                                  group_by_vals[i % group_by_vals.size()]);
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // match all nodes and perform aggregations
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_p = PROPERTY_LOOKUP("n", prop);
  symbol_table[*n_p->expression_] = n.sym_;

  auto produce =
      MakeAggregationProduce(n.op_, symbol_table, storage, {n_p},
                             {Aggregation::Op::COUNT}, {n_p}, {n.sym_});

  auto results = CollectProduce(produce, symbol_table, *dba).GetResults();
  ASSERT_EQ(results.size(), group_by_vals.size() - 2);
  TypedValue::unordered_set result_group_bys;
  for (const auto &row : results) {
    ASSERT_EQ(2, row.size());
    result_group_bys.insert(row[1]);
  }
  ASSERT_EQ(result_group_bys.size(), group_by_vals.size() - 2);
  EXPECT_TRUE(std::is_permutation(
      group_by_vals.begin(), group_by_vals.end() - 2, result_group_bys.begin(),
      TypedValue::BoolEqual{}));
}

TEST(QueryPlan, AggregateMultipleGroupBy) {
  // in this test we have 3 different properties that have different values
  // for different records and assert that we get the correct combination
  // of values in our groups
  Dbms dbms;
  auto dba = dbms.active();

  auto prop1 = dba->property("prop1");
  auto prop2 = dba->property("prop2");
  auto prop3 = dba->property("prop3");
  for (int i = 0; i < 2 * 3 * 5; ++i) {
    auto v = dba->insert_vertex();
    v.PropsSet(prop1, (bool)(i % 2));
    v.PropsSet(prop2, i % 3);
    v.PropsSet(prop3, "value" + std::to_string(i % 5));
  }
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  // match all nodes and perform aggregations
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_p1 = PROPERTY_LOOKUP("n", prop1);
  auto n_p2 = PROPERTY_LOOKUP("n", prop2);
  auto n_p3 = PROPERTY_LOOKUP("n", prop3);
  symbol_table[*n_p1->expression_] = n.sym_;
  symbol_table[*n_p2->expression_] = n.sym_;
  symbol_table[*n_p3->expression_] = n.sym_;

  auto produce = MakeAggregationProduce(n.op_, symbol_table, storage, {n_p1},
                                        {Aggregation::Op::COUNT},
                                        {n_p1, n_p2, n_p3}, {n.sym_});

  auto results = CollectProduce(produce, symbol_table, *dba).GetResults();
  EXPECT_EQ(results.size(), 2 * 3 * 5);
}

TEST(QueryPlan, AggregateAdvance) {
  // we simulate 'CREATE (n {x: 42}) WITH count(n.x) AS c MATCH (m) RETURN m,
  // m.x, c'
  // to get correct results we need to advance the command in aggregation
  // since we only test aggregation, we'll simplify the logical plan and only
  // check the count and not all the results

  auto check = [&](bool advance) {
    Dbms dbms;
    auto dba = dbms.active();
    AstTreeStorage storage;
    SymbolTable symbol_table;

    auto node = NODE("n");
    auto sym_n = symbol_table.CreateSymbol("n");
    symbol_table[*node->identifier_] = sym_n;
    auto create = std::make_shared<CreateNode>(node, nullptr);

    auto aggr_sym = symbol_table.CreateSymbol("aggr_sym");
    auto n_p = PROPERTY_LOOKUP("n", dba->property("x"));
    symbol_table[*n_p->expression_] = sym_n;
    auto aggregate = std::make_shared<Aggregate>(
        create, std::vector<Aggregate::Element>{Aggregate::Element{
                    n_p, Aggregation::Op::COUNT, aggr_sym}},
        std::vector<Expression *>{}, std::vector<Symbol>{}, advance);
    auto match = MakeScanAll(storage, symbol_table, "m", aggregate);
    EXPECT_EQ(advance ? 1 : 0, PullAll(match.op_, *dba, symbol_table));
  };
//  check(false);
  check(true);
}

TEST(QueryPlan, AggregateTypes) {
  // testing exceptions that can get emitted by an aggregation
  // does not check all combinations that can result in an exception
  // (that logic is defined and tested by TypedValue)

  Dbms dbms;
  auto dba = dbms.active();

  auto p1 = dba->property("p1");  // has only string props
  dba->insert_vertex().PropsSet(p1, "string");
  dba->insert_vertex().PropsSet(p1, "str2");
  auto p2 = dba->property("p2");  // combines int and bool
  dba->insert_vertex().PropsSet(p2, 42);
  dba->insert_vertex().PropsSet(p2, true);
  dba->advance_command();

  AstTreeStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_p1 = PROPERTY_LOOKUP("n", p1);
  symbol_table[*n_p1->expression_] = n.sym_;
  auto n_p2 = PROPERTY_LOOKUP("n", p2);
  symbol_table[*n_p2->expression_] = n.sym_;

  auto aggregate = [&](Expression *expression, Aggregation::Op aggr_op) {
    auto produce = MakeAggregationProduce(n.op_, symbol_table, storage,
                                          {expression}, {aggr_op}, {}, {});
    CollectProduce(produce, symbol_table, *dba).GetResults();
  };

  // everythin except for COUNT fails on a Vertex
  auto n_id = n_p1->expression_;
  aggregate(n_id, Aggregation::Op::COUNT);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::MIN), TypedValueException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::MAX), TypedValueException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::AVG), TypedValueException);
  EXPECT_THROW(aggregate(n_id, Aggregation::Op::SUM), TypedValueException);

  // on strings AVG and SUM fail
  aggregate(n_p1, Aggregation::Op::COUNT);
  aggregate(n_p1, Aggregation::Op::MIN);
  aggregate(n_p1, Aggregation::Op::MAX);
  EXPECT_THROW(aggregate(n_p1, Aggregation::Op::AVG), TypedValueException);
  EXPECT_THROW(aggregate(n_p1, Aggregation::Op::SUM), TypedValueException);

  // combination of int and bool, everything except count fails
  aggregate(n_p2, Aggregation::Op::COUNT);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::MIN), TypedValueException);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::MAX), TypedValueException);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::AVG), TypedValueException);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::SUM), TypedValueException);
}
