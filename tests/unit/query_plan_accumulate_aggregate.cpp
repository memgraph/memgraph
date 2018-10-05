#include <algorithm>
#include <iterator>
#include <memory>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/result_stream_faker.hpp"
#include "database/single_node/graph_db.hpp"
#include "query/context.hpp"
#include "query/exceptions.hpp"
#include "query/plan/operator.hpp"
#include "query_plan_common.hpp"

using namespace query;
using namespace query::plan;
using query::test_common::ToList;
using query::test_common::ToMap;
using testing::UnorderedElementsAre;

TEST(QueryPlan, Accumulate) {
  // simulate the following two query execution on an empty db
  // CREATE ({x:0})-[:T]->({x:0})
  // MATCH (n)--(m) SET n.x = n.x + 1, m.x = m.x + 1 RETURN n.x, m.x
  // without accumulation we expected results to be [[1, 1], [2, 2]]
  // with accumulation we expect them to be [[2, 2], [2, 2]]

  auto check = [&](bool accumulate) {
    database::SingleNode db;
    auto dba_ptr = db.Access();
    auto &dba = *dba_ptr;
    auto prop = dba.Property("x");

    auto v1 = dba.InsertVertex();
    v1.PropsSet(prop, 0);
    auto v2 = dba.InsertVertex();
    v2.PropsSet(prop, 0);
    dba.InsertEdge(v1, v2, dba.EdgeType("T"));
    dba.AdvanceCommand();

    AstStorage storage;
    SymbolTable symbol_table;

    auto n = MakeScanAll(storage, symbol_table, "n");
    auto r_m =
        MakeExpand(storage, symbol_table, n.op_, n.sym_, "r",
                   EdgeAtom::Direction::BOTH, {}, "m", false, GraphView::OLD);

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
    symbol_table[*n_p_ne] = symbol_table.CreateSymbol("n_p_ne", true);
    auto m_p_ne = NEXPR("m.p", m_p);
    symbol_table[*m_p_ne] = symbol_table.CreateSymbol("m_p_ne", true);
    auto produce = MakeProduce(last_op, n_p_ne, m_p_ne);
    auto results = CollectProduce(produce.get(), symbol_table, dba);
    std::vector<int> results_data;
    for (const auto &row : results)
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
    database::SingleNode db;
    auto dba = db.Access();
    AstStorage storage;
    SymbolTable symbol_table;

    auto node = NODE("n");
    auto sym_n = symbol_table.CreateSymbol("n", true);
    symbol_table[*node->identifier_] = sym_n;
    auto create = std::make_shared<CreateNode>(nullptr, node);
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
    AstStorage &storage, const std::vector<Expression *> aggr_inputs,
    const std::vector<Aggregation::Op> aggr_ops,
    const std::vector<Expression *> group_by_exprs,
    const std::vector<Symbol> remember) {
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
        symbol_table.CreateSymbol("aggregation", true);
    symbol_table[*named_expr] =
        symbol_table.CreateSymbol("named_expression", true);
    // the key expression is only used in COLLECT_MAP
    Expression *key_expr_ptr =
        aggr_op == Aggregation::Op::COLLECT_MAP ? LITERAL("key") : nullptr;
    aggregates.emplace_back(
        Aggregate::Element{*aggr_inputs_it++, key_expr_ptr, aggr_op,
                           symbol_table[*named_expr->expression_]});
  }

  // Produce will also evaluate group_by expressions and return them after the
  // aggregations.
  for (auto group_by_expr : group_by_exprs) {
    auto named_expr = NEXPR("", group_by_expr);
    named_expressions.push_back(named_expr);
    symbol_table[*named_expr] =
        symbol_table.CreateSymbol("named_expression", true);
  }
  auto aggregation =
      std::make_shared<Aggregate>(input, aggregates, group_by_exprs, remember);
  return std::make_shared<Produce>(aggregation, named_expressions);
}

/** Test fixture for all the aggregation ops in one return. */
class QueryPlanAggregateOps : public ::testing::Test {
 protected:
  database::SingleNode db;
  std::unique_ptr<database::GraphDbAccessor> dba_ptr{db.Access()};
  database::GraphDbAccessor &dba{*dba_ptr};
  storage::Property prop = dba.Property("prop");

  AstStorage storage;
  SymbolTable symbol_table;

  void AddData() {
    // setup is several nodes most of which have an int property set
    // we will take the sum, avg, min, max and count
    // we won't group by anything
    dba.InsertVertex().PropsSet(prop, 5);
    dba.InsertVertex().PropsSet(prop, 7);
    dba.InsertVertex().PropsSet(prop, 12);
    // a missing property (null) gets ignored by all aggregations except
    // COUNT(*)
    dba.InsertVertex();
    dba.AdvanceCommand();
  }

  auto AggregationResults(bool with_group_by,
                          std::vector<Aggregation::Op> ops = {
                              Aggregation::Op::COUNT, Aggregation::Op::COUNT,
                              Aggregation::Op::MIN, Aggregation::Op::MAX,
                              Aggregation::Op::SUM, Aggregation::Op::AVG,
                              Aggregation::Op::COLLECT_LIST,
                              Aggregation::Op::COLLECT_MAP}) {
    // match all nodes and perform aggregations
    auto n = MakeScanAll(storage, symbol_table, "n");
    auto n_p = PROPERTY_LOOKUP("n", prop);
    symbol_table[*n_p->expression_] = n.sym_;

    std::vector<Expression *> aggregation_expressions(ops.size(), n_p);
    std::vector<Expression *> group_bys;
    if (with_group_by) group_bys.push_back(n_p);
    aggregation_expressions[0] = nullptr;
    auto produce =
        MakeAggregationProduce(n.op_, symbol_table, storage,
                               aggregation_expressions, ops, group_bys, {});
    return CollectProduce(produce.get(), symbol_table, dba);
  }
};

TEST_F(QueryPlanAggregateOps, WithData) {
  AddData();
  auto results = AggregationResults(false);

  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 8);
  // count(*)
  ASSERT_EQ(results[0][0].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][0].Value<int64_t>(), 4);
  // count
  ASSERT_EQ(results[0][1].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][1].Value<int64_t>(), 3);
  // min
  ASSERT_EQ(results[0][2].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][2].Value<int64_t>(), 5);
  // max
  ASSERT_EQ(results[0][3].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][3].Value<int64_t>(), 12);
  // sum
  ASSERT_EQ(results[0][4].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][4].Value<int64_t>(), 24);
  // avg
  ASSERT_EQ(results[0][5].type(), TypedValue::Type::Double);
  EXPECT_FLOAT_EQ(results[0][5].Value<double>(), 24 / 3.0);
  // collect list
  ASSERT_EQ(results[0][6].type(), TypedValue::Type::List);
  EXPECT_THAT(ToList<int64_t>(results[0][6]), UnorderedElementsAre(5, 7, 12));
  // collect map
  ASSERT_EQ(results[0][7].type(), TypedValue::Type::Map);
  auto map = ToMap<int64_t>(results[0][7]);
  ASSERT_EQ(map.size(), 1);
  EXPECT_EQ(map.begin()->first, "key");
  EXPECT_FALSE(std::set<int>({5, 7, 12}).insert(map.begin()->second).second);
}

TEST_F(QueryPlanAggregateOps, WithoutDataWithGroupBy) {
  {
    auto results = AggregationResults(true, {Aggregation::Op::COUNT});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = AggregationResults(true, {Aggregation::Op::SUM});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = AggregationResults(true, {Aggregation::Op::AVG});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = AggregationResults(true, {Aggregation::Op::MIN});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = AggregationResults(true, {Aggregation::Op::MAX});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = AggregationResults(true, {Aggregation::Op::COLLECT_LIST});
    EXPECT_EQ(results.size(), 0);
  }
  {
    auto results = AggregationResults(true, {Aggregation::Op::COLLECT_MAP});
    EXPECT_EQ(results.size(), 0);
  }
}

TEST_F(QueryPlanAggregateOps, WithoutDataWithoutGroupBy) {
  auto results = AggregationResults(false);
  ASSERT_EQ(results.size(), 1);
  ASSERT_EQ(results[0].size(), 8);
  // count(*)
  ASSERT_EQ(results[0][0].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][0].Value<int64_t>(), 0);
  // count
  ASSERT_EQ(results[0][1].type(), TypedValue::Type::Int);
  EXPECT_EQ(results[0][1].Value<int64_t>(), 0);
  // min
  EXPECT_TRUE(results[0][2].IsNull());
  // max
  EXPECT_TRUE(results[0][3].IsNull());
  // sum
  EXPECT_TRUE(results[0][4].IsNull());
  // avg
  EXPECT_TRUE(results[0][5].IsNull());
  // collect list
  ASSERT_EQ(results[0][6].type(), TypedValue::Type::List);
  EXPECT_EQ(ToList<int64_t>(results[0][6]).size(), 0);
  // collect map
  ASSERT_EQ(results[0][7].type(), TypedValue::Type::Map);
  EXPECT_EQ(ToMap<int64_t>(results[0][7]).size(), 0);
}

TEST(QueryPlan, AggregateGroupByValues) {
  // Tests that distinct groups are aggregated properly for values of all types.
  // Also test the "remember" part of the Aggregation API as final results are
  // obtained via a property lookup of a remembered node.
  database::SingleNode db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  // a vector of PropertyValue to be set as property values on vertices
  // most of them should result in a distinct group (commented where not)
  std::vector<PropertyValue> group_by_vals;
  group_by_vals.emplace_back(4);
  group_by_vals.emplace_back(7);
  group_by_vals.emplace_back(7.3);
  group_by_vals.emplace_back(7.2);
  group_by_vals.emplace_back("Johhny");
  group_by_vals.emplace_back("Jane");
  group_by_vals.emplace_back("1");
  group_by_vals.emplace_back(true);
  group_by_vals.emplace_back(false);
  group_by_vals.emplace_back(std::vector<PropertyValue>{1});
  group_by_vals.emplace_back(std::vector<PropertyValue>{1, 2});
  group_by_vals.emplace_back(std::vector<PropertyValue>{2, 1});
  group_by_vals.emplace_back(PropertyValue::Null);
  // should NOT result in another group because 7.0 == 7
  group_by_vals.emplace_back(7.0);
  // should NOT result in another group
  group_by_vals.emplace_back(std::vector<PropertyValue>{1, 2.0});

  // generate a lot of vertices and set props on them
  auto prop = dba.Property("prop");
  for (int i = 0; i < 1000; ++i)
    dba.InsertVertex().PropsSet(prop, group_by_vals[i % group_by_vals.size()]);
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  // match all nodes and perform aggregations
  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_p = PROPERTY_LOOKUP("n", prop);
  symbol_table[*n_p->expression_] = n.sym_;

  auto produce =
      MakeAggregationProduce(n.op_, symbol_table, storage, {n_p},
                             {Aggregation::Op::COUNT}, {n_p}, {n.sym_});

  auto results = CollectProduce(produce.get(), symbol_table, dba);
  ASSERT_EQ(results.size(), group_by_vals.size() - 2);
  TypedValue::unordered_set result_group_bys;
  for (const auto &row : results) {
    ASSERT_EQ(2, row.size());
    result_group_bys.insert(row[1]);
  }
  ASSERT_EQ(result_group_bys.size(), group_by_vals.size() - 2);
  EXPECT_TRUE(
      std::is_permutation(group_by_vals.begin(), group_by_vals.end() - 2,
                          result_group_bys.begin(), TypedValue::BoolEqual{}));
}

TEST(QueryPlan, AggregateMultipleGroupBy) {
  // in this test we have 3 different properties that have different values
  // for different records and assert that we get the correct combination
  // of values in our groups
  database::SingleNode db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  auto prop1 = dba.Property("prop1");
  auto prop2 = dba.Property("prop2");
  auto prop3 = dba.Property("prop3");
  for (int i = 0; i < 2 * 3 * 5; ++i) {
    auto v = dba.InsertVertex();
    v.PropsSet(prop1, (bool)(i % 2));
    v.PropsSet(prop2, i % 3);
    v.PropsSet(prop3, "value" + std::to_string(i % 5));
  }
  dba.AdvanceCommand();

  AstStorage storage;
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

  auto results = CollectProduce(produce.get(), symbol_table, dba);
  EXPECT_EQ(results.size(), 2 * 3 * 5);
}

TEST(QueryPlan, AggregateNoInput) {
  database::SingleNode db;
  auto dba = db.Access();
  AstStorage storage;
  SymbolTable symbol_table;

  auto two = LITERAL(2);
  auto output = NEXPR("two", IDENT("two"));
  symbol_table[*output->expression_] = symbol_table.CreateSymbol("two", true);

  auto produce = MakeAggregationProduce(nullptr, symbol_table, storage, {two},
                                        {Aggregation::Op::COUNT}, {}, {});
  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  EXPECT_EQ(1, results.size());
  EXPECT_EQ(1, results[0].size());
  EXPECT_EQ(TypedValue::Type::Int, results[0][0].type());
  EXPECT_EQ(1, results[0][0].Value<int64_t>());
}

TEST(QueryPlan, AggregateCountEdgeCases) {
  // tests for detected bugs in the COUNT aggregation behavior
  // ensure that COUNT returns correctly for
  //  - 0 vertices in database
  //  - 1 vertex in database, property not set
  //  - 1 vertex in database, property set
  //  - 2 vertices in database, property set on one
  //  - 2 vertices in database, property set on both

  database::SingleNode db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;
  auto prop = dba.Property("prop");

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_p = PROPERTY_LOOKUP("n", prop);
  symbol_table[*n_p->expression_] = n.sym_;

  // returns -1 when there are no results
  // otherwise returns MATCH (n) RETURN count(n.prop)
  auto count = [&]() {
    auto produce = MakeAggregationProduce(n.op_, symbol_table, storage, {n_p},
                                          {Aggregation::Op::COUNT}, {}, {});
    auto results = CollectProduce(produce.get(), symbol_table, dba);
    if (results.size() == 0) return -1L;
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(1, results[0].size());
    EXPECT_EQ(TypedValue::Type::Int, results[0][0].type());
    return results[0][0].Value<int64_t>();
  };

  // no vertices yet in database
  EXPECT_EQ(0, count());

  // one vertex, no property set
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(0, count());

  // one vertex, property set
  for (VertexAccessor va : dba.Vertices(false)) va.PropsSet(prop, 42);
  dba.AdvanceCommand();
  EXPECT_EQ(1, count());

  // two vertices, one with property set
  dba.InsertVertex();
  dba.AdvanceCommand();
  EXPECT_EQ(1, count());

  // two vertices, both with property set
  for (VertexAccessor va : dba.Vertices(false)) va.PropsSet(prop, 42);
  dba.AdvanceCommand();
  EXPECT_EQ(2, count());
}

TEST(QueryPlan, AggregateFirstValueTypes) {
  // testing exceptions that get emitted by the first-value
  // type check

  database::SingleNode db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  auto v1 = dba.InsertVertex();
  auto prop_string = dba.Property("string");
  v1.PropsSet(prop_string, "johhny");
  auto prop_int = dba.Property("int");
  v1.PropsSet(prop_int, 12);
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_prop_string = PROPERTY_LOOKUP("n", prop_string);
  symbol_table[*n_prop_string->expression_] = n.sym_;
  auto n_prop_int = PROPERTY_LOOKUP("n", prop_int);
  symbol_table[*n_prop_int->expression_] = n.sym_;
  auto n_id = n_prop_string->expression_;

  auto aggregate = [&](Expression *expression, Aggregation::Op aggr_op) {
    auto produce = MakeAggregationProduce(n.op_, symbol_table, storage,
                                          {expression}, {aggr_op}, {}, {});
    CollectProduce(produce.get(), symbol_table, dba);
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
  EXPECT_THROW(aggregate(n_prop_string, Aggregation::Op::AVG),
               QueryRuntimeException);
  EXPECT_THROW(aggregate(n_prop_string, Aggregation::Op::SUM),
               QueryRuntimeException);

  // on ints nothing fails
  aggregate(n_prop_int, Aggregation::Op::COUNT);
  aggregate(n_prop_int, Aggregation::Op::MIN);
  aggregate(n_prop_int, Aggregation::Op::MAX);
  aggregate(n_prop_int, Aggregation::Op::AVG);
  aggregate(n_prop_int, Aggregation::Op::SUM);
  aggregate(n_prop_int, Aggregation::Op::COLLECT_LIST);
  aggregate(n_prop_int, Aggregation::Op::COLLECT_MAP);
}

TEST(QueryPlan, AggregateTypes) {
  // testing exceptions that can get emitted by an aggregation
  // does not check all combinations that can result in an exception
  // (that logic is defined and tested by TypedValue)

  database::SingleNode db;
  auto dba_ptr = db.Access();
  auto &dba = *dba_ptr;

  auto p1 = dba.Property("p1");  // has only string props
  dba.InsertVertex().PropsSet(p1, "string");
  dba.InsertVertex().PropsSet(p1, "str2");
  auto p2 = dba.Property("p2");  // combines int and bool
  dba.InsertVertex().PropsSet(p2, 42);
  dba.InsertVertex().PropsSet(p2, true);
  dba.AdvanceCommand();

  AstStorage storage;
  SymbolTable symbol_table;

  auto n = MakeScanAll(storage, symbol_table, "n");
  auto n_p1 = PROPERTY_LOOKUP("n", p1);
  symbol_table[*n_p1->expression_] = n.sym_;
  auto n_p2 = PROPERTY_LOOKUP("n", p2);
  symbol_table[*n_p2->expression_] = n.sym_;

  auto aggregate = [&](Expression *expression, Aggregation::Op aggr_op) {
    auto produce = MakeAggregationProduce(n.op_, symbol_table, storage,
                                          {expression}, {aggr_op}, {}, {});
    CollectProduce(produce.get(), symbol_table, dba);
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

  // combination of int and bool, everything except COUNT and COLLECT fails
  aggregate(n_p2, Aggregation::Op::COUNT);
  aggregate(n_p2, Aggregation::Op::COLLECT_LIST);
  aggregate(n_p2, Aggregation::Op::COLLECT_MAP);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::MIN), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::MAX), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::AVG), QueryRuntimeException);
  EXPECT_THROW(aggregate(n_p2, Aggregation::Op::SUM), QueryRuntimeException);
}

TEST(QueryPlan, Unwind) {
  database::SingleNode db;
  auto dba = db.Access();
  AstStorage storage;
  SymbolTable symbol_table;

  // UNWIND [ [1, true, "x"], [], ["bla"] ] AS x UNWIND x as y RETURN x, y
  auto input_expr = storage.Create<PrimitiveLiteral>(std::vector<PropertyValue>{
      std::vector<PropertyValue>{1, true, "x"}, std::vector<PropertyValue>{},
      std::vector<PropertyValue>{"bla"}});

  auto x = symbol_table.CreateSymbol("x", true);
  auto unwind_0 = std::make_shared<plan::Unwind>(nullptr, input_expr, x);
  auto x_expr = IDENT("x");
  symbol_table[*x_expr] = x;
  auto y = symbol_table.CreateSymbol("y", true);
  auto unwind_1 = std::make_shared<plan::Unwind>(unwind_0, x_expr, y);

  auto x_ne = NEXPR("x", x_expr);
  symbol_table[*x_ne] = symbol_table.CreateSymbol("x_ne", true);
  auto y_ne = NEXPR("y", IDENT("y"));
  symbol_table[*y_ne->expression_] = y;
  symbol_table[*y_ne] = symbol_table.CreateSymbol("y_ne", true);
  auto produce = MakeProduce(unwind_1, x_ne, y_ne);

  auto results = CollectProduce(produce.get(), symbol_table, *dba);
  ASSERT_EQ(4, results.size());
  const std::vector<int> expected_x_card{3, 3, 3, 1};
  auto expected_x_card_it = expected_x_card.begin();
  const std::vector<TypedValue> expected_y{1, true, "x", "bla"};
  auto expected_y_it = expected_y.begin();
  for (const auto &row : results) {
    ASSERT_EQ(2, row.size());
    ASSERT_EQ(row[0].type(), TypedValue::Type::List);
    EXPECT_EQ(row[0].Value<std::vector<TypedValue>>().size(),
              *expected_x_card_it);
    EXPECT_EQ(row[1].type(), expected_y_it->type());
    expected_x_card_it++;
    expected_y_it++;
  }
}
