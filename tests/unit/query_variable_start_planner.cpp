#include <algorithm>
#include <variant>

#include "gtest/gtest.h"

#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/planner.hpp"
#include "utils/algorithm.hpp"

#include "query_plan_common.hpp"

#include "formatters.hpp"

using namespace query::plan;
using query::AstStorage;
using Type = query::EdgeAtom::Type;
using Direction = query::EdgeAtom::Direction;

// Functions for printing resulting rows from a query.
template <class TAccessor>
std::string ToString(const std::vector<TypedValue> &row, const TAccessor &acc) {
  std::ostringstream os;
  utils::PrintIterable(os, row, ", ", [&](auto &stream, const auto &item) { stream << ToString(item, acc); });
  return os.str();
}
template <class TAccessor>
std::string ToString(const std::vector<std::vector<TypedValue>> &rows, const TAccessor &acc) {
  std::ostringstream os;
  utils::PrintIterable(os, rows, "\n", [&](auto &stream, const auto &item) { stream << ToString(item, acc); });
  return os.str();
}

namespace {

template <class TAccessor>
void AssertRows(const std::vector<std::vector<TypedValue>> &datum, std::vector<std::vector<TypedValue>> expected,
                const TAccessor &acc) {
  auto row_equal = [](const auto &row1, const auto &row2) {
    if (row1.size() != row2.size()) {
      return false;
    }
    TypedValue::BoolEqual value_eq;
    auto row1_it = row1.begin();
    for (auto row2_it = row2.begin(); row2_it != row2.end(); ++row1_it, ++row2_it) {
      if (!value_eq(*row1_it, *row2_it)) {
        return false;
      }
    }
    return true;
  };
  ASSERT_TRUE(std::is_permutation(datum.begin(), datum.end(), expected.begin(), expected.end(), row_equal))
      << "Actual rows:" << std::endl
      << ToString(datum, acc) << std::endl
      << "Expected rows:" << std::endl
      << ToString(expected, acc);
};

void CheckPlansProduce(size_t expected_plan_count, query::CypherQuery *query, AstStorage &storage,
                       query::DbAccessor *dba,
                       std::function<void(const std::vector<std::vector<TypedValue>> &)> check) {
  auto symbol_table = query::MakeSymbolTable(query);
  auto planning_context = MakePlanningContext(&storage, &symbol_table, query, dba);
  auto query_parts = CollectQueryParts(symbol_table, storage, query);
  EXPECT_TRUE(query_parts.query_parts.size() > 0);
  auto single_query_parts = query_parts.query_parts.at(0).single_query_parts;
  auto plans = MakeLogicalPlanForSingleQuery<VariableStartPlanner>(single_query_parts, &planning_context);
  EXPECT_EQ(std::distance(plans.begin(), plans.end()), expected_plan_count);
  for (const auto &plan : plans) {
    auto *produce = dynamic_cast<Produce *>(plan.get());
    ASSERT_TRUE(produce);
    auto context = MakeContext(storage, symbol_table, dba);
    auto results = CollectProduce(*produce, &context);
    check(results);
  }
}

TEST(TestVariableStartPlanner, MatchReturn) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // Make a graph (v1) -[:r]-> (v2)
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("r")).HasValue());
  dba.AdvanceCommand();
  // Test MATCH (n) -[r]-> (m) RETURN n
  AstStorage storage;
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r", Direction::OUT), NODE("m"))), RETURN("n")));
  // We have 2 nodes `n` and `m` from which we could start, so expect 2 plans.
  CheckPlansProduce(2, query, storage, &dba, [&](const auto &results) {
    // We expect to produce only a single (v1) node.
    AssertRows(results, {{TypedValue(query::VertexAccessor(v1))}}, dba);
  });
}

TEST(TestVariableStartPlanner, MatchTripletPatternReturn) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // Make a graph (v1) -[:r]-> (v2) -[:r]-> (v3)
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("r")).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v2, &v3, dba.NameToEdgeType("r")).HasValue());
  dba.AdvanceCommand();
  {
    // Test `MATCH (n) -[r]-> (m) -[e]-> (l) RETURN n`
    AstStorage storage;
    auto *query = QUERY(SINGLE_QUERY(
        MATCH(PATTERN(NODE("n"), EDGE("r", Direction::OUT), NODE("m"), EDGE("e", Direction::OUT), NODE("l"))),
        RETURN("n")));
    // We have 3 nodes: `n`, `m` and `l` from which we could start.
    CheckPlansProduce(3, query, storage, &dba, [&](const auto &results) {
      // We expect to produce only a single (v1) node.
      AssertRows(results, {{TypedValue(query::VertexAccessor(v1))}}, dba);
    });
  }
  {
    // Equivalent to `MATCH (n) -[r]-> (m), (m) -[e]-> (l) RETURN n`.
    AstStorage storage;
    auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r", Direction::OUT), NODE("m")),
                                           PATTERN(NODE("m"), EDGE("e", Direction::OUT), NODE("l"))),
                                     RETURN("n")));
    CheckPlansProduce(3, query, storage, &dba, [&](const auto &results) {
      AssertRows(results, {{TypedValue(query::VertexAccessor(v1))}}, dba);
    });
  }
}

TEST(TestVariableStartPlanner, MatchOptionalMatchReturn) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // Make a graph (v1) -[:r]-> (v2) -[:r]-> (v3)
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("r")).HasValue());
  ASSERT_TRUE(dba.InsertEdge(&v2, &v3, dba.NameToEdgeType("r")).HasValue());
  dba.AdvanceCommand();
  // Test MATCH (n) -[r]-> (m) OPTIONAL MATCH (m) -[e]-> (l) RETURN n, l
  AstStorage storage;
  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r", Direction::OUT), NODE("m"))),
                         OPTIONAL_MATCH(PATTERN(NODE("m"), EDGE("e", Direction::OUT), NODE("l"))), RETURN("n", "l")));
  // We have 2 nodes `n` and `m` from which we could start the MATCH, and 2
  // nodes for OPTIONAL MATCH. This should produce 2 * 2 plans.
  CheckPlansProduce(4, query, storage, &dba, [&](const auto &results) {
    // We expect to produce 2 rows:
    //   * (v1), (v3)
    //   * (v2), null
    AssertRows(results,
               {{TypedValue(query::VertexAccessor(v1)), TypedValue(query::VertexAccessor(v3))},
                {TypedValue(query::VertexAccessor(v2)), TypedValue()}},
               dba);
  });
}

TEST(TestVariableStartPlanner, MatchOptionalMatchMergeReturn) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // Graph (v1) -[:r]-> (v2)
  query::VertexAccessor v1(dba.InsertVertex());
  query::VertexAccessor v2(dba.InsertVertex());
  auto r_type_name = "r";
  auto r_type = dba.NameToEdgeType(r_type_name);
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, r_type).HasValue());
  dba.AdvanceCommand();
  // Test MATCH (n) -[r]-> (m) OPTIONAL MATCH (m) -[e]-> (l)
  //      MERGE (u) -[q:r]-> (v) RETURN n, m, l, u, v
  AstStorage storage;
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r", Direction::OUT), NODE("m"))),
                                   OPTIONAL_MATCH(PATTERN(NODE("m"), EDGE("e", Direction::OUT), NODE("l"))),
                                   MERGE(PATTERN(NODE("u"), EDGE("q", Direction::OUT, {r_type_name}), NODE("v"))),
                                   RETURN("n", "m", "l", "u", "v")));
  // Since MATCH, OPTIONAL MATCH and MERGE each have 2 nodes from which we can
  // start, we generate 2 * 2 * 2 plans.
  CheckPlansProduce(8, query, storage, &dba, [&](const auto &results) {
    // We expect to produce a single row: (v1), (v2), null, (v1), (v2)
    AssertRows(results, {{TypedValue(v1), TypedValue(v2), TypedValue(), TypedValue(v1), TypedValue(v2)}}, dba);
  });
}

TEST(TestVariableStartPlanner, MatchWithMatchReturn) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // Graph (v1) -[:r]-> (v2)
  query::VertexAccessor v1(dba.InsertVertex());
  query::VertexAccessor v2(dba.InsertVertex());
  ASSERT_TRUE(dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("r")).HasValue());
  dba.AdvanceCommand();
  // Test MATCH (n) -[r]-> (m) WITH n MATCH (m) -[r]-> (l) RETURN n, m, l
  AstStorage storage;
  auto *query =
      QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), EDGE("r", Direction::OUT), NODE("m"))), WITH("n"),
                         MATCH(PATTERN(NODE("m"), EDGE("r", Direction::OUT), NODE("l"))), RETURN("n", "m", "l")));
  // We can start from 2 nodes in each match. Since WITH separates query parts,
  // we expect to get 2 plans for each, which totals 2 * 2.
  CheckPlansProduce(4, query, storage, &dba, [&](const auto &results) {
    // We expect to produce a single row: (v1), (v1), (v2)
    AssertRows(results, {{TypedValue(v1), TypedValue(v1), TypedValue(v2)}}, dba);
  });
}

TEST(TestVariableStartPlanner, MatchVariableExpand) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  // Graph (v1) -[:r1]-> (v2) -[:r2]-> (v3)
  auto v1 = dba.InsertVertex();
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto r1 = *dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("r1"));
  auto r2 = *dba.InsertEdge(&v2, &v3, dba.NameToEdgeType("r2"));
  dba.AdvanceCommand();
  // Test MATCH (n) -[r*]-> (m) RETURN r
  AstStorage storage;
  auto edge = EDGE_VARIABLE("r", Type::DEPTH_FIRST, Direction::OUT);
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r")));
  // We expect to get a single column with the following rows:
  TypedValue r1_list(std::vector<TypedValue>{TypedValue(r1)});  // [r1]
  TypedValue r2_list(std::vector<TypedValue>{TypedValue(r2)});  // [r2]
  // [r1, r2]
  TypedValue r1_r2_list(std::vector<TypedValue>{TypedValue(r1), TypedValue(r2)});
  CheckPlansProduce(2, query, storage, &dba, [&](const auto &results) {
    AssertRows(results, {{r1_list}, {r2_list}, {r1_r2_list}}, dba);
  });
}

TEST(TestVariableStartPlanner, MatchVariableExpandReferenceNode) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  auto id = dba.NameToProperty("id");
  // Graph (v1 {id:1}) -[:r1]-> (v2 {id: 2}) -[:r2]-> (v3 {id: 3})
  auto v1 = dba.InsertVertex();
  ASSERT_TRUE(v1.SetProperty(id, storage::PropertyValue(1)).HasValue());
  auto v2 = dba.InsertVertex();
  ASSERT_TRUE(v2.SetProperty(id, storage::PropertyValue(2)).HasValue());
  auto v3 = dba.InsertVertex();
  ASSERT_TRUE(v3.SetProperty(id, storage::PropertyValue(3)).HasValue());
  auto r1 = *dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("r1"));
  auto r2 = *dba.InsertEdge(&v2, &v3, dba.NameToEdgeType("r2"));
  dba.AdvanceCommand();
  // Test MATCH (n) -[r*..n.id]-> (m) RETURN r
  AstStorage storage;
  auto edge = EDGE_VARIABLE("r", Type::DEPTH_FIRST, Direction::OUT);
  edge->upper_bound_ = PROPERTY_LOOKUP("n", id);
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), edge, NODE("m"))), RETURN("r")));
  // We expect to get a single column with the following rows:
  // [r1] (v1 -[*..1]-> v2)
  TypedValue r1_list(std::vector<TypedValue>{TypedValue(r1)});
  // [r2] (v2 -[*..2]-> v3)
  TypedValue r2_list(std::vector<TypedValue>{TypedValue(r2)});
  CheckPlansProduce(2, query, storage, &dba, [&](const auto &results) {
    AssertRows(results, {{r1_list}, {r2_list}}, dba);
  });
}

TEST(TestVariableStartPlanner, MatchVariableExpandBoth) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  auto id = dba.NameToProperty("id");
  // Graph (v1 {id:1}) -[:r1]-> (v2) -[:r2]-> (v3)
  auto v1 = dba.InsertVertex();
  ASSERT_TRUE(v1.SetProperty(id, storage::PropertyValue(1)).HasValue());
  auto v2 = dba.InsertVertex();
  auto v3 = dba.InsertVertex();
  auto r1 = *dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("r1"));
  auto r2 = *dba.InsertEdge(&v2, &v3, dba.NameToEdgeType("r2"));
  dba.AdvanceCommand();
  // Test MATCH (n {id:1}) -[r*]- (m) RETURN r
  AstStorage storage;
  auto edge = EDGE_VARIABLE("r", Type::DEPTH_FIRST, Direction::BOTH);
  auto node_n = NODE("n");
  std::get<0>(node_n->properties_)[storage.GetPropertyIx("id")] = LITERAL(1);
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(node_n, edge, NODE("m"))), RETURN("r")));
  // We expect to get a single column with the following rows:
  TypedValue r1_list(std::vector<TypedValue>{TypedValue(r1)});  // [r1]
  // [r1, r2]
  TypedValue r1_r2_list(std::vector<TypedValue>{TypedValue(r1), TypedValue(r2)});
  CheckPlansProduce(2, query, storage, &dba, [&](const auto &results) {
    AssertRows(results, {{r1_list}, {r1_r2_list}}, dba);
  });
}

TEST(TestVariableStartPlanner, MatchBfs) {
  storage::Storage db;
  auto storage_dba = db.Access();
  query::DbAccessor dba(&storage_dba);
  auto id = dba.NameToProperty("id");
  // Graph (v1 {id:1}) -[:r1]-> (v2 {id: 2}) -[:r2]-> (v3 {id: 3})
  auto v1 = dba.InsertVertex();
  ASSERT_TRUE(v1.SetProperty(id, storage::PropertyValue(1)).HasValue());
  auto v2 = dba.InsertVertex();
  ASSERT_TRUE(v2.SetProperty(id, storage::PropertyValue(2)).HasValue());
  auto v3 = dba.InsertVertex();
  ASSERT_TRUE(v3.SetProperty(id, storage::PropertyValue(3)).HasValue());
  auto r1 = *dba.InsertEdge(&v1, &v2, dba.NameToEdgeType("r1"));
  ASSERT_TRUE(dba.InsertEdge(&v2, &v3, dba.NameToEdgeType("r2")).HasValue());
  dba.AdvanceCommand();
  // Test MATCH (n) -[r *bfs..10](r, n | n.id <> 3)]-> (m) RETURN r
  AstStorage storage;
  auto *bfs = storage.Create<query::EdgeAtom>(IDENT("r"), EdgeAtom::Type::BREADTH_FIRST, Direction::OUT,
                                              std::vector<query::EdgeTypeIx>{});
  bfs->filter_lambda_.inner_edge = IDENT("r");
  bfs->filter_lambda_.inner_node = IDENT("n");
  bfs->filter_lambda_.expression = NEQ(PROPERTY_LOOKUP("n", id), LITERAL(3));
  bfs->upper_bound_ = LITERAL(10);
  auto *query = QUERY(SINGLE_QUERY(MATCH(PATTERN(NODE("n"), bfs, NODE("m"))), RETURN("r")));
  // We expect to get a single column with the following rows:
  TypedValue r1_list(std::vector<TypedValue>{TypedValue(r1)});  // [r1]
  CheckPlansProduce(2, query, storage, &dba, [&](const auto &results) { AssertRows(results, {{r1_list}}, dba); });
}

}  // namespace
