#include <algorithm>

#include "gtest/gtest.h"

#include "dbms/dbms.hpp"
#include "query/frontend/semantic/symbol_generator.hpp"
#include "query/frontend/semantic/symbol_table.hpp"
#include "query/plan/planner.hpp"
#include "utils/algorithm.hpp"

#include "query_plan_common.hpp"

using namespace query::plan;
using query::AstTreeStorage;
using Direction = query::EdgeAtom::Direction;

namespace std {

// Overloads for printing resulting rows from a query.
std::ostream &operator<<(std::ostream &stream,
                         const std::vector<TypedValue> &row) {
  PrintIterable(stream, row);
  return stream;
}
std::ostream &operator<<(std::ostream &stream,
                         const std::vector<std::vector<TypedValue>> &rows) {
  PrintIterable(stream, rows, "\n");
  return stream;
}

}  // namespace std

namespace {

auto MakeSymbolTable(query::Query &query) {
  query::SymbolTable symbol_table;
  query::SymbolGenerator symbol_generator(symbol_table);
  query.Accept(symbol_generator);
  return symbol_table;
}

void AssertRows(const std::vector<std::vector<TypedValue>> &datum,
                std::vector<std::vector<TypedValue>> expected) {
  auto row_equal = [](const auto &row1, const auto &row2) {
    if (row1.size() != row2.size()) {
      return false;
    }
    TypedValue::BoolEqual value_eq;
    auto row1_it = row1.begin();
    for (auto row2_it = row2.begin(); row2_it != row2.end();
         ++row1_it, ++row2_it) {
      if (!value_eq(*row1_it, *row2_it)) {
        return false;
      }
    }
    return true;
  };
  ASSERT_TRUE(std::is_permutation(datum.begin(), datum.end(), expected.begin(),
                                  expected.end(), row_equal))
      << "Actual rows:" << std::endl
      << datum << std::endl
      << "Expected rows:" << std::endl
      << expected;
};

void CheckPlansProduce(
    size_t expected_plan_count, AstTreeStorage &storage, GraphDbAccessor &dba,
    std::function<void(const std::vector<std::vector<TypedValue>> &)> check) {
  auto symbol_table = MakeSymbolTable(*storage.query());
  auto plans =
      MakeLogicalPlan<VariableStartPlanner>(storage, symbol_table, dba);
  EXPECT_EQ(std::distance(plans.begin(), plans.end()), expected_plan_count);
  for (const auto &plan : plans) {
    auto *produce = dynamic_cast<Produce *>(plan.get());
    ASSERT_TRUE(produce);
    auto results = CollectProduce(produce, symbol_table, dba);
    check(results);
  }
}

TEST(TestVariableStartPlanner, MatchReturn) {
  Dbms dbms;
  auto dba = dbms.active();
  // Make a graph (v1) -[:r]-> (v2)
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  dba->insert_edge(v1, v2, dba->edge_type("r"));
  dba->advance_command();
  // Test MATCH (n) -[r]-> (m) RETURN n
  AstTreeStorage storage;
  QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r", nullptr, Direction::OUT), NODE("m"))),
      RETURN("n"));
  // We have 2 nodes `n` and `m` from which we could start, so expect 2 plans.
  CheckPlansProduce(2, storage, *dba, [&](const auto &results) {
    // We expect to produce only a single (v1) node.
    AssertRows(results, {{v1}});
  });
}

TEST(TestVariableStartPlanner, MatchTripletPatternReturn) {
  Dbms dbms;
  auto dba = dbms.active();
  // Make a graph (v1) -[:r]-> (v2) -[:r]-> (v3)
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  auto v3 = dba->insert_vertex();
  dba->insert_edge(v1, v2, dba->edge_type("r"));
  dba->insert_edge(v2, v3, dba->edge_type("r"));
  dba->advance_command();
  {
    // Test `MATCH (n) -[r]-> (m) -[e]-> (l) RETURN n`
    AstTreeStorage storage;
    QUERY(
        MATCH(PATTERN(NODE("n"), EDGE("r", nullptr, Direction::OUT), NODE("m"),
                      EDGE("e", nullptr, Direction::OUT), NODE("l"))),
        RETURN("n"));
    // We have 3 nodes: `n`, `m` and `l` from which we could start.
    CheckPlansProduce(3, storage, *dba, [&](const auto &results) {
      // We expect to produce only a single (v1) node.
      AssertRows(results, {{v1}});
    });
  }
  {
    // Equivalent to `MATCH (n) -[r]-> (m), (m) -[e]-> (l) RETURN n`.
    AstTreeStorage storage;
    QUERY(
        MATCH(
            PATTERN(NODE("n"), EDGE("r", nullptr, Direction::OUT), NODE("m")),
            PATTERN(NODE("m"), EDGE("e", nullptr, Direction::OUT), NODE("l"))),
        RETURN("n"));
    CheckPlansProduce(3, storage, *dba, [&](const auto &results) {
      AssertRows(results, {{v1}});
    });
  }
}

TEST(TestVariableStartPlanner, MatchOptionalMatchReturn) {
  Dbms dbms;
  auto dba = dbms.active();
  // Make a graph (v1) -[:r]-> (v2) -[:r]-> (v3)
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  auto v3 = dba->insert_vertex();
  dba->insert_edge(v1, v2, dba->edge_type("r"));
  dba->insert_edge(v2, v3, dba->edge_type("r"));
  dba->advance_command();
  // Test MATCH (n) -[r]-> (m) OPTIONAL MATCH (m) -[e]-> (l) RETURN n, l
  AstTreeStorage storage;
  QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r", nullptr, Direction::OUT), NODE("m"))),
      OPTIONAL_MATCH(
          PATTERN(NODE("m"), EDGE("e", nullptr, Direction::OUT), NODE("l"))),
      RETURN("n", "l"));
  // We have 2 nodes `n` and `m` from which we could start the MATCH, and 2
  // nodes for OPTIONAL MATCH. This should produce 2 * 2 plans.
  CheckPlansProduce(4, storage, *dba, [&](const auto &results) {
    // We expect to produce 2 rows:
    //   * (v1), (v3)
    //   * (v2), null
    AssertRows(results, {{v1, v3}, {v2, TypedValue::Null}});
  });
}

TEST(TestVariableStartPlanner, MatchOptionalMatchMergeReturn) {
  Dbms dbms;
  auto dba = dbms.active();
  // Graph (v1) -[:r]-> (v2)
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  auto r_type = dba->edge_type("r");
  dba->insert_edge(v1, v2, r_type);
  dba->advance_command();
  // Test MATCH (n) -[r]-> (m) OPTIONAL MATCH (m) -[e]-> (l)
  //      MERGE (u) -[q:r]-> (v) RETURN n, m, l, u, v
  AstTreeStorage storage;
  QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r", nullptr, Direction::OUT), NODE("m"))),
      OPTIONAL_MATCH(
          PATTERN(NODE("m"), EDGE("e", nullptr, Direction::OUT), NODE("l"))),
      MERGE(PATTERN(NODE("u"), EDGE("q", r_type, Direction::OUT), NODE("v"))),
      RETURN("n", "m", "l", "u", "v"));
  // Since MATCH, OPTIONAL MATCH and MERGE each have 2 nodes from which we can
  // start, we generate 2 * 2 * 2 plans.
  CheckPlansProduce(8, storage, *dba, [&](const auto &results) {
    // We expect to produce a single row: (v1), (v2), null, (v1), (v2)
    AssertRows(results, {{v1, v2, TypedValue::Null, v1, v2}});
  });
}

TEST(TestVariableStartPlanner, MatchWithMatchReturn) {
  Dbms dbms;
  auto dba = dbms.active();
  // Graph (v1) -[:r]-> (v2)
  auto v1 = dba->insert_vertex();
  auto v2 = dba->insert_vertex();
  dba->insert_edge(v1, v2, dba->edge_type("r"));
  dba->advance_command();
  // Test MATCH (n) -[r]-> (m) WITH n MATCH (m) -[r]-> (l) RETURN n, m, l
  AstTreeStorage storage;
  QUERY(
      MATCH(PATTERN(NODE("n"), EDGE("r", nullptr, Direction::OUT), NODE("m"))),
      WITH("n"),
      MATCH(PATTERN(NODE("m"), EDGE("r", nullptr, Direction::OUT), NODE("l"))),
      RETURN("n", "m", "l"));
  // We can start from 2 nodes in each match. Since WITH separates query parts,
  // we expect to get 2 plans for each, which totals 2 * 2.
  CheckPlansProduce(4, storage, *dba, [&](const auto &results) {
    // We expect to produce a single row: (v1), (v1), (v2)
    AssertRows(results, {{v1, v1, v2}});
  });
}

}  // namespace
