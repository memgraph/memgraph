#include <cstdlib>

#include "communication/bolt/v1/value.hpp"
#include "communication/result_stream_faker.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "glue/communication.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"
#include "query_common.hpp"

namespace {

auto ToEdgeList(const communication::bolt::Value &v) {
  std::vector<communication::bolt::Edge> list;
  for (auto x : v.ValueList()) {
    list.push_back(x.ValueEdge());
  }
  return list;
};

}  // namespace

// TODO: This is not a unit test, but tests/integration dir is chaotic at the
// moment. After tests refactoring is done, move/rename this.

class InterpreterTest : public ::testing::Test {
 protected:
  database::GraphDb db_;
  query::InterpreterContext interpreter_context_{&db_};
  query::Interpreter interpreter_{&interpreter_context_};

  /**
   * Execute the given query and commit the transaction.
   *
   * Return the query stream.
   */
  auto Interpret(const std::string &query,
                 const std::map<std::string, PropertyValue> &params = {}) {
    ResultStreamFaker stream;

    auto [header, _] = interpreter_.Prepare(query, params);
    stream.Header(header);
    auto summary = interpreter_.PullAll(&stream);
    stream.Summary(summary);

    return stream;
  }
};

// Run query with different ast twice to see if query executes correctly when
// ast is read from cache.
TEST_F(InterpreterTest, AstCache) {
  {
    auto stream = Interpret("RETURN 2 + 3");
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "2 + 3");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 5);
  }
  {
    // Cached ast, different literals.
    auto stream = Interpret("RETURN 5 + 4");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 9);
  }
  {
    // Different ast (because of different types).
    auto stream = Interpret("RETURN 5.5 + 4");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueDouble(), 9.5);
  }
  {
    // Cached ast, same literals.
    auto stream = Interpret("RETURN 2 + 3");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 5);
  }
  {
    // Cached ast, different literals.
    auto stream = Interpret("RETURN 10.5 + 1");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueDouble(), 11.5);
  }
  {
    // Cached ast, same literals, different whitespaces.
    auto stream = Interpret("RETURN  10.5 + 1");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueDouble(), 11.5);
  }
  {
    // Cached ast, same literals, different named header.
    auto stream = Interpret("RETURN  10.5+1");
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "10.5+1");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueDouble(), 11.5);
  }
}

// Run query with same ast multiple times with different parameters.
TEST_F(InterpreterTest, Parameters) {
  {
    auto stream = Interpret("RETURN $2 + $`a b`", {{"2", PropertyValue(10)},
                                                   {"a b", PropertyValue(15)}});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "$2 + $`a b`");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 25);
  }
  {
    // Not needed parameter.
    auto stream = Interpret("RETURN $2 + $`a b`", {{"2", PropertyValue(10)},
                                                   {"a b", PropertyValue(15)},
                                                   {"c", PropertyValue(10)}});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "$2 + $`a b`");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 25);
  }
  {
    // Cached ast, different parameters.
    auto stream =
        Interpret("RETURN $2 + $`a b`",
                  {{"2", PropertyValue("da")}, {"a b", PropertyValue("ne")}});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueString(), "dane");
  }
  {
    // Non-primitive literal.
    auto stream = Interpret(
        "RETURN $2",
        {{"2", PropertyValue(std::vector<PropertyValue>{
                   PropertyValue(5), PropertyValue(2), PropertyValue(3)})}});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    auto result = query::test_common::ToIntList(
        glue::ToTypedValue(stream.GetResults()[0][0]));
    ASSERT_THAT(result, testing::ElementsAre(5, 2, 3));
  }
  {
    // Cached ast, unprovided parameter.
    ASSERT_THROW(Interpret("RETURN $2 + $`a b`", {{"2", PropertyValue("da")},
                                                  {"ab", PropertyValue("ne")}}),
                 query::UnprovidedParameterError);
  }
}

// Test bfs end to end.
TEST_F(InterpreterTest, Bfs) {
  srand(0);
  const auto kNumLevels = 10;
  const auto kNumNodesPerLevel = 100;
  const auto kNumEdgesPerNode = 100;
  const auto kNumUnreachableNodes = 1000;
  const auto kNumUnreachableEdges = 100000;
  const auto kReachable = "reachable";
  const auto kId = "id";

  std::vector<std::vector<VertexAccessor>> levels(kNumLevels);
  int id = 0;

  // Set up.
  {
    auto dba = db_.Access();
    auto add_node = [&](int level, bool reachable) {
      auto node = dba.InsertVertex();
      node.PropsSet(dba.Property(kId), PropertyValue(id++));
      node.PropsSet(dba.Property(kReachable), PropertyValue(reachable));
      levels[level].push_back(node);
      return node;
    };

    auto add_edge = [&](VertexAccessor &v1, VertexAccessor &v2,
                        bool reachable) {
      auto edge = dba.InsertEdge(v1, v2, dba.EdgeType("edge"));
      edge.PropsSet(dba.Property(kReachable), PropertyValue(reachable));
    };

    // Add source node.
    add_node(0, true);

    // Add reachable nodes.
    for (int i = 1; i < kNumLevels; ++i) {
      for (int j = 0; j < kNumNodesPerLevel; ++j) {
        auto node = add_node(i, true);
        for (int k = 0; k < kNumEdgesPerNode; ++k) {
          auto &node2 = levels[i - 1][rand() % levels[i - 1].size()];
          add_edge(node2, node, true);
        }
      }
    }

    // Add unreachable nodes.
    for (int i = 0; i < kNumUnreachableNodes; ++i) {
      auto node = add_node(rand() % kNumLevels,  // Not really important.
                           false);
      for (int j = 0; j < kNumEdgesPerNode; ++j) {
        auto &level = levels[rand() % kNumLevels];
        auto &node2 = level[rand() % level.size()];
        add_edge(node2, node, true);
        add_edge(node, node2, true);
      }
    }

    // Add unreachable edges.
    for (int i = 0; i < kNumUnreachableEdges; ++i) {
      auto &level1 = levels[rand() % kNumLevels];
      auto &node1 = level1[rand() % level1.size()];
      auto &level2 = levels[rand() % kNumLevels];
      auto &node2 = level2[rand() % level2.size()];
      add_edge(node1, node2, false);
    }

    dba.Commit();
  }

  auto stream = Interpret(
      "MATCH (n {id: 0})-[r *bfs..5 (e, n | n.reachable and "
      "e.reachable)]->(m) RETURN n, r, m");

  ASSERT_EQ(stream.GetHeader().size(), 3U);
  EXPECT_EQ(stream.GetHeader()[0], "n");
  EXPECT_EQ(stream.GetHeader()[1], "r");
  EXPECT_EQ(stream.GetHeader()[2], "m");
  ASSERT_EQ(stream.GetResults().size(), 5 * kNumNodesPerLevel);

  auto dba = db_.Access();
  int expected_level = 1;
  int remaining_nodes_in_level = kNumNodesPerLevel;
  std::unordered_set<int64_t> matched_ids;

  for (const auto &result : stream.GetResults()) {
    const auto &begin = result[0].ValueVertex();
    const auto &edges = ToEdgeList(result[1]);
    const auto &end = result[2].ValueVertex();

    // Check that path is of expected length. Returned paths should be from
    // shorter to longer ones.
    EXPECT_EQ(edges.size(), expected_level);
    // Check that starting node is correct.
    EXPECT_EQ(edges.front().from, begin.id);
    EXPECT_EQ(begin.properties.at(kId).ValueInt(), 0);
    for (int i = 1; i < static_cast<int>(edges.size()); ++i) {
      // Check that edges form a connected path.
      EXPECT_EQ(edges[i - 1].to.AsInt(), edges[i].from.AsInt());
    }
    auto matched_id = end.properties.at(kId).ValueInt();
    EXPECT_EQ(edges.back().to, end.id);
    // Check that we didn't match that node already.
    EXPECT_TRUE(matched_ids.insert(matched_id).second);
    // Check that shortest path was found.
    EXPECT_TRUE(matched_id > kNumNodesPerLevel * (expected_level - 1) &&
                matched_id <= kNumNodesPerLevel * expected_level);
    if (!--remaining_nodes_in_level) {
      remaining_nodes_in_level = kNumNodesPerLevel;
      ++expected_level;
    }
  }
}

TEST_F(InterpreterTest, CreateIndexInMulticommandTransaction) {
  Interpret("BEGIN");
  ASSERT_THROW(Interpret("CREATE INDEX ON :X(y)"),
               query::IndexInMulticommandTxException);
  Interpret("ROLLBACK");
}

// Test shortest path end to end.
TEST_F(InterpreterTest, ShortestPath) {
  Interpret(
      "CREATE (n:A {x: 1}), (m:B {x: 2}), (l:C {x: 1}), (n)-[:r1 {w: 1 "
      "}]->(m)-[:r2 {w: 2}]->(l), (n)-[:r3 {w: 4}]->(l)");

  auto stream =
      Interpret("MATCH (n)-[e *wshortest 5 (e, n | e.w) ]->(m) return e");

  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader()[0], "e");
  ASSERT_EQ(stream.GetResults().size(), 3U);

  auto dba = db_.Access();
  std::vector<std::vector<std::string>> expected_results{
      {"r1"}, {"r2"}, {"r1", "r2"}};

  for (const auto &result : stream.GetResults()) {
    const auto &edges = ToEdgeList(result[0]);

    std::vector<std::string> datum;
    datum.reserve(edges.size());

    for (const auto &edge : edges) {
      datum.push_back(edge.type);
    }

    bool any_match = false;
    for (const auto &expected : expected_results) {
      if (expected == datum) {
        any_match = true;
        break;
      }
    }

    EXPECT_TRUE(any_match);
  }
}

// NOLINTNEXTLINE(hicpp-special-member-functions)
TEST_F(InterpreterTest, UniqueConstraintTest) {
  Interpret("CREATE CONSTRAINT ON (n:A) ASSERT n.a, n.b IS UNIQUE;");
  Interpret("CREATE (:A{a:1, b:1})");
  Interpret("CREATE (:A{a:2, b:2})");
  ASSERT_THROW(Interpret("CREATE (:A{a:1, b:1})"),
               query::QueryRuntimeException);
  Interpret("MATCH (n:A{a:2, b:2}) SET n.a=1");
  Interpret("CREATE (:A{a:2, b:2})");
  Interpret("MATCH (n:A{a:2, b:2}) DETACH DELETE n");
  Interpret("CREATE (n:A{a:2, b:2})");
}

TEST_F(InterpreterTest, ExplainQuery) {
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 0U);
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 0U);
  auto stream = Interpret("EXPLAIN MATCH (n) RETURN *;");
  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader().front(), "QUERY PLAN");
  std::vector<std::string> expected_rows{" * Produce {n}", " * ScanAll (n)",
                                         " * Once"};
  ASSERT_EQ(stream.GetResults().size(), expected_rows.size());
  auto expected_it = expected_rows.begin();
  for (const auto &row : stream.GetResults()) {
    ASSERT_EQ(row.size(), 1U);
    EXPECT_EQ(row.front().ValueString(), *expected_it);
    ++expected_it;
  }
  // We should have a plan cache for MATCH ...
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 1U);
  // We should have AST cache for EXPLAIN ... and for inner MATCH ...
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 2U);
  Interpret("MATCH (n) RETURN *;");
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 1U);
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 2U);
}

TEST_F(InterpreterTest, ExplainQueryWithParams) {
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 0U);
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 0U);
  auto stream = Interpret("EXPLAIN MATCH (n) WHERE n.id = $id RETURN *;",
                          {{"id", PropertyValue(42)}});
  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader().front(), "QUERY PLAN");
  std::vector<std::string> expected_rows{" * Produce {n}", " * Filter",
                                         " * ScanAll (n)", " * Once"};
  ASSERT_EQ(stream.GetResults().size(), expected_rows.size());
  auto expected_it = expected_rows.begin();
  for (const auto &row : stream.GetResults()) {
    ASSERT_EQ(row.size(), 1U);
    EXPECT_EQ(row.front().ValueString(), *expected_it);
    ++expected_it;
  }
  // We should have a plan cache for MATCH ...
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 1U);
  // We should have AST cache for EXPLAIN ... and for inner MATCH ...
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 2U);
  Interpret("MATCH (n) WHERE n.id = $id RETURN *;",
            {{"id", PropertyValue("something else")}});
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 1U);
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 2U);
}

TEST_F(InterpreterTest, ProfileQuery) {
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 0U);
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 0U);
  auto stream = Interpret("PROFILE MATCH (n) RETURN *;");
  std::vector<std::string> expected_header{"OPERATOR", "ACTUAL HITS",
                                           "RELATIVE TIME", "ABSOLUTE TIME"};
  EXPECT_EQ(stream.GetHeader(), expected_header);
  std::vector<std::string> expected_rows{"* Produce", "* ScanAll", "* Once"};
  ASSERT_EQ(stream.GetResults().size(), expected_rows.size());
  auto expected_it = expected_rows.begin();
  for (const auto &row : stream.GetResults()) {
    ASSERT_EQ(row.size(), 4U);
    EXPECT_EQ(row.front().ValueString(), *expected_it);
    ++expected_it;
  }
  // We should have a plan cache for MATCH ...
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 1U);
  // We should have AST cache for PROFILE ... and for inner MATCH ...
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 2U);
  Interpret("MATCH (n) RETURN *;");
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 1U);
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 2U);
}

TEST_F(InterpreterTest, ProfileQueryWithParams) {
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 0U);
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 0U);
  auto stream = Interpret("PROFILE MATCH (n) WHERE n.id = $id RETURN *;",
                          {{"id", PropertyValue(42)}});
  std::vector<std::string> expected_header{"OPERATOR", "ACTUAL HITS",
                                           "RELATIVE TIME", "ABSOLUTE TIME"};
  EXPECT_EQ(stream.GetHeader(), expected_header);
  std::vector<std::string> expected_rows{"* Produce", "* Filter", "* ScanAll",
                                         "* Once"};
  ASSERT_EQ(stream.GetResults().size(), expected_rows.size());
  auto expected_it = expected_rows.begin();
  for (const auto &row : stream.GetResults()) {
    ASSERT_EQ(row.size(), 4U);
    EXPECT_EQ(row.front().ValueString(), *expected_it);
    ++expected_it;
  }
  // We should have a plan cache for MATCH ...
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 1U);
  // We should have AST cache for PROFILE ... and for inner MATCH ...
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 2U);
  Interpret("MATCH (n) WHERE n.id = $id RETURN *;",
            {{"id", PropertyValue("something else")}});
  EXPECT_EQ(interpreter_context_.plan_cache.size(), 1U);
  EXPECT_EQ(interpreter_context_.ast_cache.size(), 2U);
}
