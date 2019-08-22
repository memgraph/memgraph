#include <cstdlib>

#include "communication/result_stream_faker.hpp"
#include "database/single_node/graph_db_accessor.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"
#include "query_common.hpp"

// TODO: This is not a unit test, but tests/integration dir is chaotic at the
// moment. After tests refactoring is done, move/rename this.

class InterpreterTest : public ::testing::Test {
 protected:
  database::GraphDb db_;
  query::Interpreter interpreter_;

  auto Interpret(const std::string &query,
                 const std::map<std::string, PropertyValue> &params = {}) {
    auto dba = db_.Access();
    ResultStreamFaker<query::TypedValue> stream;
    auto results = interpreter_(query, dba, params, false);
    stream.Header(results.header());
    results.PullAll(stream);
    stream.Summary(results.summary());
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
    auto stream = Interpret("RETURN $2 + $`a b`", {{"2", 10}, {"a b", 15}});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "$2 + $`a b`");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 25);
  }
  {
    // Not needed parameter.
    auto stream =
        Interpret("RETURN $2 + $`a b`", {{"2", 10}, {"a b", 15}, {"c", 10}});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "$2 + $`a b`");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueInt(), 25);
  }
  {
    // Cached ast, different parameters.
    auto stream = Interpret("RETURN $2 + $`a b`", {{"2", "da"}, {"a b", "ne"}});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].ValueString(), "dane");
  }
  {
    // Non-primitive literal.
    auto stream =
        Interpret("RETURN $2", {{"2", std::vector<PropertyValue>{5, 2, 3}}});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    auto result = query::test_common::ToIntList(stream.GetResults()[0][0]);
    ASSERT_THAT(result, testing::ElementsAre(5, 2, 3));
  }
  {
    // Cached ast, unprovided parameter.
    ASSERT_THROW(Interpret("RETURN $2 + $`a b`", {{"2", "da"}, {"ab", "ne"}}),
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
      node.PropsSet(dba.Property(kId), id++);
      node.PropsSet(dba.Property(kReachable), reachable);
      levels[level].push_back(node);
      return node;
    };

    auto add_edge = [&](VertexAccessor &v1, VertexAccessor &v2,
                        bool reachable) {
      auto edge = dba.InsertEdge(v1, v2, dba.EdgeType("edge"));
      edge.PropsSet(dba.Property(kReachable), reachable);
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

  auto dba = db_.Access();
  ResultStreamFaker<query::TypedValue> stream;
  auto results = interpreter_(
      "MATCH (n {id: 0})-[r *bfs..5 (e, n | n.reachable and "
      "e.reachable)]->(m) RETURN r",
      dba, {}, false);
  stream.Header(results.header());
  results.PullAll(stream);
  stream.Summary(results.summary());

  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader()[0], "r");
  ASSERT_EQ(stream.GetResults().size(), 5 * kNumNodesPerLevel);

  int expected_level = 1;
  int remaining_nodes_in_level = kNumNodesPerLevel;
  std::unordered_set<int64_t> matched_ids;

  for (const auto &result : stream.GetResults()) {
    const auto &edges = query::test_common::ToEdgeList(result[0]);
    // Check that path is of expected length. Returned paths should be from
    // shorter to longer ones.
    EXPECT_EQ(edges.size(), expected_level);
    // Check that starting node is correct.
    EXPECT_EQ(
        edges[0].from().PropsAt(dba.Property(kId)).ValueInt(),
        0);
    for (int i = 1; i < static_cast<int>(edges.size()); ++i) {
      // Check that edges form a connected path.
      EXPECT_EQ(edges[i - 1].to(), edges[i].from());
    }
    auto matched_id =
        edges.back().to().PropsAt(dba.Property(kId)).ValueInt();
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
  ResultStreamFaker<query::TypedValue> stream;
  auto dba = db_.Access();
  ASSERT_THROW(
      interpreter_("CREATE INDEX ON :X(y)", dba, {}, true).PullAll(stream),
      query::IndexInMulticommandTxException);
}

// Test shortest path end to end.
TEST_F(InterpreterTest, ShortestPath) {
  {
    ResultStreamFaker<query::TypedValue> stream;
    auto dba = db_.Access();
    interpreter_(
        "CREATE (n:A {x: 1}), (m:B {x: 2}), (l:C {x: 1}), (n)-[:r1 {w: 1 "
        "}]->(m)-[:r2 {w: 2}]->(l), (n)-[:r3 {w: 4}]->(l)",
        dba, {}, true)
        .PullAll(stream);

    dba.Commit();
  }

  ResultStreamFaker<query::TypedValue> stream;
  auto dba = db_.Access();
  auto results =
      interpreter_("MATCH (n)-[e *wshortest 5 (e, n | e.w) ]->(m) return e",
                   dba, {}, false);
  stream.Header(results.header());
  results.PullAll(stream);
  stream.Summary(results.summary());

  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader()[0], "e");
  ASSERT_EQ(stream.GetResults().size(), 3U);

  std::vector<std::vector<std::string>> expected_results{
      {"r1"}, {"r2"}, {"r1", "r2"}};

  for (const auto &result : stream.GetResults()) {
    const auto &edges = query::test_common::ToEdgeList(result[0]);

    std::vector<std::string> datum;
    for (const auto &edge : edges) {
      datum.push_back(dba.EdgeTypeName(edge.EdgeType()));
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
  ResultStreamFaker<query::TypedValue> stream;
  {
    auto dba = db_.Access();
    interpreter_("CREATE CONSTRAINT ON (n:A) ASSERT n.a, n.b IS UNIQUE;", dba,
                 {}, true)
        .PullAll(stream);
    dba.Commit();
  }

  {
    auto dba = db_.Access();
    interpreter_("CREATE (:A{a:1, b:1})", dba, {}, true).PullAll(stream);
    dba.Commit();
  }

  {
    auto dba = db_.Access();
    interpreter_("CREATE (:A{a:2, b:2})", dba, {}, true).PullAll(stream);
    dba.Commit();
  }

  {
    auto dba = db_.Access();
    ASSERT_THROW(
        interpreter_("CREATE (:A{a:1, b:1})", dba, {}, true).PullAll(stream),
        query::QueryRuntimeException);
    dba.Commit();
  }

  {
    auto dba = db_.Access();
    interpreter_("MATCH (n:A{a:2, b:2}) SET n.a=1", dba, {}, true)
        .PullAll(stream);
    interpreter_("CREATE (:A{a:2, b:2})", dba, {}, true).PullAll(stream);
    dba.Commit();
  }

  {
    auto dba = db_.Access();
    interpreter_("MATCH (n:A{a:2, b:2}) DETACH DELETE n", dba, {}, true)
        .PullAll(stream);
    interpreter_("CREATE (n:A{a:2, b:2})", dba, {}, true).PullAll(stream);
    dba.Commit();
  }
}
