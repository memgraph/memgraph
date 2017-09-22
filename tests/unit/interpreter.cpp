#include <cstdlib>

#include "communication/result_stream_faker.hpp"
#include "database/dbms.hpp"
#include "database/graph_db_accessor.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "query/exceptions.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"
#include "query_common.hpp"

// TODO: This is not a unit test, but tests/integration dir is chaotic at the
// moment. After tests refactoring is done, move/rename this.

namespace {

// Run query with different ast twice to see if query executes correctly when
// ast is read from cache.
TEST(Interpreter, AstCache) {
  query::Interpreter interpreter;
  Dbms dbms;
  {
    ResultStreamFaker stream;
    auto dba = dbms.active();
    interpreter.Interpret("RETURN 2 + 3", *dba, stream, {});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "2 + 3");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 5);
  }
  {
    // Cached ast, different literals.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    interpreter.Interpret("RETURN 5 + 4", *dba, stream, {});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 9);
  }
  {
    // Different ast (because of different types).
    ResultStreamFaker stream;
    auto dba = dbms.active();
    interpreter.Interpret("RETURN 5.5 + 4", *dba, stream, {});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<double>(), 9.5);
  }
  {
    // Cached ast, same literals.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    interpreter.Interpret("RETURN 2 + 3", *dba, stream, {});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 5);
  }
  {
    // Cached ast, different literals.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    interpreter.Interpret("RETURN 10.5 + 1", *dba, stream, {});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<double>(), 11.5);
  }
  {
    // Cached ast, same literals, different whitespaces.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    interpreter.Interpret("RETURN  10.5 + 1", *dba, stream, {});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<double>(), 11.5);
  }
  {
    // Cached ast, same literals, different named header.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    interpreter.Interpret("RETURN  10.5+1", *dba, stream, {});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "10.5+1");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<double>(), 11.5);
  }
}

// Run query with same ast multiple times with different parameters.
TEST(Interpreter, Parameters) {
  query::Interpreter interpreter;
  Dbms dbms;
  {
    ResultStreamFaker stream;
    auto dba = dbms.active();
    interpreter.Interpret("RETURN $2 + $`a b`", *dba, stream,
                          {{"2", 10}, {"a b", 15}});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "$2 + $`a b`");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 25);
  }
  {
    // Not needed parameter.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    interpreter.Interpret("RETURN $2 + $`a b`", *dba, stream,
                          {{"2", 10}, {"a b", 15}, {"c", 10}});
    ASSERT_EQ(stream.GetHeader().size(), 1U);
    EXPECT_EQ(stream.GetHeader()[0], "$2 + $`a b`");
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<int64_t>(), 25);
  }
  {
    // Cached ast, different parameters.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    interpreter.Interpret("RETURN $2 + $`a b`", *dba, stream,
                          {{"2", "da"}, {"a b", "ne"}});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    ASSERT_EQ(stream.GetResults()[0][0].Value<std::string>(), "dane");
  }
  {
    // Non-primitive literal.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    interpreter.Interpret("RETURN $2", *dba, stream,
                          {{"2", std::vector<query::TypedValue>{5, 2, 3}}});
    ASSERT_EQ(stream.GetResults().size(), 1U);
    ASSERT_EQ(stream.GetResults()[0].size(), 1U);
    auto result = query::test_common::ToList<int64_t>(
        stream.GetResults()[0][0].Value<std::vector<query::TypedValue>>());
    ASSERT_THAT(result, testing::ElementsAre(5, 2, 3));
  }
  {
    // Cached ast, unprovided parameter.
    ResultStreamFaker stream;
    auto dba = dbms.active();
    ASSERT_THROW(interpreter.Interpret("RETURN $2 + $`a b`", *dba, stream,
                                       {{"2", "da"}, {"ab", "ne"}}),
                 query::UnprovidedParameterError);
  }
}

// Test bfs end to end.
TEST(Interpreter, Bfs) {
  srand(0);
  const auto kNumLevels = 10;
  const auto kNumNodesPerLevel = 100;
  const auto kNumEdgesPerNode = 100;
  const auto kNumUnreachableNodes = 1000;
  const auto kNumUnreachableEdges = 100000;
  const auto kReachable = "reachable";
  const auto kId = "id";

  query::Interpreter interpreter;
  Dbms dbms;
  ResultStreamFaker stream;
  std::vector<std::vector<VertexAccessor>> levels(kNumLevels);
  int id = 0;

  // Set up.
  {
    auto dba = dbms.active();
    auto add_node = [&](int level, bool reachable) {
      auto node = dba->InsertVertex();
      node.PropsSet(dba->Property(kId), id++);
      node.PropsSet(dba->Property(kReachable), reachable);
      levels[level].push_back(node);
      return node;
    };

    auto add_edge = [&](VertexAccessor &v1, VertexAccessor &v2,
                        bool reachable) {
      auto edge = dba->InsertEdge(v1, v2, dba->EdgeType("edge"));
      edge.PropsSet(dba->Property(kReachable), reachable);
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

    dba->Commit();
  }

  auto dba = dbms.active();

  interpreter.Interpret(
      "MATCH (n {id: 0})-bfs[r](e, n | n.reachable and e.reachable, 5)->(m) "
      "RETURN r",
      *dba, stream, {});

  ASSERT_EQ(stream.GetHeader().size(), 1U);
  EXPECT_EQ(stream.GetHeader()[0], "r");
  ASSERT_EQ(stream.GetResults().size(), 5 * kNumNodesPerLevel);

  int expected_level = 1;
  int remaining_nodes_in_level = kNumNodesPerLevel;
  std::unordered_set<int64_t> matched_ids;

  for (const auto &result : stream.GetResults()) {
    const auto &edges =
        query::test_common::ToList<EdgeAccessor>(result[0].ValueList());
    // Check that path is of expected length. Returned paths should be from
    // shorter to longer ones.
    EXPECT_EQ(edges.size(), expected_level);
    // Check that starting node is correct.
    EXPECT_EQ(edges[0].from().PropsAt(dba->Property(kId)).Value<int64_t>(), 0);
    for (int i = 1; i < static_cast<int>(edges.size()); ++i) {
      // Check that edges form a connected path.
      EXPECT_EQ(edges[i - 1].to(), edges[i].from());
    }
    auto matched_id =
        edges.back().to().PropsAt(dba->Property(kId)).Value<int64_t>();
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
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
