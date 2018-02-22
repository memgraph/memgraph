#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "database/graph_db.hpp"
#include "distributed_common.hpp"
#include "query/interpreter.hpp"
#include "query_common.hpp"
#include "query_plan_common.hpp"

using namespace distributed;
using namespace database;

TEST_F(DistributedGraphDbTest, RemotePullTest) {
  using Interpreter = query::Interpreter;
  std::map<std::string, query::TypedValue> params = {};

  GraphDbAccessor dba(master());

  ResultStreamFaker result;
  Interpreter interpreter_;
  interpreter_("OPTIONAL MATCH(n) UNWIND(RANGE(0, 20)) AS X RETURN 1", dba,
               params, false)
      .PullAll(result);

  // Three instances (master + 2 workers) with 21 result each.
  uint expected_results = 3U * 21;
  ASSERT_EQ(result.GetHeader().size(), 1U);
  EXPECT_EQ(result.GetHeader()[0], "1");
  ASSERT_EQ(result.GetResults().size(), expected_results);

  for (uint i = 0; i < expected_results; ++i) {
    ASSERT_EQ(result.GetResults()[i].size(), 1U);
    ASSERT_EQ(result.GetResults()[i][0].Value<int64_t>(), 1);
  }
}

TEST_F(DistributedGraphDbTest, RemotePullNoResultsTest) {
  using Interpreter = query::Interpreter;
  std::map<std::string, query::TypedValue> params = {};

  GraphDbAccessor dba(master());

  ResultStreamFaker result;
  Interpreter interpreter_;
  interpreter_("MATCH (n) RETURN n", dba, params, false).PullAll(result);

  ASSERT_EQ(result.GetHeader().size(), 1U);
  EXPECT_EQ(result.GetHeader()[0], "n");
  ASSERT_EQ(result.GetResults().size(), 0U);
}

TEST_F(DistributedGraphDbTest, RemoteExpandTest2) {
  // Make a fully connected graph with vertices scattered across master and
  // worker storage.
  // Vertex count is low, because test gets exponentially slower. The expected
  // result size is ~ vertices^3, and then that is compared at the end in no
  // particular order which causes O(result_size^2) comparisons.
  int verts_per_storage = 3;
  std::vector<storage::VertexAddress> vertices;
  vertices.reserve(verts_per_storage * 3);
  auto add_vertices = [this, &vertices, &verts_per_storage](auto &db) {
    for (int i = 0; i < verts_per_storage; ++i)
      vertices.push_back(InsertVertex(db));
  };
  add_vertices(master());
  add_vertices(worker(1));
  add_vertices(worker(2));
  auto get_edge_type = [](int v1, int v2) {
    return std::to_string(v1) + "-" + std::to_string(v2);
  };
  std::vector<std::string> edge_types;
  edge_types.reserve(vertices.size() * vertices.size());
  for (int i = 0; i < vertices.size(); ++i) {
    for (int j = 0; j < vertices.size(); ++j) {
      auto edge_type = get_edge_type(i, j);
      edge_types.push_back(edge_type);
      InsertEdge(vertices[i], vertices[j], edge_type);
    }
  }
  query::Interpreter interpret;
  std::map<std::string, query::TypedValue> params;
  GraphDbAccessor dba(master());
  ResultStreamFaker result;
  interpret("MATCH (n)-[r1]-(m)-[r2]-(l) RETURN type(r1), type(r2)", dba,
            params, false)
      .PullAll(result);
  ASSERT_EQ(result.GetHeader().size(), 2U);
  // We expect the number of results to be:
  size_t expected_result_size =
      // pick (n)
      vertices.size() *
      // pick both directed edges to other (m) and a
      // single edge to (m) which equals (n), hence -1
      (2 * vertices.size() - 1) *
      // Pick as before, but exclude the previously taken edge, hence another -1
      (2 * vertices.size() - 1 - 1);
  std::vector<std::vector<std::string>> expected;
  expected.reserve(expected_result_size);
  for (int n = 0; n < vertices.size(); ++n) {
    for (int m = 0; m < vertices.size(); ++m) {
      std::vector<std::string> r1s{get_edge_type(n, m)};
      if (n != m) r1s.push_back(get_edge_type(m, n));
      for (int l = 0; l < vertices.size(); ++l) {
        std::vector<std::string> r2s{get_edge_type(m, l)};
        if (m != l) r2s.push_back(get_edge_type(l, m));
        for (const auto &r1 : r1s) {
          for (const auto &r2 : r2s) {
            if (r1 == r2) continue;
            expected.push_back({r1, r2});
          }
        }
      }
    }
  }
  ASSERT_EQ(expected.size(), expected_result_size);
  ASSERT_EQ(result.GetResults().size(), expected_result_size);
  std::vector<std::vector<std::string>> got;
  got.reserve(result.GetResults().size());
  for (const auto &res : result.GetResults()) {
    std::vector<std::string> row;
    row.reserve(res.size());
    for (const auto &col : res) {
      row.push_back(col.Value<std::string>());
    }
    got.push_back(row);
  }
  ASSERT_THAT(got, testing::UnorderedElementsAreArray(expected));
}
