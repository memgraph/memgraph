#include <chrono>
#include <experimental/optional>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "database/graph_db.hpp"
#include "distributed/plan_consumer.hpp"
#include "distributed/pull_rpc_clients.hpp"
#include "distributed_common.hpp"
#include "query/interpreter.hpp"
#include "query_common.hpp"
#include "query_plan_common.hpp"
#include "utils/timer.hpp"

// We use this to ensure a cached plan is removed from the concurrent map and
// properly destructed.
DECLARE_int32(skiplist_gc_interval);

using namespace distributed;
using namespace database;
using namespace std::literals::chrono_literals;

class DistributedInterpretationTest : public DistributedGraphDbTest {
 protected:
  DistributedInterpretationTest() : DistributedGraphDbTest("interpretation") {}

  void SetUp() override {
    DistributedGraphDbTest::SetUp();
    interpreter_.emplace(master());
  }

  void TearDown() override {
    interpreter_ = std::experimental::nullopt;
    DistributedGraphDbTest::TearDown();
  }

  auto RunWithDba(const std::string &query, GraphDbAccessor &dba) {
    std::map<std::string, query::TypedValue> params = {};
    ResultStreamFaker result;
    interpreter_.value()(query, dba, params, false).PullAll(result);
    return result.GetResults();
  }

  auto Run(const std::string &query) {
    GraphDbAccessor dba(master());
    auto results = RunWithDba(query, dba);
    dba.Commit();
    return results;
  }

 private:
  std::experimental::optional<query::Interpreter> interpreter_;
};

TEST_F(DistributedInterpretationTest, PullTest) {
  auto results = Run("OPTIONAL MATCH(n) UNWIND(RANGE(0, 20)) AS X RETURN 1");
  ASSERT_EQ(results.size(), 3 * 21);

  for (auto result : results) {
    ASSERT_EQ(result.size(), 1U);
    ASSERT_EQ(result[0].ValueInt(), 1);
  }
}

TEST_F(DistributedInterpretationTest, PullNoResultsTest) {
  auto results = Run("MATCH (n) RETURN n");
  ASSERT_EQ(results.size(), 0U);
}

TEST_F(DistributedInterpretationTest, CreateExpand) {
  InsertVertex(master());
  InsertVertex(worker(1));
  InsertVertex(worker(1));
  InsertVertex(worker(2));
  InsertVertex(worker(2));
  InsertVertex(worker(2));

  Run("MATCH (n) CREATE (n)-[:T]->(m) RETURN n");

  EXPECT_EQ(VertexCount(master()), 2);
  EXPECT_EQ(VertexCount(worker(1)), 4);
  EXPECT_EQ(VertexCount(worker(2)), 6);
}

TEST_F(DistributedInterpretationTest, RemoteExpandTest2) {
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
  for (size_t i = 0; i < vertices.size(); ++i) {
    for (size_t j = 0; j < vertices.size(); ++j) {
      auto edge_type = get_edge_type(i, j);
      edge_types.push_back(edge_type);
      InsertEdge(vertices[i], vertices[j], edge_type);
    }
  }

  auto results = Run("MATCH (n)-[r1]-(m)-[r2]-(l) RETURN type(r1), type(r2)");
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
  for (size_t n = 0; n < vertices.size(); ++n) {
    for (size_t m = 0; m < vertices.size(); ++m) {
      std::vector<std::string> r1s{get_edge_type(n, m)};
      if (n != m) r1s.push_back(get_edge_type(m, n));
      for (size_t l = 0; l < vertices.size(); ++l) {
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
  ASSERT_EQ(results.size(), expected_result_size);
  std::vector<std::vector<std::string>> got;
  got.reserve(results.size());
  for (const auto &res : results) {
    std::vector<std::string> row;
    row.reserve(res.size());
    for (const auto &col : res) {
      row.push_back(col.Value<std::string>());
    }
    got.push_back(row);
  }
  ASSERT_THAT(got, testing::UnorderedElementsAreArray(expected));
}

TEST_F(DistributedInterpretationTest, Cartesian) {
  // Create some data on the master and both workers.
  storage::Property prop;
  {
    GraphDbAccessor dba{master()};
    auto tx_id = dba.transaction_id();
    GraphDbAccessor dba1{worker(1), tx_id};
    GraphDbAccessor dba2{worker(2), tx_id};
    prop = dba.Property("prop");
    auto add_data = [prop](GraphDbAccessor &dba, int value) {
      dba.InsertVertex().PropsSet(prop, value);
    };

    for (int i = 0; i < 10; ++i) add_data(dba, i);
    for (int i = 10; i < 20; ++i) add_data(dba1, i);
    for (int i = 20; i < 30; ++i) add_data(dba2, i);

    dba.Commit();
  }

  std::vector<std::vector<int64_t>> expected;
  for (int64_t i = 0; i < 30; ++i)
    for (int64_t j = 0; j < 30; ++j) expected.push_back({i, j});

  auto results = Run("MATCH (n), (m) RETURN n.prop, m.prop;");

  size_t expected_result_size = 30 * 30;
  ASSERT_EQ(expected.size(), expected_result_size);
  ASSERT_EQ(results.size(), expected_result_size);

  std::vector<std::vector<int64_t>> got;
  got.reserve(results.size());
  for (const auto &res : results) {
    std::vector<int64_t> row;
    row.reserve(res.size());
    for (const auto &col : res) {
      row.push_back(col.Value<int64_t>());
    }
    got.push_back(row);
  }

  ASSERT_THAT(got, testing::UnorderedElementsAreArray(expected));
}

class TestQueryWaitsOnFutures : public DistributedInterpretationTest {
 protected:
  int QueryExecutionTimeSec(int worker_id) override {
    return worker_id == 2 ? 3 : 1;
  }
};

TEST_F(TestQueryWaitsOnFutures, Test) {
  const int kVertexCount = 10;
  auto make_fully_connected = [](database::GraphDb &db) {
    database::GraphDbAccessor dba(db);
    std::vector<VertexAccessor> vertices;
    for (int i = 0; i < kVertexCount; ++i)
      vertices.emplace_back(dba.InsertVertex());
    auto et = dba.EdgeType("et");
    for (auto &from : vertices)
      for (auto &to : vertices) dba.InsertEdge(from, to, et);
    dba.Commit();
  };

  make_fully_connected(worker(1));
  ASSERT_EQ(VertexCount(worker(1)), kVertexCount);
  ASSERT_EQ(EdgeCount(worker(1)), kVertexCount * kVertexCount);

  {
    utils::Timer timer;
    try {
      Run("MATCH ()--()--()--()--()--()--() RETURN count(1)");
    } catch (...) {
    }
    double seconds = timer.Elapsed().count();
    EXPECT_GT(seconds, 1);
    EXPECT_LT(seconds, 2);
  }

  make_fully_connected(worker(2));
  ASSERT_EQ(VertexCount(worker(2)), kVertexCount);
  ASSERT_EQ(EdgeCount(worker(2)), kVertexCount * kVertexCount);

  {
    utils::Timer timer;
    try {
      Run("MATCH ()--()--()--()--()--()--() RETURN count(1)");
    } catch (...) {
    }
    double seconds = timer.Elapsed().count();
    EXPECT_GT(seconds, 3);
  }
}

TEST_F(DistributedInterpretationTest, PlanExpiration) {
  FLAGS_query_plan_cache_ttl = 1;
  Run("MATCH (n) RETURN n");
  auto ids1 = worker(1).plan_consumer().CachedPlanIds();
  ASSERT_EQ(ids1.size(), 1);
  // Sleep so the cached plan becomes invalid.
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));
  Run("MATCH (n) RETURN n");
  // Sleep so the invalidated plan (removed from cache which is a concurrent
  // map) gets destructed and thus remote caches cleared.
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  auto ids2 = worker(1).plan_consumer().CachedPlanIds();
  ASSERT_EQ(ids2.size(), 1);
  EXPECT_NE(ids1, ids2);
}

TEST_F(DistributedInterpretationTest, ConcurrentPlanExpiration) {
  FLAGS_query_plan_cache_ttl = 1;
  auto count_vertices = [this]() {
    utils::Timer timer;
    while (timer.Elapsed() < 3s) {
      Run("MATCH () RETURN count(1)");
    }
  };
  std::vector<std::thread> counters;
  for (size_t i = 0; i < std::thread::hardware_concurrency(); ++i)
    counters.emplace_back(count_vertices);
  for (auto &t : counters) t.join();
}

TEST_F(DistributedInterpretationTest, OngoingProduceKeyTest) {
  int worker_count = 10;
  for (int i = 0; i < worker_count; ++i) {
    InsertVertex(master());
    InsertVertex(worker(1));
    InsertVertex(worker(2));
  }

  GraphDbAccessor dba(master());
  auto count1 = RunWithDba("MATCH (n) RETURN count(n)", dba);
  dba.AdvanceCommand();
  auto count2 = RunWithDba("MATCH (n) RETURN count(n)", dba);

  ASSERT_EQ(count1[0][0].ValueInt(), 3 * worker_count);
  ASSERT_EQ(count2[0][0].ValueInt(), 3 * worker_count);
}

TEST_F(DistributedInterpretationTest, AdvanceCommandOnWorkers) {
  GraphDbAccessor dba(master());
  RunWithDba("UNWIND RANGE(1, 10) as x CREATE (:A {id: x})", dba);
  dba.AdvanceCommand();
  // Advance commands on workers also.
  auto futures = dba.db().pull_clients().NotifyAllTransactionCommandAdvanced(
      dba.transaction_id());
  for (auto &future : futures) future.wait();

  auto count = RunWithDba("MATCH (n) RETURN count(n)", dba);
  ASSERT_EQ(count[0][0].ValueInt(), 10);
}

int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_skiplist_gc_interval = 1;
  return RUN_ALL_TESTS();
}
