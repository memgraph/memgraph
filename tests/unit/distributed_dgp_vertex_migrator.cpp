#include "distributed_common.hpp"

#include <memory>
#include <thread>
#include <unordered_set>

#include "gtest/gtest.h"

#include "distributed/dgp/vertex_migrator.hpp"
#include "distributed/updates_rpc_clients.hpp"

using namespace distributed;
using namespace database;

/// Check if the following data is migrated correctly accross the cluster:
///   * cypher_id
///   * labels
///   * edge_types
///   * properties

class DistributedVertexMigratorTest : public DistributedGraphDbTest {
 private:
  struct GraphSize {
    int worker_id;
    int vertex_no;
    int edge_no;
  };

 public:
  DistributedVertexMigratorTest() : DistributedGraphDbTest("vertex_migrator") {}

  /**
   * Prefill the cluster with vertices and edges so that the ids are not the
   * same across the cluster.
   */
  void FillOutCluster(const std::vector<int> graph_sizes) {
    for (int i = 0; i < graph_sizes[0]; ++i) {
      auto vaddr = InsertVertex(master());
      InsertEdge(vaddr, vaddr, "edge");
    }
    for (int i = 0; i < graph_sizes[1]; ++i) {
      auto vaddr = InsertVertex(worker(1));
      InsertEdge(vaddr, vaddr, "edge");
    }
    for (int i = 0; i < graph_sizes[2]; ++i) {
      auto vaddr = InsertVertex(worker(2));
      InsertEdge(vaddr, vaddr, "edge");
    }
  }

  /**
   * Wait for all futures and commit the transaction.
   */
  void MasterApplyUpdatesAndCommit(database::GraphDbAccessor *dba) {
    {
      auto apply_futures = master().updates_clients().UpdateApplyAll(
          master().WorkerId(), dba->transaction().id_);
      // Destructor waits on application
    }
    dba->Commit();
  }

  /**
   * Migrate vertex with a given cypher_id from a given database to a given
   * machine.
   */
  void MigrateVertexAndCommit(database::GraphDbAccessor *from_dba,
                              int64_t cypher_id, int to_worker_id) {
    auto vacc = FindVertex(from_dba, cypher_id);
    distributed::dgp::VertexMigrator migrator(from_dba);
    migrator.MigrateVertex(*vacc, to_worker_id);
    MasterApplyUpdatesAndCommit(from_dba);
  }

  /**
   * Assert number of vertices and edges on each worker.
   *
   * @param sizes An array of structs that hold information about graph size
   * on each worker.
   */
  void CheckGraphSizes(const std::vector<GraphSize> &graph_sizes) {
    for (auto &graph_size : graph_sizes) {
      if (graph_size.worker_id == 0) {  // on master
        ASSERT_EQ(VertexCount(master()), graph_size.vertex_no);
        ASSERT_EQ(EdgeCount(master()), graph_size.edge_no);
      } else {  // on workers
        ASSERT_EQ(VertexCount(worker(graph_size.worker_id)),
                  graph_size.vertex_no);
        ASSERT_EQ(EdgeCount(worker(graph_size.worker_id)), graph_size.edge_no);
      }
    }
  }

  /**
   * Collect all visible cypher_ids into an unordered_map for easier
   * checking.
   */
  auto CollectVertexCypherIds(database::GraphDbAccessor *dba) {
    std::unordered_set<int64_t> cypher_ids;
    for (auto &vertex : dba->Vertices(false)) {
      cypher_ids.emplace(vertex.CypherId());
    }
    return cypher_ids;
  }

  /**
   * Collect all visible cypher_ids into an unordered_map for easier
   * checking.
   */
  auto CollectEdgeCypherIds(database::GraphDbAccessor *dba) {
    std::unordered_set<int64_t> cypher_ids;
    for (auto &edge : dba->Edges(false)) {
      cypher_ids.emplace(edge.CypherId());
    }
    return cypher_ids;
  }

  /**
   * Check that container contains all containees.
   *
   * @tparam type of elements in the sets.
   */
  template <typename T>
  auto ContainsAll(const std::unordered_set<T> &container,
                   const std::unordered_set<T> &containees) {
    //  TODO (C++20): container.contains(item);
    return std::all_of(containees.begin(), containees.end(),
                       [&container](T item) {
                         return container.find(item) != container.end();
                       });
  }

  /**
   * Find vertex with a given cypher_id within a given database.
   */
  std::optional<VertexAccessor> FindVertex(database::GraphDbAccessor *dba,
                                           int64_t cypher_id) {
    for (auto &vertex : dba->Vertices(false)) {
      if (vertex.CypherId() == cypher_id)
        return std::optional<VertexAccessor>(vertex);
    }
    return std::nullopt;
  }

  /**
   * Find edge with a given cypher_id within a given database.
   */
  std::optional<EdgeAccessor> FindEdge(database::GraphDbAccessor *dba,
                                       int64_t cypher_id) {
    for (auto &edge : dba->Edges(false)) {
      if (edge.CypherId() == cypher_id)
        return std::optional<EdgeAccessor>(edge);
    }
    return std::nullopt;
  }
};

TEST_F(DistributedVertexMigratorTest, MigrationofLabelsEdgeTypesAndProperties) {
  {
    auto dba = master().Access();
    auto va = dba->InsertVertex();
    auto vb = dba->InsertVertex();
    va.add_label(dba->Label("l"));
    va.add_label(dba->Label("k"));
    vb.add_label(dba->Label("l"));
    vb.add_label(dba->Label("k"));
    va.PropsSet(dba->Property("p"), 42);
    vb.PropsSet(dba->Property("p"), 42);

    auto ea = dba->InsertEdge(va, vb, dba->EdgeType("edge"));
    ea.PropsSet(dba->Property("pe"), 43);
    auto eb = dba->InsertEdge(vb, va, dba->EdgeType("edge"));
    eb.PropsSet(dba->Property("pe"), 43);
    dba->Commit();
  }

  {
    auto dba = master().Access();
    distributed::dgp::VertexMigrator migrator(dba.get());
    for (auto &vertex : dba->Vertices(false)) {
      migrator.MigrateVertex(vertex, worker(1).WorkerId());
    }
    MasterApplyUpdatesAndCommit(dba.get());
  }

  {
    auto dba = worker(1).Access();
    EXPECT_EQ(VertexCount(master()), 0);
    ASSERT_EQ(VertexCount(worker(1)), 2);
    for (auto vertex : dba->Vertices(false)) {
      ASSERT_EQ(vertex.labels().size(), 2);
      EXPECT_EQ(vertex.labels()[0], dba->Label("l"));
      EXPECT_EQ(vertex.labels()[1], dba->Label("k"));
      EXPECT_EQ(vertex.PropsAt(dba->Property("p")).Value<int64_t>(), 42);
    }

    ASSERT_EQ(EdgeCount(worker(1)), 2);
    auto edge = *dba->Edges(false).begin();
    EXPECT_EQ(edge.PropsAt(dba->Property("pe")).Value<int64_t>(), 43);
    EXPECT_EQ(edge.EdgeType(), dba->EdgeType("edge"));
  }
}

TEST_F(DistributedVertexMigratorTest, MigrationOfSelfLoopEdge) {
  FillOutCluster({10, 0, 0});

  // Create additional node on master and migrate that node to worker1.
  auto vaddr = InsertVertex(master());
  auto eaddr = InsertEdge(vaddr, vaddr, "edge");
  auto dba = master().Access();
  VertexAccessor vacc(vaddr, *dba);
  EdgeAccessor eacc(eaddr, *dba);
  auto initial_vcypher_id = vacc.CypherId();
  auto initial_ecypher_id = eacc.CypherId();
  {
    auto dba = master().Access();
    MigrateVertexAndCommit(dba.get(), initial_vcypher_id, worker(1).WorkerId());
  }

  // Check grpah size and cypher_ids.
  CheckGraphSizes({{0, 10, 10}, {1, 1, 1}, {2, 0, 0}});
  {
    auto dba = worker(1).Access();
    auto vaccessor = *dba->Vertices(false).begin();
    auto eaccessor = *dba->Edges(false).begin();
    ASSERT_EQ(vaccessor.CypherId(), initial_vcypher_id);
    ASSERT_EQ(eaccessor.CypherId(), initial_ecypher_id);
    ASSERT_TRUE(eaccessor.from_addr().is_local());
    ASSERT_TRUE(eaccessor.to_addr().is_local());
  }
}

TEST_F(DistributedVertexMigratorTest, MigrationOfSimpleVertex) {
  FillOutCluster({1, 100, 200});

  auto v1addr = InsertVertex(master());
  auto v2addr = InsertVertex(master());
  auto e1addr = InsertEdge(v1addr, v2addr, "edge");
  auto dba = master().Access();
  auto original_v1_cypher_id = VertexAccessor(v1addr, *dba).CypherId();
  auto original_v2_cypher_id = VertexAccessor(v2addr, *dba).CypherId();
  std::unordered_set<int64_t> original_v_cypher_ids = {original_v1_cypher_id,
                                                       original_v2_cypher_id};
  auto original_e1_cypher_id = EdgeAccessor(e1addr, *dba).CypherId();
  std::unordered_set<int64_t> original_e_cypher_ids = {original_e1_cypher_id};
  CheckGraphSizes({{0, 3, 2}, {1, 100, 100}, {2, 200, 200}});

  // Migrate v2 from master to worker1.
  {
    auto dba = master().Access();
    MigrateVertexAndCommit(dba.get(), original_v2_cypher_id,
                           worker(1).WorkerId());
  }
  CheckGraphSizes({{0, 2, 2}, {1, 101, 100}, {2, 200, 200}});
  {
    auto dba = worker(1).Access();
    auto v2acc = FindVertex(dba.get(), original_v2_cypher_id);
    ASSERT_TRUE(v2acc);
  }

  // Migrate v1 from master to worker1.
  {
    auto dba = master().Access();
    MigrateVertexAndCommit(dba.get(), original_v1_cypher_id,
                           worker(1).WorkerId());
  }
  CheckGraphSizes({{0, 1, 1}, {1, 102, 101}, {2, 200, 200}});
  {
    auto dba = worker(1).Access();
    auto worker1_v_cypher_ids = CollectVertexCypherIds(dba.get());
    auto worker1_e_cypher_ids = CollectEdgeCypherIds(dba.get());
    ASSERT_TRUE(ContainsAll(worker1_v_cypher_ids, original_v_cypher_ids));
    ASSERT_TRUE(ContainsAll(worker1_e_cypher_ids, original_e_cypher_ids));
  }

  // Migrate v1 from worker1 to worker2.
  {
    auto dba = worker(1).Access();
    MigrateVertexAndCommit(dba.get(), original_v1_cypher_id,
                           worker(2).WorkerId());
  }
  CheckGraphSizes({{0, 1, 1}, {1, 101, 100}, {2, 201, 201}});
  {
    auto dba = worker(2).Access();
    auto worker2_v_cypher_ids = CollectVertexCypherIds(dba.get());
    auto worker2_e_cypher_ids = CollectEdgeCypherIds(dba.get());
    ASSERT_TRUE(ContainsAll(worker2_v_cypher_ids, {original_v1_cypher_id}));
    ASSERT_TRUE(ContainsAll(worker2_e_cypher_ids, {original_e1_cypher_id}));
  }

  // Migrate v2 from worker1 to master.
  {
    auto dba = worker(1).Access();
    MigrateVertexAndCommit(dba.get(), original_v2_cypher_id,
                           master().WorkerId());
  }
  CheckGraphSizes({{0, 2, 1}, {1, 100, 100}, {2, 201, 201}});
  {
    auto master_dba = master().Access();
    auto worker2_dba = worker(2).Access();
    auto master_v_cypher_ids = CollectVertexCypherIds(master_dba.get());
    auto worker2_v_cypher_ids = CollectVertexCypherIds(worker2_dba.get());
    auto worker2_e_cypher_ids = CollectEdgeCypherIds(worker2_dba.get());
    ASSERT_TRUE(ContainsAll(master_v_cypher_ids, {original_v2_cypher_id}));
    ASSERT_TRUE(ContainsAll(worker2_v_cypher_ids, {original_v1_cypher_id}));
    ASSERT_TRUE(ContainsAll(worker2_e_cypher_ids, {original_e1_cypher_id}));
  }

  // Migrate v2 from master wo worker2.
  {
    auto dba = master().Access();
    MigrateVertexAndCommit(dba.get(), original_v2_cypher_id,
                           worker(2).WorkerId());
  }
  CheckGraphSizes({{0, 1, 1}, {1, 100, 100}, {2, 202, 201}});
  {
    auto dba = worker(2).Access();
    auto worker2_v_cypher_ids = CollectVertexCypherIds(dba.get());
    auto worker2_e_cypher_ids = CollectEdgeCypherIds(dba.get());
    ASSERT_TRUE(ContainsAll(worker2_v_cypher_ids, original_v_cypher_ids));
    ASSERT_TRUE(ContainsAll(worker2_e_cypher_ids, original_e_cypher_ids));
  }
}

TEST_F(DistributedVertexMigratorTest, MigrationOfVertexWithMultipleEdges) {
  FillOutCluster({1, 100, 200});

  auto m_v1addr = InsertVertex(master());
  auto m_v2addr = InsertVertex(master());

  auto v3addr = InsertVertex(master());
  auto dba = master().Access();
  auto original_v3_cypher_id = VertexAccessor(v3addr, *dba).CypherId();

  auto w1_v1addr = InsertVertex(worker(1));
  auto w1_v2addr = InsertVertex(worker(1));

  auto w2_v1addr = InsertVertex(worker(2));
  auto w2_v2addr = InsertVertex(worker(2));

  auto e1addr = InsertEdge(v3addr, m_v1addr, "edge");
  auto e2addr = InsertEdge(v3addr, m_v2addr, "edge");
  auto e3addr = InsertEdge(v3addr, w1_v1addr, "edge");
  auto e4addr = InsertEdge(v3addr, w1_v2addr, "edge");
  auto e5addr = InsertEdge(v3addr, w2_v1addr, "edge");
  auto e6addr = InsertEdge(v3addr, w2_v2addr, "edge");
  std::unordered_set<int64_t> original_e_cypher_ids = {
      EdgeAccessor(e1addr, *dba).CypherId(),
      EdgeAccessor(e2addr, *dba).CypherId(),
      EdgeAccessor(e3addr, *dba).CypherId(),
      EdgeAccessor(e4addr, *dba).CypherId(),
      EdgeAccessor(e5addr, *dba).CypherId(),
      EdgeAccessor(e6addr, *dba).CypherId()};

  CheckGraphSizes({{0, 4, 7}, {1, 102, 100}, {2, 202, 200}});

  // Migrate v3 from master to worker1.
  {
    auto dba = master().Access();
    MigrateVertexAndCommit(dba.get(), original_v3_cypher_id,
                           worker(1).WorkerId());
  }
  CheckGraphSizes({{0, 3, 1}, {1, 103, 106}, {2, 202, 200}});
  {
    auto dba = worker(1).Access();
    auto worker1_v_cypher_ids = CollectVertexCypherIds(dba.get());
    auto worker1_e_cypher_ids = CollectEdgeCypherIds(dba.get());
    ASSERT_TRUE(ContainsAll(worker1_v_cypher_ids, {original_v3_cypher_id}));
    ASSERT_TRUE(ContainsAll(worker1_e_cypher_ids, original_e_cypher_ids));
  }

  // Migrate v3 from worker1 to worker2.
  {
    auto dba = worker(1).Access();
    MigrateVertexAndCommit(dba.get(), original_v3_cypher_id,
                           worker(2).WorkerId());
  }
  CheckGraphSizes({{0, 3, 1}, {1, 102, 100}, {2, 203, 206}});
  {
    auto dba = worker(2).Access();
    auto worker2_v_cypher_ids = CollectVertexCypherIds(dba.get());
    auto worker2_e_cypher_ids = CollectEdgeCypherIds(dba.get());
    ASSERT_TRUE(ContainsAll(worker2_v_cypher_ids, {original_v3_cypher_id}));
    ASSERT_TRUE(ContainsAll(worker2_e_cypher_ids, original_e_cypher_ids));
  }

  // Migrate v3 from worker2 back to master.
  {
    auto dba = worker(2).Access();
    MigrateVertexAndCommit(dba.get(), original_v3_cypher_id,
                           master().WorkerId());
  }
  CheckGraphSizes({{0, 4, 7}, {1, 102, 100}, {2, 202, 200}});
  {
    auto dba = master().Access();
    auto master_v_cypher_ids = CollectVertexCypherIds(dba.get());
    auto master_e_cypher_ids = CollectEdgeCypherIds(dba.get());
    ASSERT_TRUE(ContainsAll(master_v_cypher_ids, {original_v3_cypher_id}));
    ASSERT_TRUE(ContainsAll(master_e_cypher_ids, original_e_cypher_ids));
  }
}
