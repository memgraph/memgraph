#include "distributed_common.hpp"

#include <memory>
#include <thread>
#include <unordered_set>

#include "gtest/gtest.h"

#include "distributed/updates_rpc_clients.hpp"
#include "storage/dynamic_graph_partitioner/vertex_migrator.hpp"

using namespace distributed;
using namespace database;

DECLARE_bool(generate_vertex_ids);
DECLARE_bool(generate_edge_ids);

class DistributedVertexMigratorTest : public DistributedGraphDbTest {
 public:
  DistributedVertexMigratorTest() : DistributedGraphDbTest("vertex_migrator") {}
};

// Check if the auto-generated gid property is unchanged after migration
TEST_F(DistributedVertexMigratorTest, VertexEdgeGidSaved) {
  FLAGS_generate_vertex_ids = true;
  FLAGS_generate_edge_ids = true;
  // Fill master so that the ids are not the same on master and worker 1
  for (int i = 0; i < 10; ++i) {
    auto va = InsertVertex(master());
    InsertEdge(va, va, "edge");
  }

  auto va = InsertVertex(master());
  auto ea = InsertEdge(va, va, "edge");
  PropertyValue old_vgid_property(42);
  PropertyValue old_egid_property(42);
  {
    database::GraphDbAccessor dba(master());
    VertexAccessor vaccessor(va, dba);
    old_vgid_property =
        vaccessor.PropsAt(dba.Property(PropertyValueStore::IdPropertyName));
    EXPECT_FALSE(old_vgid_property.IsNull());

    EdgeAccessor eaccessor(ea, dba);
    old_egid_property =
        eaccessor.PropsAt(dba.Property(PropertyValueStore::IdPropertyName));
    EXPECT_FALSE(old_egid_property.IsNull());
  }
  {
    database::GraphDbAccessor dba(master());
    VertexAccessor accessor(va, dba);
    VertexMigrator migrator(&dba);
    migrator.MigrateVertex(accessor, worker(1).WorkerId());
    {
      auto apply_futures = master().updates_clients().UpdateApplyAll(
          master().WorkerId(), dba.transaction().id_);
      // Destructor waits on application
    }
    dba.Commit();
  }
  ASSERT_EQ(VertexCount(worker(1)), 1);
  {
    database::GraphDbAccessor dba(worker(1));
    auto vaccessor = *dba.Vertices(false).begin();
    auto eaccessor = *dba.Edges(false).begin();
    auto new_vgid_property =
        vaccessor.PropsAt(dba.Property(PropertyValueStore::IdPropertyName));
    auto new_egid_property =
        eaccessor.PropsAt(dba.Property(PropertyValueStore::IdPropertyName));
    EXPECT_EQ(old_vgid_property.Value<int64_t>(),
              new_vgid_property.Value<int64_t>());
    EXPECT_EQ(old_egid_property.Value<int64_t>(),
              new_egid_property.Value<int64_t>());
  }
}

// Checks if two connected nodes from master will be transfered to worker 1 and
// if edge from vertex on the worker 2 will now point to worker 1 after transfer
TEST_F(DistributedVertexMigratorTest, SomeTransfer) {
  auto va = InsertVertex(master());
  auto vb = InsertVertex(master());
  auto vc = InsertVertex(worker(2));
  InsertEdge(va, vb, "edge");
  InsertEdge(vc, va, "edge");
  {
    database::GraphDbAccessor dba(master());
    VertexMigrator migrator(&dba);
    for (auto &vertex : dba.Vertices(false)) {
      migrator.MigrateVertex(vertex, worker(1).WorkerId());
    }
    {
      auto apply_futures = master().updates_clients().UpdateApplyAll(
          master().WorkerId(), dba.transaction().id_);
      // Destructor waits on application
    }
    dba.Commit();
  }

  EXPECT_EQ(VertexCount(master()), 0);
  EXPECT_EQ(EdgeCount(master()), 0);
  EXPECT_EQ(VertexCount(worker(1)), 2);
  EXPECT_EQ(EdgeCount(worker(1)), 1);

  EXPECT_EQ(VertexCount(worker(2)), 1);
  ASSERT_EQ(EdgeCount(worker(2)), 1);
  {
    database::GraphDbAccessor dba(worker(2));
    auto edge = *dba.Edges(false).begin();

    // Updated remote edge on another worker
    EXPECT_EQ(edge.to_addr().worker_id(), worker(1).WorkerId());
  }
}

// Check if cycle edge is transfered only once since it's contained in both in
// and out edges of a vertex and if not handled correctly could cause problems
TEST_F(DistributedVertexMigratorTest, EdgeCycle) {
  auto va = InsertVertex(master());
  InsertEdge(va, va, "edge");
  {
    database::GraphDbAccessor dba(master());
    VertexMigrator migrator(&dba);
    for (auto &vertex : dba.Vertices(false)) {
      migrator.MigrateVertex(vertex, worker(1).WorkerId());
    }
    {
      auto apply_futures = master().updates_clients().UpdateApplyAll(
          master().WorkerId(), dba.transaction().id_);
      // Destructor waits on application
    }
    dba.Commit();
  }

  EXPECT_EQ(VertexCount(master()), 0);
  EXPECT_EQ(EdgeCount(master()), 0);
  EXPECT_EQ(VertexCount(worker(1)), 1);
  EXPECT_EQ(EdgeCount(worker(1)), 1);
}

TEST_F(DistributedVertexMigratorTest, TransferLabelsAndProperties) {
  {
    database::GraphDbAccessor dba(master());
    auto va = dba.InsertVertex();
    auto vb = dba.InsertVertex();
    va.add_label(dba.Label("l"));
    vb.add_label(dba.Label("l"));
    va.PropsSet(dba.Property("p"), 42);
    vb.PropsSet(dba.Property("p"), 42);

    auto ea = dba.InsertEdge(va, vb, dba.EdgeType("edge"));
    ea.PropsSet(dba.Property("pe"), 43);
    auto eb = dba.InsertEdge(vb, va, dba.EdgeType("edge"));
    eb.PropsSet(dba.Property("pe"), 43);
    dba.Commit();
  }

  {
    database::GraphDbAccessor dba(master());
    VertexMigrator migrator(&dba);
    for (auto &vertex : dba.Vertices(false)) {
      migrator.MigrateVertex(vertex, worker(1).WorkerId());
    }
    {
      auto apply_futures = master().updates_clients().UpdateApplyAll(
          master().WorkerId(), dba.transaction().id_);
      // Destructor waits on application
    }
    dba.Commit();
  }

  {
    database::GraphDbAccessor dba(worker(1));
    EXPECT_EQ(VertexCount(master()), 0);
    ASSERT_EQ(VertexCount(worker(1)), 2);
    for (auto vertex : dba.Vertices(false)) {
      ASSERT_EQ(vertex.labels().size(), 1);
      EXPECT_EQ(vertex.labels()[0], dba.Label("l"));
      EXPECT_EQ(vertex.PropsAt(dba.Property("p")).Value<int64_t>(), 42);
    }

    ASSERT_EQ(EdgeCount(worker(1)), 2);
    auto edge = *dba.Edges(false).begin();
    EXPECT_EQ(edge.PropsAt(dba.Property("pe")).Value<int64_t>(), 43);
    EXPECT_EQ(edge.EdgeType(), dba.EdgeType("edge"));
  }
}
