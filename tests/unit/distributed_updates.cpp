#include <gtest/gtest.h>

#include "database/graph_db_accessor.hpp"
#include "distributed/remote_updates_rpc_clients.hpp"
#include "distributed/remote_updates_rpc_server.hpp"

#include "distributed_common.hpp"

class DistributedUpdateTest : public DistributedGraphDbTest {
 protected:
  std::unique_ptr<database::GraphDbAccessor> dba1;
  std::unique_ptr<database::GraphDbAccessor> dba2;
  storage::Label label;
  std::unique_ptr<VertexAccessor> v1_dba1;
  std::unique_ptr<VertexAccessor> v1_dba2;

  void SetUp() override {
    DistributedGraphDbTest::SetUp();

    database::GraphDbAccessor dba_tx1{worker(1)};
    auto v = dba_tx1.InsertVertex();
    auto v_ga = v.GlobalAddress();
    dba_tx1.Commit();

    dba1 = std::make_unique<database::GraphDbAccessor>(worker(1));
    dba2 = std::make_unique<database::GraphDbAccessor>(worker(2),
                                                       dba1->transaction_id());

    v1_dba1 = std::make_unique<VertexAccessor>(v_ga, *dba1);
    v1_dba2 = std::make_unique<VertexAccessor>(v_ga, *dba2);
    ASSERT_FALSE(v1_dba2->address().is_local());
    label = dba1->Label("l");
    v1_dba2->add_label(label);
  }

  void TearDown() override {
    dba2 = nullptr;
    dba1 = nullptr;
    DistributedGraphDbTest::TearDown();
  }
};

#define EXPECT_LABEL(var, old_result, new_result) \
  {                                               \
    var->SwitchOld();                             \
    EXPECT_EQ(var->has_label(label), old_result); \
    var->SwitchNew();                             \
    EXPECT_EQ(var->has_label(label), new_result); \
  }

TEST_F(DistributedUpdateTest, RemoteUpdateLocalOnly) {
  EXPECT_LABEL(v1_dba2, false, true);
  EXPECT_LABEL(v1_dba1, false, false);
}

TEST_F(DistributedUpdateTest, RemoteUpdateApply) {
  EXPECT_LABEL(v1_dba1, false, false);
  worker(1).remote_updates_server().Apply(dba1->transaction_id());
  EXPECT_LABEL(v1_dba1, false, true);
}

#undef EXPECT_LABEL

TEST_F(DistributedGraphDbTest, CreateVertex) {
  gid::Gid gid;
  {
    database::GraphDbAccessor dba{worker(1)};
    auto v = dba.InsertVertexIntoRemote(2, {}, {});
    gid = v.gid();
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba{worker(2)};
    auto v = dba.FindVertex(gid, false);
    ASSERT_TRUE(v);
  }
}

TEST_F(DistributedGraphDbTest, CreateVertexWithUpdate) {
  gid::Gid gid;
  storage::Property prop;
  {
    database::GraphDbAccessor dba{worker(1)};
    auto v = dba.InsertVertexIntoRemote(2, {}, {});
    gid = v.gid();
    prop = dba.Property("prop");
    v.PropsSet(prop, 42);
    worker(2).remote_updates_server().Apply(dba.transaction_id());
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba{worker(2)};
    auto v = dba.FindVertex(gid, false);
    ASSERT_TRUE(v);
    EXPECT_EQ(v->PropsAt(prop).Value<int64_t>(), 42);
  }
}

TEST_F(DistributedGraphDbTest, CreateVertexWithData) {
  gid::Gid gid;
  storage::Label l1;
  storage::Label l2;
  storage::Property prop;
  {
    database::GraphDbAccessor dba{worker(1)};
    l1 = dba.Label("l1");
    l2 = dba.Label("l2");
    prop = dba.Property("prop");
    auto v = dba.InsertVertexIntoRemote(2, {l1, l2}, {{prop, 42}});
    gid = v.gid();

    // Check local visibility before commit.
    EXPECT_TRUE(v.has_label(l1));
    EXPECT_TRUE(v.has_label(l2));
    EXPECT_EQ(v.PropsAt(prop).Value<int64_t>(), 42);

    worker(2).remote_updates_server().Apply(dba.transaction_id());
    dba.Commit();
  }
  {
    database::GraphDbAccessor dba{worker(2)};
    auto v = dba.FindVertex(gid, false);
    ASSERT_TRUE(v);
    // Check remote data after commit.
    EXPECT_TRUE(v->has_label(l1));
    EXPECT_TRUE(v->has_label(l2));
    EXPECT_EQ(v->PropsAt(prop).Value<int64_t>(), 42);
  }
}

class DistributedEdgeCreateTest : public DistributedGraphDbTest {
 protected:
  storage::VertexAddress w1_a;
  storage::VertexAddress w1_b;
  storage::VertexAddress w2_a;
  storage::EdgeAddress e_ga;

  void SetUp() override {
    DistributedGraphDbTest::SetUp();
    w1_a = InsertVertex(worker(1));
    w1_b = InsertVertex(worker(1));
    w2_a = InsertVertex(worker(2));
  }

  void CreateEdge(database::GraphDb &creator, storage::VertexAddress from_addr,
                  storage::VertexAddress to_addr) {
    database::GraphDbAccessor dba{creator};
    auto edge_type = dba.EdgeType("et");
    VertexAccessor v1{from_addr, dba};
    VertexAccessor v2{to_addr, dba};
    e_ga = dba.InsertEdge(v1, v2, edge_type).GlobalAddress();
    master().remote_updates_server().Apply(dba.transaction_id());
    worker(1).remote_updates_server().Apply(dba.transaction_id());
    worker(2).remote_updates_server().Apply(dba.transaction_id());
    dba.Commit();
  }

  int EdgeCount(database::GraphDb &db) {
    database::GraphDbAccessor dba(db);
    auto edges = dba.Edges(false);
    return std::distance(edges.begin(), edges.end());
  };

  void CheckCounts(int master_count, int worker1_count, int worker2_count) {
    EXPECT_EQ(EdgeCount(master()), master_count);
    EXPECT_EQ(EdgeCount(worker(1)), worker1_count);
    EXPECT_EQ(EdgeCount(worker(2)), worker2_count);
  }

  void CheckState(database::GraphDb &db, bool edge_is_local,
                  storage::VertexAddress from_addr,
                  storage::VertexAddress to_addr) {
    database::GraphDbAccessor dba{db};

    // Check edge data.
    {
      EdgeAccessor edge{e_ga, dba};
      EXPECT_EQ(edge.address().is_local(), edge_is_local);
      EXPECT_EQ(edge.GlobalAddress(), e_ga);
      auto from = edge.from();
      EXPECT_EQ(from.GlobalAddress(), from_addr);
      auto to = edge.to();
      EXPECT_EQ(to.GlobalAddress(), to_addr);
    }

    auto edges = [](auto iterable) {
      std::vector<EdgeAccessor> res;
      for (auto edge : iterable) res.emplace_back(edge);
      return res;
    };

    // Check `from` data.
    {
      VertexAccessor from{from_addr, dba};
      ASSERT_EQ(edges(from.out()).size(), 1);
      EXPECT_EQ(edges(from.out())[0].GlobalAddress(), e_ga);
      // In case of cycles we have 1 in the `in` edges.
      EXPECT_EQ(edges(from.in()).size(), from_addr == to_addr);
    }

    // Check `to` data.
    {
      VertexAccessor to{to_addr, dba};
      // In case of cycles we have 1 in the `out` edges.
      EXPECT_EQ(edges(to.out()).size(), from_addr == to_addr);
      ASSERT_EQ(edges(to.in()).size(), 1);
      EXPECT_EQ(edges(to.in())[0].GlobalAddress(), e_ga);
    }
  }

  void CheckAll(storage::VertexAddress from_addr,
                storage::VertexAddress to_addr) {
    int edge_worker = from_addr.worker_id();
    CheckCounts(edge_worker == 0, edge_worker == 1, edge_worker == 2);
    CheckState(master(), edge_worker == 0, from_addr, to_addr);
    CheckState(worker(1), edge_worker == 1, from_addr, to_addr);
    CheckState(worker(2), edge_worker == 2, from_addr, to_addr);
  }

  void TearDown() override { DistributedGraphDbTest::TearDown(); }
};

TEST_F(DistributedEdgeCreateTest, LocalRemote) {
  CreateEdge(worker(1), w1_a, w2_a);
  CheckAll(w1_a, w2_a);
}

TEST_F(DistributedEdgeCreateTest, RemoteLocal) {
  CreateEdge(worker(2), w1_a, w2_a);
  CheckAll(w1_a, w2_a);
}

TEST_F(DistributedEdgeCreateTest, RemoteRemoteDifferentWorkers) {
  CreateEdge(master(), w1_a, w2_a);
  CheckAll(w1_a, w2_a);
}

TEST_F(DistributedEdgeCreateTest, RemoteRemoteSameWorkers) {
  CreateEdge(master(), w1_a, w1_b);
  CheckAll(w1_a, w1_b);
}

TEST_F(DistributedEdgeCreateTest, RemoteRemoteCycle) {
  CreateEdge(master(), w1_a, w1_a);
  CheckAll(w1_a, w1_a);
}
