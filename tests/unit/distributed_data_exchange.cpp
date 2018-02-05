#include <unordered_map>

#include "gtest/gtest.h"

#include "database/graph_db_accessor.hpp"
#include "storage/edge_accessor.hpp"
#include "storage/vertex_accessor.hpp"

#include "distributed_common.hpp"

using namespace database;

TEST_F(DistributedGraphDbTest, RemoteDataGetting) {
  // Only old data is visible remotely, so create and commit some data.
  gid::Gid v1_id, v2_id, e1_id;

  {
    GraphDbAccessor dba{master()};
    auto v1 = dba.InsertVertex();
    auto v2 = dba.InsertVertex();
    auto e1 = dba.InsertEdge(v1, v2, dba.EdgeType("et"));

    // Set some data so we see we're getting the right stuff.
    v1.PropsSet(dba.Property("p1"), 42);
    v1.add_label(dba.Label("label"));
    v2.PropsSet(dba.Property("p2"), "value");
    e1.PropsSet(dba.Property("p3"), true);

    v1_id = v1.gid();
    v2_id = v2.gid();
    e1_id = e1.gid();

    dba.Commit();
  }

  // The master must start a transaction before workers can work in it.
  GraphDbAccessor master_dba{master()};

  {
    GraphDbAccessor w1_dba{worker(1), master_dba.transaction_id()};
    VertexAccessor v1_in_w1{{v1_id, 0}, w1_dba};
    EXPECT_NE(v1_in_w1.GetOld(), nullptr);
    EXPECT_EQ(v1_in_w1.GetNew(), nullptr);
    EXPECT_EQ(v1_in_w1.PropsAt(w1_dba.Property("p1")).Value<int64_t>(), 42);
    EXPECT_TRUE(v1_in_w1.has_label(w1_dba.Label("label")));
  }

  {
    GraphDbAccessor w2_dba{worker(2), master_dba.transaction_id()};
    VertexAccessor v2_in_w2{{v2_id, 0}, w2_dba};
    EXPECT_NE(v2_in_w2.GetOld(), nullptr);
    EXPECT_EQ(v2_in_w2.GetNew(), nullptr);
    EXPECT_EQ(v2_in_w2.PropsAt(w2_dba.Property("p2")).Value<std::string>(),
              "value");
    EXPECT_FALSE(v2_in_w2.has_label(w2_dba.Label("label")));

    VertexAccessor v1_in_w2{{v1_id, 0}, w2_dba};
    EdgeAccessor e1_in_w2{{e1_id, 0}, w2_dba};
    EXPECT_EQ(e1_in_w2.from(), v1_in_w2);
    EXPECT_EQ(e1_in_w2.to(), v2_in_w2);
    EXPECT_EQ(e1_in_w2.EdgeType(), w2_dba.EdgeType("et"));
    EXPECT_EQ(e1_in_w2.PropsAt(w2_dba.Property("p3")).Value<bool>(), true);
  }
}

TEST_F(DistributedGraphDbTest, RemoteExpansion) {
  // Model (v1)-->(v2), where each vertex is on one worker.
  std::vector<Edges::VertexAddress> v_ga;
  {
    GraphDbAccessor dba{master()};
    v_ga.emplace_back(GraphDbAccessor(worker(1), dba.transaction_id())
                          .InsertVertex()
                          .GlobalAddress());
    v_ga.emplace_back(GraphDbAccessor(worker(2), dba.transaction_id())
                          .InsertVertex()
                          .GlobalAddress());
    dba.Commit();
  }
  {
    GraphDbAccessor dba{master()};
    auto edge_type = dba.EdgeType("et");
    auto prop = dba.Property("prop");
    GraphDbAccessor dba1{worker(1), dba.transaction_id()};
    auto edge_ga =
        dba1.InsertOnlyEdge(v_ga[0], v_ga[1], edge_type,
                            dba1.db().storage().EdgeGenerator().Next())
            .GlobalAddress();
    for (int i : {0, 1}) {
      GraphDbAccessor dba_w{worker(i + 1), dba.transaction_id()};
      auto v = dba_w.FindVertexChecked(v_ga[i].gid(), false);
      // Update the vertex to create a new record.
      v.PropsSet(prop, 42);
      auto &edges = i == 0 ? v.GetNew()->out_ : v.GetNew()->in_;
      edges.emplace(v_ga[(i + 1) % 2], edge_ga, edge_type);
    }
    dba.Commit();
  }
  {
    // Expand on the master for three hops. Collect vertex gids.
    GraphDbAccessor dba{master()};
    std::vector<VertexAccessor> visited;

    auto expand = [&visited, &dba](auto &v) {
      for (auto e : v.out()) return e.to();
      for (auto e : v.in()) return e.from();
      CHECK(false) << "No edge in vertex";
    };

    // Do a few hops back and forth, all on the master.
    VertexAccessor v{v_ga[0], dba};
    for (int i = 0; i < 5; ++i) {
      v = expand(v);
      EXPECT_FALSE(v.address().is_local());
      EXPECT_EQ(v.address(), v_ga[(i + 1) % 2]);
    }
  }
}
