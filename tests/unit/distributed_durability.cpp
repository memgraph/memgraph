#include "distributed_common.hpp"

#include "database/graph_db_accessor.hpp"
#include "durability/snapshooter.hpp"

class DistributedDurability : public DistributedGraphDbTest {
 public:
  void AddVertices() {
    AddVertex(master(), "master");
    AddVertex(worker(1), "worker1");
    AddVertex(worker(2), "worker2");
  }
  void CheckVertices(int expected_count) {
    CheckVertex(master(), expected_count, "master");
    CheckVertex(worker(1), expected_count, "worker1");
    CheckVertex(worker(2), expected_count, "worker2");
  }
  void RestartWithRecovery() {
    ShutDown();
    Initialize([](database::Config config) {
      config.db_recover_on_startup = true;
      return config;
    });
  }

 private:
  void AddVertex(database::GraphDb &db, const std::string &label) {
    database::GraphDbAccessor dba(db);
    auto vertex = dba.InsertVertex();
    vertex.add_label(dba.Label(label));
    dba.Commit();
  }

  void CheckVertex(database::GraphDb &db, int expected_count,
                   const std::string &label) {
    database::GraphDbAccessor dba(db);
    auto it = dba.Vertices(false);
    std::vector<VertexAccessor> vertices{it.begin(), it.end()};
    EXPECT_EQ(vertices.size(), expected_count);
    for (auto &vertex : vertices) {
      ASSERT_EQ(vertex.labels().size(), 1);
      EXPECT_EQ(vertex.labels()[0], dba.Label(label));
    }
  }
};

TEST_F(DistributedDurability, MakeSnapshot) {
  // Create a graph with 3 nodes with 3 labels, one on each and make a snapshot
  // of it
  {
    AddVertices();
    database::GraphDbAccessor dba(master());
    master().MakeSnapshot(dba);
  }
  // Recover the graph and check if it's the same as before
  {
    RestartWithRecovery();
    CheckVertices(1);
  }
}

TEST_F(DistributedDurability, SnapshotOnExit) {
  {
    TearDown();
    Initialize([](database::Config config) {
      config.snapshot_on_exit = true;
      return config;
    });
    AddVertices();
  }
  // Recover the graph and check if it's the same as before
  {
    RestartWithRecovery();
    CheckVertices(1);
  }
}

TEST_F(DistributedDurability, RecoveryFromSameSnapshot) {
  {
    AddVertices();
    // Make snapshot on one worker, expect it won't recover from that.
    database::GraphDbAccessor dba(worker(1));
    worker(1).MakeSnapshot(dba);
  }
  {
    RestartWithRecovery();
    CheckVertices(0);
    AddVertices();
    database::GraphDbAccessor dba(master());
    master().MakeSnapshot(dba);
  }
  {
    RestartWithRecovery();
    CheckVertices(1);
    AddVertices();
    CheckVertices(2);
    // Make snapshot on one worker, expect it won't recover from that.
    database::GraphDbAccessor dba(worker(1));
    worker(1).MakeSnapshot(dba);
  }
  {
    RestartWithRecovery();
    CheckVertices(1);
  }
}

TEST_F(DistributedDurability, RecoveryFailure) {
  {
    AddVertices();
    // Make a snapshot on the master without the right snapshots on workers.
    database::GraphDbAccessor dba(master());
    bool status = durability::MakeSnapshot(master(), dba, tmp_dir_, 100);
    ASSERT_TRUE(status);
  }
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(RestartWithRecovery(), "worker failed to recover");
}
