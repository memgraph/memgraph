#include "distributed_common.hpp"

#include "database/graph_db_accessor.hpp"

class DistributedDurability : public DistributedGraphDbTest {
 public:
  void write_labels() {
    add_label(master(), "master");
    add_label(worker(1), "worker1");
    add_label(worker(2), "worker2");
  }
  void check_labels() {
    check_label(master(), "master");
    check_label(worker(1), "worker1");
    check_label(worker(2), "worker2");
  }

 private:
  void add_label(database::GraphDb &db, const std::string &label) {
    database::GraphDbAccessor dba(db);
    auto vertex = dba.InsertVertex();
    vertex.add_label(dba.Label(label));
    dba.Commit();
  }

  void check_label(database::GraphDb &db, const std::string &label) {
    database::GraphDbAccessor dba(db);
    auto it = dba.Vertices(false);
    ASSERT_NE(it.begin(), it.end());
    auto vertex = *it.begin();
    ASSERT_EQ(vertex.labels().size(), 1);
    EXPECT_EQ(vertex.labels()[0], dba.Label(label));
  }
};

TEST_F(DistributedDurability, MakeSnapshot) {
  // Create a graph with 3 nodes with 3 labels, one on each and make a snapshot
  // of it
  {
    write_labels();
    database::GraphDbAccessor dba(master());
    master().MakeSnapshot(dba);
  }
  // Recover the graph and check if it's the same as before
  {
    ShutDown();
    Initialize([](database::Config config) {
      config.db_recover_on_startup = true;
      return config;
    });
    check_labels();
  }
}

TEST_F(DistributedDurability, SnapshotOnExit) {
  {
    TearDown();
    Initialize([](database::Config config) {
      config.snapshot_on_exit = true;
      return config;
    });
    write_labels();
  }
  // Recover the graph and check if it's the same as before
  {
    // This should force the db to make a snapshot
    ShutDown();

    Initialize([](database::Config config) {
      config.db_recover_on_startup = true;
      return config;
    });
    check_labels();
  }
}
