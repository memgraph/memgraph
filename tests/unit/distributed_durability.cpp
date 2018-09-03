#include "experimental/filesystem"

#include "distributed_common.hpp"

#include "database/graph_db_accessor.hpp"
#include "durability/paths.hpp"
#include "durability/snapshooter.hpp"
#include "durability/version.hpp"
#include "utils/string.hpp"

std::vector<fs::path> DirFiles(fs::path dir) {
  std::vector<fs::path> files;
  if (fs::exists(dir))
    for (auto &file : fs::directory_iterator(dir)) files.push_back(file.path());
  return files;
}

class DistributedDurability : public DistributedGraphDbTest {
 public:
  DistributedDurability() : DistributedGraphDbTest("distributed") {}
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
    DistributedGraphDbTest::ShutDown();
    Initialize([](database::Config config) {
      config.db_recover_on_startup = true;
      return config;
    });
  }

  void RestartWithWal(bool synchronous_commit) {
    DistributedGraphDbTest::ShutDown();
    Initialize([synchronous_commit](database::Config config) {
      config.durability_enabled = true;
      config.synchronous_commit = synchronous_commit;
      return config;
    });
  }

  void FlushAllWal() {
    master().wal().Flush();
    worker(1).wal().Flush();
    worker(2).wal().Flush();
  }

  void CheckDeltas(database::StateDelta::Type op) {
    for (int i = 0; i < kWorkerCount + 1; ++i) {
      auto wal_dir = GetDurabilityDirectory(i) / durability::kWalDir;
      auto wal_files = DirFiles(wal_dir);
      ASSERT_EQ(wal_files.size(), 1);

      auto wal_file = wal_files[0];
      HashedFileReader reader;
      ASSERT_TRUE(reader.Open(wal_file));
      communication::bolt::Decoder<HashedFileReader> decoder{reader};
      std::vector<database::StateDelta> deltas;

      // check magic number
      auto magic_number = durability::kWalMagic;
      reader.Read(magic_number.data(), magic_number.size());
      ASSERT_EQ(magic_number, durability::kWalMagic);

      // check version
      communication::bolt::Value dv;
      decoder.ReadValue(&dv);
      ASSERT_EQ(dv.ValueInt(), durability::kVersion);

      while (true) {
        auto delta = database::StateDelta::Decode(reader, decoder);
        if (delta) {
          deltas.emplace_back(*delta);
        } else {
          break;
        }
      }
      reader.Close();

      if (i == 0) {
        // The master always has TRANSACTION_BEGIN and `op`.
        ASSERT_EQ(deltas.size(), 2);
        EXPECT_EQ(deltas[1].type, op);
      }
      else {
        // The workers only have `op`.
        ASSERT_EQ(deltas.size(), 1);
        EXPECT_EQ(deltas[0].type, op);
      }
    }
  }

 private:
  void AddVertex(database::GraphDb &db, const std::string &label) {
    auto dba = db.Access();
    auto vertex = dba->InsertVertex();
    vertex.add_label(dba->Label(label));
    dba->Commit();
  }

  void CheckVertex(database::GraphDb &db, int expected_count,
                   const std::string &label) {
    auto dba = db.Access();
    auto it = dba->Vertices(false);
    std::vector<VertexAccessor> vertices{it.begin(), it.end()};
    EXPECT_EQ(vertices.size(), expected_count);
    for (auto &vertex : vertices) {
      ASSERT_EQ(vertex.labels().size(), 1);
      EXPECT_EQ(vertex.labels()[0], dba->Label(label));
    }
  }
};

TEST_F(DistributedDurability, MakeSnapshot) {
  // Create a graph with 3 nodes with 3 labels, one on each and make a snapshot
  // of it
  {
    AddVertices();
    auto dba = master().Access();
    master().MakeSnapshot(*dba);
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
    auto dba = worker(1).Access();
    worker(1).MakeSnapshot(*dba);
  }
  {
    RestartWithRecovery();
    CheckVertices(0);
    AddVertices();
    auto dba = master().Access();
    master().MakeSnapshot(*dba);
  }
  {
    RestartWithRecovery();
    CheckVertices(1);
    AddVertices();
    CheckVertices(2);
    // Make snapshot on one worker, expect it won't recover from that.
    auto dba = worker(1).Access();
    worker(1).MakeSnapshot(*dba);
  }
  {
    RestartWithRecovery();
    CheckVertices(1);
  }
}

/* TODO (msantl): FIXME
TEST_F(DistributedDurability, RecoveryFailure) {
  {
    AddVertices();
    // Make a snapshot on the master without the right snapshots on workers.
    auto dba = master().Access();
    bool status = durability::MakeSnapshot(master(), *dba, master().WorkerId(),
                                           tmp_dir_, 100);
    ASSERT_TRUE(status);
  }
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  EXPECT_DEATH(RestartWithRecovery(), "worker failed to recover");
}
*/

TEST_F(DistributedDurability, WalWrite) {
  {
    CleanDurability();
    RestartWithWal(false);
    auto dba = master().Access();
    dba->Commit();
    FlushAllWal();
    CheckDeltas(database::StateDelta::Type::TRANSACTION_COMMIT);
  }
  {
    CleanDurability();
    RestartWithWal(false);
    auto dba = master().Access();
    dba->Abort();
    FlushAllWal();
    CheckDeltas(database::StateDelta::Type::TRANSACTION_ABORT);
  }
}

TEST_F(DistributedDurability, WalSynchronizedWrite) {
  {
    CleanDurability();
    RestartWithWal(true);
    auto dba = master().Access();
    dba->Commit();
    CheckDeltas(database::StateDelta::Type::TRANSACTION_COMMIT);
  }
  {
    CleanDurability();
    RestartWithWal(true);
    auto dba = master().Access();
    dba->Abort();
    CheckDeltas(database::StateDelta::Type::TRANSACTION_ABORT);
  }
}
