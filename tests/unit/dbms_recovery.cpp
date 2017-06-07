#include <experimental/filesystem>
#include "dbms/dbms.hpp"
#include "gtest/gtest.h"

namespace fs = std::experimental::filesystem;

const fs::path SNAPSHOTS_DBMS_RECOVERY_ALL_DB = std::tmpnam(nullptr);
const fs::path SNAPSHOTS_DBMS_RECOVERY_DEFAULT_DB_DIR =
    SNAPSHOTS_DBMS_RECOVERY_ALL_DB / "default";

std::vector<fs::path> GetFilesFromDir(
    const std::string &snapshots_default_db_dir) {
  std::vector<fs::path> files;
  for (auto &file : fs::directory_iterator(snapshots_default_db_dir))
    files.push_back(file.path());
  return files;
}

void CleanDbDir() {
  if (!fs::exists(SNAPSHOTS_DBMS_RECOVERY_DEFAULT_DB_DIR)) return;
  std::vector<fs::path> files =
      GetFilesFromDir(SNAPSHOTS_DBMS_RECOVERY_DEFAULT_DB_DIR);
  for (auto file : files) fs::remove(file);
}

class DbmsRecoveryTest : public ::testing::Test {
 protected:
  virtual void TearDown() {
    CleanDbDir();
    CONFIG(config::SNAPSHOTS_PATH) = snapshots_path_setup_;
    CONFIG(config::SNAPSHOT_CYCLE_SEC) = snapshot_cycle_sec_setup_;
  }

  virtual void SetUp() {
    CleanDbDir();
    snapshots_path_setup_ = CONFIG(config::SNAPSHOTS_PATH);
    snapshot_cycle_sec_setup_ = CONFIG(config::SNAPSHOT_CYCLE_SEC);
    CONFIG(config::SNAPSHOTS_PATH) = SNAPSHOTS_DBMS_RECOVERY_ALL_DB;
    CONFIG(config::SNAPSHOT_CYCLE_SEC) = "-1";
  }
  std::string snapshots_path_setup_;
  std::string snapshot_cycle_sec_setup_;
};

void CreateSnapshot() {
  CONFIG(config::RECOVERY) = "false";
  Dbms dbms;
  auto dba = dbms.active();

  // setup (v1) - [:likes] -> (v2) <- [:hates] - (v3)
  auto va1 = dba->insert_vertex();
  auto va2 = dba->insert_vertex();
  dba->insert_edge(va1, va2, dba->edge_type("likes"));
  auto va3 = dba->insert_vertex();
  dba->insert_edge(va3, va2, dba->edge_type("hates"));
  dba->advance_command();

  Snapshooter snapshooter;
  snapshooter.MakeSnapshot(*dba.get(), SNAPSHOTS_DBMS_RECOVERY_DEFAULT_DB_DIR,
                           1);
}

void RecoverDbms() {
  CONFIG(config::RECOVERY) = "true";
  Dbms dbms;
  auto dba = dbms.active();

  std::vector<VertexAccessor> vertices;
  std::vector<EdgeAccessor> edges;

  int vertex_count = 0;
  for (auto const &vertex : dba->vertices(false)) {
    vertices.push_back(vertex);
    vertex_count++;
  }
  EXPECT_EQ(vertex_count, 3);

  int edge_count = 0;
  for (auto const &edge : dba->edges(false)) {
    EXPECT_NE(vertices.end(),
              std::find(vertices.begin(), vertices.end(), edge.to()));
    EXPECT_NE(vertices.end(),
              std::find(vertices.begin(), vertices.end(), edge.from()));
    edges.push_back(edge);
    edge_count++;
  }
  EXPECT_EQ(edge_count, 2);
  EXPECT_EQ(edges[0].to() == edges[1].to(), true);
  EXPECT_EQ(edges[0].from() == edges[1].from(), false);
}

TEST_F(DbmsRecoveryTest, TestDbmsRecovery) {
  CreateSnapshot();
  RecoverDbms();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
