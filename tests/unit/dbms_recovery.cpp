#include <experimental/filesystem>

#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include "database/dbms.hpp"

DECLARE_bool(recovery_on_startup);
DECLARE_string(snapshot_directory);
DECLARE_int32(snapshot_cycle_sec);

namespace fs = std::experimental::filesystem;

char tmp[] = "XXXXXX";
const fs::path SNAPSHOTS_DBMS_RECOVERY_ALL_DB = mkdtemp(tmp);
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
  virtual void TearDown() { CleanDbDir(); }

  virtual void SetUp() {
    CleanDbDir();
    FLAGS_snapshot_directory = SNAPSHOTS_DBMS_RECOVERY_ALL_DB;
    FLAGS_snapshot_cycle_sec = -1;
  }
};

void CreateSnapshot() {
  FLAGS_snapshot_recover_on_startup = false;
  Dbms dbms;
  auto dba = dbms.active();

  // setup (v1) - [:likes] -> (v2) <- [:hates] - (v3)
  auto va1 = dba->InsertVertex();
  auto va2 = dba->InsertVertex();
  dba->InsertEdge(va1, va2, dba->EdgeType("likes"));
  auto va3 = dba->InsertVertex();
  dba->InsertEdge(va3, va2, dba->EdgeType("hates"));
  dba->AdvanceCommand();

  Snapshooter snapshooter;
  EXPECT_EQ(snapshooter.MakeSnapshot(*dba.get(),
                                     SNAPSHOTS_DBMS_RECOVERY_DEFAULT_DB_DIR, 1),
            true);
}

void RecoverDbms() {
  FLAGS_snapshot_recover_on_startup = true;
  Dbms dbms;
  auto dba = dbms.active();

  std::vector<VertexAccessor> vertices;
  std::vector<EdgeAccessor> edges;

  int vertex_count = 0;
  for (auto const &vertex : dba->Vertices(false)) {
    vertices.push_back(vertex);
    vertex_count++;
  }
  EXPECT_EQ(vertex_count, 3);

  int edge_count = 0;
  for (auto const &edge : dba->Edges(false)) {
    EXPECT_NE(vertices.end(),
              std::find(vertices.begin(), vertices.end(), edge.to()));
    EXPECT_NE(vertices.end(),
              std::find(vertices.begin(), vertices.end(), edge.from()));
    edges.push_back(edge);
    edge_count++;
  }
  ASSERT_EQ(edge_count, 2);
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
