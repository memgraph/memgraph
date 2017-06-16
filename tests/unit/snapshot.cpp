#include <experimental/filesystem>

#include "gflags/gflags.h"
#include "gtest/gtest.h"

#include "dbms/dbms.hpp"
#include "durability/snapshooter.hpp"

DECLARE_bool(snapshot_on_db_exit);
DECLARE_int32(snapshot_cycle_sec);
DECLARE_string(snapshot_directory);

namespace fs = std::experimental::filesystem;

char tmp[] = "XXXXXX";
const fs::path SNAPSHOTS_FOLDER_ALL_DB = mkdtemp(tmp);
const fs::path SNAPSHOTS_TEST_DEFAULT_DB_DIR =
    SNAPSHOTS_FOLDER_ALL_DB / "default";

// Other functionality is tested in recovery tests.

std::vector<fs::path> GetFilesFromDir(
    const fs::path &snapshots_default_db_dir) {
  std::vector<fs::path> files;
  for (auto &file : fs::directory_iterator(snapshots_default_db_dir))
    files.push_back(file.path());
  return files;
}

void CleanDbDir() {
  if (!fs::exists(SNAPSHOTS_TEST_DEFAULT_DB_DIR)) return;
  std::vector<fs::path> files = GetFilesFromDir(SNAPSHOTS_TEST_DEFAULT_DB_DIR);
  for (auto file : files) {
    fs::remove(file);
  }
}

class SnapshotTest : public ::testing::Test {
 protected:
  virtual void TearDown() { CleanDbDir(); }

  virtual void SetUp() {
    CleanDbDir();
    FLAGS_snapshot_cycle_sec = -1;
  }
  std::string snapshot_cycle_sec_setup_;
};

TEST_F(SnapshotTest, CreateLessThanMaxRetainedSnapshotsTests) {
  const int max_retained_snapshots = 10;
  Dbms dbms;

  for (int i = 0; i < 3; ++i) {
    auto dba = dbms.active();
    Snapshooter snapshooter;
    snapshooter.MakeSnapshot(*dba.get(), SNAPSHOTS_TEST_DEFAULT_DB_DIR,
                             max_retained_snapshots);
  }

  std::vector<fs::path> files = GetFilesFromDir(SNAPSHOTS_TEST_DEFAULT_DB_DIR);
  EXPECT_EQ(files.size(), 3);
}

TEST_F(SnapshotTest, CreateMoreThanMaxRetainedSnapshotsTests) {
  const int max_retained_snapshots = 2;
  Dbms dbms;

  fs::path first_snapshot;
  for (int i = 0; i < 3; ++i) {
    auto dba = dbms.active();
    Snapshooter snapshooter;
    snapshooter.MakeSnapshot(*dba.get(), SNAPSHOTS_TEST_DEFAULT_DB_DIR,
                             max_retained_snapshots);
    if (i == 0) {
      std::vector<fs::path> files_begin =
          GetFilesFromDir(SNAPSHOTS_TEST_DEFAULT_DB_DIR);
      EXPECT_EQ(files_begin.size(), 1);
      first_snapshot = files_begin[0];
    }
  }

  std::vector<fs::path> files_end =
      GetFilesFromDir(SNAPSHOTS_TEST_DEFAULT_DB_DIR);
  EXPECT_EQ(files_end.size(), 2);
  EXPECT_EQ(fs::exists(first_snapshot), false);
}

TEST_F(SnapshotTest, CreateSnapshotWithUnlimitedMaxRetainedSnapshots) {
  const int max_retained_snapshots = -1;
  Dbms dbms;

  for (int i = 0; i < 10; ++i) {
    auto dba = dbms.active();
    Snapshooter snapshooter;
    snapshooter.MakeSnapshot(*dba.get(), SNAPSHOTS_TEST_DEFAULT_DB_DIR,
                             max_retained_snapshots);
  }

  std::vector<fs::path> files = GetFilesFromDir(SNAPSHOTS_TEST_DEFAULT_DB_DIR);
  EXPECT_EQ(files.size(), 10);
}

TEST_F(SnapshotTest, TestSnapshotFileOnDbDestruct) {
  {
    FLAGS_snapshot_directory = SNAPSHOTS_FOLDER_ALL_DB;
    FLAGS_snapshot_on_db_exit = true;
    Dbms dbms;
    auto dba = dbms.active();
  }
  std::vector<fs::path> files = GetFilesFromDir(SNAPSHOTS_TEST_DEFAULT_DB_DIR);
  // snapshot is created on dbms destruction
  EXPECT_EQ(files.size(), 1);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
