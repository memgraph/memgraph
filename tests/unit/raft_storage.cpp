#include <experimental/optional>

#include "gtest/gtest.h"

#include "communication/raft/storage/file.hpp"
#include "communication/raft/test_utils.hpp"

using communication::raft::LogEntry;
using communication::raft::SimpleFileStorage;
using communication::raft::test_utils::IntState;

TEST(SimpleFileStorageTest, All) {
  typedef LogEntry<IntState> Log;
  auto GetLog = [](int term, int d) {
    return Log{term, IntState::Change{IntState::Change::Type::SET, d}};
  };

  {
    SimpleFileStorage<IntState> storage(fs::path("raft_storage_test_dir"));
    EXPECT_EQ(storage.GetTermAndVotedFor().first, 0);
    EXPECT_EQ(storage.GetTermAndVotedFor().second, std::experimental::nullopt);
    EXPECT_EQ(storage.GetLastLogIndex(), 0);

    storage.WriteTermAndVotedFor(1, "a");
    EXPECT_EQ(storage.GetTermAndVotedFor().first, 1);
    EXPECT_EQ(*storage.GetTermAndVotedFor().second, "a");

    storage.AppendLogEntry(GetLog(1, 1));
    storage.AppendLogEntry(GetLog(1, 2));

    EXPECT_EQ(storage.GetLastLogIndex(), 2);

    EXPECT_EQ(storage.GetLogSuffix(1),
              std::vector<Log>({GetLog(1, 1), GetLog(1, 2)}));
  }

  {
    SimpleFileStorage<IntState> storage(fs::path("raft_storage_test_dir"));

    EXPECT_EQ(storage.GetTermAndVotedFor().first, 1);
    EXPECT_EQ(*storage.GetTermAndVotedFor().second, "a");
    EXPECT_EQ(storage.GetLastLogIndex(), 2);
    EXPECT_EQ(storage.GetLogSuffix(1),
              std::vector<Log>({GetLog(1, 1), GetLog(1, 2)}));

    storage.TruncateLogSuffix(2);
    EXPECT_EQ(storage.GetLogSuffix(1), std::vector<Log>({GetLog(1, 1)}));

    storage.WriteTermAndVotedFor(2, std::experimental::nullopt);
    storage.AppendLogEntry(GetLog(2, 3));

    EXPECT_EQ(storage.GetTermAndVotedFor().first, 2);
    EXPECT_EQ(storage.GetTermAndVotedFor().second, std::experimental::nullopt);
    EXPECT_EQ(storage.GetLogSuffix(1),
              std::vector<Log>({GetLog(1, 1), GetLog(2, 3)}));
  }

  {
    SimpleFileStorage<IntState> storage(fs::path("raft_storage_test_dir"));

    EXPECT_EQ(storage.GetTermAndVotedFor().first, 2);
    EXPECT_EQ(storage.GetTermAndVotedFor().second, std::experimental::nullopt);
    EXPECT_EQ(storage.GetLogSuffix(1),
              std::vector<Log>({GetLog(1, 1), GetLog(2, 3)}));
  }

  fs::remove("raft_storage_test_dir/metadata");
  fs::remove("raft_storage_test_dir/1");
  fs::remove("raft_storage_test_dir/2");
  fs::remove("raft_storage_test_dir");
}
