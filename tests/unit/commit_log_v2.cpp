#include "storage/v2/commit_log.hpp"

#include "gtest/gtest.h"

namespace {
constexpr size_t ids_per_block = 8192 * 64;
}  // namespace

TEST(CommitLog, Simple) {
  storage::CommitLog log;
  EXPECT_EQ(log.OldestActive(), 0);

  log.MarkFinished(1);
  EXPECT_EQ(log.OldestActive(), 0);

  log.MarkFinished(0);
  EXPECT_EQ(log.OldestActive(), 2);
}

TEST(CommitLog, Fields) {
  storage::CommitLog log;

  for (uint64_t i = 0; i < 64; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), i + 1);
  }

  for (uint64_t i = 128; i < 192; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), 64);
  }

  for (uint64_t i = 64; i < 128; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), i < 127 ? i + 1 : 192);
  }
}

TEST(CommitLog, Blocks) {
  storage::CommitLog log;

  for (uint64_t i = 0; i < ids_per_block; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), i + 1);
  }

  for (uint64_t i = ids_per_block * 2; i < ids_per_block * 3; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), ids_per_block);
  }

  for (uint64_t i = ids_per_block; i < ids_per_block; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), i < ids_per_block - 1 ? i + 1 : ids_per_block * 3);
  }
}

TEST(CommitLog, TrackAfterInitialId) {
  const auto check_marking_ids = [](auto *log, auto current_oldest_active) {
    ASSERT_EQ(log->OldestActive(), current_oldest_active);
    log->MarkFinished(current_oldest_active);
    ++current_oldest_active;
    ASSERT_EQ(log->OldestActive(), current_oldest_active);
  };

  for (uint64_t i = 0; i < 2 * ids_per_block; ++i) {
    storage::CommitLog log{i};
    check_marking_ids(&log, i);
  }
}
