#include "storage/v2/commit_log.hpp"

#include "gtest/gtest.h"

TEST(CommitLog, Simple) {
  CommitLog log;
  EXPECT_EQ(log.OldestActive(), 0);

  log.MarkFinished(1);
  EXPECT_EQ(log.OldestActive(), 0);

  log.MarkFinished(0);
  EXPECT_EQ(log.OldestActive(), 2);
}

TEST(CommitLog, Fields) {
  CommitLog log;

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
  CommitLog log;

  for (uint64_t i = 0; i < 8192 * 64; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), i + 1);
  }

  for (uint64_t i = 8192 * 64 * 2; i < 8192 * 64 * 3; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), 8192 * 64);
  }

  for (uint64_t i = 8192 * 64; i < 8192 * 64; ++i) {
    log.MarkFinished(i);
    EXPECT_EQ(log.OldestActive(), i < 8192 * 64 - 1 ? i + 1 : 8192 * 64 * 3);
  }
}
