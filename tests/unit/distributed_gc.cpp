#include <gtest/gtest.h>

#include "distributed_common.hpp"

TEST_F(DistributedGraphDbTest, GarbageCollect) {
  database::GraphDbAccessor dba{master()};
  auto tx = dba.transaction_id();
  dba.Commit();

  // Create multiple transactions so that the commit log can be cleared
  for (int i = 0; i < tx::CommitLog::kBitsetBlockSize; ++i) {
    database::GraphDbAccessor dba{master()};
  }

  master().CollectGarbage();
  worker(1).CollectGarbage();
  worker(2).CollectGarbage();
  EXPECT_EQ(master().tx_engine().Info(tx).is_committed(), true);

  database::GraphDbAccessor dba2{master()};
  auto tx_last = dba2.transaction_id();
  dba2.Commit();

  worker(1).CollectGarbage();
  worker(2).CollectGarbage();
  master().CollectGarbage();

  EXPECT_DEATH(master().tx_engine().Info(tx), "chunk is nullptr");
  EXPECT_DEATH(worker(1).tx_engine().Info(tx), "chunk is nullptr");
  EXPECT_DEATH(worker(2).tx_engine().Info(tx), "chunk is nullptr");
  EXPECT_EQ(master().tx_engine().Info(tx_last).is_committed(), true);
  EXPECT_EQ(worker(1).tx_engine().Info(tx_last).is_committed(), true);
  EXPECT_EQ(worker(2).tx_engine().Info(tx_last).is_committed(), true);
}

TEST_F(DistributedGraphDbTest, GarbageCollectBlocked) {
  database::GraphDbAccessor dba{master()};
  auto tx = dba.transaction_id();
  dba.Commit();

  // Block garbage collection because this is a still alive transaction on the
  // worker
  database::GraphDbAccessor dba3{worker(1)};

  // Create multiple transactions so that the commit log can be cleared
  for (int i = 0; i < tx::CommitLog::kBitsetBlockSize; ++i) {
    database::GraphDbAccessor dba{master()};
  }

  // Query for a large id so that the commit log new block is created
  master().tx_engine().Info(tx::CommitLog::kBitsetBlockSize);

  master().CollectGarbage();
  worker(1).CollectGarbage();
  worker(2).CollectGarbage();
  EXPECT_EQ(master().tx_engine().Info(tx).is_committed(), true);

  database::GraphDbAccessor dba2{master()};
  auto tx_last = dba2.transaction_id();
  dba2.Commit();

  worker(1).CollectGarbage();
  worker(2).CollectGarbage();
  master().CollectGarbage();

  EXPECT_EQ(master().tx_engine().Info(tx).is_committed(), true);
  EXPECT_EQ(worker(1).tx_engine().Info(tx).is_committed(), true);
  EXPECT_EQ(worker(2).tx_engine().Info(tx).is_committed(), true);
  EXPECT_EQ(master().tx_engine().Info(tx_last).is_committed(), true);
  EXPECT_EQ(worker(1).tx_engine().Info(tx_last).is_committed(), true);
  EXPECT_EQ(worker(2).tx_engine().Info(tx_last).is_committed(), true);
}

int main(int argc, char **argv) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
