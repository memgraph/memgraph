#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "raft/replication_log.hpp"

using namespace tx;

TEST(ReplicationLog, ActiveReplicated) {
  raft::ReplicationLog rlog;
  const tx::TransactionId tx_id = 10;

  EXPECT_FALSE(rlog.is_replicated(tx_id));
  EXPECT_FALSE(rlog.is_active(tx_id));

  rlog.set_active(tx_id);

  EXPECT_FALSE(rlog.is_replicated(tx_id));
  EXPECT_TRUE(rlog.is_active(tx_id));

  rlog.set_replicated(tx_id);

  EXPECT_TRUE(rlog.is_replicated(tx_id));
  EXPECT_FALSE(rlog.is_active(tx_id));
}

TEST(ReplicationLog, GarbageCollect) {
  raft::ReplicationLog rlog;

  auto set_active = [&rlog](tx::TransactionId tx_id) {
    rlog.set_active(tx_id);
    EXPECT_TRUE(rlog.is_active(tx_id));
  };

  auto set_replicated = [&rlog](tx::TransactionId tx_id) {
    rlog.set_replicated(tx_id);
    EXPECT_TRUE(rlog.is_replicated(tx_id));
    EXPECT_FALSE(rlog.is_active(tx_id));
  };

  const int n = raft::ReplicationLog::kBitsetBlockSize;

  for (int i = 1; i < 3 * n; ++i) {
    set_active(i);
  }

  for (int i = 1; i < 2 * n; ++i) {
    set_replicated(i);
  }

  rlog.garbage_collect_older(n);

  for (int i = 1; i < n; ++i) {
    EXPECT_FALSE(rlog.is_active(i));
    EXPECT_FALSE(rlog.is_replicated(i));
  }

  for (int i = n; i < 2 * n; ++i) {
    EXPECT_FALSE(rlog.is_active(i));
    EXPECT_TRUE(rlog.is_replicated(i));
  }

  for (int i = 2 * n; i < 3 * n; ++i) {
    EXPECT_TRUE(rlog.is_active(i));
    EXPECT_FALSE(rlog.is_replicated(i));
  }
}
