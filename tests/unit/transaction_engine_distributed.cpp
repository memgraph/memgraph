#include <unordered_set>

#include "gtest/gtest.h"

#include "communication/messaging/distributed.hpp"
#include "io/network/network_endpoint.hpp"
#include "transactions/engine_master.hpp"
#include "transactions/engine_rpc_messages.hpp"
#include "transactions/engine_worker.hpp"

using namespace tx;
using namespace communication::messaging;

class WorkerEngineTest : public testing::Test {
 protected:
  const std::string local{"127.0.0.1"};

  System master_system_{local, 0};
  MasterEngine master_{master_system_};

  System worker_system_{local, 0};
  WorkerEngine worker_{worker_system_, master_system_.endpoint()};
};

TEST_F(WorkerEngineTest, LocalBegin) {
  master_.Begin();
  master_.Begin();
  worker_.LocalBegin(1);
  worker_.LocalBegin(2);
  int count = 0;
  worker_.LocalForEachActiveTransaction([&count](Transaction &t) {
    ++count;
    if (t.id_ == 1) {
      EXPECT_EQ(t.snapshot(),
                tx::Snapshot(std::vector<tx::transaction_id_t>{}));
    } else {
      EXPECT_EQ(t.snapshot(), tx::Snapshot({1}));
    }
  });
  EXPECT_EQ(count, 2);
}

TEST_F(WorkerEngineTest, Info) {
  auto *tx_1 = master_.Begin();
  auto *tx_2 = master_.Begin();
  // We can't check active transactions in the worker (see comments there for
  // info).
  master_.Commit(*tx_1);
  EXPECT_TRUE(master_.Info(1).is_committed());
  EXPECT_TRUE(worker_.Info(1).is_committed());
  master_.Abort(*tx_2);
  EXPECT_TRUE(master_.Info(2).is_aborted());
  EXPECT_TRUE(worker_.Info(2).is_aborted());
}

TEST_F(WorkerEngineTest, GlobalGcSnapshot) {
  auto *tx_1 = master_.Begin();
  master_.Begin();
  master_.Commit(*tx_1);
  EXPECT_EQ(master_.GlobalGcSnapshot(), tx::Snapshot({1, 2}));
  EXPECT_EQ(worker_.GlobalGcSnapshot(), master_.GlobalGcSnapshot());
}

TEST_F(WorkerEngineTest, GlobalActiveTransactions) {
  auto *tx_1 = master_.Begin();
  master_.Begin();
  auto *tx_3 = master_.Begin();
  master_.Begin();
  master_.Commit(*tx_1);
  master_.Abort(*tx_3);
  EXPECT_EQ(worker_.GlobalActiveTransactions(), tx::Snapshot({2, 4}));
}

TEST_F(WorkerEngineTest, GlobalIsActive) {
  auto *tx_1 = master_.Begin();
  master_.Begin();
  auto *tx_3 = master_.Begin();
  master_.Begin();
  master_.Commit(*tx_1);
  master_.Abort(*tx_3);
  EXPECT_FALSE(worker_.GlobalIsActive(1));
  EXPECT_TRUE(worker_.GlobalIsActive(2));
  EXPECT_FALSE(worker_.GlobalIsActive(3));
  EXPECT_TRUE(worker_.GlobalIsActive(4));
}

TEST_F(WorkerEngineTest, LocalLast) {
  master_.Begin();
  EXPECT_EQ(worker_.LocalLast(), 0);
  worker_.LocalBegin(1);
  EXPECT_EQ(worker_.LocalLast(), 1);
  master_.Begin();
  EXPECT_EQ(worker_.LocalLast(), 1);
  master_.Begin();
  EXPECT_EQ(worker_.LocalLast(), 1);
  master_.Begin();
  worker_.LocalBegin(4);
  EXPECT_EQ(worker_.LocalLast(), 4);
}

TEST_F(WorkerEngineTest, LocalForEachActiveTransaction) {
  master_.Begin();
  worker_.LocalBegin(1);
  master_.Begin();
  master_.Begin();
  master_.Begin();
  worker_.LocalBegin(4);
  std::unordered_set<tx::transaction_id_t> local;
  worker_.LocalForEachActiveTransaction(
      [&local](Transaction &t) { local.insert(t.id_); });
  EXPECT_EQ(local, std::unordered_set<tx::transaction_id_t>({1, 4}));
}
