#include <algorithm>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

#include "communication/rpc/server.hpp"
#include "distributed/cluster_discovery_master.hpp"
#include "distributed/coordination_master.hpp"
#include "io/network/endpoint.hpp"
#include "transactions/engine_master.hpp"
#include "transactions/engine_rpc_messages.hpp"
#include "transactions/engine_worker.hpp"

using namespace tx;
using namespace communication::rpc;
using namespace distributed;

class WorkerEngineTest : public testing::Test {
 protected:
  const std::string local{"127.0.0.1"};

  Server master_server_{{local, 0}};
  MasterCoordination master_coordination_{master_server_.endpoint()};
  RpcWorkerClients rpc_worker_clients_{master_coordination_};
  ClusterDiscoveryMaster cluster_disocvery_{
      master_server_, master_coordination_, rpc_worker_clients_};

  MasterEngine master_{master_server_, rpc_worker_clients_};
  ClientPool master_client_pool{master_server_.endpoint()};

  WorkerEngine worker_{master_client_pool};
};

TEST_F(WorkerEngineTest, BeginOnWorker) {
  worker_.Begin();
  auto second = worker_.Begin();
  EXPECT_EQ(master_.RunningTransaction(second->id_)->snapshot().size(), 1);
}

TEST_F(WorkerEngineTest, AdvanceOnWorker) {
  auto tx = worker_.Begin();
  auto cid = tx->cid();
  EXPECT_EQ(worker_.Advance(tx->id_), cid + 1);
}

TEST_F(WorkerEngineTest, CommitOnWorker) {
  auto tx = worker_.Begin();
  auto tx_id = tx->id_;
  worker_.Commit(*tx);
  EXPECT_TRUE(master_.Info(tx_id).is_committed());
}

TEST_F(WorkerEngineTest, AbortOnWorker) {
  auto tx = worker_.Begin();
  auto tx_id = tx->id_;
  worker_.Abort(*tx);
  EXPECT_TRUE(master_.Info(tx_id).is_aborted());
}

TEST_F(WorkerEngineTest, RunningTransaction) {
  master_.Begin();
  master_.Begin();
  worker_.RunningTransaction(1);
  worker_.RunningTransaction(2);
  int count = 0;
  worker_.LocalForEachActiveTransaction([&count](Transaction &t) {
    ++count;
    if (t.id_ == 1) {
      EXPECT_EQ(t.snapshot(),
                tx::Snapshot(std::vector<tx::TransactionId>{}));
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

TEST_F(WorkerEngineTest, LocalLast) {
  master_.Begin();
  EXPECT_EQ(worker_.LocalLast(), 0);
  worker_.RunningTransaction(1);
  EXPECT_EQ(worker_.LocalLast(), 1);
  master_.Begin();
  EXPECT_EQ(worker_.LocalLast(), 1);
  master_.Begin();
  EXPECT_EQ(worker_.LocalLast(), 1);
  master_.Begin();
  worker_.RunningTransaction(4);
  EXPECT_EQ(worker_.LocalLast(), 4);
}

TEST_F(WorkerEngineTest, LocalForEachActiveTransaction) {
  master_.Begin();
  worker_.RunningTransaction(1);
  master_.Begin();
  master_.Begin();
  master_.Begin();
  worker_.RunningTransaction(4);
  std::unordered_set<tx::TransactionId> local;
  worker_.LocalForEachActiveTransaction(
      [&local](Transaction &t) { local.insert(t.id_); });
  EXPECT_EQ(local, std::unordered_set<tx::TransactionId>({1, 4}));
}

TEST_F(WorkerEngineTest, EnsureTxIdGreater) {
  ASSERT_LE(master_.Begin()->id_, 40);
  worker_.EnsureNextIdGreater(42);
  EXPECT_EQ(master_.Begin()->id_, 43);
  EXPECT_EQ(worker_.Begin()->id_, 44);
}

TEST_F(WorkerEngineTest, GlobalNext) {
  auto tx = master_.Begin();
  EXPECT_NE(worker_.LocalLast(), worker_.GlobalLast());
  EXPECT_EQ(master_.LocalLast(), worker_.GlobalLast());
  EXPECT_EQ(worker_.GlobalLast(), tx->id_);
}
