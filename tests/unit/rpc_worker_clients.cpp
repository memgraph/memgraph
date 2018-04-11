#include "boost/archive/binary_iarchive.hpp"
#include "boost/archive/binary_oarchive.hpp"
#include "boost/serialization/export.hpp"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/rpc/messages.hpp"
#include "communication/rpc/server.hpp"
#include "distributed/cluster_discovery_master.hpp"
#include "distributed/cluster_discovery_worker.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "distributed/rpc_worker_clients.hpp"
#include "distributed/serialization.hpp"
#include "io/network/endpoint.hpp"

namespace distributed {

RPC_NO_MEMBER_MESSAGE(IncrementCounterReq);
RPC_NO_MEMBER_MESSAGE(IncrementCounterRes);

using IncrementCounterRpc =
    communication::rpc::RequestResponse<IncrementCounterReq,
                                        IncrementCounterRes>;
};  // namespace distributed

BOOST_CLASS_EXPORT(distributed::IncrementCounterReq);
BOOST_CLASS_EXPORT(distributed::IncrementCounterRes);

class RpcWorkerClientsTest : public ::testing::Test {
 protected:
  const io::network::Endpoint kLocalHost{"127.0.0.1", 0};
  const int kWorkerCount = 2;
  void SetUp() override {
    for (int i = 1; i <= kWorkerCount; ++i) {
      workers_server_.emplace_back(
          std::make_unique<communication::rpc::Server>(kLocalHost));

      workers_coord_.emplace_back(
          std::make_unique<distributed::WorkerCoordination>(
              *workers_server_.back(), master_server_.endpoint()));

      cluster_discovery_.emplace_back(
          std::make_unique<distributed::ClusterDiscoveryWorker>(
              *workers_server_.back(), *workers_coord_.back(),
              rpc_workers_.GetClientPool(0)));

      cluster_discovery_.back()->RegisterWorker(i);

      workers_server_.back()->Register<distributed::IncrementCounterRpc>(
          [this, i](const distributed::IncrementCounterReq &) {
            workers_cnt_[i]++;
            return std::make_unique<distributed::IncrementCounterRes>();
          });
    }
  }

  void TearDown() override {
    std::vector<std::thread> wait_on_shutdown;
    for (int i = 0; i < workers_coord_.size(); ++i) {
      wait_on_shutdown.emplace_back([i, this]() {
        workers_coord_[i]->WaitForShutdown();
        workers_server_[i] = nullptr;
      });
    }

    std::this_thread::sleep_for(300ms);

    // Starts server shutdown and notifies the workers
    master_coord_ = std::experimental::nullopt;
    for (auto &worker : wait_on_shutdown) worker.join();
  }

  std::vector<std::unique_ptr<communication::rpc::Server>> workers_server_;
  std::vector<std::unique_ptr<distributed::WorkerCoordination>> workers_coord_;
  std::vector<std::unique_ptr<distributed::ClusterDiscoveryWorker>>
      cluster_discovery_;
  std::unordered_map<int, int> workers_cnt_;

  communication::rpc::Server master_server_{kLocalHost};
  std::experimental::optional<distributed::MasterCoordination> master_coord_{
      master_server_.endpoint()};

  distributed::RpcWorkerClients rpc_workers_{*master_coord_};
  distributed::ClusterDiscoveryMaster cluster_disocvery_{
      master_server_, *master_coord_, rpc_workers_};
};

TEST_F(RpcWorkerClientsTest, GetWorkerIds) {
  EXPECT_THAT(rpc_workers_.GetWorkerIds(), testing::UnorderedElementsAreArray(
                                               master_coord_->GetWorkerIds()));
}

TEST_F(RpcWorkerClientsTest, GetClientPool) {
  auto &pool1 = rpc_workers_.GetClientPool(1);
  auto &pool2 = rpc_workers_.GetClientPool(2);
  EXPECT_NE(&pool1, &pool2);
  EXPECT_EQ(&pool1, &rpc_workers_.GetClientPool(1));
}

TEST_F(RpcWorkerClientsTest, ExecuteOnWorker) {
  auto execute = [](auto &client) -> void {
    ASSERT_TRUE(client.template Call<distributed::IncrementCounterRpc>());
  };

  rpc_workers_.ExecuteOnWorker<void>(1, execute).get();
  EXPECT_EQ(workers_cnt_[0], 0);
  EXPECT_EQ(workers_cnt_[1], 1);
  EXPECT_EQ(workers_cnt_[2], 0);
}

TEST_F(RpcWorkerClientsTest, ExecuteOnWorkers) {
  auto execute = [](auto &client) -> void {
    ASSERT_TRUE(client.template Call<distributed::IncrementCounterRpc>());
  };

  // Skip master
  for (auto &future : rpc_workers_.ExecuteOnWorkers<void>(0, execute))
    future.get();

  EXPECT_EQ(workers_cnt_[0], 0);
  EXPECT_EQ(workers_cnt_[1], 1);
  EXPECT_EQ(workers_cnt_[2], 1);
}
