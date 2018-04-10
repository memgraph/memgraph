#include <atomic>
#include <experimental/optional>
#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "communication/rpc/client_pool.hpp"
#include "communication/rpc/server.hpp"
#include "distributed/cluster_discovery_master.hpp"
#include "distributed/cluster_discovery_worker.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "distributed/rpc_worker_clients.hpp"
#include "io/network/endpoint.hpp"

using communication::rpc::Server;
using communication::rpc::ClientPool;
using namespace distributed;
using namespace std::literals::chrono_literals;

const int kWorkerCount = 5;
const std::string kLocal = "127.0.0.1";

class WorkerCoordinationInThread {
 public:
  WorkerCoordinationInThread(io::network::Endpoint master_endpoint,
                             int desired_id) {
    std::atomic<bool> init_done{false};
    worker_thread_ =
        std::thread([this, master_endpoint, desired_id, &init_done] {
          server_.emplace(Endpoint(kLocal, 0));
          coord_.emplace(*server_, master_endpoint);
          client_pool_.emplace(master_endpoint);
          discovery_.emplace(*server_, *coord_, *client_pool_);
          // Try and register the worker with the desired id. If another worker
          // is already using the desired id it will exit here.
          discovery_->RegisterWorker(desired_id);
          worker_id_ = desired_id;
          init_done = true;
          coord_->WaitForShutdown();
        });

    while (!init_done) std::this_thread::sleep_for(10ms);
  }

  int worker_id() const { return worker_id_; }
  auto endpoint() const { return server_->endpoint(); }
  auto worker_endpoint(int worker_id) { return coord_->GetEndpoint(worker_id); }
  auto worker_ids() { return coord_->GetWorkerIds(); }
  void join() { worker_thread_.join(); }

 private:
  std::thread worker_thread_;
  std::experimental::optional<Server> server_;
  std::experimental::optional<WorkerCoordination> coord_;
  std::experimental::optional<ClientPool> client_pool_;
  std::experimental::optional<ClusterDiscoveryWorker> discovery_;
  std::atomic<int> worker_id_{0};
};

TEST(Distributed, Coordination) {
  Server master_server({kLocal, 0});
  std::vector<std::unique_ptr<WorkerCoordinationInThread>> workers;
  {
    MasterCoordination master_coord(master_server.endpoint());
    RpcWorkerClients rpc_worker_clients(master_coord);
    ClusterDiscoveryMaster master_discovery_(master_server, master_coord,
                                             rpc_worker_clients);

    for (int i = 1; i <= kWorkerCount; ++i)
      workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
          master_server.endpoint(), i));

    // Expect that all workers have a different ID.
    std::unordered_set<int> worker_ids;
    for (const auto &w : workers) worker_ids.insert(w->worker_id());
    ASSERT_EQ(worker_ids.size(), kWorkerCount);

    // Check endpoints.
    for (auto &w1 : workers) {
      for (auto &w2 : workers) {
        EXPECT_EQ(w1->worker_endpoint(w2->worker_id()), w2->endpoint());
      }
    }
  }  // Coordinated shutdown.

  for (auto &worker : workers) worker->join();
}

TEST(Distributed, DesiredAndUniqueId) {
  Server master_server({kLocal, 0});
  std::vector<std::unique_ptr<WorkerCoordinationInThread>> workers;
  {
    MasterCoordination master_coord(master_server.endpoint());
    RpcWorkerClients rpc_worker_clients(master_coord);
    ClusterDiscoveryMaster master_discovery_(master_server, master_coord,
                                             rpc_worker_clients);

    workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
        master_server.endpoint(), 42));
    EXPECT_EQ(workers[0]->worker_id(), 42);

    EXPECT_DEATH(
        workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
            master_server.endpoint(), 42)),
        "");
  }

  for (auto &worker : workers) worker->join();
}

TEST(Distributed, CoordinationWorkersId) {
  Server master_server({kLocal, 0});
  std::vector<std::unique_ptr<WorkerCoordinationInThread>> workers;
  {
    MasterCoordination master_coord(master_server.endpoint());
    RpcWorkerClients rpc_worker_clients(master_coord);
    ClusterDiscoveryMaster master_discovery_(master_server, master_coord,
                                             rpc_worker_clients);

    workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
        master_server.endpoint(), 42));
    workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
        master_server.endpoint(), 43));

    std::vector<int> ids;
    ids.push_back(0);

    for (auto &worker : workers) ids.push_back(worker->worker_id());
    EXPECT_THAT(master_coord.GetWorkerIds(),
                testing::UnorderedElementsAreArray(ids));
  }

  for (auto &worker : workers) worker->join();
}

TEST(Distributed, ClusterDiscovery) {
  Server master_server({kLocal, 0});
  std::vector<std::unique_ptr<WorkerCoordinationInThread>> workers;
  {
    MasterCoordination master_coord(master_server.endpoint());
    RpcWorkerClients rpc_worker_clients(master_coord);
    ClusterDiscoveryMaster master_discovery_(master_server, master_coord,
                                             rpc_worker_clients);
    std::vector<int> ids;
    int worker_count = 10;

    ids.push_back(0);
    for (int i = 1; i <= worker_count; ++i) {
      workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
          master_server.endpoint(), i));

      ids.push_back(i);
    }

    EXPECT_THAT(master_coord.GetWorkerIds(),
                testing::UnorderedElementsAreArray(ids));
    for (auto &worker : workers) {
      EXPECT_THAT(worker->worker_ids(),
                  testing::UnorderedElementsAreArray(ids));
    }
  }

  for (auto &worker : workers) worker->join();
}
