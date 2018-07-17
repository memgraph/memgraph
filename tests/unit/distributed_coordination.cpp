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

using communication::rpc::ClientPool;
using communication::rpc::Server;
using namespace distributed;
using namespace std::literals::chrono_literals;

const int kWorkerCount = 5;
const std::string kLocal = "127.0.0.1";

class WorkerCoordinationInThread {
  struct Worker {
    Worker(Endpoint master_endpoint) : master_endpoint(master_endpoint) {}
    Endpoint master_endpoint;
    Server server{{kLocal, 0}};
    WorkerCoordination coord{server, master_endpoint};
    ClientPool client_pool{master_endpoint};
    ClusterDiscoveryWorker discovery{server, coord, client_pool};
    std::atomic<int> worker_id_{0};
  };

 public:
  WorkerCoordinationInThread(io::network::Endpoint master_endpoint,
                             int desired_id = -1) {
    std::atomic<bool> init_done{false};
    worker_thread_ =
        std::thread([this, master_endpoint, desired_id, &init_done] {
          worker.emplace(master_endpoint);
          worker->discovery.RegisterWorker(desired_id);
          worker->worker_id_ = desired_id;
          init_done = true;
          worker->coord.WaitForShutdown();
          worker = std::experimental::nullopt;
        });

    while (!init_done) std::this_thread::sleep_for(10ms);
  }

  int worker_id() const { return worker->worker_id_; }
  auto endpoint() const { return worker->server.endpoint(); }
  auto worker_endpoint(int worker_id) {
    return worker->coord.GetEndpoint(worker_id);
  }
  auto worker_ids() { return worker->coord.GetWorkerIds(); }
  void join() { worker_thread_.join(); }
  void NotifyWorkerRecovered() {
    std::experimental::optional<durability::RecoveryInfo> no_recovery_info;
    worker->discovery.NotifyWorkerRecovered(no_recovery_info);
  }

 private:
  std::thread worker_thread_;
  std::experimental::optional<Worker> worker;
};

TEST(Distributed, Coordination) {
  Server master_server({kLocal, 0});
  std::vector<std::unique_ptr<WorkerCoordinationInThread>> workers;
  {
    MasterCoordination master_coord(master_server.endpoint());
    master_coord.SetRecoveredSnapshot(std::experimental::nullopt);
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
    master_coord.SetRecoveredSnapshot(std::experimental::nullopt);
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
    master_coord.SetRecoveredSnapshot(std::experimental::nullopt);
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
    master_coord.SetRecoveredSnapshot(std::experimental::nullopt);
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

TEST(Distributed, KeepsTrackOfRecovered) {
  Server master_server({kLocal, 0});
  std::vector<std::unique_ptr<WorkerCoordinationInThread>> workers;
  {
    MasterCoordination master_coord(master_server.endpoint());
    master_coord.SetRecoveredSnapshot(std::experimental::nullopt);
    RpcWorkerClients rpc_worker_clients(master_coord);
    ClusterDiscoveryMaster master_discovery_(master_server, master_coord,
                                             rpc_worker_clients);
    int worker_count = 10;
    for (int i = 1; i <= worker_count; ++i) {
      workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
          master_server.endpoint(), i));
      workers.back()->NotifyWorkerRecovered();
      EXPECT_THAT(master_coord.CountRecoveredWorkers(), i);
    }
  }

  for (auto &worker : workers) worker->join();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  return RUN_ALL_TESTS();
}
