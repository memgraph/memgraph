#include <experimental/optional>
#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"

#include "communication/messaging/distributed.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "io/network/endpoint.hpp"

using communication::messaging::System;
using namespace distributed;

const int kWorkerCount = 5;
const std::string kLocal = "127.0.0.1";

class WorkerInThread {
 public:
  WorkerInThread(io::network::Endpoint master_endpoint, int desired_id = -1) {
    worker_thread_ = std::thread([this, master_endpoint, desired_id] {
      system_.emplace(Endpoint(kLocal, 0));
      coord_.emplace(*system_, master_endpoint);
      worker_id_ = coord_->RegisterWorker(desired_id);
      coord_->WaitForShutdown();
    });
  }

  int worker_id() const { return worker_id_; }
  auto endpoint() const { return system_->endpoint(); }
  auto worker_endpoint(int worker_id) { return coord_->GetEndpoint(worker_id); }
  void join() { worker_thread_.join(); }

 private:
  std::thread worker_thread_;
  std::experimental::optional<System> system_;
  std::experimental::optional<WorkerCoordination> coord_;
  std::atomic<int> worker_id_{0};
};

TEST(Distributed, Coordination) {
  System master_system({kLocal, 0});
  std::vector<std::unique_ptr<WorkerInThread>> workers;
  {
    MasterCoordination master_coord(master_system);

    for (int i = 0; i < kWorkerCount; ++i)
      workers.emplace_back(
          std::make_unique<WorkerInThread>(master_system.endpoint()));

    // Wait till all the workers are safely initialized.
    std::this_thread::sleep_for(300ms);

    // Expect that all workers have a different ID.
    std::unordered_set<int> worker_ids;
    for (const auto &w : workers) worker_ids.insert(w->worker_id());
    EXPECT_EQ(worker_ids.size(), kWorkerCount);

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
  System master_system({kLocal, 0});
  std::vector<std::unique_ptr<WorkerInThread>> workers;
  {
    MasterCoordination master_coord(master_system);

    workers.emplace_back(
        std::make_unique<WorkerInThread>(master_system.endpoint(), 42));
    std::this_thread::sleep_for(200ms);
    workers.emplace_back(
        std::make_unique<WorkerInThread>(master_system.endpoint(), 42));
    std::this_thread::sleep_for(200ms);

    EXPECT_EQ(workers[0]->worker_id(), 42);
    EXPECT_NE(workers[1]->worker_id(), 42);
  }

  for (auto &worker : workers) worker->join();
}
