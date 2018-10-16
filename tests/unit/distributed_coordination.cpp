#include <atomic>
#include <experimental/optional>
#include <memory>
#include <thread>
#include <unordered_set>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "distributed/cluster_discovery_master.hpp"
#include "distributed/cluster_discovery_worker.hpp"
#include "distributed/coordination_master.hpp"
#include "distributed/coordination_worker.hpp"
#include "io/network/endpoint.hpp"
#include "utils/file.hpp"

using namespace distributed;
using namespace std::literals::chrono_literals;

const int kWorkerCount = 5;
const std::string kLocal = "127.0.0.1";

class WorkerCoordinationInThread {
  struct Worker {
    Worker(Endpoint master_endpoint, int worker_id)
        : coord({kLocal, 0}, worker_id, master_endpoint),
          discovery(&coord),
          worker_id(worker_id) {}
    WorkerCoordination coord;
    ClusterDiscoveryWorker discovery;
    std::atomic<int> worker_id;
  };

 public:
  WorkerCoordinationInThread(io::network::Endpoint master_endpoint,
                             fs::path durability_directory,
                             int desired_id = -1) {
    std::atomic<bool> init_done{false};
    worker_thread_ = std::thread(
        [this, master_endpoint, durability_directory, desired_id, &init_done] {
          worker.emplace(master_endpoint, desired_id);
          worker->discovery.RegisterWorker(desired_id, durability_directory);
          init_done = true;
          // We don't shutdown the worker coordination here because it will be
          // shutdown by the master. We only wait for the shutdown to be
          // finished.
          EXPECT_TRUE(worker->coord.AwaitShutdown());
          worker = std::experimental::nullopt;
        });

    while (!init_done) std::this_thread::sleep_for(10ms);
  }

  int worker_id() { return worker->worker_id; }
  auto endpoint() { return worker->coord.GetServerEndpoint(); }
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

class Distributed : public ::testing::Test {
 public:
  void SetUp() override { ASSERT_TRUE(utils::EnsureDir(tmp_dir_)); }

  void TearDown() override {
    if (fs::exists(tmp_dir_)) fs::remove_all(tmp_dir_);
  }

  const fs::path tmp_dir(const fs::path &path) const { return tmp_dir_ / path; }

 private:
  fs::path tmp_dir_{fs::temp_directory_path() /
                    "MG_test_unit_distributed_coordination"};
};

TEST_F(Distributed, Coordination) {
  std::vector<std::unique_ptr<WorkerCoordinationInThread>> workers;

  MasterCoordination master_coord({kLocal, 0});
  master_coord.SetRecoveredSnapshot(std::experimental::nullopt);
  ClusterDiscoveryMaster master_discovery_(&master_coord, tmp_dir("master"));

  for (int i = 1; i <= kWorkerCount; ++i)
    workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
        master_coord.GetServerEndpoint(), tmp_dir(fmt::format("worker{}", i)),
        i));

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

  // Coordinated shutdown.
  master_coord.Shutdown();
  EXPECT_TRUE(master_coord.AwaitShutdown());
  for (auto &worker : workers) worker->join();
}

TEST_F(Distributed, DesiredAndUniqueId) {
  std::vector<std::unique_ptr<WorkerCoordinationInThread>> workers;

  MasterCoordination master_coord({kLocal, 0});
  master_coord.SetRecoveredSnapshot(std::experimental::nullopt);
  ClusterDiscoveryMaster master_discovery_(&master_coord, tmp_dir("master"));

  workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
      master_coord.GetServerEndpoint(), tmp_dir("worker42"), 42));
  EXPECT_EQ(workers[0]->worker_id(), 42);

  EXPECT_DEATH(
      workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
          master_coord.GetServerEndpoint(), tmp_dir("worker42"), 42)),
      "");

  // Coordinated shutdown.
  master_coord.Shutdown();
  EXPECT_TRUE(master_coord.AwaitShutdown());
  for (auto &worker : workers) worker->join();
}

TEST_F(Distributed, CoordinationWorkersId) {
  std::vector<std::unique_ptr<WorkerCoordinationInThread>> workers;

  MasterCoordination master_coord({kLocal, 0});
  master_coord.SetRecoveredSnapshot(std::experimental::nullopt);
  ClusterDiscoveryMaster master_discovery_(&master_coord, tmp_dir("master"));

  workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
      master_coord.GetServerEndpoint(), tmp_dir("worker42"), 42));
  workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
      master_coord.GetServerEndpoint(), tmp_dir("worker43"), 43));

  std::vector<int> ids;
  ids.push_back(0);

  for (auto &worker : workers) ids.push_back(worker->worker_id());
  EXPECT_THAT(master_coord.GetWorkerIds(),
              testing::UnorderedElementsAreArray(ids));

  // Coordinated shutdown.
  master_coord.Shutdown();
  EXPECT_TRUE(master_coord.AwaitShutdown());
  for (auto &worker : workers) worker->join();
}

TEST_F(Distributed, ClusterDiscovery) {
  std::vector<std::unique_ptr<WorkerCoordinationInThread>> workers;

  MasterCoordination master_coord({kLocal, 0});
  master_coord.SetRecoveredSnapshot(std::experimental::nullopt);
  ClusterDiscoveryMaster master_discovery_(&master_coord, tmp_dir("master"));
  std::vector<int> ids;
  int worker_count = 10;

  ids.push_back(0);
  for (int i = 1; i <= worker_count; ++i) {
    workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
        master_coord.GetServerEndpoint(), tmp_dir(fmt::format("worker", i)),
        i));

    ids.push_back(i);
  }

  EXPECT_THAT(master_coord.GetWorkerIds(),
              testing::UnorderedElementsAreArray(ids));
  for (auto &worker : workers) {
    EXPECT_THAT(worker->worker_ids(), testing::UnorderedElementsAreArray(ids));
  }

  // Coordinated shutdown.
  master_coord.Shutdown();
  EXPECT_TRUE(master_coord.AwaitShutdown());
  for (auto &worker : workers) worker->join();
}

TEST_F(Distributed, KeepsTrackOfRecovered) {
  std::vector<std::unique_ptr<WorkerCoordinationInThread>> workers;

  MasterCoordination master_coord({kLocal, 0});
  master_coord.SetRecoveredSnapshot(std::experimental::nullopt);
  ClusterDiscoveryMaster master_discovery_(&master_coord, tmp_dir("master"));
  int worker_count = 10;
  for (int i = 1; i <= worker_count; ++i) {
    workers.emplace_back(std::make_unique<WorkerCoordinationInThread>(
        master_coord.GetServerEndpoint(), tmp_dir(fmt::format("worker{}", i)),
        i));
    workers.back()->NotifyWorkerRecovered();
    EXPECT_THAT(master_coord.CountRecoveredWorkers(), i);
  }

  // Coordinated shutdown.
  master_coord.Shutdown();
  EXPECT_TRUE(master_coord.AwaitShutdown());
  for (auto &worker : workers) worker->join();
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  return RUN_ALL_TESTS();
}
