#include <memory>
#include <thread>

#include "gtest/gtest.h"

#include "database/distributed/distributed_graph_db.hpp"
#include "distributed_common.hpp"
#include "io/network/endpoint.hpp"
#include "query_plan_common.hpp"

namespace fs = std::experimental::filesystem;
using namespace std::literals::chrono_literals;

class DistributedDynamicWorker : public ::testing::Test {
 public:
  const std::string kLocal = "127.0.0.1";

  std::unique_ptr<database::Master> CreateMaster(
      std::function<database::Config(database::Config config)> modify_config) {
    database::Config master_config;
    master_config.master_endpoint = {kLocal, 0};
    master_config.durability_directory = GetDurabilityDirectory(0);
    // Flag needs to be updated due to props on disk storage.
    FLAGS_durability_directory = GetDurabilityDirectory(0);
    auto master =
        std::make_unique<database::Master>(modify_config(master_config));
    master->Start();
    std::this_thread::sleep_for(200ms);
    return master;
  }

  std::unique_ptr<WorkerInThread> CreateWorker(
      io::network::Endpoint master_endpoint, int worker_id,
      std::function<database::Config(database::Config config)> modify_config) {
    database::Config config;
    config.durability_directory = GetDurabilityDirectory(worker_id);
    // Flag needs to be updated due to props on disk storage.
    FLAGS_durability_directory = GetDurabilityDirectory(worker_id);

    config.worker_id = worker_id;
    config.master_endpoint = master_endpoint;
    config.worker_endpoint = {kLocal, 0};

    auto worker = std::make_unique<WorkerInThread>(modify_config(config));
    std::this_thread::sleep_for(200ms);
    return worker;
  }

  fs::path GetDurabilityDirectory(int worker_id) {
    if (worker_id == 0) return tmp_dir_ / "master";
    return tmp_dir_ / fmt::format("worker{}", worker_id);
  }

  void CleanDurability() {
    if (fs::exists(tmp_dir_)) fs::remove_all(tmp_dir_);
  }

 private:
  fs::path tmp_dir_ =
      fs::temp_directory_path() / ("MG_test_unit_distributed_worker_addition");
};

TEST_F(DistributedDynamicWorker, IndexExistsOnNewWorker) {
  auto modify_config = [](database::Config config) { return config; };
  auto master = CreateMaster(modify_config);

  // Lets insert some data and build label property index
  {
    auto dba = master->Access();
    storage::Label label;
    storage::Property property;
    label = dba->Label("label");
    property = dba->Property("property");

    for (int i = 0; i < 100; ++i) {
      auto vertex = dba->InsertVertex();
      vertex.add_label(label);
      vertex.PropsSet(property, 1);
    }
    dba->Commit();
  }

  {
    auto dba = master->Access();
    storage::Label label;
    storage::Property property;
    label = dba->Label("label");
    property = dba->Property("property");

    dba->BuildIndex(label, property, false);
    EXPECT_TRUE(dba->LabelPropertyIndexExists(label, property));
    EXPECT_EQ(CountIterable(dba->Vertices(label, property, false)), 100);
  }

  auto num_workers = master->GetWorkerIds().size();

  auto worker1 = CreateWorker(master->endpoint(), 1, modify_config);

  while (master->GetWorkerIds().size() < num_workers + 1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  while (worker1->worker_.GetWorkerIds().size() < num_workers + 1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  // Check that the new worker has that index
  {
    auto dba = worker1->db()->Access();
    storage::Label label;
    storage::Property property;
    label = dba->Label("label");
    property = dba->Property("property");

    EXPECT_TRUE(dba->LabelPropertyIndexExists(label, property));
  }

  master->Shutdown();
  EXPECT_TRUE(master->AwaitShutdown());
  worker1 = nullptr;
}

TEST_F(DistributedDynamicWorker, IndexExistsOnNewWorkerAfterRecovery) {
  auto modify_config = [](database::Config config) { return config; };
  auto durability_config = [](database::Config config) {
    config.durability_enabled = true;
    config.snapshot_on_exit = true;
    return config;
  };
  auto recovery_config = [](database::Config config) {
    config.recovering_cluster_size = 1;
    config.db_recover_on_startup = true;
    return config;
  };

  {
    auto master = CreateMaster(durability_config);

    // Lets insert some data and build label property index
    {
      auto dba = master->Access();
      storage::Label label;
      storage::Property property;
      label = dba->Label("label");
      property = dba->Property("property");

      for (int i = 0; i < 100; ++i) {
        auto vertex = dba->InsertVertex();
        vertex.add_label(label);
        vertex.PropsSet(property, 1);
      }
      dba->Commit();
    }

    {
      auto dba = master->Access();
      storage::Label label;
      storage::Property property;
      label = dba->Label("label");
      property = dba->Property("property");

      dba->BuildIndex(label, property, false);
      EXPECT_TRUE(dba->LabelPropertyIndexExists(label, property));
    }

    master->Shutdown();
    EXPECT_TRUE(master->AwaitShutdown());
  }

  {
    auto master = CreateMaster(recovery_config);

    // Make sure the index is recovered on master.
    {
      auto dba = master->Access();
      storage::Label label;
      storage::Property property;
      label = dba->Label("label");
      property = dba->Property("property");

      EXPECT_TRUE(dba->LabelPropertyIndexExists(label, property));
    }

    auto num_workers = master->GetWorkerIds().size();

    auto worker1 = CreateWorker(master->endpoint(), 1, modify_config);

    while (master->GetWorkerIds().size() < num_workers + 1) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    while (worker1->worker_.GetWorkerIds().size() < num_workers + 1) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Check that the new worker has that index.
    {
      auto dba = worker1->db()->Access();
      storage::Label label;
      storage::Property property;
      label = dba->Label("label");
      property = dba->Property("property");

      EXPECT_TRUE(dba->LabelPropertyIndexExists(label, property));
    }

    master->Shutdown();
    EXPECT_TRUE(master->AwaitShutdown());
    worker1 = nullptr;
  }
}
