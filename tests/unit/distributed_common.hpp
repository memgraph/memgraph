#include <memory>

#include <gtest/gtest.h>

#include "database/graph_db.hpp"
#include "transactions/engine_master.hpp"

class DistributedGraphDbTest : public ::testing::Test {
  const std::string kLocal = "127.0.0.1";
  const int kWorkerCount = 2;

  class WorkerInThread {
   public:
    WorkerInThread(database::Config config) : worker_(config) {
      thread_ = std::thread([this, config] { worker_.WaitForShutdown(); });
    }

    ~WorkerInThread() {
      if (thread_.joinable()) thread_.join();
    }

    database::Worker worker_;
    std::thread thread_;
  };

 protected:
  void SetUp() override {
    const auto kInitTime = 200ms;

    database::Config master_config;
    master_config.master_endpoint = {kLocal, 0};
    master_ = std::make_unique<database::Master>(master_config);
    std::this_thread::sleep_for(kInitTime);

    auto worker_config = [this](int worker_id) {
      database::Config config;
      config.worker_id = worker_id;
      config.master_endpoint = master_->endpoint();
      config.worker_endpoint = {kLocal, 0};
      return config;
    };

    for (int i = 0; i < kWorkerCount; ++i) {
      workers_.emplace_back(
          std::make_unique<WorkerInThread>(worker_config(i + 1)));
      std::this_thread::sleep_for(kInitTime);
    }
  }

  void TearDown() override {
    // Kill master first because it will expect a shutdown response from the
    // workers.
    master_ = nullptr;
    for (int i = kWorkerCount - 1; i >= 0; --i) workers_[i] = nullptr;
  }

  database::Master &master() { return *master_; }
  auto &master_tx_engine() {
    return dynamic_cast<tx::MasterEngine &>(master_->tx_engine());
  }

  database::Worker &worker(int worker_id) {
    return workers_[worker_id - 1]->worker_;
  }

 private:
  std::unique_ptr<database::Master> master_;
  std::vector<std::unique_ptr<WorkerInThread>> workers_;
};
