#include <experimental/optional>

#include <gtest/gtest.h>

#include "database/graph_db.hpp"
#include "transactions/engine_master.hpp"

template <typename T>
using optional = std::experimental::optional<T>;

class DistributedGraphDbTest : public ::testing::Test {
  const std::string kLocal = "127.0.0.1";
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
    master_.emplace(master_config);
    std::this_thread::sleep_for(kInitTime);

    auto worker_config = [this](int worker_id) {
      database::Config config;
      config.worker_id = worker_id;
      config.master_endpoint = master_->endpoint();
      config.worker_endpoint = {kLocal, 0};
      return config;
    };

    worker1_.emplace(worker_config(1));
    std::this_thread::sleep_for(kInitTime);
    worker2_.emplace(worker_config(2));
    std::this_thread::sleep_for(kInitTime);
  }

  void TearDown() override {
    // Kill master first because it will expect a shutdown response from the
    // workers.
    master_ = std::experimental::nullopt;

    worker2_ = std::experimental::nullopt;
    worker1_ = std::experimental::nullopt;
  }

  database::Master &master() { return *master_; }
  auto &master_tx_engine() {
    return dynamic_cast<tx::MasterEngine &>(master_->tx_engine());
  }
  database::Worker &worker1() { return worker1_->worker_; }
  database::Worker &worker2() { return worker2_->worker_; }

 private:
  optional<database::Master> master_;
  optional<WorkerInThread> worker1_;
  optional<WorkerInThread> worker2_;
};
