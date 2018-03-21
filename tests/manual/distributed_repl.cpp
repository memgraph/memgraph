#include <chrono>
#include <iostream>
#include <memory>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include "database/graph_db.hpp"
#include "query/interpreter.hpp"
#include "query/repl.hpp"
#include "utils/flag_validation.hpp"

DEFINE_VALIDATED_int32(worker_count, 1,
                       "The number of worker nodes in cluster.",
                       FLAG_IN_RANGE(1, 1000));
DECLARE_int32(min_log_level);

const std::string kLocal = "127.0.0.1";

class WorkerInThread {
 public:
  explicit WorkerInThread(database::Config config) : worker_(config) {
    thread_ = std::thread([this, config] { worker_.WaitForShutdown(); });
  }

  ~WorkerInThread() {
    if (thread_.joinable()) thread_.join();
  }

  database::Worker worker_;
  std::thread thread_;
};

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_min_log_level = google::ERROR;
  google::InitGoogleLogging(argv[0]);

  // Start the master
  database::Config master_config;
  master_config.master_endpoint = {kLocal, 0};
  auto master = std::make_unique<database::Master>(master_config);
  // Allow the master to get initialized before making workers.
  std::this_thread::sleep_for(std::chrono::milliseconds(250));

  std::vector<std::unique_ptr<WorkerInThread>> workers;
  for (int i = 0; i < FLAGS_worker_count; ++i) {
    database::Config config;
    config.worker_id = i + 1;
    config.master_endpoint = master->endpoint();
    config.worker_endpoint = {kLocal, 0};
    workers.emplace_back(std::make_unique<WorkerInThread>(config));
  }

  // Start the REPL
  query::Repl(*master);

  master = nullptr;
  return 0;
}
