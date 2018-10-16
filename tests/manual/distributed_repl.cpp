#include <chrono>
#include <experimental/filesystem>
#include <iostream>
#include <memory>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "database/distributed/distributed_graph_db.hpp"
#include "query/distributed_interpreter.hpp"
#include "query/repl.hpp"
#include "utils/file.hpp"
#include "utils/flag_validation.hpp"

DEFINE_VALIDATED_int32(worker_count, 1,
                       "The number of worker nodes in cluster.",
                       FLAG_IN_RANGE(1, 1000));
DECLARE_int32(min_log_level);
DECLARE_string(durability_directory);

namespace fs = std::experimental::filesystem;

const std::string kLocal = "127.0.0.1";

class WorkerInThread {
 public:
  explicit WorkerInThread(database::Config config) : worker_(config) {
    thread_ = std::thread([this, config] {
      worker_.Start();
      EXPECT_TRUE(worker_.AwaitShutdown());
    });
  }

  ~WorkerInThread() {
    if (thread_.joinable()) thread_.join();
  }

  database::Worker worker_;
  std::thread thread_;
};

fs::path GetDurabilityDirectory(const fs::path &path, int worker_id) {
  if (worker_id == 0) return path / "master";
  return path / fmt::format("worker{}", worker_id);
}

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  FLAGS_min_log_level = google::ERROR;
  google::InitGoogleLogging(argv[0]);

  fs::path tmp_dir =
      fs::temp_directory_path() / "MG_test_manual_distributed_repl";
  EXPECT_TRUE(utils::EnsureDir(tmp_dir));

  // Start the master
  database::Config master_config;
  master_config.master_endpoint = {kLocal, 0};
  master_config.durability_directory = GetDurabilityDirectory(tmp_dir, 0);
  // Flag needs to be updated due to props on disk storage.
  FLAGS_durability_directory = GetDurabilityDirectory(tmp_dir, 0);
  auto master = std::make_unique<database::Master>(master_config);
  master->Start();
  // Allow the master to get initialized before making workers.
  std::this_thread::sleep_for(std::chrono::milliseconds(250));

  std::vector<std::unique_ptr<WorkerInThread>> workers;
  for (int i = 0; i < FLAGS_worker_count; ++i) {
    database::Config config;
    config.worker_id = i + 1;
    config.master_endpoint = master->endpoint();
    config.worker_endpoint = {kLocal, 0};
    config.durability_directory = GetDurabilityDirectory(tmp_dir, i + 1);
    // Flag needs to be updated due to props on disk storage.
    FLAGS_durability_directory = GetDurabilityDirectory(tmp_dir, i + 1);
    workers.emplace_back(std::make_unique<WorkerInThread>(config));
  }

  // Start the REPL
  {
    query::DistributedInterpreter interpreter(master.get());
    query::Repl(master.get(), &interpreter);
  }

  master = nullptr;
  return 0;
}
