#pragma once

#include <chrono>
#include <experimental/filesystem>
#include <vector>

#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "communication/result_stream_faker.hpp"
#include "database/distributed_graph_db.hpp"
#include "database/graph_db_accessor.hpp"
#include "glue/communication.hpp"
#include "query/distributed_interpreter.hpp"
#include "query/typed_value.hpp"
#include "utils/file.hpp"

DECLARE_string(durability_directory);

namespace fs = std::experimental::filesystem;

class WorkerInThread {
 public:
  explicit WorkerInThread(database::Config config) : worker_(config) {
    thread_ =
        std::thread([this, config] { EXPECT_TRUE(worker_.AwaitShutdown()); });
  }

  ~WorkerInThread() {
    if (thread_.joinable()) thread_.join();
  }

  database::Worker worker_;
  std::thread thread_;
};

class Cluster {
  const std::chrono::microseconds kInitTime{200};
  const std::string kLocal = "127.0.0.1";

 public:
  Cluster(int worker_count, const std::string &test_name) {
    tmp_dir_ = fs::temp_directory_path() / "MG_test_unit_distributed_common_" /
               test_name;
    EXPECT_TRUE(utils::EnsureDir(tmp_dir_));

    database::Config master_config;
    master_config.master_endpoint = {kLocal, 0};
    master_config.durability_directory = GetDurabilityDirectory(0);
    // Flag needs to be updated due to props on disk storage.
    FLAGS_durability_directory = GetDurabilityDirectory(0);
    master_ = std::make_unique<database::Master>(master_config);
    interpreter_ =
        std::make_unique<query::DistributedInterpreter>(master_.get());
    std::this_thread::sleep_for(kInitTime);

    auto worker_config = [this](int worker_id) {
      database::Config config;
      config.worker_id = worker_id;
      config.master_endpoint = master_->endpoint();
      config.durability_directory = GetDurabilityDirectory(worker_id);
      config.worker_endpoint = {kLocal, 0};
      return config;
    };

    for (int i = 0; i < worker_count; ++i) {
      // Flag needs to be updated due to props on disk storage.
      FLAGS_durability_directory = GetDurabilityDirectory(i + 1);
      workers_.emplace_back(
          std::make_unique<WorkerInThread>(worker_config(i + 1)));
      std::this_thread::sleep_for(kInitTime);
    }
  }

  void Stop() {
    interpreter_ = nullptr;
    master_->Shutdown();
    EXPECT_TRUE(master_->AwaitShutdown());
    workers_.clear();
  }

  ~Cluster() {
    if (master_) Stop();
  }

  auto Execute(const std::string &query,
               std::map<std::string, PropertyValue> params = {}) {
    auto dba = master_->Access();
    ResultStreamFaker<query::TypedValue> result;
    (*interpreter_)(query, *dba, params, false).PullAll(result);
    dba->Commit();
    return result.GetResults();
  };

  fs::path GetDurabilityDirectory(int worker_id) {
    if (worker_id == 0) return tmp_dir_ / "master";
    return tmp_dir_ / fmt::format("worker{}", worker_id);
  }

 private:
  std::unique_ptr<database::Master> master_;
  std::vector<std::unique_ptr<WorkerInThread>> workers_;
  std::unique_ptr<query::DistributedInterpreter> interpreter_;
  fs::path tmp_dir_{fs::temp_directory_path() /
                    "MG_test_manual_distributed_common"};
};

void CheckResults(
    const std::vector<std::vector<query::TypedValue>> &results,
    const std::vector<std::vector<query::TypedValue>> &expected_rows,
    const std::string &msg) {
  query::TypedValue::BoolEqual equality;
  CHECK(results.size() == expected_rows.size())
      << msg << " (expected " << expected_rows.size() << " rows "
      << ", got " << results.size() << ")";
  for (size_t row_id = 0; row_id < results.size(); ++row_id) {
    auto &result = results[row_id];
    auto &expected = expected_rows[row_id];
    CHECK(result.size() == expected.size())
        << msg << " (expected " << expected.size() << " elements in row "
        << row_id << ", got " << result.size() << ")";
    for (size_t col_id = 0; col_id < result.size(); ++col_id) {
      CHECK(equality(result[col_id], expected[col_id]))
          << msg << " (expected value '" << expected[col_id] << "' got '"
          << result[col_id] << "' in row " << row_id << " col " << col_id
          << ")";
    }
  }
}
