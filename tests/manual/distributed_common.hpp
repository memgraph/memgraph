#pragma once

#include <chrono>
#include <vector>

#include "communication/result_stream_faker.hpp"
#include "database/graph_db_accessor.hpp"
#include "glue/conversion.hpp"
#include "query/interpreter.hpp"
#include "query/typed_value.hpp"

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

class Cluster {
  const std::chrono::microseconds kInitTime{200};
  const std::string kLocal = "127.0.0.1";

 public:
  Cluster(int worker_count) {
    database::Config masterconfig;
    masterconfig.master_endpoint = {kLocal, 0};
    master_ = std::make_unique<database::Master>(masterconfig);
    interpreter_ = std::make_unique<query::Interpreter>(*master_);
    std::this_thread::sleep_for(kInitTime);

    auto worker_config = [this](int worker_id) {
      database::Config config;
      config.worker_id = worker_id;
      config.master_endpoint = master_->endpoint();
      config.worker_endpoint = {kLocal, 0};
      return config;
    };

    for (int i = 0; i < worker_count; ++i) {
      workers_.emplace_back(
          std::make_unique<WorkerInThread>(worker_config(i + 1)));
      std::this_thread::sleep_for(kInitTime);
    }
  }

  void Stop() {
    interpreter_ = nullptr;
    master_ = nullptr;
    workers_.clear();
  }

  ~Cluster() {
    if (master_) Stop();
  }

  auto Execute(const std::string &query,
               std::map<std::string, query::TypedValue> params = {}) {
    database::GraphDbAccessor dba(*master_);
    ResultStreamFaker<query::TypedValue> result;
    interpreter_->operator()(query, dba, params, false).PullAll(result);
    dba.Commit();
    return result.GetResults();
  };

 private:
  std::unique_ptr<database::Master> master_;
  std::vector<std::unique_ptr<WorkerInThread>> workers_;
  std::unique_ptr<query::Interpreter> interpreter_;
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
