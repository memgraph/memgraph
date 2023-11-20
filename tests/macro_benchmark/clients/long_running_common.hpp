// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "json/json.hpp"

#include "utils/logging.hpp"
#include "utils/timer.hpp"

#include "common.hpp"

const int MAX_RETRIES = 30;

DEFINE_string(db, "", "Database queries are executed on.");
DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_int32(num_workers, 1, "Number of workers");
DEFINE_string(output, "", "Output file");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");
DEFINE_int32(duration, 30, "Number of seconds to execute benchmark");

DEFINE_string(group, "unknown", "Test group name");
DEFINE_string(scenario, "unknown", "Test scenario name");

std::atomic<uint64_t> executed_queries{0};
std::atomic<uint64_t> executed_steps{0};
std::atomic<uint64_t> serialization_errors{0};

class TestClient {
 public:
  TestClient() {
    Endpoint endpoint(FLAGS_address, FLAGS_port);
    client_.Connect(endpoint, FLAGS_username, FLAGS_password);
  }

  virtual ~TestClient() = default;

  auto ConsumeStats() {
    std::unique_lock<memgraph::utils::SpinLock> guard(lock_);
    auto stats = stats_;
    stats_.clear();
    return stats;
  }

  void Run() {
    runner_thread_ = std::thread([&] {
      while (keep_running_) {
        Step();
        ++executed_steps;
      }
    });
  }

  void Stop() {
    keep_running_ = false;
    runner_thread_.join();
  }

 protected:
  virtual void Step() = 0;

  std::optional<memgraph::communication::bolt::QueryData> Execute(const std::string &query,
                                                                  const std::map<std::string, Value> &params,
                                                                  const std::string &query_name = "") {
    memgraph::communication::bolt::QueryData result;
    int retries;
    memgraph::utils::Timer timer;
    try {
      std::tie(result, retries) = ExecuteNTimesTillSuccess(client_, query, params, MAX_RETRIES);
    } catch (const memgraph::utils::BasicException &e) {
      serialization_errors += MAX_RETRIES;
      return std::nullopt;
    }
    auto wall_time = timer.Elapsed();
    auto metadata = result.metadata;
    metadata["wall_time"] = wall_time.count();
    {
      std::unique_lock<memgraph::utils::SpinLock> guard(lock_);
      if (query_name != "") {
        stats_[query_name].push_back(std::move(metadata));
      } else {
        stats_[query].push_back(std::move(metadata));
      }
    }
    ++executed_queries;
    serialization_errors += retries;
    return result;
  }

  memgraph::utils::SpinLock lock_;
  std::unordered_map<std::string, std::vector<std::map<std::string, Value>>> stats_;

  std::atomic<bool> keep_running_{true};
  std::thread runner_thread_;

 private:
  memgraph::communication::ClientContext context_{FLAGS_use_ssl};
  Client client_{context_};
};

void RunMultithreadedTest(std::vector<std::unique_ptr<TestClient>> &clients) {
  MG_ASSERT((int)clients.size() == FLAGS_num_workers);

  // Open stream for writing stats.
  std::streambuf *buf;
  std::ofstream f;
  if (FLAGS_output != "") {
    f.open(FLAGS_output);
    buf = f.rdbuf();
  } else {
    buf = std::cout.rdbuf();
  }
  std::ostream out(buf);

  memgraph::utils::Timer timer;
  for (auto &client : clients) {
    client->Run();
  }
  spdlog::info("Starting test with {} workers", clients.size());
  while (timer.Elapsed().count() < FLAGS_duration) {
    std::unordered_map<std::string, std::map<std::string, Value>> aggregated_stats;

    using namespace std::chrono_literals;
    std::unordered_map<std::string, std::vector<std::map<std::string, Value>>> stats;
    for (const auto &client : clients) {
      auto client_stats = client->ConsumeStats();
      for (const auto &client_query_stats : client_stats) {
        auto &query_stats = stats[client_query_stats.first];
        query_stats.insert(query_stats.end(), client_query_stats.second.begin(), client_query_stats.second.end());
      }
    }

    // TODO: Here we combine pure values, json and Value which is a
    // little bit chaotic. Think about refactoring this part to only use json
    // and write Value to json converter.
    const std::vector<std::string> fields = {
        "wall_time",
        "parsing_time",
        "planning_time",
        "plan_execution_time",
    };
    for (const auto &query_stats : stats) {
      std::map<std::string, double> new_aggregated_query_stats;
      for (const auto &stats : query_stats.second) {
        for (const auto &field : fields) {
          auto it = stats.find(field);
          if (it != stats.end()) {
            new_aggregated_query_stats[field] += it->second.ValueDouble();
          }
        }
      }
      int64_t new_count = query_stats.second.size();

      auto &aggregated_query_stats = aggregated_stats[query_stats.first];
      aggregated_query_stats.insert({"count", Value(0)});
      auto old_count = aggregated_query_stats["count"].ValueInt();
      aggregated_query_stats["count"].ValueInt() += new_count;
      for (const auto &stat : new_aggregated_query_stats) {
        auto it = aggregated_query_stats.insert({stat.first, Value(0.0)}).first;
        it->second = (it->second.ValueDouble() * old_count + stat.second) / (old_count + new_count);
      }
    }

    out << "{\"num_executed_queries\": " << executed_queries << ", "
        << "\"num_executed_steps\": " << executed_steps << ", "
        << "\"elapsed_time\": " << timer.Elapsed().count() << ", \"queries\": [";
    memgraph::utils::PrintIterable(out, aggregated_stats, ", ", [](auto &stream, const auto &x) {
      stream << "{\"query\": " << nlohmann::json(x.first) << ", \"stats\": ";
      PrintJsonValue(stream, Value(x.second));
      stream << "}";
    });
    out << "]}" << std::endl;
    out.flush();
    std::this_thread::sleep_for(1s);
  }
  spdlog::info("Stopping workers...");
  for (auto &client : clients) {
    client->Stop();
  }
  spdlog::info("Stopped workers...");
}
