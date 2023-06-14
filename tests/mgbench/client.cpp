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

#include <algorithm>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <numeric>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <math.h>
#include <json/json.hpp>

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/value.hpp"
#include "communication/init.hpp"
#include "spdlog/formatter.h"
#include "spdlog/spdlog.h"
#include "utils/exceptions.hpp"
#include "utils/logging.hpp"
#include "utils/string.hpp"
#include "utils/timer.hpp"

DEFINE_string(address, "127.0.0.1", "Server address.");
DEFINE_int32(port, 7687, "Server port.");
DEFINE_string(username, "", "Username for the database.");
DEFINE_string(password, "", "Password for the database.");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");

DEFINE_uint64(num_workers, 1,
              "Number of workers that should be used to concurrently execute "
              "the supplied queries.");
DEFINE_uint64(max_retries, 50, "Maximum number of retries for each query.");
DEFINE_bool(queries_json, false,
            "Set to true to load all queries as as single JSON encoded list. Each item "
            "in the list should contain another list whose first element is the query "
            "that should be executed and the second element should be a dictionary of "
            "query parameters for that query.");

DEFINE_string(input, "", "Input file. By default stdin is used.");
DEFINE_string(output, "", "Output file. By default stdout is used.");
DEFINE_bool(validation, false,
            "Set to true to run client in validation mode."
            "Validation mode works for singe query and returns results for validation"
            "with metadata");
DEFINE_int64(time_dependent_execution, 0,
             "Time-dependent executions execute the queries for a specified number of seconds."
             "If all queries are executed, and there is still time, queries are rerun again."
             "If the time runs out, the client is done with the job and returning results.");

std::pair<std::map<std::string, memgraph::communication::bolt::Value>, uint64_t> ExecuteNTimesTillSuccess(
    memgraph::communication::bolt::Client *client, const std::string &query,
    const std::map<std::string, memgraph::communication::bolt::Value> &params, int max_attempts) {
  for (uint64_t i = 0; i < max_attempts; ++i) {
    try {
      auto ret = client->Execute(query, params);

      return {std::move(ret.metadata), i};
    } catch (const memgraph::utils::BasicException &e) {
      if (i == max_attempts - 1) {
        LOG_FATAL("Could not execute query '{}' {} times! Error message: {}", query, max_attempts, e.what());
      } else {
        continue;
      }
    }
  }
  LOG_FATAL("Could not execute query '{}' {} times!", query, max_attempts);
}

// Validation returns results and metadata
std::pair<std::map<std::string, memgraph::communication::bolt::Value>,
          std::vector<std::vector<memgraph::communication::bolt::Value>>>
ExecuteValidationNTimesTillSuccess(memgraph::communication::bolt::Client *client, const std::string &query,
                                   const std::map<std::string, memgraph::communication::bolt::Value> &params,
                                   int max_attempts) {
  for (uint64_t i = 0; i < max_attempts; ++i) {
    try {
      auto ret = client->Execute(query, params);

      return {std::move(ret.metadata), std::move(ret.records)};
    } catch (const memgraph::utils::BasicException &e) {
      if (i == max_attempts - 1) {
        LOG_FATAL("Could not execute query '{}' {} times! Error message: {}", query, max_attempts, e.what());
      } else {
        continue;
      }
    }
  }
  LOG_FATAL("Could not execute query '{}' {} times!", query, max_attempts);
}

memgraph::communication::bolt::Value JsonToBoltValue(const nlohmann::json &data) {
  switch (data.type()) {
    case nlohmann::json::value_t::null:
      return {};
    case nlohmann::json::value_t::boolean:
      return {data.get<bool>()};
    case nlohmann::json::value_t::string:
      return {data.get<std::string>()};
    case nlohmann::json::value_t::number_integer:
      return {data.get<int64_t>()};
    case nlohmann::json::value_t::number_unsigned:
      return {static_cast<int64_t>(data.get<uint64_t>())};
    case nlohmann::json::value_t::number_float:
      return {data.get<double>()};
    case nlohmann::json::value_t::array: {
      std::vector<memgraph::communication::bolt::Value> vec;
      vec.reserve(data.size());
      for (const auto &item : data.get<nlohmann::json::array_t>()) {
        vec.emplace_back(JsonToBoltValue(item));
      }
      return {std::move(vec)};
    }
    case nlohmann::json::value_t::object: {
      std::map<std::string, memgraph::communication::bolt::Value> map;
      for (const auto &item : data.get<nlohmann::json::object_t>()) {
        map.emplace(item.first, JsonToBoltValue(item.second));
      }
      return {std::move(map)};
    }
    case nlohmann::json::value_t::binary:
    case nlohmann::json::value_t::discarded:
      LOG_FATAL("Unexpected JSON type!");
  }
}

class Metadata final {
 private:
  struct Record {
    uint64_t count{0};
    double average{0.0};
    double minimum{std::numeric_limits<double>::infinity()};
    double maximum{-std::numeric_limits<double>::infinity()};
  };

 public:
  void Append(const std::map<std::string, memgraph::communication::bolt::Value> &values) {
    for (const auto &item : values) {
      if (!item.second.IsInt() && !item.second.IsDouble()) continue;
      auto [it, emplaced] = storage_.emplace(item.first, Record());
      auto &record = it->second;
      double value = 0.0;
      if (item.second.IsInt()) {
        value = item.second.ValueInt();
      } else {
        value = item.second.ValueDouble();
      }
      ++record.count;
      record.average += value;
      record.minimum = std::min(record.minimum, value);
      record.maximum = std::max(record.maximum, value);
    }
  }

  nlohmann::json Export() {
    nlohmann::json data = nlohmann::json::object();
    for (const auto &item : storage_) {
      nlohmann::json row = nlohmann::json::object();
      row["average"] = item.second.average / item.second.count;
      row["minimum"] = item.second.minimum;
      row["maximum"] = item.second.maximum;
      data[item.first] = row;
    }
    return data;
  }

  Metadata &operator+=(const Metadata &other) {
    for (const auto &item : other.storage_) {
      auto [it, emplaced] = storage_.emplace(item.first, Record());
      auto &record = it->second;
      record.count += item.second.count;
      record.average += item.second.average;
      record.minimum = std::min(record.minimum, item.second.minimum);
      record.maximum = std::max(record.maximum, item.second.maximum);
    }
    return *this;
  }

 private:
  std::map<std::string, Record> storage_;
};

nlohmann::json LatencyStatistics(std::vector<std::vector<double>> &worker_query_latency) {
  nlohmann::json statistics = nlohmann::json::object();
  std::vector<double> query_latency;
  for (int i = 0; i < FLAGS_num_workers; i++) {
    for (auto &e : worker_query_latency[i]) {
      query_latency.push_back(e);
    }
  }
  auto iterations = query_latency.size();
  const int lower_bound = 10;
  if (iterations > lower_bound) {
    std::sort(query_latency.begin(), query_latency.end());
    statistics["iterations"] = iterations;
    statistics["min"] = query_latency.front();
    statistics["max"] = query_latency.back();
    statistics["mean"] = std::accumulate(query_latency.begin(), query_latency.end(), 0.0) / iterations;
    statistics["p99"] = query_latency[floor(iterations * 0.99)];
    statistics["p95"] = query_latency[floor(iterations * 0.95)];
    statistics["p90"] = query_latency[floor(iterations * 0.90)];
    statistics["p75"] = query_latency[floor(iterations * 0.75)];
    statistics["p50"] = query_latency[floor(iterations * 0.50)];

  } else {
    spdlog::info("To few iterations to calculate latency values!");
    statistics["iterations"] = iterations;
  }
  return statistics;
}

void ExecuteTimeDependentWorkload(
    const std::vector<std::pair<std::string, std::map<std::string, memgraph::communication::bolt::Value>>> &queries,
    std::ostream *stream) {
  std::vector<std::thread> threads;
  threads.reserve(FLAGS_num_workers);

  std::vector<uint64_t> worker_retries(FLAGS_num_workers, 0);
  std::vector<Metadata> worker_metadata(FLAGS_num_workers, Metadata());
  std::vector<double> worker_duration(FLAGS_num_workers, 0.0);
  std::vector<std::vector<double>> worker_query_durations(FLAGS_num_workers);

  // Start workers and execute queries.
  auto size = queries.size();
  std::atomic<bool> run(false);
  std::atomic<uint64_t> ready(0);
  std::atomic<uint64_t> position(0);
  std::atomic<bool> start_workload_timer(false);

  std::chrono::time_point<std::chrono::steady_clock> workload_start;
  std::chrono::duration<double> time_limit = std::chrono::seconds(FLAGS_time_dependent_execution);
  for (int worker = 0; worker < FLAGS_num_workers; ++worker) {
    threads.push_back(std::thread([&, worker]() {
      memgraph::io::network::Endpoint endpoint(FLAGS_address, FLAGS_port);
      memgraph::communication::ClientContext context(FLAGS_use_ssl);
      memgraph::communication::bolt::Client client(context);
      client.Connect(endpoint, FLAGS_username, FLAGS_password);

      ready.fetch_add(1, std::memory_order_acq_rel);
      while (!run.load(std::memory_order_acq_rel))
        ;
      auto &retries = worker_retries[worker];
      auto &metadata = worker_metadata[worker];
      auto &duration = worker_duration[worker];
      auto &query_duration = worker_query_durations[worker];

      // After all threads have been initialised, start the workload timer
      if (!start_workload_timer.load()) {
        workload_start = std::chrono::steady_clock::now();
        start_workload_timer.store(true);
      }

      memgraph::utils::Timer worker_timer;
      while (std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() -
                                                                       workload_start) < time_limit) {
        auto pos = position.fetch_add(1, std::memory_order_acq_rel);
        if (pos >= size) {
          /// Get back to inital position
          position.store(0, std::memory_order_acq_rel);
          pos = position.fetch_add(1, std::memory_order_acq_rel);
        }
        const auto &query = queries[pos];
        memgraph::utils::Timer query_timer;
        auto ret = ExecuteNTimesTillSuccess(&client, query.first, query.second, FLAGS_max_retries);
        query_duration.emplace_back(query_timer.Elapsed().count());
        retries += ret.second;
        metadata.Append(ret.first);
        duration = worker_timer.Elapsed().count();
      }
      client.Close();
    }));
  }

  // Synchronize workers and collect runtime.
  while (ready.load(std::memory_order_acq_rel) < FLAGS_num_workers)
    ;

  run.store(true);
  for (int i = 0; i < FLAGS_num_workers; ++i) {
    threads[i].join();
  }

  // Create and output summary.
  Metadata final_metadata;
  uint64_t final_retries = 0;
  double final_duration = 0.0;
  for (int i = 0; i < FLAGS_num_workers; ++i) {
    final_metadata += worker_metadata[i];
    final_retries += worker_retries[i];
    final_duration += worker_duration[i];
  }

  int total_iterations = 0;
  std::for_each(worker_query_durations.begin(), worker_query_durations.end(),
                [&](const std::vector<double> &v) { total_iterations += v.size(); });

  final_duration /= FLAGS_num_workers;
  double execution_delta = time_limit.count() / final_duration;

  // This is adjusted throughput based on how much longer did workload execution time took.
  double throughput = (total_iterations / final_duration) * execution_delta;
  double raw_throughput = total_iterations / final_duration;

  nlohmann::json summary = nlohmann::json::object();
  summary["count"] = queries.size();
  summary["duration"] = final_duration;
  summary["time_limit"] = FLAGS_time_dependent_execution;
  summary["queries_executed"] = total_iterations;
  summary["throughput"] = throughput;
  summary["raw_throughput"] = raw_throughput;
  summary["latency_stats"] = LatencyStatistics(worker_query_durations);
  summary["retries"] = final_retries;
  summary["metadata"] = final_metadata.Export();
  summary["num_workers"] = FLAGS_num_workers;

  (*stream) << summary.dump() << std::endl;
}

void ExecuteWorkload(
    const std::vector<std::pair<std::string, std::map<std::string, memgraph::communication::bolt::Value>>> &queries,
    std::ostream *stream) {
  std::vector<std::thread> threads;
  threads.reserve(FLAGS_num_workers);

  auto total_time_start = std::chrono::steady_clock::now();

  std::vector<uint64_t> worker_retries(FLAGS_num_workers, 0);
  std::vector<Metadata> worker_metadata(FLAGS_num_workers, Metadata());
  std::vector<double> worker_duration(FLAGS_num_workers, 0.0);
  std::vector<std::vector<double>> worker_query_durations(FLAGS_num_workers);

  // Start workers and execute queries.
  auto size = queries.size();
  std::atomic<bool> run(false);
  std::atomic<uint64_t> ready(0);
  std::atomic<uint64_t> position(0);
  for (int worker = 0; worker < FLAGS_num_workers; ++worker) {
    threads.push_back(std::thread([&, worker]() {
      memgraph::io::network::Endpoint endpoint(FLAGS_address, FLAGS_port);
      memgraph::communication::ClientContext context(FLAGS_use_ssl);
      memgraph::communication::bolt::Client client(context);
      client.Connect(endpoint, FLAGS_username, FLAGS_password);

      ready.fetch_add(1, std::memory_order_acq_rel);
      while (!run.load(std::memory_order_acq_rel))
        ;

      auto &retries = worker_retries[worker];
      auto &metadata = worker_metadata[worker];
      auto &duration = worker_duration[worker];
      auto &query_duration = worker_query_durations[worker];

      memgraph::utils::Timer worker_timer;
      while (true) {
        auto pos = position.fetch_add(1, std::memory_order_acq_rel);
        if (pos >= size) break;
        const auto &query = queries[pos];
        memgraph::utils::Timer query_timer;
        auto ret = ExecuteNTimesTillSuccess(&client, query.first, query.second, FLAGS_max_retries);
        query_duration.emplace_back(query_timer.Elapsed().count());
        retries += ret.second;
        metadata.Append(ret.first);
      }
      duration = worker_timer.Elapsed().count();
      client.Close();
    }));
  }

  // Synchronize workers and collect runtime.
  while (ready.load(std::memory_order_acq_rel) < FLAGS_num_workers)
    ;
  run.store(true, std::memory_order_acq_rel);

  for (int i = 0; i < FLAGS_num_workers; ++i) {
    threads[i].join();
  }

  // Create and output summary.
  Metadata final_metadata;
  uint64_t final_retries = 0;
  double final_duration = 0.0;
  for (int i = 0; i < FLAGS_num_workers; ++i) {
    final_metadata += worker_metadata[i];
    final_retries += worker_retries[i];
    final_duration += worker_duration[i];
  }

  auto total_time_end = std::chrono::steady_clock::now();
  auto total_time = std::chrono::duration_cast<std::chrono::duration<double>>(total_time_end - total_time_start);

  final_duration /= FLAGS_num_workers;
  nlohmann::json summary = nlohmann::json::object();
  summary["total_time"] = total_time.count();
  summary["count"] = queries.size();
  summary["duration"] = final_duration;
  summary["throughput"] = static_cast<double>(queries.size()) / final_duration;
  summary["retries"] = final_retries;
  summary["metadata"] = final_metadata.Export();
  summary["num_workers"] = FLAGS_num_workers;
  summary["latency_stats"] = LatencyStatistics(worker_query_durations);
  (*stream) << summary.dump() << std::endl;
}

nlohmann::json BoltRecordsToJSONStrings(std::vector<std::vector<memgraph::communication::bolt::Value>> &results) {
  nlohmann::json res = nlohmann::json::object();
  std::ostringstream oss;
  for (int i = 0; i < results.size(); i++) {
    oss << results[i];
    res[std::to_string(i)] = oss.str();
  }
  return res;
}

/// Validation mode works on single thread with 1 query.
void ExecuteValidation(
    const std::vector<std::pair<std::string, std::map<std::string, memgraph::communication::bolt::Value>>> &queries,
    std::ostream *stream) {
  spdlog::info("Running validation mode, number of workers forced to 1");
  FLAGS_num_workers = 1;

  Metadata metadata = Metadata();
  double duration = 0.0;
  std::vector<std::vector<memgraph::communication::bolt::Value>> results;

  auto size = queries.size();

  memgraph::io::network::Endpoint endpoint(FLAGS_address, FLAGS_port);
  memgraph::communication::ClientContext context(FLAGS_use_ssl);
  memgraph::communication::bolt::Client client(context);
  client.Connect(endpoint, FLAGS_username, FLAGS_password);

  memgraph::utils::Timer timer;
  if (size == 1) {
    const auto &query = queries[0];
    auto ret = ExecuteValidationNTimesTillSuccess(&client, query.first, query.second, FLAGS_max_retries);
    metadata.Append(ret.first);
    results = ret.second;
    duration = timer.Elapsed().count();
    client.Close();
  } else {
    spdlog::info("Validation works with single query, pass just one query!");
  }

  nlohmann::json summary = nlohmann::json::object();
  summary["count"] = 1;
  summary["duration"] = duration;
  summary["metadata"] = metadata.Export();
  summary["results"] = BoltRecordsToJSONStrings(results);
  summary["num_workers"] = FLAGS_num_workers;

  (*stream) << summary.dump() << std::endl;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  spdlog::info("Running a bolt client with following settings:");
  spdlog::info("Address: {} ", FLAGS_address);
  spdlog::info("Port: {} ", FLAGS_port);
  spdlog::info("Username: {} ", FLAGS_username);
  spdlog::info("Password: {} ", FLAGS_password);
  spdlog::info("Usessl: {} ", FLAGS_use_ssl);
  spdlog::info("Num of worker: {}", FLAGS_num_workers);
  spdlog::info("Max retries: {}", FLAGS_max_retries);
  spdlog::info("Query JSON: {}", FLAGS_queries_json);
  spdlog::info("Input: {}", FLAGS_input);
  spdlog::info("Output: {}", FLAGS_output);
  spdlog::info("Validation: {}", FLAGS_validation);
  spdlog::info("Time dependent execution: {}", FLAGS_time_dependent_execution);

  memgraph::communication::SSLInit sslInit;

  std::ifstream ifile;
  std::istream *istream{&std::cin};
  if (FLAGS_input != "") {
    MG_ASSERT(std::filesystem::is_regular_file(FLAGS_input), "Input file isn't a regular file or it doesn't exist!");
    ifile.open(FLAGS_input);
    MG_ASSERT(ifile, "Couldn't open input file!");
    istream = &ifile;
  }

  std::ofstream ofile;
  std::ostream *ostream{&std::cout};
  if (FLAGS_output != "") {
    ofile.open(FLAGS_output);
    MG_ASSERT(ifile, "Couldn't open output file!");
    ostream = &ofile;
  }

  std::vector<std::pair<std::string, std::map<std::string, memgraph::communication::bolt::Value>>> queries;
  if (!FLAGS_queries_json) {
    // Load simple queries.
    std::string query;
    while (std::getline(*istream, query)) {
      auto trimmed = memgraph::utils::Trim(query);
      if (trimmed == "" || trimmed == ";") {
        ExecuteWorkload(queries, ostream);
        queries.clear();
        continue;
      }
      queries.emplace_back(query, std::map<std::string, memgraph::communication::bolt::Value>{});
    }
  } else {
    // Load advanced queries.
    std::string row;
    while (std::getline(*istream, row)) {
      auto data = nlohmann::json::parse(row);
      MG_ASSERT(data.is_array() && data.size() > 0,
                "The root item of the loaded JSON queries must be a non-empty "
                "array!");
      MG_ASSERT(data.is_array() && data.size() == 2, "Each item of the loaded JSON queries must be an array!");
      if (data.size() == 0) {
        ExecuteWorkload(queries, ostream);
        queries.clear();
        continue;
      }
      MG_ASSERT(data.size() == 2,
                "Each item of the loaded JSON queries that has "
                "data must be an array of length 2!");
      const auto &query = data[0];
      const auto &param = data[1];
      MG_ASSERT(query.is_string() && param.is_object(),
                "The query must be a string and the parameters must be a "
                "dictionary!");
      auto bolt_param = JsonToBoltValue(param);
      MG_ASSERT(bolt_param.IsMap(), "The Bolt parameters must be a map!");
      queries.emplace_back(query, std::move(bolt_param.ValueMap()));
    }
  }

  if (FLAGS_validation) {
    ExecuteValidation(queries, ostream);
  } else if (FLAGS_time_dependent_execution > 0) {
    ExecuteTimeDependentWorkload(queries, ostream);
  } else {
    ExecuteWorkload(queries, ostream);
  }

  return 0;
}
