// Copyright 2022 Memgraph Ltd.
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
#include <string>
#include <thread>
#include <vector>

#include <gflags/gflags.h>
#include <json/json.hpp>

#include "communication/bolt/client.hpp"
#include "communication/bolt/v1/value.hpp"
#include "communication/init.hpp"
#include "utils/exceptions.hpp"
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

void Execute(
    const std::vector<std::pair<std::string, std::map<std::string, memgraph::communication::bolt::Value>>> &queries,
    std::ostream *stream) {
  std::vector<std::thread> threads;
  threads.reserve(FLAGS_num_workers);

  std::vector<uint64_t> worker_retries(FLAGS_num_workers, 0);
  std::vector<Metadata> worker_metadata(FLAGS_num_workers, Metadata());
  std::vector<double> worker_duration(FLAGS_num_workers, 0.0);

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
      memgraph::utils::Timer timer;
      while (true) {
        auto pos = position.fetch_add(1, std::memory_order_acq_rel);
        if (pos >= size) break;
        const auto &query = queries[pos];
        auto ret = ExecuteNTimesTillSuccess(&client, query.first, query.second, FLAGS_max_retries);
        retries += ret.second;
        metadata.Append(ret.first);
      }
      duration = timer.Elapsed().count();
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
  final_duration /= FLAGS_num_workers;
  nlohmann::json summary = nlohmann::json::object();
  summary["count"] = queries.size();
  summary["duration"] = final_duration;
  summary["throughput"] = static_cast<double>(queries.size()) / final_duration;
  summary["retries"] = final_retries;
  summary["metadata"] = final_metadata.Export();
  summary["num_workers"] = FLAGS_num_workers;
  (*stream) << summary.dump() << std::endl;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

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
        Execute(queries, ostream);
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
        Execute(queries, ostream);
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
  Execute(queries, ostream);

  return 0;
}
