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

#include <atomic>
#include <fstream>
#include <limits>
#include <random>
#include <set>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "utils/exceptions.hpp"
#include "utils/timer.hpp"

using EndpointT = memgraph::io::network::Endpoint;
using ClientContextT = memgraph::communication::ClientContext;
using ClientT = memgraph::communication::bolt::Client;
using ValueT = memgraph::communication::bolt::Value;
using QueryDataT = memgraph::communication::bolt::QueryData;
using ExceptionT = memgraph::communication::bolt::ClientQueryException;

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");
DEFINE_int32(worker_count, 1, "The number of workers that operate on the graph independently");
DEFINE_string(stats_file, "", "File into which to write statistics.");

class GraphSession {
 public:
  GraphSession(int id)
      : id_(id),
        generator_{std::random_device{}()},
        uni_gen_{std::random_device{}()},
        uni_dis_{std::numeric_limits<unsigned long long>::min(), std::numeric_limits<unsigned long long>::max()} {
    EndpointT endpoint(FLAGS_address, FLAGS_port);
    client_ = std::make_unique<ClientT>(&context_);
    client_->Connect(endpoint, FLAGS_username, FLAGS_password);
  }

 private:
  uint64_t id_;
  ClientContextT context_{FLAGS_use_ssl};
  std::unique_ptr<ClientT> client_;
  uint64_t executed_queries_{0};
  std::map<std::string, uint64_t> query_failures_;
  std::mt19937 generator_;
  memgraph::utils::Timer timer_;

  std::mt19937 uni_gen_;
  std::uniform_int_distribution<unsigned long long> uni_dis_;

 private:
  double GetRandom() { return std::generate_canonical<double, 10>(generator_); }

  bool Bernoulli(double p) { return GetRandom() < p; }

  uint64_t RandomElement(std::set<uint64_t> &data) {
    uint64_t min = *data.begin(), max = *data.rbegin();
    uint64_t val = std::floor(GetRandom() * (max - min) + min);
    auto it = data.lower_bound(val);
    return *it;
  }

  std::string RandomElement(std::set<std::string> &data) {
    uint64_t pos = std::floor(GetRandom() * data.size());
    auto it = data.begin();
    std::advance(it, pos);
    return *it;
  }

  void AddQueryFailure(const std::string &what) {
    auto it = query_failures_.find(what);
    if (it != query_failures_.end()) {
      ++it->second;
    } else {
      query_failures_.insert(std::make_pair(what, 1));
    }
  }

  QueryDataT ExecuteWithoutCatch(const std::string &query) {
    SPDLOG_INFO("Runner {} executing query: {}", id_, query);
    executed_queries_ += 1;
    return client_->Execute(query, {});
  }

  QueryDataT Execute(const std::string &query) {
    try {
      return ExecuteWithoutCatch(query);
    } catch (const ExceptionT &e) {
      AddQueryFailure(e.what());
      return QueryDataT();
    }
  }

 public:
  void Run() {
    // TODO(gitbuda): Implement
    for (int i = 0; i < 200000; ++i) {
      client_->Execute(fmt::format("MATCH (n:Label{}) RETURN n;", uni_dis_(uni_gen_)), {});
    }
  }

  uint64_t GetExecutedQueries() { return executed_queries_; }

  uint64_t GetFailedQueries() {
    uint64_t failed = 0;
    for (const auto &item : query_failures_) {
      failed += item.second;
    }
    return failed;
  }
};

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::communication::SSLInit sslInit;

  spdlog::info("Starting Memgraph parser stress test");

  // Create client cna clean database instance.
  EndpointT endpoint(FLAGS_address, FLAGS_port);
  ClientContextT context(FLAGS_use_ssl);
  ClientT client(&context);
  client.Connect(endpoint, FLAGS_username, FLAGS_password);
  client.Execute("MATCH (n) DETACH DELETE n", {});
  client.Close();

  // Initialize sessions and start workers and join at the end.
  std::vector<GraphSession> sessions;
  sessions.reserve(FLAGS_worker_count);
  for (int i = 0; i < FLAGS_worker_count; ++i) {
    sessions.emplace_back(i);
  }
  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_worker_count; ++i) {
    threads.push_back(std::thread([&, i]() { sessions[i].Run(); }));
  }
  for (int i = 0; i < FLAGS_worker_count; ++i) {
    threads[i].join();
  }

  if (FLAGS_stats_file != "") {
    uint64_t executed = 0, failed = 0;
    for (int i = 0; i < FLAGS_worker_count; ++i) {
      executed += sessions[i].GetExecutedQueries();
      failed += sessions[i].GetFailedQueries();
    }
    std::ofstream stream(FLAGS_stats_file);
    stream << executed << std::endl << failed << std::endl;
    spdlog::info("Written statistics to file: {}", FLAGS_stats_file);
  }

  spdlog::info("All query runners done");

  return 0;
}
