// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <limits>
#include <random>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"
#include "mgclient.hpp"
#include "utils/timer.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");
DEFINE_int32(worker_count, 1, "The number of concurrent workers executing queries against the server.");
DEFINE_int32(per_worker_query_count, 100, "The number of queries each worker will try to execute.");
DEFINE_string(isolation_level, "", "Database isolation level.");
DEFINE_string(storage_mode, "", "Database storage_mode.");

auto make_client() {
  mg::Client::Params params;
  params.host = FLAGS_address;
  params.port = static_cast<uint16_t>(FLAGS_port);
  params.use_ssl = FLAGS_use_ssl;
  return mg::Client::Connect(params);
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  mg::Client::Init();

  spdlog::info("Cleaning the database instance...");
  auto client = make_client();
  client->Execute("MATCH (n) DETACH DELETE n");
  client->DiscardAll();

  spdlog::info(fmt::format("Starting parser stress test with {} workers and {} queries per worker...",
                           FLAGS_worker_count, FLAGS_per_worker_query_count));
  std::vector<std::thread> threads;
  memgraph::utils::Timer timer;
  for (int i = 0; i < FLAGS_worker_count; ++i) {
    threads.push_back(std::thread([]() {
      auto client = make_client();
      std::mt19937 generator{std::random_device{}()};
      std::uniform_int_distribution<uint64_t> distribution{std::numeric_limits<uint64_t>::min(),
                                                           std::numeric_limits<uint64_t>::max()};
      for (int i = 0; i < FLAGS_per_worker_query_count; ++i) {
        try {
          auto is_executed = client->Execute(fmt::format("MATCH (n:Label{}) RETURN n;", distribution(generator)));
          if (!is_executed) {
            LOG_FATAL("One of the parser stress test queries failed.");
          }
          client->FetchAll();
        } catch (const std::exception &e) {
          LOG_FATAL("One of the parser stress test queries failed.");
        }
      }
    }));
  }

  std::ranges::for_each(threads, [](auto &t) { t.join(); });
  spdlog::info(
      fmt::format("All queries executed in {:.4f}s. The parser managed to handle the load.", timer.Elapsed().count()));
  mg::Client::Finalize();

  return 0;
}
