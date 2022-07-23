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

#include <limits>
#include <random>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>

#include "communication/bolt/client.hpp"
#include "io/network/endpoint.hpp"

DEFINE_string(address, "127.0.0.1", "Server address");
DEFINE_int32(port, 7687, "Server port");
DEFINE_string(username, "", "Username for the database");
DEFINE_string(password, "", "Password for the database");
DEFINE_bool(use_ssl, false, "Set to true to connect with SSL to the server.");
DEFINE_int32(client_count, 1, "The number of concurrent clients executing queries against the server.");
DEFINE_int32(per_client_query_count, 100, "The number of queries each client will try to execute.");

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::communication::SSLInit sslInit;

  spdlog::info("Cleaning the database instance...");
  memgraph::io::network::Endpoint endpoint(FLAGS_address, FLAGS_port);
  memgraph::communication::ClientContext context(FLAGS_use_ssl);
  memgraph::communication::bolt::Client client(&context);
  client.Connect(endpoint, FLAGS_username, FLAGS_password);
  client.Execute("MATCH (n) DETACH DELETE n", {});
  client.Close();

  spdlog::info(fmt::format("Starting parser stress test with {} clients and {} queries per client...",
                           FLAGS_client_count, FLAGS_per_client_query_count));
  std::vector<std::thread> threads;
  for (int i = 0; i < FLAGS_client_count; ++i) {
    threads.push_back(std::thread([]() {
      memgraph::io::network::Endpoint endpoint(FLAGS_address, FLAGS_port);
      memgraph::communication::ClientContext context(FLAGS_use_ssl);
      memgraph::communication::bolt::Client client(&context);
      client.Connect(endpoint, FLAGS_username, FLAGS_password);
      std::mt19937 generator{std::random_device{}()};
      std::uniform_int_distribution<uint64_t> distribution{std::numeric_limits<uint64_t>::min(),
                                                           std::numeric_limits<uint64_t>::max()};
      for (int i = 0; i < FLAGS_per_client_query_count; ++i) {
        try {
          client.Execute(fmt::format("MATCH (n:Label{}) RETURN n;", distribution(generator)), {});
        } catch (const memgraph::communication::bolt::ClientQueryException &e) {
          LOG_FATAL("One of the parser stress test queries failed.");
        }
      }
      client.Close();
    }));
  }

  for (int i = 0; i < FLAGS_client_count; ++i) {
    threads[i].join();
  }
  spdlog::info("All clients done. The parser managed to handle the load.");

  return 0;
}
