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

#include <chrono>
#include <random>
#include <thread>
#include <unordered_set>

#include <fmt/format.h>
#include <gflags/gflags.h>

#include "common.hpp"
#include "io/network/fmt.hpp"
#include "utils/logging.hpp"
#include "utils/thread.hpp"
#include "utils/timer.hpp"

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E Replication Read-write Benchmark");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  auto database_endpoints = mg::e2e::replication::ParseDatabaseEndpoints(FLAGS_database_endpoints);

  mg::Client::Init();

  {
    auto client = mg::e2e::replication::Connect(database_endpoints[0]);
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
    client->Execute("CREATE INDEX ON :Node(id);");
    client->DiscardAll();
    client->Execute("CREATE CONSTRAINT ON (n:Node) ASSERT n.id IS UNIQUE;");
    client->DiscardAll();

    // Sleep a bit so the constraints get replicated.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (const auto &database_endpoint : database_endpoints) {
      auto client = mg::e2e::replication::Connect(database_endpoint);
      client->Execute("SHOW CONSTRAINT INFO;");
      if (const auto data = client->FetchAll()) {
        const auto label_name = (*data)[0][1].ValueString();
        const auto property_name = (*data)[0][2].ValueList()[0].ValueString();
        if (label_name != "Node" || property_name != "id") {
          LOG_FATAL("{} does NOT have a valid constraint created.", database_endpoint);
        }
      } else {
        LOG_FATAL("Unable to get CONSTRAINT INFO from {}", database_endpoint);
      }
    }
    spdlog::info("All constraints are in-place.");

    for (int i = 0; i < FLAGS_nodes; ++i) {
      client->Execute("CREATE (:Node {id:" + std::to_string(i) + "});");
      client->DiscardAll();
    }
    mg::e2e::replication::IntGenerator edge_generator("EdgeCreateGenerator", 0, FLAGS_nodes - 1);
    for (int i = 0; i < FLAGS_edges; ++i) {
      client->Execute("MATCH (n {id:" + std::to_string(edge_generator.Next()) +
                      "}), (m {id:" + std::to_string(edge_generator.Next()) + "}) CREATE (n)-[:Edge]->(m);");
      client->DiscardAll();
    }
  }

  {
    const int num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    std::vector<double> thread_duration;
    threads.reserve(num_threads);
    thread_duration.resize(num_threads);

    for (int i = 0; i < num_threads; ++i) {
      const auto &database_endpoint = database_endpoints[i % database_endpoints.size()];
      threads.emplace_back(
          [i, &database_endpoint, cluster_size = database_endpoints.size(), &local_duration = thread_duration[i]] {
            auto client = mg::e2e::replication::Connect(database_endpoint);
            mg::e2e::replication::IntGenerator node_update_generator(fmt::format("NodeUpdateGenerator {}", i), 0,
                                                                     FLAGS_nodes - 1);
            memgraph::utils::Timer t;

            while (true) {
              local_duration = t.Elapsed().count();
              if (local_duration >= FLAGS_reads_duration_limit) break;
              // In the main case try to update.
              if (i % cluster_size == 0) {
                try {
                  client->Execute("MATCH (n:Node {id:" + std::to_string(node_update_generator.Next()) +
                                  "}) SET n.id = " + std::to_string(node_update_generator.Next()) + " RETURN n.id;");
                  client->FetchAll();
                } catch (const std::exception &e) {
                  // Pass.
                }
              } else {  // In the replica case fetch all unique ids.
                try {
                  client->Execute("MATCH (n) RETURN n.id;");
                  const auto data = client->FetchAll();
                  std::unordered_set<int64_t> unique;
                  for (const auto &value : *data) {
                    unique.insert(value[0].ValueInt());
                  }
                  if ((*data).size() != unique.size()) {
                    LOG_FATAL("Some ids are equal.");
                  }
                } catch (const std::exception &e) {
                  LOG_FATAL(e.what());
                  break;
                }
              }
            }
          });
    }

    for (auto &t : threads) {
      if (t.joinable()) t.join();
    }
  }

  {
    auto client = mg::e2e::replication::Connect(database_endpoints[0]);
    client->Execute("DROP CONSTRAINT ON (n:Node) ASSERT n.id IS UNIQUE");
    client->DiscardAll();
    // Sleep a bit so the drop constraints get replicated.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (const auto &database_endpoint : database_endpoints) {
      auto client = mg::e2e::replication::Connect(database_endpoint);
      client->Execute("SHOW CONSTRAINT INFO;");
      if (const auto data = client->FetchAll()) {
        if (!(*data).empty()) {
          LOG_FATAL("{} still have some constraints.", database_endpoint);
        }
      } else {
        LOG_FATAL("Unable to get CONSTRAINT INFO from {}", database_endpoint);
      }
    }
    spdlog::info("All constraints were deleted.");
  }

  return 0;
}
