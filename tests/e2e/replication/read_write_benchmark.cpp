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
#include <fstream>
#include <random>
#include <thread>

#include <fmt/format.h>
#include <gflags/gflags.h>
#include <json/json.hpp>

#include "common.hpp"
#include "utils/logging.hpp"
#include "utils/thread.hpp"
#include "utils/timer.hpp"

DEFINE_string(output_file, "memgraph__e2e__replication__read_write_benchmark.json",
              "Output file where the results should be in JSON format.");

int main(int argc, char **argv) {
  google::SetUsageMessage("Memgraph E2E Replication Read-write Benchmark");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  const auto database_endpoints = mg::e2e::replication::ParseDatabaseEndpoints(FLAGS_database_endpoints);
  nlohmann::json output;
  output["nodes"] = FLAGS_nodes;
  output["edges"] = FLAGS_edges;

  mg::Client::Init();

  {
    auto client = mg::e2e::replication::Connect(database_endpoints[0]);
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
    client->Execute("CREATE INDEX ON :Node(id);");
    client->DiscardAll();

    // Sleep a bit so the index get replicated.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (const auto &database_endpoint : database_endpoints) {
      auto client = mg::e2e::replication::Connect(database_endpoint);
      client->Execute("SHOW INDEX INFO;");
      if (auto data = client->FetchAll()) {
        auto label_name = (*data)[0][1].ValueString();
        auto property_name = (*data)[0][2].ValueString();
        if (label_name != "Node" || property_name != "id") {
          LOG_FATAL("{} does NOT have valid indexes created.", database_endpoint.SocketAddress());
        }
      } else {
        LOG_FATAL("Unable to get INDEX INFO from {}", database_endpoint.SocketAddress());
      }
    }
    spdlog::info("All indexes are in-place.");

    memgraph::utils::Timer node_write_timer;
    for (int i = 0; i < FLAGS_nodes; ++i) {
      client->Execute("CREATE (:Node {id:" + std::to_string(i) + "});");
      client->DiscardAll();
    }
    output["node_write_time"] = node_write_timer.Elapsed().count();

    mg::e2e::replication::IntGenerator edge_generator("EdgeCreateGenerator", 0, FLAGS_nodes - 1);
    memgraph::utils::Timer edge_write_timer;
    for (int i = 0; i < FLAGS_edges; ++i) {
      client->Execute("MATCH (n {id:" + std::to_string(edge_generator.Next()) +
                      "}), (m {id:" + std::to_string(edge_generator.Next()) + "}) CREATE (n)-[:Edge]->(m);");
      client->DiscardAll();
    }
    output["edge_write_time"] = node_write_timer.Elapsed().count();
  }

  {  // Benchmark read queries.
    const int num_threads = std::thread::hardware_concurrency();
    std::atomic<int64_t> query_counter{0};
    std::vector<std::thread> threads;
    std::vector<double> thread_duration;
    threads.reserve(num_threads);
    thread_duration.resize(num_threads);

    for (int i = 0; i < num_threads; ++i) {
      const auto &database_endpoint = database_endpoints[i % database_endpoints.size()];
      threads.emplace_back([i, &database_endpoint, &query_counter, &local_duration = thread_duration[i]] {
        memgraph::utils::ThreadSetName(fmt::format("BenchWriter{}", i));
        auto client = mg::e2e::replication::Connect(database_endpoint);
        mg::e2e::replication::IntGenerator node_generator(fmt::format("NodeReadGenerator {}", i), 0, FLAGS_nodes - 1);
        memgraph::utils::Timer t;

        while (true) {
          local_duration = t.Elapsed().count();
          if (local_duration >= FLAGS_reads_duration_limit) break;
          try {
            client->Execute("MATCH (n {id:" + std::to_string(node_generator.Next()) + "})-[e]->(m) RETURN e, m;");
            client->DiscardAll();
            query_counter.fetch_add(1);
          } catch (const std::exception &e) {
            LOG_FATAL(e.what());
            break;
          }
        }
      });
    }

    for (auto &t : threads) {
      if (t.joinable()) t.join();
    }

    double all_duration = 0;
    for (auto &d : thread_duration) all_duration += d;
    output["total_read_duration"] = all_duration;
    double per_thread_read_duration = all_duration / num_threads;
    output["per_thread_read_duration"] = per_thread_read_duration;
    output["read_per_second"] = query_counter / per_thread_read_duration;
    output["read_queries"] = query_counter.load();

    spdlog::info("Output data: {}", output.dump());
    std::ofstream output_file(FLAGS_output_file);
    output_file << output.dump();
    output_file.close();
  }

  {
    auto client = mg::e2e::replication::Connect(database_endpoints[0]);
    client->Execute("DROP INDEX ON :Node(id);");
    client->DiscardAll();
    // Sleep a bit so the drop index get replicated.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (const auto &database_endpoint : database_endpoints) {
      auto client = mg::e2e::replication::Connect(database_endpoint);
      client->Execute("SHOW INDEX INFO;");
      if (const auto data = client->FetchAll()) {
        if (!(*data).empty()) {
          LOG_FATAL("{} still have some indexes.", database_endpoint.SocketAddress());
        }
      } else {
        LOG_FATAL("Unable to get INDEX INFO from {}", database_endpoint.SocketAddress());
      }
    }
    spdlog::info("All indexes were deleted.");
  }

  mg::Client::Finalize();

  return 0;
}
