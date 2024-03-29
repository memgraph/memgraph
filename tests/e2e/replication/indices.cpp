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
  google::SetUsageMessage("Memgraph E2E Replication Indices Test");
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  memgraph::logging::RedirectToStderr();

  auto database_endpoints = mg::e2e::replication::ParseDatabaseEndpoints(FLAGS_database_endpoints);

  mg::Client::Init();

  auto check_index = [](const auto &data, size_t i, const auto &label, const std::string &property = "") {
    const auto label_name = (*data)[i][1].ValueString();
    const auto p_type = (*data)[i][2].type();
    const auto property_name = p_type == mg::Value::Type::String ? (*data)[i][2].ValueString() : "";
    if (label_name != label || property_name != property) {
      LOG_FATAL("Invalid index.");
    }
  };

  auto check_delete_stats = [](const auto &data, size_t i, const auto &label, const std::string &property = "") {
    const auto label_name = (*data)[i][0].ValueString();
    const auto p_type = (*data)[i][1].type();
    const auto property_name = p_type == mg::Value::Type::String ? (*data)[i][1].ValueString() : "";
    if (label_name != label || property_name != property) {
      LOG_FATAL("Invalid stats.");
    }
  };

  {
    auto client = mg::e2e::replication::Connect(database_endpoints[0]);
    client->Execute("MATCH (n) DETACH DELETE n;");
    client->DiscardAll();
    client->Execute("CREATE INDEX ON :Node;");
    client->DiscardAll();
    client->Execute("CREATE INDEX ON :Node(id);");
    client->DiscardAll();
    client->Execute("CREATE INDEX ON :Node(id2);");
    client->DiscardAll();
    client->Execute("CREATE INDEX ON :Node2;");
    client->DiscardAll();

    // Sleep a bit so the constraints get replicated.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (const auto &database_endpoint : database_endpoints) {
      auto client = mg::e2e::replication::Connect(database_endpoint);
      client->Execute("SHOW INDEX INFO;");
      if (const auto data = client->FetchAll()) {
        if (data->size() != 4) {
          LOG_FATAL("Missing indices!");
        }
        check_index(data, 0, "Node");
        check_index(data, 1, "Node2");
        check_index(data, 2, "Node", "id");
        check_index(data, 3, "Node", "id2");
      } else {
        LOG_FATAL("Unable to get INDEX INFO from {}", database_endpoint);
      }
    }
    spdlog::info("All indices are in-place.");

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

    client->Execute("ANALYZE GRAPH;");
    client->DiscardAll();
  }

  {
    for (int i = 1; i < database_endpoints.size(); ++i) {
      auto &database_endpoint = database_endpoints[i];
      auto client = mg::e2e::replication::Connect(database_endpoint);
      try {
        // Hacky way to determine if the statistics were replicated
        client->Execute("ANALYZE GRAPH DELETE STATISTICS;");
        const auto data = client->FetchAll();
        if (data->size() != 4) {
          LOG_FATAL("Not all statistics were replicated! Failed endpoint {}:{}", database_endpoint.address,
                    database_endpoint.port);
        }
        check_delete_stats(data, 0, "Node");
        check_delete_stats(data, 1, "Node2");
        check_delete_stats(data, 2, "Node", "id");
        check_delete_stats(data, 3, "Node", "id2");
      } catch (const std::exception &e) {
        LOG_FATAL(e.what());
        break;
      }
    }
  }

  {
    auto client = mg::e2e::replication::Connect(database_endpoints[0]);
    client->Execute("ANALYZE GRAPH;");  // Resend stats
    client->DiscardAll();
    client->Execute("ANALYZE GRAPH DELETE STATISTICS;");
    client->DiscardAll();
    // Sleep a bit so the drop constraints get replicated.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (int i = 1; i < database_endpoints.size(); ++i) {
      auto &database_endpoint = database_endpoints[i];
      auto client = mg::e2e::replication::Connect(database_endpoints[i]);
      client->Execute("ANALYZE GRAPH DELETE STATISTICS;");
      if (const auto data = client->FetchAll()) {
        // Hacky way to determine if the statistics were replicated
        if (data->size() != 0) {
          LOG_FATAL("Not all statistics were replicated! Failed endpoint {}:{}", database_endpoint.address,
                    database_endpoint.port);
        }
      } else {
        LOG_FATAL("Unable to delete statistics from {}", database_endpoints[i]);
      }
    }
  }

  {
    auto client = mg::e2e::replication::Connect(database_endpoints[0]);
    client->Execute("DROP INDEX ON :Node;");
    client->DiscardAll();
    client->Execute("DROP INDEX ON :Node(id);");
    client->DiscardAll();
    client->Execute("DROP INDEX ON :Node(id2);");
    client->DiscardAll();
    client->Execute("DROP INDEX ON :Node2;");
    client->DiscardAll();

    // Sleep a bit so the constraints get replicated.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    for (const auto &database_endpoint : database_endpoints) {
      auto client = mg::e2e::replication::Connect(database_endpoint);
      client->Execute("SHOW INDEX INFO;");
      if (const auto data = client->FetchAll()) {
        if (!data->empty()) {
          LOG_FATAL("Undeleted indices!");
        }
      } else {
        LOG_FATAL("Unable to get INDEX INFO from {}", database_endpoint);
      }
    }
    spdlog::info("All indices have been deleted.");
  }

  return 0;
}
