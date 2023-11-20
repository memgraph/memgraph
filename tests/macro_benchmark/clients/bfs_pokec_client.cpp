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

#include <array>
#include <chrono>
#include <fstream>
#include <iostream>
#include <queue>
#include <random>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <gflags/gflags.h>
#include <json/json.hpp>

#include "io/network/utils.hpp"
#include "utils/algorithm.hpp"
#include "utils/logging.hpp"
#include "utils/timer.hpp"

#include "long_running_common.hpp"

using memgraph::communication::bolt::Edge;
using memgraph::communication::bolt::Value;
using memgraph::communication::bolt::Vertex;

class BfsPokecClient : public TestClient {
 public:
  BfsPokecClient(int id, const std::string &db) : TestClient(), rg_(id), db_(db) {
    auto result = Execute("MATCH (n:User) RETURN count(1)", {}, "NumNodes");
    MG_ASSERT(result, "Read-only query should not fail");
    num_nodes_ = result->records[0][0].ValueInt();
  }

 private:
  std::mt19937 rg_;
  std::string db_;
  int num_nodes_;

  int RandomId() {
    std::uniform_int_distribution<int64_t> dist(1, num_nodes_);
    auto id = dist(rg_);
    return id;
  }

  void BfsWithDestinationNode() {
    auto start = RandomId();
    auto end = RandomId();
    while (start == end) {
      end = RandomId();
    }
    if (FLAGS_db == "memgraph") {
      auto result = Execute(
          "MATCH (n:User {id: $start}), (m:User {id: $end}), "
          "p = (n)-[*bfs..15]->(m) "
          "RETURN extract(n in nodes(p) | n.id) AS path",
          {{"start", start}, {"end", end}}, "Bfs");
      MG_ASSERT(result, "Read-only query should not fail!");
    } else if (FLAGS_db == "neo4j") {
      auto result = Execute(
          "MATCH p = shortestPath("
          "(n:User {id: $start})-[*..15]->(m:User {id: $end}))"
          "RETURN [x in nodes(p) | x.id] AS path;",
          {{"start", start}, {"end", end}}, "Bfs");
      MG_ASSERT(result, "Read-only query should not fail!");
    }
  }

  void BfsWithoutDestinationNode() {
    auto start = RandomId();
    if (FLAGS_db == "memgraph") {
      auto result = Execute(
          "MATCH p = (n:User {id: $start})-[*bfs..15]->(m:User) WHERE m != n "
          "RETURN extract(n in nodes(p) | n.id) AS path",
          {{"start", start}}, "Bfs");
      MG_ASSERT(result, "Read-only query should not fail!");
    } else {
      auto result = Execute(
          "MATCH p = shortestPath("
          "(n:User {id: $start})-[*..15]->(m:User)) WHERE m <> n "
          "RETURN [x in nodes(p) | x.id] AS path;",
          {{"start", start}}, "Bfs");
      MG_ASSERT(result, "Read-only query should not fail!");
    }
  }

 public:
  void Step() override {
    if (FLAGS_scenario == "with_destination_node") {
      BfsWithDestinationNode();
      return;
    }

    if (FLAGS_scenario == "without_destination_node") {
      BfsWithoutDestinationNode();
      return;
    }

    LOG_FATAL("Should not get here: unknown scenario!");
  }
};

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  Endpoint endpoint(FLAGS_address, FLAGS_port);
  ClientContext context(FLAGS_use_ssl);
  Client client(context);
  client.Connect(endpoint, FLAGS_username, FLAGS_password);

  std::vector<std::unique_ptr<TestClient>> clients;
  for (auto i = 0; i < FLAGS_num_workers; ++i) {
    clients.emplace_back(std::make_unique<BfsPokecClient>(i, "memgraph"));
  }

  RunMultithreadedTest(clients);
  return 0;
}
