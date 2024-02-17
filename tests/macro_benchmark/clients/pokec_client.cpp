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

// TODO: work in progress.
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

#include "communication/bolt/v1/value.hpp"
#include "io/network/utils.hpp"
#include "utils/algorithm.hpp"
#include "utils/timer.hpp"

#include "communication/bolt/v1/fmt.hpp"
#include "long_running_common.hpp"

using memgraph::communication::bolt::Edge;
using memgraph::communication::bolt::Value;
using memgraph::communication::bolt::Vertex;

struct VertexAndEdges {
  Vertex vertex;
  std::vector<Edge> edges;
  std::vector<Vertex> vertices;
};

const std::string INDEPENDENT_LABEL = "User";

class PokecClient : public TestClient {
 public:
  PokecClient(int id, std::vector<int64_t> to_remove, nlohmann::json config)
      : TestClient(), rg_(id), config_(config), to_remove_(to_remove) {}

 private:
  std::mt19937 rg_;
  nlohmann::json config_;
  std::vector<int64_t> to_remove_;
  std::vector<VertexAndEdges> removed_;

  auto MatchVertex(const std::string &label, int64_t id) {
    return Execute(fmt::format("MATCH (n :{} {{id : $id}}) RETURN n", label), {{"id", id}});
  }

  auto MatchNeighbours(const std::string &label, int64_t id) {
    return Execute(fmt::format("MATCH (n :{} {{id : $id}})-[e]-(m) RETURN n, e, m", label), {{"id", id}});
  }

  auto DetachDeleteVertex(const std::string &label, int64_t id) {
    return Execute(fmt::format("MATCH (n :{} {{id : $id}}) DETACH DELETE n", label), {{"id", id}});
  }

  auto CreateVertex(const Vertex &vertex) {
    std::stringstream os;
    os << "CREATE (n :";
    memgraph::utils::PrintIterable(os, vertex.labels, ":");
    os << " {";
    memgraph::utils::PrintIterable(os, vertex.properties, ", ", [&](auto &stream, const auto &pair) {
      if (pair.second.type() == Value::Type::String) {
        stream << pair.first << ": \"" << pair.second << "\"";
      } else {
        stream << pair.first << ": " << pair.second;
      }
    });
    os << "})";
    return Execute(os.str(), {}, "CREATE (n :labels... {...})");
  }

  auto GetAverageAge2(int64_t id) {
    return Execute(
        "MATCH (n :User {id: $id})-[]-(m) "
        "RETURN AVG(n.age + m.age)",
        {{"id", id}});
  }

  auto GetAverageAge3(int64_t id) {
    return Execute(
        "MATCH (n :User {id: $id})-[]-(m)-[]-(k) "
        "RETURN AVG(n.age + m.age + k.age)",
        {{"id", id}});
  }

  auto CreateEdge(const Vertex &from, const std::string &from_label, int64_t from_id, const std::string &to_label,
                  int64_t to_id, const Edge &edge) {
    std::stringstream os;
    os << fmt::format("MATCH (n :{} {{id : {}}}) ", from_label, from_id);
    os << fmt::format("MATCH (m :{} {{id : {}}}) ", to_label, to_id);
    os << "CREATE (n)";
    if (edge.to == from.id) {
      os << "<-";
    } else {
      os << "-";
    }
    os << "[:" << edge.type << " {";
    memgraph::utils::PrintIterable(os, edge.properties, ", ", [&](auto &stream, const auto &pair) {
      if (pair.second.type() == Value::Type::String) {
        stream << pair.first << ": \"" << pair.second << "\"";
      } else {
        stream << pair.first << ": " << pair.second;
      }
    });
    os << "}]";
    if (edge.from == from.id) {
      os << "->";
    } else {
      os << "-";
    }
    os << "(m) ";
    os << "RETURN n.id";
    auto ret = Execute(os.str(), {},
                       "MATCH (n :label {id: ...}) MATCH (m :label {id: ...}) "
                       "CREATE (n)-[:type ...]-(m)");
    MG_ASSERT(ret->records.size() == 1U, "from_id: {} to_id: {} ret.records.size(): {}", from_id, to_id,
              ret->records.size());
    return ret;
  }

  VertexAndEdges RetrieveAndDeleteVertex(const std::string &label, int64_t id) {
    auto vertex_record = MatchVertex(label, id)->records;

    MG_ASSERT(vertex_record.size() == 1U, "id: {} vertex_record.size(): {}", id, vertex_record.size());

    auto records = MatchNeighbours(label, id)->records;

    DetachDeleteVertex(label, id);

    std::vector<Edge> edges;
    edges.reserve(records.size());
    for (const auto &record : records) {
      edges.push_back(record[1].ValueEdge());
    }

    std::vector<Vertex> vertices;
    vertices.reserve(records.size());
    for (const auto &record : records) {
      vertices.push_back(record[2].ValueVertex());
    }

    return {vertex_record[0][0].ValueVertex(), edges, vertices};
  }

  void ReturnVertexAndEdges(const VertexAndEdges &vertex_and_edges, const std::string &label) {
    int num_queries = 0;
    CreateVertex(vertex_and_edges.vertex);
    ++num_queries;

    for (int i = 0; i < static_cast<int>(vertex_and_edges.vertices.size()); ++i) {
      auto records =
          CreateEdge(vertex_and_edges.vertex, label, vertex_and_edges.vertex.properties.at("id").ValueInt(), label,
                     vertex_and_edges.vertices[i].properties.at("id").ValueInt(), vertex_and_edges.edges[i])
              ->records;
      MG_ASSERT(records.size() == 1U, "Graph in invalid state {}", vertex_and_edges.vertex.properties.at("id"));
      ++num_queries;
    }
  }

 public:
  virtual void Step() override {
    std::uniform_real_distribution<> real_dist(0.0, 1.0);
    if (real_dist(rg_) < config_["read_probability"]) {
      std::uniform_int_distribution<> read_query_dist(0, 1);
      int id = real_dist(rg_) * to_remove_.size();
      switch (read_query_dist(rg_)) {
        case 0:
          GetAverageAge2(id);
          break;
        case 1:
          GetAverageAge3(id);
          break;
        default:
          LOG_FATAL("Should not get here");
      }
    } else {
      auto remove_random = [&](auto &v) {
        MG_ASSERT(v.size());
        std::uniform_int_distribution<> int_dist(0, v.size() - 1);
        std::swap(v.back(), v[int_dist(rg_)]);
        auto ret = v.back();
        v.pop_back();
        return ret;
      };
      if (real_dist(rg_) < static_cast<double>(removed_.size()) / (removed_.size() + to_remove_.size())) {
        auto vertices_and_edges = remove_random(removed_);
        ReturnVertexAndEdges(vertices_and_edges, INDEPENDENT_LABEL);
        to_remove_.push_back(vertices_and_edges.vertex.properties["id"].ValueInt());
      } else {
        auto node_id = remove_random(to_remove_);
        auto ret = RetrieveAndDeleteVertex(INDEPENDENT_LABEL, node_id);
        removed_.push_back(ret);
      }
    }
  }
};

int64_t NumNodes(Client &client, const std::string &label) {
  auto result = ExecuteNTimesTillSuccess(client, "MATCH (n :" + label + ") RETURN COUNT(n) as cnt", {}, MAX_RETRIES);
  return result.first.records[0][0].ValueInt();
}

std::vector<int64_t> Neighbours(Client &client, const std::string &label, int64_t id) {
  auto result = ExecuteNTimesTillSuccess(
      client, "MATCH (n :" + label + " {id: " + std::to_string(id) + "})-[e]-(m) RETURN m.id", {}, MAX_RETRIES);
  std::vector<int64_t> ret;
  for (const auto &record : result.first.records) {
    ret.push_back(record[0].ValueInt());
  }
  return ret;
}

std::vector<int64_t> IndependentSet(Client &client, const std::string &label) {
  const int64_t num_nodes = NumNodes(client, label);
  std::vector<int64_t> independent_nodes_ids;
  std::vector<int64_t> ids;
  std::unordered_set<int64_t> independent;
  for (int64_t i = 1; i <= num_nodes; ++i) {
    ids.push_back(i);
    independent.insert(i);
  }
  {
    std::mt19937 mt;
    std::shuffle(ids.begin(), ids.end(), mt);
  }

  for (auto i : ids) {
    if (independent.find(i) == independent.end()) continue;
    independent.erase(i);
    std::vector<int64_t> neighbour_ids = Neighbours(client, label, i);
    independent_nodes_ids.push_back(i);
    for (auto j : neighbour_ids) {
      independent.erase(j);
    }
  }
  spdlog::info("Number of nodes: {}\nNumber of independent nodes: {}", num_nodes, independent_nodes_ids.size());

  return independent_nodes_ids;
}

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  memgraph::communication::SSLInit sslInit;

  nlohmann::json config;
  std::cin >> config;

  auto independent_nodes_ids = [&] {
    Endpoint endpoint(memgraph::io::network::ResolveHostname(FLAGS_address), FLAGS_port);
    ClientContext context(FLAGS_use_ssl);
    Client client(context);
    client.Connect(endpoint, FLAGS_username, FLAGS_password);
    return IndependentSet(client, INDEPENDENT_LABEL);
  }();

  int64_t next_to_assign = 0;
  std::vector<std::unique_ptr<TestClient>> clients;
  clients.reserve(FLAGS_num_workers);

  for (int i = 0; i < FLAGS_num_workers; ++i) {
    int64_t size = independent_nodes_ids.size();
    int64_t next_next_to_assign = next_to_assign + size / FLAGS_num_workers + (i < size % FLAGS_num_workers);

    std::vector<int64_t> to_remove(independent_nodes_ids.begin() + next_to_assign,
                                   independent_nodes_ids.begin() + next_next_to_assign);
    spdlog::info("{} {}", next_to_assign, next_next_to_assign);
    next_to_assign = next_next_to_assign;

    clients.emplace_back(std::make_unique<PokecClient>(i, to_remove, config));
  }

  RunMultithreadedTest(clients);

  return 0;
}
