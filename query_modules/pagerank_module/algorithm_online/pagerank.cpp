// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "pagerank.hpp"

#include <random>
#include <unordered_map>

#include <mg_graph.hpp>

namespace pagerank_online_alg {
namespace {

class PageRankData {
  ///
  ///@brief Context for storing data for dynamic pagerank
  ///
  ///
 public:
  void Init() {
    walks.clear();
    walks_counter.clear();
    walks_table.clear();
  }

  bool IsEmpty() const { return walks.empty(); }

  /// Keeping the information about walks on the graph
  std::vector<std::vector<std::uint64_t>> walks;

  // Keeping the information of walk appearance in algorithm for faster calculation
  std::unordered_map<std::uint64_t, uint64_t> walks_counter;

  /// Table that keeps the node appearance and walk ID
  std::unordered_map<std::uint64_t, std::unordered_set<std::uint64_t>> walks_table;
};

// Dynamic global context
// This is unsafe in the multithreaded environment, the workaround would be building a thread-safe dynamic storage
// implementation
PageRankData context;
std::uint64_t global_R;
double global_epsilon;

int GetRandInt(int from, int to) {
  static std::minstd_rand eng{std::random_device{}()};
  std::uniform_int_distribution<int> dist{from, to - 1};
  return dist(eng);
}

float GetRandFloat() {
  static std::minstd_rand eng{std::random_device{}()};
  static std::uniform_real_distribution<float> dist{};
  return dist(eng);
}

///
///@brief Function for vector normalization
///
///@param rank Vector that needs to be normalized
///
void NormalizeRank(std::vector<std::pair<std::uint64_t, double>> &rank) {
  const double sum =
      std::accumulate(rank.begin(), rank.end(), 0.0, [](auto sum, const auto &p) { return sum + p.second; });
  for (auto &[node_id, value] : rank) {
    value /= sum;
  }
}

///
///@brief Calculates pagerank based on current information stored in global context
///
///@return std::vector<std::pair<std::uint64_t, double>>
///
std::vector<std::pair<std::uint64_t, double>> CalculatePageRank() {
  std::vector<std::pair<std::uint64_t, double>> pageranks;

  auto R = pagerank_online_alg::global_R;
  auto eps = pagerank_online_alg::global_epsilon;

  auto n = pagerank_online_alg::context.walks_counter.size();
  pageranks.reserve(n);
  for (auto const [node_id, total] : pagerank_online_alg::context.walks_counter) {
    auto rank = total / ((n * R) / eps);
    pageranks.emplace_back(node_id, rank);
  }

  NormalizeRank(pageranks);
  return pageranks;
}

///
///@brief Creates a route starting from start_id, stores it in walk and updates the walk_index. Route is created via
/// random walk depending on random number generator.
///
///@param graph Graph for route creation
///@param start_id Starting node in graph creation
///@param walk Walk vector that stores a route
///@param walk_index Index of a walk for context storing
///@param epsilon Probability of stopping the route creation
///
void CreateRoute(const mg_graph::GraphView<> &graph, std::uint64_t start_id, std::vector<std::uint64_t> &walk,
                 std::uint64_t walk_index, double epsilon) {
  std::uint64_t current_id = start_id;
  while (true) {
    auto neighbors = graph.Neighbours(current_id);
    if (neighbors.empty()) break;

    // Pick and add the random outer edge
    auto number_of_neighbors = neighbors.size();
    auto next_id = neighbors[GetRandInt(0, number_of_neighbors)].node_id;
    next_id = graph.GetMemgraphNodeId(next_id);

    walk.emplace_back(next_id);
    pagerank_online_alg::context.walks_table[next_id].insert(walk_index);
    pagerank_online_alg::context.walks_counter[next_id]++;

    // Finish walk when random number is smaller than epsilon
    // Average length of walk is 1/epsilon
    if (GetRandFloat() < epsilon) {
      break;
    }

    current_id = graph.GetInnerNodeId(next_id);
  }
}

///
///@brief Updates the context based on new edge addition. Reverts previous walks made from starting node and updates
/// them.
///
///@param graph Graph for updating
///@param new_edge New edge
///
void UpdateCreate(const mg_graph::GraphView<> &graph, const std::pair<std::uint64_t, std::uint64_t> &new_edge) {
  auto [from, to] = new_edge;

  std::unordered_set<std::uint64_t> walk_table_copy(pagerank_online_alg::context.walks_table[from]);
  for (auto walk_index : walk_table_copy) {
    auto &walk = pagerank_online_alg::context.walks[walk_index];

    auto position = std::find(walk.begin(), walk.end(), from) + 1;
    while (position != walk.end()) {
      auto node_id = *position;
      pagerank_online_alg::context.walks_table[node_id].erase(walk_index);
      pagerank_online_alg::context.walks_counter[node_id]--;
      position++;
    }
    walk.erase(std::find(walk.begin(), walk.end(), from) + 1, walk.end());

    auto current_id = from;
    auto half_eps = pagerank_online_alg::global_epsilon / 2.0;
    CreateRoute(graph, graph.GetInnerNodeId(current_id), walk, walk_index, half_eps);
  }
}

///
///@brief Updates the context based on adding the new vertex. This means adding it to a context tables and creating
/// walks from it.
///
///@param graph Graph for updating
///@param new_vertex New vertex
///
void UpdateCreate(const mg_graph::GraphView<> &graph, std::uint64_t new_vertex) {
  auto R = pagerank_online_alg::global_R;
  auto eps = pagerank_online_alg::global_epsilon;

  auto walk_index = pagerank_online_alg::context.walks.size();
  for (std::uint64_t i = 0; i < R; ++i) {
    std::vector<std::uint64_t> walk{new_vertex};

    pagerank_online_alg::context.walks_table[new_vertex].insert(walk_index);
    pagerank_online_alg::context.walks_counter[new_vertex]++;

    CreateRoute(graph, graph.GetInnerNodeId(new_vertex), walk, walk_index, eps);

    pagerank_online_alg::context.walks.emplace_back(std::move(walk));
    walk_index++;
  }
}

///
///@brief Removes the edge from the context and updates walks. This method works by updating walks that contain starting
/// node because they no longer exist.
///
///@param graph Graph for updating
///@param removed_edge Deleted edge
///
void UpdateDelete(const mg_graph::GraphView<> &graph, const std::pair<std::uint64_t, std::uint64_t> &removed_edge) {
  auto [from, to] = removed_edge;

  std::unordered_set<std::uint64_t> walk_table_copy(pagerank_online_alg::context.walks_table[from]);
  for (auto walk_index : walk_table_copy) {
    auto &walk = pagerank_online_alg::context.walks[walk_index];

    auto position = std::find(walk.begin(), walk.end(), from) + 1;

    if (position == walk.end()) {
      continue;
    }

    while (position != walk.end()) {
      auto node_id = *position;
      pagerank_online_alg::context.walks_table[node_id].erase(walk_index);
      pagerank_online_alg::context.walks_counter[node_id]--;
      position++;
    }
    walk.erase(std::find(walk.begin(), walk.end(), from) + 1, walk.end());

    auto current_id = from;

    // Skip creating routes if node does not exist anymore
    if (!graph.NodeExists(current_id)) {
      continue;
    }

    auto half_eps = pagerank_online_alg::global_epsilon / 2.0;
    CreateRoute(graph, graph.GetInnerNodeId(current_id), walk, walk_index, half_eps);
  }
}

///
///@brief Deletes vertex from context. This is trivial because we are sure that no edge exists around that node.
///
///@param graph Graph for updating
///@param removed_vertex Removed vertex
///
void UpdateDelete(const mg_graph::GraphView<> &graph, std::uint64_t removed_vertex) {
  pagerank_online_alg::context.walks_table.erase(removed_vertex);
  pagerank_online_alg::context.walks_counter.erase(removed_vertex);
}

bool IsInconsistent(const mg_graph::GraphView<> &graph) {
  for (auto const [node_id] : graph.Nodes()) {
    auto external_id = graph.GetMemgraphNodeId(node_id);
    if (pagerank_online_alg::context.walks_counter.find(external_id) ==
        pagerank_online_alg::context.walks_counter.end()) {
      return true;
    }
  }
  return false;
}
}  // namespace

std::vector<std::pair<std::uint64_t, double>> SetPagerank(const mg_graph::GraphView<> &graph, std::uint64_t R,
                                                          double epsilon) {
  pagerank_online_alg::global_R = R;
  pagerank_online_alg::global_epsilon = epsilon;
  pagerank_online_alg::context.Init();

  auto walk_index = 0;
  for (auto [node_id] : graph.Nodes()) {
    // We have R random walks for each node in the graph
    for (std::uint64_t i = 0; i < R; i++) {
      std::vector<std::uint64_t> walk;

      auto current_id = graph.GetMemgraphNodeId(node_id);
      walk.emplace_back(current_id);
      pagerank_online_alg::context.walks_table[current_id].insert(walk_index);
      pagerank_online_alg::context.walks_counter[current_id]++;

      CreateRoute(graph, graph.GetInnerNodeId(current_id), walk, walk_index, epsilon);

      pagerank_online_alg::context.walks.emplace_back(std::move(walk));
      walk_index++;
    }
  }

  return CalculatePageRank();
}

std::vector<std::pair<std::uint64_t, double>> GetPagerank(const mg_graph::GraphView<> &graph) {
  if (pagerank_online_alg::context.IsEmpty()) {
    return SetPagerank(graph);
  }
  if (IsInconsistent(graph)) {
    throw std::runtime_error(
        "Graph has been modified, therefore is inconsistent with cached results, please update the Pagerank by calling "
        "set/reset!");
  }
  return CalculatePageRank();
}

std::vector<std::pair<std::uint64_t, double>> UpdatePagerank(
    const mg_graph::GraphView<> &graph, const std::vector<std::uint64_t> &new_vertices,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &new_edges,
    const std::vector<std::uint64_t> &deleted_vertices,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &deleted_edges) {
  if (pagerank_online_alg::context.IsEmpty()) {
    return SetPagerank(graph);
  }

  for (const auto &edge : deleted_edges) {
    UpdateDelete(graph, edge);
  }
  for (const auto vertex : deleted_vertices) {
    UpdateDelete(graph, vertex);
  }
  for (const auto vertex : new_vertices) {
    UpdateCreate(graph, vertex);
  }
  for (const auto &edge : new_edges) {
    UpdateCreate(graph, edge);
  }

  return CalculatePageRank();
}

void Reset() { pagerank_online_alg::context.Init(); }
}  // namespace pagerank_online_alg
