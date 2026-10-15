// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <queue>
#include <unordered_map>
#include <unordered_set>

#include "bipartite_matching.hpp"

namespace bipartite_matching_util {
bool BipartiteMatchingDFS(const std::uint64_t node, const std::vector<std::vector<std::uint64_t>> &adj_list,
                          std::vector<bool> &visited, std::vector<std::optional<std::uint64_t>> &matched) {
  if (visited[node]) return false;

  visited[node] = true;
  for (const auto next : adj_list[node]) {
    if (const auto matched_next = matched[next];
        !matched_next || BipartiteMatchingDFS(*matched_next, adj_list, visited, matched)) {
      matched[next] = node;
      return true;
    }
  }
  return false;
}

bool IsGraphBipartiteColoring(const mg_graph::GraphView<> &graph, std::vector<std::int8_t> &colors) {
  auto node_size = graph.Nodes().size();

  for (std::uint64_t i = 0; i < node_size; i++) {
    if (colors[i] == -1 && !IsSubgraphBipartiteColoring(graph, colors, i)) {
      return false;
    }
  }

  return true;
}

bool IsSubgraphBipartiteColoring(const mg_graph::GraphView<> &graph, std::vector<std::int8_t> &colors,
                                 const std::uint64_t node_index) {
  // Data structure used in BFS
  std::queue<std::uint64_t> unvisited;

  colors[node_index] = static_cast<std::int8_t>(1);
  unvisited.push(node_index);
  while (!unvisited.empty()) {
    auto current_index = unvisited.front();
    unvisited.pop();
    for (const auto &neighbour : graph.Neighbours(current_index)) {
      // Self loops are not allowed
      if (neighbour.node_id == current_index) return false;

      // If neighbor has the same color as the current node, graph is not bipartite
      if (colors[neighbour.node_id] == colors[current_index]) return false;

      if (colors[neighbour.node_id] == -1) {
        // Set to the opposite color = 0/1
        colors[neighbour.node_id] = static_cast<std::int8_t>(1 - colors[current_index]);
        unvisited.push(neighbour.node_id);
      }
    }
  }

  return true;
}

std::uint64_t MaximumMatching(const std::vector<std::pair<std::uint64_t, std::uint64_t>> &disjoint_edges) {
  std::unordered_set<std::uint64_t> group_a;
  std::unordered_set<std::uint64_t> group_b;

  for (const auto &[from, to] : disjoint_edges) {
    group_a.insert(from);
    group_b.insert(to);
  }

  auto size_a = group_a.size();
  auto size_b = group_b.size();

  // matched[i] = j <==> i-th node from B is matched with j-th node from A
  std::vector<std::optional<std::uint64_t>> matched(size_b + 1, std::nullopt);

  std::vector<std::vector<std::uint64_t>> adj_list(size_a + 1);
  for (const auto &[from, to] : disjoint_edges) adj_list[from].push_back(to);

  std::uint64_t maximum_matching = 0;
  for (std::uint64_t node = 1; node <= size_a; ++node) {
    // Keeping in track which group A nodes are visited
    std::vector<bool> visited_a(size_a + 1, false);

    maximum_matching += bipartite_matching_util::BipartiteMatchingDFS(node, adj_list, visited_a, matched);
  }

  return maximum_matching;
}

}  // namespace bipartite_matching_util

namespace bipartite_matching_alg {

std::uint64_t BipartiteMatching(const mg_graph::GraphView<> &graph) {
  // -1 to indicate that color is not set to that node
  std::vector<std::int8_t> colors(graph.Nodes().size(), -1);

  if (!bipartite_matching_util::IsGraphBipartiteColoring(graph, colors)) return 0;

  std::vector<std::pair<std::uint64_t, std::uint64_t>> disjoint_edges;

  std::unordered_map<std::uint64_t, std::uint64_t> left_subset;
  std::unordered_map<std::uint64_t, std::uint64_t> right_subset;

  for (const auto [id, from, to] : graph.Edges()) {
    auto left_edge = colors[from] == 0 ? from : to;
    auto right_edge = colors[from] == 1 ? from : to;

    if (!left_subset.contains(left_edge)) {
      left_subset[left_edge] = left_subset.size() + 1;
    }
    if (!right_subset.contains(right_edge)) {
      right_subset[right_edge] = right_subset.size() + 1;
    }

    auto subset_from = left_subset[left_edge];
    auto subset_to = right_subset[right_edge];

    disjoint_edges.emplace_back(subset_from, subset_to);
  }

  return bipartite_matching_util::MaximumMatching(disjoint_edges);
}

}  // namespace bipartite_matching_alg
