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

#include <unordered_set>

#include <mg_exceptions.hpp>
#include <mg_graph.hpp>

#include "cycles.hpp"

namespace cycles_util {

NodeState::NodeState(std::size_t number_of_nodes) {
  visited.resize(number_of_nodes, false);
  parent.resize(number_of_nodes, std::nullopt);
  depth.resize(number_of_nodes, 0);
}

void NodeState::SetVisited(std::uint64_t node_id) { visited[node_id] = true; }

bool NodeState::IsVisited(std::uint64_t node_id) const { return visited[node_id]; }

void NodeState::SetParent(std::uint64_t parent_id, std::uint64_t node_id) { parent[parent_id] = node_id; }

std::optional<std::uint64_t> NodeState::GetParent(std::uint64_t node_id) const { return parent[node_id]; }

void NodeState::SetDepth(std::uint64_t node_id, std::uint64_t node_depth) { depth[node_id] = node_depth; }

std::uint64_t NodeState::GetDepth(std::uint64_t node_id) const { return depth[node_id]; }

void FindNonSpanningTreeEdges(std::uint64_t node_id, const mg_graph::GraphView<> &graph, NodeState *state,
                              std::set<std::pair<uint64_t, uint64_t>> *non_st_edges) {
  std::unordered_set<std::uint64_t> unique_neighbour;
  state->SetVisited(node_id);
  for (const auto &neigh : graph.Neighbours(node_id)) {
    auto next_id = neigh.node_id;

    // Check if is returning edge or already visited neighbour
    if (const auto parent = state->GetParent(node_id);
        parent && (next_id == *parent || unique_neighbour.find(next_id) != unique_neighbour.end())) {
      continue;
    }

    unique_neighbour.insert(next_id);

    if (state->IsVisited(next_id)) {
      auto sorted_edge = std::minmax(node_id, next_id);
      non_st_edges->insert(sorted_edge);
      continue;
    }

    // Set depth and parent for the next ST iteration
    state->SetParent(next_id, node_id);
    state->SetDepth(next_id, state->GetDepth(node_id) + 1);
    FindNonSpanningTreeEdges(next_id, graph, state, non_st_edges);
  }
}

void FindFundamentalCycles(const std::set<std::pair<std::uint64_t, std::uint64_t>> &non_st_edges,
                           const NodeState &state, std::vector<std::vector<std::uint64_t>> *fundamental_cycles) {
  for (const auto &[from, to] : non_st_edges) {
    fundamental_cycles->emplace_back(FindFundamentalCycle(from, to, state));
  }
}

std::vector<std::uint64_t> FindFundamentalCycle(std::uint64_t node_a, std::uint64_t node_b, const NodeState &state) {
  std::vector<std::uint64_t> cycle;
  if (state.depth[node_a] < state.depth[node_b]) {
    std::swap(node_a, node_b);
  }

  // climb until a and b reach the same depth:
  //
  //       ()               ()
  //      /  \             /  \.
  //    ()   (b)   -->   (a)  (b)
  //   /  \             /   \.
  // (a)  ()           ()   ()
  cycle.emplace_back(node_a);
  while (state.depth[node_a] > state.depth[node_b]) {
    const auto maybe_parent = state.GetParent(node_a);
    if (!maybe_parent) {
      throw mg_exception::InvalidIDException();
    }
    node_a = *maybe_parent;
    cycle.emplace_back(node_a);
  }

  if (node_a != node_b) {
    throw std::runtime_error("There should be no cross edges in DFS tree of an undirected graph.");
  }

  // Close the cycle
  cycle.emplace_back(cycle[0]);
  return cycle;
}

void CombineCycles(std::uint32_t mask, const std::vector<std::vector<std::uint64_t>> &fundamental_cycles,
                   const mg_graph::GraphView<> &graph, std::vector<std::vector<mg_graph::Node<>>> *cycles) {
  std::map<std::pair<std::uint64_t, std::uint64_t>, std::uint64_t> edge_cnt;
  for (std::size_t i = 0; i < fundamental_cycles.size(); ++i) {
    if ((mask & (1 << i)) == 0) continue;
    for (std::size_t j = 1; j < fundamental_cycles[i].size(); ++j) {
      auto edge = std::minmax(fundamental_cycles[i][j], fundamental_cycles[i][j - 1]);
      edge_cnt[edge]++;
    }
  }

  std::unordered_map<std::uint64_t, std::vector<std::uint64_t>> adj_list;
  std::unordered_set<std::uint64_t> nodes;
  for (const auto &[key, value] : edge_cnt) {
    if (value % 2 == 0) continue;

    const auto [from, to] = key;
    adj_list[from].push_back(to);
    adj_list[to].push_back(from);
    nodes.insert(from);
    nodes.insert(to);
  }

  // deg(v) = 2 for all vertices in a cycle.
  for (const auto node : nodes) {
    if (adj_list[node].size() != 2) return;
  }

  std::unordered_set<std::uint64_t> visited;
  std::vector<mg_graph::Node<>> cycle;
  auto curr_node = *nodes.begin();

  while (visited.find(curr_node) == visited.end()) {
    cycle.push_back({curr_node});
    visited.insert(curr_node);
    for (const auto next_node : adj_list[curr_node]) {
      if (visited.find(next_node) == visited.end()) {
        curr_node = next_node;
        break;
      }
    }
  }

  for (const auto node : nodes) {
    if (visited.find(node) == visited.end()) {
      return;
    }
  }

  if (cycle.size() > 2) {
    cycles->emplace_back(cycle);
  }
}

void GetCyclesFromFundamentals(const std::vector<std::vector<uint64_t>> &fundamental_cycles,
                               const mg_graph::GraphView<> &graph, std::vector<std::vector<mg_graph::Node<>>> *cycles) {
  std::uint32_t size = 1 << fundamental_cycles.size();
  for (std::uint32_t mask = 1; mask < size; ++mask) {
    CombineCycles(mask, fundamental_cycles, graph, cycles);
  }
}

}  // namespace cycles_util

namespace cycles_alg {

std::vector<std::vector<mg_graph::Node<>>> GetCycles(const mg_graph::GraphView<> &graph) {
  auto number_of_nodes = graph.Nodes().size();

  cycles_util::NodeState state(number_of_nodes);

  std::vector<std::vector<mg_graph::Node<>>> cycles;
  // TODO: Solve for each connected component
  for (const auto &node : graph.Nodes()) {
    if (state.IsVisited(node.id)) {
      continue;
    }

    // First we find edges that do not lie on a DFS tree (basically,
    // backedges and crossedges). From those edges we expand into the
    // spanning tree to obtain all fundamental cycles.
    state.SetParent(node.id, node.id);
    std::set<std::pair<std::uint64_t, std::uint64_t>> non_st_edges;
    cycles_util::FindNonSpanningTreeEdges(node.id, graph, &state, &non_st_edges);

    // After finding non spanning-tree edges, we obtain fundamental cycles
    std::vector<std::vector<std::uint64_t>> fundamental_cycles;
    cycles_util::FindFundamentalCycles(non_st_edges, state, &fundamental_cycles);

    // Getting elementary cycles from the subset of fundamental ones
    cycles_util::GetCyclesFromFundamentals(fundamental_cycles, graph, &cycles);
  }

  std::set<std::pair<std::uint64_t, std::uint64_t>> multi_edges;
  for (const auto &edge : graph.Edges()) {
    const auto sorted_edge = std::minmax(edge.from, edge.to);
    if (multi_edges.find(sorted_edge) != multi_edges.end()) {
      continue;
    }
    multi_edges.insert(sorted_edge);

    const auto [from, to] = sorted_edge;
    const auto &edges = graph.GetEdgesBetweenNodes(from, to);
    // Add neighbor cycles and self-loops
    if (edges.size() > 1 || from == to) {
      cycles.push_back({{from}, {to}});
    }
  }

  return cycles;
}

}  // namespace cycles_alg
