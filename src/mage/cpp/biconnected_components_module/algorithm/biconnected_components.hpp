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

#pragma once

#include <mg_graph.hpp>

#include <cstdint>
#include <stack>
#include <unordered_map>
#include <unordered_set>

namespace bcc_utility {

/// Simple struct that keeps the state of nodes in algorithms that rely on
/// DFS traversal.
struct NodeState {
  std::unordered_map<std::uint64_t, bool> visited;
  std::unordered_map<std::uint64_t, std::uint64_t> discovery, low_link;
  std::uint64_t counter = 0;

  explicit NodeState(std::uint64_t number_of_nodes);
  void Update(std::uint64_t node_id);
};

///
///@brief DFS Algorithm for obtaining the biconnected components within the graph
///
///@param node_id Starting Node ID
///@param parent_id Parental Node ID
///@param state Current node state
///@param edge_stack Current state of edge stack
///@param bcc_edges Current biconnected components
///@param graph Graph to work on
///
void BccDFS(std::uint64_t node_id, std::uint64_t parent_id, bcc_utility::NodeState *state,
            std::stack<mg_graph::Edge<>> *edge_stack, std::vector<std::vector<mg_graph::Edge<>>> *bcc_edges,
            std::vector<std::unordered_set<std::uint64_t>> *bcc_nodes, const mg_graph::GraphView<> &graph,
            std::unordered_set<std::uint64_t> &articulation_points);

}  // namespace bcc_utility

namespace bcc_algorithm {

///
///@brief Method for getting all of the biconnected components inside of a GraphView
///
///@param graph GraphView object
///@return std::vector<std::vector<mg_graph::Edge<>>>
///
std::vector<std::vector<mg_graph::Edge<>>> GetBiconnectedComponents(
    const mg_graph::GraphView<> &graph, std::unordered_set<std::uint64_t> &articulation_points,
    std::vector<std::unordered_set<std::uint64_t>> &bcc_nodes);

}  // namespace bcc_algorithm
