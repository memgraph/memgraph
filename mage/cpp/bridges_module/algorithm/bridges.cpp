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

#include <algorithm>

#include "bridges.hpp"

namespace bridges_util {

NodeState::NodeState(std::size_t number_of_nodes) {
  visited.resize(number_of_nodes, false);
  discovery.resize(number_of_nodes, 0);
  low_link.resize(number_of_nodes, 0);
}

void NodeState::Update(std::uint64_t node_id) {
  counter++;
  visited[node_id] = true;
  discovery[node_id] = counter;
  low_link[node_id] = counter;
}

void BridgeDfs(std::uint64_t node_id, std::uint64_t parent_id, NodeState *state, std::vector<mg_graph::Edge<>> *bridges,
               const mg_graph::GraphView<> &graph) {
  state->Update(node_id);

  for (const auto &neigh : graph.Neighbours(node_id)) {
    auto next_id = neigh.node_id;
    if (state->visited[next_id]) {
      if (next_id != parent_id) {
        state->low_link[node_id] = std::min(state->low_link[node_id], state->discovery[next_id]);
      }
      continue;
    }

    BridgeDfs(next_id, node_id, state, bridges, graph);
    state->low_link[node_id] = std::min(state->low_link[node_id], state->low_link[next_id]);

    const auto &edge = graph.GetEdge(neigh.edge_id);
    if (state->low_link[next_id] > state->discovery[node_id]) {
      if (graph.GetEdgesBetweenNodes(edge.from, edge.to).size() == 1) bridges->push_back(edge);
    }
  }
}

}  // namespace bridges_util

namespace bridges_alg {

std::vector<mg_graph::Edge<>> GetBridges(const mg_graph::GraphView<> &graph) {
  auto number_of_nodes = graph.Nodes().size();
  bridges_util::NodeState state(number_of_nodes);

  std::vector<mg_graph::Edge<>> bridges;
  for (const auto &node : graph.Nodes()) {
    if (!state.visited[node.id]) {
      bridges_util::BridgeDfs(node.id, node.id, &state, &bridges, graph);
    }
  }
  return bridges;
}

}  // namespace bridges_alg
