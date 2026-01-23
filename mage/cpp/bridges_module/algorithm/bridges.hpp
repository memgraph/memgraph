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

namespace bridges_util {

/// Simple struct that keeps the state of nodes in algorithms that rely on
/// DFS traversal.
struct NodeState {
  std::vector<bool> visited;
  std::vector<std::uint64_t> discovery, low_link;
  std::uint64_t counter = 0;

  explicit NodeState(std::size_t number_of_nodes);

  void Update(std::uint64_t node_id);
};

void BridgeDfs(std::uint64_t node_id, std::uint64_t parent_id, bridges_util::NodeState *state,
               std::vector<mg_graph::Edge<>> *bridges, const mg_graph::GraphView<> &graph);
}  // namespace bridges_util

namespace bridges_alg {

std::vector<mg_graph::Edge<>> GetBridges(const mg_graph::GraphView<> &graph);

}  // namespace bridges_alg
