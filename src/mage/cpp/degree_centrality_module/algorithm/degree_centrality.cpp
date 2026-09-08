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

#include "degree_centrality.hpp"

namespace degree_centrality_alg {

std::vector<double> GetDegreeCentrality(const mg_graph::GraphView<> &graph, const AlgorithmType algorithm_type) {
  const auto &nodes = graph.Nodes();
  auto number_of_nodes = nodes.size();

  // Initialize centrality values
  std::vector<double> degree_centralities(number_of_nodes, 0.0);

  // Degree centrality is the proportion of neighbors and maximum degree (n-1)
  for (const auto [node_id] : graph.Nodes()) {
    std::size_t degree = 0;

    switch (algorithm_type) {
      case AlgorithmType::kUndirected:
        degree = graph.OutNeighbours(node_id).size() + graph.InNeighbours(node_id).size();
        break;

      case AlgorithmType::kOut:
        degree = graph.OutNeighbours(node_id).size();
        break;

      case AlgorithmType::kIn:
        degree = graph.InNeighbours(node_id).size();
        break;
    }

    // Degree centrality can be > 1 in multi-relational graphs
    degree_centralities[node_id] = static_cast<double>(degree) / static_cast<double>((number_of_nodes - 1));
  }
  return degree_centralities;
}

}  // namespace degree_centrality_alg
