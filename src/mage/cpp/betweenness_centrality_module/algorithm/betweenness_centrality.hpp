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

#pragma once

#include <stack>
#include <vector>

#include <mg_graph.hpp>

namespace betweenness_centrality_util {

///
///@brief A method that performs an augmented BFS algorithm to count the number of shortest paths
/// between the source node and all nodes in the given graph. This method has O(m) time complexity.
///
///@param source_node The starting node of the bfs traversal
///@param graph The graph that is traversed by the BFS algorithm
///@param visited The container for storing nodes in the order in which they are discovered by the BFS algorithm
///@param predecessors The container for storing nodes that are located just before a specific node on
/// the shortest path from the source node to the aforementioned node
///@param shortest_paths_counter The container for storing the number of shortest paths from the source node to
/// all nodes in the given graph
///
void BFS(const std::uint64_t source_node, const mg_graph::GraphView<> &graph, std::stack<std::uint64_t> &visited,
         std::vector<std::vector<std::uint64_t>> &predecessors, std::vector<std::uint64_t> &shortest_paths_counter,
         std::vector<int> &distance);

}  // namespace betweenness_centrality_util

namespace betweenness_centrality_alg {

///
///@brief A method that computes the betweenness centrality for nodes of the given graph.
/// The betweenness centrality of a node is defined as the sum of the of all-pairs shortest
/// paths that pass through the node divided by the number of all-pairs shortest paths in
/// the graph. The algorithm has O(nm) time complexity.
///
///@param graph Graph for exploration
///@param directed Indicates whether the algorithm is performed on a directed or undirected graph
///@param normalized If true the betweenness values are normalized by dividing the value by the
/// number of pairs of nodes not including the node whose value we normalize. For undirected graph
/// the normalization constant is 2/((n-1)(n-2)), and for directed is 1/((n-1)(n-2)).
///@return A vector that contains betweenness centrality scores placed on indices that correspond
/// to the identifiers of the nodes.
///
std::vector<double> BetweennessCentrality(const mg_graph::GraphView<> &graph, bool directed, bool normalize,
                                          int threads);

}  // namespace betweenness_centrality_alg
