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

#include <optional>

#include <mg_graph.hpp>

namespace bipartite_matching_util {

///
///@brief Method for updating the state of matched and visited nodes within the graph. The DFS is done on each node, and
/// on each successive node a matched container is updated
///
///@param node Node for current check
///@param adj_list List of adjacent nodes
///@param visited Container for keeping in track visited nodes
///@param matched Container for keeping in track matched nodes
///@return true If bipartite matching DFS can be ran
///@return false If bipartite matching DFS cannot be ran
///
bool BipartiteMatchingDFS(const std::uint64_t node, const std::vector<std::vector<std::uint64_t>> &adj_list,
                          std::vector<bool> &visited, std::vector<std::optional<std::uint64_t>> &matched);

///
///@brief Function for checking whether the graph is bipartite. A bipartite graph is a graph whose vertices can be
/// divided into two disjoint and independent sets U and V such that every edge connects a vertex in U to one in V.
///
///@param graph Graph to explore
///@param colors Coloring structure for the graph
///@return true If graph is bipartite
///@return false If graph is not bipartite
///
bool IsGraphBipartiteColoring(const mg_graph::GraphView<> &graph, std::vector<std::int8_t> &colors);

///
///@brief The method runs a BFS algorithm and colors the graph in 2 colors. The graph is bipartite if for any vertex all
/// neighbors have the opposite color. In that case, method will return true. Otherwise, false.
///
///@param graph Graph to explore
///@param colors Container for keeping in track the color of all nodes in graph
///@param node_index Current index in bipartite coloring
///@return true
///@return false
///
bool IsSubgraphBipartiteColoring(const mg_graph::GraphView<> &graph, std::vector<std::int8_t> &colors,
                                 const std::uint64_t node_index);

///
/// @brief Returns the size of the maximum matching in a given bipartite graph. The
/// graph is defined by the sizes of two disjoint sets of nodes and by a set of
/// edges between those two sets of nodes. The nodes in both sets should be
/// indexed from 1 to `set_size`.
///
/// The algorithm runs in O(|V|*|E|) time where V represents a set of nodes and
/// E represents a set of edges.
///@param disjoint_edges Edges between a two set of
///@return std::uint64_t Maximum edges matching
///
std::uint64_t MaximumMatching(const std::vector<std::pair<std::uint64_t, std::uint64_t>> &disjoint_edges);

}  // namespace bipartite_matching_util

namespace bipartite_matching_alg {

/// @brief Returns the size of the maximum matching in a given bipartite graph. If the graph is bipartite, edge mapping
/// into 2 disjoint sets is performed and matching method is executed.The graph is defined by the sizes of two disjoint
/// sets of nodes and by a set of edges between those two sets of nodes. The nodes in both sets should be indexed from 1
/// to `set_size`.
///
///@param graph Graph to explore
///@return std::uint64_t Maximum bipartite matching
///
std::uint64_t BipartiteMatching(const mg_graph::GraphView<> &graph);

}  // namespace bipartite_matching_alg

namespace bipartite_matching_alg {}
