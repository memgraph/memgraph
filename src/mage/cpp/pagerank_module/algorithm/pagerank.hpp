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

#include <cstdint>
#include <unordered_map>
#include <vector>

struct mgp_graph;
struct mgp_memory;

namespace pagerank_alg {

// Defines edge's from and to node index. Just an alias for user convenience.
using EdgePair = std::pair<std::uint64_t, std::uint64_t>;

/// A directed, unweighted graph.
/// Self loops and multiple edges are allowed and they will affect the result.
/// Node ids are integers from interval [0, number of nodes in graph - 1].
/// Graph is allowed to be disconnected.
class PageRankGraph {
 public:
  PageRankGraph() = default;

  /// Creates graph with given number of nodes with node ids from interval
  /// [0, number_of_nodes - 1] and with given edges between them.
  /// Node ids describing edges have to be integers from
  /// interval [0, number of nodes in graph - 1].
  /// @param number_of_nodes -- number of nodes in graph
  /// @param number_of_edges -- number of edges in graph
  /// @param edges -- pairs (source, target) representing directed edges
  PageRankGraph(std::uint64_t number_of_nodes, std::uint64_t number_of_edges, const std::vector<EdgePair> &edges);

  PageRankGraph(mgp_graph *memgraph_graph, mgp_memory *memory);

  std::uint64_t GetMemgraphNodeId(std::uint64_t node_id) const;

  /// @return -- number of nodes in graph
  std::uint64_t GetNodeCount() const;

  /// @return -- number of edges in graph
  std::uint64_t GetEdgeCount() const;

  /// @return -- a reference to ordered vector of edges
  const std::vector<EdgePair> &GetOrderedEdges() const;

  /// Returns out degree of node node_id
  /// @param node_id -- node name
  /// @return -- out degree of node node_id
  std::uint64_t GetOutDegree(std::uint64_t node_id) const;

 private:
  /// node_count equals number of nodes in graph
  std::uint64_t node_count_;
  /// edge_count equals number of edges in graph
  std::uint64_t edge_count_;
  /// directed edges (source, target) (source -> target) ordered by target
  std::vector<EdgePair> ordered_edges_;
  /// out degree for each node in graph because it is required in calculating
  /// PageRank
  std::vector<std::uint64_t> out_degree_;

  std::vector<std::uint64_t> id_to_memgraph;
  std::unordered_map<std::uint64_t, std::uint64_t> memgraph_to_id;

  friend PageRankGraph CreatePageRankGraph(mgp_graph *memgraph_graph, mgp_memory *memory);
};

/// If we present nodes as pages and directed edges between them as links the
/// PageRank algorithm outputs a probability distribution used to represent the
/// likelihood that a person randomly clicking on links will arrive at any
/// particular page.
///
/// PageRank theory holds that an imaginary surfer who is randomly clicking on
/// links will eventually stop clicking. The probability, at any step, that the
/// person will continue randomly clicking on links is called a damping factor,
/// otherwise next page is chosen randomly among all pages.
///
/// PageRank is computed iteratively using following formula:
/// Rank(n, t + 1) = (1 - d) / number_of_nodes
///                + d * sum { Rank(in_neighbour_of_n, t) /
///                out_degree(in_neighbour_of_n)}
/// Where Rank(n, t) is PageRank of node n at iteration t
/// At the end Rank values are normalized to sum 1 to form probability
/// distribution.
///
/// Default arguments are equal to default arguments in NetworkX PageRank
/// implementation:
/// https://networkx.github.io/documentation/networkx-1.10/reference/generated/
/// networkx.algorithms.link_analysis.pagerank_alg.pagerank_module.html
///
/// @param graph -- a directed, unweighted, not necessarily connected graph
/// which can contain multiple edges and self-loops.
/// @param max_iterations -- maximum number of iterations performed by PageRank.
/// @param damping_factor -- a real number from interval [0, 1], as described
/// above
/// @return -- probability distribution, as described above
std::vector<double> ParallelIterativePageRank(const PageRankGraph &graph, size_t max_iterations = 100,
                                              double damping_factor = 0.85, double stop_epsilon = 10e-6,
                                              uint32_t number_of_threads = 1);

}  // namespace pagerank_alg
