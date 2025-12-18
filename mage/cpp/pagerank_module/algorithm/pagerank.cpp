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

#include <algorithm>
#include <future>
#include <numeric>

namespace pagerank_alg {

namespace {

/// @tparam T Node ID data type.
template <typename T>
class AdjacencyList {
 public:
  AdjacencyList() = default;
  explicit AdjacencyList(std::uint64_t node_count) : list_(node_count) {}

  auto GetNodeCount() const { return list_.size(); }

  /// AdjacentPair is a pair of T. Values of T have to be >= 0 and < node_count
  /// because they represent position in the underlying std::vector.
  void AddAdjacentPair(T left_node_id, T right_node_id, bool undirected = false) {
    list_[left_node_id].push_back(right_node_id);
    if (undirected) {
      list_[right_node_id].push_back(left_node_id);
    }
  }

  /// Be careful and don't call AddAdjacentPair while you have reference to this
  /// vector because the referenced vector could be resized (moved) which means
  /// that the reference is going to become invalid.
  ///
  /// @return A reference to std::vector of adjacent node ids.
  const auto &GetAdjacentNodes(T node_id) const { return list_[node_id]; }

 private:
  std::vector<std::vector<T>> list_;
};

/// Calculates optimal borders for dividing edges in number_of_threads
/// consecutive partitions (blocks) such that the maximal block size is minimal
/// For example: if number_of_edges = 10 and number_of_threads = 3:
/// optimal borders = {0, 3, 6, 10} so obtained blocks of edges
/// are [0, 3>, [3, 6> and [6, 10>.
///
/// @param graph -- graph
/// @param number_of_threads -- number of threads
std::vector<std::uint64_t> CalculateOptimalBorders(const PageRankGraph &graph, const std::uint64_t number_of_threads) {
  std::vector<std::uint64_t> borders;

  if (number_of_threads == 0) {
    throw std::runtime_error("Number of threads can't be zero (0)!");
  }

  for (std::uint64_t border_index = 0; border_index <= number_of_threads; border_index++) {
    borders.push_back(border_index * graph.GetEdgeCount() / number_of_threads);
  }
  return borders;
}

/// Calculating PageRank block related to [lo, hi> interval of edges
/// Graph edges are ordered by target node ids so target nodes of an interval
/// of edges also form an interval of nodes.
/// Required PageRank values of nodes from this interval will be sent
/// as vector of values using new_rank_promise.
///
/// @param graph -- graph
/// @param old_rank -- rank of nodes after previous iterations
/// @param damping_factor -- damping factor
/// @param lo -- left bound of interval of edges
/// @param hi -- right bound of interval of edges
/// @param new_rank_promise -- used for sending information about calculated new
/// PageRank block
void ThreadPageRankIteration(const PageRankGraph &graph, const std::vector<double> &old_rank, const std::uint64_t lo,
                             const std::uint64_t hi, std::promise<std::vector<double>> new_rank_promise) {
  std::vector<double> new_rank(graph.GetNodeCount(), 0);
  // Calculate sums of PR(page)/C(page) scores for the entire block (from lo to hi edges).
  for (std::size_t edge_id = lo; edge_id < hi; edge_id++) {
    const auto [source, target] = graph.GetOrderedEdges()[edge_id];
    // Add the score of target node to the sum.
    new_rank[source] += old_rank[target] / graph.GetOutDegree(target);
  }
  new_rank_promise.set_value(std::move(new_rank));
}

/// Merging PageRank blocks from ThreadPageRankIteration.
///
/// @param graph -- graph
/// @param damping_factor -- damping factor
/// @param block -- PageRank block calculated in ThreadPageRankIteration
/// @param rank_next -- PageRanks which will be updated
void AddCurrentBlockToRankNext(const PageRankGraph &graph, const double damping_factor,
                               const std::vector<double> &block, std::vector<double> &rank_next) {
  // The block vector contains partially precalculated sums of PR(page)/C(page)
  // for each node. Node index in the block vector corresponds to the node index
  // in the rank_next vector.
  for (std::size_t node_index = 0; node_index < block.size(); node_index++) {
    rank_next[node_index] += damping_factor * block[node_index];
  }
}

/// Adds remaining PageRank values
/// Adds PageRank values of nodes that haven't been added by
/// AddCurrentBlockToRankNext. That are nodes whose id is greater
/// than id of target node of the last edge in ordered edge list.
///
/// @param graph -- graph
/// @param damping_factor -- damping factor
/// @param rank_next -- PageRank which will be updated
void CompleteRankNext(const PageRankGraph &graph, const double damping_factor, std::vector<double> &rank_next) {
  while (rank_next.size() < graph.GetNodeCount()) {
    rank_next.push_back((1.0 - damping_factor) / graph.GetNodeCount());
  }
}

/// Checks whether PageRank algorithm should continue iterating
/// Checks if maximal number of iterations was reached or
/// if difference between every component of previous and current PageRank
/// was less than stop_epsilon.
///
/// @param rank -- PageRank before the last iteration
/// @param rank_next -- PageRank after the last iteration
/// @param max_iterations -- maximal number of iterations
/// @param stop_epsilon -- stop epsilon
/// @param number_of_iterations -- current number of operations
bool CheckContinueIterate(const std::vector<double> &rank, const std::vector<double> &rank_next,
                          const std::size_t max_iterations, const double stop_epsilon,
                          const std::size_t number_of_iterations) {
  if (number_of_iterations == max_iterations) {
    return false;
  }
  for (std::size_t node_id = 0; node_id < rank.size(); node_id++) {
    if (std::abs(rank[node_id] - rank_next[node_id]) > stop_epsilon) {
      return true;
    }
  }
  return false;
}

/// Normalizing PageRank
/// Divides all values with sum of the values to get their sum equal 1
///
/// @param rank -- PageRank
void NormalizeRank(std::vector<double> &rank) {
  const double sum = std::accumulate(rank.begin(), rank.end(), 0.0);
  for (double &value : rank) {
    value /= sum;
  }
}

}  // namespace

PageRankGraph::PageRankGraph(const std::uint64_t number_of_nodes, const std::uint64_t number_of_edges,
                             const std::vector<EdgePair> &edges)
    : node_count_(number_of_nodes), edge_count_(number_of_edges), out_degree_(number_of_nodes) {
  AdjacencyList<std::uint64_t> in_neighbours(number_of_nodes);

  for (const auto &[from, to] : edges) {
    // Because PageRank needs a set of nodes that point to a given node.
    in_neighbours.AddAdjacentPair(from, to);
    out_degree_[from] += 1;
  }
  for (std::size_t node_id = 0; node_id < number_of_nodes; node_id++) {
    const auto &adjacent_nodes = in_neighbours.GetAdjacentNodes(node_id);
    for (const auto adjacent_node : adjacent_nodes) {
      ordered_edges_.emplace_back(adjacent_node, node_id);
    }
  }
}

std::uint64_t PageRankGraph::GetMemgraphNodeId(std::uint64_t node_id) const { return id_to_memgraph[node_id]; }

std::uint64_t PageRankGraph::GetNodeCount() const { return node_count_; }

std::uint64_t PageRankGraph::GetEdgeCount() const { return edge_count_; }

const std::vector<EdgePair> &PageRankGraph::GetOrderedEdges() const { return ordered_edges_; }

std::uint64_t PageRankGraph::GetOutDegree(const std::uint64_t node_id) const { return out_degree_[node_id]; }

std::vector<double> ParallelIterativePageRank(const PageRankGraph &graph, std::size_t max_iterations,
                                              double damping_factor, double stop_epsilon, uint32_t number_of_threads) {
  number_of_threads = std::min(number_of_threads, std::thread::hardware_concurrency());

  auto borders = CalculateOptimalBorders(graph, number_of_threads);

  std::vector<double> rank(graph.GetNodeCount(), 1.0 / graph.GetNodeCount());
  // Because we increment number_of_iterations at the end of while loop.
  bool continue_iterate = max_iterations != 0;

  std::size_t number_of_iterations = 0;
  while (continue_iterate) {
    std::vector<std::promise<std::vector<double>>> page_rank_promise(number_of_threads);

    std::vector<std::future<std::vector<double>>> page_rank_future;
    page_rank_future.reserve(number_of_threads);

    std::transform(page_rank_promise.begin(), page_rank_promise.end(), std::back_inserter(page_rank_future),
                   [](auto &pr_promise) { return pr_promise.get_future(); });

    std::vector<std::thread> my_threads;
    my_threads.reserve(number_of_threads);
    for (std::size_t cluster_id = 0; cluster_id < number_of_threads; cluster_id++) {
      my_threads.emplace_back([&, lo = borders[cluster_id], hi = borders[cluster_id + 1]](
                                  auto promise) { ThreadPageRankIteration(graph, rank, lo, hi, std::move(promise)); },
                              std::move(page_rank_promise[cluster_id]));
    }

    std::vector<double> rank_next(graph.GetNodeCount(), (1.0 - damping_factor) / graph.GetNodeCount());
    for (std::size_t cluster_id = 0; cluster_id < number_of_threads; cluster_id++) {
      std::vector<double> block = page_rank_future[cluster_id].get();
      AddCurrentBlockToRankNext(graph, damping_factor, block, rank_next);
    }
    CompleteRankNext(graph, damping_factor, rank_next);

    for (std::uint64_t i = 0; i < number_of_threads; i++) {
      if (my_threads[i].joinable()) {
        my_threads[i].join();
      }
    }
    rank.swap(rank_next);
    number_of_iterations++;
    continue_iterate = CheckContinueIterate(rank, rank_next, max_iterations, stop_epsilon, number_of_iterations);
  }
  NormalizeRank(rank);
  return rank;
}

}  // namespace pagerank_alg
