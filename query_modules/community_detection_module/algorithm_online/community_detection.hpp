// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

#pragma once

#include <memory>

#include "mg_graph.hpp"

namespace LabelRankT {

class LabelRankT {
 private:
  /* #region parameters */
  /// default edge weight
  static constexpr double DEFAULT_WEIGHT = 1;

  /// graph directedness
  bool is_directed = false;
  /// whether to use weights
  bool is_weighted = false;

  /// similarity threshold used in the node selection step, values within [0, 1]
  double similarity_threshold = 0.7;
  /// exponent used in the inflation step
  double exponent = 4;
  /// lowest probability at the cutoff step
  double min_value = 0.1;

  /// weight property name
  std::string weight_property = "weight";
  /// default self-loop weight
  double w_selfloop = 1.0;

  /// maximum number of iterations
  std::uint64_t max_iterations = 100;
  /// maximum number of updates for any node
  std::uint64_t max_updates = 5;
  /* #endregion */

  /* #region structures */
  /// reference to current graph
  std::unique_ptr<mg_graph::Graph<>> graph;

  /// map containing each node’s community label probabilities
  std::unordered_map<std::uint64_t, std::unordered_map<std::uint64_t, double>> label_Ps;
  /// sum of weights of each node’s in-edges
  std::unordered_map<std::uint64_t, double> sum_w;
  /// how many times each node has been updated
  std::unordered_map<std::uint64_t, std::uint64_t> times_updated;

  /// flag whether community labels have already been calculated
  bool calculated = false;
  /* #endregion */

  /* #region helper_methods */
  ///@brief Returns a vector of all graph nodes’ Memgraph IDs.
  ///
  ///@return -- vector of all graph nodes’ Memgraph IDs
  std::vector<std::uint64_t> NodesMemgraphIDs() const;

  ///@brief Returns in-neighbors’ Memgraph IDs for a given node’s Memgraph ID.
  /// If the graph is undirected, all neighbors are included.
  ///
  ///@param node_id -- given node’s Memgraph ID
  ///
  ///@return -- vector of neighbors’ Memgraph IDs
  std::unordered_set<std::uint64_t> InNeighborsMemgraphIDs(std::uint64_t node_id) const;

  ///@brief Initializes internal data structures.
  void SetStructures(std::uint64_t node_id);

  ///@brief Removes deleted nodes’ entries from internal data structures.
  ///
  ///@param deleted_nodes_ids -- set of deleted nodes’ IDs
  void RemoveDeletedNodes(const std::unordered_set<std::uint64_t> &deleted_nodes_ids);

  ///@brief Retrieves the total weight of edges between two nodes.
  /// With undirected graphs it considers all edges between the given node pair.
  /// On a directed graph, it checks if the source and target nodes match.
  ///
  ///@param from_node_id -- source node’s Memgraph ID
  ///@param to_node_id -- target node’s Memgraph ID
  ///
  ///@return -- total weight of edges between from_node_id and to_node_id
  double GetTotalWeightBetween(std::uint64_t from_node_id, std::uint64_t to_node_id) const;

  ///@brief Retrieves given edge’s weight.
  double GetWeight(std::uint64_t edge_id) const;

  ///@brief Resets each node’s number of updates.
  void ResetTimesUpdated();

  /// Returns given node’s current community label.
  /// If no label was found (e.g. with the min_value set too high),
  /// the assigned label number is -1.
  ///
  /// @return -- given node’s current community label
  std::int64_t NodeLabel(std::uint64_t node_id);

  /// Returns all nodes’ current community labels.
  //
  /// Community label numbers are initially derived from Memgraph’s node IDs. As
  /// those grow larger with graph updates, this method renumbers them so that
  /// they begin with 1.
  std::unordered_map<std::uint64_t, std::int64_t> AllLabels();

  ///@return -- given node’s most probable community labels
  std::unordered_set<std::uint64_t> MostProbableLabels(std::uint64_t node_id);
  /* #endregion */

  /* #region label_propagation_steps */
  ///@brief Checks if given node’s label probabilities are sufficiently distinct
  /// from its neighbors’.
  /// For a label probability vector to be considered sufficiently distinct, it
  /// has to be a subset of less than k% label probability vectors of its
  /// neighbors.
  ///
  ///@param node_i -- given node
  ///@param similarity_threshold -- the k% threshold above
  ///
  ///@return -- whether the criterion is met
  bool DistinctEnough(std::uint64_t node_i_id, double similarity_threshold);

  ///@brief Performs label propagation on given node’s label probability vector.
  /// Label propagation works by calculating a weighted sum of the label
  /// probability vectors of the node’s neighbors.
  ///
  ///@param node_i -- given node
  ///
  ///@return -- updated label probabilities for given node
  std::unordered_map<std::uint64_t, double> Propagate(std::uint64_t node_i_id);

  ///@brief Raises given node’s label probabilities to specified power and
  /// normalizes the result.
  ///
  ///@param node_label_Ps -- given node’s label probabilities
  ///@param exponent -- smallest acceptable value
  ///
  ///@return -- updated label probabilities for given node
  void Inflate(std::unordered_map<std::uint64_t, double> &node_label_Ps, double exponent) const;

  ///@brief Removes values under a set threshold from given node’s label
  /// probability vector.
  ///
  ///@param node_label_Ps -- given node’s label probabilities
  ///@param min_value -- smallest acceptable value
  ///
  ///@return -- updated label probabilities for given node
  void Cutoff(std::unordered_map<std::uint64_t, double> &node_label_Ps, double min_value) const;

  ///@brief Performs an iteration of the LabelRankT algorithm.
  /// Each iteration has four steps: node selection, label propagation,
  /// inflation and cutoff.
  ///
  ///@param incremental -- whether the algorithm is ran incrementally
  ///@param changed_nodes -- the set of changed nodes (for incremental update)
  ///
  ///@return -- {whether no nodes have been updated, maximum number of updates
  /// of any node so far} pair
  std::pair<bool, std::uint64_t> Iteration(const std::vector<uint64_t> &nodes_memgraph_ids, bool incremental = false,
                                           const std::unordered_set<std::uint64_t> &changed_nodes = {},
                                           const std::unordered_set<std::uint64_t> &to_delete = {});
  /* #endregion */

  ///@brief Handles calculation of community labels and associated data
  /// structures for both dynamic and non-dynamic uses of the algorithm.
  ///
  ///@param graph -- current graph
  ///@param changed_nodes -- list of changed nodes (for incremental update)
  ///@param to_delete -- list of deleted nodes (for incremental update)
  ///@param persist -- whether to store results
  ///
  ///@return -- {node id, community label} pairs
  std::unordered_map<std::uint64_t, std::int64_t> CalculateLabels(
      std::unique_ptr<mg_graph::Graph<>> &&graph, const std::unordered_set<std::uint64_t> &changed_nodes = {},
      const std::unordered_set<std::uint64_t> &to_delete = {}, bool persist = true);

 public:
  ///@brief Creates an instance of the LabelRankT algorithm.
  LabelRankT() = default;

  ///@brief Returns previously calculated community labels.
  /// If no calculation has been done previously, calculates community labels
  /// with default parameter values.
  ///
  ///@param graph -- current graph
  ///
  ///@return -- {node id, community label} pairs
  std::unordered_map<std::uint64_t, std::int64_t> GetLabels(std::unique_ptr<mg_graph::Graph<>> &&graph);

  ///@brief Calculates and returns community labels using LabelRankT. The labels
  /// and the parameters for their calculation are reused in online
  /// community detection with UpdateLabels().
  ///
  ///@param graph -- current graph
  ///@param is_weighted -- whether graph is directed
  ///@param is_weighted -- whether graph is weighted
  ///@param similarity_threshold -- similarity threshold used in the node
  /// selection step, values in [0, 1]
  ///@param exponent -- exponent used in the inflation step
  ///@param min_value -- smallest acceptable probability in the cutoff step
  ///@param weight_property -- weight-containing edge property’s name
  ///@param w_selfloop -- default weight of self-loops
  ///@param max_iterations -- maximum number of iterations
  ///@param max_updates -- maximum number of updates for any node
  std::unordered_map<std::uint64_t, std::int64_t> SetLabels(std::unique_ptr<mg_graph::Graph<>> &&graph,
                                                            bool is_directed = false, bool is_weighted = false,
                                                            double similarity_threshold = 0.7, double exponent = 4.0,
                                                            double min_value = 0.1,
                                                            std::string weight_property = "weight",
                                                            double w_selfloop = 1.0, std::uint64_t max_iterations = 100,
                                                            std::uint64_t max_updates = 5);

  ///@brief Updates changed nodes’ community labels with LabelRankT.
  /// The maximum numbers of iterations and updates are reused from previous
  /// CalculateLabels() calls. If no calculation has been done previously,
  /// calculates community labels with default parameter values.
  ///
  ///@param graph -- current graph
  ///@param updated_nodes -- list of updated (added, modified) nodes
  ///@param updated_edges -- list of updated (added, modified) edges
  ///@param deleted_nodes -- list of deleted nodes
  ///@param deleted_edges -- list of deleted edges
  ///
  ///@return -- {node id, community label} pairs
  std::unordered_map<std::uint64_t, std::int64_t> UpdateLabels(
      std::unique_ptr<mg_graph::Graph<>> &&graph, const std::vector<std::uint64_t> &modified_nodes = {},
      const std::vector<std::pair<std::uint64_t, std::uint64_t>> &modified_edges = {},
      const std::vector<std::uint64_t> &deleted_nodes = {},
      const std::vector<std::pair<std::uint64_t, std::uint64_t>> &deleted_edges = {});
};
}  // namespace LabelRankT
