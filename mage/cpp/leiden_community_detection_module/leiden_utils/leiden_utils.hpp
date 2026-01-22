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

#include <memory>
#include <vector>

#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>

namespace leiden_alg {

///
/// @brief A struct representing a graph using an adjacency list.
///
/// The graph is stored as a vector of adjacency lists, where each index corresponds to a node_id.
/// Each adjacency list contains pairs of neighboring node identifiers and the weights of edges to those neighbors.
///
struct Graph {
  Graph() = default;
  explicit Graph(std::uint64_t size) : adjacency_list(size), node_weights(size, 0.0) {}

  ///
  /// @brief Adds a directed edge between two nodes in the graph with an optional weight.
  ///
  /// @param u The identifier of the source node.
  /// @param v The identifier of the destination node.
  /// @param edge_weight The weight of the edge (default is 1.0).
  ///
  void AddEdge(std::uint64_t u, std::uint64_t v, double edge_weight = 1.0) {
    adjacency_list[u].emplace_back(v, edge_weight);
  }

  ///
  /// @brief Checks if a given node is present in the graph.
  ///
  /// @param u The identifier of the node.
  /// @return True if the node is present, false otherwise.
  ///
  bool IsVertexInGraph(std::uint64_t u) const { return u < adjacency_list.size(); }

  ///
  /// @brief Returns the number of nodes in the graph.
  ///
  /// @return The size of the graph (number of nodes).
  ///
  std::size_t Size() const { return adjacency_list.size(); }

  ///
  /// @brief Retrieves the neighbors and edge weights of a given node.
  ///
  /// @param node_id The identifier of the node whose neighbors are requested.
  /// @return A constant reference to a vector of pairs representing the neighbors and the edge weights.
  ///
  const std::vector<std::pair<std::uint64_t, double>> &Neighbors(std::uint64_t node_id) const {
    return adjacency_list[node_id];
  }

  ///
  /// @brief Updates the weight of a given node.
  ///
  /// @param node_id The identifier of the node.
  /// @param weight The new weight of the node.
  ///
  void UpdateNodeWeight(std::uint64_t node_id, double weight = 1.0) { node_weights[node_id] += weight; }

  ///
  /// @brief Retrieves the weight of a given node.
  ///
  /// @param node_id The identifier of the node.
  /// @return The weight of the node.
  ///
  double GetNodeWeight(std::uint64_t node_id) const { return node_weights[node_id]; }

  std::vector<std::vector<std::pair<std::uint64_t, double>>> adjacency_list;  ///< node_id -> (neighbor_id, edge_weight)
  std::vector<double> node_weights;                                           ///< node_id -> node_weight
};

///
/// @brief A struct representing a partitioning of nodes into communities.
///
/// The partitions are stored in two ways:
/// - A vector of vectors where each community_id maps to the nodes within that community.
/// - A vector that maps each node_id to its community_id.
///
struct Partitions {
  std::vector<std::vector<std::uint64_t>> communities;  ///< community_id -> node_ids within the community.
  std::vector<std::uint64_t> community_id;              ///< node_id -> community_id.

  ///
  /// @brief Retrieves the community identifier for a given node.
  ///
  /// @param node_id The identifier of the node.
  /// @return The community identifier that the node belongs to.
  ///
  std::uint64_t GetCommunityForNode(std::uint64_t node_id) const { return community_id[node_id]; }

  ///
  /// @brief Retrieves the weight (size) of a given community.
  ///
  /// @param community_id The identifier of the community.
  /// @return The number of nodes in the community.
  ///
  std::uint64_t GetCommunityWeight(std::uint64_t community_id) const { return communities[community_id].size(); }
};

///
/// @brief A struct representing an intermediary community in a hierarchical clustering process.
///
/// This struct is used in the construction of a dendrogram (a tree-like structure) where each level of the tree
/// represents a different stage of community detection. Each intermediary community contains:
/// - A community_id: The identifier for this community.
/// - A level: The current level of the community in the hierarchy.
/// - A parent: A pointer to the parent community at the previous level.
///
struct IntermediaryCommunityId {
  std::uint64_t community_id;                       ///< The identifier of the community.
  std::uint64_t level;                              ///< The level of this community in the hierarchy.
  std::shared_ptr<IntermediaryCommunityId> parent;  ///< A shared pointer to the parent community.
};

///
/// @brief Alias for a dendrogram, a hierarchical structure of communities.
///
/// A dendrogram is represented as a vector of vectors, where each inner vector contains pointers to
/// intermediary community identifiers. Each level of the dendrogram corresponds to a different stage
/// in the hierarchical community detection process.
///
using Dendrogram = std::vector<std::vector<std::shared_ptr<IntermediaryCommunityId>>>;

///
/// @brief Creates intermediary communities at a given level in the dendrogram.
///
/// This method constructs the hierarchy for the dendrogram by assigning community identifiers and setting the parent
/// relationships for the current level.
///
/// @param intermediary_communities A reference to the dendrogram, which is updated with new intermediary communities.
/// @param communities The current set of communities, represented as a vector of node_ids.
/// @param current_level The level at which the intermediary communities are being created.
///
void CreateIntermediaryCommunities(Dendrogram &intermediary_communities,
                                   const std::vector<std::vector<std::uint64_t>> &communities,
                                   std::uint64_t current_level);

}  // namespace leiden_alg
