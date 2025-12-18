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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <deque>
#include <future>
#include <memory>
#include <random>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "data_structures/graph_view.hpp"
#include "leiden.hpp"
#include "leiden_utils/leiden_utils.hpp"
namespace leiden_alg {

const double MAX_DOUBLE = std::numeric_limits<double>::max();

///
/// @brief Moves nodes to the best-fitting community based on edge weights and modularity criteria.
///
/// This function iteratively evaluates each node in the graph and attempts to move it to a neighboring community
/// that maximizes its connection strength, as defined by the edge weights and a modularity parameter (gamma).
/// The process uses a fast approach by shuffling the nodes and visiting them in a randomized order, allowing
/// for efficient community updates while minimizing the number of redundant calculations. After evaluating
/// all neighbors, the node is moved to the best community if it results in a better modularity gain.
///
/// @param partitions The current partition structure containing communities and their members.
/// @param graph The graph being analyzed, consisting of nodes and edges.
/// @param gamma A parameter that influences the modularity calculation during community assignments.
/// @param resolution_parameter A threshold value that determines the minimum improvement required to move a node.
/// @return The number of empty communities created during the process.
///
/// @note The function modifies the `partitions` structure directly, updating the community assignments of nodes
/// and maintaining the integrity of the partitioning process.
///
std::uint64_t MoveNodesFast(Partitions &partitions, const Graph &graph, double gamma, double resolution_parameter) {
  std::deque<std::uint64_t> nodes;
  std::unordered_set<std::uint64_t> nodes_set;
  std::vector<double> edge_weights_per_community(partitions.communities.size(), 0);
  std::vector<char> visited(partitions.communities.size(), false);
  std::vector<std::uint64_t> neighbor_communities(partitions.communities.size(), 0);
  std::uint64_t number_of_empty_communities = 0;
  nodes_set.reserve(graph.Size());

  for (std::uint64_t i = 0; i < graph.Size(); i++) {
    nodes.push_back(i);
    nodes_set.insert(i);
  }
  static std::mt19937 gen(std::random_device{}());
  std::shuffle(nodes.begin(), nodes.end(), gen);

  while (!nodes.empty()) {
    auto node_id = nodes.front();
    nodes.pop_front();
    nodes_set.erase(node_id);
    auto best_community = partitions.GetCommunityForNode(node_id);
    const auto current_community = best_community;

    std::uint64_t number_of_neighbor_communities = 0;

    // find neighbor communities and calculate edge weights per each community
    for (const auto &[neighbor_id, weight] : graph.Neighbors(node_id)) {
      const auto community_id_of_neighbor = partitions.GetCommunityForNode(neighbor_id);
      if (!visited[community_id_of_neighbor]) {
        visited[community_id_of_neighbor] = 1;
        neighbor_communities[number_of_neighbor_communities++] = community_id_of_neighbor;
      }
      edge_weights_per_community[community_id_of_neighbor] += weight;
    }

    // delta - current modularity gain, (-1 because don't count the node itself as a part of the community)
    const auto current_delta = static_cast<double>(edge_weights_per_community[best_community]) -
                               (static_cast<double>(partitions.GetCommunityWeight(best_community)) - 1) * gamma;
    auto best_delta = current_delta;
    for (std::uint64_t i = 0; i < number_of_neighbor_communities; i++) {
      const auto community_id_of_neighbor = neighbor_communities[i];

      // look only at the neighbors that are not in the same community
      if (community_id_of_neighbor != best_community) {
        const auto delta = edge_weights_per_community[community_id_of_neighbor] -
                           static_cast<double>(partitions.GetCommunityWeight(community_id_of_neighbor)) * gamma;
        if (delta > best_delta + resolution_parameter) {
          best_delta = delta;
          best_community = community_id_of_neighbor;
        }
      }

      // reset edge weight for the community and mark it as not visited -> important for the next iteration
      edge_weights_per_community[community_id_of_neighbor] = 0.0;
      visited[community_id_of_neighbor] = 0;
    }

    if (current_community != best_community) {
      // remove the node from the current community
      auto &community = partitions.communities[partitions.GetCommunityForNode(node_id)];
      auto iterator = std::find(community.begin(), community.end(), node_id);

      std::iter_swap(iterator, community.end() - 1);
      community.pop_back();
      if (community.empty()) {
        number_of_empty_communities++;
      }

      partitions.communities[best_community].push_back(node_id);
      partitions.community_id[node_id] = best_community;

      // add neighbors to the queue
      for (const auto &[neighbor_id, _] : graph.Neighbors(node_id)) {
        // if the neighbor is not in the queue and it's not in the same community as the node, add it to the queue
        if (nodes_set.find(neighbor_id) == nodes_set.end() &&
            partitions.GetCommunityForNode(neighbor_id) != best_community) {
          nodes.push_back(neighbor_id);
          nodes_set.insert(neighbor_id);
        }
      }
    }
  }

  return number_of_empty_communities;
}

///
/// @brief Puts all nodes into singleton communities.
/// @param graph The graph to be partitioned.
/// @return A partitioning of nodes into singleton communities.
///
Partitions CreateSingletonPartition(const Graph &graph) {
  Partitions partitions;
  for (std::uint64_t i = 0; i < graph.Size(); i++) {
    partitions.communities.push_back({i});
    partitions.community_id.push_back(i);
  }
  return partitions;
}

///
/// @brief Creates intermediary communities for the hierarchical structure of communities.
/// @param partitions Partitions of nodes into communities.
/// @param node_id The identifier of the node.
/// @return True if node is in a singleton community, false otherwise.
///
bool IsInSingletonCommunity(const Partitions &partitions, std::uint64_t node_id) {
  auto community_id = partitions.GetCommunityForNode(node_id);
  return partitions.communities[community_id].size() == 1;
}

///
/// @brief Merges nodes from a specified subset of a partition into new communities based on edge weights and modularity
/// criteria.
///
/// This function refines partitions by considering nodes in the given subset of the graph and attempting to merge them
/// into neighboring communities. It uses edge weights between communities, modularity criteria, and probabilistic
/// merging to determine the optimal community for each node. The process ensures that empty communities are tracked,
/// because they will be used later to determine if the partitioning process should stop.
///
/// @param refined_partitions The partition structure to be updated with new community assignments.
/// @param graph The graph being analyzed, with nodes and edges.
/// @param subset The index of the community subset currently being processed for node merging.
/// @param partitions The original partition containing the nodes to be merged.
/// @param gamma A parameter controlling the influence of community size during merging.
/// @param theta A parameter used in probabilistic merging to scale the impact of modularity changes.
/// @param resolution_parameter A threshold used to decide when merging is beneficial.
/// @param number_of_empty_communities A reference to track how many communities become empty after merging.
///
/// @note This function operates on a subset of nodes, updating both the partition and external edge weights to reflect
/// the new structure.
///
void MergeNodesSubset(Partitions &refined_partitions, const Graph &graph, const Partitions &original_partitions,
                      std::uint64_t subset, double gamma, double theta, double resolution_parameter,
                      std::uint64_t &number_of_empty_communities) {
  // TODO (DavIvek): Consider making some kind of mappings from subset to original partitions -> this vectors could be
  // smaller external_edge_weight[i] - tracks the external edge weight between nodes in cluster i and the other clusters
  // in the same subset
  std::vector<double> external_edge_weight(refined_partitions.communities.size(), 0);
  std::vector<std::uint64_t> neighbor_communities(refined_partitions.communities.size(), 0);
  std::vector<char> visited(refined_partitions.communities.size(), false);
  std::vector<double> edge_weights(refined_partitions.communities.size(),
                                   0);  // edge weight from the node to the community
  std::vector<double> probability_of_merging(refined_partitions.communities.size(), 0);
  double subset_weight = 0;

  const auto &subset_community = original_partitions.communities[subset];

  // at the beginning all nodes are in singleton communities
  for (const auto &node_id : subset_community) {
    for (const auto &[neighbor_id, edge_weight] : graph.Neighbors(node_id)) {
      if (original_partitions.GetCommunityForNode(neighbor_id) == subset) {
        external_edge_weight[refined_partitions.GetCommunityForNode(node_id)] += edge_weight;
      }
    }
    subset_weight += graph.GetNodeWeight(node_id);
  }

  for (const auto &node_id : subset_community) {
    const auto current_community = refined_partitions.GetCommunityForNode(node_id);
    const auto node_weight = graph.GetNodeWeight(node_id);
    auto number_of_neighbor_communities = 0;
    // check if node is in a singleton community (that means it hasn't been moved to a different community yet) and if
    // node is a well connected node
    // S - subset, v - node (vertex)
    // gamma * |v| * (|S| - |v|) -> right term in the equation
    const auto right_term = gamma * node_weight * (subset_weight - node_weight);
    if (IsInSingletonCommunity(refined_partitions, node_id) && external_edge_weight[current_community] >= right_term) {
      auto total_cum_sum = 0.0;

      auto max_delta = 0.0;
      auto best_community = current_community;

      // find neighbor communities and calculate edge weights per each community
      for (const auto &[neighbor_id, weight] : graph.Neighbors(node_id)) {
        if (original_partitions.GetCommunityForNode(neighbor_id) == subset) {
          const auto refined_community = refined_partitions.GetCommunityForNode(neighbor_id);
          edge_weights[refined_community] += weight;
          if (!visited[refined_community]) {
            visited[refined_community] = 1;
            neighbor_communities[number_of_neighbor_communities++] = refined_community;
          }
        }
      }

      // it's important that this goes from 0 to neighbor_communities.size() because we need to update first
      // neighbor_communities.size() elements of probability_of_merging
      for (auto j = 0; j < number_of_neighbor_communities; j++) {
        const auto neighbor_community = neighbor_communities[j];
        if (refined_partitions.GetCommunityWeight(neighbor_community) == 0) continue;  // skip empty communities

        // check if the community is a well connected community
        // C - current community, S - subset
        // gamma * |C| * (|S| - |C|) -> right term in the equation
        const auto right_term =
            gamma * static_cast<double>(refined_partitions.GetCommunityWeight(neighbor_community)) *
            static_cast<double>((subset_community.size() - refined_partitions.GetCommunityWeight(neighbor_community)));
        if (external_edge_weight[neighbor_community] >= right_term) {
          // delta - current modularity gain
          const auto delta = edge_weights[neighbor_community] -
                             static_cast<double>(refined_partitions.GetCommunityWeight(neighbor_community)) * gamma;

          if (delta > resolution_parameter) {
            total_cum_sum += std::exp(delta / theta);
          }

          if (delta > max_delta + resolution_parameter) {
            max_delta = delta;
            best_community = neighbor_community;
          }
        }
        // reset edge weight for the community and mark it as not visited -> important for the next iteration
        probability_of_merging[j] = total_cum_sum;
        edge_weights[neighbor_community] = 0.0;
        visited[neighbor_community] = 0;
      }
      if (total_cum_sum < MAX_DOUBLE) {
        static std::minstd_rand gen(std::random_device{}());
        std::uniform_real_distribution<double> dis(0, total_cum_sum);
        const auto random_number = dis(gen);

        // find the best community based on the random number
        const auto last = probability_of_merging.begin() + static_cast<std::int64_t>(number_of_neighbor_communities);
        const auto best_community_index =
            std::lower_bound(probability_of_merging.begin(), last, random_number) - probability_of_merging.begin();
        best_community = neighbor_communities[best_community_index];
      }

      refined_partitions.communities[best_community].push_back(node_id);
      refined_partitions.community_id[node_id] = best_community;

      // remove the node from the current community, note that it is a singleton community
      refined_partitions.communities[current_community].clear();
      number_of_empty_communities++;

      // update the external edge weight for the new community and edge weights for the communities
      for (const auto &[neighbor_id, weight] : graph.Neighbors(node_id)) {
        const auto neighbor_community = refined_partitions.GetCommunityForNode(neighbor_id);
        if (neighbor_community == subset) {
          // update external edge weight for the new community
          if (neighbor_community == best_community) {
            external_edge_weight[best_community] -= weight;
          } else {
            external_edge_weight[best_community] += weight;
          }
        }
      }
    }
  }
}

///
/// @brief Refines the current partitioning of the graph by attempting to merge nodes between communities.
///
/// This function iterates through the current partitions and refines them by merging nodes that belong to communities
/// larger than one node. It applies the merge operation based on a modularity optimization, influenced by the
/// parameters gamma, theta, and resolution. The refined partitions are returned, along with the number of empty
/// communities detected during the process.
///
/// @param partitions The current set of partitions representing node communities.
/// @param graph The graph on which the partitions are being refined.
/// @param gamma Controls the resolution for modularity optimization; higher values favor smaller communities.
/// @param theta A parameter influencing the merging of nodes between communities.
/// @param resolution_parameter A parameter that adjusts the resolution of the communities.
/// @return A pair containing the refined partitions and the number of empty communities resulting from the refinement.
///
/// @note Communities with only one node are ignored during refinement.
///
std::pair<Partitions, std::uint64_t> RefinePartition(const Partitions &original_partitions, const Graph &graph,
                                                     double gamma, double theta, double resolution_parameter) {
  auto refined_partitions = CreateSingletonPartition(graph);
  std::uint64_t number_of_empty_communities = 0;
  for (std::uint64_t i = 0; i < original_partitions.communities.size(); i++) {
    if (original_partitions.communities[i].size() > 1) {
      MergeNodesSubset(refined_partitions, graph, original_partitions, i, gamma, theta, resolution_parameter,
                       number_of_empty_communities);
    }
  }
  return {refined_partitions, number_of_empty_communities};
}

///
/// @brief Aggregates the graph by replacing nodes with communities and builds a new partition and adjacency list.
///
/// This function remaps the detected communities to form a new graph, where each community is treated as a new node.
/// The adjacency list is rebuilt to reflect edges between these communities, and the new partitions are created based
/// on the aggregation of nodes into their respective communities. Additionally, the function updates the intermediary
/// dendrogram structure for hierarchical community detection.
///
/// @param refined_partitions The refined partitions of the graph from a previous iteration.
/// @param graph The graph being analyzed, whose nodes are aggregated into communities.
/// @param original_partitions The original partitioning of the graph.
/// @param intermediary_communities The hierarchical structure of communities, updated with the new level.
/// @param current_level The current level of the dendrogram hierarchy being built.
/// @return A pair containing the new partitions and the aggregated graph with communities as nodes.
///
/// @note This function leverages parallelism via `std::async` for creating the intermediary communities, as it operates
///       on data that does not interfere with the main graph processing loop.
///
std::pair<Partitions, Graph> AggregateGraph(const Partitions &refined_partitions, const Graph &graph,
                                            const Partitions &original_partitions, Dendrogram &intermediary_communities,
                                            std::uint64_t current_level) {
  std::vector<std::vector<std::uint64_t>> remapped_communities;  // nodes and communities should go from 0 to n
  std::unordered_map<std::uint64_t, std::uint64_t>
      old_community_to_new_community;  // old_community_id -> new_community_id
  std::uint64_t new_community_id = 0;
  Partitions new_partitions;
  Graph aggregated_graph;

  intermediary_communities.emplace_back();
  // remap communities to go from 0 to n -> this is important since we are using vectors everywhere and positions are
  // important
  for (std::uint64_t i = 0; i < refined_partitions.communities.size(); i++) {
    const auto &community = refined_partitions.communities[i];
    if (!community.empty()) {
      remapped_communities.push_back(community);
      old_community_to_new_community[i] = new_community_id;
      new_community_id++;
    }
  }

  // we can run this in parallel since we are not modifying the same data as in the main thread
  auto future = std::async(std::launch::async, CreateIntermediaryCommunities, std::ref(intermediary_communities),
                           std::ref(remapped_communities), current_level);

  std::unordered_map<std::uint64_t, std::unordered_set<std::uint64_t>>
      edge_exists;  // keep track of already added edges

  // now we now that max size of the new adjacency list is new_community_id because communities become nodes
  aggregated_graph.adjacency_list.resize(new_community_id);
  aggregated_graph.node_weights.resize(new_community_id, 0.0);

  // create new adjacency list -> if there is an edge between two communities, add it to the new adjacency list
  for (std::uint64_t i = 0; i < graph.adjacency_list.size(); i++) {
    const auto community_id = refined_partitions.GetCommunityForNode(i);
    const auto new_community_id = old_community_to_new_community[community_id];
    for (const auto &[neighbor_id, weight] : graph.adjacency_list[i]) {
      const auto neighbor_community_id = refined_partitions.GetCommunityForNode(neighbor_id);
      const auto new_neighbor_community_id = old_community_to_new_community[neighbor_community_id];

      if (new_community_id == new_neighbor_community_id)
        continue;  // if the nodes are in the same community, we don't need to add the edge
      // first check if the edge already exists
      auto edge_exists_iter = edge_exists.find(new_community_id);
      if (edge_exists_iter != edge_exists.end() &&
          edge_exists_iter->second.find(new_neighbor_community_id) != edge_exists_iter->second.end())
        continue;  // edge already exists

      // add edge to the adjacency list, update node weights and add the edge to the edge_exists
      edge_exists[new_community_id].insert(new_neighbor_community_id);
      edge_exists[new_neighbor_community_id].insert(new_community_id);
      aggregated_graph.AddEdge(new_community_id, new_neighbor_community_id, weight);
      aggregated_graph.AddEdge(new_neighbor_community_id, new_community_id, weight);
      aggregated_graph.UpdateNodeWeight(new_community_id, weight);
      aggregated_graph.UpdateNodeWeight(new_neighbor_community_id, weight);
    }
  }

  new_community_id = 0;
  new_partitions.communities.reserve(remapped_communities.size());
  new_partitions.community_id.resize(graph.Size(), 0);

  // create new partitions -> communities that are a subset of the original community are added to the new partitions as
  // a part of the same community
  for (const auto &community_list : original_partitions.communities) {
    bool new_community_created = false;
    for (const auto &community_id : community_list) {
      // if community was added to remapped communities that means it wasn't empty
      const auto remapped_community_id = old_community_to_new_community.find(community_id);
      if (remapped_community_id != old_community_to_new_community.end()) {
        if (!new_community_created) {
          new_partitions.communities.emplace_back();
          new_community_created = true;
        }
        new_partitions.communities[new_community_id].push_back(remapped_community_id->second);
        new_partitions.community_id[remapped_community_id->second] = new_community_id;
      }
    }
    if (new_community_created) {
      new_community_id++;
    }
  }

  future.wait();

  return {new_partitions, aggregated_graph};
}

///
/// @brief Checks if all communities are singleton communities.
/// @param partitions The partitioning of nodes into communities.
/// @return True if all communities are singleton communities, false otherwise.
///
bool AllSingletonCommunities(const Partitions &partitions) {
  return std::ranges::all_of(partitions.communities, [](const auto &community) { return community.size() == 1; });
}

///
/// @brief Implements the Leiden community detection algorithm to find hierarchical communities in a graph.
///
/// This function takes a graph from Memgraph, converts it to an internal graph representation, and iteratively refines
/// the partitioning of nodes into communities. The Leiden algorithm is used to move nodes between communities to
/// optimize modularity and merge them in subsequent steps. The algorithm works on undirected graphs, and it returns a
/// dendrogram, which represents the hierarchical structure of communities across multiple levels.
///
/// The process involves the following steps:
/// - Initializing each node in its own community (singleton partition).
/// - Iteratively moving nodes between communities to optimize the modularity function, adjusted by the parameters
/// gamma,
///   theta, and resolution.
/// - Refining the partitions to merge communities inside the same subset (which is a community in the original
/// partition).
/// - Creating new levels in the dendrogram as the community structure is aggregated into coarser levels.
/// - The process stops when all nodes are placed in singleton communities, or when the maximum number of iterations is
/// reached.
///
/// @param memgraph_graph The graph from Memgraph to be analyzed.
/// @param gamma Controls the resolution of the detected communities; higher values favor smaller communities.
/// @param theta A parameter influencing community merging, particularly based on edge modularity.
/// @param resolution_parameter A parameter to adjust the granularity of the communities.
/// @param max_iterations The maximum number of iterations for the algorithm.
/// @return A dendrogram representing hierarchical communities, where each level captures a coarser aggregation of
/// nodes.
///
Dendrogram Leiden(const mg_graph::GraphView<> &memgraph_graph, double gamma, double theta, double resolution_parameter,
                  std::uint64_t max_iterations) {
  if (memgraph_graph.Nodes().empty() || memgraph_graph.Edges().empty()) {
    return {};
  }

  Graph graph(memgraph_graph.Nodes().size());
  Partitions partitions;
  Dendrogram intermediary_communities;  // level -> community_ids
  intermediary_communities.emplace_back();
  intermediary_communities[0].reserve(memgraph_graph.Nodes().size());
  std::uint64_t level = 0;
  double sum_of_weights = 0.0;
  boost::unordered_map<std::pair<std::uint64_t, std::uint64_t>, bool> edge_exists;
  edge_exists.reserve(memgraph_graph.Edges().size());

  for (const auto &[id, from, to] : memgraph_graph.Edges()) {
    const auto edge_weight = memgraph_graph.IsWeighted() ? memgraph_graph.GetWeight(id)
                                                         : 1.0;  // Make it positive and cast to Double, fixed to 1.0
    // always add the edge, because the algorithm works on undirected graphs

    if (edge_exists.find(std::make_pair(from, to)) != edge_exists.end() ||
        edge_exists.find(std::make_pair(to, from)) != edge_exists.end()) {
      continue;
    }

    graph.AddEdge(from, to, edge_weight);
    graph.AddEdge(to, from, edge_weight);
    graph.UpdateNodeWeight(from, edge_weight);
    graph.UpdateNodeWeight(to, edge_weight);
    edge_exists[std::make_pair(to, from)] = true;
    sum_of_weights += edge_weight;
  }
  edge_exists.clear();
  bool any_isolated_nodes = memgraph_graph.Nodes().size() != graph.Size();

  // initialize partitions and leafs of the dendrogram
  partitions = CreateSingletonPartition(graph);
  intermediary_communities[0].reserve(memgraph_graph.Nodes().size());
  for (std::uint64_t i = 0; i < memgraph_graph.Nodes().size(); i++) {
    intermediary_communities[0].push_back(
        std::make_shared<IntermediaryCommunityId>(IntermediaryCommunityId{i, 0, nullptr}));
  }

  gamma /= sum_of_weights;
  bool done = false;
  std::uint64_t number_of_iterations = 0;
  while (!done) {
    number_of_iterations++;
    const auto empty_communities = MoveNodesFast(partitions, graph, gamma, resolution_parameter);
    done = AllSingletonCommunities(partitions);
    if (!done) {
      auto [refined_partitions, refined_empty_communities] =
          RefinePartition(partitions, graph, gamma, theta, resolution_parameter);
      // if there are no isolated nodes and we have only one non-empty community, we can stop -> otherwise, we will end
      // up with all nodes in the same community which doesn't make sense
      if (refined_empty_communities == (refined_partitions.communities.size() - 1) && !any_isolated_nodes) {
        done = true;
      }
      // this means that no nodes were moved to a different community -> we will use communities from MoveNodesFast
      else if (refined_empty_communities == 0) {
        // this check is mandatory because we can end up with all nodes in the same community
        if (empty_communities == partitions.communities.size() - 1) {
          done = true;
          continue;
        }
        refined_partitions = std::move(partitions);
        std::tie(partitions, graph) =
            AggregateGraph(refined_partitions, graph, refined_partitions, intermediary_communities, level);
      } else {
        std::tie(partitions, graph) =
            AggregateGraph(refined_partitions, graph, partitions, intermediary_communities, level);
      }
      level++;
    }
    if (number_of_iterations >= max_iterations) {
      done = true;
    }
  }

  return intermediary_communities;
}

std::vector<std::vector<std::uint64_t>> GetCommunities(const mg_graph::GraphView<> &graph, double gamma, double theta,
                                                       double resolution_parameter, std::uint64_t max_iterations) {
  std::vector<std::vector<std::uint64_t>> node_and_community_hierarchy;  // node_id -> list of community_ids
  node_and_community_hierarchy.reserve(graph.Nodes().size());
  const auto communities_hierarchy = Leiden(graph, gamma, theta, resolution_parameter, max_iterations);
  if (!communities_hierarchy.empty()) {
    for (const auto &node : communities_hierarchy[0]) {
      std::vector<std::uint64_t> community_ids;
      community_ids.reserve(communities_hierarchy.size());
      auto current_community = node->parent;  // we don't need the leaf, since it's the same as the node_id
      while (current_community != nullptr) {
        community_ids.push_back(current_community->community_id);
        current_community = current_community->parent;
      }

      if (community_ids.empty()) {
        throw std::runtime_error("No communities detected.");
      }
      node_and_community_hierarchy.emplace_back(std::move(community_ids));
    }
  }

  return node_and_community_hierarchy;
}

}  // namespace leiden_alg
