// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

#include <algorithm>
#include <memory>
#include <queue>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <omp.h>

///@brief Remove repeat elements from vector except for the first instance. The vector is modified in-place.
///
///@param vector Vector to be operated on
template <typename T>
void RemoveDuplicates(std::vector<T> &vector);

namespace online_bc {
enum class Operation { CREATE_EDGE, CREATE_NODE, CREATE_ATTACH_NODE, DELETE_EDGE, DELETE_NODE, DETACH_DELETE_NODE };

struct BFSResult {
  std::unordered_map<std::uint64_t, std::uint64_t> n_shortest_paths;
  std::unordered_map<std::uint64_t, std::uint64_t> distances;
  std::unordered_map<std::uint64_t, std::set<std::uint64_t>> predecessors;
  std::vector<std::uint64_t> bfs_order;
};

/// Returns Memgraph IDs of nodes neighboring the target node.
///
/// @param node_id target node id
std::unordered_set<std::uint64_t> NeighborsMemgraphIDs(const mg_graph::GraphView<> &graph, const std::uint64_t node_id);

class OnlineBC {
 private:
  /// Maps nodes (i.e. Memgraph IDs) to their betweenness centrality scores
  std::unordered_map<std::uint64_t, double> node_bc_scores;

  /// Betweenness centrality score initialization flag
  bool initialized = false;

  /// Avoids counting edges twice in case of undirected graphs
  static constexpr double NO_DOUBLE_COUNT = 2.0;

  ///@brief Returns whether previously calculated betweenness centrality scores are inconsistent with the current graph,
  /// i.e. if the nodes which have associated betweenness centrality scores are not the same as the graph’s nodes.
  ///
  ///@param graph Current graph
  bool Inconsistent(const mg_graph::GraphView<> &graph) const;

  ///@brief Normalize each node’s betweenness centrality score by the number of node pairs not
  /// containing said node. The normalization constant is 2/((N-1)(N-2)) for undirected and 1/((N-1)(N-2)) for directed
  /// graphs, with N being the number of graph nodes.
  ///
  ///@param node_bc_scores Graph nodes with associated betweenness centrality scores
  ///@param graph_order Order (Nº of nodes) of the graph
  ///
  ///@return Normalized betweenness centrality scores
  std::unordered_map<std::uint64_t, double> NormalizeBC(const std::unordered_map<std::uint64_t, double> &node_bc_scores,
                                                        const std::uint64_t graph_order) const;

  ///@brief Wrapper for the offline betweenness centrality algorithm that maps betweenness centrality scores to their
  /// nodes’ Memgraph IDs.
  ///
  ///@param graph Current graph
  ///@param threads Number of concurrent threads
  void CallBrandesAlgorithm(const mg_graph::GraphView<> &graph, const std::uint64_t threads);

  ///@brief Returns whether the (undirected) graph is connected.
  ///
  ///@param graph Current graph
  bool Connected(const mg_graph::GraphView<> &graph);

  ///@brief Returns the nodes and the articulation points of the biconnected component affected by the update.
  ///
  ///@param graph Graph containing affected edge (new graph if edge was created, old graph if edge was deleted)
  ///@param updated_edge Created/deleted edge
  ///
  ///@return (nodes, articulation points) in the affected biconnected component
  std::tuple<std::unordered_set<std::uint64_t>, std::unordered_set<std::uint64_t>> IsolateAffectedBCC(
      const mg_graph::GraphView<> &graph, const std::pair<std::uint64_t, std::uint64_t> updated_edge) const;

  ///@brief Computes lengths of shortest paths between a given node and all other nodes in the graph, restricted to
  /// one biconnected component.
  ///
  ///@param graph Graph traversed by the algorithm
  ///@param start_id Start node ID
  ///@param bcc_nodes Nodes in the biconnected component
  ///
  ///@return {node ID, start->node distance} pairs
  std::unordered_map<std::uint64_t, std::uint64_t> SSSPLengths(
      const mg_graph::GraphView<> &graph, const std::uint64_t start_id,
      const std::unordered_set<std::uint64_t> &bcc_nodes) const;

  ///@brief For each articulation point of the biconnected component affected by the update, returns the order (Nº
  /// of nodes) of the portion of the graph reachable from the articulation point through edges outside that component.
  /// The graph is traversed using breadth-first search.
  ///
  ///@param graph Graph traversed by the algorithm
  ///@param affected_bcc_articulation_points Articulation points in the affected biconnected component
  ///@param affected_bcc_nodes Nodes in the affected biconnected component
  ///
  ///@return {articulation point ID, attached subgraph order} pairs
  std::unordered_map<std::uint64_t, std::uint64_t> PeripheralSubgraphOrders(
      const mg_graph::GraphView<> &graph, std::unordered_set<std::uint64_t> affected_bcc_articulation_points,
      std::unordered_set<std::uint64_t> affected_bcc_nodes) const;

  ///@brief Performs a breadth-first search from a given node in the Brandes and iCentral algorithms. Search can be
  /// restricted to one biconnected component.
  ///
  ///@param graph Graph traversed by the BFS
  ///@param start_id Start node ID
  ///@param compensate_for_deleted_node If true, guarantees correct results for the DETACH_DELETE_NODE operation
  ///@param affected_bcc_only Nodes in the affected biconnected component
  ///@param affected_bcc_nodes Nodes in the affected biconnected component
  ///
  ///@return Nº of shortest paths to each node from the start node
  /// Distances to each node from the start node
  /// Each node’s immediate predecessors on the shortest paths from the start node
  /// IDs of nodes visited by the BFS, in reverse order
  BFSResult BFS(const mg_graph::GraphView<> &graph, const std::uint64_t start_id,
                const bool compensate_for_deleted_node = false, const bool affected_bcc_only = false,
                const std::unordered_set<std::uint64_t> &affected_bcc_nodes = {}) const;

  ///@brief Updates the breadth-first search’s data structures after insertion of an edge, starting out from the more
  /// distant node of that edge. Search is restricted to one biconnected component.
  ///
  ///@param graph Graph traversed by the BFS
  ///@param updated_edge Created/deleted edge
  ///@param affected_bcc_nodes Nodes in the affected biconnected component
  ///@param start_id_initial Start node ID (initial BFS)
  ///@param n_shortest_paths_initial Nº of shortest paths to each node from the start node (initial BFS)
  ///@param distances_initial Distances to each node from the start node (initial BFS)
  ///@param predecessors_initial Each node’s immediate predecessors on the shortest paths from the start node (initial
  /// BFS)
  ///
  ///@return Updated: Nº of shortest paths to each node from the start node
  /// Updated: distances to each node from the start node
  /// Updated: each node’s immediate predecessors on the shortest paths from the start node
  /// IDs of nodes visited by the partial BFS, in reverse order
  BFSResult PartialBFS(const mg_graph::GraphView<> &graph, const std::pair<std::uint64_t, std::uint64_t> updated_edge,
                       const std::unordered_set<std::uint64_t> &affected_bcc_nodes,
                       const std::uint64_t start_id_initial,
                       const std::unordered_map<std::uint64_t, std::uint64_t> &n_shortest_paths_initial,
                       const std::unordered_map<std::uint64_t, std::uint64_t> &distances_initial,
                       const std::unordered_map<std::uint64_t, std::set<std::uint64_t>> &predecessors_initial) const;

  ///@brief As the order from PartialBFS() contains only nodes whose distance changes after the update, this function
  /// adds remaining nodes from BFS() in correct order (descending by distance).
  ///
  ///@param initial_order IDs of nodes visited by the initial BFS, in reverse order
  ///@param initial_distances Distances to each node from the start node
  ///@param partial_order IDs of nodes visited by the partial BFS, in reverse order
  ///@param updated_distances Distances to each node from the start node, updated by partial BFS
  ///
  ///@return Node IDs sorted in descending order by node’s distance from the start node
  std::vector<std::uint64_t> MergeBFSOrders(
      const std::vector<std::uint64_t> &initial_order,
      const std::unordered_map<std::uint64_t, std::uint64_t> &initial_distances,
      const std::vector<std::uint64_t> &partial_order,
      const std::unordered_map<std::uint64_t, std::uint64_t> &updated_distances) const;

  ///@brief Performs an iteration of iCentral that updates the betweenness centrality scores in two steps:
  /// 1) subtracts given node’s old graph contribution to other nodes’ betweenness centrality scores,
  /// 2) adds given node’s new graph contribution to other nodes’ betweenness centrality scores.
  ///
  ///@param graph Graph without the updated edge
  ///@param operation Type of graph update (one of {CREATE_EDGE, DELETE_EDGE})
  ///@param s_id ID of the node whose contribution to other nodes’ betweenness centrality scores has changed
  ///@param affected_bcc_nodes Nodes in the affected biconnected component
  ///@param affected_bcc_articulation_points Articulation points in the affected biconnected component
  ///@param updated_edge Created/deleted edge (needed for partial BFS)
  ///@param peripheral_subgraph_orders {node ID, order of subgraph reachable from node} pairs
  void iCentralIteration(const mg_graph::GraphView<> &graph, const Operation operation, const std::uint64_t s_id,
                         const std::unordered_set<std::uint64_t> &affected_bcc_nodes,
                         const std::unordered_set<std::uint64_t> &affected_bcc_articulation_points,
                         const std::pair<std::uint64_t, std::uint64_t> updated_edge,
                         const std::unordered_map<std::uint64_t, std::uint64_t> &peripheral_subgraph_orders);

 public:
  OnlineBC() = default;

  ///@brief Getter for checking initialization status
  ///
  inline bool Initialized() const { return this->initialized; };

  ///@brief Computes initial betweenness centrality scores with Brandes’ algorithm.
  ///
  ///@param graph Current graph
  ///@param normalize If true, normalizes each node’s betweenness centrality score by the number of node pairs not
  /// containing said node. The normalization constant is 2/((N-1)(N-2)) for undirected and 1/((N-1)(N-2)) for directed
  /// graphs, with N being the number of graph nodes.
  ///@param threads Number of concurrent threads
  ///
  ///@return {node ID, BC score} pairs
  std::unordered_map<std::uint64_t, double> Set(const mg_graph::GraphView<> &graph, const bool normalize = true,
                                                const std::uint64_t threads = std::thread::hardware_concurrency());

  ///@brief Returns previously computed betweenness centrality scores.
  /// If this->computed flag is set to false, computes betweenness centrality scores with default parameter values.
  ///
  ///@param graph Current graph
  ///@param normalize If true, normalizes each node’s betweenness centrality score by the number of node pairs not
  /// containing said node. The normalization constant is 2/((N-1)(N-2)) for undirected and 1/((N-1)(N-2)) for directed
  /// graphs, with N being the number of graph nodes.
  ///
  ///@return {node ID, BC score} pairs
  std::unordered_map<std::uint64_t, double> Get(const mg_graph::GraphView<> &graph, const bool normalize = true) const;

  ///@brief Uses iCentral to recompute betweenness centrality scores after edge updates.
  ///
  ///@param prior_graph Graph as before the last update
  ///@param current_graph Current graph
  ///@param operation Type of graph update (one of {CREATE_EDGE, DELETE_EDGE})
  ///@param updated_edge Created/deleted edge
  ///@param normalize If true, normalizes each node’s betweenness centrality score by the number of node pairs not
  /// containing said node. The normalization constant is 2/((N-1)(N-2)) for undirected and 1/((N-1)(N-2)) for directed
  /// graphs, with N being the number of graph nodes.
  ///@param threads Number of concurrent threads
  ///
  ///@return {node ID, BC score} pairs
  std::unordered_map<std::uint64_t, double> EdgeUpdate(
      const mg_graph::GraphView<> &prior_graph, const mg_graph::GraphView<> &current_graph, const Operation operation,
      const std::pair<std::uint64_t, std::uint64_t> updated_edge, const bool normalize = true,
      const std::uint64_t threads = std::thread::hardware_concurrency());

  ///@brief Uses a single iteration of Brandes’ algorithm to recompute betweenness centrality scores after updates
  /// consisting of an edge and a node solely connected to it.
  ///
  ///@param current_graph Current graph
  ///@param operation Type of graph update (one of {CREATE_ATTACH_NODE, DETACH_DELETE_NODE})
  ///@param updated_node Created/deleted node
  ///@param updated_edge Created/deleted edge
  ///@param normalize If true, normalizes each node’s betweenness centrality score by the number of node pairs not
  /// containing said node. The normalization constant is 2/((N-1)(N-2)) for undirected and 1/((N-1)(N-2)) for directed
  /// graphs, with N being the number of graph nodes.
  ///
  ///@return {node ID, BC score} pairs
  std::unordered_map<std::uint64_t, double> NodeEdgeUpdate(const mg_graph::GraphView<> &current_graph,
                                                           const Operation operation, const std::uint64_t updated_node,
                                                           const std::pair<std::uint64_t, std::uint64_t> updated_edge,
                                                           const bool normalize = true);

  ///@brief Adds or removes the betweenness centrality score entry of created/deleted nodes with degree 0.
  ///
  ///@param operation Type of graph update (one of {CREATE_NODE, DELETE_NODE})
  ///@param updated_node Created/deleted node
  ///@param normalize If true, normalizes each node’s betweenness centrality score by the number of node pairs not
  /// containing said node. The normalization constant is 2/((N-1)(N-2)) for undirected and 1/((N-1)(N-2)) for directed
  /// graphs, with N being the number of graph nodes.
  ///
  ///@return {node ID, BC score} pairs
  std::unordered_map<std::uint64_t, double> NodeUpdate(const Operation operation, const std::uint64_t updated_node,
                                                       const bool normalize = true);

  ///@brief Resets the state of the algorithm (initialization and previously calculated node_bc_scores).
  void Reset();
};
}  // namespace online_bc
