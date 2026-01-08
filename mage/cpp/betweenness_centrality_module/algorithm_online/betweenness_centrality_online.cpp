// Copyright 2026 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

#include <mg_graph.hpp>
#include <ranges>

#include "../../biconnected_components_module/algorithm/biconnected_components.hpp"
#include "../algorithm/betweenness_centrality.hpp"
#include "betweenness_centrality_online.hpp"

template <typename T>
void RemoveDuplicates(std::vector<T> &vector) {
  std::unordered_set<T> seen;

  auto new_end = std::remove_if(vector.begin(), vector.end(), [&seen](const T &value) {
    if (seen.contains(value)) return true;

    seen.insert(value);
    return false;
  });

  vector.erase(new_end, vector.end());
}

namespace online_bc {
std::unordered_set<std::uint64_t> NeighborsMemgraphIDs(const mg_graph::GraphView<> &graph,
                                                       const std::uint64_t node_id) {
  std::unordered_set<std::uint64_t> neighbors_memgraph_ids;
  for (const auto &neighbor : graph.Neighbours(graph.GetInnerNodeId(node_id))) {
    neighbors_memgraph_ids.insert(graph.GetMemgraphNodeId(neighbor.node_id));
  }
  return neighbors_memgraph_ids;
}

bool OnlineBC::Inconsistent(const mg_graph::GraphView<> &graph) const {
  if (graph.Nodes().size() != this->node_bc_scores.size()) return true;

  return std::ranges::any_of(graph.Nodes(), [this, &graph](const auto &node) {
    return !this->node_bc_scores.contains(graph.GetMemgraphNodeId(node.id));
  });
}

std::unordered_map<std::uint64_t, double> OnlineBC::NormalizeBC(
    const std::unordered_map<std::uint64_t, double> &node_bc_scores, const std::uint64_t graph_order) {
  const double normalization_factor = 2.0 / static_cast<double>((graph_order - 1) * (graph_order - 2));
  std::unordered_map<std::uint64_t, double> normalized_bc_scores;
  for (const auto [node_id, bc_score] : node_bc_scores) {
    normalized_bc_scores[node_id] = bc_score * normalization_factor;
  }

  return normalized_bc_scores;
}

void OnlineBC::CallBrandesAlgorithm(const mg_graph::GraphView<> &graph, const std::uint64_t threads) {
  this->node_bc_scores.clear();

  const auto bc_scores =
      betweenness_centrality_alg::BetweennessCentrality(graph, false, false, static_cast<int>(threads));
  for (std::uint64_t node_id = 0; node_id < graph.Nodes().size(); ++node_id) {
    this->node_bc_scores[graph.GetMemgraphNodeId(node_id)] = bc_scores[node_id];
  }
}

bool OnlineBC::Connected(const mg_graph::GraphView<> &graph) {
  const auto start_node_id = graph.Nodes().front().id;
  std::unordered_set<std::uint64_t> visited({start_node_id});

  std::queue<std::uint64_t> queue({start_node_id});
  while (!queue.empty()) {
    const auto current_id = queue.front();
    queue.pop();

    for (const auto neighbor : graph.Neighbours(current_id)) {
      if (!visited.contains(neighbor.node_id)) {
        queue.push(neighbor.node_id);
        visited.insert(neighbor.node_id);
      }
    }
  }

  return visited.size() == graph.Nodes().size();
}

std::tuple<std::unordered_set<std::uint64_t>, std::unordered_set<std::uint64_t>> OnlineBC::IsolateAffectedBCC(
    const mg_graph::GraphView<> &graph, const std::pair<std::uint64_t, std::uint64_t> updated_edge) {
  std::unordered_set<std::uint64_t> articulation_points_by_bcc;
  std::vector<std::unordered_set<std::uint64_t>> nodes_by_bcc;
  const auto edges_by_bcc = bcc_algorithm::GetBiconnectedComponents(graph, articulation_points_by_bcc, nodes_by_bcc);

  std::unordered_set<std::uint64_t> affected_bcc_nodes;
  std::unordered_set<std::uint64_t> affected_bcc_articulation_points;

  const std::pair<std::uint64_t, std::uint64_t> updated_edge_inner(
      {graph.GetInnerNodeId(updated_edge.first), graph.GetInnerNodeId(updated_edge.second)});

  for (std::size_t i = 0; i < edges_by_bcc.size(); i++) {
    if (std::any_of(edges_by_bcc[i].begin(), edges_by_bcc[i].end(), [updated_edge_inner](auto &edge) {
          return edge.from == updated_edge_inner.first && edge.to == updated_edge_inner.second;  // if edge in BCC
        })) {
      for (const auto node : nodes_by_bcc[i]) {
        affected_bcc_nodes.insert(graph.GetMemgraphNodeId(node));
      }
      for (const auto node : articulation_points_by_bcc) {
        if (affected_bcc_nodes.contains(node)) affected_bcc_articulation_points.insert(node);
      }
    }
  }

  return {affected_bcc_nodes, affected_bcc_articulation_points};
}

std::unordered_map<std::uint64_t, std::uint64_t> OnlineBC::SSSPLengths(
    const mg_graph::GraphView<> &graph, const std::uint64_t start_id,
    const std::unordered_set<std::uint64_t> &affected_bcc_nodes) {
  std::unordered_map<std::uint64_t, std::uint64_t> distances;

  std::queue<std::uint64_t> queue({start_id});
  while (!queue.empty()) {
    const auto current_id = queue.front();
    queue.pop();

    for (const auto neighbor_id : NeighborsMemgraphIDs(graph, current_id)) {
      if (!affected_bcc_nodes.contains(neighbor_id)) continue;

      if (!distances.contains(neighbor_id)) {  // if unvisited
        queue.push(neighbor_id);
        distances[neighbor_id] = distances[current_id] + 1;
      }
    }
  }

  return distances;
}

std::unordered_map<std::uint64_t, std::uint64_t> OnlineBC::PeripheralSubgraphOrders(
    const mg_graph::GraphView<> &graph, std::unordered_set<std::uint64_t> affected_bcc_articulation_points,
    std::unordered_set<std::uint64_t> affected_bcc_nodes) {
  std::unordered_map<std::uint64_t, std::uint64_t> peripheral_subgraph_orders;
  for (const auto articulation_point_id : affected_bcc_articulation_points) {
    std::unordered_set<std::uint64_t> visited({articulation_point_id});

    std::queue<std::uint64_t> queue({articulation_point_id});
    while (!queue.empty()) {
      const auto current_id = queue.front();
      queue.pop();

      for (const auto neighbor_id : NeighborsMemgraphIDs(graph, current_id)) {
        if (affected_bcc_nodes.contains(neighbor_id)) continue;

        if (!visited.contains(neighbor_id)) {
          queue.push(neighbor_id);
          visited.insert(neighbor_id);
        }
      }
    }

    visited.erase(articulation_point_id);
    peripheral_subgraph_orders[articulation_point_id] = visited.size();
  }

  return peripheral_subgraph_orders;
}

BFSResult OnlineBC::BFS(const mg_graph::GraphView<> &graph, const std::uint64_t start_id,
                        const bool compensate_for_deleted_node, const bool affected_bcc_only,
                        const std::unordered_set<std::uint64_t> &affected_bcc_nodes) {
  std::unordered_map<std::uint64_t, std::uint64_t> n_shortest_paths({{start_id, 1}});
  std::unordered_map<std::uint64_t, std::uint64_t> distances;
  std::unordered_map<std::uint64_t, std::set<std::uint64_t>> predecessors;
  std::vector<std::uint64_t> bfs_order({start_id});

  std::queue<std::uint64_t> queue({start_id});
  while (!queue.empty()) {
    const auto current_id = queue.front();
    queue.pop();

    for (const auto neighbor_id : NeighborsMemgraphIDs(graph, current_id)) {
      if (affected_bcc_only && !affected_bcc_nodes.contains(neighbor_id)) continue;

      if (!distances.contains(neighbor_id)) {  // if unvisited
        distances[neighbor_id] = distances[current_id] + 1;

        queue.push(neighbor_id);
        bfs_order.push_back(neighbor_id);
      }

      if (distances[neighbor_id] == distances[current_id] + 1) {
        n_shortest_paths[neighbor_id] += n_shortest_paths[current_id];
        predecessors[neighbor_id].insert(current_id);
      }
    }
  }

  if (!compensate_for_deleted_node) n_shortest_paths[start_id] = 0;
  predecessors[start_id] = std::set<std::uint64_t>();
  std::ranges::reverse(bfs_order);

  BFSResult result = {.n_shortest_paths = n_shortest_paths,
                      .distances = distances,
                      .predecessors = predecessors,
                      .bfs_order = bfs_order};

  return result;
}

BFSResult OnlineBC::PartialBFS(const mg_graph::GraphView<> &graph,
                               const std::pair<std::uint64_t, std::uint64_t> updated_edge,
                               const std::unordered_set<std::uint64_t> &affected_bcc_nodes,
                               const std::uint64_t start_id_initial,
                               const std::unordered_map<std::uint64_t, std::uint64_t> &n_shortest_paths_initial,
                               const std::unordered_map<std::uint64_t, std::uint64_t> &distances_initial,
                               const std::unordered_map<std::uint64_t, std::set<std::uint64_t>> &predecessors_initial) {
  // Partial BFS starts from the updated edge’s node that is further away from initial BFS’s start node
  std::uint64_t start_id = 0;
  std::uint64_t before_start_id = 0;
  if (distances_initial.empty() ||
      distances_initial.at(updated_edge.first) < distances_initial.at(updated_edge.second)) {
    start_id = updated_edge.second;
    before_start_id = updated_edge.first;
  } else {
    start_id = updated_edge.first;
    before_start_id = updated_edge.second;
  }

  std::unordered_map<std::uint64_t, std::int64_t> delta_n_shortest_paths;  // Δ can be negative
  std::unordered_map<std::uint64_t, std::uint64_t> n_shortest_paths = n_shortest_paths_initial;
  std::unordered_map<std::uint64_t, std::uint64_t> distances = distances_initial;
  std::unordered_map<std::uint64_t, std::set<std::uint64_t>> predecessors = predecessors_initial;
  std::vector<std::uint64_t> bfs_order({start_id});

  n_shortest_paths[start_id_initial] = 1;

  // Recompute data structures for the start node
  if (distances_initial.empty() ||
      distances_initial.at(start_id) > distances_initial.at(before_start_id) + 1) {  // New path shorter than before
    delta_n_shortest_paths[start_id] =
        static_cast<std::int64_t>(n_shortest_paths[before_start_id] - n_shortest_paths[start_id]);
    n_shortest_paths[start_id] = n_shortest_paths[before_start_id];
    distances[start_id] = distances_initial.at(before_start_id) + 1;
    predecessors[start_id] = {before_start_id};
  } else if (distances_initial.at(start_id) == distances_initial.at(before_start_id) + 1) {  // New same-length path
    delta_n_shortest_paths[start_id] = static_cast<std::int64_t>(n_shortest_paths[before_start_id]);
    n_shortest_paths[start_id] += delta_n_shortest_paths[start_id];
    predecessors[start_id].insert({before_start_id});
  }

  std::queue<std::uint64_t> queue({start_id});
  while (!queue.empty()) {
    const auto current_id = queue.front();
    queue.pop();

    for (const auto neighbor_id : NeighborsMemgraphIDs(graph, current_id)) {
      if (!affected_bcc_nodes.contains(neighbor_id)) continue;

      if (distances[neighbor_id] < distances[current_id] + 1) {  // New path not the shortest
        continue;
      }
      if (distances[neighbor_id] == distances[current_id] + 1) {  // New same-length path
        delta_n_shortest_paths[neighbor_id] = (distances_initial.at(neighbor_id) <= distances_initial.at(current_id))
                                                  ? static_cast<std::int64_t>(n_shortest_paths[current_id])
                                                  : static_cast<std::int64_t>(delta_n_shortest_paths[current_id]);
        n_shortest_paths[neighbor_id] += delta_n_shortest_paths[neighbor_id];
        predecessors[neighbor_id].insert(current_id);

        queue.push(neighbor_id);
        bfs_order.push_back(neighbor_id);
      } else if (distances[neighbor_id] > distances[current_id] + 1) {  // New path shorter than before
        delta_n_shortest_paths[neighbor_id] =
            delta_n_shortest_paths[current_id] - static_cast<std::int64_t>(n_shortest_paths[neighbor_id]);
        n_shortest_paths[neighbor_id] = n_shortest_paths[current_id];
        distances[neighbor_id] = distances[current_id] + 1;
        predecessors[neighbor_id] = {current_id};

        queue.push(neighbor_id);
        bfs_order.push_back(neighbor_id);
      }
    }
  }

  n_shortest_paths[start_id_initial] = 0;
  predecessors[start_id_initial] = std::set<std::uint64_t>();

  std::ranges::reverse(bfs_order);
  RemoveDuplicates(bfs_order);

  return BFSResult{.n_shortest_paths = n_shortest_paths,
                   .distances = distances,
                   .predecessors = predecessors,
                   .bfs_order = bfs_order};
}

std::vector<std::uint64_t> OnlineBC::MergeBFSOrders(
    const std::vector<std::uint64_t> &initial_order,
    const std::unordered_map<std::uint64_t, std::uint64_t> &initial_distances,
    const std::vector<std::uint64_t> &partial_order,
    const std::unordered_map<std::uint64_t, std::uint64_t> &updated_distances) {
  std::unordered_set<std::uint64_t> overlap{partial_order.begin(), partial_order.end()};
  std::vector<std::uint64_t> initial_order_no_overlap;
  std::ranges::copy_if(initial_order, std::back_inserter(initial_order_no_overlap),
                       [&overlap](const std::uint64_t node_id) { return !overlap.contains(node_id); });

  std::vector<std::uint64_t> merged_order;
  merged_order.reserve(initial_order.size());
  auto longest_distances_first = [&initial_distances, &updated_distances](const std::uint64_t node_from_initial,
                                                                          const std::uint64_t node_from_partial) {
    return initial_distances.at(node_from_initial) > updated_distances.at(node_from_partial);
  };
  std::ranges::merge(initial_order_no_overlap, partial_order, std::back_inserter(merged_order),
                     longest_distances_first);

  return merged_order;
}

void OnlineBC::iCentralIteration(const mg_graph::GraphView<> &graph, const Operation operation,
                                 const std::uint64_t s_id, const std::unordered_set<std::uint64_t> &affected_bcc_nodes,
                                 const std::unordered_set<std::uint64_t> &affected_bcc_articulation_points,
                                 const std::pair<std::uint64_t, std::uint64_t> updated_edge,
                                 const std::unordered_map<std::uint64_t, std::uint64_t> &peripheral_subgraph_orders) {
  // Avoid counting edges twice since the graph is undirected
  const double divisor = NO_DOUBLE_COUNT;

  const auto [n_shortest_paths_no_edge, distances_no_edge, predecessors_no_edge, reverse_bfs_order_no_edge] =
      BFS(graph, s_id, false, true, affected_bcc_nodes);
  const auto [n_shortest_paths_with_edge, distances_with_edge, predecessors_with_edge,
              partial_reverse_bfs_order_with_edge] =
      PartialBFS(graph, updated_edge, affected_bcc_nodes, s_id, n_shortest_paths_no_edge, distances_no_edge,
                 predecessors_no_edge);
  const auto reverse_bfs_order_with_edge = MergeBFSOrders(reverse_bfs_order_no_edge, distances_no_edge,
                                                          partial_reverse_bfs_order_with_edge, distances_with_edge);

  std::unordered_map<std::uint64_t, std::uint64_t> n_shortest_paths_prior;
  std::unordered_map<std::uint64_t, std::uint64_t> n_shortest_paths_current;
  std::unordered_map<std::uint64_t, std::uint64_t> distances_prior;
  std::unordered_map<std::uint64_t, std::uint64_t> distances_current;
  std::unordered_map<std::uint64_t, std::set<std::uint64_t>> predecessors_prior;
  std::unordered_map<std::uint64_t, std::set<std::uint64_t>> predecessors_current;
  std::vector<std::uint64_t> reverse_bfs_order_prior;
  std::vector<std::uint64_t> reverse_bfs_order_current;
  if (operation == Operation::CREATE_EDGE) {
    n_shortest_paths_prior = n_shortest_paths_no_edge;
    distances_prior = distances_no_edge;
    predecessors_prior = predecessors_no_edge;
    reverse_bfs_order_prior = reverse_bfs_order_no_edge;
    n_shortest_paths_current = n_shortest_paths_with_edge;
    distances_current = distances_with_edge;
    predecessors_current = predecessors_with_edge;
    reverse_bfs_order_current = reverse_bfs_order_with_edge;
  } else if (operation == Operation::DELETE_EDGE) {
    n_shortest_paths_prior = n_shortest_paths_with_edge;
    distances_prior = distances_with_edge;
    predecessors_prior = predecessors_with_edge;
    reverse_bfs_order_prior = reverse_bfs_order_with_edge;
    n_shortest_paths_current = n_shortest_paths_no_edge;
    distances_current = distances_no_edge;
    predecessors_current = predecessors_no_edge;
    reverse_bfs_order_current = reverse_bfs_order_no_edge;
  }

  // Step 1: remove s_id’s contribution to the betweenness centrality scores in the prior graph

  std::unordered_map<std::uint64_t, double> dependency_s_on;
  std::unordered_map<std::uint64_t, double> ext_dependency_s_on;

  for (const auto w_id : reverse_bfs_order_prior) {
    if (affected_bcc_articulation_points.contains(s_id) && affected_bcc_articulation_points.contains(w_id)) {
      ext_dependency_s_on[w_id] = static_cast<double>(peripheral_subgraph_orders.at(s_id)) *
                                  static_cast<double>(peripheral_subgraph_orders.at(w_id));
    }

    for (const auto p_id : predecessors_prior.at(w_id)) {
      auto coefficient =
          static_cast<double>(n_shortest_paths_prior.at(p_id)) / static_cast<double>(n_shortest_paths_prior.at(w_id));
      dependency_s_on[p_id] += (coefficient * (1 + dependency_s_on[w_id]));
      if (affected_bcc_articulation_points.contains(s_id)) {
        ext_dependency_s_on[p_id] += (ext_dependency_s_on[w_id] * coefficient);
      }
    }

    if (s_id != w_id) {
#pragma omp atomic update
      this->node_bc_scores[w_id] -= dependency_s_on[w_id] / divisor;
    }

    if (affected_bcc_articulation_points.contains(s_id)) {
#pragma omp atomic update
      this->node_bc_scores[w_id] -= dependency_s_on[w_id] * static_cast<double>(peripheral_subgraph_orders.at(s_id));
#pragma omp atomic update
      this->node_bc_scores[w_id] -= ext_dependency_s_on[w_id] / divisor;
    }
  }

  // Step 2: add s_id’s contribution to the betweenness centrality scores in the current graph

  dependency_s_on.clear();
  ext_dependency_s_on.clear();

  for (const auto w_id : reverse_bfs_order_current) {
    if (affected_bcc_articulation_points.contains(s_id) && affected_bcc_articulation_points.contains(w_id)) {
      ext_dependency_s_on[w_id] = static_cast<double>(peripheral_subgraph_orders.at(s_id)) *
                                  static_cast<double>(peripheral_subgraph_orders.at(w_id));
    }

    for (const auto p_id : predecessors_current.at(w_id)) {
      auto coefficient = static_cast<double>(n_shortest_paths_current.at(p_id)) /
                         static_cast<double>(n_shortest_paths_current.at(w_id));
      dependency_s_on[p_id] += coefficient * (1 + dependency_s_on[w_id]);
      if (affected_bcc_articulation_points.contains(s_id)) {
        ext_dependency_s_on[p_id] += ext_dependency_s_on[w_id] * coefficient;
      }
    }

    if (s_id != w_id) {
#pragma omp atomic update
      this->node_bc_scores[w_id] += dependency_s_on[w_id] / divisor;
    }

    if (affected_bcc_articulation_points.contains(s_id)) {
#pragma omp atomic update
      this->node_bc_scores[w_id] += dependency_s_on[w_id] * static_cast<double>(peripheral_subgraph_orders.at(s_id));
#pragma omp atomic update
      this->node_bc_scores[w_id] += ext_dependency_s_on[w_id] / divisor;
    }
  }
}

std::unordered_map<std::uint64_t, double> OnlineBC::Set(const mg_graph::GraphView<> &graph, const bool normalize,
                                                        const std::uint64_t threads) {
  CallBrandesAlgorithm(graph, threads);
  this->initialized = true;

  if (normalize) return NormalizeBC(this->node_bc_scores, graph.Nodes().size());

  return this->node_bc_scores;
}

std::unordered_map<std::uint64_t, double> OnlineBC::Get(const mg_graph::GraphView<> &graph,
                                                        const bool normalize) const {
  if (Inconsistent(graph)) {
    throw std::runtime_error(
        "Graph has been modified and is thus inconsistent with cached betweenness centrality scores; to update them, "
        "please call set/reset!");
  }

  if (normalize) return NormalizeBC(this->node_bc_scores, graph.Nodes().size());

  return this->node_bc_scores;
}

std::unordered_map<std::uint64_t, double> OnlineBC::EdgeUpdate(
    const mg_graph::GraphView<> &prior_graph, const mg_graph::GraphView<> &current_graph, const Operation operation,
    const std::pair<std::uint64_t, std::uint64_t> updated_edge, const bool normalize, const std::uint64_t threads) {
  if (operation == Operation::CREATE_EDGE) {
    const bool first_endpoint_isolated = prior_graph.Neighbours(prior_graph.GetInnerNodeId(updated_edge.first)).empty();
    const bool second_endpoint_isolated =
        prior_graph.Neighbours(prior_graph.GetInnerNodeId(updated_edge.second)).empty();

    if (first_endpoint_isolated && second_endpoint_isolated) {
      return Set(current_graph, normalize, threads);
    }
    if (first_endpoint_isolated) {
      return NodeEdgeUpdate(current_graph, Operation::CREATE_ATTACH_NODE, updated_edge.first, updated_edge, normalize);
    }
    if (second_endpoint_isolated) {
      return NodeEdgeUpdate(current_graph, Operation::CREATE_ATTACH_NODE, updated_edge.second, updated_edge, normalize);
    }
    if (!Connected(prior_graph)) {
      return Set(current_graph, normalize, threads);
    }
  }

  const mg_graph::GraphView<> &graph_without_updated_edge =
      (operation == Operation::CREATE_EDGE) ? prior_graph : current_graph;
  const mg_graph::GraphView<> &graph_with_updated_edge =
      (operation == Operation::CREATE_EDGE) ? current_graph : prior_graph;

  std::unordered_set<std::uint64_t> articulation_points_by_bcc;
  std::vector<std::unordered_set<std::uint64_t>> nodes_by_bcc;
  std::vector<std::vector<mg_graph::Edge<>>> edges_by_bcc;
  edges_by_bcc =
      bcc_algorithm::GetBiconnectedComponents(graph_with_updated_edge, articulation_points_by_bcc, nodes_by_bcc);

  // Extract tuple values explicitly (OpenMP doesn't support structured bindings)
  const auto affected_bcc_result = IsolateAffectedBCC(graph_with_updated_edge, updated_edge);
  const auto &affected_bcc_nodes = std::get<0>(affected_bcc_result);
  const auto &affected_bcc_articulation_points = std::get<1>(affected_bcc_result);

  const auto distances_first = SSSPLengths(graph_with_updated_edge, updated_edge.first, affected_bcc_nodes);
  const auto distances_second = SSSPLengths(graph_with_updated_edge, updated_edge.second, affected_bcc_nodes);

  const auto peripheral_subgraph_orders =
      PeripheralSubgraphOrders(prior_graph, affected_bcc_articulation_points, affected_bcc_nodes);

  // OpenMP might throw errors when iterating over STL containers
  const auto array_size = affected_bcc_nodes.size();
  std::vector<std::uint64_t> affected_bcc_nodes_array;
  affected_bcc_nodes_array.reserve(array_size);
  std::ranges::copy(affected_bcc_nodes, std::back_inserter(affected_bcc_nodes_array));

  omp_set_dynamic(0);
  omp_set_num_threads(static_cast<int>(threads));
#pragma omp parallel for
  for (std::uint64_t i = 0; i < array_size; i++) {
    auto node_id = affected_bcc_nodes_array[i];
    if (distances_first.at(node_id) != distances_second.at(node_id)) {
      iCentralIteration(graph_without_updated_edge, operation, node_id, affected_bcc_nodes,
                        affected_bcc_articulation_points, updated_edge, peripheral_subgraph_orders);
    }
  }

  if (normalize) return NormalizeBC(this->node_bc_scores, current_graph.Nodes().size());

  return this->node_bc_scores;
}

std::unordered_map<std::uint64_t, double> OnlineBC::NodeEdgeUpdate(
    const mg_graph::GraphView<> &current_graph, const Operation operation, const std::uint64_t updated_node,
    const std::pair<std::uint64_t, std::uint64_t> updated_edge, const bool normalize) {
  std::unordered_map<std::uint64_t, double> dependency;

  std::uint64_t start_id = updated_node;  // Start node for Brandes BFS

  const bool compensate_for_deleted_node = (operation == Operation::DETACH_DELETE_NODE);

  // Get existing node if start node is deleted
  if (operation == Operation::DETACH_DELETE_NODE) {
    if (updated_edge.first == updated_node)
      start_id = updated_edge.second;
    else
      start_id = updated_edge.first;
  }

  auto [n_shortest_paths, _, predecessors, visited] = BFS(current_graph, start_id, compensate_for_deleted_node);

  for (const auto current_id : visited) {
    for (auto p_id : predecessors[current_id]) {
      auto coefficient =
          static_cast<double>(n_shortest_paths[p_id]) / static_cast<double>(n_shortest_paths[current_id]);
      dependency[p_id] += coefficient * (1 + dependency[current_id]);
    }

    if (current_id != updated_node) {
      if (operation == Operation::CREATE_ATTACH_NODE)
        this->node_bc_scores[current_id] += dependency[current_id];
      else if (operation == Operation::DETACH_DELETE_NODE)
        this->node_bc_scores[current_id] -= dependency[current_id];
    }
  }

  if (operation == Operation::CREATE_ATTACH_NODE) {
    this->node_bc_scores[updated_node] = 0;
  } else if (operation == Operation::DETACH_DELETE_NODE) {
    this->node_bc_scores.erase(updated_node);
  }

  if (normalize) return NormalizeBC(this->node_bc_scores, current_graph.Nodes().size());

  return this->node_bc_scores;
}

std::unordered_map<std::uint64_t, double> OnlineBC::NodeUpdate(const Operation operation,
                                                               const std::uint64_t updated_node, const bool normalize) {
  if (operation == Operation::CREATE_NODE) {
    this->node_bc_scores[updated_node] = 0;
  } else if (operation == Operation::DELETE_NODE) {
    this->node_bc_scores.erase(updated_node);
  }

  if (normalize) return NormalizeBC(this->node_bc_scores, this->node_bc_scores.size());

  return this->node_bc_scores;
}

void OnlineBC::Reset() {
  this->node_bc_scores.clear();
  this->initialized = false;
}

}  // namespace online_bc
