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

#include "katz.hpp"
#include <algorithm>
#include <queue>
#include <ranges>

namespace katz_alg {
namespace {

class KatzCentralityData {
 public:
  ///@brief Clear the algorithm context (the data structures used by it)
  void Clear() {
    centralities.clear();
    omegas.clear();
    active_nodes.clear();
    iteration = 0;
  }

  ///@brief Initialize the context
  ///@param graph Current graph
  template <typename T>
  void Init(const T &graph) {
    Clear();

    // Initialize the centrality vector
    std::unordered_map<std::uint64_t, double> centrality;
    InitVertexMap(centrality, 0.0, graph);
    centralities.emplace_back(std::move(centrality));

    // Initialize the omega
    std::unordered_map<std::uint64_t, double> omega;
    InitVertexMap(omega, 1.0, graph);
    omegas.emplace_back(std::move(omega));

    // Initialize the lower bound vector
    InitVertexMap(lr, 0.0, graph);

    // Initialize the upper bound vector
    InitVertexMap(ur, 0.0, graph);
  }

  ///@brief Initialize next iteration’s context and increment iteration count
  ///@param graph Current graph
  template <typename T>
  void AddIteration(const T &graph) {
    iteration++;

    // Initialize the centrality vector
    std::unordered_map<std::uint64_t, double> centrality;
    InitVertexMap(centrality, 0.0, graph);
    centralities.emplace_back(std::move(centrality));

    // Initialize the omega
    std::unordered_map<std::uint64_t, double> omega;
    InitVertexMap(omega, 0.0, graph);
    omegas.emplace_back(std::move(omega));
  }

  bool IsInitialized() const { return !centralities.empty() && iteration > 0; }

  // Each node’s centrality in each iteration
  std::vector<std::unordered_map<std::uint64_t, double>> centralities;
  // Each node’s out-degree in each iteration
  std::vector<std::unordered_map<std::uint64_t, double>> omegas;

  // Each node’s centrality lower bound
  std::unordered_map<std::uint64_t, double> lr;
  // Each node’s centrality upper bound
  std::unordered_map<std::uint64_t, double> ur;

  // Nodes used to determine convergence
  std::set<std::uint64_t> active_nodes;

  // Current iteration
  std::uint64_t iteration = 0;

 private:
  ///@brief Initialize per-node maps with default values (using Memgraph IDs)
  ///@param map Target data structure
  ///@param default_value Default value
  ///@param graph Current graph
  static void InitVertexMap(std::unordered_map<std::uint64_t, double> &map, const double default_value,
                            const mg_graph::GraphView<> &graph) {
    for (const auto &[_v] : graph.Nodes()) {
      const auto v = graph.GetMemgraphNodeId(_v);  // Convert a node’s inner ID to Memgraph ID
      map[v] = default_value;
    }
  }

  /// @brief Above function's C++ API equivalent
  static void InitVertexMap(std::unordered_map<std::uint64_t, double> &map, const double default_value,
                            const mgp::Graph &graph) {
    for (const auto &node : graph.Nodes()) {
      map[node.Id().AsUint()] = default_value;
    }
  }
};

/// Algorithm context
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
KatzCentralityData context;

/// Attenuation factor
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
double alpha;

/// Active nodes cutoff (for determining convergence)
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::uint64_t k;

/// Separation tolerance factor
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
double epsilon;

std::vector<std::pair<std::uint64_t, double>> WrapResults() {
  return {katz_alg::context.centralities[katz_alg::context.iteration].begin(),
          katz_alg::context.centralities[katz_alg::context.iteration].end()};
}

/// @brief Returns the highest degree node’s degree
/// @param graph Current graph
/// @return Node degree
std::uint64_t MaxDegree(const mg_graph::GraphView<> &graph) {
  std::vector<std::size_t> deg_vector;

  // Calculate the graph degree vector
  std::transform(graph.Nodes().begin(), graph.Nodes().end(), std::back_inserter(deg_vector),
                 [&graph](mg_graph::Node<> node) -> std::size_t { return graph.Neighbours(node.id).size(); });
  const auto deg_max = *std::ranges::max_element(deg_vector);

  return deg_max;
}

/// @brief Above function’s C++ API equivalent
std::uint64_t MaxDegree(const mgp::Graph &graph) {
  std::uint64_t max_degree = 0;
  for (const auto &node : graph.Nodes()) {
    std::uint64_t degree = 0;
    for (const auto &relationship : node.OutRelationships()) {
      degree++;
    }
    max_degree = std::max(degree, max_degree);
  }
  return max_degree;
}

/// @brief Determines convergence by testing whether the upper and lower bounds of active nodes are ϵ-separated.
/// @param active_nodes Active nodes
/// @param k Active nodes cutoff
/// @param epsilon Separation tolerance factor
/// @return Whether the algorithm converged
bool Converged(std::set<std::uint64_t> &active_nodes, std::uint64_t k, const double epsilon) {
  // Fetch current iteration data
  const auto centrality = katz_alg::context.centralities[context.iteration];
  const auto lr = katz_alg::context.lr;
  const auto ur = katz_alg::context.ur;

  // TODO: this is a controversial decision
  k = centrality.size();

  // Create the active centrality vector
  std::vector<std::pair<std::uint64_t, double>> active_centrality;
  active_centrality.reserve(active_nodes.size());
  for (const auto m : active_nodes) {
    active_centrality.emplace_back(m, centrality.at(m));
  }

  // Sort the first `k` centralities in descending order
  const auto k_sorted = std::min(static_cast<std::ptrdiff_t>(k), static_cast<std::ptrdiff_t>(active_centrality.size()));
  std::partial_sort(active_centrality.begin(), active_centrality.begin() + k_sorted, active_centrality.end(),
                    [](std::pair<std::uint64_t, double> a, std::pair<std::uint64_t, double> b) -> bool {
                      return a.second > b.second;
                    });

  // Deactivate nodes which are ϵ-separated from the top `k` highest lower bound nodes
  for (std::size_t i = k; i < centrality.size(); i++) {
    const auto node_id = static_cast<std::uint64_t>(static_cast<double>(active_centrality[i].first) - epsilon);
    if (ur.at(node_id) < lr.at(active_centrality[static_cast<std::size_t>(k) - 1].first)) {
      active_centrality.erase(active_centrality.begin() + static_cast<std::ptrdiff_t>(i));
      active_nodes.erase(active_centrality[i].first);
    }
  }

  // If there are more than `k` active nodes, the algorithm hasn’t converged
  if (active_centrality.size() > k) {
    return false;
  }

  // Check if the top of the partial list has converged
  const auto size = std::min(active_centrality.size(), k);
  for (std::size_t i = 1; i < size; i++) {
    const double u = ur.at(active_centrality[i].first);
    const double l = lr.at(active_centrality[i - 1].first);

    if (u - epsilon >= l) {
      return false;
    }
  }
  return true;
}

/// @brief Iteratively tightens the upper and lower bounds of Katz centrality values until convergence. Records updates
/// in the algorithm context.
/// @param active_nodes Active nodes
/// @param graph Current graph
/// @param alpha Attenuation factor
/// @param k Active nodes cutoff
/// @param epsilon Separation tolerance factor
/// @param gamma Convergence factor
/// @return Results
std::vector<std::pair<std::uint64_t, double>> KatzCentralityLoop(std::set<std::uint64_t> &active_nodes,
                                                                 const mg_graph::GraphView<> &graph, const double alpha,
                                                                 const std::uint64_t k, const double epsilon,
                                                                 const double gamma) {
  do {
    katz_alg::context.AddIteration(graph);
    const auto &iteration = katz_alg::context.iteration;

    for (const auto &[_v] : graph.Nodes()) {
      const auto v = graph.GetMemgraphNodeId(_v);  // Convert a node’s inner ID to Memgraph ID

      // Fetch the number of descendants
      for (const auto &[_u, _] : graph.InNeighbours(_v)) {
        const auto u = graph.GetMemgraphNodeId(_u);  // Convert a node’s inner ID to Memgraph ID
        katz_alg::context.omegas[iteration][v] += katz_alg::context.omegas[iteration - 1][u];
      }
      katz_alg::context.centralities[iteration][v] =
          katz_alg::context.centralities[iteration - 1][v] +
          pow(alpha, static_cast<double>(iteration)) * katz_alg::context.omegas[iteration][v];

      // Update the lower and upper bounds
      katz_alg::context.lr[v] = katz_alg::context.centralities[iteration][v];
      katz_alg::context.ur[v] =
          katz_alg::context.centralities[iteration][v] +
          pow(alpha, static_cast<double>(iteration + 1)) * katz_alg::context.omegas[iteration][v] * gamma;
    }
  } while (!Converged(active_nodes, k, epsilon));

  return WrapResults();
}

/// @brief Above function’s C++ API equivalent
std::vector<std::pair<std::uint64_t, double>> KatzCentralityLoop(std::set<std::uint64_t> &active_nodes,
                                                                 const mgp::Graph &graph, const double alpha,
                                                                 const std::uint64_t k, const double epsilon,
                                                                 const double gamma) {
  do {
    katz_alg::context.AddIteration(graph);
    const auto &iteration = katz_alg::context.iteration;

    for (const auto &node : graph.Nodes()) {
      const auto v = node.Id().AsUint();

      // Fetch the number of descendants
      for (const auto &in_relationship : node.InRelationships()) {
        const auto u = in_relationship.From().Id().AsUint();
        katz_alg::context.omegas[iteration][v] += katz_alg::context.omegas[iteration - 1][u];
      }
      katz_alg::context.centralities[iteration][v] =
          katz_alg::context.centralities[iteration - 1][v] +
          pow(alpha, static_cast<double>(iteration)) * katz_alg::context.omegas[iteration][v];

      // Update the lower and upper bounds
      katz_alg::context.lr[v] = katz_alg::context.centralities[iteration][v];
      katz_alg::context.ur[v] =
          katz_alg::context.centralities[iteration][v] +
          pow(alpha, static_cast<double>(iteration + 1)) * katz_alg::context.omegas[iteration][v] * gamma;
    }
  } while (!Converged(active_nodes, k, epsilon));

  return WrapResults();
}

/// @brief Recomputes the first `context.iterations` iterations from the static algorithm and updates the data
/// structures with the newly created and deleted edges.
/// @param context_new Local algorithm context
/// @param affected_nodes Affected nodes (adjacent relationships changed)
/// @param created_relationships Newly-created relationships
/// @param deleted_relationships Newly-deleted relationships
/// @param created_relationship_ids Newly-created relationships’ IDs
/// @param graph Current graph
void UpdateLevel(KatzCentralityData &context_new, std::set<std::uint64_t> &affected_nodes, const mgp::Graph &graph,
                 const std::vector<std::pair<std::uint64_t, uint64_t>> &created_relationships,
                 const std::vector<std::pair<std::uint64_t, uint64_t>> &deleted_relationships,
                 const std::set<std::uint64_t> &created_relationship_ids) {
  const auto i = context_new.iteration;

  // Create an update node queue
  std::queue<std::uint64_t> queue;
  for (const auto v : affected_nodes) {
    queue.push(v);
  }

  // Update new context with old values
  for (const auto [id, value] : katz_alg::context.omegas[i]) {
    context_new.omegas[i][id] = value;
  }

  // Update degrees in paths of length `iteration` until the queue is empty
  while (!queue.empty()) {
    const auto v = queue.front();
    queue.pop();

    if (!graph.ContainsNode(mgp::Id::FromUint(v))) continue;  // Node is deleted in the final iteration

    for (const auto &r : graph.GetNodeById(mgp::Id::FromUint(v)).OutRelationships()) {
      const auto relationship_id = r.Id().AsUint();
      const auto w = r.To().Id().AsUint();

      // If `w` is not updated, add to queue
      if (!affected_nodes.contains(w)) {
        queue.push(w);
      }
      affected_nodes.emplace(w);

      if (created_relationship_ids.contains(relationship_id)) continue;  // Skip new relationships

      context_new.omegas[i][w] += context_new.omegas[i - 1][v] - katz_alg::context.omegas[i - 1][v];
    }
  }

  // Add the new relationships to this iteration’s degrees
  for (const auto [v, w] : created_relationships) {
    context_new.omegas[i][w] += context_new.omegas[i - 1][v];
  }

  // Subtract the deleted relationships from this iteration’s degrees
  for (const auto [v, w] : deleted_relationships) {
    context_new.omegas[i][w] -= katz_alg::context.omegas[i - 1][v];
  }

  // Update centralities
  for (const auto w : affected_nodes) {
    if (i != 1) {
      katz_alg::context.centralities[i][w] = katz_alg::context.centralities[i - 1][w] +
                                             pow(katz_alg::alpha, static_cast<double>(i)) * context_new.omegas[i][w];
    } else {
      katz_alg::context.centralities[i][w] +=
          pow(katz_alg::alpha, static_cast<double>(i)) * (context_new.omegas[i][w] - katz_alg::context.omegas[i][w]);
    }
  }
}

/// @brief Returns whether the Katz centrality results to be retrieved are applicable to the current state of the graph,
/// i.e. whether there have been any modifications.
/// @param graph Current graph
/// @return result inconsistence
bool IsInconsistent(const mg_graph::GraphView<> &graph) {
  for (const auto [node_id] : graph.Nodes()) {
    auto external_id = graph.GetMemgraphNodeId(node_id);
    if (!katz_alg::context.centralities[katz_alg::context.iteration].contains(external_id)) {
      return true;
    }
  }

  return std::ranges::any_of(katz_alg::context.centralities[katz_alg::context.iteration],
                             [&graph](const auto &entry) { return !graph.NodeExists(entry.first); });
}
}  // namespace

bool NoPreviousData() { return !katz_alg::context.IsInitialized(); }

std::vector<std::pair<std::uint64_t, double>> GetKatz(const mg_graph::GraphView<> &graph) {
  // If there’s no context, initialize it and compute centrality with default parameters
  if (!katz_alg::context.IsInitialized()) {
    return SetKatz(graph);
  }

  if (IsInconsistent(graph)) {
    throw std::runtime_error(
        "Graph has been modified and is thus inconsistent with cached Katz centrality scores. To update them, "
        "please call set/reset!");
  }

  return WrapResults();
}

std::vector<std::pair<std::uint64_t, double>> SetKatz(const mg_graph::GraphView<> &graph, const double alpha,
                                                      const double epsilon) {
  katz_alg::alpha = alpha;
  katz_alg::k = k;  // NOLINT(clang-diagnostic-self-assign)
  katz_alg::epsilon = epsilon;
  katz_alg::context.Init(graph);

  if (graph.Edges().empty()) {
    return WrapResults();
  }

  const auto deg_max = MaxDegree(graph);
  const double gamma = static_cast<double>(deg_max) / (1. - (alpha * alpha * static_cast<double>(deg_max)));

  // Initialize the active vector
  std::transform(graph.Nodes().begin(), graph.Nodes().end(),
                 std::inserter(katz_alg::context.active_nodes, katz_alg::context.active_nodes.end()),
                 [&graph](mg_graph::Node<> node) -> std::uint64_t { return graph.GetMemgraphNodeId(node.id); });

  return KatzCentralityLoop(katz_alg::context.active_nodes, graph, katz_alg::alpha, katz_alg::k, katz_alg::epsilon,
                            gamma);
}

std::vector<std::pair<std::uint64_t, double>> UpdateKatz(
    const mgp::Graph &graph, const std::vector<std::uint64_t> &created_nodes,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &created_relationships,
    const std::vector<std::uint64_t> &created_relationship_ids, const std::vector<std::uint64_t> &deleted_nodes,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &deleted_relationships) {
  std::uint64_t n_relationships = 0;
  for (const auto &z : graph.Relationships()) {
    n_relationships++;
  }

  if (n_relationships == 0) {
    katz_alg::context.Init(graph);
    return WrapResults();
  }

  const auto deg_max = MaxDegree(graph);
  const double gamma = static_cast<double>(deg_max) / (1. - (alpha * static_cast<double>(deg_max)));

  // Update context for new nodes for each iteration
  for (const auto v : created_nodes) {
    katz_alg::context.omegas[0][v] = 1;
    for (std::uint64_t i = 0; i <= katz_alg::context.iteration; i++) {
      katz_alg::context.centralities[i][v] = 0;
    }
  }

  // Determine updated nodes
  std::set<std::uint64_t> affected_nodes;
  for (const auto [v, w] : created_relationships) {
    affected_nodes.emplace(v);
    affected_nodes.emplace(w);
  }
  for (const auto [v, w] : deleted_relationships) {
    affected_nodes.emplace(v);
    affected_nodes.emplace(w);
  }

  // Recompute iterations in a local context
  KatzCentralityData context_new;
  context_new.Init(graph);
  const std::set<std::uint64_t> created_relationship_ids_set(created_relationship_ids.begin(),
                                                             created_relationship_ids.end());
  for (std::uint64_t i = 0; i < katz_alg::context.iteration; i++) {
    context_new.AddIteration(graph);
    UpdateLevel(context_new, affected_nodes, graph, created_relationships, deleted_relationships,
                created_relationship_ids_set);
  }

  // Update global context
  for (std::uint64_t i = 1; i < context_new.iteration + 1; i++) {
    for (const auto [id, value] : context_new.omegas[i]) {
      katz_alg::context.omegas[i][id] = value;
    }
  }

  // Update the lower and upper bounds
  for (const auto w : affected_nodes) {
    const auto iteration = katz_alg::context.iteration;

    katz_alg::context.lr[w] = katz_alg::context.centralities[iteration][w];
    katz_alg::context.ur[w] =
        katz_alg::context.centralities[iteration][w] +
        pow(katz_alg::alpha, static_cast<double>(iteration + 1)) * katz_alg::context.omegas[iteration][w];
  }

  // Check whether to keep nodes active
  std::vector<double> min_lr_vector;
  min_lr_vector.reserve(katz_alg::context.active_nodes.size());
  for (const auto active_node : katz_alg::context.active_nodes) {
    min_lr_vector.emplace_back(katz_alg::context.lr[active_node]);
  }

  auto min_lr = *std::ranges::min_element(min_lr_vector);
  for (const auto &node : graph.Nodes()) {
    const auto w = node.Id().AsUint();

    if (katz_alg::context.ur[w] >= (min_lr - katz_alg::epsilon)) {
      katz_alg::context.active_nodes.emplace(w);
    }
  }

  // Erase deleted node values since all relationships are certainly detached
  for (const auto v : deleted_nodes) {
    for (std::uint64_t i = 0; i <= katz_alg::context.iteration; i++) {
      katz_alg::context.omegas[i].erase(katz_alg::context.omegas[i].find(v));
      katz_alg::context.centralities[i].erase(katz_alg::context.centralities[i].find(v));
    }
    katz_alg::context.active_nodes.erase(katz_alg::context.active_nodes.find(v));
  }

  return KatzCentralityLoop(katz_alg::context.active_nodes, graph, katz_alg::alpha, katz_alg::k, katz_alg::epsilon,
                            gamma);
}

void Reset() { katz_alg::context.Clear(); }

}  // namespace katz_alg
