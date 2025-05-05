// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

#include "katz.hpp"
#include <queue>

namespace katz_alg {
namespace {

class KatzCentralityData {
 public:
  ///
  ///@brief Clearing the resulting storages in algorithm context
  ///
  void Clear() {
    centralities.clear();
    omegas.clear();
    active_nodes.clear();
    iteration = 0;
  }

  ///
  ///@brief Initialize resulting structures
  ///
  ///@param graph Graph for calculation
  ///
  void Init(const mg_graph::GraphView<> &graph) {
    Clear();

    // Initialize the centrality vector
    std::unordered_map<std::uint64_t, double> centrality;
    InitVertexMap(centrality, 0.0, graph);
    centralities.emplace_back(std::move(centrality));

    // Initialize the omega
    std::unordered_map<std::uint64_t, double> omega;
    InitVertexMap(omega, 1.0, graph);
    omegas.emplace_back(std::move(omega));

    // Initialize the lr vector
    InitVertexMap(lr, 0.0, graph);
    // Initialize the ur vector
    InitVertexMap(ur, 0.0, graph);
  }

  ///
  ///@brief Adds one iteration and initialize the result storage
  ///
  ///@param graph Graph for calculation
  ///
  void AddIteration(const mg_graph::GraphView<> &graph) {
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

  bool IsInitialized() { return !centralities.empty() && iteration > 0; }

  // Vector for storing centralities for historical iterations
  std::vector<std::unordered_map<std::uint64_t, double>> centralities;
  // Degree of outcomming edges in certain level
  std::vector<std::unordered_map<std::uint64_t, double>> omegas;

  // Vector for storing the lower bound of centrality
  std::unordered_map<std::uint64_t, double> lr;
  // Vector for storing the lower bound of centrality
  std::unordered_map<std::uint64_t, double> ur;

  // Set of nodes used for convergence calculation
  std::set<std::uint64_t> active_nodes;

  // Current algorithm iteration step
  std::uint64_t iteration = 0;

 private:
  ///
  ///@brief Initialize result maps with default values and memgraph IDs
  ///
  ///@param map Map used for storing values
  ///@param default_value  Default value
  ///@param graph Graph used for fetching ID
  ///
  void InitVertexMap(std::unordered_map<std::uint64_t, double> &map, double default_value,
                     const mg_graph::GraphView<> &graph) {
    for (auto &[_v] : graph.Nodes()) {
      // Transform inner ID to Memgraph
      auto v = graph.GetMemgraphNodeId(_v);
      map[v] = default_value;
    }
  }
};

/// Storage for dynamic algorithm intermediate results
KatzCentralityData context;
/// Attenuation factor
double alpha{};
/// Top-k for convergence calculation
std::uint64_t k{};
/// Tolerance factor for separation calculation
double epsilon{};

///
///@brief Calculates maximum degree in graph. Degree is denoted by number of edges going from node.
///
///@param graph Graph for calculation
///@return std::uint64_t
///
std::uint64_t MaxDegree(const mg_graph::GraphView<> &graph) {
  std::vector<std::size_t> deg_vector;
  // Calculate the vector of graph degrees
  std::transform(graph.Nodes().begin(), graph.Nodes().end(), std::back_inserter(deg_vector),
                 [&graph](mg_graph::Node<> vertex) -> std::size_t { return graph.Neighbours(vertex.id).size(); });
  auto deg_max = *std::max_element(deg_vector.begin(), deg_vector.end());
  return deg_max;
}

///
///@brief Calculates whether the upper and lower boundaries within the list of active nodes are epsilon-separated.
///
///@param active_nodes Active nodes for convergence calculations
///@param k Factor for sorting only first k
///@param epsilon Factor of separation/tolerance
///@return true
///@return false
///
bool Converged(std::set<std::uint64_t> &active_nodes, std::uint64_t k, double epsilon) {
  // Fetch the information about current iteration
  auto centrality = katz_alg::context.centralities[context.iteration];
  auto lr = katz_alg::context.lr;
  auto ur = katz_alg::context.ur;

  // TODO: this is controversial decision
  k = centrality.size();

  // Create a vector of active centralities and their values
  std::vector<std::pair<std::uint64_t, double>> active_centrality;
  for (auto m : active_nodes) {
    active_centrality.emplace_back(m, centrality.at(m));
  }

  // Partially sort the centralities. Keep only the first `k` sorted
  std::partial_sort(active_centrality.begin(), active_centrality.begin() + std::min(k, active_centrality.size()),
                    active_centrality.end(),
                    [](std::pair<std::uint64_t, double> a, std::pair<std::uint64_t, double> b) -> bool {
                      return a.second > b.second;
                    });

  // Erase from set of active nodes if centralities are separated
  for (std::size_t i = k; i < centrality.size(); i++) {
    if (ur.at(active_centrality[i].first - epsilon) < lr.at(active_centrality[k - 1].first)) {
      active_centrality.erase(active_centrality.begin() + i);
      active_nodes.erase(active_centrality[i].first);
    }
  }

  // If the size of active nodes is higher than `k`, algorithm hasn't converged
  if (active_centrality.size() > k) {
    return false;
  }

  // Next step is checking whether the top of the partial list has converged too
  auto size = std::min(active_centrality.size(), k);
  for (std::size_t i = 1; i < size; i++) {
    double u = ur.at(active_centrality[i].first);
    double l = lr.at(active_centrality[i - 1].first);

    if (u - epsilon >= l) {
      return false;
    }
  }
  return true;
}

///
///@brief Wrap the current results and return the last iteration ranking
///
///@return std::vector<std::pair<std::uint64_t, double>> Results in format [node, ranking   ]
///
std::vector<std::pair<std::uint64_t, double>> WrapResults() {
  return std::vector<std::pair<std::uint64_t, double>>(
      katz_alg::context.centralities[katz_alg::context.iteration].begin(),
      katz_alg::context.centralities[katz_alg::context.iteration].end());
}

///
///@brief Running the convergence process of shrinking the upper and lower bound for katz centrality. Updates are stored
/// in context data structure.
///
///@param active_nodes Active nodes - list of nodes in convergence process
///@param graph Graph for convergence calculation
///@param alpha Attenuation factor
///@param k Parameter for keeping the first k node centralities sorted
///@param epsilon Tolerance factor
///@param gamma Convergence factor
///@return std::vector<std::pair<std::uint64_t, double>>
///
std::vector<std::pair<std::uint64_t, double>> KatzCentralityLoop(std::set<std::uint64_t> &active_nodes,
                                                                 const mg_graph::GraphView<> &graph, double alpha,
                                                                 std::uint64_t k, double epsilon, double gamma) {
  do {
    katz_alg::context.AddIteration(graph);
    auto &iteration = katz_alg::context.iteration;

    for (auto &[_v] : graph.Nodes()) {
      // Transform inner ID to Memgraph
      auto v = graph.GetMemgraphNodeId(_v);

      // Fetch the number of descendants
      for (auto &[_u, _] : graph.InNeighbours(_v)) {
        // Transform inner ID to Memgraph
        auto u = graph.GetMemgraphNodeId(_u);
        katz_alg::context.omegas[iteration][v] += katz_alg::context.omegas[iteration - 1][u];
      }
      katz_alg::context.centralities[iteration][v] = katz_alg::context.centralities[iteration - 1][v] +
                                                     pow(alpha, iteration) * katz_alg::context.omegas[iteration][v];

      // Update the lower and upper bound
      katz_alg::context.lr[v] = katz_alg::context.centralities[iteration][v];
      katz_alg::context.ur[v] = katz_alg::context.centralities[iteration][v] +
                                pow(alpha, iteration + 1) * katz_alg::context.omegas[iteration][v] * gamma;
    }
  } while (!Converged(active_nodes, k, epsilon));

  // Transform the resulting global values
  return WrapResults();
}

///
///@brief Update the first {0... context.iterations} iterations from static algorithm. This updates data structures
/// based on the newly created and deleted edges.
///
///@param context_new New graph context storing the data for updated graph
///@param from_nodes Set of updated node centralities
///@param new_edges List of newly created edges
///@param deleted_edges List of deleted edges
///@param new_edge_ids List of ids of created edges
///@param graph Updated graph for calculation
///
void UpdateLevel(KatzCentralityData &context_new, std::set<std::uint64_t> &updated_nodes,
                 const std::vector<std::pair<std::uint64_t, uint64_t>> &new_edges,
                 const std::vector<std::pair<std::uint64_t, uint64_t>> &deleted_edges,
                 const std::set<std::uint64_t> &new_edge_ids, const mg_graph::GraphView<> &graph) {
  auto i = context_new.iteration;

  // Create a queue of nodes for update
  std::queue<std::uint64_t> queue;
  for (auto v : updated_nodes) {
    queue.push(v);
  }

  // Update new context with old values
  for (auto [id, value] : katz_alg::context.omegas[i]) {
    context_new.omegas[i][id] = value;
  }

  // Till queue is empty, update degrees in paths of length `iteration`
  while (!queue.empty()) {
    auto v = queue.front();
    queue.pop();

    // Node is deleted in last iteration
    if (!graph.NodeExists(v)) continue;
    for (auto [w_, edge_id] : graph.OutNeighbours(graph.GetInnerNodeId(v))) {
      auto w = graph.GetMemgraphNodeId(w_);

      // If `w` is not updated, add to queue
      if (updated_nodes.find(w) == updated_nodes.end()) {
        queue.push(w);
      }
      updated_nodes.emplace(w);

      // If edge is recently created, skip it
      if (new_edge_ids.find(edge_id) != new_edge_ids.end()) continue;

      context_new.omegas[i][w] += context_new.omegas[i - 1][v] - katz_alg::context.omegas[i - 1][v];
    }
  }

  // Update new degrees with new edges
  for (auto [w, v] : new_edges) {
    context_new.omegas[i][v] += context_new.omegas[i - 1][w];
  }

  // Update new degrees by deling values from deleted edges
  for (auto [w, v] : deleted_edges) {
    context_new.omegas[i][v] -= katz_alg::context.omegas[i - 1][w];
  }

  // Update centralities as last step
  for (auto w : updated_nodes) {
    if (i != 1) {
      katz_alg::context.centralities[i][w] =
          katz_alg::context.centralities[i - 1][w] + pow(katz_alg::alpha, i) * context_new.omegas[i][w];
    } else {
      katz_alg::context.centralities[i][w] +=
          pow(katz_alg::alpha, i) * (context_new.omegas[i][w] - katz_alg::context.omegas[i][w]);
    }
  }
}

///
///@brief Graph is inconsistent if the results are not matching with the current graph state
///
///@param graph Graph for consistency check
///@return true Results are matching
///@return false There are missing nodes.
///
bool IsInconsistent(const mg_graph::GraphView<> &graph) {
  for (auto const [node_id] : graph.Nodes()) {
    auto external_id = graph.GetMemgraphNodeId(node_id);
    if (katz_alg::context.centralities[katz_alg::context.iteration].find(external_id) ==
        katz_alg::context.centralities[katz_alg::context.iteration].end()) {
      return true;
    }
  }

  for (auto [node_id, _] : katz_alg::context.centralities[katz_alg::context.iteration]) {
    if (!graph.NodeExists(node_id)) {
      return true;
    }
  }

  return false;
}
}  // namespace

std::vector<std::pair<std::uint64_t, double>> GetKatz(const mg_graph::GraphView<> &graph) {
  // Create context and calculate values if not initialized
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

std::vector<std::pair<std::uint64_t, double>> SetKatz(const mg_graph::GraphView<> &graph, double alpha,
                                                      double epsilon) {
  katz_alg::alpha = alpha;
  // katz_alg::k = k;
  katz_alg::epsilon = epsilon;
  katz_alg::context.Init(graph);

  if (graph.Edges().empty()) {
    return WrapResults();
  }

  auto deg_max = MaxDegree(graph);
  double gamma = deg_max / (1. - (alpha * alpha * deg_max));

  // Initialize the active vector
  std::transform(graph.Nodes().begin(), graph.Nodes().end(),
                 std::inserter(katz_alg::context.active_nodes, katz_alg::context.active_nodes.end()),
                 [&graph](mg_graph::Node<> vertex) -> std::uint64_t { return graph.GetMemgraphNodeId(vertex.id); });

  return KatzCentralityLoop(katz_alg::context.active_nodes, graph, katz_alg::alpha, katz_alg::k, katz_alg::epsilon,
                            gamma);
}

std::vector<std::pair<std::uint64_t, double>> UpdateKatz(
    const mg_graph::GraphView<> &graph, const std::vector<std::uint64_t> &new_vertices,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &new_edges, const std::vector<std::uint64_t> &new_edge_ids,
    const std::vector<std::uint64_t> &deleted_vertices,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &deleted_edges) {
  // Create context and calculate values if not initialized
  if (!katz_alg::context.IsInitialized()) {
    return SetKatz(graph);
  }

  if (graph.Edges().empty()) {
    katz_alg::context.Init(graph);
    return WrapResults();
  }

  auto deg_max = MaxDegree(graph);
  double gamma = deg_max / (1. - (alpha * deg_max));

  // Update context for new vertices for each iteration
  for (auto v : new_vertices) {
    katz_alg::context.omegas[0][v] = 1;
    for (std::uint64_t i = 0; i <= katz_alg::context.iteration; i++) {
      katz_alg::context.centralities[i][v] = 0;
    }
  }

  // Updated nodes
  std::set<std::uint64_t> updated_nodes;
  for (auto [w, v] : new_edges) {
    updated_nodes.emplace(w);
    updated_nodes.emplace(v);
  }
  for (auto [w, v] : deleted_edges) {
    updated_nodes.emplace(w);
    updated_nodes.emplace(v);
  }

  // Create new context for updating the dynamic graph
  KatzCentralityData context_new;
  context_new.Init(graph);
  std::set<std::uint64_t> new_edge_ids_set(new_edge_ids.begin(), new_edge_ids.end());
  for (std::uint64_t i = 0; i < katz_alg::context.iteration; i++) {
    context_new.AddIteration(graph);
    UpdateLevel(context_new, updated_nodes, new_edges, deleted_edges, new_edge_ids_set, graph);
  }

  // After updating iteration results, store them in global value
  for (std::uint64_t i = 1; i < context_new.iteration + 1; i++) {
    for (auto [id, value] : context_new.omegas[i]) {
      katz_alg::context.omegas[i][id] = value;
    }
  }

  // Update the lower and upper bound
  for (auto w : updated_nodes) {
    auto iteration = katz_alg::context.iteration;

    katz_alg::context.lr[w] = katz_alg::context.centralities[iteration][w];
    katz_alg::context.ur[w] = katz_alg::context.centralities[iteration][w] +
                              pow(katz_alg::alpha, iteration + 1) * katz_alg::context.omegas[iteration][w];
  }

  // Check whether node should be active again
  std::vector<double> min_lr_vector;
  for (auto active_node : katz_alg::context.active_nodes) {
    min_lr_vector.emplace_back(katz_alg::context.lr[active_node]);
  }

  auto min_lr = *std::min_element(min_lr_vector.begin(), min_lr_vector.end());
  for (auto [w_] : graph.Nodes()) {
    auto w = graph.GetMemgraphNodeId(w_);

    if (katz_alg::context.ur[w] >= (min_lr - katz_alg::epsilon)) {
      katz_alg::context.active_nodes.emplace(w);
    }
  }

  // Erase values for deleted vertices, since all edges are certainly detached
  for (auto v : deleted_vertices) {
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
