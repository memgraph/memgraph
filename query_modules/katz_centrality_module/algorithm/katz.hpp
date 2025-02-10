// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

#include <algorithm>
#include <cmath>
#include <map>
#include <set>

#include <mg_exceptions.hpp>
#include <mg_graph.hpp>

namespace katz_alg {
namespace {

class KatzCentralityData;

///
///@brief Calculates maximum degree in graph. Degree is denoted by number of edges going from node.
///
///@param graph Graph for calculation
///@return std::uint64_t
///
std::uint64_t MaxDegree(const mg_graph::GraphView<> &graph);

///
///@brief Calculates whether the upper and lower boundaries within the list of active nodes are epsilon-separated.
///
///@param active_nodes Active nodes for convergence calculations
///@param k Factor for sorting only first k
///@param epsilon Factor of separation/tolerance
///@return true
///@return false
///
bool Converged(std::set<std::uint64_t> &active_nodes, std::uint64_t k, double epsilon);

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
                                                                 std::uint64_t k, double epsilon, double gamma);

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
void UpdateLevel(KatzCentralityData &context_new, std::set<std::uint64_t> &from_nodes,
                 const std::vector<std::pair<std::uint64_t, std::uint64_t>> &new_edges,
                 const std::vector<std::pair<std::uint64_t, std::uint64_t>> &deleted_edges,
                 const std::set<std::uint64_t> &new_edge_ids, const mg_graph::GraphView<> &graph);

///
///@brief Wrap the current results and return the last iteration ranking
///
///@return std::vector<std::pair<std::uint64_t, double>> Results in format [node, ranking   ]
///
std::vector<std::pair<std::uint64_t, double>> WrapResults();

///
///@brief Graph is inconsistent if the results are not matching with the current graph state
///
///@param graph Graph for consistency check
///@return true Results are matching
///@return false There are missing nodes.
///
bool IsInconsistent(const mg_graph::GraphView<> &graph);
}  // namespace

///
///@brief Start the iterations of static katz centrality of Katz centrality approximations. The algorithm is based on
/// shrinking the upper and lower boundaries till convergence. Iteration results are stored in context and reused in
/// dynamic algorithm. In each step, centrality score for convergence calculation, its lower and upper bounds are
/// calculated. this is approximative algorithm that has the guarantee about preserving the centrality rank, however, it
/// does not guarantee the correct result.
///
///@param graph Graph for calculation
///@param alpha Attenuation factor
///@param epsilon Tolerance for convergence
///@return std::vector<std::pair<std::uint64_t, double>>
///
std::vector<std::pair<std::uint64_t, double>> SetKatz(const mg_graph::GraphView<> &graph, double alpha = 0.2,
                                                      double epsilon = 1e-2);

///
///@brief Fetch already computed results of Katz Centrality. If results have not been created, create them by calling
/// set. If graph is not in the consistent state (changes were made before the last update) an error is thrown.
///
///@param graph Graph for calculation
///@return std::vector<std::pair<std::uint64_t, double>>
///
std::vector<std::pair<std::uint64_t, double>> GetKatz(const mg_graph::GraphView<> &graph);

///
///@brief Update the Katz centrality based on the new and deleted entities in the graph. All previous iteration steps
/// should be updated for both deleted edges and newly created ones. Once they are updated, if the boundaries hasn't
/// already converged, few iterations of static algorithm is re-run. The update is calculated regarding previous context
/// state, however if ran without previously calling GetKatz, new context is created and algorithm is run staticly.
///
///@param graph Input graph
///@param new_vertices New vertices in graph
///@param new_edges New edges in graph
///@param new_edge_ids ID of new edges to keep track of newly generated
///@param deleted_vertices Deleted vertices in graph
///@param deleted_edges Deleted edges in graph
///@return std::vector<std::pair<std::uint64_t, double>>
///
std::vector<std::pair<std::uint64_t, double>> UpdateKatz(
    const mg_graph::GraphView<> &graph, const std::vector<std::uint64_t> &new_vertices,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &new_edges, const std::vector<std::uint64_t> &new_edge_ids,
    const std::vector<std::uint64_t> &deleted_vertices,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &deleted_edges);

///
///@brief Reset the context once this method is triggered. Context and iterations start with blank data structures.
///
///
void Reset();
}  // namespace katz_alg
