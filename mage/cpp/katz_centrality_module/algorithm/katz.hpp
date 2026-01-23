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
#include <map>
#include <set>

#include <mg_exceptions.hpp>
#include <mg_graph.hpp>
#include <mgp.hpp>

namespace katz_alg {

/// @brief Returns whether the algorithm has been run before calling `katz_centrality_online.update()`
/// @return whether the algorithm has been run before current update
bool NoPreviousData();

/// @brief Statically computes Katz centrality. This is an approximative algorithm that guarantees preserving the
/// Katz centrality ranking, but not the exact scores.
/// @param graph Current graph
/// @param alpha Attenuation factor
/// @param epsilon Separation tolerance factor
/// @return (node_id, Katz centrality) pairs
std::vector<std::pair<std::uint64_t, double>> SetKatz(const mg_graph::GraphView<> &graph, const double alpha = 0.2,
                                                      const double epsilon = 1e-2);

/// @brief Retrieves already-computed Katz centrality scores. If there aren’t any, calls SetKatz() with default
/// parameters. If the graph has been modified since the previous computation, throws an error.
/// @param graph Current graph
/// @return (node_id, Katz centrality) pairs
std::vector<std::pair<std::uint64_t, double>> GetKatz(const mg_graph::GraphView<> &graph);

/// @brief Updates the Katz centrality scores after the graph is modified.
/// @param graph Current graph
/// @param created_nodes Newly-created nodes
/// @param created_relationships Newly-created relationships
/// @param created_relationship_ids Newly-created relationships’ IDs
/// @param deleted_nodes Newly-deleted nodes
/// @param deleted_relationships Newly-deleted relationships
/// @return (node_id, Katz centrality) pairs
std::vector<std::pair<std::uint64_t, double>> UpdateKatz(
    const mgp::Graph &graph, const std::vector<std::uint64_t> &created_nodes,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &created_relationships,
    const std::vector<std::uint64_t> &created_relationship_ids, const std::vector<std::uint64_t> &deleted_nodes,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &deleted_relationships);

///@brief Resets the algorithm context by clearing its data structures.
void Reset();
}  // namespace katz_alg
