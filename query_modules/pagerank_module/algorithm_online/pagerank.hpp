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

#include "data_structures/graph_view.hpp"

namespace pagerank_online_alg {

///
///@brief Recreates context and calculates Pagerank based on method developed by Bahmani et. al.
///[http://snap.stanford.edu/class/cs224w-readings/bahmani10pagerank.pdf]. It creates R random walks from each node in
/// the graph and calculate pagerank approximation, depending in how many walks does a certain node appears in.
///
///@param graph Graph to calculate pagerank on
///@param R Number of random walks per node
///@param epsilon Stopping epsilong, walks of size (1/epsilon)
///@return std::vector<std::pair<std::uint64_t, double>>
///
std::vector<std::pair<std::uint64_t, double>> SetPagerank(const mg_graph::GraphView<> &graph, std::uint64_t R = 10,
                                                          double epsilon = 0.2);

///
///@brief Method for getting the values from the current Pagerank context. However if context is not set, method throws
/// an exception
///
///@param graph Graph to check consistency for
///@return std::vector<std::pair<std::uint64_t, double>>
///
std::vector<std::pair<std::uint64_t, double>> GetPagerank(const mg_graph::GraphView<> &graph);

///
///@brief Function called on vertex/edge creation or deletion. This method works with already changed graph. It
/// sequentially updates the pagerank by first updating deleted edges, deleted nodes and then adds both nodes and edges
/// among them
///
///@param graph Graph in the (t+1) step
///@param new_vertices Vertices created from the next step
///@param new_edges Edges created from the last step
///@param deleted_vertices Vertices deleted from the last iteration
///@param deleted_edges Edges deleted from the last iteration
///@return std::vector<std::pair<std::uint64_t, double>>
///
std::vector<std::pair<std::uint64_t, double>> UpdatePagerank(
    const mg_graph::GraphView<> &graph, const std::vector<std::uint64_t> &new_vertices,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &new_edges,
    const std::vector<std::uint64_t> &deleted_vertices,
    const std::vector<std::pair<std::uint64_t, uint64_t>> &deleted_edges);

///
///@brief Method for resetting the context and initializing structures
///
///
void Reset();

}  // namespace pagerank_online_alg
