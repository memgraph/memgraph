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

#include <cstdint>
#include <vector>

#include <mg_procedure.h>
#include <mg_exceptions.hpp>
#include <mg_graph.hpp>

namespace leiden_alg {
///
/// @brief Performs the Leiden community detection algorithm on the given graph.
///
/// @param graph The graph on which to perform community detection.
/// @param gamma Parameter that controls the resolution of the algorithm.
/// @param theta Parameter that adjusts merging of communities based on modularity.
/// @param resolution_parameter Controls the granularity of the detected communities.
/// @param max_iterations The maximum number of iterations the algorithm will run.
/// @return A vector of vectors where each vector represents a community hierarchy for a node.
///
std::vector<std::vector<std::uint64_t>> GetCommunities(const mg_graph::GraphView<> &graph, double gamma, double theta,
                                                       double resolution_parameter, std::uint64_t max_iterations);

}  // namespace leiden_alg
