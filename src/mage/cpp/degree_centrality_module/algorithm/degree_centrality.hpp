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

#include <mg_graph.hpp>

namespace degree_centrality_alg {

enum class AlgorithmType { kUndirected = 0, kOut = 1, kIn = 2 };

std::vector<double> GetDegreeCentrality(const mg_graph::GraphView<> &graph, const AlgorithmType algorithm_type);

}  // namespace degree_centrality_alg
