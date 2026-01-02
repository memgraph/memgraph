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
#include <mg_procedure.h>
#include <unordered_map>

// This import should go before below imports since it is a dependency for the subsequent ones
#include "defs.h"
///////////////////////
#include "basic_comm.h"
#include "basic_util.h"
#include "color_comm.h"

namespace louvain_alg {

// Used for removing the ambiguity of existing graph instances
using GrappoloGraph = graph;

struct LouvainGraph {
  std::vector<std::tuple<uint64_t, uint64_t, double>> edges;
  std::unordered_map<uint64_t, uint64_t> memgraph_id_to_id;
};

/**
 * Grappolo community detection algorithm. Implementation by: https://github.com/Exa-Graph/grappolo
 * This function takes graph and calls a parallel clustering using the Louvain method as the serial template.
 * Depending on the <coloring> variable, instance of a graph algorithm with graph coloring is called. Next three
 * parameters are used for stopping the algorithm. If less than <minGraphSize> nodes are left in the graph, algorithm
 * stops. The algorithm will stop the iterations in the current phase when the gain in modularity is less than
 * <threshold> or <coloringThreshold>, depending on the algorithm type.
 *
 * @param grappoloGraph Grappolo graph instance
 * @param coloring If true, graph coloring is applied
 * @param min_graph_size Determines when multi-phase operations should stop. Execution stops when the coarsened graph
 * has collapsed the current graph to a fewer than `minGraphSize` nodes.
 * @param threshold The algorithm will stop the iterations in the current phase when the gain in modularity is less than
 * `threshold`
 * @param coloringThreshold The algorithm will stop the iterations in the current phase of coloring algorithm when the
 * gain in modularity is less than `coloringThreshold`
 * @return Vector of community indices
 */
std::vector<int64_t> GrappoloCommunityDetection(GrappoloGraph &grappolo_graph, mgp_graph *graph, bool coloring,
                                                uint64_t min_graph_size, double threshold, double coloring_threshold,
                                                int num_threads);

LouvainGraph GetLouvainGraph(mgp_graph *memgraph_graph, mgp_memory *memory, const char *weight_property,
                             double default_weight = 1.0);
LouvainGraph GetLouvainSubgraph(mgp_memory *memory, mgp_graph *memgraph_graph, mgp_list *subgraph_nodes,
                                mgp_list *subgraph_edges, const char *weight_property, double default_weight = 1.0);
void GetGrappoloSuitableGraph(GrappoloGraph &grappolo_graph, int num_threads, const LouvainGraph &edges);
}  // namespace louvain_alg
