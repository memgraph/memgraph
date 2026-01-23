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

#include "louvain.hpp"
#include <omp.h>
#include "mg_utils.hpp"
#include "mgp.hpp"

namespace louvain_alg {

constexpr int kReplaceMap = 0;
constexpr int kThreadsOpt = 1;
constexpr int kNumColors = 16;

// NOLINTBEGIN(google-runtime-int)
std::vector<int64_t> GrappoloCommunityDetection(GrappoloGraph &grappolo_graph, mgp_graph *graph, bool coloring,
                                                uint64_t min_graph_size, double threshold, double coloring_threshold,
                                                int num_threads) {
  auto number_of_vertices = grappolo_graph.numVertices;

  auto *cluster_array = static_cast<long *>(malloc(number_of_vertices * sizeof(long)));
#pragma omp parallel for default(none) shared(number_of_vertices, cluster_array)
  for (long i = 0; i < number_of_vertices; i++) {
    cluster_array[i] = -1;
  }

  // Dynamically set currently.
  if (coloring) {
    runMultiPhaseColoring(&grappolo_graph, graph, cluster_array, coloring, kNumColors, kReplaceMap,
                          static_cast<long>(min_graph_size), threshold, coloring_threshold, num_threads, kThreadsOpt);
  } else {
    runMultiPhaseBasic(&grappolo_graph, graph, cluster_array, kReplaceMap, static_cast<long>(min_graph_size), threshold,
                       coloring_threshold, num_threads, kThreadsOpt);
  }

  // Store clustering information in vector
  std::vector<long> result;
  result.reserve(number_of_vertices);
  for (long i = 0; i < number_of_vertices; ++i) {
    result.emplace_back(cluster_array[i]);
  }
  // Detach memory, no need to free graph instance, it will be removed inside algorithm
  free(cluster_array);

  // Return empty vector if algorithm has failed
  return result;
}

LouvainGraph GetLouvainGraph(mgp_graph *memgraph_graph, mgp_memory *memory, const char *weight_property,
                             double default_weight) {
  LouvainGraph louvain_graph;
  ;
  louvain_graph.edges.reserve(mgp::graph_approximate_edge_count(memgraph_graph));
  louvain_graph.memgraph_id_to_id.reserve(mgp::graph_approximate_vertex_count(memgraph_graph));

  auto *vertices_it = mgp::graph_iter_vertices(memgraph_graph, memory);  // Safe vertex iterator creation
  const mg_utility::OnScopeExit delete_vertices_it([&vertices_it] { mgp::vertices_iterator_destroy(vertices_it); });
  for (auto *source = mgp::vertices_iterator_get(vertices_it); source;
       source = mgp::vertices_iterator_next(vertices_it)) {
    if (mgp::must_abort(memgraph_graph)) [[unlikely]] {
      throw mgp::TerminatedMustAbortException();
    }
    uint64_t source_id = 0;
    auto *edges_it = mgp::vertex_iter_out_edges(source, memory);  // Safe edge iterator creation
    const mg_utility::OnScopeExit delete_edges_it([&edges_it] { mgp::edges_iterator_destroy(edges_it); });
    auto memgraph_source_id = mgp::vertex_get_id(source).as_int;
    auto memgraph_source_id_it = louvain_graph.memgraph_id_to_id.find(memgraph_source_id);
    if (memgraph_source_id_it == louvain_graph.memgraph_id_to_id.end()) {
      source_id = static_cast<uint64_t>(louvain_graph.memgraph_id_to_id.size());
      louvain_graph.memgraph_id_to_id[memgraph_source_id] = source_id;
    } else {
      source_id = memgraph_source_id_it->second;
    }
    for (auto *out_edge = mgp::edges_iterator_get(edges_it); out_edge; out_edge = mgp::edges_iterator_next(edges_it)) {
      if (mgp::must_abort(memgraph_graph)) [[unlikely]] {
        throw mgp::TerminatedMustAbortException();
      }
      uint64_t destination_id = 0;
      auto *destination = mgp::edge_get_to(out_edge);
      const double weight = mg_utility::GetNumericProperty(out_edge, weight_property, memory, default_weight);
      auto memgraph_destination_id = mgp::vertex_get_id(destination).as_int;
      auto memgraph_destination_id_it = louvain_graph.memgraph_id_to_id.find(memgraph_destination_id);
      if (memgraph_destination_id_it == louvain_graph.memgraph_id_to_id.end()) {
        destination_id = static_cast<uint64_t>(louvain_graph.memgraph_id_to_id.size());
        louvain_graph.memgraph_id_to_id[memgraph_destination_id] = destination_id;
      } else {
        destination_id = memgraph_destination_id_it->second;
      }
      louvain_graph.edges.emplace_back(source_id, destination_id, weight);
    }
  }
  return louvain_graph;
}

LouvainGraph GetLouvainSubgraph(mgp_memory *memory, mgp_graph *memgraph_graph, mgp_list *subgraph_nodes,
                                mgp_list *subgraph_edges, const char *weight_property, double default_weight) {
  LouvainGraph louvain_graph;
  louvain_graph.edges.reserve(mgp::list_size(subgraph_edges));
  louvain_graph.memgraph_id_to_id.reserve(mgp::list_size(subgraph_nodes));
  auto number_of_vertices = 0;

  for (std::size_t i = 0; i < mgp::list_size(subgraph_nodes); i++) {
    if (mgp::must_abort(memgraph_graph)) [[unlikely]] {
      throw mgp::TerminatedMustAbortException();
    }
    auto *vertex = mgp::value_get_vertex(mgp::list_at(subgraph_nodes, i));
    auto vertex_id = mgp::vertex_get_id(vertex).as_int;
    auto vertex_id_it = louvain_graph.memgraph_id_to_id.find(vertex_id);
    if (vertex_id_it == louvain_graph.memgraph_id_to_id.end()) {
      louvain_graph.memgraph_id_to_id[vertex_id] = number_of_vertices++;
    }
  }

  for (std::size_t i = 0; i < mgp::list_size(subgraph_edges); i++) {
    if (mgp::must_abort(memgraph_graph)) [[unlikely]] {
      throw mgp::TerminatedMustAbortException();
    }
    auto *edge = mgp::value_get_edge(mgp::list_at(subgraph_edges, i));
    auto *source = mgp::edge_get_from(edge);
    auto *destination = mgp::edge_get_to(edge);
    auto source_id = mgp::vertex_get_id(source).as_int;
    auto destination_id = mgp::vertex_get_id(destination).as_int;
    if (louvain_graph.memgraph_id_to_id.contains(source_id) &&
        louvain_graph.memgraph_id_to_id.contains(destination_id)) {
      const double weight = mg_utility::GetNumericProperty(edge, weight_property, memory, default_weight);
      louvain_graph.edges.emplace_back(louvain_graph.memgraph_id_to_id[source_id],
                                       louvain_graph.memgraph_id_to_id[destination_id], weight);
    }
  }
  return louvain_graph;
}

void GetGrappoloSuitableGraph(GrappoloGraph &grappolo_graph, int num_threads, const LouvainGraph &louvain_graph) {
  const auto number_of_edges = louvain_graph.edges.size();

  std::vector<edge> tmp_edge_list(number_of_edges);  // Every edge stored ONCE
  auto edge_index = 0;
  for (const auto &[source, destination, weight] : louvain_graph.edges) {
    tmp_edge_list[edge_index].head = static_cast<long>(source);       // The H index: Zero-based indexing
    tmp_edge_list[edge_index].tail = static_cast<long>(destination);  // The T index: Zero-based indexing
    tmp_edge_list[edge_index].weight = weight;                        // The weight
    edge_index++;
  }
  const auto number_of_vertices = louvain_graph.memgraph_id_to_id.size();

  omp_set_num_threads(num_threads);
  auto *edge_list_ptrs = static_cast<int64_t *>(malloc((number_of_vertices + 1) * sizeof(int64_t)));
  if (edge_list_ptrs == nullptr) {
    throw mg_exception::NotEnoughMemoryException();
  }

  auto *edge_list = static_cast<edge *>(malloc(number_of_edges * 2 * sizeof(edge)));  // Every edge stored twice
  if (edge_list == nullptr) {
    throw mg_exception::NotEnoughMemoryException();
  }

#pragma omp parallel for default(none) shared(number_of_vertices, edge_list_ptrs)
  for (std::size_t i = 0; i <= number_of_vertices; i++) edge_list_ptrs[i] = 0;  // For first touch purposes

    // Build the EdgeListPtr Array: Cumulative addition
#pragma omp parallel for default(none) shared(number_of_edges, tmp_edge_list, edge_list_ptrs)
  for (std::size_t i = 0; i < number_of_edges; i++) {
    __sync_fetch_and_add(&edge_list_ptrs[tmp_edge_list[i].head + 1], 1);  // Leave 0th position intact
    __sync_fetch_and_add(&edge_list_ptrs[tmp_edge_list[i].tail + 1], 1);  // Leave 0th position intact
  }
  for (std::size_t i = 0; i < number_of_vertices; i++) {
    edge_list_ptrs[i + 1] += edge_list_ptrs[i];  // Prefix Sum
  }
  // The last element of Cumulative will hold the total number of characters
  if (2 * number_of_edges != edge_list_ptrs[number_of_vertices]) {
    throw std::runtime_error("Community detection error: Error while graph fetching in the edge prefix sum creation");
  }

  // Keep track of how many edges have been added for a vertex:
  std::vector<long> added;
  try {
    added.resize(number_of_vertices, 0);
  } catch (const std::bad_alloc &) {
    throw mg_exception::NotEnoughMemoryException();
  }
#pragma omp parallel for default(none) shared(number_of_vertices, added)
  for (std::size_t i = 0; i < number_of_vertices; i++) added[i] = 0;

    // Build the edgeList from edgeListTmp:

#pragma omp parallel for default(none) shared(number_of_edges, tmp_edge_list, edge_list_ptrs, added, edge_list)
  for (std::size_t i = 0; i < number_of_edges; i++) {
    auto head = tmp_edge_list[i].head;
    auto tail = tmp_edge_list[i].tail;
    auto weight = tmp_edge_list[i].weight;

    auto index = edge_list_ptrs[head] + __sync_fetch_and_add(&added[head], 1);
    edge_list[index].head = head;
    edge_list[index].tail = tail;
    edge_list[index].weight = weight;
    // Add the other way:
    index = edge_list_ptrs[tail] + __sync_fetch_and_add(&added[tail], 1);
    edge_list[index].head = tail;
    edge_list[index].tail = head;
    edge_list[index].weight = weight;
  }
  grappolo_graph.numVertices = static_cast<long>(number_of_vertices);
  grappolo_graph.numEdges = static_cast<long>(number_of_edges);
  grappolo_graph.edgeListPtrs = edge_list_ptrs;
  grappolo_graph.edgeList = edge_list;
  grappolo_graph.sVertices = static_cast<long>(number_of_vertices);
}
}  // namespace louvain_alg
// NOLINTEND(google-runtime-int)
