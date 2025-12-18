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

#include "_mgp.hpp"
#include "algorithm/pagerank.hpp"

#include <mg_utils.hpp>

namespace pagerank_alg {
PageRankGraph CreatePageRankGraph(mgp_graph *memgraph_graph, mgp_memory *memory) {
  PageRankGraph graph{};

  auto approx_vertex_count = mgp::graph_approximate_vertex_count(memgraph_graph);
  auto approx_edge_count = mgp::graph_approximate_edge_count(memgraph_graph);

  graph.id_to_memgraph.reserve(approx_vertex_count);
  graph.memgraph_to_id.reserve(approx_vertex_count);
  graph.ordered_edges_.reserve(approx_edge_count);

  auto *vertices_it = mgp::graph_iter_vertices(memgraph_graph, memory);  // Safe vertex iterator creation
  mg_utility::OnScopeExit delete_vertices_it([&vertices_it] { mgp::vertices_iterator_destroy(vertices_it); });
  for (auto *source = mgp::vertices_iterator_get(vertices_it); source;
       source = mgp::vertices_iterator_next(vertices_it)) {
    auto *edges_it = mgp::vertex_iter_out_edges(source, memory);  // Safe edge iterator creation
    mg_utility::OnScopeExit delete_edges_it([&edges_it] { mgp::edges_iterator_destroy(edges_it); });

    auto source_id = mgp::vertex_get_id(source).as_int;
    for (auto *out_edge = mgp::edges_iterator_get(edges_it); out_edge; out_edge = mgp::edges_iterator_next(edges_it)) {
      auto *destination = mgp::edge_get_to(out_edge);
      auto destination_id = mgp::vertex_get_id(destination).as_int;
      graph.ordered_edges_.emplace_back(destination_id, source_id);
    }
    graph.memgraph_to_id[source_id] = graph.id_to_memgraph.size();
    graph.id_to_memgraph.emplace_back(source_id);
  }

  graph.node_count_ = graph.id_to_memgraph.size();
  graph.edge_count_ = graph.ordered_edges_.size();

  graph.out_degree_.resize(graph.node_count_, 0);
  for (auto &edge : graph.ordered_edges_) {
    edge = {graph.memgraph_to_id[edge.first], graph.memgraph_to_id[edge.second]};
    graph.out_degree_[edge.second] += 1;
  }
  return graph;
}

}  // namespace pagerank_alg

namespace {
constexpr char const *kProcedureGet = "get";

constexpr char const *kFieldNode = "node";
constexpr char const *kFieldRank = "rank";

constexpr char const *kArgumentMaxIterations = "max_iterations";
constexpr char const *kArgumentDampingFactor = "damping_factor";
constexpr char const *kArgumentStopEpsilon = "stop_epsilon";
constexpr char const *kArgumentNumThreads = "num_of_threads";

void InsertPagerankRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const std::uint64_t node_id,
                          double rank) {
  auto *vertex = mgp::graph_get_vertex_by_id(graph, mgp_vertex_id{.as_int = static_cast<int64_t>(node_id)}, memory);
  if (!vertex) {
    if (mgp::graph_is_transactional(graph)) {
      throw mg_exception::InvalidIDException();
    }
    return;
  }

  auto *record = mgp::result_new_record(result);
  if (record == nullptr) throw mg_exception::NotEnoughMemoryException();

  mg_utility::InsertNodeValueResult(record, kFieldNode, vertex, memory);
  mg_utility::InsertDoubleValueResult(record, kFieldRank, rank, memory);
}

/// Memgraph query module implementation of parallel pagerank_module algorithm.
/// PageRank is the algorithm for measuring influence of connected nodes.
///
/// @param args Memgraph module arguments
/// @param memgraphGraph Memgraph graph instance
/// @param result Memgraph result storage
/// @param memory Memgraph memory storage
void PagerankWrapper(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto max_iterations = mgp::value_get_int(mgp::list_at(args, 0));
    auto damping_factor = mgp::value_get_double(mgp::list_at(args, 1));
    auto stop_epsilon = mgp::value_get_double(mgp::list_at(args, 2));
    auto num_threads = mgp::value_get_int(mgp::list_at(args, 3));

    auto pagerank_graph = pagerank_alg::CreatePageRankGraph(memgraph_graph, memory);
    auto pageranks = pagerank_alg::ParallelIterativePageRank(pagerank_graph, max_iterations, damping_factor,
                                                             stop_epsilon, num_threads);

    for (std::uint64_t node_id = 0; node_id < pagerank_graph.GetNodeCount(); ++node_id) {
      InsertPagerankRecord(memgraph_graph, result, memory, pagerank_graph.GetMemgraphNodeId(node_id),
                           pageranks[node_id]);
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp_value *default_max_iterations;
  mgp_value *default_damping_factor;
  mgp_value *default_stop_epsilon;
  mgp_value *default_num_threads;
  try {
    auto *pagerank_proc = mgp::module_add_read_procedure(module, kProcedureGet, PagerankWrapper);

    default_max_iterations = mgp::value_make_int(100, memory);
    default_damping_factor = mgp::value_make_double(0.85, memory);
    default_stop_epsilon = mgp::value_make_double(1e-5, memory);
    default_num_threads = mgp::value_make_int(1, memory);

    mgp::proc_add_opt_arg(pagerank_proc, kArgumentMaxIterations, mgp::type_int(), default_max_iterations);
    mgp::proc_add_opt_arg(pagerank_proc, kArgumentDampingFactor, mgp::type_float(), default_damping_factor);
    mgp::proc_add_opt_arg(pagerank_proc, kArgumentStopEpsilon, mgp::type_float(), default_stop_epsilon);
    mgp::proc_add_opt_arg(pagerank_proc, kArgumentNumThreads, mgp::type_int(), default_num_threads);

    // Query module output record
    mgp::proc_add_result(pagerank_proc, kFieldNode, mgp::type_node());
    mgp::proc_add_result(pagerank_proc, kFieldRank, mgp::type_float());

  } catch (const std::exception &e) {
    // Destroy values if exception occurs earlier
    mgp_value_destroy(default_max_iterations);
    mgp_value_destroy(default_damping_factor);
    mgp_value_destroy(default_stop_epsilon);
    mgp_value_destroy(default_num_threads);
    return 1;
  }

  mgp_value_destroy(default_max_iterations);
  mgp_value_destroy(default_damping_factor);
  mgp_value_destroy(default_stop_epsilon);
  mgp_value_destroy(default_num_threads);

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
