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

#include <mg_utils.hpp>

#include "algorithm_online/pagerank.hpp"
#include "mgp.hpp"

constexpr char const *kProcedureSet = "set";
constexpr char const *kProcedureGet = "get";
constexpr char const *kProcedureUpdate = "update";
constexpr char const *kProcedureReset = "reset";

constexpr char const *kFieldNode = "node";
constexpr char const *kFieldRank = "rank";
constexpr char const *kFieldMessage = "message";

constexpr char const *kArgumentWalksPerNode = "walks_per_node";
constexpr char const *kArgumentWalkStopEpsilon = "walk_stop_epsilon";

constexpr char const *kArgumentCreatedVertices = "created_vertices";
constexpr char const *kArgumentCreatedEdges = "created_edges";
constexpr char const *kArgumentDeletedVertices = "deleted_vertices";
constexpr char const *kArgumentDeletedEdges = "deleted_edges";

void InsertPagerankRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const std::uint64_t node_id,
                          const double rank) {
  auto *record = mgp::result_new_record(result);

  if (mg_utility::InsertNodeValueResult(graph, record, kFieldNode, node_id, memory)) {
    mg_utility::InsertDoubleValueResult(record, kFieldRank, rank, memory);
  }
}

void InsertMessageRecord(mgp_result *result, mgp_memory *memory, const char *message) {
  auto *record = mgp::result_new_record(result);

  mg_utility::InsertStringValueResult(record, kFieldMessage, message, memory);
}

void OnlinePagerankGet(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result, "To use pagerank online module you need a valid enterprise license.");
      return;
    }

    auto graph = mg_utility::GetGraphView(memgraph_graph, result, memory, mg_graph::GraphType::kDirectedGraph);

    auto pageranks = pagerank_online_alg::GetPagerank(*graph);

    for (auto const &[node_id, rank] : pageranks) {
      InsertPagerankRecord(memgraph_graph, result, memory, node_id, rank);
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void OnlinePagerankSet(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result, "To use pagerank online module you need a valid enterprise license.");
      return;
    }

    auto walks_per_node = mgp::value_get_int(mgp::list_at(args, 0));
    auto walk_stop_epsilon = mgp::value_get_double(mgp::list_at(args, 1));

    auto graph = mg_utility::GetGraphView(memgraph_graph, result, memory, mg_graph::GraphType::kDirectedGraph);

    auto pageranks = pagerank_online_alg::SetPagerank(*graph, walks_per_node, walk_stop_epsilon);

    for (auto const &[node_id, rank] : pageranks) {
      InsertPagerankRecord(memgraph_graph, result, memory, node_id, rank);
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void OnlinePagerankUpdate(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result, "To use pagerank online module you need a valid enterprise license.");
      return;
    }

    // Created vertices
    auto created_vertices_list = mgp::value_get_list(mgp::list_at(args, 0));
    auto size = mgp::list_size(created_vertices_list);
    auto created_vertices = std::vector<std::uint64_t>(size);
    for (std::size_t i = 0; i < size; i++) {
      created_vertices[i] = mgp::vertex_get_id(mgp::value_get_vertex(mgp::list_at(created_vertices_list, i))).as_int;
    }

    auto created_edges_list = mgp::value_get_list(mgp::list_at(args, 1));
    size = mgp::list_size(created_edges_list);
    auto created_edges = std::vector<std::pair<std::uint64_t, std::uint64_t>>(size);
    for (std::size_t i = 0; i < size; i++) {
      auto edge = mgp::value_get_edge(mgp::list_at(created_edges_list, i));
      auto from = mgp::vertex_get_id(mgp::edge_get_from(edge)).as_int;
      auto to = mgp::vertex_get_id(mgp::edge_get_to(edge)).as_int;
      created_edges[i] = std::make_pair(from, to);
    }

    // Deleted vertices
    auto deleted_vertices_list = mgp::value_get_list(mgp::list_at(args, 2));
    size = mgp::list_size(deleted_vertices_list);
    auto deleted_vertices = std::vector<std::uint64_t>(size);
    for (std::size_t i = 0; i < size; i++) {
      deleted_vertices[i] = mgp::vertex_get_id(mgp::value_get_vertex(mgp::list_at(deleted_vertices_list, i))).as_int;
    }

    auto deleted_edges_list = mgp::value_get_list(mgp::list_at(args, 3));
    size = mgp::list_size(deleted_edges_list);
    auto deleted_edges = std::vector<std::pair<std::uint64_t, std::uint64_t>>(size);
    for (std::size_t i = 0; i < size; i++) {
      auto edge = mgp::value_get_edge(mgp::list_at(deleted_edges_list, i));
      auto from = mgp::vertex_get_id(mgp::edge_get_from(edge)).as_int;
      auto to = mgp::vertex_get_id(mgp::edge_get_to(edge)).as_int;
      deleted_edges[i] = std::make_pair(from, to);
    }

    auto graph = mg_utility::GetGraphView(memgraph_graph, result, memory, mg_graph::GraphType::kDirectedGraph);

    auto pageranks =
        pagerank_online_alg::UpdatePagerank(*graph, created_vertices, created_edges, deleted_vertices, deleted_edges);

    for (auto const [node_id, rank] : pageranks) {
      InsertPagerankRecord(memgraph_graph, result, memory, node_id, rank);
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void OnlinePagerankReset(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result, "To use pagerank online module you need a valid enterprise license.");
      return;
    }

    pagerank_online_alg::Reset();
    InsertMessageRecord(result, memory, "Pagerank context is reset! Before running again it will run initialization.");
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    InsertMessageRecord(result, memory, "Reset failed: An exception occurred, please check your module!");
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  // Online approximative PageRank solution
  {
    try {
      auto *pagerank_proc = mgp::module_add_read_procedure(module, kProcedureSet, OnlinePagerankSet);

      auto default_walks_per_node = mgp::value_make_int(10, memory);
      auto default_walk_stop_epsilon = mgp::value_make_double(0.1, memory);

      mgp::proc_add_opt_arg(pagerank_proc, kArgumentWalksPerNode, mgp::type_int(), default_walks_per_node);
      mgp::proc_add_opt_arg(pagerank_proc, kArgumentWalkStopEpsilon, mgp::type_float(), default_walk_stop_epsilon);

      mgp::value_destroy(default_walks_per_node);
      mgp::value_destroy(default_walk_stop_epsilon);

      // Query module output record
      mgp::proc_add_result(pagerank_proc, kFieldNode, mgp::type_node());
      mgp::proc_add_result(pagerank_proc, kFieldRank, mgp::type_float());

    } catch (const std::exception &e) {
      return 1;
    }
  }

  // Online approximative PageRank get results
  {
    try {
      auto *pagerank_proc = mgp::module_add_read_procedure(module, kProcedureGet, OnlinePagerankGet);

      // Query module output record
      mgp::proc_add_result(pagerank_proc, kFieldNode, mgp::type_node());
      mgp::proc_add_result(pagerank_proc, kFieldRank, mgp::type_float());

    } catch (const std::exception &e) {
      return 1;
    }
  }

  // Online approximative PageRank update state
  {
    try {
      auto *pagerank_proc = mgp::module_add_read_procedure(module, kProcedureUpdate, OnlinePagerankUpdate);

      auto default_created_vertices = mgp::value_make_list(mgp::list_make_empty(0, memory));
      auto default_created_edges = mgp::value_make_list(mgp::list_make_empty(0, memory));
      auto default_deleted_vertices = mgp::value_make_list(mgp::list_make_empty(0, memory));
      auto default_deleted_edges = mgp::value_make_list(mgp::list_make_empty(0, memory));

      mgp::proc_add_opt_arg(pagerank_proc, kArgumentCreatedVertices,
                            mgp::type_nullable(mgp::type_list(mgp::type_node())), default_created_vertices);
      mgp::proc_add_opt_arg(pagerank_proc, kArgumentCreatedEdges,
                            mgp::type_nullable(mgp::type_list(mgp::type_relationship())), default_created_edges);
      mgp::proc_add_opt_arg(pagerank_proc, kArgumentDeletedVertices,
                            mgp::type_nullable(mgp::type_list(mgp::type_node())), default_deleted_vertices);
      mgp::proc_add_opt_arg(pagerank_proc, kArgumentDeletedEdges,
                            mgp::type_nullable(mgp::type_list(mgp::type_relationship())), default_deleted_edges);

      mgp::value_destroy(default_created_vertices);
      mgp::value_destroy(default_created_edges);
      mgp::value_destroy(default_deleted_vertices);
      mgp::value_destroy(default_deleted_edges);

      // Query module output record
      mgp::proc_add_result(pagerank_proc, kFieldNode, mgp::type_node());
      mgp::proc_add_result(pagerank_proc, kFieldRank, mgp::type_float());

    } catch (const std::exception &e) {
      return 1;
    }
  }

  // Add reset procedure
  {
    try {
      auto *pagerank_proc = mgp::module_add_read_procedure(module, kProcedureReset, OnlinePagerankReset);
      mgp::proc_add_result(pagerank_proc, kFieldMessage, mgp::type_string());

    } catch (const std::exception &e) {
      return 1;
    }
  }

  // When module is reloaded, we should reset the data structures
  pagerank_online_alg::Reset();

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
