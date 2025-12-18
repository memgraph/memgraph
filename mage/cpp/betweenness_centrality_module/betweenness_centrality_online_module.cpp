// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

#include <algorithm>
#include <string>
#include <thread>

#include <mg_generate.hpp>
#include <mg_graph.hpp>
#include <mg_utils.hpp>

#include "algorithm_online/betweenness_centrality_online.hpp"
#include "mg_procedure.h"

namespace {
constexpr char const *kProcedureSet = "set";
constexpr char const *kProcedureGet = "get";
constexpr char const *kProcedureUpdate = "update";
constexpr char const *kProcedureReset = "reset";

constexpr char const *kArgumentCreatedVertices = "created_vertices";
constexpr char const *kArgumentCreatedEdges = "created_edges";
constexpr char const *kArgumentDeletedVertices = "deleted_vertices";
constexpr char const *kArgumentDeletedEdges = "deleted_edges";
constexpr char const *kArgumentNormalize = "normalize";
constexpr char const *kArgumentThreads = "threads";

constexpr char const *kFieldNode = "node";
constexpr char const *kFieldBCScore = "betweenness_centrality";
constexpr char const *kFieldMessage = "message";

auto algorithm = online_bc::OnlineBC();

///@brief Tests if given node is connected to the rest of the graph via given edge.
bool ConnectedVia(std::uint64_t node_id, std::pair<std::uint64_t, std::uint64_t> edge) {
  return (edge.first == node_id || edge.second == node_id) && edge.first != edge.second;  // exclude self-loops
}
}  // namespace

void InsertOnlineBCRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const std::uint64_t node_id,
                          double bc_score) {
  auto *node = mg_utility::GetNodeForInsertion(node_id, graph, memory);
  if (!node) return;

  auto *record = mgp::result_new_record(result);
  if (record == nullptr) throw mg_exception::NotEnoughMemoryException();

  mg_utility::InsertNodeValueResult(record, kFieldNode, node, memory);
  mg_utility::InsertDoubleValueResult(record, kFieldBCScore, bc_score, memory);
}

void InsertMessageRecord(mgp_result *result, mgp_memory *memory, const char *message) {
  auto *record = mgp::result_new_record(result);

  mg_utility::InsertStringValueResult(record, kFieldMessage, message, memory);
}

void Set(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result,
                                "To use betweenness centrality online module you need a valid enterprise license.");
      return;
    }

    const auto normalize = mgp::value_get_bool(mgp::list_at(args, 0));
    auto threads = mgp::value_get_int(mgp::list_at(args, 1));

    if (threads <= 0) threads = std::thread::hardware_concurrency();

    auto graph = mg_utility::GetGraphView(memgraph_graph, result, memory, mg_graph::GraphType::kUndirectedGraph);
    const auto node_bc_scores = algorithm.Set(*graph, normalize, threads);

    for (const auto [node_id, bc_score] : node_bc_scores) {
      InsertOnlineBCRecord(memgraph_graph, result, memory, node_id, bc_score);
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void Get(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result,
                                "To use betweenness centrality online module you need a valid enterprise license.");
      return;
    }
    const auto normalize = mgp::value_get_bool(mgp::list_at(args, 0));

    auto graph = mg_utility::GetGraphView(memgraph_graph, result, memory, mg_graph::GraphType::kUndirectedGraph);

    std::unordered_map<uint64_t, double> node_bc_scores;
    if (!algorithm.Initialized())
      node_bc_scores = algorithm.Set(*graph, normalize);
    else
      node_bc_scores = algorithm.Get(*graph, normalize);

    for (const auto [node_id, bc_score] : node_bc_scores) {
      InsertOnlineBCRecord(memgraph_graph, result, memory, node_id, bc_score);
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void Update(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result,
                                "To use betweenness centrality online module you need a valid enterprise license.");
      return;
    }
    const auto normalize = mgp::value_get_bool(mgp::list_at(args, 4));
    const auto threads = mgp::value_get_int(mgp::list_at(args, 5));

    auto graph = mg_utility::GetGraphView(memgraph_graph, result, memory, mg_graph::GraphType::kUndirectedGraph);
    std::unordered_map<uint64_t, double> node_bc_scores;

    if (!algorithm.Initialized()) {
      node_bc_scores = algorithm.Set(*graph, normalize, threads);
    } else {
      std::vector<std::uint64_t> graph_nodes_ids;
      for (const auto [node_id] : graph->Nodes()) {
        graph_nodes_ids.push_back(graph->GetMemgraphNodeId(node_id));
      }

      const auto created_nodes = mg_utility::GetNodeIDs(mgp::value_get_list(mgp::list_at(args, 0)));
      const auto created_edges = mg_utility::GetEdgeEndpointIDs(mgp::value_get_list(mgp::list_at(args, 1)));
      const auto deleted_nodes = mg_utility::GetNodeIDs(mgp::value_get_list(mgp::list_at(args, 2)));
      const auto deleted_edges = mg_utility::GetEdgeEndpointIDs(mgp::value_get_list(mgp::list_at(args, 3)));

      // Check if online update can be used
      if (created_nodes.size() == 0 && deleted_nodes.size() == 0) {  // Edge update
        // Get edges as before the update
        std::vector<std::pair<std::uint64_t, std::uint64_t>> prior_edges_ids;
        for (const auto edge_inner_ids : graph->Edges()) {
          const std::pair<std::uint64_t, std::uint64_t> edge{graph->GetMemgraphNodeId(edge_inner_ids.from),
                                                             graph->GetMemgraphNodeId(edge_inner_ids.to)};
          // Newly created edges aren’t part of the prior graph
          if (std::find(created_edges.begin(), created_edges.end(), edge) == created_edges.end())
            prior_edges_ids.push_back(edge);
        }
        for (const auto deleted_edge : deleted_edges) {
          prior_edges_ids.push_back(deleted_edge);
        }

        auto prior_graph =
            mg_generate::BuildGraph(graph_nodes_ids, prior_edges_ids, mg_graph::GraphType::kUndirectedGraph);

        node_bc_scores = algorithm.Get(*prior_graph, normalize);

        for (const auto created_edge : created_edges) {
          prior_edges_ids.push_back(created_edge);
          graph = mg_generate::BuildGraph(graph_nodes_ids, prior_edges_ids, mg_graph::GraphType::kUndirectedGraph);
          node_bc_scores = algorithm.EdgeUpdate(*prior_graph, *graph, online_bc::Operation::CREATE_EDGE, created_edge,
                                                normalize, threads);
          prior_graph = std::move(graph);
        }
        for (const auto deleted_edge : deleted_edges) {
          prior_edges_ids.erase(std::remove(prior_edges_ids.begin(), prior_edges_ids.end(), deleted_edge),
                                prior_edges_ids.end());
          graph = mg_generate::BuildGraph(graph_nodes_ids, prior_edges_ids, mg_graph::GraphType::kUndirectedGraph);
          node_bc_scores = algorithm.EdgeUpdate(*prior_graph, *graph, online_bc::Operation::DELETE_EDGE, deleted_edge,
                                                normalize, threads);
          prior_graph = std::move(graph);
        }
      } else if (created_edges.size() == 0 && deleted_edges.size() == 0) {  // Node update
        for (const auto created_node_id : created_nodes) {
          algorithm.NodeUpdate(online_bc::Operation::CREATE_NODE, created_node_id, normalize);
        }
        for (const auto deleted_node_id : deleted_nodes) {
          algorithm.NodeUpdate(online_bc::Operation::DELETE_NODE, deleted_node_id, normalize);
        }
      } else if (created_nodes.size() == 1 && created_edges.size() == 1 &&
                 ConnectedVia(created_nodes[0], created_edges[0])) {  // Create-and-attach-node update
        algorithm.NodeEdgeUpdate(*graph, online_bc::Operation::CREATE_ATTACH_NODE, created_nodes[0], created_edges[0],
                                 normalize);
      } else if (deleted_nodes.size() == 1 && deleted_edges.size() == 1 &&
                 ConnectedVia(deleted_nodes[0], deleted_edges[0])) {  // Detach-and-delete-node update
        algorithm.NodeEdgeUpdate(*graph, online_bc::Operation::DETACH_DELETE_NODE, deleted_nodes[0], deleted_edges[0],
                                 normalize);
      } else {  // Default to offline update
        node_bc_scores = algorithm.Set(*graph, normalize, threads);
      }
    }

    for (const auto [node_id, bc_score] : node_bc_scores) {
      InsertOnlineBCRecord(memgraph_graph, result, memory, node_id, bc_score);
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void Reset(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result,
                                "To use betweenness centrality online module you need a valid enterprise license.");
      return;
    }
    algorithm = online_bc::OnlineBC();
    InsertMessageRecord(result, memory, "The algorithm has been successfully reset!");
  } catch (const std::exception &e) {
    InsertMessageRecord(result, memory, "Reset failed: An exception occurred, please check your module!");
    return;
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  // Add .set()
  {
    try {
      auto *set_proc = mgp::module_add_read_procedure(module, kProcedureSet, Set);

      auto default_normalize = mgp::value_make_bool(true, memory);
      auto default_threads = mgp::value_make_int(std::thread::hardware_concurrency(), memory);

      mgp::proc_add_opt_arg(set_proc, kArgumentNormalize, mgp::type_bool(), default_normalize);
      mgp::proc_add_opt_arg(set_proc, kArgumentThreads, mgp::type_int(), default_threads);

      mgp::value_destroy(default_normalize);
      mgp::value_destroy(default_threads);

      mgp::proc_add_result(set_proc, kFieldNode, mgp::type_node());
      mgp::proc_add_result(set_proc, kFieldBCScore, mgp::type_float());
    } catch (const std::exception &e) {
      return 1;
    }
  }

  // Add .get()
  {
    try {
      auto *get_proc = mgp::module_add_read_procedure(module, kProcedureGet, Get);

      auto default_normalize = mgp::value_make_bool(true, memory);
      mgp::proc_add_opt_arg(get_proc, kArgumentNormalize, mgp::type_bool(), default_normalize);

      mgp::proc_add_result(get_proc, kFieldNode, mgp::type_node());
      mgp::proc_add_result(get_proc, kFieldBCScore, mgp::type_float());
    } catch (const std::exception &e) {
      return 1;
    }
  }

  // Add .update()
  {
    try {
      auto *update_proc = mgp::module_add_read_procedure(module, kProcedureUpdate, Update);

      auto default_vertices = mgp::value_make_list(mgp::list_make_empty(0, memory));
      auto default_edges = mgp::value_make_list(mgp::list_make_empty(0, memory));
      auto default_normalize = mgp::value_make_bool(true, memory);
      auto default_threads = mgp::value_make_int(std::thread::hardware_concurrency(), memory);

      mgp::proc_add_opt_arg(update_proc, kArgumentCreatedVertices, mgp::type_list(mgp::type_node()), default_vertices);
      mgp::proc_add_opt_arg(update_proc, kArgumentCreatedEdges, mgp::type_list(mgp::type_relationship()),
                            default_edges);
      mgp::proc_add_opt_arg(update_proc, kArgumentDeletedVertices, mgp::type_list(mgp::type_node()), default_vertices);
      mgp::proc_add_opt_arg(update_proc, kArgumentDeletedEdges, mgp::type_list(mgp::type_relationship()),
                            default_edges);
      mgp::proc_add_opt_arg(update_proc, kArgumentNormalize, mgp::type_bool(), default_normalize);
      mgp::proc_add_opt_arg(update_proc, kArgumentThreads, mgp::type_int(), default_threads);

      mgp::value_destroy(default_vertices);
      mgp::value_destroy(default_edges);
      mgp::value_destroy(default_normalize);
      mgp::value_destroy(default_threads);

      mgp::proc_add_result(update_proc, kFieldNode, mgp::type_node());
      mgp::proc_add_result(update_proc, kFieldBCScore, mgp::type_float());
    } catch (const std::exception &e) {
      return 1;
    }
  }

  // Add .reset()
  {
    try {
      auto *reset_proc = mgp::module_add_read_procedure(module, kProcedureReset, Reset);
      mgp::proc_add_result(reset_proc, kFieldMessage, mgp::type_string());
    } catch (const std::exception &e) {
      return 1;
    }
  }

  algorithm.Reset();

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
