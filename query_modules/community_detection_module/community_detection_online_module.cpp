// Copyright 2025 Memgraph Ltd.
//
// Licensed as a Memgraph Enterprise file under the Memgraph Enterprise
// License (the "License"); by using this file, you agree to be bound by the terms of the License, and you may not use
// this file except in compliance with the License. You may obtain a copy of the License at https://memgraph.com/legal.

#include <mg_graph.hpp>
#include <mg_utils.hpp>

#include "algorithm_online/community_detection.hpp"
#include "mg_procedure.h"
#include "mgp.hpp"

namespace {
constexpr std::string_view kFieldNode{"node"};
constexpr std::string_view kFieldCommunityId{"community_id"};

constexpr std::string_view kFieldMessage{"message"};

constexpr std::string_view kDirected{"directed"};
constexpr std::string_view kWeighted{"weighted"};
constexpr std::string_view kSimilarityThreshold{"similarity_threshold"};
constexpr std::string_view kExponent{"exponent"};
constexpr std::string_view kMinValue{"min_value"};
constexpr std::string_view kWeightProperty{"weight_property"};
constexpr std::string_view kWSelfloop{"w_selfloop"};
constexpr std::string_view kMaxIterations{"max_iterations"};
constexpr std::string_view kMaxUpdates{"max_updates"};

constexpr std::string_view kCreatedVertices{"createdVertices"};
constexpr std::string_view kCreatedEdges{"createdEdges"};
constexpr std::string_view kUpdatedVertices{"updatedVertices"};
constexpr std::string_view kUpdatedEdges{"updatedEdges"};
constexpr std::string_view kDeletedVertices{"deletedVertices"};
constexpr std::string_view kDeletedEdges{"deletedEdges"};

LabelRankT::LabelRankT algorithm = LabelRankT::LabelRankT();
bool initialized = false;

auto saved_directedness = false;
auto saved_weightedness = false;
std::string saved_weight_property = "weight";

constexpr double DEFAULT_WEIGHT = 1.0;
}  // namespace

void InsertCommunityDetectionRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, std::uint64_t node_id,
                                    std::uint64_t community_id) {
  auto *record = mgp::result_new_record(result);

  if (mg_utility::InsertNodeValueResult(graph, record, kFieldNode.data(), node_id, memory)) {
    mg_utility::InsertIntValueResult(record, kFieldCommunityId.data(), community_id, memory);
  }
}

void InsertMessageRecord(mgp_result *result, mgp_memory *memory, const char *message) {
  auto *record = mgp::result_new_record(result);

  mg_utility::InsertStringValueResult(record, kFieldMessage.data(), message, memory);
}

void Set(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result,
                                "To use community detection online module you need a valid enterprise license.");
      return;
    }

    const auto directed = mgp::value_get_bool(mgp::list_at(args, 0));
    const auto weighted = mgp::value_get_bool(mgp::list_at(args, 1));
    const auto similarity_threshold = mgp::value_get_double(mgp::list_at(args, 2));
    const auto exponent = mgp::value_get_double(mgp::list_at(args, 3));
    const auto min_value = mgp::value_get_double(mgp::list_at(args, 4));
    const auto weight_property = mgp::value_get_string(mgp::list_at(args, 5));
    const auto w_selfloop = weighted ? mgp::value_get_double(mgp::list_at(args, 6)) : 1.0;
    const auto max_iterations = mgp::value_get_int(mgp::list_at(args, 7));
    const auto max_updates = mgp::value_get_int(mgp::list_at(args, 8));

    ::saved_directedness = directed;
    ::saved_weightedness = weighted;
    ::saved_weight_property = weight_property;

    const auto graph_type =
        saved_directedness ? mg_graph::GraphType::kDirectedGraph : mg_graph::GraphType::kUndirectedGraph;
    auto graph = saved_weightedness ? mg_utility::GetWeightedGraphView(memgraph_graph, result, memory, graph_type,
                                                                       saved_weight_property.c_str(), DEFAULT_WEIGHT)
                                    : mg_utility::GetGraphView(memgraph_graph, result, memory, graph_type);

    const auto labels = algorithm.SetLabels(std::move(graph), directed, weighted, similarity_threshold, exponent,
                                            min_value, weight_property, w_selfloop, max_iterations, max_updates);
    ::initialized = true;

    for (const auto [node_id, label] : labels) {
      InsertCommunityDetectionRecord(memgraph_graph, result, memory, node_id, label);
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void Get(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result,
                                "To use community detection online module you need a valid enterprise license.");
      return;
    }

    const auto graph_type =
        saved_directedness ? mg_graph::GraphType::kDirectedGraph : mg_graph::GraphType::kUndirectedGraph;
    auto graph = saved_weightedness ? mg_utility::GetWeightedGraphView(memgraph_graph, result, memory, graph_type,
                                                                       saved_weight_property.c_str(), DEFAULT_WEIGHT)
                                    : mg_utility::GetGraphView(memgraph_graph, result, memory, graph_type);

    const auto labels = initialized ? algorithm.GetLabels(std::move(graph)) : algorithm.SetLabels(std::move(graph));

    for (const auto [node_id, label] : labels) {
      // Previously calculated labels returned by GetLabels() may contain
      // deleted nodes; skip them as they cannot be inserted
      auto *node = mgp::graph_get_vertex_by_id(memgraph_graph, mgp_vertex_id{.as_int = (int)node_id}, memory);
      if (!node) {
        mgp::vertex_destroy(node);
        continue;
      }

      mgp::vertex_destroy(node);
      InsertCommunityDetectionRecord(memgraph_graph, result, memory, node_id, label);
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void Update(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result,
                                "To use community detection online module you need a valid enterprise license.");
      return;
    }

    const auto created_nodes = mgp::value_get_list(mgp::list_at(args, 0));
    const auto created_edges = mgp::value_get_list(mgp::list_at(args, 1));
    const auto updated_nodes = mgp::value_get_list(mgp::list_at(args, 2));
    const auto updated_edges = mgp::value_get_list(mgp::list_at(args, 3));
    const auto deleted_nodes = mgp::value_get_list(mgp::list_at(args, 4));
    const auto deleted_edges = mgp::value_get_list(mgp::list_at(args, 5));

    const auto graph_type =
        saved_directedness ? mg_graph::GraphType::kDirectedGraph : mg_graph::GraphType::kUndirectedGraph;
    auto graph = saved_weightedness ? mg_utility::GetWeightedGraphView(memgraph_graph, result, memory, graph_type,
                                                                       saved_weight_property.c_str(), DEFAULT_WEIGHT)
                                    : mg_utility::GetGraphView(memgraph_graph, result, memory, graph_type);

    if (initialized) {
      auto modified_node_ids = mg_utility::GetNodeIDs(created_nodes);
      auto modified_edge_endpoint_ids = mg_utility::GetEdgeEndpointIDs(created_edges);

      auto updated_node_ids = mg_utility::GetNodeIDs(updated_nodes);
      modified_node_ids.insert(modified_node_ids.end(), updated_node_ids.begin(), updated_node_ids.end());
      auto updated_edge_endpoint_ids = mg_utility::GetEdgeEndpointIDs(updated_edges);
      modified_edge_endpoint_ids.insert(modified_edge_endpoint_ids.end(), updated_edge_endpoint_ids.begin(),
                                        updated_edge_endpoint_ids.end());

      const auto deleted_node_ids = mg_utility::GetNodeIDs(deleted_nodes);
      const auto deleted_edge_endpoint_ids = mg_utility::GetEdgeEndpointIDs(deleted_edges);

      const auto labels = algorithm.UpdateLabels(std::move(graph), modified_node_ids, modified_edge_endpoint_ids,
                                                 deleted_node_ids, deleted_edge_endpoint_ids);

      for (const auto [node_id, label] : labels) {
        InsertCommunityDetectionRecord(memgraph_graph, result, memory, node_id, label);
      }
    } else {
      const auto labels = algorithm.SetLabels(std::move(graph));

      for (const auto [node_id, label] : labels) {
        InsertCommunityDetectionRecord(memgraph_graph, result, memory, node_id, label);
      }
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void Reset(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    if (!mgp_is_enterprise_valid()) {
      mgp::result_set_error_msg(result,
                                "To use community detection online module you need a valid enterprise license.");
      return;
    }

    ::algorithm = LabelRankT::LabelRankT();
    ::initialized = false;

    ::saved_directedness = false;
    ::saved_weightedness = false;
    ::saved_weight_property = "weight";

    InsertMessageRecord(result, memory, "The algorithm has been successfully reset!");
  } catch (const std::exception &e) {
    InsertMessageRecord(result, memory, "Reset failed: An exception occurred, please check your module!");
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  {
    try {
      auto *set_proc = mgp::module_add_read_procedure(module, "set", Set);

      auto default_directed = mgp::value_make_bool(0, memory);
      auto default_weighted = mgp::value_make_bool(0, memory);
      auto default_similarity_threshold = mgp::value_make_double(0.7, memory);
      auto default_exponent = mgp::value_make_double(4.0, memory);
      auto default_min_value = mgp::value_make_double(0.1, memory);
      auto default_weight_property = mgp::value_make_string("weight", memory);
      auto default_w_selfloop = mgp::value_make_double(1.0, memory);
      auto default_max_iterations = mgp::value_make_int(100, memory);
      auto default_max_updates = mgp::value_make_int(5, memory);

      mgp::proc_add_opt_arg(set_proc, kDirected.data(), mgp::type_bool(), default_directed);
      mgp::proc_add_opt_arg(set_proc, kWeighted.data(), mgp::type_bool(), default_weighted);
      mgp::proc_add_opt_arg(set_proc, kSimilarityThreshold.data(), mgp::type_float(), default_similarity_threshold);
      mgp::proc_add_opt_arg(set_proc, kExponent.data(), mgp::type_float(), default_exponent);
      mgp::proc_add_opt_arg(set_proc, kMinValue.data(), mgp::type_float(), default_min_value);
      mgp::proc_add_opt_arg(set_proc, kWeightProperty.data(), mgp::type_string(), default_weight_property);
      mgp::proc_add_opt_arg(set_proc, kWSelfloop.data(), mgp::type_float(), default_w_selfloop);
      mgp::proc_add_opt_arg(set_proc, kMaxIterations.data(), mgp::type_int(), default_max_iterations);
      mgp::proc_add_opt_arg(set_proc, kMaxUpdates.data(), mgp::type_int(), default_max_updates);

      mgp::proc_add_result(set_proc, kFieldNode.data(), mgp::type_node());
      mgp::proc_add_result(set_proc, kFieldCommunityId.data(), mgp::type_int());

      mgp::value_destroy(default_directed);
      mgp::value_destroy(default_weighted);
      mgp::value_destroy(default_similarity_threshold);
      mgp::value_destroy(default_exponent);
      mgp::value_destroy(default_min_value);
      mgp::value_destroy(default_weight_property);
      mgp::value_destroy(default_w_selfloop);
      mgp::value_destroy(default_max_iterations);
      mgp::value_destroy(default_max_updates);
    } catch (const std::exception &e) {
      return 1;
    }
  }

  {
    try {
      auto *get_proc = mgp::module_add_read_procedure(module, "get", Get);

      mgp::proc_add_result(get_proc, kFieldNode.data(), mgp::type_node());
      mgp::proc_add_result(get_proc, kFieldCommunityId.data(), mgp::type_int());
    } catch (const std::exception &e) {
      return 1;
    }
  }

  {
    try {
      auto *update_proc = mgp::module_add_read_procedure(module, "update", Update);

      auto default_created_vertices = mgp::value_make_list(mgp::list_make_empty(0, memory));
      auto default_created_edges = mgp::value_make_list(mgp::list_make_empty(0, memory));
      auto default_updated_vertices = mgp::value_make_list(mgp::list_make_empty(0, memory));
      auto default_updated_edges = mgp::value_make_list(mgp::list_make_empty(0, memory));
      auto default_deleted_vertices = mgp::value_make_list(mgp::list_make_empty(0, memory));
      auto default_deleted_edges = mgp::value_make_list(mgp::list_make_empty(0, memory));

      mgp::proc_add_opt_arg(update_proc, kCreatedVertices.data(), mgp::type_list(mgp::type_node()),
                            default_created_vertices);
      mgp::proc_add_opt_arg(update_proc, kCreatedEdges.data(), mgp::type_list(mgp::type_relationship()),
                            default_deleted_edges);
      mgp::proc_add_opt_arg(update_proc, kUpdatedVertices.data(), mgp::type_list(mgp::type_node()),
                            default_updated_vertices);
      mgp::proc_add_opt_arg(update_proc, kUpdatedEdges.data(), mgp::type_list(mgp::type_relationship()),
                            default_updated_edges);
      mgp::proc_add_opt_arg(update_proc, kDeletedVertices.data(), mgp::type_list(mgp::type_node()),
                            default_deleted_vertices);
      mgp::proc_add_opt_arg(update_proc, kDeletedEdges.data(), mgp::type_list(mgp::type_relationship()),
                            default_deleted_edges);

      mgp::proc_add_result(update_proc, kFieldNode.data(), mgp::type_node());
      mgp::proc_add_result(update_proc, kFieldCommunityId.data(), mgp::type_int());

      mgp::value_destroy(default_created_vertices);
      mgp::value_destroy(default_created_edges);
      mgp::value_destroy(default_updated_vertices);
      mgp::value_destroy(default_updated_edges);
      mgp::value_destroy(default_deleted_vertices);
      mgp::value_destroy(default_deleted_edges);
    } catch (const std::exception &e) {
      return 1;
    }
  }

  {
    try {
      auto *reset_proc = mgp::module_add_read_procedure(module, "reset", Reset);

      mgp::proc_add_result(reset_proc, kFieldMessage.data(), mgp::type_string());
    } catch (const std::exception &e) {
      return 1;
    }
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
