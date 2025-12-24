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

#include <atomic>
#include <cstdint>
#include <mg_exceptions.hpp>
#include <mg_utils.hpp>

#include "_mgp.hpp"
#include "algorithm/leiden.hpp"
#include "mg_procedure.h"
#include "mgp.hpp"

namespace {

const char *kProcedureGet = "get";
const char *kProcedureGetSubgraph = "get_subgraph";
const char *kArgumentSubgraphNodes = "subgraph_nodes";
const char *kArgumentSubgraphRelationships = "subgraph_relationships";

const char *kFieldNode = "node";
const char *kFieldCommunity = "community_id";
const char *kFieldCommunities = "communities";
const char *kDefaultWeightProperty = "weight";
const double kDefaultGamma = 1.0;
const double kDefaultTheta = 0.01;
const double kDefaultResolutionParameter = 0.01;
const std::uint64_t kDefaultMaxIterations = std::numeric_limits<std::uint64_t>::max();

void InsertLeidenRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const std::uint64_t node_id,
                        const std::vector<std::uint64_t> &community) {
  auto *vertex = mg_utility::GetNodeForInsertion(node_id, graph, memory);
  if (!vertex) return;

  mgp_result_record *record = mgp::result_new_record(result);
  if (record == nullptr) throw mg_exception::NotEnoughMemoryException();

  mg_utility::InsertNodeValueResult(record, kFieldNode, vertex, memory);
  mg_utility::InsertIntValueResult(record, kFieldCommunity, community.back(), memory);

  auto *community_list = mgp::list_make_empty(0, memory);
  for (const auto &community_id : community) {
    mgp::list_append_extend(community_list, mgp::value_make_int(community_id, memory));
  }

  mg_utility::InsertListValueResult(record, kFieldCommunities, community_list, memory);
}

void LeidenCommunityDetection(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory,
                              bool subgraph) {
  auto index = 0;
  mgp_list *subgraph_nodes = nullptr;
  mgp_list *subgraph_relationships = nullptr;
  if (subgraph) {
    subgraph_nodes = mgp::value_get_list(mgp::list_at(args, index++));
    subgraph_relationships = mgp::value_get_list(mgp::list_at(args, index++));
  }
  const auto *weight_property = mgp::value_get_string(mgp::list_at(args, index++));
  const auto gamma = mgp::value_get_double(mgp::list_at(args, index++));
  const auto theta = mgp::value_get_double(mgp::list_at(args, index++));
  const auto resolution_parameter = mgp::value_get_double(mgp::list_at(args, index++));
  const auto max_iterations = mgp::value_get_int(mgp::list_at(args, index++));

  const auto graph =
      subgraph
          ? mg_utility::GetWeightedSubgraphView(memgraph_graph, result, memory, subgraph_nodes, subgraph_relationships,
                                                mg_graph::GraphType::kUndirectedGraph, weight_property, 1.0)
          : mg_utility::GetWeightedGraphView(memgraph_graph, result, memory, mg_graph::GraphType::kUndirectedGraph,
                                             weight_property, 1.0);
  auto communities = leiden_alg::GetCommunities(*graph, gamma, theta, resolution_parameter, max_iterations);
  if (communities.empty()) {
    mgp::result_set_error_msg(result, "No communities detected.");
    return;
  }

  for (std::size_t i = 0; i < communities.size(); i++) {
    InsertLeidenRecord(memgraph_graph, result, memory, graph->GetMemgraphNodeId(i), communities[i]);
  }
}

void OnGraph(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  try {
    LeidenCommunityDetection(args, memgraph_graph, result, memory, false);
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void OnSubgraph(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  try {
    LeidenCommunityDetection(args, memgraph_graph, result, memory, true);
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

extern "C" int mgp_init_module(mgp_module *module, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  try {
    auto *const default_weight_property = mgp::value_make_string(kDefaultWeightProperty, memory);
    auto *const default_gamma = mgp::value_make_double(kDefaultGamma, memory);
    auto *const default_theta = mgp::value_make_double(kDefaultTheta, memory);
    auto *const default_resolution_parameter = mgp::value_make_double(kDefaultResolutionParameter, memory);
    auto *const default_max_iterations = mgp::value_make_int(kDefaultMaxIterations, memory);

    {
      auto *proc = mgp::module_add_read_procedure(module, kProcedureGet, OnGraph);
      mgp::proc_add_opt_arg(proc, "weight_property", mgp::type_string(), default_weight_property);
      mgp::proc_add_opt_arg(proc, "gamma", mgp::type_float(), default_gamma);
      mgp::proc_add_opt_arg(proc, "theta", mgp::type_float(), default_theta);
      mgp::proc_add_opt_arg(proc, "resolution_parameter", mgp::type_float(), default_resolution_parameter);
      mgp::proc_add_opt_arg(proc, "number_of_iterations", mgp::type_int(), default_max_iterations);

      mgp::proc_add_result(proc, kFieldNode, mgp::type_node());
      mgp::proc_add_result(proc, kFieldCommunity, mgp::type_int());
      mgp::proc_add_result(proc, kFieldCommunities, mgp::type_list(mgp::type_int()));
    }

    {
      auto *proc = mgp::module_add_read_procedure(module, kProcedureGetSubgraph, OnSubgraph);

      mgp::proc_add_arg(proc, kArgumentSubgraphNodes, mgp::type_list(mgp::type_node()));
      mgp::proc_add_arg(proc, kArgumentSubgraphRelationships, mgp::type_list(mgp::type_relationship()));
      mgp::proc_add_opt_arg(proc, "weight_property", mgp::type_string(), default_weight_property);
      mgp::proc_add_opt_arg(proc, "gamma", mgp::type_float(), default_gamma);
      mgp::proc_add_opt_arg(proc, "theta", mgp::type_float(), default_theta);
      mgp::proc_add_opt_arg(proc, "resolution_parameter", mgp::type_float(), default_resolution_parameter);
      mgp::proc_add_opt_arg(proc, "number_of_iterations", mgp::type_int(), default_max_iterations);

      mgp::proc_add_result(proc, kFieldNode, mgp::type_node());
      mgp::proc_add_result(proc, kFieldCommunity, mgp::type_int());
      mgp::proc_add_result(proc, kFieldCommunities, mgp::type_list(mgp::type_int()));
    }

    mgp::value_destroy(default_gamma);
    mgp::value_destroy(default_theta);
    mgp::value_destroy(default_weight_property);
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }

}  // namespace
