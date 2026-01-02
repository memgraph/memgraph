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

#include <thread>

#include <mg_utils.hpp>

#include "algorithm/betweenness_centrality.hpp"

namespace {

constexpr char const *kProcedureGet = "get";

constexpr char const *kFieldBCScore = "betweenness_centrality";
constexpr char const *kFieldNode = "node";

constexpr char const *kArgumentDirected = "directed";
constexpr char const *kArgumentNormalized = "normalized";
constexpr char const *kArgumentThreads = "threads";

void InsertBCRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const double betweenness_centrality,
                    int node_id) {
  auto *node = mg_utility::GetNodeForInsertion(node_id, graph, memory);
  if (!node) return;

  auto *record = mgp::result_new_record(result);
  if (record == nullptr) throw mg_exception::NotEnoughMemoryException();

  mg_utility::InsertNodeValueResult(record, kFieldNode, node, memory);
  mg_utility::InsertDoubleValueResult(record, kFieldBCScore, betweenness_centrality, memory);
}

void GetBetweennessCentrality(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    const auto directed = mgp::value_get_bool(mgp::list_at(args, 0));
    const auto normalize = mgp::value_get_bool(mgp::list_at(args, 1));
    auto threads = mgp::value_get_int(mgp::list_at(args, 2));

    if (threads <= 0)
      threads = (std::thread::hardware_concurrency() > 1) ? (std::thread::hardware_concurrency() / 2) : 1;
    threads = std::min(std::thread::hardware_concurrency(), static_cast<unsigned int>(threads));

    const auto graph_type = directed ? mg_graph::GraphType::kDirectedGraph : mg_graph::GraphType::kUndirectedGraph;

    const auto graph = mg_utility::GetGraphView(memgraph_graph, result, memory, graph_type);
    const auto BC = betweenness_centrality_alg::BetweennessCentrality(*graph, directed, normalize, threads);

    const auto number_of_nodes = graph->Nodes().size();
    for (std::uint64_t node_id = 0; node_id < number_of_nodes; ++node_id)
      InsertBCRecord(memgraph_graph, result, memory, BC[node_id], graph->GetMemgraphNodeId(node_id));

  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

// Each module needs to define mgp_init_module function.
// Here you can register multiple procedures your module supports.
extern "C" int mgp_init_module(mgp_module *module, mgp_memory *memory) {
  mgp_value *bool_value_directed;
  mgp_value *bool_value_normalized;
  mgp_value *int_value_threads;

  try {
    auto *proc = mgp::module_add_read_procedure(module, kProcedureGet, GetBetweennessCentrality);

    // Query module arguments
    bool_value_directed = mgp::value_make_bool(true, memory);
    bool_value_normalized = mgp::value_make_bool(true, memory);
    int_value_threads = mgp::value_make_int(std::thread::hardware_concurrency(), memory);

    mgp::proc_add_opt_arg(proc, kArgumentDirected, mgp::type_bool(), bool_value_directed);
    mgp::proc_add_opt_arg(proc, kArgumentNormalized, mgp::type_bool(), bool_value_normalized);
    mgp::proc_add_opt_arg(proc, kArgumentThreads, mgp::type_int(), int_value_threads);

    // Query module output record
    mgp::proc_add_result(proc, kFieldNode, mgp::type_node());
    mgp::proc_add_result(proc, kFieldBCScore, mgp::type_float());

  } catch (const std::exception &e) {
    // Destroy the values if exception occurs
    mgp_value_destroy(bool_value_directed);
    mgp_value_destroy(bool_value_normalized);
    mgp_value_destroy(int_value_threads);
    return 1;
  }

  mgp_value_destroy(bool_value_directed);
  mgp_value_destroy(bool_value_normalized);
  mgp_value_destroy(int_value_threads);

  return 0;
}

// This is an optional function if you need to release any resources before the
// module is unloaded. You will probably need this if you acquired some
// resources in mgp_init_module.
extern "C" int mgp_shutdown_module() {
  // Return 0 to indicate success.
  return 0;
}
