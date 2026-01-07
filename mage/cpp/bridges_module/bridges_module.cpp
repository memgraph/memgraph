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

#include <mg_exceptions.hpp>
#include <mg_utils.hpp>

#include "algorithm/bridges.hpp"

namespace {

constexpr const char *const kProcedureGet = "get";

// constexpr const char *const fieldEdgeID = "edge_id";
constexpr const char *const k_field_node_from = "node_from";
constexpr const char *const k_field_node_to = "node_to";

void InsertBridgeRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const std::uint64_t node_from_id,
                        const std::uint64_t node_to_id) {
  auto *node_from = mg_utility::GetNodeForInsertion(static_cast<int>(node_from_id), graph, memory);
  auto *node_to = mg_utility::GetNodeForInsertion(static_cast<int>(node_to_id), graph, memory);
  if (!node_from || !node_to) return;

  auto *record = mgp::result_new_record(result);
  if (record == nullptr) throw mg_exception::NotEnoughMemoryException();

  mg_utility::InsertNodeValueResult(record, k_field_node_from, node_from, memory);
  mg_utility::InsertNodeValueResult(record, k_field_node_to, node_to, memory);
}

// NOLINTNEXTLINE(misc-unused-parameters)
void GetBridges(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto graph = mg_utility::GetGraphView(memgraph_graph, result, memory, mg_graph::GraphType::kUndirectedGraph);
    auto bridges = bridges_alg::GetBridges(*graph);

    for (const auto &bridge_edge : bridges) {
      InsertBridgeRecord(memgraph_graph, result, memory, graph->GetMemgraphNodeId(bridge_edge.from),
                         graph->GetMemgraphNodeId(bridge_edge.to));
    }
  } catch (const std::exception &e) {
    // We must not let any exceptions out of our module.
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}
}  // namespace

// Each module needs to define mgp_init_module function.
// Here you can register multiple procedures your module supports.
// NOLINTNEXTLINE(misc-unused-parameters)
extern "C" int mgp_init_module(mgp_module *module, mgp_memory *memory) {
  try {
    auto *proc = mgp::module_add_read_procedure(module, kProcedureGet, GetBridges);
    mgp::proc_add_result(proc, k_field_node_from, mgp::type_node());
    mgp::proc_add_result(proc, k_field_node_to, mgp::type_node());
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

// This is an optional function if you need to release any resources before the
// module is unloaded. You will probably need this if you acquired some
// resources in mgp_init_module.
extern "C" int mgp_shutdown_module() {
  // Return 0 to indicate success.
  return 0;
}
