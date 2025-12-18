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

#include "algorithm/katz.hpp"

#include <mg_utils.hpp>

namespace {

constexpr char const *kProcedureGet = "get";

constexpr char const *kArgumentAlpha = "alpha";
constexpr char const *kArgumentEpsilon = "epsilon";

constexpr char const *kFieldNode = "node";
constexpr char const *kFieldRank = "rank";

void InsertKatzRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, const double katz_centrality,
                      const int node_id) {
  auto *node = mg_utility::GetNodeForInsertion(node_id, graph, memory);
  if (!node) return;

  auto *record = mgp::result_new_record(result);
  if (record == nullptr) throw mg_exception::NotEnoughMemoryException();

  mg_utility::InsertNodeValueResult(record, kFieldNode, node, memory);
  mg_utility::InsertDoubleValueResult(record, kFieldRank, katz_centrality, memory);
}

void GetKatzCentrality(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto alpha = mgp::value_get_double(mgp::list_at(args, 0));
    auto epsilon = mgp::value_get_double(mgp::list_at(args, 1));

    auto graph = mg_utility::GetGraphView(memgraph_graph, result, memory, mg_graph::GraphType::kDirectedGraph);
    auto katz_centralities = katz_alg::SetKatz(*graph, alpha, epsilon);

    for (auto &[vertex_id, centrality] : katz_centralities) {
      InsertKatzRecord(memgraph_graph, result, memory, centrality, vertex_id);
    }
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

}  // namespace

extern "C" int mgp_init_module(mgp_module *module, mgp_memory *memory) {
  try {
    // Static Katz centrality
    {
      auto default_alpha = mgp::value_make_double(0.2, memory);
      auto default_epsilon = mgp::value_make_double(1e-2, memory);

      auto *proc = mgp::module_add_read_procedure(module, kProcedureGet, GetKatzCentrality);

      mgp::proc_add_opt_arg(proc, kArgumentAlpha, mgp::type_float(), default_alpha);
      mgp::proc_add_opt_arg(proc, kArgumentEpsilon, mgp::type_float(), default_epsilon);

      mgp::proc_add_result(proc, kFieldNode, mgp::type_node());
      mgp::proc_add_result(proc, kFieldRank, mgp::type_float());

      mgp::value_destroy(default_alpha);
      mgp::value_destroy(default_alpha);
    }
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
