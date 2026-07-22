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

#include "algorithm/bipartite_matching.hpp"

namespace {

constexpr char const *kProcedureMax = "max";

constexpr char const *kFieldMatchingNumber = "maximum_bipartite_matching";

void InsertBipartiteMatchingRecord(mgp_result *result, mgp_memory *memory, const int matching_number) {
  auto *record = mgp::result_new_record(result);

  mg_utility::InsertIntValueResult(record, kFieldMatchingNumber, matching_number, memory);
}

// NOLINTNEXTLINE(misc-unused-parameters)
void GetMaximumBipartiteMatching(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    auto graph = mg_utility::GetGraphView(memgraph_graph, result, memory, mg_graph::GraphType::kUndirectedGraph);
    auto maximum_bipartite_matching = bipartite_matching_alg::BipartiteMatching(*graph);

    InsertBipartiteMatchingRecord(result, memory, static_cast<int>(maximum_bipartite_matching));
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
    mgp_proc *proc = mgp::module_add_read_procedure(module, kProcedureMax, GetMaximumBipartiteMatching);

    mgp::proc_add_result(proc, kFieldMatchingNumber, mgp::type_int());
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
