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

#include <mgp.hpp>
#include <string_view>

constexpr std::string_view kProcedure = "procedure";
constexpr std::string_view kProcedureArg = "arg";
constexpr std::string_view kProcedureResult = "result";

void Procedure(mgp_list * /*args*/, mgp_graph * /*graph*/, mgp_result *result, mgp_memory *memory) {
  auto *result_record = mgp::result_new_record(result);
  mgp::result_record_insert(result_record, kProcedureResult.data(), mgp::value_make_int(0, memory));
}

extern "C" int mgp_init_module(struct mgp_module *query_module, struct mgp_memory *memory) {
  try {
    auto *proc = mgp::module_add_read_procedure(query_module, kProcedure.data(), Procedure);
    mgp::proc_add_arg(proc, kProcedureArg.data(), mgp::type_node());
    mgp::proc_add_result(proc, kProcedureResult.data(), mgp::type_int());

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
