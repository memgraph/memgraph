// Copyright 2023 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <chrono>
#include <thread>

#include "_mgp.hpp"
#include "mg_exceptions.hpp"
#include "mg_procedure.h"

constexpr std::string_view kFunctionPassRelationship = "pass_relationship";
constexpr std::string_view kPassRelationshipArg = "relationship";

constexpr std::string_view kProcedurePassNodeWithId = "pass_node_with_id";
constexpr std::string_view kPassNodeWithIdArg = "node";
constexpr std::string_view kPassNodeWithIdFieldNode = "node";
constexpr std::string_view kPassNodeWithIdFieldId = "id";

void PassRelationship(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  auto *relationship = mgp::list_at(args, 0);

  std::this_thread::sleep_for(std::chrono::seconds(1));

  mgp::func_result_set_value(res, relationship, memory);
}

void PassNodeWithId(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  auto *node = mgp::value_get_vertex(mgp::list_at(args, 0));
  auto node_id = mgp::vertex_get_id(node).as_int;

  std::this_thread::sleep_for(std::chrono::seconds(1));

  auto *result_record = mgp::result_new_record(result);
  mgp::result_record_insert(result_record, kPassNodeWithIdFieldNode.data(), mgp::value_make_vertex(node));
  mgp::result_record_insert(result_record, kPassNodeWithIdFieldId.data(), mgp::value_make_int(node_id, memory));
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    auto *func = mgp::module_add_function(module, kFunctionPassRelationship.data(), PassRelationship);
    mgp::func_add_arg(func, kPassRelationshipArg.data(), mgp::type_relationship());

    auto *proc = mgp::module_add_read_procedure(module, kProcedurePassNodeWithId.data(), PassNodeWithId);
    mgp::proc_add_arg(proc, kPassNodeWithIdArg.data(), mgp::type_node());
    mgp::proc_add_result(proc, kPassNodeWithIdFieldNode.data(), mgp::type_node());
    mgp::proc_add_result(proc, kPassNodeWithIdFieldId.data(), mgp::type_int());
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
