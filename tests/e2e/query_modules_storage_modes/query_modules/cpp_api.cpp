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

#include <mgp.hpp>

constexpr std::string_view kFunctionPassRelationship = "pass_relationship";
constexpr std::string_view kPassRelationshipArg = "relationship";

constexpr std::string_view kProcedurePassNodeWithId = "pass_node_with_id";
constexpr std::string_view kPassNodeWithIdArg = "node";
constexpr std::string_view kPassNodeWithIdFieldNode = "node";
constexpr std::string_view kPassNodeWithIdFieldId = "id";

void PassRelationship(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    const auto arguments = mgp::List(args);
    auto result = mgp::Result(res);

    const auto relationship = arguments[0].ValueRelationship();

    std::this_thread::sleep_for(std::chrono::seconds(1));

    result.SetValue(relationship);
  } catch (const std::exception &e) {
    mgp::func_result_set_error_msg(res, e.what(), memory);
    return;
  }
}

void PassNodeWithId(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);
    const auto arguments = mgp::List(args);
    const auto record_factory = mgp::RecordFactory(result);

    const auto node = arguments[0].ValueNode();
    const auto node_id = node.Id().AsInt();

    std::this_thread::sleep_for(std::chrono::seconds(1));

    auto record = record_factory.NewRecord();
    record.Insert(kPassNodeWithIdFieldNode.data(), node);
    record.Insert(kPassNodeWithIdFieldId.data(), node_id);
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

extern "C" int mgp_init_module(struct mgp_module *query_module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);

    mgp::AddFunction(PassRelationship, kFunctionPassRelationship,
                     {mgp::Parameter(kPassRelationshipArg, mgp::Type::Relationship)}, query_module, memory);

    mgp::AddProcedure(
        PassNodeWithId, kProcedurePassNodeWithId, mgp::ProcedureType::Read,
        {mgp::Parameter(kPassNodeWithIdArg, mgp::Type::Node)},
        {mgp::Return(kPassNodeWithIdFieldNode, mgp::Type::Node), mgp::Return(kPassNodeWithIdFieldId, mgp::Type::Int)},
        query_module, memory);
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
