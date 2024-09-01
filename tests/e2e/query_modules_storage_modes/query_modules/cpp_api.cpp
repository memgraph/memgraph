// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <thread>

#include <mgp.hpp>

constexpr std::string_view kFunctionPassRelationship = "pass_relationship";
constexpr std::string_view kPassRelationshipArg = "relationship";

constexpr std::string_view kProcedurePassNodeWithId = "pass_node_with_id";
constexpr std::string_view kPassNodeWithIdArg = "node";
constexpr std::string_view kPassNodeWithIdFieldNode = "node";
constexpr std::string_view kPassNodeWithIdFieldId = "id";

constexpr std::string_view kProcedureReset = "reset";
constexpr std::string_view kProcedureDeleteVertex = "delete_vertex";
constexpr std::string_view kProcedureDeleteEdge = "delete_edge";

constexpr std::string_view kArgument = "arg";

std::condition_variable condition;
std::mutex lock;
int turn = 0;

namespace {
void wait_turn(auto func) {
  std::unique_lock<std::mutex> guard(lock);
  condition.wait(guard, [&func] { return func(turn); });
  turn++;
  condition.notify_all();
}
}  // namespace

void Reset(mgp_list *args, mgp_graph *memgraph_graph, mgp_result * /*result*/, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  turn = 0;
}

void DeleteVertex(mgp_list *args, mgp_graph *memgraph_graph, mgp_result * /*result*/, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);

  auto vertex = arguments[0].ValueNode();
  auto graph = mgp::Graph(memgraph_graph);

  wait_turn([](int x) { return x == 1 || x == 2; });
  graph.DetachDeleteNode(vertex);
  wait_turn([](int x) { return x == 1 || x == 2; });
}

void DeleteEdge(mgp_list *args, mgp_graph *memgraph_graph, mgp_result * /*result*/, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);

  auto edge = arguments[0].ValueRelationship();
  auto graph = mgp::Graph(memgraph_graph);

  wait_turn([](int x) { return x == 1 || x == 2; });
  graph.DeleteRelationship(edge);
  wait_turn([](int x) { return x == 1 || x == 2; });
}

void PassRelationship(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  const auto arguments = mgp::List(args);
  auto result = mgp::Result(res);

  const auto relationship = arguments[0].ValueRelationship();
  wait_turn([](int x) { return x == 0 || x > 2; });

  wait_turn([](int x) { return x == 0 || x > 2; });
  result.SetValue(relationship);
}

void PassNodeWithId(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);

  const auto node = arguments[0].ValueNode();
  const auto node_id = node.Id().AsInt();
  wait_turn([](int x) { return x == 0 || x > 2; });

  wait_turn([](int x) { return x == 0 || x > 2; });
  auto record = record_factory.NewRecord();
  record.Insert(kPassNodeWithIdFieldNode.data(), node);
  record.Insert(kPassNodeWithIdFieldId.data(), node_id);
}

extern "C" int mgp_init_module(struct mgp_module *query_module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);

    mgp::AddProcedure(Reset, kProcedureReset, mgp::ProcedureType::Read, {}, {}, query_module, memory);
    mgp::AddProcedure(DeleteVertex, kProcedureDeleteVertex, mgp::ProcedureType::Write,
                      {mgp::Parameter(kArgument, mgp::Type::Node)}, {}, query_module, memory);
    mgp::AddProcedure(DeleteEdge, kProcedureDeleteEdge, mgp::ProcedureType::Write,
                      {mgp::Parameter(kArgument, mgp::Type::Relationship)}, {}, query_module, memory);

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
