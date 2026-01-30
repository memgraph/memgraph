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

#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <mutex>

#include "_mgp.hpp"
#include "mg_procedure.h"
#include "mgp.hpp"

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

namespace {

enum class State : uint8_t {
  BEGIN = 0,
  READER_READY = 1,
  WRITER_READY = 2,
  AT_LEAST_ONE_WRITE_DONE = 3,
} global_state = State::BEGIN;

void UpdateGlobalState() {
  switch (global_state) {
    case State::BEGIN:
      global_state = State::READER_READY;
      break;
    case State::READER_READY:
      global_state = State::WRITER_READY;
      break;
    case State::WRITER_READY:
      global_state = State::AT_LEAST_ONE_WRITE_DONE;
      break;
    case State::AT_LEAST_ONE_WRITE_DONE:
      break;
  }
}

void wait_turn(auto check_expected) {
  std::unique_lock<std::mutex> guard(lock);
  condition.wait(guard, [&check_expected] { return std::invoke(check_expected); });
  UpdateGlobalState();
  condition.notify_all();
}
}  // namespace

void Reset(mgp_list * /*args*/, mgp_graph * /*graph*/, mgp_result * /*result*/, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  global_state = State::BEGIN;
}

void DeleteVertex(mgp_list *args, mgp_graph *graph, mgp_result * /*result*/, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  mgp_vertex *vertex;
  {
    auto error = mgp_value_get_vertex(mgp::list_at(args, 0), &vertex);
    assert(error == mgp_error::MGP_ERROR_NO_ERROR);
  }
  {
    wait_turn([]() { return global_state == State::READER_READY; });
    auto error = mgp_graph_detach_delete_vertex(graph, vertex);
    assert(error == mgp_error::MGP_ERROR_NO_ERROR);
  }
  wait_turn([]() { return global_state == State::WRITER_READY; });
}

void DeleteEdge(mgp_list *args, mgp_graph *graph, mgp_result * /*result*/, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  mgp_edge *edge;
  {
    auto error = mgp_value_get_edge(mgp::list_at(args, 0), &edge);
    assert(error == mgp_error::MGP_ERROR_NO_ERROR);
  }
  {
    wait_turn([]() { return global_state == State::READER_READY; });
    auto error = mgp_graph_delete_edge(graph, edge);
    assert(error == mgp_error::MGP_ERROR_NO_ERROR);
  }
  wait_turn([]() { return global_state == State::WRITER_READY; });
}

void PassRelationship(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  auto *relationship = mgp::list_at(args, 0);

  auto check = []() { return global_state == State::BEGIN || global_state == State::AT_LEAST_ONE_WRITE_DONE; };
  wait_turn(check);
  wait_turn([]() { return global_state == State::AT_LEAST_ONE_WRITE_DONE; });
  mgp::func_result_set_value(res, relationship, memory);
}

void PassNodeWithId(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  auto *node = mgp::value_get_vertex(mgp::list_at(args, 0));
  auto node_id = mgp::vertex_get_id(node).as_int;

  auto check = []() { return global_state == State::BEGIN || global_state == State::AT_LEAST_ONE_WRITE_DONE; };
  wait_turn(check);
  wait_turn([]() { return global_state == State::AT_LEAST_ONE_WRITE_DONE; });

  auto *result_record = mgp::result_new_record(result);
  mgp::result_record_insert(result_record, kPassNodeWithIdFieldNode.data(), mgp::value_make_vertex(node));
  mgp::result_record_insert(result_record, kPassNodeWithIdFieldId.data(), mgp::value_make_int(node_id, memory));
}

extern "C" int mgp_init_module(struct mgp_module *query_module, struct mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard{memory};
  try {
    { [[maybe_unused]] auto *proc = mgp::module_add_write_procedure(query_module, kProcedureReset.data(), Reset); }
    {
      auto *proc = mgp::module_add_write_procedure(query_module, kProcedureDeleteVertex.data(), DeleteVertex);
      mgp::proc_add_arg(proc, kArgument.data(), mgp::type_node());
    }
    {
      auto *proc = mgp::module_add_write_procedure(query_module, kProcedureDeleteEdge.data(), DeleteEdge);
      mgp::proc_add_arg(proc, kArgument.data(), mgp::type_relationship());
    }
    {
      auto *func = mgp::module_add_function(query_module, kFunctionPassRelationship.data(), PassRelationship);
      mgp::func_add_arg(func, kPassRelationshipArg.data(), mgp::type_relationship());
    }
    {
      auto *proc = mgp::module_add_read_procedure(query_module, kProcedurePassNodeWithId.data(), PassNodeWithId);
      mgp::proc_add_arg(proc, kPassNodeWithIdArg.data(), mgp::type_node());
      mgp::proc_add_result(proc, kPassNodeWithIdFieldNode.data(), mgp::type_node());
      mgp::proc_add_result(proc, kPassNodeWithIdFieldId.data(), mgp::type_int());
    }
  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
