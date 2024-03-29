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

#include <mgp.hpp>

void ProcImpl(std::vector<mgp::Value> arguments, mgp::Graph graph, mgp::RecordFactory record_factory) {
  auto record = record_factory.NewRecord();
  record.Insert("out", true);
}

void SampleReadProc(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    // The outcommented way of assigning the memory pointer is still
    // working, but it is deprecated because of certain concurrency
    // issues. Please use the guard instead.
    // mgp::memory = memory;
    mgp::MemoryDispatcherGuard guard(memory);

    std::vector<mgp::Value> arguments;
    for (size_t i = 0; i < mgp::list_size(args); i++) {
      auto arg = mgp::Value(mgp::list_at(args, i));
      arguments.push_back(arg);
    }

    ProcImpl(arguments, mgp::Graph(memgraph_graph), mgp::RecordFactory(result));
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void AddXNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  // The outcommented way of assigning the memory pointer is still
  // working, but it is deprecated because of certain concurrency
  // issues. Please use the guard instead.
  // mgp::memory = memory;
  mgp::MemoryDispatcherGuard guard(memory);
  auto graph = mgp::Graph(memgraph_graph);

  std::vector<mgp::Value> arguments;
  for (size_t i = 0; i < mgp::list_size(args); i++) {
    auto arg = mgp::Value(mgp::list_at(args, i));
    arguments.push_back(arg);
  }

  for (int i = 0; i < arguments[0].ValueInt(); i++) {
    graph.CreateNode();
  }
}

void Multiply(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  // The outcommented way of assigning the memory pointer is still
  // working, but it is deprecated because of certain concurrency
  // issues. Please use the guard instead.
  // mgp::memory = memory;
  mgp::MemoryDispatcherGuard guard(memory);

  std::vector<mgp::Value> arguments;
  for (size_t i = 0; i < mgp::list_size(args); i++) {
    auto arg = mgp::Value(mgp::list_at(args, i));
    arguments.push_back(arg);
  }

  auto result = mgp::Result(res);

  auto first = arguments[0].ValueInt();
  auto second = arguments[1].ValueInt();

  result.SetValue(first * second);
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    // The outcommented way of assigning the memory pointer is still
    // working, but it is deprecated because of certain concurrency
    // issues. Please use the guard instead.
    // mgp::memory = memory;
    mgp::MemoryDispatcherGuard guard(memory);

    AddProcedure(SampleReadProc, "return_true", mgp::ProcedureType::Read,
                 {mgp::Parameter("param_1", mgp::Type::Int), mgp::Parameter("param_2", mgp::Type::Double, 2.3)},
                 {mgp::Return("out", mgp::Type::Bool)}, module, memory);
  } catch (const std::exception &e) {
    return 1;
  }

  try {
    // The outcommented way of assigning the memory pointer is still
    // working, but it is deprecated because of certain concurrency
    // issues. Please use the guard instead.
    // mgp::memory = memory;
    mgp::MemoryDispatcherGuard guard(memory);

    mgp::AddProcedure(AddXNodes, "add_x_nodes", mgp::ProcedureType::Write, {mgp::Parameter("param_1", mgp::Type::Int)},
                      {}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  try {
    // The outcommented way of assigning the memory pointer is still
    // working, but it is deprecated because of certain concurrency
    // issues. Please use the guard instead.
    // mgp::memory = memory;
    mgp::MemoryDispatcherGuard guard(memory);

    mgp::AddFunction(Multiply, "multiply",
                     {mgp::Parameter("int", mgp::Type::Int), mgp::Parameter("int", mgp::Type::Int, (int64_t)3)}, module,
                     memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
