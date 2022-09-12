// Copyright 2022 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <mage.hpp>
#include <mg_exceptions.hpp>

void ProcImpl(std::vector<mage::Value> arguments, mage::Graph graph, mage::RecordFactory record_factory) {
  auto record = record_factory.NewRecord();
  record.Insert("out", true);
}

void SampleReadProc(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  try {
    mage::memory = memory;

    std::vector<mage::Value> arguments;
    for (size_t i = 0; i < mgp::list_size(args); i++) {
      auto arg = mage::Value(mgp::list_at(args, i));
      arguments.push_back(arg);
    }

    ProcImpl(arguments, mage::Graph(memgraph_graph), mage::RecordFactory(result));
  } catch (const std::exception &e) {
    mgp::result_set_error_msg(result, e.what());
    return;
  }
}

void AddXNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mage::memory = memory;
  auto graph = mage::Graph(memgraph_graph);

  std::vector<mage::Value> arguments;
  for (size_t i = 0; i < mgp::list_size(args); i++) {
    auto arg = mage::Value(mgp::list_at(args, i));
    arguments.push_back(arg);
  }

  for (int i = 0; i < arguments[0].ValueInt(); i++) {
    graph.CreateNode();
  }
}

void Multiply(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory) {
  mage::memory = memory;

  std::vector<mage::Value> arguments;
  for (size_t i = 0; i < mgp::list_size(args); i++) {
    auto arg = mage::Value(mgp::list_at(args, i));
    arguments.push_back(arg);
  }

  auto result = mage::Result(res);

  auto first = arguments[0].ValueInt();
  auto second = arguments[1].ValueInt();

  result.SetValue(first * second);
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mage::memory = memory;

    AddProcedure(SampleReadProc, "return_true", mage::ProdecureType::Read,
                 {mage::Parameter("param_1", mage::Type::Int), mage::Parameter("param_2", mage::Type::Double, 2.3)},
                 {mage::Return("out", mage::Type::Bool)}, module, memory);
  } catch (const std::exception &e) {
    return 1;
  }

  try {
    mage::memory = memory;

    mage::AddProcedure(AddXNodes, "add_x_nodes", mage::ProdecureType::Write,
                       {mage::Parameter("param_1", mage::Type::Int)}, {}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  try {
    mage::memory = memory;

    mage::AddFunction(Multiply, "multiply",
                      {mage::Parameter("int", mage::Type::Int), mage::Parameter("int", mage::Type::Int, (int64_t)3)},
                      module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
