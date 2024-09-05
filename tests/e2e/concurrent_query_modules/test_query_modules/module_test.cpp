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

#include <chrono>
#include <cmath>
#include <list>
#include <thread>

constexpr char const *kProcedure = "node";
constexpr char const *kArgument = "node";
constexpr char const *kReturn = "node";

void Procedure(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto &arguments = mgp::List(args);
  const auto record_factory = mgp::RecordFactory(result);
  try {
    const auto node{arguments[0].ValueNode()};
    auto record = record_factory.NewRecord();
    record.Insert(kReturn, node);
  } catch (const std::exception &e) {
    record_factory.SetErrorMessage(e.what());
    return;
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard(memory);
    AddProcedure(Procedure, kProcedure, mgp::ProcedureType::Read, {mgp::Parameter(kArgument, mgp::Type::Node)},
                 {mgp::Return(kReturn, mgp::Type::Node)}, module, memory);
  } catch (const std::exception &e) {
    return 1;
  }
  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
