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

#include "mg_procedure.h"

#include "mgp.hpp"

namespace {

constexpr std::string_view ProcedureFrom = "set_from";
constexpr std::string_view ProcedureTo = "set_to";

void SetFrom(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  try {
    auto rel = arguments[0].ValueRelationship();
    auto new_from = arguments[0].ValueNode();
    mgp::Graph graph{memgraph_graph};
    graph.SetFrom(rel, new_from);
  } catch (const std::exception &e) {
    return;
  }
}

void SetTo(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  try {
    auto rel = arguments[0].ValueRelationship();
    auto new_to = arguments[0].ValueNode();
    mgp::Graph graph{memgraph_graph};
    graph.SetTo(rel, new_to);
  } catch (const std::exception &e) {
    return;
  }
}

}  // namespace

// Each module needs to define mgp_init_module function.
// Here you can register multiple functions/procedures your module supports.
extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  {
    try {
      mgp::MemoryDispatcherGuard guard(memory);

      mgp::AddProcedure(
          SetFrom, ProcedureFrom, mgp::ProcedureType::Write,
          {mgp::Parameter("relationship", mgp::Type::Relationship), mgp::Parameter("node_from", mgp::Type::Node)}, {},
          module, memory);
      mgp::AddProcedure(
          SetTo, ProcedureTo, mgp::ProcedureType::Write,
          {mgp::Parameter("relationship", mgp::Type::Relationship), mgp::Parameter("node_to", mgp::Type::Node)}, {},
          module, memory);

    } catch (const std::exception &e) {
      return 1;
    }
  }

  return 0;
}

// This is an optional function if you need to release any resources before the
// module is unloaded. You will probably need this if you acquired some
// resources in mgp_init_module.
extern "C" int mgp_shutdown_module() { return 0; }
