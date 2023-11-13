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

constexpr std::string_view ProcedureCreateNodes = "create_nodes";

void CreateNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory) {
  mgp::MemoryDispatcherGuard guard(memory);
  const auto arguments = mgp::List(args);
  try {
    auto graph = mgp::Graph{memgraph_graph};
    for (int i = 0; i < 1000; ++i) {
      graph.CreateNode();
    }
  } catch (const std::exception &e) {
    return;
  }
}

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  {
    try {
      mgp::MemoryDispatcherGuard guard(memory);

      mgp::AddProcedure(CreateNodes, ProcedureCreateNodes, mgp::ProcedureType::Write, {}, {}, module, memory);

    } catch (const std::exception &e) {
      return 1;
    }
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
