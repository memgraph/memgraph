// Copyright 2026 Memgraph Ltd.
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

#include "algorithm/neighbors.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};
    AddProcedure(Neighbors::AtHop, Neighbors::kProcedureAtHop, mgp::ProcedureType::Read,
                 {mgp::Parameter(Neighbors::kArgumentsNode, mgp::Type::Node),
                  mgp::Parameter(Neighbors::kArgumentsRelType, {mgp::Type::List, mgp::Type::String}),
                  mgp::Parameter(Neighbors::kArgumentsDistance, mgp::Type::Int)},
                 {mgp::Return(Neighbors::kReturnAtHop, mgp::Type::Node)}, module, memory);

    AddProcedure(Neighbors::ByHop, Neighbors::kProcedureByHop, mgp::ProcedureType::Read,
                 {mgp::Parameter(Neighbors::kArgumentsNode, mgp::Type::Node),
                  mgp::Parameter(Neighbors::kArgumentsRelType, {mgp::Type::List, mgp::Type::String}),
                  mgp::Parameter(Neighbors::kArgumentsDistance, mgp::Type::Int)},
                 {mgp::Return(Neighbors::kReturnByHop, {mgp::Type::List, mgp::Type::Node})}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
