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

#include "algorithm/algo.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    AddProcedure(
        Algo::AStar, Algo::kProcedureAStar, mgp::ProcedureType::Read,
        {mgp::Parameter(Algo::kAStarStart, mgp::Type::Node), mgp::Parameter(Algo::kAStarTarget, mgp::Type::Node),
         mgp::Parameter(Algo::kAStarConfig, mgp::Type::Map)},
        {mgp::Return(Algo::kAStarPath, mgp::Type::Path), mgp::Return(Algo::kAStarWeight, mgp::Type::Double)}, module,
        memory);
    AddProcedure(Algo::Cover, Algo::kProcedureCover, mgp::ProcedureType::Read,
                 {mgp::Parameter(Algo::kCoverArg1, {mgp::Type::List, mgp::Type::Node})},
                 {mgp::Return(Algo::kCoverRet1, mgp::Type::Relationship)}, module, memory);

    AddProcedure(Algo::AllSimplePaths, Algo::kProcedureAllSimplePaths, mgp::ProcedureType::Read,
                 {mgp::Parameter(Algo::kAllSimplePathsArg1, mgp::Type::Node),
                  mgp::Parameter(Algo::kAllSimplePathsArg2, mgp::Type::Node),
                  mgp::Parameter(Algo::kAllSimplePathsArg3, {mgp::Type::List, mgp::Type::String}),
                  mgp::Parameter(Algo::kAllSimplePathsArg4, mgp::Type::Int)},
                 {mgp::Return(Algo::kResultAllSimplePaths, mgp::Type::Path)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
