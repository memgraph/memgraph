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

#include <mgp.hpp>

#include "algorithm/algo.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    AddProcedure(Algo::AStar, std::string(Algo::kProcedureAStar).c_str(), mgp::ProcedureType::Read,
                 {mgp::Parameter(std::string(Algo::kAStarStart).c_str(), mgp::Type::Node),
                  mgp::Parameter(std::string(Algo::kAStarTarget).c_str(), mgp::Type::Node),
                  mgp::Parameter(std::string(Algo::kAStarConfig).c_str(), mgp::Type::Map)},
                 {mgp::Return(std::string(Algo::kAStarPath).c_str(), mgp::Type::Path),
                  mgp::Return(std::string(Algo::kAStarWeight).c_str(), mgp::Type::Double)},
                 module, memory);
    AddProcedure(Algo::Cover, std::string(Algo::kProcedureCover).c_str(), mgp::ProcedureType::Read,
                 {mgp::Parameter(std::string(Algo::kCoverArg1).c_str(), {mgp::Type::List, mgp::Type::Node})},
                 {mgp::Return(std::string(Algo::kCoverRet1).c_str(), mgp::Type::Relationship)}, module, memory);

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
