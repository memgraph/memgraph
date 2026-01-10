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

#include "algorithm/path.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    mgp::AddFunction(Path::Elements, Path::kProcedureElements, {mgp::Parameter(Path::kElementsArg1, mgp::Type::Path)},
                     module, memory);

    mgp::AddFunction(
        Path::Combine, Path::kProcedureCombine,
        {mgp::Parameter(Path::kCombineArg1, mgp::Type::Path), mgp::Parameter(Path::kCombineArg2, mgp::Type::Path)},
        module, memory);

    mgp::AddFunction(Path::Slice, Path::kProcedureSlice,
                     {mgp::Parameter(Path::kSliceArg1, mgp::Type::Path),
                      mgp::Parameter(Path::kSliceArg2, mgp::Type::Int, static_cast<int64_t>(0)),
                      mgp::Parameter(Path::kSliceArg3, mgp::Type::Int, static_cast<int64_t>(-1))},
                     module, memory);

    AddProcedure(Path::Expand, std::string(Path::kProcedureExpand), mgp::ProcedureType::Read,
                 {mgp::Parameter(std::string(Path::kArgumentStartExpand), mgp::Type::Any),
                  mgp::Parameter(std::string(Path::kArgumentRelationshipsExpand), {mgp::Type::List, mgp::Type::String}),
                  mgp::Parameter(std::string(Path::kArgumentLabelsExpand), {mgp::Type::List, mgp::Type::String}),
                  mgp::Parameter(std::string(Path::kArgumentMinHopsExpand), mgp::Type::Int),
                  mgp::Parameter(std::string(Path::kArgumentMaxHopsExpand), mgp::Type::Int)},
                 {mgp::Return(std::string(Path::kResultExpand), mgp::Type::Path)}, module, memory);

    auto empty_list = mgp::Value(mgp::List{});
    auto empty_map = mgp::Map{};
    empty_map.Insert("key", empty_list);

    AddProcedure(Path::Create, Path::kProcedureCreate, mgp::ProcedureType::Read,
                 {mgp::Parameter(Path::kCreateArg1, mgp::Type::Node),
                  mgp::Parameter(Path::kCreateArg2, {mgp::Type::Map, mgp::Type::List}, mgp::Value(empty_map))},
                 {mgp::Return(Path::kResultCreate, mgp::Type::Path)}, module, memory);

    AddProcedure(
        Path::SubgraphNodes, Path::kProcedureSubgraphNodes, mgp::ProcedureType::Read,
        {mgp::Parameter(Path::kArgumentsStart, mgp::Type::Any), mgp::Parameter(Path::kArgumentsConfig, mgp::Type::Map)},
        {mgp::Return(Path::kReturnSubgraphNodes, mgp::Type::Node)}, module, memory);

    AddProcedure(
        Path::SubgraphAll, Path::kProcedureSubgraphAll, mgp::ProcedureType::Read,
        {mgp::Parameter(Path::kArgumentsStart, mgp::Type::Any), mgp::Parameter(Path::kArgumentsConfig, mgp::Type::Map)},
        {mgp::Return(Path::kReturnNodesSubgraphAll, {mgp::Type::List, mgp::Type::Node}),
         mgp::Return(Path::kReturnRelsSubgraphAll, {mgp::Type::List, mgp::Type::Relationship})},
        module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
