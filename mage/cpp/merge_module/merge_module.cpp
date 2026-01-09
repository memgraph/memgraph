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

#include "algorithm/merge.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};
    AddProcedure(Merge::Node, std::string(Merge::kProcedureNode), mgp::ProcedureType::Write,
                 {mgp::Parameter(std::string(Merge::kNodeArg1), {mgp::Type::List, mgp::Type::String}),
                  mgp::Parameter(std::string(Merge::kNodeArg2), {mgp::Type::Map, mgp::Type::Any}),
                  mgp::Parameter(std::string(Merge::kNodeArg3), {mgp::Type::Map, mgp::Type::Any}),
                  mgp::Parameter(std::string(Merge::kNodeArg4), {mgp::Type::Map, mgp::Type::Any})},
                 {mgp::Return(std::string(Merge::kNodeRes), mgp::Type::Node)}, module, memory);

    AddProcedure(Merge::Relationship, Merge::kProcedureRelationship, mgp::ProcedureType::Write,
                 {mgp::Parameter(Merge::kRelationshipArg1, mgp::Type::Node),
                  mgp::Parameter(Merge::kRelationshipArg2, mgp::Type::String),
                  mgp::Parameter(Merge::kRelationshipArg3, mgp::Type::Map),
                  mgp::Parameter(Merge::kRelationshipArg4, mgp::Type::Map),
                  mgp::Parameter(Merge::kRelationshipArg5, mgp::Type::Node),
                  mgp::Parameter(Merge::kRelationshipArg6, mgp::Type::Map)},
                 {mgp::Return(Merge::kRelationshipResult, mgp::Type::Relationship)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
