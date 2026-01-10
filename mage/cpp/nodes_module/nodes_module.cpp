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

#include <cstddef>
#include <mgp.hpp>

#include "algorithm/nodes.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    AddProcedure(Nodes::Link, std::string(Nodes::kProcedureLink), mgp::ProcedureType::Write,
                 {mgp::Parameter(std::string(Nodes::kArgumentNodesLink), {mgp::Type::List, mgp::Type::Node}),
                  mgp::Parameter(std::string(Nodes::kArgumentTypeLink), mgp::Type::String)},
                 {}, module, memory);

    AddProcedure(
        Nodes::RelationshipTypes, Nodes::kProcedureRelationshipTypes, mgp::ProcedureType::Read,
        {mgp::Parameter(Nodes::kRelationshipTypesArg1, mgp::Type::Any),
         mgp::Parameter(Nodes::kRelationshipTypesArg2, {mgp::Type::List, mgp::Type::String}, mgp::Value(mgp::List{}))},
        {mgp::Return(Nodes::kResultRelationshipTypes, {mgp::Type::List, mgp::Type::Map})}, module, memory);

    AddProcedure(Nodes::Delete, Nodes::kProcedureDelete, mgp::ProcedureType::Write,
                 {mgp::Parameter(Nodes::kDeleteArg1, mgp::Type::Any)}, {}, module, memory);

    AddProcedure(
        Nodes::RelationshipsExist, std::string(Nodes::kProcedureRelationshipsExist), mgp::ProcedureType::Read,
        {mgp::Parameter(std::string(Nodes::kArgumentNodesRelationshipsExist), {mgp::Type::List, mgp::Type::Any}),
         mgp::Parameter(std::string(Nodes::kArgumentRelationshipsRelationshipsExist),
                        {mgp::Type::List, mgp::Type::String})},
        {mgp::Return(std::string(Nodes::kReturnRelationshipsExist), {mgp::Type::Map, mgp::Type::Any})}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
