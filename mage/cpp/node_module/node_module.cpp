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

#include "algorithm/node.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};
    AddProcedure(Node::RelationshipsExist, std::string(Node::kProcedureRelationshipsExist).c_str(),
                 mgp::ProcedureType::Read,
                 {mgp::Parameter(std::string(Node::kArgumentNodesRelationshipsExist).c_str(), mgp::Type::Node),
                  mgp::Parameter(std::string(Node::kArgumentRelationshipsRelationshipsExist).c_str(),
                                 {mgp::Type::List, mgp::Type::String})},
                 {mgp::Return(std::string(Node::kReturnRelationshipsExist).c_str(), {mgp::Type::Map, mgp::Type::Any})},
                 module, memory);

    AddProcedure(
        Node::RelationshipExists, Node::kProcedureRelationshipExists, mgp::ProcedureType::Read,
        {mgp::Parameter(Node::kArgumentsNode, mgp::Type::Node),
         mgp::Parameter(Node::kArgumentsPattern, {mgp::Type::List, mgp::Type::String}, mgp::Value(mgp::List{}))},
        {mgp::Return(Node::kReturnRelationshipExists, mgp::Type::Bool)}, module, memory);

    AddProcedure(
        Node::RelationshipTypes, Node::kProcedureRelationshipTypes, mgp::ProcedureType::Read,
        {mgp::Parameter(Node::kRelationshipTypesArg1, mgp::Type::Node),
         mgp::Parameter(Node::kRelationshipTypesArg2, {mgp::Type::List, mgp::Type::String}, mgp::Value(mgp::List{}))},
        {mgp::Return(Node::kResultRelationshipTypes, {mgp::Type::List, mgp::Type::String})}, module, memory);

    mgp::AddFunction(Node::DegreeIn, Node::kFunctionDegreeIn,
                     {mgp::Parameter(Node::kDegreeInArg1, mgp::Type::Node),
                      mgp::Parameter(Node::kDegreeInArg2, mgp::Type::String, "")},
                     module, memory);

    mgp::AddFunction(Node::DegreeOut, Node::kFunctionDegreeOut,
                     {mgp::Parameter(Node::kDegreeOutArg1, mgp::Type::Node),
                      mgp::Parameter(Node::kDegreeOutArg2, mgp::Type::String, "")},
                     module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
