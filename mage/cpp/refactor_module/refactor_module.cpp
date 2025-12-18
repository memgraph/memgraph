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

#include "algorithm/refactor.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    mgp::MemoryDispatcherGuard guard{memory};

    AddProcedure(Refactor::From, Refactor::kProcedureFrom, mgp::ProcedureType::Write,
                 {mgp::Parameter(Refactor::kFromArg1, mgp::Type::Relationship),
                  mgp::Parameter(Refactor::kFromArg2, mgp::Type::Node)},
                 {mgp::Return(Refactor::kFromResult, mgp::Type::Relationship)}, module, memory);

    AddProcedure(Refactor::To, Refactor::kProcedureTo, mgp::ProcedureType::Write,
                 {mgp::Parameter(Refactor::kToArg1, mgp::Type::Relationship),
                  mgp::Parameter(Refactor::kToArg2, mgp::Type::Node)},
                 {mgp::Return(Refactor::kToResult, mgp::Type::Relationship)}, module, memory);

    AddProcedure(Refactor::Categorize, Refactor::kProcedureCategorize, mgp::ProcedureType::Write,
                 {
                     mgp::Parameter(Refactor::kArgumentsCatSourceKey, mgp::Type::String),
                     mgp::Parameter(Refactor::kArgumentsCatRelType, mgp::Type::String),
                     mgp::Parameter(Refactor::kArgumentsCatRelOutgoing, mgp::Type::Bool),
                     mgp::Parameter(Refactor::kArgumentsCatLabelName, mgp::Type::String),
                     mgp::Parameter(Refactor::kArgumentsCatPropKey, mgp::Type::String),
                     mgp::Parameter(Refactor::kArgumentsCopyPropKeys, {mgp::Type::List, mgp::Type::String},
                                    mgp::Value(mgp::List{})),
                 },
                 {mgp::Return(Refactor::kReturnCategorize, mgp::Type::String)}, module, memory);

    AddProcedure(Refactor::CloneNodes, Refactor::kProcedureCloneNodes, mgp::ProcedureType::Write,
                 {mgp::Parameter(Refactor::kArgumentsNodesToClone, {mgp::Type::List, mgp::Type::Node}),
                  mgp::Parameter(Refactor::kArgumentsCloneRels, mgp::Type::Bool, false),
                  mgp::Parameter(Refactor::kArgumentsSkipPropClone, {mgp::Type::List, mgp::Type::String},
                                 mgp::Value(mgp::List{}))},
                 {mgp::Return(Refactor::kResultClonedNodeId, mgp::Type::Int),
                  mgp::Return(Refactor::kResultNewNode, mgp::Type::Node),
                  mgp::Return(Refactor::kResultCloneNodeError, mgp::Type::String)},
                 module, memory);

    AddProcedure(
        Refactor::CloneSubgraphFromPaths, Refactor::kProcedureCSFP, mgp::ProcedureType::Write,
        {mgp::Parameter(Refactor::kArgumentsPath, {mgp::Type::List, mgp::Type::Path}),
         mgp::Parameter(Refactor::kArgumentsConfigMap, {mgp::Type::Map, mgp::Type::Any}, mgp::Value(mgp::Map{}))},
        {mgp::Return(Refactor::kResultClonedNodeId, mgp::Type::Int),
         mgp::Return(Refactor::kResultNewNode, mgp::Type::Node),
         mgp::Return(Refactor::kResultCloneNodeError, mgp::Type::String)},
        module, memory);

    AddProcedure(
        Refactor::CloneSubgraph, Refactor::kProcedureCloneSubgraph, mgp::ProcedureType::Write,
        {mgp::Parameter(Refactor::kArgumentsNodes, {mgp::Type::List, mgp::Type::Node}),
         mgp::Parameter(Refactor::kArgumentsRels, {mgp::Type::List, mgp::Type::Relationship}, mgp::Value(mgp::List())),
         mgp::Parameter(Refactor::kArgumentsConfigMap, {mgp::Type::Map, mgp::Type::Any}, mgp::Value(mgp::Map{}))},
        {mgp::Return(Refactor::kResultClonedNodeId, mgp::Type::Int),
         mgp::Return(Refactor::kResultNewNode, mgp::Type::Node),
         mgp::Return(Refactor::kResultCloneNodeError, mgp::Type::String)},
        module, memory);

    AddProcedure(Refactor::RenameLabel, Refactor::kProcedureRenameLabel, mgp::ProcedureType::Write,
                 {mgp::Parameter(Refactor::kRenameLabelArg1, mgp::Type::String),
                  mgp::Parameter(Refactor::kRenameLabelArg2, mgp::Type::String),
                  mgp::Parameter(Refactor::kRenameLabelArg3, {mgp::Type::List, mgp::Type::Node})},
                 {mgp::Return(Refactor::kRenameLabelResult, mgp::Type::Int)}, module, memory);

    AddProcedure(Refactor::RenameNodeProperty, Refactor::kProcedureRenameNodeProperty, mgp::ProcedureType::Write,
                 {mgp::Parameter(Refactor::kRenameNodePropertyArg1, mgp::Type::String),
                  mgp::Parameter(Refactor::kRenameNodePropertyArg2, mgp::Type::String),
                  mgp::Parameter(Refactor::kRenameNodePropertyArg3, {mgp::Type::List, mgp::Type::Node})},
                 {mgp::Return(Refactor::kRenameNodePropertyResult, mgp::Type::Int)}, module, memory);

    AddProcedure(Refactor::Invert, std::string(Refactor::kProcedureInvert).c_str(), mgp::ProcedureType::Write,
                 {mgp::Parameter(std::string(Refactor::kArgumentRelationship).c_str(), mgp::Type::Any)},
                 {mgp::Return(std::string(Refactor::kResultIdInvert).c_str(), mgp::Type::Int),
                  mgp::Return(std::string(Refactor::kResultRelationshipInvert).c_str(), mgp::Type::Relationship),
                  mgp::Return(std::string(Refactor::kResultErrorInvert).c_str(), mgp::Type::String)},
                 module, memory);

    AddProcedure(Refactor::CollapseNode, std::string(Refactor::kProcedureCollapseNode).c_str(),
                 mgp::ProcedureType::Write,
                 {mgp::Parameter(std::string(Refactor::kArgumentNodesCollapseNode).c_str(), mgp::Type::Any),
                  mgp::Parameter(std::string(Refactor::kArgumentTypeCollapseNode).c_str(), mgp::Type::String)},
                 {mgp::Return(std::string(Refactor::kReturnIdCollapseNode).c_str(), mgp::Type::Int),
                  mgp::Return(std::string(Refactor::kReturnRelationshipCollapseNode).c_str(), mgp::Type::Relationship)},
                 module, memory);

    AddProcedure(Refactor::DeleteAndReconnect, Refactor::kProcedureDeleteAndReconnect, mgp::ProcedureType::Write,
                 {mgp::Parameter(Refactor::kDeleteAndReconnectArg1, mgp::Type::Path),
                  mgp::Parameter(Refactor::kDeleteAndReconnectArg2, {mgp::Type::List, mgp::Type::Node}),
                  mgp::Parameter(Refactor::kDeleteAndReconnectArg3, mgp::Type::Map, mgp::Value(mgp::Map()))},
                 {mgp::Return(Refactor::kReturnDeleteAndReconnect1, {mgp::Type::List, mgp::Type::Node}),
                  mgp::Return(Refactor::kReturnDeleteAndReconnect2, {mgp::Type::List, mgp::Type::Relationship})},
                 module, memory);

    AddProcedure(Refactor::ExtractNode, Refactor::kProcedureExtractNode, mgp::ProcedureType::Write,
                 {mgp::Parameter(Refactor::kExtractNodeArg1, mgp::Type::Any),
                  mgp::Parameter(Refactor::kExtractNodeArg2, {mgp::Type::List, mgp::Type::String}),
                  mgp::Parameter(Refactor::kExtractNodeArg3, mgp::Type::String),
                  mgp::Parameter(Refactor::kExtractNodeArg4, mgp::Type::String)},
                 {mgp::Return(Refactor::kResultExtractNode1, mgp::Type::Int),
                  mgp::Return(Refactor::kResultExtractNode2, mgp::Type::Node),
                  mgp::Return(Refactor::kResultExtractNode3, mgp::Type::String)},
                 module, memory);

    AddProcedure(Refactor::NormalizeAsBoolean, Refactor::kProcedureNormalizeAsBoolean, mgp::ProcedureType::Write,
                 {mgp::Parameter(Refactor::kNormalizeAsBooleanArg1, mgp::Type::Any),
                  mgp::Parameter(Refactor::kNormalizeAsBooleanArg2, mgp::Type::String),
                  mgp::Parameter(Refactor::kNormalizeAsBooleanArg3, {mgp::Type::List, mgp::Type::Any}),
                  mgp::Parameter(Refactor::kNormalizeAsBooleanArg4, {mgp::Type::List, mgp::Type::Any})},
                 {}, module, memory);

    AddProcedure(Refactor::RenameType, Refactor::kProcedureRenameType, mgp::ProcedureType::Write,
                 {mgp::Parameter(Refactor::kRenameTypeArg1, mgp::Type::String),
                  mgp::Parameter(Refactor::kRenameTypeArg2, mgp::Type::String),
                  mgp::Parameter(Refactor::kRenameTypeArg3, {mgp::Type::List, mgp::Type::Relationship})},
                 {mgp::Return(Refactor::kResultRenameType, mgp::Type::Int)}, module, memory);

    AddProcedure(Refactor::RenameTypeProperty, std::string(Refactor::kProcedureRenameTypeProperty).c_str(),
                 mgp::ProcedureType::Write,
                 {mgp::Parameter(std::string(Refactor::kRenameTypePropertyArg1).c_str(), mgp::Type::String),
                  mgp::Parameter(std::string(Refactor::kRenameTypePropertyArg2).c_str(), mgp::Type::String),
                  mgp::Parameter(std::string(Refactor::kRenameTypePropertyArg3).c_str(),
                                 {mgp::Type::List, mgp::Type::Relationship})},
                 {mgp::Return(std::string(Refactor::kRenameTypePropertyResult).c_str(), mgp::Type::Int)}, module,
                 memory);

    AddProcedure(Refactor::MergeNodes, Refactor::kProcedureMergeNodes, mgp::ProcedureType::Write,
                 {mgp::Parameter(Refactor::kMergeNodesArgNodes, {mgp::Type::List, mgp::Type::Node}),
                  mgp::Parameter(Refactor::kMergeNodesArgConfig, mgp::Type::Map, mgp::Value(mgp::Map{}))},
                 {mgp::Return(Refactor::kMergeNodesResult, mgp::Type::Node)}, module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
