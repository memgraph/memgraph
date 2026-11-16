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

#include "algorithm/create.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    AddProcedure(Create::SetRelProperty,
                 Create::kProcedureSetRelProp,
                 mgp::ProcedureType::Write,
                 {mgp::Parameter(Create::kArgumentsRelationship, mgp::Type::Any),
                  mgp::Parameter(Create::kArgumentsKey, mgp::Type::String),
                  mgp::Parameter(Create::kArgumentsValue, mgp::Type::Any)},
                 {mgp::Return(Create::kReturnRelProp, mgp::Type::Relationship)},
                 module,
                 memory);

    AddProcedure(Create::RemoveLabels,
                 Create::kProcedureRemoveLabels,
                 mgp::ProcedureType::Write,
                 {mgp::Parameter(Create::kArgumentNodesRemoveLabels, mgp::Type::Any),
                  mgp::Parameter(Create::kArgumentsLabels, {mgp::Type::List, mgp::Type::String})},
                 {mgp::Return(Create::kReturnRemoveLabels, mgp::Type::Node)},
                 module,
                 memory);

    {
      // Value list elements are nullable so a null value reaches the body (keys must be non-null strings).
      auto *proc = mgp::module_add_write_procedure(
          module, std::string(Create::kProcedureSetProperties).c_str(), Create::SetProperties);
      mgp::proc_add_arg(proc, std::string(Create::kArgumentsNodes).c_str(), mgp::type_any());
      mgp::proc_add_arg(proc, std::string(Create::kArgumentsKeys).c_str(), mgp::type_list(mgp::type_string()));
      mgp::proc_add_arg(
          proc, std::string(Create::kArgumentsValues).c_str(), mgp::type_list(mgp::type_nullable(mgp::type_any())));
      mgp::proc_add_result(proc, std::string(Create::kReturnProperties).c_str(), mgp::type_node());
    }

    AddProcedure(Create::Node,
                 Create::kProcedureNode,
                 mgp::ProcedureType::Write,
                 {mgp::Parameter(Create::kArgumentsLabelsList, {mgp::Type::List, mgp::Type::String}),
                  mgp::Parameter(Create::kArgumentsProperties, mgp::Type::Map)},
                 {mgp::Return(Create::kReturnNode, mgp::Type::Node)},
                 module,
                 memory);

    AddProcedure(Create::Nodes,
                 Create::kProcedureNodes,
                 mgp::ProcedureType::Write,
                 {mgp::Parameter(Create::kArgumentLabelsNodes, {mgp::Type::List, mgp::Type::String}),
                  mgp::Parameter(Create::kArgumentPropertiesNodes, {mgp::Type::List, mgp::Type::Map})},
                 {mgp::Return(Create::kReturnNodes, mgp::Type::Node)},
                 module,
                 memory);

    AddProcedure(Create::RemoveProperties,
                 Create::kProcedureRemoveProperties,
                 mgp::ProcedureType::Write,
                 {mgp::Parameter(Create::kArgumentNodeRemoveProperties, mgp::Type::Any),
                  mgp::Parameter(Create::kArgumentKeysRemoveProperties, {mgp::Type::List, mgp::Type::String})},
                 {mgp::Return(Create::kReturntRemoveProperties, mgp::Type::Node)},
                 module,
                 memory);

    AddProcedure(Create::SetProperty,
                 Create::kProcedureSetProperty,
                 mgp::ProcedureType::Write,
                 {mgp::Parameter(Create::kArgumentNodeSetProperty, mgp::Type::Any),
                  mgp::Parameter(Create::kArgumentKeySetProperty, mgp::Type::String),
                  mgp::Parameter(Create::kArgumentValueSetProperty, mgp::Type::Any)},
                 {mgp::Return(Create::kReturntSetProperty, mgp::Type::Node)},
                 module,
                 memory);

    AddProcedure(Create::RemoveRelProperties,
                 Create::kProcedureRemoveRelProperties,
                 mgp::ProcedureType::Write,
                 {mgp::Parameter(Create::kRemoveRelPropertiesArg1, mgp::Type::Any),
                  mgp::Parameter(Create::kRemoveRelPropertiesArg2, {mgp::Type::List, mgp::Type::String})},
                 {mgp::Return(Create::kResultRemoveRelProperties, mgp::Type::Relationship)},
                 module,
                 memory);

    {
      // Value list elements are nullable so a null value reaches the body (keys must be non-null strings).
      auto *proc = mgp::module_add_write_procedure(
          module, std::string(Create::kProcedureSetRelProperties).c_str(), Create::SetRelProperties);
      mgp::proc_add_arg(proc, std::string(Create::kSetRelPropertiesArg1).c_str(), mgp::type_any());
      mgp::proc_add_arg(proc, std::string(Create::kSetRelPropertiesArg2).c_str(), mgp::type_list(mgp::type_string()));
      mgp::proc_add_arg(proc,
                        std::string(Create::kSetRelPropertiesArg3).c_str(),
                        mgp::type_list(mgp::type_nullable(mgp::type_any())));
      mgp::proc_add_result(proc, std::string(Create::kResultSetRelProperties).c_str(), mgp::type_relationship());
    }

    AddProcedure(Create::Relationship,
                 Create::kProcedureRelationship,
                 mgp::ProcedureType::Write,
                 {mgp::Parameter(Create::kRelationshipArg1, mgp::Type::Node),
                  mgp::Parameter(Create::kRelationshipArg2, mgp::Type::String),
                  mgp::Parameter(Create::kRelationshipArg3, mgp::Type::Map),
                  mgp::Parameter(Create::kRelationshipArg4, mgp::Type::Node)},
                 {mgp::Return(Create::kResultRelationship, mgp::Type::Relationship)},
                 module,
                 memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
