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

#include "_mgp.hpp"
#include "algorithm/map.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    mgp::AddFunction(Map::Flatten, std::string(Map::kProcedureFlatten),
                     {mgp::Parameter(std::string(Map::kArgumentMapFlatten), {mgp::Type::Map, mgp::Type::Any}),
                      mgp::Parameter(std::string(Map::kArgumentDelimiterFlatten), mgp::Type::String, ".")},
                     module, memory);

    mgp::AddFunction(
        Map::FromLists, std::string(Map::kProcedureFromLists),
        {mgp::Parameter(std::string(Map::kArgumentListKeysFromLists), {mgp::Type::List, mgp::Type::String}),
         mgp::Parameter(std::string(Map::kArgumentListValuesFromLists), {mgp::Type::List, mgp::Type::Any})},
        module, memory);

    mgp::AddFunction(
        Map::RemoveKey, Map::kProcedureRemoveKey,
        {mgp::Parameter(Map::kArgumentsInputMap, mgp::Type::Map), mgp::Parameter(Map::kArgumentsKey, mgp::Type::String),
         mgp::Parameter(Map::kArgumentsIsRecursive, mgp::Type::Map, mgp::Value(mgp::Map()))},
        module, memory);

    mgp::AddFunction(Map::FromPairs, Map::kProcedureFromPairs,
                     {mgp::Parameter(Map::kArgumentsInputList, {mgp::Type::List, mgp::Type::List})}, module, memory);

    {
      // mgp::AddFunction(Map::Merge, Map::kProcedureMerge,
      //                  {mgp::Parameter(Map::kArgumentsInputMap1, mgp::Type::Any, mgp::Value(mgp::Map())),
      //                   mgp::Parameter(Map::kArgumentsInputMap2, mgp::Type::Any, mgp::Value(mgp::Map()))},
      //                  module, memory);

      auto *func = mgp::module_add_function(module, std::string(Map::kProcedureMerge).c_str(), Map::Merge);
      mgp::func_add_arg(func, std::string(Map::kArgumentsInputMap1).c_str(), mgp::type_nullable(mgp::type_map()));
      mgp::func_add_arg(func, std::string(Map::kArgumentsInputMap2).c_str(), mgp::type_nullable(mgp::type_map()));
    }

    mgp::AddFunction(
        Map::RemoveKeys, std::string(Map::kProcedureRemoveKeys),
        {mgp::Parameter(std::string(Map::kArgumentsInputMapRemoveKeys), mgp::Type::Map),
         mgp::Parameter(std::string(Map::kArgumentsKeysListRemoveKeys), {mgp::Type::List, mgp::Type::String}),
         mgp::Parameter(std::string(Map::kArgumentsRecursiveRemoveKeys), mgp::Type::Map, mgp::Value(mgp::Map()))},
        module, memory);

    AddProcedure(Map::FromNodes, Map::kProcedureFromNodes, mgp::ProcedureType::Read,
                 {mgp::Parameter(Map::kFromNodesArg1, mgp::Type::String),
                  mgp::Parameter(Map::kFromNodesArg2, mgp::Type::String)},
                 {mgp::Return(Map::kResultFromNodes, mgp::Type::Map)}, module, memory);

    mgp::AddFunction(Map::FromValues, Map::kProcedureFromValues,
                     {mgp::Parameter(Map::kFromValuesArg1, {mgp::Type::List, mgp::Type::Any})}, module, memory);

    mgp::AddFunction(
        Map::SetKey, Map::kProcedureSetKey,
        {mgp::Parameter(Map::kSetKeyArg1, mgp::Type::Map), mgp::Parameter(Map::kSetKeyArg2, mgp::Type::String),
         mgp::Parameter(Map::kSetKeyArg3, mgp::Type::Any)},
        module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
