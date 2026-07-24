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

    mgp::AddFunction(Map::Flatten,
                     Map::kProcedureFlatten,
                     {mgp::Parameter(Map::kArgumentMapFlatten, {mgp::Type::Map, mgp::Type::Any}),
                      mgp::Parameter(Map::kArgumentDelimiterFlatten, mgp::Type::String, ".")},
                     module,
                     memory);

    {
      // Value list elements are nullable so null values are kept (keys must be non-null strings).
      auto *func = mgp::module_add_function(module, Map::kProcedureFromLists, Map::FromLists);
      mgp::func_add_arg(func, Map::kArgumentListKeysFromLists, mgp::type_list(mgp::type_string()));
      mgp::func_add_arg(func, Map::kArgumentListValuesFromLists, mgp::type_list(mgp::type_nullable(mgp::type_any())));
    }

    mgp::AddFunction(Map::RemoveKey,
                     Map::kProcedureRemoveKey,
                     {mgp::Parameter(Map::kArgumentsInputMap, mgp::Type::Map),
                      mgp::Parameter(Map::kArgumentsKey, mgp::Type::String),
                      mgp::Parameter(Map::kArgumentsIsRecursive, mgp::Type::Map, mgp::Value(mgp::Map()))},
                     module,
                     memory);

    mgp::AddFunction(Map::FromPairs,
                     Map::kProcedureFromPairs,
                     {mgp::Parameter(Map::kArgumentsInputList, {mgp::Type::List, mgp::Type::List})},
                     module,
                     memory);

    {
      // mgp::AddFunction(Map::Merge, Map::kProcedureMerge,
      //                  {mgp::Parameter(Map::kArgumentsInputMap1, mgp::Type::Any, mgp::Value(mgp::Map())),
      //                   mgp::Parameter(Map::kArgumentsInputMap2, mgp::Type::Any, mgp::Value(mgp::Map()))},
      //                  module, memory);

      auto *func = mgp::module_add_function(module, Map::kProcedureMerge, Map::Merge);
      mgp::func_add_arg(func, Map::kArgumentsInputMap1, mgp::type_nullable(mgp::type_map()));
      mgp::func_add_arg(func, Map::kArgumentsInputMap2, mgp::type_nullable(mgp::type_map()));
    }

    mgp::AddFunction(Map::RemoveKeys,
                     Map::kProcedureRemoveKeys,
                     {mgp::Parameter(Map::kArgumentsInputMapRemoveKeys, mgp::Type::Map),
                      mgp::Parameter(Map::kArgumentsKeysListRemoveKeys, {mgp::Type::List, mgp::Type::String}),
                      mgp::Parameter(Map::kArgumentsRecursiveRemoveKeys, mgp::Type::Map, mgp::Value(mgp::Map()))},
                     module,
                     memory);

    AddProcedure(Map::FromNodes,
                 Map::kProcedureFromNodes,
                 mgp::ProcedureType::Read,
                 {mgp::Parameter(Map::kFromNodesArg1, mgp::Type::String),
                  mgp::Parameter(Map::kFromNodesArg2, mgp::Type::String)},
                 {mgp::Return(Map::kResultFromNodes, mgp::Type::Map)},
                 module,
                 memory);

    {
      // List elements are nullable so a null key can be passed (and skipped).
      auto *func = mgp::module_add_function(module, Map::kProcedureFromValues, Map::FromValues);
      mgp::func_add_arg(func, Map::kFromValuesArg1, mgp::type_list(mgp::type_nullable(mgp::type_any())));
    }

    {
      // Nullable params so a null map is treated as empty and a null key is a no-op.
      auto *func = mgp::module_add_function(module, Map::kProcedureSetKey, Map::SetKey);
      mgp::func_add_arg(func, Map::kSetKeyArg1, mgp::type_nullable(mgp::type_map()));
      mgp::func_add_arg(func, Map::kSetKeyArg2, mgp::type_nullable(mgp::type_string()));
      mgp::func_add_arg(func, Map::kSetKeyArg3, mgp::type_nullable(mgp::type_any()));
    }

    {
      // value defaults to null and fail defaults to true.
      auto *func = mgp::module_add_function(module, Map::kProcedureGet, Map::Get);
      mgp::func_add_arg(func, Map::kArgumentMapGet, mgp::type_map());
      mgp::func_add_arg(func, Map::kArgumentKeyGet, mgp::type_string());
      const auto value_default = mgp::Value();
      mgp::func_add_opt_arg(func, Map::kArgumentValueGet, mgp::type_nullable(mgp::type_any()), value_default.ptr());
      const auto fail_default = mgp::Value(true);
      mgp::func_add_opt_arg(func, Map::kArgumentFailGet, mgp::type_bool(), fail_default.ptr());
    }

    mgp::AddFunction(Map::MergeList,
                     Map::kProcedureMergeList,
                     {mgp::Parameter(Map::kArgumentMapsMergeList, {mgp::Type::List, mgp::Type::Map})},
                     module,
                     memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
