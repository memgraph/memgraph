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

#include "algorithm/collections.hpp"

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    // List/scalar arguments are registered as nullable so a NULL argument reaches the
    // function body (mgp::AddFunction only builds non-nullable types, which the engine
    // rejects during argument validation). Each body then handles NULL explicitly.
    const auto nullable_list = [] { return mgp::type_nullable(mgp::type_list(mgp::type_any())); };
    const auto nullable_any = [] { return mgp::type_nullable(mgp::type_any()); };

    const auto add_list_func = [&](mgp_func_cb cb, std::string_view name, std::string_view arg) {
      auto *func = mgp::module_add_function(module, std::string(name).c_str(), cb);
      mgp::func_add_arg(func, std::string(arg).c_str(), nullable_list());
    };
    const auto add_two_list_func =
        [&](mgp_func_cb cb, std::string_view name, std::string_view arg1, std::string_view arg2) {
          auto *func = mgp::module_add_function(module, std::string(name).c_str(), cb);
          mgp::func_add_arg(func, std::string(arg1).c_str(), nullable_list());
          mgp::func_add_arg(func, std::string(arg2).c_str(), nullable_list());
        };
    const auto add_list_scalar_func =
        [&](mgp_func_cb cb, std::string_view name, std::string_view list_arg, std::string_view scalar_arg) {
          auto *func = mgp::module_add_function(module, std::string(name).c_str(), cb);
          mgp::func_add_arg(func, std::string(list_arg).c_str(), nullable_list());
          mgp::func_add_arg(func, std::string(scalar_arg).c_str(), nullable_any());
        };

    add_list_func(Collections::SumLongs, Collections::kProcedureSumLongs, Collections::kSumLongsArg1);
    add_list_func(Collections::Avg, Collections::kProcedureAvg, Collections::kAvgArg1);
    add_two_list_func(Collections::ContainsAll,
                      Collections::kProcedureContainsAll,
                      Collections::kContainsAllArg1,
                      Collections::kContainsAllArg2);
    add_two_list_func(Collections::Intersection,
                      Collections::kProcedureIntersection,
                      Collections::kIntersectionArg1,
                      Collections::kIntersectionArg2);
    add_two_list_func(Collections::RemoveAll,
                      Collections::kProcedureRemoveAll,
                      Collections::kArgumentsInputList,
                      Collections::kArgumentsRemoveList);
    add_list_func(Collections::Sum, Collections::kProcedureSum, Collections::kInputList);
    add_two_list_func(Collections::Union,
                      Collections::kProcedureUnion,
                      Collections::kArgumentsInputList1,
                      Collections::kArgumentsInputList2);
    add_list_func(Collections::Sort, Collections::kProcedureSort, Collections::kArgumentSort);
    add_list_scalar_func(Collections::ContainsSorted,
                         Collections::kProcedureCS,
                         Collections::kArgumentInputList,
                         Collections::kArgumentElement);
    add_list_func(Collections::Max, Collections::kProcedureMax, Collections::kArgumentMax);
    add_list_func(Collections::Pairs, Collections::kProcedurePairs, Collections::kArgumentPairs);
    add_list_scalar_func(Collections::Contains,
                         Collections::kProcedureContains,
                         Collections::kArgumentListContains,
                         Collections::kArgumentValueContains);
    add_two_list_func(Collections::UnionAll,
                      Collections::kProcedureUnionAll,
                      Collections::kArgumentList1UnionAll,
                      Collections::kArgumentList2UnionAll);
    add_list_func(Collections::Min, Collections::kProcedureMin, Collections::kArgumentListMin);
    add_list_func(Collections::ToSet, Collections::kProcedureToSet, Collections::kArgumentListToSet);
    add_list_func(Collections::FrequenciesAsMap,
                  Collections::kProcedureFrequenciesAsMap,
                  Collections::kArgumentListFrequenciesAsMap);

    // flatten keeps its optional empty-list default while also accepting an explicit NULL.
    {
      auto *func =
          mgp::module_add_function(module, std::string(Collections::kProcedureFlatten).c_str(), Collections::Flatten);
      const auto default_empty_list = mgp::Value(mgp::List{});
      mgp::func_add_opt_arg(
          func, std::string(Collections::kArgumentListFlatten).c_str(), nullable_list(), default_empty_list.ptr());
    }

    // split is a read procedure: NULL input list -> no rows.
    {
      auto *proc =
          mgp::module_add_read_procedure(module, std::string(Collections::kProcedureSplit).c_str(), Collections::Split);
      mgp::proc_add_arg(proc, std::string(Collections::kArgumentInputList).c_str(), nullable_list());
      mgp::proc_add_arg(proc, std::string(Collections::kArgumentDelimiter).c_str(), nullable_any());
      mgp::proc_add_result(proc, std::string(Collections::kReturnSplit).c_str(), mgp::type_list(mgp::type_any()));
    }

    // partition is a read procedure: NULL input list -> no rows.
    {
      auto *proc = mgp::module_add_read_procedure(
          module, std::string(Collections::kProcedurePartition).c_str(), Collections::Partition);
      mgp::proc_add_arg(proc, std::string(Collections::kArgumentListPartition).c_str(), nullable_list());
      // size is intentionally non-nullable: a NULL size is rejected during argument
      // validation, matching the compatibility layer which also errors on a null size.
      mgp::proc_add_arg(proc, std::string(Collections::kArgumentSizePartition).c_str(), mgp::type_int());
      mgp::proc_add_result(
          proc, std::string(Collections::kReturnValuePartition).c_str(), mgp::type_list(mgp::type_any()));
    }

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
