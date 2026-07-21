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

namespace {

// Type recipes (see mg_procedure argument validation):
//  - `LIST? OF ANY?`: whole argument may be NULL and NULL elements reach the body. Used by functions
//    that emulate the compatibility layer's null-element handling.
//  - `LIST? OF ANY`:  whole argument may be NULL but a NULL element is rejected during validation.
//    Used by the numeric/ordering functions that would otherwise throw on a NULL element.
//  - `ANY?`:          scalar that may be NULL and reaches the body.
mgp_type *ListOfNullable() { return mgp::type_nullable(mgp::type_list(mgp::type_nullable(mgp::type_any()))); }

mgp_type *ListOfAny() { return mgp::type_nullable(mgp::type_list(mgp::type_any())); }

mgp_type *NullableAny() { return mgp::type_nullable(mgp::type_any()); }

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    // Registered directly through the C API so element types can be expressed as `LIST? OF ANY?`
    // (NULL elements reach the body), which the high-level mgp::AddFunction/Parameter cannot encode.

    // Numeric/ordering functions: whole list may be NULL, but a NULL element is rejected during
    // validation (LIST? OF ANY) so the body never has to compare against NULL.
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureSumLongs, Collections::SumLongs);
      mgp::func_add_arg(func, Collections::kSumLongsArg1, ListOfAny());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureAvg, Collections::Avg);
      mgp::func_add_arg(func, Collections::kAvgArg1, ListOfAny());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureSum, Collections::Sum);
      mgp::func_add_arg(func, Collections::kInputList, ListOfAny());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureSort, Collections::Sort);
      mgp::func_add_arg(func, Collections::kArgumentSort, ListOfAny());
    }
    {
      // contains_sorted: keep the whole list nullable (contains_sorted(null, x) -> false) but reject a
      // NULL element and a NULL search value at the type layer (both would NPE in the compatibility layer).
      auto *func = mgp::module_add_function(module, Collections::kProcedureCS, Collections::ContainsSorted);
      mgp::func_add_arg(func, Collections::kArgumentInputList, ListOfAny());
      mgp::func_add_arg(func, Collections::kArgumentElement, mgp::type_any());
    }

    // Functions that handle NULL elements explicitly (LIST? OF ANY?).
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureContainsAll, Collections::ContainsAll);
      mgp::func_add_arg(func, Collections::kContainsAllArg1, ListOfNullable());
      mgp::func_add_arg(func, Collections::kContainsAllArg2, ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureIntersection, Collections::Intersection);
      mgp::func_add_arg(func, Collections::kIntersectionArg1, ListOfNullable());
      mgp::func_add_arg(func, Collections::kIntersectionArg2, ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureRemoveAll, Collections::RemoveAll);
      mgp::func_add_arg(func, Collections::kArgumentsInputList, ListOfNullable());
      mgp::func_add_arg(func, Collections::kArgumentsRemoveList, ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureUnion, Collections::Union);
      mgp::func_add_arg(func, Collections::kArgumentsInputList1, ListOfNullable());
      mgp::func_add_arg(func, Collections::kArgumentsInputList2, ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureUnionAll, Collections::UnionAll);
      mgp::func_add_arg(func, Collections::kArgumentList1UnionAll, ListOfNullable());
      mgp::func_add_arg(func, Collections::kArgumentList2UnionAll, ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureMax, Collections::Max);
      mgp::func_add_arg(func, Collections::kArgumentMax, ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureMin, Collections::Min);
      mgp::func_add_arg(func, Collections::kArgumentListMin, ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureToSet, Collections::ToSet);
      mgp::func_add_arg(func, Collections::kArgumentListToSet, ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedurePairs, Collections::Pairs);
      mgp::func_add_arg(func, Collections::kArgumentPairs, ListOfNullable());
    }
    {
      // contains: the list handles NULL elements and the search value may be NULL (contains(list, null) -> false).
      auto *func = mgp::module_add_function(module, Collections::kProcedureContains, Collections::Contains);
      mgp::func_add_arg(func, Collections::kArgumentListContains, ListOfNullable());
      mgp::func_add_arg(func, Collections::kArgumentValueContains, NullableAny());
    }
    {
      auto *func =
          mgp::module_add_function(module, Collections::kProcedureFrequenciesAsMap, Collections::FrequenciesAsMap);
      mgp::func_add_arg(func, Collections::kArgumentListFrequenciesAsMap, ListOfNullable());
    }
    {
      // flatten keeps its optional empty-list default while also accepting an explicit NULL and NULL elements.
      auto flatten_default = mgp::Value(mgp::List{});
      auto *func = mgp::module_add_function(module, Collections::kProcedureFlatten, Collections::Flatten);
      mgp::func_add_opt_arg(func, Collections::kArgumentListFlatten, ListOfNullable(), flatten_default.ptr());
    }

    // Read procedures.
    {
      // split: a NULL input list yields no rows; the delimiter may be NULL (a NULL delimiter never matches).
      auto *proc = mgp::module_add_read_procedure(module, Collections::kProcedureSplit, Collections::Split);
      mgp::proc_add_arg(proc, Collections::kArgumentInputList, ListOfNullable());
      mgp::proc_add_arg(proc, Collections::kArgumentDelimiter, NullableAny());
      mgp::proc_add_result(proc, Collections::kReturnSplit, mgp::type_list(mgp::type_nullable(mgp::type_any())));
    }
    {
      // partition: a NULL input list yields no rows; size is intentionally non-nullable, matching the
      // compatibility layer which also errors on a null size.
      auto *proc = mgp::module_add_read_procedure(module, Collections::kProcedurePartition, Collections::Partition);
      mgp::proc_add_arg(proc, Collections::kArgumentListPartition, ListOfNullable());
      mgp::proc_add_arg(proc, Collections::kArgumentSizePartition, mgp::type_int());
      mgp::proc_add_result(
          proc, Collections::kReturnValuePartition, mgp::type_list(mgp::type_nullable(mgp::type_any())));
    }

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
