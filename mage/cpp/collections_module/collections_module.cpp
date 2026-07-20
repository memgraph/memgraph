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
      auto *func = mgp::module_add_function(module, Collections::kProcedureSumLongs.data(), Collections::SumLongs);
      mgp::func_add_arg(func, Collections::kSumLongsArg1.data(), ListOfAny());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureAvg.data(), Collections::Avg);
      mgp::func_add_arg(func, Collections::kAvgArg1.data(), ListOfAny());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureSum.data(), Collections::Sum);
      mgp::func_add_arg(func, Collections::kInputList.data(), ListOfAny());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureSort.data(), Collections::Sort);
      mgp::func_add_arg(func, Collections::kArgumentSort.data(), ListOfAny());
    }
    {
      // contains_sorted: keep the whole list nullable (contains_sorted(null, x) -> false) but reject a
      // NULL element and a NULL search value at the type layer (both would NPE in the compatibility layer).
      auto *func = mgp::module_add_function(module, Collections::kProcedureCS.data(), Collections::ContainsSorted);
      mgp::func_add_arg(func, Collections::kArgumentInputList.data(), ListOfAny());
      mgp::func_add_arg(func, Collections::kArgumentElement.data(), mgp::type_any());
    }

    // Functions that handle NULL elements explicitly (LIST? OF ANY?).
    {
      auto *func =
          mgp::module_add_function(module, Collections::kProcedureContainsAll.data(), Collections::ContainsAll);
      mgp::func_add_arg(func, Collections::kContainsAllArg1.data(), ListOfNullable());
      mgp::func_add_arg(func, Collections::kContainsAllArg2.data(), ListOfNullable());
    }
    {
      auto *func =
          mgp::module_add_function(module, Collections::kProcedureIntersection.data(), Collections::Intersection);
      mgp::func_add_arg(func, Collections::kIntersectionArg1.data(), ListOfNullable());
      mgp::func_add_arg(func, Collections::kIntersectionArg2.data(), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureRemoveAll.data(), Collections::RemoveAll);
      mgp::func_add_arg(func, Collections::kArgumentsInputList.data(), ListOfNullable());
      mgp::func_add_arg(func, Collections::kArgumentsRemoveList.data(), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureUnion.data(), Collections::Union);
      mgp::func_add_arg(func, Collections::kArgumentsInputList1.data(), ListOfNullable());
      mgp::func_add_arg(func, Collections::kArgumentsInputList2.data(), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureUnionAll.data(), Collections::UnionAll);
      mgp::func_add_arg(func, Collections::kArgumentList1UnionAll.data(), ListOfNullable());
      mgp::func_add_arg(func, Collections::kArgumentList2UnionAll.data(), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureMax.data(), Collections::Max);
      mgp::func_add_arg(func, Collections::kArgumentMax.data(), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureMin.data(), Collections::Min);
      mgp::func_add_arg(func, Collections::kArgumentListMin.data(), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedureToSet.data(), Collections::ToSet);
      mgp::func_add_arg(func, Collections::kArgumentListToSet.data(), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, Collections::kProcedurePairs.data(), Collections::Pairs);
      mgp::func_add_arg(func, Collections::kArgumentPairs.data(), ListOfNullable());
    }
    {
      // contains: the list handles NULL elements and the search value may be NULL (contains(list, null) -> false).
      auto *func = mgp::module_add_function(module, Collections::kProcedureContains.data(), Collections::Contains);
      mgp::func_add_arg(func, Collections::kArgumentListContains.data(), ListOfNullable());
      mgp::func_add_arg(func, Collections::kArgumentValueContains.data(), NullableAny());
    }
    {
      auto *func = mgp::module_add_function(
          module, Collections::kProcedureFrequenciesAsMap.data(), Collections::FrequenciesAsMap);
      mgp::func_add_arg(func, Collections::kArgumentListFrequenciesAsMap.data(), ListOfNullable());
    }
    {
      // flatten keeps its optional empty-list default while also accepting an explicit NULL and NULL elements.
      auto flatten_default = mgp::Value(mgp::List{});
      auto *func = mgp::module_add_function(module, Collections::kProcedureFlatten.data(), Collections::Flatten);
      mgp::func_add_opt_arg(func, Collections::kArgumentListFlatten.data(), ListOfNullable(), flatten_default.ptr());
    }

    // Read procedures.
    {
      // split: a NULL input list yields no rows; the delimiter may be NULL (a NULL delimiter never matches).
      auto *proc = mgp::module_add_read_procedure(module, Collections::kProcedureSplit.data(), Collections::Split);
      mgp::proc_add_arg(proc, Collections::kArgumentInputList.data(), ListOfNullable());
      mgp::proc_add_arg(proc, Collections::kArgumentDelimiter.data(), NullableAny());
      mgp::proc_add_result(proc, Collections::kReturnSplit.data(), mgp::type_list(mgp::type_nullable(mgp::type_any())));
    }
    {
      // partition: a NULL input list yields no rows; size is intentionally non-nullable, matching the
      // compatibility layer which also errors on a null size.
      auto *proc =
          mgp::module_add_read_procedure(module, Collections::kProcedurePartition.data(), Collections::Partition);
      mgp::proc_add_arg(proc, Collections::kArgumentListPartition.data(), ListOfNullable());
      mgp::proc_add_arg(proc, Collections::kArgumentSizePartition.data(), mgp::type_int());
      mgp::proc_add_result(
          proc, Collections::kReturnValuePartition.data(), mgp::type_list(mgp::type_nullable(mgp::type_any())));
    }

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
