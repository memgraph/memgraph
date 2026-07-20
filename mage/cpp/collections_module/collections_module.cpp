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

const char *CStr(std::string_view sv) { return sv.data(); }

}  // namespace

extern "C" int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  try {
    const mgp::MemoryDispatcherGuard guard{memory};

    // Registered directly through the C API so element types can be expressed as `LIST? OF ANY?`
    // (NULL elements reach the body), which the high-level mgp::AddFunction/Parameter cannot encode.

    // Numeric/ordering functions: whole list may be NULL, but a NULL element is rejected during
    // validation (LIST? OF ANY) so the body never has to compare against NULL.
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureSumLongs), Collections::SumLongs);
      mgp::func_add_arg(func, CStr(Collections::kSumLongsArg1), ListOfAny());
    }
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureAvg), Collections::Avg);
      mgp::func_add_arg(func, CStr(Collections::kAvgArg1), ListOfAny());
    }
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureSum), Collections::Sum);
      mgp::func_add_arg(func, CStr(Collections::kInputList), ListOfAny());
    }
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureSort), Collections::Sort);
      mgp::func_add_arg(func, CStr(Collections::kArgumentSort), ListOfAny());
    }
    {
      // contains_sorted: keep the whole list nullable (contains_sorted(null, x) -> false) but reject a
      // NULL element and a NULL search value at the type layer (both would NPE in the compatibility layer).
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureCS), Collections::ContainsSorted);
      mgp::func_add_arg(func, CStr(Collections::kArgumentInputList), ListOfAny());
      mgp::func_add_arg(func, CStr(Collections::kArgumentElement), mgp::type_any());
    }

    // Functions that handle NULL elements explicitly (LIST? OF ANY?).
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureContainsAll), Collections::ContainsAll);
      mgp::func_add_arg(func, CStr(Collections::kContainsAllArg1), ListOfNullable());
      mgp::func_add_arg(func, CStr(Collections::kContainsAllArg2), ListOfNullable());
    }
    {
      auto *func =
          mgp::module_add_function(module, CStr(Collections::kProcedureIntersection), Collections::Intersection);
      mgp::func_add_arg(func, CStr(Collections::kIntersectionArg1), ListOfNullable());
      mgp::func_add_arg(func, CStr(Collections::kIntersectionArg2), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureRemoveAll), Collections::RemoveAll);
      mgp::func_add_arg(func, CStr(Collections::kArgumentsInputList), ListOfNullable());
      mgp::func_add_arg(func, CStr(Collections::kArgumentsRemoveList), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureUnion), Collections::Union);
      mgp::func_add_arg(func, CStr(Collections::kArgumentsInputList1), ListOfNullable());
      mgp::func_add_arg(func, CStr(Collections::kArgumentsInputList2), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureUnionAll), Collections::UnionAll);
      mgp::func_add_arg(func, CStr(Collections::kArgumentList1UnionAll), ListOfNullable());
      mgp::func_add_arg(func, CStr(Collections::kArgumentList2UnionAll), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureMax), Collections::Max);
      mgp::func_add_arg(func, CStr(Collections::kArgumentMax), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureMin), Collections::Min);
      mgp::func_add_arg(func, CStr(Collections::kArgumentListMin), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureToSet), Collections::ToSet);
      mgp::func_add_arg(func, CStr(Collections::kArgumentListToSet), ListOfNullable());
    }
    {
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedurePairs), Collections::Pairs);
      mgp::func_add_arg(func, CStr(Collections::kArgumentPairs), ListOfNullable());
    }
    {
      // contains: the list handles NULL elements and the search value may be NULL (contains(list, null) -> false).
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureContains), Collections::Contains);
      mgp::func_add_arg(func, CStr(Collections::kArgumentListContains), ListOfNullable());
      mgp::func_add_arg(func, CStr(Collections::kArgumentValueContains), NullableAny());
    }
    {
      auto *func = mgp::module_add_function(
          module, CStr(Collections::kProcedureFrequenciesAsMap), Collections::FrequenciesAsMap);
      mgp::func_add_arg(func, CStr(Collections::kArgumentListFrequenciesAsMap), ListOfNullable());
    }
    {
      // flatten keeps its optional empty-list default while also accepting an explicit NULL and NULL elements.
      auto flatten_default = mgp::Value(mgp::List{});
      auto *func = mgp::module_add_function(module, CStr(Collections::kProcedureFlatten), Collections::Flatten);
      mgp::func_add_opt_arg(func, CStr(Collections::kArgumentListFlatten), ListOfNullable(), flatten_default.ptr());
    }

    // Read procedures.
    {
      // split: a NULL input list yields no rows; the delimiter may be NULL (a NULL delimiter never matches).
      auto *proc = mgp::module_add_read_procedure(module, CStr(Collections::kProcedureSplit), Collections::Split);
      mgp::proc_add_arg(proc, CStr(Collections::kArgumentInputList), ListOfNullable());
      mgp::proc_add_arg(proc, CStr(Collections::kArgumentDelimiter), NullableAny());
      mgp::proc_add_result(proc, CStr(Collections::kReturnSplit), mgp::type_list(mgp::type_nullable(mgp::type_any())));
    }
    {
      // partition: a NULL input list yields no rows; size is intentionally non-nullable, matching the
      // compatibility layer which also errors on a null size.
      auto *proc =
          mgp::module_add_read_procedure(module, CStr(Collections::kProcedurePartition), Collections::Partition);
      mgp::proc_add_arg(proc, CStr(Collections::kArgumentListPartition), ListOfNullable());
      mgp::proc_add_arg(proc, CStr(Collections::kArgumentSizePartition), mgp::type_int());
      mgp::proc_add_result(
          proc, CStr(Collections::kReturnValuePartition), mgp::type_list(mgp::type_nullable(mgp::type_any())));
    }

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
