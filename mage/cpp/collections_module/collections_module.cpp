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

    mgp::AddFunction(Collections::SumLongs, Collections::kProcedureSumLongs,
                     {mgp::Parameter(Collections::kSumLongsArg1, {mgp::Type::List, mgp::Type::Any})}, module, memory);

    mgp::AddFunction(Collections::Avg, Collections::kProcedureAvg,
                     {mgp::Parameter(Collections::kAvgArg1, {mgp::Type::List, mgp::Type::Any})}, module, memory);

    mgp::AddFunction(Collections::ContainsAll, Collections::kProcedureContainsAll,
                     {mgp::Parameter(Collections::kContainsAllArg1, {mgp::Type::List, mgp::Type::Any}),
                      mgp::Parameter(Collections::kContainsAllArg2, {mgp::Type::List, mgp::Type::Any})},
                     module, memory);

    mgp::AddFunction(Collections::Intersection, Collections::kProcedureIntersection,
                     {mgp::Parameter(Collections::kIntersectionArg1, {mgp::Type::List, mgp::Type::Any}),
                      mgp::Parameter(Collections::kIntersectionArg2, {mgp::Type::List, mgp::Type::Any})},
                     module, memory);

    mgp::AddFunction(Collections::RemoveAll, Collections::kProcedureRemoveAll,
                     {mgp::Parameter(Collections::kArgumentsInputList, {mgp::Type::List, mgp::Type::Any}),
                      mgp::Parameter(Collections::kArgumentsRemoveList, {mgp::Type::List, mgp::Type::Any})},
                     module, memory);

    mgp::AddFunction(Collections::Sum, Collections::kProcedureSum,
                     {mgp::Parameter(Collections::kInputList, {mgp::Type::List, mgp::Type::Any})}, module, memory);

    mgp::AddFunction(Collections::Union, Collections::kProcedureUnion,
                     {mgp::Parameter(Collections::kArgumentsInputList1, {mgp::Type::List, mgp::Type::Any}),
                      mgp::Parameter(Collections::kArgumentsInputList2, {mgp::Type::List, mgp::Type::Any})},
                     module, memory);

    mgp::AddFunction(Collections::Sort, Collections::kProcedureSort,
                     {mgp::Parameter(Collections::kArgumentSort, {mgp::Type::List, mgp::Type::Any})}, module, memory);

    mgp::AddFunction(Collections::ContainsSorted, Collections::kProcedureCS,
                     {mgp::Parameter(Collections::kArgumentInputList, {mgp::Type::List, mgp::Type::Any}),
                      mgp::Parameter(Collections::kArgumentElement, mgp::Type::Any)},
                     module, memory);

    mgp::AddFunction(Collections::Max, Collections::kProcedureMax,
                     {mgp::Parameter(Collections::kArgumentMax, {mgp::Type::List, mgp::Type::Any})}, module, memory);

    AddProcedure(Collections::Split, Collections::kProcedureSplit, mgp::ProcedureType::Read,
                 {mgp::Parameter(Collections::kArgumentInputList, {mgp::Type::List, mgp::Type::Any}),
                  mgp::Parameter(Collections::kArgumentDelimiter, mgp::Type::Any)},
                 {mgp::Return(Collections::kReturnSplit, {mgp::Type::List, mgp::Type::Any})}, module, memory);

    mgp::AddFunction(Collections::Pairs, Collections::kProcedurePairs,
                     {mgp::Parameter(Collections::kArgumentPairs, {mgp::Type::List, mgp::Type::Any})}, module, memory);

    mgp::AddFunction(Collections::Contains, Collections::kProcedureContains,
                     {mgp::Parameter(Collections::kArgumentListContains, {mgp::Type::List, mgp::Type::Any}),
                      mgp::Parameter(Collections::kArgumentValueContains, mgp::Type::Any)},
                     module, memory);

    mgp::AddFunction(Collections::Min, Collections::kProcedureMin,
                     {mgp::Parameter(Collections::kArgumentListMin, {mgp::Type::List, mgp::Type::Any})}, module,
                     memory);

    mgp::AddFunction(Collections::UnionAll, Collections::kProcedureUnionAll,
                     {mgp::Parameter(Collections::kArgumentList1UnionAll, {mgp::Type::List, mgp::Type::Any}),
                      mgp::Parameter(Collections::kArgumentList2UnionAll, {mgp::Type::List, mgp::Type::Any})},
                     module, memory);

    mgp::AddFunction(Collections::ToSet, Collections::kProcedureToSet,
                     {mgp::Parameter(Collections::kArgumentListToSet, {mgp::Type::List, mgp::Type::Any})}, module,
                     memory);

    mgp::AddFunction(Collections::FrequenciesAsMap, Collections::kProcedureFrequenciesAsMap,
                     {mgp::Parameter(Collections::kArgumentListFrequenciesAsMap, {mgp::Type::List, mgp::Type::Any})},
                     module, memory);

    AddProcedure(Collections::Partition, Collections::kProcedurePartition, mgp::ProcedureType::Read,
                 {mgp::Parameter(Collections::kArgumentListPartition, {mgp::Type::List, mgp::Type::Any}),
                  mgp::Parameter(Collections::kArgumentSizePartition, mgp::Type::Int)},
                 {mgp::Return(Collections::kReturnValuePartition, {mgp::Type::List, mgp::Type::Any})}, module, memory);

    mgp::AddFunction(
        Collections::Flatten, Collections::kProcedureFlatten,
        {mgp::Parameter(Collections::kArgumentListFlatten, {mgp::Type::List, mgp::Type::Any}, mgp::Value(mgp::List{}))},
        module, memory);

  } catch (const std::exception &e) {
    return 1;
  }

  return 0;
}

extern "C" int mgp_shutdown_module() { return 0; }
