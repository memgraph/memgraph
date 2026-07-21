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

#pragma once

#include <mgp.hpp>
#include <string>

namespace Collections {

/* sum_longs constants */
constexpr const char *kProcedureSumLongs = "sum_longs";
constexpr const char *kSumLongsArg1 = "numbers";

/* avg constants */
constexpr const char *kProcedureAvg = "avg";
constexpr const char *kAvgArg1 = "numbers";

/* contains_all constants */
constexpr const char *kProcedureContainsAll = "contains_all";
constexpr const char *kContainsAllArg1 = "coll";
constexpr const char *kContainsAllArg2 = "values";

/* intersection constants */
constexpr const char *kProcedureIntersection = "intersection";
constexpr const char *kIntersectionArg1 = "first";
constexpr const char *kIntersectionArg2 = "second";

/* remove_all constants */
constexpr const char *kProcedureRemoveAll = "remove_all";
constexpr const char *kArgumentsInputList = "first";
constexpr const char *kArgumentsRemoveList = "second";

/* sum constants */
constexpr const char *kProcedureSum = "sum";
constexpr const char *kInputList = "numbers";

/* union constants */
constexpr const char *kProcedureUnion = "union";
constexpr const char *kArgumentsInputList1 = "first";
constexpr const char *kArgumentsInputList2 = "second";

/* sort constants */
constexpr const char *kProcedureSort = "sort";
constexpr const char *kArgumentSort = "coll";

/* contains constants */
constexpr const char *kProcedureCS = "contains_sorted";
constexpr const char *kArgumentInputList = "coll";
constexpr const char *kArgumentElement = "value";

/* max constants */
constexpr const char *kProcedureMax = "max";
constexpr const char *kArgumentMax = "values";

/* split constants */
constexpr const char *kProcedureSplit = "split";
constexpr const char *kReturnSplit = "splitted";
constexpr const char *kArgumentDelimiter = "delimiter";
constexpr const char *kResultSplit = "splitted";

/* pairs constants */
constexpr const char *kProcedurePairs = "pairs";
constexpr const char *kArgumentPairs = "list";

/* contains constants */
constexpr const char *kProcedureContains = "contains";
constexpr const char *kArgumentListContains = "coll";
constexpr const char *kArgumentValueContains = "value";

/* union_all constants */
constexpr const char *kProcedureUnionAll = "union_all";
constexpr const char *kArgumentList1UnionAll = "first";
constexpr const char *kArgumentList2UnionAll = "second";

/* min constants */
constexpr const char *kProcedureMin = "min";
constexpr const char *kArgumentListMin = "values";

/* to_set constants */
constexpr const char *kProcedureToSet = "to_set";
constexpr const char *kArgumentListToSet = "values";

/* partition constants */
constexpr const char *kReturnValuePartition = "result";
constexpr const char *kProcedurePartition = "partition";
constexpr const char *kArgumentListPartition = "list";
constexpr const char *kArgumentSizePartition = "partition_size";

/* flatten constants */
constexpr const char *kProcedureFlatten = "flatten";
constexpr const char *kArgumentListFlatten = "list";

/* frequencies_as_map constants */
constexpr const char *kProcedureFrequenciesAsMap = "frequencies_as_map";
constexpr const char *kArgumentListFrequenciesAsMap = "coll";

void SumLongs(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Avg(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void ContainsAll(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Intersection(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void RemoveAll(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Sum(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Union(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Sort(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void ContainsSorted(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Max(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Split(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Pairs(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Contains(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void UnionAll(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Min(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void ToSet(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Partition(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Flatten(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void FrequenciesAsMap(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

}  // namespace Collections
