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

#pragma once

#include <mgp.hpp>
#include <sstream>
#include <string>
#include <string_view>

namespace Collections {

/* sum_longs constants */
constexpr const std::string_view kProcedureSumLongs = "sum_longs";
constexpr const std::string_view kSumLongsArg1 = "numbers";

/* avg constants */
constexpr const std::string_view kProcedureAvg = "avg";
constexpr const std::string_view kAvgArg1 = "numbers";

/* contains_all constants */
constexpr const std::string_view kProcedureContainsAll = "contains_all";
constexpr const std::string_view kContainsAllArg1 = "coll";
constexpr const std::string_view kContainsAllArg2 = "values";

/* intersection constants */
constexpr const std::string_view kProcedureIntersection = "intersection";
constexpr const std::string_view kIntersectionArg1 = "first";
constexpr const std::string_view kIntersectionArg2 = "second";

/* remove_all constants */
constexpr std::string_view kProcedureRemoveAll = "remove_all";
constexpr std::string_view kArgumentsInputList = "first";
constexpr std::string_view kArgumentsRemoveList = "second";

/* sum constants */
constexpr std::string_view kProcedureSum = "sum";
constexpr std::string_view kInputList = "numbers";

/* union constants */
constexpr std::string_view kProcedureUnion = "union";
constexpr std::string_view kArgumentsInputList1 = "first";
constexpr std::string_view kArgumentsInputList2 = "second";

/* sort constants */
constexpr std::string_view kProcedureSort = "sort";
constexpr std::string_view kArgumentSort = "coll";

/* contains constants */
constexpr std::string_view kProcedureCS = "contains_sorted";
constexpr std::string_view kArgumentInputList = "coll";
constexpr std::string_view kArgumentElement = "value";

/* max constants */
constexpr std::string_view kProcedureMax = "max";
constexpr std::string_view kArgumentMax = "values";

/* split constants */
constexpr std::string_view kProcedureSplit = "split";
constexpr std::string_view kReturnSplit = "splitted";
constexpr std::string_view kArgumentDelimiter = "delimiter";
constexpr std::string_view kResultSplit = "splitted";

/* pairs constants */
constexpr std::string_view kProcedurePairs = "pairs";
constexpr std::string_view kArgumentPairs = "list";

/* contains constants */
constexpr std::string_view kProcedureContains = "contains";
constexpr std::string_view kArgumentListContains = "coll";
constexpr std::string_view kArgumentValueContains = "value";

/* union_all constants */
constexpr std::string_view kProcedureUnionAll = "union_all";
constexpr std::string_view kArgumentList1UnionAll = "first";
constexpr std::string_view kArgumentList2UnionAll = "second";

/* min constants */
constexpr std::string_view kProcedureMin = "min";
constexpr std::string_view kArgumentListMin = "values";

/* to_set constants */
constexpr std::string_view kProcedureToSet = "to_set";
constexpr std::string_view kArgumentListToSet = "values";

/* partition constants */
constexpr std::string_view kReturnValuePartition = "result";
constexpr std::string_view kProcedurePartition = "partition";
constexpr std::string_view kArgumentListPartition = "list";
constexpr std::string_view kArgumentSizePartition = "partition_size";

/* flatten constants */
constexpr std::string_view kProcedureFlatten = "flatten";
constexpr std::string_view kArgumentListFlatten = "list";

/* frequencies_as_map constants */
constexpr std::string_view kProcedureFrequenciesAsMap = "frequencies_as_map";
constexpr std::string_view kArgumentListFrequenciesAsMap = "coll";

void SetResult(mgp::Result &result, const mgp::Value &value);

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
