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
#include <string>
#include <string_view>
#include <unordered_set>

namespace Map {

/* from_nodes constants */
constexpr const std::string_view kProcedureFromNodes = "from_nodes";
constexpr const std::string_view kFromNodesArg1 = "label";
constexpr const std::string_view kFromNodesArg2 = "property";
constexpr const std::string_view kResultFromNodes = "map";

/* from_values constants */
constexpr const std::string_view kProcedureFromValues = "from_values";
constexpr const std::string_view kFromValuesArg1 = "values";

/* set_key constants */
constexpr const std::string_view kProcedureSetKey = "set_key";
constexpr const std::string_view kSetKeyArg1 = "map";
constexpr const std::string_view kSetKeyArg2 = "key";
constexpr const std::string_view kSetKeyArg3 = "value";
;

/* remove_key constants */
constexpr std::string_view kProcedureRemoveKey = "remove_key";
constexpr std::string_view kArgumentsInputMap = "map";
constexpr std::string_view kArgumentsKey = "key";
constexpr std::string_view kArgumentsIsRecursive = "config";

/* from_pairs constants */
constexpr std::string_view kProcedureFromPairs = "from_pairs";
constexpr std::string_view kArgumentsInputList = "pairs";

/* merge constants */
constexpr std::string_view kProcedureMerge = "merge";
constexpr std::string_view kArgumentsInputMap1 = "first";
constexpr std::string_view kArgumentsInputMap2 = "second";

/* flatten constants */
constexpr std::string_view kProcedureFlatten = "flatten";
constexpr std::string_view kArgumentMapFlatten = "map";
constexpr std::string_view kArgumentDelimiterFlatten = "delimiter";

/* from_lists constants */
constexpr std::string_view kProcedureFromLists = "from_lists";
constexpr std::string_view kArgumentListKeysFromLists = "keys";
constexpr std::string_view kArgumentListValuesFromLists = "values";

/* remove_keys constants */
constexpr std::string_view kProcedureRemoveKeys = "remove_keys";
constexpr std::string_view kArgumentsInputMapRemoveKeys = "map";
constexpr std::string_view kArgumentsKeysListRemoveKeys = "keys";
constexpr std::string_view kArgumentsRecursiveRemoveKeys = "config";

void FromNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void FromValues(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void SetKey(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void RemoveKey(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void RemoveRecursion(mgp::Map &result, bool recursive, std::string_view key);

void FromPairs(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Merge(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Flatten(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void FlattenRecursion(mgp::Map &result, const mgp::Map &input, const std::string &key, const std::string &delimiter);

void FromLists(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void RemoveRecursionSet(mgp::Map &result, bool recursive, std::unordered_set<std::string> &set);

void RemoveKeys(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

}  // namespace Map
