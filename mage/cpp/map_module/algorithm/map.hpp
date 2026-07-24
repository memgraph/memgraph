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
#include <string_view>
#include <unordered_set>

namespace Map {

/* from_nodes constants */
constexpr const char *kProcedureFromNodes = "from_nodes";
constexpr const char *kFromNodesArg1 = "label";
constexpr const char *kFromNodesArg2 = "property";
constexpr const char *kResultFromNodes = "map";

/* from_values constants */
constexpr const char *kProcedureFromValues = "from_values";
constexpr const char *kFromValuesArg1 = "values";

/* set_key constants */
constexpr const char *kProcedureSetKey = "set_key";
constexpr const char *kSetKeyArg1 = "map";
constexpr const char *kSetKeyArg2 = "key";
constexpr const char *kSetKeyArg3 = "value";

/* remove_key constants */
constexpr const char *kProcedureRemoveKey = "remove_key";
constexpr const char *kArgumentsInputMap = "map";
constexpr const char *kArgumentsKey = "key";
constexpr const char *kArgumentsIsRecursive = "config";

/* from_pairs constants */
constexpr const char *kProcedureFromPairs = "from_pairs";
constexpr const char *kArgumentsInputList = "pairs";

/* merge constants */
constexpr const char *kProcedureMerge = "merge";
constexpr const char *kArgumentsInputMap1 = "map1";
constexpr const char *kArgumentsInputMap2 = "map2";

/* flatten constants */
constexpr const char *kProcedureFlatten = "flatten";
constexpr const char *kArgumentMapFlatten = "map";
constexpr const char *kArgumentDelimiterFlatten = "delimiter";

/* from_lists constants */
constexpr const char *kProcedureFromLists = "from_lists";
constexpr const char *kArgumentListKeysFromLists = "keys";
constexpr const char *kArgumentListValuesFromLists = "values";

/* remove_keys constants */
constexpr const char *kProcedureRemoveKeys = "remove_keys";
constexpr const char *kArgumentsInputMapRemoveKeys = "map";
constexpr const char *kArgumentsKeysListRemoveKeys = "keys";
constexpr const char *kArgumentsRecursiveRemoveKeys = "config";

/* get constants */
constexpr const char *kProcedureGet = "get";
constexpr const char *kArgumentMapGet = "map";
constexpr const char *kArgumentKeyGet = "key";
constexpr const char *kArgumentValueGet = "value";
constexpr const char *kArgumentFailGet = "fail";

/* merge_list constants */
constexpr const char *kProcedureMergeList = "merge_list";
constexpr const char *kArgumentMapsMergeList = "maps";

void FromNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void FromValues(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void SetKey(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void RemoveKey(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void FromPairs(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Merge(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void Flatten(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void FlattenRecursion(mgp::Map &result, const mgp::Map &input, const std::string &key, const std::string &delimiter);

void FromLists(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void RemoveRecursionSet(mgp::Map &result, bool recursive, std::unordered_set<std::string> &set);

void RemoveKeys(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

// Coerces a value to a Map: a Node/Relationship yields its properties map, a Map is
// returned as-is; other types throw.
mgp::Map ToMap(const mgp::Value &value);

// Stringifies a value for use as a map key; doubles use the shortest round-tripping form.
std::string KeyToString(const mgp::Value &value);

void Get(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void MergeList(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

}  // namespace Map
