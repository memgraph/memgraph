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

#include <string>
#include <string_view>

#include <mgp.hpp>

namespace Text {

/* join constants */
constexpr std::string_view kProcedureJoin = "join";
constexpr std::string_view kJoinArg1 = "strings";
constexpr std::string_view kJoinArg2 = "delimiter";
constexpr std::string_view kResultJoin = "string";
/* format constants */
constexpr std::string_view kProcedureFormat = "format";
constexpr std::string_view kStringToFormat = "text";
constexpr std::string_view kParameters = "params";
constexpr std::string_view kResultFormat = "result";
/* regex constants */
constexpr std::string_view kProcedureRegexGroups = "regexGroups";
constexpr std::string_view kInput = "input";
constexpr std::string_view kRegex = "regex";
constexpr std::string_view kResultRegexGroups = "results";
/* replace constants */
constexpr std::string_view kProcedureReplace = "replace";
constexpr std::string_view kText = "text";
constexpr std::string_view kReplacement = "replacement";
/* regreplace constants */
constexpr std::string_view kProcedureRegReplace = "regreplace";
constexpr size_t kMaxRegexCacheSize = 1000;
/* distance constants */
constexpr std::string_view kProcedureDistance = "distance";
constexpr std::string_view kText1 = "text1";
constexpr std::string_view kText2 = "text2";
constexpr std::string_view kProcedureIndexOf = "indexOf";
constexpr std::string_view kIndexOfText = "text";
constexpr std::string_view kIndexOfLookup = "lookup";
constexpr std::string_view kIndexOfFrom = "from";
constexpr std::string_view kIndexOfTo = "to";
constexpr std::string_view kResultIndexOf = "output";

void Join(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void Format(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void RegexGroups(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void Replace(mgp_list *args, mgp_func_context *ctx, mgp_func_result *result, mgp_memory *memory);
void RegReplace(mgp_list *args, mgp_func_context *ctx, mgp_func_result *result, mgp_memory *memory);
void Distance(mgp_list *args, mgp_func_context *ctx, mgp_func_result *result, mgp_memory *memory);
void IndexOf(mgp_list *args, mgp_func_context *ctx, mgp_func_result *result, mgp_memory *memory);
}  // namespace Text
