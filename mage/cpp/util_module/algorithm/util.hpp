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

namespace Util {
/*md5 constants*/
constexpr std::string_view kProcedureMd5 = "md5";
constexpr std::string_view kArgumentValuesMd5 = "values";
constexpr std::string_view kArgumentResultMd5 = "result";
constexpr std::string_view kArgumentStringToHash = "stringToHash";

void Md5Procedure(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void Md5Function(mgp_list *args, mgp_func_context *func_context, mgp_func_result *res, mgp_memory *memory);
std::string Md5(mgp::List arguments);
}  // namespace Util
