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

namespace Neighbors {

constexpr std::string_view kReturnAtHop = "nodes";
constexpr std::string_view kProcedureAtHop = "at_hop";

constexpr std::string_view kReturnByHop = "nodes";
constexpr std::string_view kProcedureByHop = "by_hop";

constexpr std::string_view kArgumentsNode = "node";
constexpr std::string_view kArgumentsRelType = "rel_type";
constexpr std::string_view kArgumentsDistance = "distance";

constexpr std::string_view kResultAtHop = "nodes";
constexpr std::string_view kResultByHop = "nodes";

void AtHop(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void ByHop(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

}  // namespace Neighbors
