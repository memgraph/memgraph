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

namespace Search {

constexpr const char *kProcedureNode = "node";
constexpr const char *kProcedureNodeAll = "node_all";

constexpr const char *kReturnNode = "node";
constexpr const char *kResultNode = "node";

constexpr const char *kArgumentLabelPropertyMap = "label_property_map";
constexpr const char *kArgumentOperator = "operator";
constexpr const char *kArgumentValue = "value";

// A node is emitted once per property it matches. `node` deduplicates by node id; `node_all` does not.
void Node(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void NodeAll(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

}  // namespace Search
