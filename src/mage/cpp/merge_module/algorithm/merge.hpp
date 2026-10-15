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

#include <unordered_map>
#include <unordered_set>

namespace Merge {

/* relationship constants */
constexpr const std::string_view kProcedureRelationship = "relationship";
constexpr const std::string_view kRelationshipArg1 = "startNode";
constexpr const std::string_view kRelationshipArg2 = "relationshipType";
constexpr const std::string_view kRelationshipArg3 = "identProps";
constexpr const std::string_view kRelationshipArg4 = "createProps";
constexpr const std::string_view kRelationshipArg5 = "endNode";
constexpr const std::string_view kRelationshipArg6 = "matchProps";
constexpr const std::string_view kRelationshipResult = "rel";

/* node constants */
constexpr std::string_view kProcedureNode = "node";
constexpr std::string_view kNodeArg1 = "labels";
constexpr std::string_view kNodeArg2 = "identProps";
constexpr std::string_view kNodeArg3 = "createProps";
constexpr std::string_view kNodeArg4 = "matchProps";
constexpr std::string_view kNodeRes = "node";

void Relationship(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Node(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
bool IdentProp(const mgp::Map &ident_prop, const mgp::Node &node);
bool LabelsContained(const std::unordered_set<std::string_view> &labels, const mgp::Node &node);

}  // namespace Merge
