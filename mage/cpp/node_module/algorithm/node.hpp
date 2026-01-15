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
#include <unordered_set>

namespace Node {

/*relationships_exist constants*/
constexpr std::string_view kProcedureRelationshipsExist = "relationships_exist";
constexpr std::string_view kReturnRelationshipsExist = "result";
constexpr std::string_view kArgumentNodesRelationshipsExist = "node";
constexpr std::string_view kArgumentRelationshipsRelationshipsExist = "relationships";

/* relationship_exists constants */
constexpr std::string_view kReturnRelationshipExists = "exists";
constexpr std::string_view kProcedureRelationshipExists = "relationship_exists";
constexpr std::string_view kArgumentsNode = "node";
constexpr std::string_view kArgumentsPattern = "pattern";
constexpr std::string_view kResultRelationshipExists = "exists";

/* relationship_types constants */
constexpr std::string_view kProcedureRelationshipTypes = "relationship_types";
constexpr std::string_view kRelationshipTypesArg1 = "node";
constexpr std::string_view kRelationshipTypesArg2 = "types";
constexpr std::string_view kResultRelationshipTypes = "relationship_types";

/* degree_in constants */
constexpr std::string_view kFunctionDegreeIn = "degree_in";
constexpr std::string_view kDegreeInArg1 = "node";
constexpr std::string_view kDegreeInArg2 = "type";

/* degree_out constants */
constexpr std::string_view kFunctionDegreeOut = "degree_out";
constexpr std::string_view kDegreeOutArg1 = "node";
constexpr std::string_view kDegreeOutArg2 = "type";

void RelationshipsExist(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

bool RelationshipExist(const mgp::Node &node, std::string &rel_type);

bool FindRelationship(std::unordered_set<std::string_view> types, mgp::Relationships relationships);

void RelationshipExists(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void RelationshipTypes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void DegreeIn(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

void DegreeOut(mgp_list *args, mgp_func_context *ctx, mgp_func_result *res, mgp_memory *memory);

}  // namespace Node
