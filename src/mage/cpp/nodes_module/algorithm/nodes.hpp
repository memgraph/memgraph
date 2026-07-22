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

namespace Nodes {

/* relationship_types constants */
constexpr const std::string_view kProcedureRelationshipTypes = "relationship_types";
constexpr const std::string_view kRelationshipTypesArg1 = "nodes";
constexpr const std::string_view kRelationshipTypesArg2 = "types";
constexpr const std::string_view kResultRelationshipTypes = "relationship_types";

/* delete constants */
constexpr const std::string_view kProcedureDelete = "delete";
constexpr const std::string_view kDeleteArg1 = "nodes";

/*link constants*/
constexpr size_t minimumNodeListSize = 2;
constexpr std::string_view kProcedureLink = "link";
constexpr std::string_view kArgumentNodesLink = "nodes";
constexpr std::string_view kArgumentTypeLink = "type";

/*relationships_exist constants*/
constexpr std::string_view kProcedureRelationshipsExist = "relationships_exist";
constexpr std::string_view kReturnRelationshipsExist = "result";
constexpr std::string_view kArgumentNodesRelationshipsExist = "nodes";
constexpr std::string_view kArgumentRelationshipsRelationshipsExist = "relationships";
constexpr std::string_view kRelationshipsExistStatus = "Relationships_exist_status";
constexpr std::string_view kNodeRelationshipsExist = "Node";

void RelationshipTypes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Delete(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Link(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void RelationshipsExist(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
bool RelationshipExist(const mgp::Node &node, std::string &rel_type);

}  // namespace Nodes

namespace Nodes {}  // namespace Nodes
