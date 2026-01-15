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
#include <string_view>

namespace Meta {

/* update constants */
constexpr std::string_view kProcedureUpdate = "update";
constexpr std::string_view kUpdateArg1 = "createdObjects";
constexpr std::string_view kUpdateArg2 = "deletedObjects";
constexpr std::string_view kUpdateArg3 = "removedVertexProperties";
constexpr std::string_view kUpdateArg4 = "removedEdgeProperties";
constexpr std::string_view kUpdateArg5 = "setVertexLabels";
constexpr std::string_view kUpdateArg6 = "removedVertexLabels";

/* stats constants */
constexpr std::string_view kProcedureStatsOnline = "stats_online";
constexpr std::string_view kProcedureStatsOffline = "stats_offline";
constexpr std::string_view kStatsOnlineArg1 = "update_stats";
constexpr std::string_view kReturnStats1 = "labelCount";
constexpr std::string_view kReturnStats2 = "relationshipTypeCount";
constexpr std::string_view kReturnStats3 = "propertyKeyCount";
constexpr std::string_view kReturnStats4 = "nodeCount";
constexpr std::string_view kReturnStats5 = "relationshipCount";
constexpr std::string_view kReturnStats6 = "labels";
constexpr std::string_view kReturnStats7 = "relationshipTypes";
constexpr std::string_view kReturnStats8 = "relationshipTypesCount";
constexpr std::string_view kReturnStats9 = "stats";

/* reset constants */
constexpr std::string_view kProcedureReset = "reset";

void Update(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void StatsOnline(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void StatsOffline(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Reset(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

}  // namespace Meta
