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

// `export` is a C++ keyword, so the namespace is capitalized. The Cypher namespace comes from the .so filename, which
// makes these `export.json_data`, `export.json_all` and `export.json_graph`.
namespace Export {

constexpr const char *kProcedureJsonData = "json_data";
constexpr const char *kProcedureJsonAll = "json_all";
constexpr const char *kProcedureJsonGraph = "json_graph";

constexpr const char *kArgumentNodes = "nodes";
constexpr const char *kArgumentRelationships = "rels";
constexpr const char *kArgumentGraph = "graph";
constexpr const char *kArgumentFile = "file";
constexpr const char *kArgumentConfig = "config";

// The reference's 12 columns. Declaration order only survives an explicit `YIELD file, source, ...` — `YIELD *`
// returns them alphabetically, because the kernel keeps a procedure's results in an ordered map.
constexpr const char *kReturnFile = "file";
constexpr const char *kReturnSource = "source";
constexpr const char *kReturnFormat = "format";
constexpr const char *kReturnNodes = "nodes";
constexpr const char *kReturnRelationships = "relationships";
constexpr const char *kReturnProperties = "properties";
constexpr const char *kReturnTime = "time";
constexpr const char *kReturnRows = "rows";
constexpr const char *kReturnBatchSize = "batchSize";
constexpr const char *kReturnBatches = "batches";
constexpr const char *kReturnDone = "done";
constexpr const char *kReturnData = "data";

// Keys the `graph` map argument of json_graph is read from. `edges` is accepted as an alias for `relationships`
// because that is the key project() produces.
constexpr const char *kGraphKeyNodes = "nodes";
constexpr const char *kGraphKeyRelationships = "relationships";
constexpr const char *kGraphKeyEdges = "edges";

void JsonData(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void JsonAll(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
void JsonGraph(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

}  // namespace Export
