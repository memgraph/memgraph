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

namespace Refactor {

enum class RelSelectStrategy { INCOMING = 0, OUTGOING = 1, MERGE = 2 };

enum class PropertiesStrategy { DISCARD = 0, OVERRIDE = 1, COMBINE = 2 };

struct Config {
  explicit Config(const mgp::Map &config);

  void SetRelStrategy(std::string_view strategy);
  void SetPropStrategy(std::string_view strategy);

  RelSelectStrategy rel_strategy;
  PropertiesStrategy prop_strategy;
};

/* categorize constants */
constexpr const std::string_view kProcedureCategorize = "categorize";
constexpr const std::string_view kReturnCategorize = "status";

constexpr const std::string_view kArgumentsCatSourceKey = "original_prop_key";
constexpr const std::string_view kArgumentsCatRelType = "rel_type";
constexpr const std::string_view kArgumentsCatRelOutgoing = "is_outgoing";
constexpr const std::string_view kArgumentsCatLabelName = "new_label";
constexpr const std::string_view kArgumentsCatPropKey = "new_prop_name_key";
constexpr const std::string_view kArgumentsCopyPropKeys = "copy_props_list";

constexpr const std::string_view kResultCategorize = "status";

/* clone_nodes constants */
constexpr const std::string_view kProcedureCloneNodes = "clone_nodes";
constexpr const std::string_view kArgumentsNodesToClone = "nodes";
constexpr const std::string_view kArgumentsCloneRels = "withRelationships";
constexpr const std::string_view kArgumentsSkipPropClone = "skipProperties";
constexpr const std::string_view kResultClonedNodeId = "input";
constexpr const std::string_view kResultNewNode = "output";
constexpr const std::string_view kResultCloneNodeError = "error";

/* clone_subgraph_from_paths constants */
constexpr const std::string_view kProcedureCSFP = "clone_subgraph_from_paths";
constexpr const std::string_view kArgumentsPath = "paths";
constexpr const std::string_view kArgumentsConfigMap = "config";

/* clone_subgraph constants */
constexpr const std::string_view kProcedureCloneSubgraph = "clone_subgraph";
constexpr const std::string_view kArgumentsNodes = "nodes";
constexpr const std::string_view kArgumentsRels = "rels";

/* from constants */
constexpr const std::string_view kProcedureFrom = "from";
constexpr const std::string_view kFromArg1 = "relationship";
constexpr const std::string_view kFromArg2 = "new_from";
constexpr const std::string_view kFromResult = "relationship";

/* to constants */
constexpr const std::string_view kProcedureTo = "to";
constexpr const std::string_view kToArg1 = "relationship";
constexpr const std::string_view kToArg2 = "new_to";
constexpr const std::string_view kToResult = "relationship";

/* rename_label constants */
constexpr std::string_view kProcedureRenameLabel = "rename_label";
constexpr std::string_view kRenameLabelArg1 = "old_label";
constexpr std::string_view kRenameLabelArg2 = "new_label";
constexpr std::string_view kRenameLabelArg3 = "nodes";
constexpr std::string_view kRenameLabelResult = "nodes_changed";

/* rename_node_property constants */
constexpr std::string_view kProcedureRenameNodeProperty = "rename_node_property";
constexpr std::string_view kRenameNodePropertyArg1 = "old_property";
constexpr std::string_view kRenameNodePropertyArg2 = "new_property";
constexpr std::string_view kRenameNodePropertyArg3 = "nodes";
constexpr std::string_view kRenameNodePropertyResult = "nodes_changed";

/*collapse constants*/
constexpr std::string_view kProcedureCollapseNode = "collapse_node";
constexpr std::string_view kArgumentNodesCollapseNode = "nodes";
constexpr std::string_view kArgumentTypeCollapseNode = "type";
constexpr std::string_view kReturnIdCollapseNode = "id_collapsed";
constexpr std::string_view kReturnRelationshipCollapseNode = "new_relationship";

/*invert constants*/
constexpr std::string_view kProcedureInvert = "invert";
constexpr std::string_view kArgumentRelationship = "relationship";
constexpr std::string_view kResultRelationshipInvert = "output";
constexpr std::string_view kResultIdInvert = "input";
constexpr std::string_view kResultErrorInvert = "error";

/* normalize_as_boolean constants */
constexpr std::string_view kProcedureNormalizeAsBoolean = "normalize_as_boolean";
constexpr std::string_view kNormalizeAsBooleanArg1 = "entity";
constexpr std::string_view kNormalizeAsBooleanArg2 = "property_key";
constexpr std::string_view kNormalizeAsBooleanArg3 = "true_values";
constexpr std::string_view kNormalizeAsBooleanArg4 = "false_values";

/* extract_node constants */
constexpr std::string_view kProcedureExtractNode = "extract_node";
constexpr std::string_view kExtractNodeArg1 = "relationships";
constexpr std::string_view kExtractNodeArg2 = "labels";
constexpr std::string_view kExtractNodeArg3 = "outType";
constexpr std::string_view kExtractNodeArg4 = "inType";
constexpr std::string_view kResultExtractNode1 = "input";
constexpr std::string_view kResultExtractNode2 = "output";
constexpr std::string_view kResultExtractNode3 = "error";

/* delete_and_reconnect constants */
constexpr std::string_view kProcedureDeleteAndReconnect = "delete_and_reconnect";
constexpr std::string_view kDeleteAndReconnectArg1 = "path";
constexpr std::string_view kDeleteAndReconnectArg2 = "nodes";
constexpr std::string_view kDeleteAndReconnectArg3 = "config";
constexpr std::string_view kReturnDeleteAndReconnect1 = "nodes";
constexpr std::string_view kReturnDeleteAndReconnect2 = "relationships";

/* rename_type constants */
constexpr std::string_view kProcedureRenameType = "rename_type";
constexpr std::string_view kRenameTypeArg1 = "oldType";
constexpr std::string_view kRenameTypeArg2 = "newType";
constexpr std::string_view kRenameTypeArg3 = "rels";
constexpr std::string_view kResultRenameType = "relationships_changed";

/* rename_node_property constants */
constexpr std::string_view kProcedureRenameTypeProperty = "rename_type_property";
constexpr std::string_view kRenameTypePropertyArg1 = "old_property";
constexpr std::string_view kRenameTypePropertyArg2 = "new_property";
constexpr std::string_view kRenameTypePropertyArg3 = "rels";
constexpr std::string_view kRenameTypePropertyResult = "relationships_changed";

/* merge_nodes constants */
constexpr std::string_view kProcedureMergeNodes = "merge_nodes";
constexpr std::string_view kMergeNodesArgNodes = "nodes";
constexpr std::string_view kMergeNodesArgConfig = "config";
constexpr std::string_view kMergeNodesResult = "node";
constexpr std::string_view kMergeNodesPropertiesStrategy = "properties";
constexpr std::string_view kMergeNodesPropertiesStrategyAlternative = ".*";
constexpr std::string_view kMergeNodesRelationshipsStrategy = "mergeRels";
constexpr std::string_view kMergeNodesPropertiesCombine = "combine";
constexpr std::string_view kMergeNodesPropertiesDiscard = "discard";
constexpr std::string_view kMergeNodesPropertiesOverride = "override";
constexpr std::string_view kMergeNodesPropertiesOverwrite = "overwrite";
constexpr std::string_view kMergeNodesEmptyListError = "The nodes list cannot be empty";
constexpr std::string_view kMergeNodesInvalidTypeError = "All elements in the nodes list must be of type Node";
constexpr std::string_view kMergeRelationshipsInvalidValueError = "mergeRels parameter must either be true or false";
constexpr std::string_view kMergeNodesInvalidPropertyStrategyError =
    "Property strategy must be one of: combine, discard, override/overwrite";

void From(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void To(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void RenameLabel(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void RenameNodeProperty(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Categorize(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void InsertCloneNodesRecord(mgp_graph *graph, mgp_result *result, mgp_memory *memory, int cycle_id, int node_id);

void CloneNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void CloneNodesAndRels(mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory,
                       const std::vector<mgp::Node> &nodes, const std::vector<mgp::Relationship> &rels,
                       const mgp::Map &config_map);

void CloneSubgraphFromPaths(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void CloneSubgraph(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void TransferProperties(const mgp::Node &node, mgp::Relationship &rel);

void Collapse(mgp::Graph &graph, const mgp::Node &node, const std::string &type,
              const mgp::RecordFactory &record_factory);

void CollapseNode(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Invert(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

mgp::Relationship InvertRel(mgp::Graph &graph, mgp::Relationship &rel);

void DeleteAndReconnect(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void RenameTypeProperty(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void NormalizeAsBoolean(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void ExtractNode(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void RenameType(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void MergeNodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);
}  // namespace Refactor
