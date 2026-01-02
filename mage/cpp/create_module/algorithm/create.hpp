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

#include <unordered_set>

#include <mgp.hpp>

namespace Create {

/* remove_rel_properties constants */
constexpr std::string_view kProcedureRemoveRelProperties = "remove_rel_properties";
constexpr std::string_view kRemoveRelPropertiesArg1 = "relationships";
constexpr std::string_view kRemoveRelPropertiesArg2 = "keys";
constexpr std::string_view kResultRemoveRelProperties = "relationship";

/* set_rel_properties constants */
constexpr std::string_view kProcedureSetRelProperties = "set_rel_properties";
constexpr std::string_view kSetRelPropertiesArg1 = "relationships";
constexpr std::string_view kSetRelPropertiesArg2 = "keys";
constexpr std::string_view kSetRelPropertiesArg3 = "values";
constexpr std::string_view kResultSetRelProperties = "relationship";

/* relationship constants */
constexpr std::string_view kProcedureRelationship = "relationship";
constexpr std::string_view kRelationshipArg1 = "from";
constexpr std::string_view kRelationshipArg2 = "relationshipType";
constexpr std::string_view kRelationshipArg3 = "properties";
constexpr std::string_view kRelationshipArg4 = "to";
constexpr std::string_view kResultRelationship = "relationship";

/*nodes constants*/
constexpr std::string_view kProcedureNodes = "nodes";
constexpr std::string_view kArgumentLabelsNodes = "label";
constexpr std::string_view kArgumentPropertiesNodes = "props";
constexpr std::string_view kReturnNodes = "node";

/*set_property constants*/
constexpr std::string_view kProcedureSetProperty = "set_property";
constexpr std::string_view kArgumentNodeSetProperty = "nodes";
constexpr std::string_view kArgumentKeySetProperty = "key";
constexpr std::string_view kArgumentValueSetProperty = "value";
constexpr std::string_view kReturntSetProperty = "node";

/*remove_properties constants*/
constexpr std::string_view kProcedureRemoveProperties = "remove_properties";
constexpr std::string_view kArgumentNodeRemoveProperties = "nodes";
constexpr std::string_view kArgumentKeysRemoveProperties = "keys";
constexpr std::string_view kReturntRemoveProperties = "node";

/*node constants*/
constexpr std::string_view kReturnNode = "node";
constexpr std::string_view kProcedureNode = "node";
constexpr std::string_view kArgumentsLabelsList = "label";
constexpr std::string_view kArgumentsProperties = "props";
constexpr std::string_view kResultNode = "node";

/*set_properties constants*/
constexpr std::string_view kReturnProperties = "node";
constexpr std::string_view kProcedureSetProperties = "set_properties";
constexpr std::string_view kArgumentsNodes = "input_nodes";
constexpr std::string_view kArgumentsKeys = "input_keys";
constexpr std::string_view kArgumentsValues = "input_values";
constexpr std::string_view kResultProperties = "node";

/*remove_labels constants*/
constexpr std::string_view kReturnRemoveLabels = "node";
constexpr std::string_view kProcedureRemoveLabels = "remove_labels";
constexpr std::string_view kArgumentNodesRemoveLabels = "nodes";
constexpr std::string_view kArgumentsLabels = "label";
constexpr std::string_view kResultRemoveLabels = "node";

/*set_rel_properties constants*/
constexpr std::string_view kReturnRelProp = "relationship";
constexpr std::string_view kProcedureSetRelProp = "set_rel_property";
constexpr std::string_view kArgumentsRelationship = "input_rel";
constexpr std::string_view kArgumentsKey = "input_key";
constexpr std::string_view kArgumentsValue = "input_value";
constexpr std::string_view kResultRelProp = "relationship";

void RemoveRelProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void SetRelProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Relationship(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void SetProperty(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void RemoveElementProperties(mgp::Node &element, const mgp::List &labels, const mgp::RecordFactory &record_factory);

void RemoveProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Nodes(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void Node(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void SetElementProp(mgp::Node &element, const mgp::List &prop_key_list, const mgp::List &prop_value_list,
                    const mgp::RecordFactory &record_factory);

void ProcessElement(const mgp::Value &element, const mgp::Graph graph, const mgp::List &prop_key_list,
                    const mgp::List &prop_value_list, const mgp::RecordFactory &record_factory);

void SetProperties(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void RemoveElementLabels(mgp::Node &element, const mgp::List &labels, const mgp::RecordFactory &record_factory);

void ProcessElement(const mgp::Value &element, const mgp::Graph graph, const mgp::List &labels,
                    const bool labels_or_props, const mgp::RecordFactory &record_factory);

void RemoveLabels(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

void SetElementProp(mgp::Relationship &element, const mgp::List &prop_key_list, const mgp::List &prop_value_list,
                    const mgp::RecordFactory &record_factory);

void ProcessElement(const mgp::Value &element, const mgp::Graph graph, const mgp::List &prop_key_list,
                    const mgp::List &prop_value_list, const mgp::RecordFactory &record_factory,
                    std::unordered_set<mgp::Id> &relIds);

void SetRelProperty(mgp_list *args, mgp_graph *memgraph_graph, mgp_result *result, mgp_memory *memory);

}  // namespace Create
