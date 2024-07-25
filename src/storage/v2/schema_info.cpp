// Copyright 2024 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include "storage/v2/schema_info.hpp"

namespace memgraph::storage {

void PropertyInfo::MergeStats(PropertyInfo &&info) {
  for (auto &[type, number_of_occurrences] : info.property_types) {
    auto &existing_value = property_types[type];
    existing_value += number_of_occurrences;
  }
  property_types.insert(info.property_types.begin(), info.property_types.end());
  number_of_property_occurrences += info.number_of_property_occurrences;
}

void LabelsInfo::MergeStats(LabelsInfo &&info) {
  number_of_label_occurrences += info.number_of_label_occurrences;
  for (auto &[key, value] : info.properties) {
    auto &existing_value = properties[key];
    existing_value.MergeStats(std::move(value));
  }
}

void NodesInfo::MergeStats(NodesInfo &&info) {
  for (auto &[key, value] : info.node_types_properties) {
    auto &existing_value = node_types_properties[key];
    existing_value.MergeStats(std::move(value));
  }
}

void NodesInfo::AddVertex() {
  node_types_properties[LabelsSet{}].MergeStats(LabelsInfo{.number_of_label_occurrences = 1});
}

void SchemaInfo::MergeStats(SchemaInfo &&info) { nodes.MergeStats(std::move(info.nodes)); }

void SchemaInfo::AddVertex() { nodes.AddVertex(); }

}  // namespace memgraph::storage
