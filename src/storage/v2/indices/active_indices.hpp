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

#include "storage/v2/indices/edge_type_index.hpp"
#include "storage/v2/indices/edge_type_property_index.hpp"
#include "storage/v2/indices/label_index.hpp"
#include "storage/v2/indices/label_property_index.hpp"

#include <memory>
#include <vector>

namespace memgraph::storage {

struct IndicesCollection {
  std::vector<storage::LabelId> label_;
  std::vector<std::pair<storage::LabelId, std::vector<storage::PropertyPath>>> label_properties_;
  std::vector<storage::EdgeTypeId> edge_type_;
  std::vector<std::pair<storage::EdgeTypeId, storage::PropertyId>> edge_type_properties_;
  std::vector<storage::PropertyId> edge_property_;
};

struct ActiveIndices {
  ActiveIndices() = delete;  // to avoid nullptr
  explicit ActiveIndices(std::unique_ptr<LabelIndex::ActiveIndices> label,
                         std::unique_ptr<LabelPropertyIndex::ActiveIndices> label_properties,
                         std::unique_ptr<EdgeTypeIndex::ActiveIndices> edge_type,
                         std::unique_ptr<EdgeTypePropertyIndex::ActiveIndices> edge_type_properties)
      : label_{std::move(label)},
        label_properties_{std::move(label_properties)},
        edge_type_{std::move(edge_type)},
        edge_type_properties_(std::move(edge_type_properties)) {}

  bool CheckIndicesAreReady(IndicesCollection const &required_indices) const {
    // label
    for ([[maybe_unused]] auto const &label : required_indices.label_) {
      if (!label_->IndexReady(label)) return false;
    }

    // label + properties
    for (auto const &[label, properties] : required_indices.label_properties_) {
      if (!label_properties_->IndexReady(label, properties)) return false;
    }

    // edge type
    for (auto const edge_type : required_indices.edge_type_) {
      if (!edge_type_->IndexReady(edge_type)) return false;
    }

    // edge type + property
    for (auto const &[edge_type, property] : required_indices.edge_type_properties_) {
      if (!edge_type_properties_->IndexReady(edge_type, property)) return false;
    }

    return true;
  }

  std::unique_ptr<LabelIndex::ActiveIndices> label_;
  std::unique_ptr<LabelPropertyIndex::ActiveIndices> label_properties_;
  std::unique_ptr<EdgeTypeIndex::ActiveIndices> edge_type_;
  std::unique_ptr<EdgeTypePropertyIndex::ActiveIndices> edge_type_properties_;
};
}  // namespace memgraph::storage
