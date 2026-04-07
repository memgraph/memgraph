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

#include "storage/v2/indices/edge_property_index.hpp"
#include "storage/v2/indices/edge_type_index.hpp"
#include "storage/v2/indices/edge_type_property_index.hpp"
#include "storage/v2/indices/label_index.hpp"
#include "storage/v2/indices/label_property_index.hpp"

#include <memory>
#include <vector>

#include "utils/rw_lock.hpp"
#include "utils/synchronized.hpp"

namespace memgraph::storage {

struct TextIndexActiveIndices;
struct TextEdgeIndexActiveIndices;

struct IndicesCollection {
  std::vector<storage::LabelId> label_;
  std::vector<std::pair<storage::LabelId, std::vector<storage::PropertyPath>>> label_properties_;
  std::vector<storage::EdgeTypeId> edge_type_;
  std::vector<std::pair<storage::EdgeTypeId, storage::PropertyId>> edge_type_properties_;
  std::vector<storage::PropertyId> edge_property_;
};

struct ActiveIndices {
  ActiveIndices() = delete;  // to avoid nullptr

  explicit ActiveIndices(std::shared_ptr<LabelIndexActiveIndices> label,
                         std::shared_ptr<LabelPropertyIndexActiveIndices> label_properties,
                         std::shared_ptr<EdgeTypeIndexActiveIndices> edge_type,
                         std::shared_ptr<EdgeTypePropertyIndexActiveIndices> edge_type_properties,
                         std::shared_ptr<EdgePropertyIndexActiveIndices> edge_property,
                         std::shared_ptr<TextIndexActiveIndices> text,
                         std::shared_ptr<TextEdgeIndexActiveIndices> text_edge)
      : label_{std::move(label)},
        label_properties_{std::move(label_properties)},
        edge_type_{std::move(edge_type)},
        edge_type_properties_{std::move(edge_type_properties)},
        edge_property_{std::move(edge_property)},
        text_{std::move(text)},
        text_edge_{std::move(text_edge)} {}

  /// Factory methods that return a new ActiveIndices with one field replaced.
  /// Keeps layout knowledge in one place and eliminates positional argument mistakes.
  [[nodiscard]] std::shared_ptr<ActiveIndices const> WithLabel(std::shared_ptr<LabelIndexActiveIndices> x) const;
  [[nodiscard]] std::shared_ptr<ActiveIndices const> WithLabelProperties(
      std::shared_ptr<LabelPropertyIndexActiveIndices> x) const;
  [[nodiscard]] std::shared_ptr<ActiveIndices const> WithEdgeType(std::shared_ptr<EdgeTypeIndexActiveIndices> x) const;
  [[nodiscard]] std::shared_ptr<ActiveIndices const> WithEdgeTypeProperties(
      std::shared_ptr<EdgeTypePropertyIndexActiveIndices> x) const;
  [[nodiscard]] std::shared_ptr<ActiveIndices const> WithEdgeProperty(
      std::shared_ptr<EdgePropertyIndexActiveIndices> x) const;
  [[nodiscard]] std::shared_ptr<ActiveIndices const> WithText(std::shared_ptr<TextIndexActiveIndices> x) const;
  [[nodiscard]] std::shared_ptr<ActiveIndices const> WithTextEdge(std::shared_ptr<TextEdgeIndexActiveIndices> x) const;

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

    // edge property
    for (auto const property : required_indices.edge_property_) {
      if (!edge_property_->IndexReady(property)) return false;
    }

    return true;
  }

  std::shared_ptr<LabelIndexActiveIndices> label_;
  std::shared_ptr<LabelPropertyIndexActiveIndices> label_properties_;
  std::shared_ptr<EdgeTypeIndexActiveIndices> edge_type_;
  std::shared_ptr<EdgeTypePropertyIndexActiveIndices> edge_type_properties_;
  std::shared_ptr<EdgePropertyIndexActiveIndices> edge_property_;
  std::shared_ptr<TextIndexActiveIndices> text_;
  std::shared_ptr<TextEdgeIndexActiveIndices> text_edge_;
};

using ActiveIndicesPtr = std::shared_ptr<ActiveIndices const>;
using ActiveIndicesStore = utils::Synchronized<ActiveIndicesPtr, utils::WritePrioritizedRWLock>;

}  // namespace memgraph::storage
