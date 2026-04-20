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
struct PointIndexActiveIndices;
struct VectorIndexActiveIndices;
struct VectorEdgeIndexActiveIndices;

struct IndicesCollection {
  std::vector<storage::LabelId> label_;
  std::vector<std::pair<storage::LabelId, std::vector<storage::PropertyPath>>> label_properties_;
  std::vector<storage::EdgeTypeId> edge_type_;
  std::vector<std::pair<storage::EdgeTypeId, storage::PropertyId>> edge_type_properties_;
  std::vector<storage::PropertyId> edge_property_;
};

struct ActiveIndices {
  ActiveIndices() = delete;  // to avoid nullptr

  explicit ActiveIndices(
      std::shared_ptr<LabelIndexActiveIndices> label, std::shared_ptr<LabelPropertyIndexActiveIndices> label_properties,
      std::shared_ptr<EdgeTypeIndexActiveIndices> edge_type,
      std::shared_ptr<EdgeTypePropertyIndexActiveIndices> edge_type_properties,
      std::shared_ptr<EdgePropertyIndexActiveIndices> edge_property, std::shared_ptr<TextIndexActiveIndices> text,
      std::shared_ptr<TextEdgeIndexActiveIndices> text_edge, std::shared_ptr<PointIndexActiveIndices> point,
      std::shared_ptr<VectorIndexActiveIndices> vector, std::shared_ptr<VectorEdgeIndexActiveIndices> vector_edge)
      : label_{std::move(label)},
        label_properties_{std::move(label_properties)},
        edge_type_{std::move(edge_type)},
        edge_type_properties_{std::move(edge_type_properties)},
        edge_property_{std::move(edge_property)},
        text_{std::move(text)},
        text_edge_{std::move(text_edge)},
        point_{std::move(point)},
        vector_{std::move(vector)},
        vector_edge_{std::move(vector_edge)} {}

  // TODO(follow-up): enforce DMG_ASSERT that every sub-index snapshot is
  // non-null in the ctor. Currently blocked because isolated tests (e.g.
  // VectorEdgeIndexRecoveryTest) construct an ActiveIndices with only the
  // sub-index they care about and pass nullptrs for the rest. Fix by adding
  // a shared test helper that default-constructs empty sub-index snapshots
  // (each concrete ActiveIndices supports an empty container), then make
  // non-null a hard invariant here. Until then, the MG_ASSERTs inside
  // ActiveIndicesUpdater::operator() are the safety net.

  /// Returns a new ActiveIndices with one field replaced, identified by
  /// pointer-to-member. Keeps layout knowledge in one place and avoids the
  /// positional-argument foot-gun of the 10-arg ctor.
  ///
  /// Example: auto next = ai.With<&ActiveIndices::text_>(new_text);
  template <auto Member, class X>
  [[nodiscard]] std::shared_ptr<ActiveIndices const> With(X x) const {
    auto next = *this;
    next.*Member = std::move(x);
    return std::make_shared<ActiveIndices>(std::move(next));
  }

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
  std::shared_ptr<PointIndexActiveIndices> point_;
  std::shared_ptr<VectorIndexActiveIndices> vector_;
  std::shared_ptr<VectorEdgeIndexActiveIndices> vector_edge_;
};

using ActiveIndicesPtr = std::shared_ptr<ActiveIndices const>;
using ActiveIndicesStore = utils::Synchronized<ActiveIndicesPtr, utils::WritePrioritizedRWLock>;

}  // namespace memgraph::storage
