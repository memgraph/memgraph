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

#include <memory>
#include <span>

#include "storage/v2/indices/edge_property_index.hpp"
#include "storage/v2/indices/edge_type_index.hpp"
#include "storage/v2/indices/edge_type_property_index.hpp"
#include "storage/v2/indices/label_index.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/indices/text_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
#include "storage/v2/storage_mode.hpp"
#include "storage/v2/transaction.hpp"

namespace memgraph::storage {

struct Indices {
  Indices(const Config &config, StorageMode storage_mode);

  Indices(const Indices &) = delete;
  Indices(Indices &&) = delete;
  Indices &operator=(const Indices &) = delete;
  Indices &operator=(Indices &&) = delete;
  ~Indices() = default;

  /// This function should be called from garbage collection to clean up the
  /// vertex indices.
  /// TODO: unused in disk indices
  void RemoveObsoleteVertexEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) const;

  /// This function should be called from garbage collection to clean up the
  /// edge indices.
  /// TODO: unused in disk indices
  void RemoveObsoleteEdgeEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) const;

  /// Surgical removal of entries that were inserted in this transaction
  /// TODO: unused in disk indices
  void AbortEntries(std::pair<EdgeTypeId, PropertyId> edge_type_property,
                    std::span<std::tuple<Vertex *const, Vertex *const, Edge *const, PropertyValue> const> edges,
                    uint64_t exact_start_timestamp) const;

  void DropGraphClearIndices();

  struct AbortProcessor {
    LabelIndex::AbortProcessor label_;
    LabelPropertyIndex::AbortProcessor property_label_;
    EdgeTypeIndex::AbortProcessor edge_type_;

    EdgeTypePropertyIndex::IndexStats property_edge_type_;
    EdgePropertyIndex::IndexStats property_edge_;
    // TODO: point? Nothing to abort, it gets build in Commit
    // TODO: text?
    VectorIndex::IndexStats vector_;

    void CollectOnEdgeRemoval(EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex, Edge *edge);
    void CollectOnLabelRemoval(LabelId labelId, Vertex *vertex);
    void CollectOnPropertyChange(PropertyId propId, Vertex *vertex);
    void Process(Indices &indices, Transaction &tx);
  };

  auto GetAbortProcessor() const -> AbortProcessor;

  // Indices are updated whenever an update occurs, instead of only on commit or
  // advance command. This is necessary because we want indices to support `NEW`
  // view for use in Merge.

  /// This function should be called whenever a label is added to a vertex.
  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, Transaction &tx) const;

  void UpdateOnRemoveLabel(LabelId label, Vertex *vertex, const Transaction &tx) const;

  /// This function should be called whenever a property is modified on a vertex.
  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex, Transaction &tx) const;

  /// This function should be called whenever a property is modified on an edge.
  /// @throw std::bad_alloc
  void UpdateOnSetProperty(EdgeTypeId edge_type, PropertyId property, const PropertyValue &value, Vertex *from_vertex,
                           Vertex *to_vertex, Edge *edge, const Transaction &tx) const;

  void UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref, EdgeTypeId edge_type,
                            const Transaction &tx) const;

  std::unique_ptr<LabelIndex> label_index_;
  std::unique_ptr<LabelPropertyIndex> label_property_index_;
  std::unique_ptr<EdgeTypeIndex> edge_type_index_;
  std::unique_ptr<EdgeTypePropertyIndex> edge_type_property_index_;
  std::unique_ptr<EdgePropertyIndex> edge_property_index_;
  mutable TextIndex text_index_;
  PointIndexStorage point_index_;
  mutable VectorIndex vector_index_;
};

}  // namespace memgraph::storage
