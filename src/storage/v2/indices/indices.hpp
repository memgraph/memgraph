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

#pragma once

#include <memory>
#include <span>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/edge_type_index.hpp"
#include "storage/v2/indices/edge_type_property_index.hpp"
#include "storage/v2/indices/label_index.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/text_index.hpp"
#include "storage/v2/storage_mode.hpp"

namespace memgraph::storage {

struct Indices {
  Indices(const Config &config, StorageMode storage_mode);

  Indices(const Indices &) = delete;
  Indices(Indices &&) = delete;
  Indices &operator=(const Indices &) = delete;
  Indices &operator=(Indices &&) = delete;
  ~Indices() = default;

  /// This function should be called from garbage collection to clean up the
  /// index.
  /// TODO: unused in disk indices
  void RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) const;

  /// Surgical removal of entries that were inserted in this transaction
  /// TODO: unused in disk indices
  void AbortEntries(LabelId labelId, std::span<Vertex *const> vertices, uint64_t exact_start_timestamp) const;
  void AbortEntries(PropertyId property, std::span<std::pair<PropertyValue, Vertex *> const> vertices,
                    uint64_t exact_start_timestamp) const;
  void AbortEntries(LabelId label, std::span<std::pair<PropertyValue, Vertex *> const> vertices,
                    uint64_t exact_start_timestamp) const;

  void DropGraphClearIndices() const;

  struct IndexStats {
    std::vector<LabelId> label;
    LabelPropertyIndex::IndexStats property_label;
  };
  IndexStats Analysis() const;

  // Indices are updated whenever an update occurs, instead of only on commit or
  // advance command. This is necessary because we want indices to support `NEW`
  // view for use in Merge.

  /// This function should be called whenever a label is added to a vertex.
  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) const;

  void UpdateOnRemoveLabel(LabelId label, Vertex *vertex, const Transaction &tx) const;

  /// This function should be called whenever a property is modified on a vertex.
  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                           const Transaction &tx) const;

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
  mutable TextIndex text_index_;
};

}  // namespace memgraph::storage
