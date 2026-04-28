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

#include <memory>

#include "storage/v2/indices/active_indices.hpp"
#include "storage/v2/indices/active_indices_updater.hpp"
#include "storage/v2/indices/edge_property_index.hpp"
#include "storage/v2/indices/edge_type_index.hpp"
#include "storage/v2/indices/edge_type_property_index.hpp"
#include "storage/v2/indices/label_index.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/indices/point_index.hpp"
#include "storage/v2/indices/text_edge_index.hpp"
#include "storage/v2/indices/text_index.hpp"
#include "storage/v2/indices/vector_edge_index.hpp"
#include "storage/v2/indices/vector_index.hpp"
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
  /// vertex indices.
  /// TODO: unused in disk indices
  void RemoveObsoleteVertexEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) const;

  /// This function should be called from garbage collection to clean up the
  /// edge indices.
  /// TODO: unused in disk indices
  void RemoveObsoleteEdgeEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) const;

  void DropGraphClearIndices();

  /// Removes vertices from all vector indices. Must be called before
  /// the vertex is removed from the skip list (while the pointer is still valid).
  void RemoveVerticesFromVectorIndices(std::vector<Vertex *> const &vertices_to_remove) const;

  /// Removes edges from all vector edge indices. Must be called before
  /// the edge is removed from the skip list (while the pointer is still valid).
  void RemoveEdgesFromVectorEdgeIndices(std::vector<Edge *> const &edges_to_remove);

  struct AbortProcessor {
    LabelIndex::AbortProcessor label_;
    LabelPropertyIndex::AbortProcessor label_properties_;
    EdgeTypeIndex::AbortProcessor edge_type_;
    EdgeTypePropertyIndex::AbortProcessor edge_type_property_;
    EdgePropertyIndex::AbortProcessor edge_property_;
    // TODO: point? Nothing to abort, it gets built in Commit
    // TODO: text?
    VectorIndex::AbortProcessor vector_;
    VectorEdgeIndex::AbortProcessor vector_edge_;

    void CollectOnEdgeRemoval(EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex, Edge *edge);
    void CollectOnLabelRemoval(LabelId labelId, Vertex *vertex);
    void CollectOnLabelAddition(LabelId labelId, Vertex *vertex);
    void CollectOnPropertyChange(PropertyId propId, const PropertyValue &old_value, Vertex *vertex);
    void CollectOnPropertyChange(EdgeTypeId edge_type, PropertyId property, Vertex *from_vertex, Vertex *to_vertex,
                                 Edge *edge);
    bool IsInterestingEdgeProperty(PropertyId property) const;

    void Process(Indices &indices, ActiveIndices const &active_indices, uint64_t start_timestamp,
                 NameIdMapper *name_id_mapper);
  };

  auto GetAbortProcessor(ActiveIndices const &active_indices) const -> AbortProcessor;

  // Indices are updated whenever an update occurs, instead of only on commit or
  // advance command. This is necessary because we want indices to support `NEW`
  // view for use in Merge.

  /// This function should be called whenever a label is added to a vertex.
  /// @throw std::bad_alloc
  void UpdateOnAddLabel(LabelId label, Vertex *vertex, Transaction &tx, NameIdMapper *name_id_mapper);

  /// This function should be called whenever a label is removed from a vertex.
  /// @throw std::bad_alloc
  void UpdateOnRemoveLabel(LabelId label, Vertex *vertex, Transaction &tx, NameIdMapper *name_id_mapper);

  /// This function should be called whenever a property is modified on a vertex.
  /// @param old_value The value prior to the write. Used in IN_MEMORY_ANALYTICAL to
  ///                  eagerly reclaim the stale label+property skiplist entry, since
  ///                  there is no MVCC reader that could still observe it.
  /// @throw std::bad_alloc
  void UpdateOnSetProperty(PropertyId property, const PropertyValue &old_value, const PropertyValue &new_value,
                           Vertex *vertex, Transaction &tx);

  /// This function should be called whenever a property is modified on an edge.
  /// @throw std::bad_alloc
  void UpdateOnSetProperty(EdgeTypeId edge_type, PropertyId property, const PropertyValue &value, Vertex *from_vertex,
                           Vertex *to_vertex, Edge *edge, Transaction &tx);

  static void UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref, EdgeTypeId edge_type,
                                   const Transaction &tx);

  std::unique_ptr<LabelIndex> label_index_;
  std::unique_ptr<LabelPropertyIndex> label_property_index_;
  std::unique_ptr<EdgeTypeIndex> edge_type_index_;
  std::unique_ptr<EdgeTypePropertyIndex> edge_type_property_index_;
  std::unique_ptr<EdgePropertyIndex> edge_property_index_;
  /// Centralized snapshot of active indices, shared by transactions via shared_ptr.
  /// Lock ordering:
  ///   - engine_lock_ → active_indices_.WithReadLock (in CreateTransaction)
  ///   - individual index lock (e.g. index_.WithLock) → active_indices_.WithLock (in RegisterIndex/DropIndex)
  /// The ActiveIndicesUpdater is always called from within an individual index lock
  /// (exception: DiskStorage CreateIndex, which is serialized via UNIQUE access mode).
  ActiveIndicesStore active_indices_;

  /// Factory method to create an updater bound to this Indices' active_indices_ store.
  ActiveIndicesUpdater MakeUpdater() { return ActiveIndicesUpdater{active_indices_}; }

  TextIndex text_index_;
  TextEdgeIndex text_edge_index_;
  PointIndexStorage point_index_;
  VectorIndex vector_index_;
  VectorEdgeIndex vector_edge_index_;
};

}  // namespace memgraph::storage
