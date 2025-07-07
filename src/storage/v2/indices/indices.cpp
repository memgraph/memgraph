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

#include "storage/v2/indices/indices.hpp"
#include "flags/experimental.hpp"
#include "storage/v2/disk/edge_property_index.hpp"
#include "storage/v2/disk/edge_type_index.hpp"
#include "storage/v2/disk/edge_type_property_index.hpp"
#include "storage/v2/disk/label_index.hpp"
#include "storage/v2/disk/label_property_index.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/inmemory/edge_property_index.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/edge_type_property_index.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::storage {

void Indices::AbortEntries(std::pair<EdgeTypeId, PropertyId> edge_type_property,
                           std::span<std::tuple<Vertex *const, Vertex *const, Edge *const, PropertyValue> const> edges,
                           uint64_t exact_start_timestamp) const {
  static_cast<InMemoryEdgeTypePropertyIndex *>(edge_type_property_index_.get())
      ->AbortEntries(edge_type_property, edges, exact_start_timestamp);
  static_cast<InMemoryEdgePropertyIndex *>(edge_property_index_.get())
      ->AbortEntries(edge_type_property, edges, exact_start_timestamp);
}

void Indices::RemoveObsoleteVertexEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) const {
  static_cast<InMemoryLabelIndex *>(label_index_.get())->RemoveObsoleteEntries(oldest_active_start_timestamp, token);
  static_cast<InMemoryLabelPropertyIndex *>(label_property_index_.get())
      ->RemoveObsoleteEntries(oldest_active_start_timestamp, token);
  vector_index_.RemoveObsoleteEntries(token);
}

void Indices::RemoveObsoleteEdgeEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) const {
  static_cast<InMemoryEdgeTypeIndex *>(edge_type_index_.get())
      ->RemoveObsoleteEntries(oldest_active_start_timestamp, token);
  static_cast<InMemoryEdgeTypePropertyIndex *>(edge_type_property_index_.get())
      ->RemoveObsoleteEntries(oldest_active_start_timestamp, token);
  static_cast<InMemoryEdgePropertyIndex *>(edge_property_index_.get())
      ->RemoveObsoleteEntries(oldest_active_start_timestamp, token);
  vector_edge_index_.RemoveObsoleteEntries(token);
}

void Indices::DropGraphClearIndices() {
  static_cast<InMemoryLabelIndex *>(label_index_.get())->DropGraphClearIndices();
  static_cast<InMemoryLabelPropertyIndex *>(label_property_index_.get())->DropGraphClearIndices();
  static_cast<InMemoryEdgeTypeIndex *>(edge_type_index_.get())->DropGraphClearIndices();
  static_cast<InMemoryEdgeTypePropertyIndex *>(edge_type_property_index_.get())->DropGraphClearIndices();
  static_cast<InMemoryEdgePropertyIndex *>(edge_property_index_.get())->DropGraphClearIndices();
  point_index_.Clear();
  vector_index_.Clear();
  vector_edge_index_.Clear();
  if (flags::AreExperimentsEnabled(flags::Experiments::TEXT_SEARCH)) {
    text_index_.Clear();
  }
}

void Indices::UpdateOnAddLabel(LabelId label, Vertex *vertex, const Transaction &tx) const {
  tx.active_indices_.label_->UpdateOnAddLabel(label, vertex, tx);
  tx.active_indices_.label_properties_->UpdateOnAddLabel(label, vertex, tx);
  vector_index_.UpdateOnAddLabel(label, vertex);
}

void Indices::UpdateOnRemoveLabel(LabelId label, Vertex *vertex, const Transaction &tx) const {
  tx.active_indices_.label_->UpdateOnRemoveLabel(label, vertex, tx);
  tx.active_indices_.label_properties_->UpdateOnRemoveLabel(label, vertex, tx);
  vector_index_.UpdateOnRemoveLabel(label, vertex);
}

void Indices::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                  const Transaction &tx) const {
  tx.active_indices_.label_properties_->UpdateOnSetProperty(property, value, vertex, tx);
  vector_index_.UpdateOnSetProperty(property, value, vertex);
}

void Indices::UpdateOnSetProperty(EdgeTypeId edge_type, PropertyId property, const PropertyValue &value,
                                  Vertex *from_vertex, Vertex *to_vertex, Edge *edge, const Transaction &tx) const {
  edge_type_property_index_->UpdateOnSetProperty(from_vertex, to_vertex, edge, edge_type, property, value,
                                                 tx.start_timestamp);
  edge_property_index_->UpdateOnSetProperty(from_vertex, to_vertex, edge, edge_type, property, value,
                                            tx.start_timestamp);
  vector_edge_index_.UpdateOnSetProperty(from_vertex, to_vertex, edge, edge_type, property, value);
}

void Indices::UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref, EdgeTypeId edge_type,
                                   const Transaction &tx) const {
  tx.active_indices_.edge_type_->UpdateOnEdgeCreation(from, to, edge_ref, edge_type, tx);
}

Indices::Indices(const Config &config, StorageMode storage_mode) : text_index_(config.durability.storage_directory) {
  std::invoke([this, config, storage_mode]() {
    if (storage_mode == StorageMode::IN_MEMORY_TRANSACTIONAL || storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
      label_index_ = std::make_unique<InMemoryLabelIndex>();
      label_property_index_ = std::make_unique<InMemoryLabelPropertyIndex>();
      edge_type_index_ = std::make_unique<InMemoryEdgeTypeIndex>();
      edge_type_property_index_ = std::make_unique<InMemoryEdgeTypePropertyIndex>();
      edge_property_index_ = std::make_unique<InMemoryEdgePropertyIndex>();
    } else {
      label_index_ = std::make_unique<DiskLabelIndex>(config);
      label_property_index_ = std::make_unique<DiskLabelPropertyIndex>(config);
      edge_type_index_ = std::make_unique<DiskEdgeTypeIndex>();
      edge_type_property_index_ = std::make_unique<DiskEdgeTypePropertyIndex>();
      edge_property_index_ = std::make_unique<DiskEdgePropertyIndex>();
    }
  });
}

Indices::AbortProcessor Indices::GetAbortProcessor(ActiveIndices const &active_indices) const {
  return {active_indices.label_->GetAbortProcessor(),
          active_indices.label_properties_->GetAbortProcessor(),
          active_indices.edge_type_->GetAbortProcessor(),
          static_cast<InMemoryEdgeTypePropertyIndex *>(edge_type_property_index_.get())->Analysis(),
          static_cast<InMemoryEdgePropertyIndex *>(edge_property_index_.get())->Analysis(),
          vector_index_.Analysis(),
          vector_edge_index_.Analysis()};
}

void Indices::AbortProcessor::CollectOnEdgeRemoval(EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex,
                                                   Edge *edge) {
  edge_type_.CollectOnEdgeRemoval(edge_type, from_vertex, to_vertex, edge);
}

void Indices::AbortProcessor::CollectOnLabelRemoval(LabelId labelId, Vertex *vertex) {
  label_.CollectOnLabelRemoval(labelId, vertex);
  label_properties_.CollectOnLabelRemoval(labelId, vertex);
}

void Indices::AbortProcessor::CollectOnPropertyChange(PropertyId propId, Vertex *vertex) {
  label_properties_.CollectOnPropertyChange(propId, vertex);
}

void Indices::AbortProcessor::Process(Indices &indices, ActiveIndices &active_indices, uint64_t start_timestamp) {
  active_indices.label_->AbortEntries(label_.cleanup_collection_, start_timestamp);
  active_indices.label_properties_->AbortEntries(label_properties_.cleanup_collection, start_timestamp);
  active_indices.edge_type_->AbortEntries(edge_type_.cleanup_collection_, start_timestamp);
  // TODO: edge type properties
}
}  // namespace memgraph::storage
