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
#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"

namespace memgraph::storage {

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
  text_index_.Clear();
  text_edge_index_.Clear();
}

void Indices::UpdateOnAddLabel(LabelId label, Vertex *vertex, Transaction &tx) {
  tx.active_indices_.label_->UpdateOnAddLabel(label, vertex, tx);
  tx.active_indices_.label_properties_->UpdateOnAddLabel(label, vertex, tx);
  text_index_.UpdateOnAddLabel(label, vertex, tx);
}

void Indices::UpdateOnRemoveLabel(LabelId label, Vertex *vertex, Transaction &tx) {
  tx.active_indices_.label_->UpdateOnRemoveLabel(label, vertex, tx);
  tx.active_indices_.label_properties_->UpdateOnRemoveLabel(label, vertex, tx);
  text_index_.UpdateOnRemoveLabel(label, vertex, tx);
}

void Indices::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex, Transaction &tx,
                                  NameIdMapper *name_id_mapper) {
  tx.active_indices_.label_properties_->UpdateOnSetProperty(property, value, vertex, tx);
  // vector_index_.UpdateOnSetProperty(value, vertex, name_id_mapper);
  text_index_.UpdateOnSetProperty(vertex, tx, property);
}

void Indices::UpdateOnSetProperty(EdgeTypeId edge_type, PropertyId property, const PropertyValue &value,
                                  Vertex *from_vertex, Vertex *to_vertex, Edge *edge, Transaction &tx) {
  tx.active_indices_.edge_type_properties_->UpdateOnSetProperty(from_vertex, to_vertex, edge, edge_type, property,
                                                                value, tx.start_timestamp);
  tx.active_indices_.edge_property_->UpdateOnSetProperty(from_vertex, to_vertex, edge, edge_type, property, value,
                                                         tx.start_timestamp);
  vector_edge_index_.UpdateOnSetProperty(from_vertex, to_vertex, edge, edge_type, property, value);
  text_edge_index_.UpdateOnSetProperty(edge, from_vertex, to_vertex, edge_type, tx, property);
}

void Indices::UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref, EdgeTypeId edge_type,
                                   const Transaction &tx) {
  tx.active_indices_.edge_type_->UpdateOnEdgeCreation(from, to, edge_ref, edge_type, tx);
}

Indices::Indices(const Config &config, StorageMode storage_mode)
    : text_index_(config.durability.storage_directory), text_edge_index_(config.durability.storage_directory) {
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
  return AbortProcessor{.label_ = active_indices.label_->GetAbortProcessor(),
                        .label_properties_ = active_indices.label_properties_->GetAbortProcessor(),
                        .edge_type_ = active_indices.edge_type_->GetAbortProcessor(),
                        .edge_type_property_ = active_indices.edge_type_properties_->GetAbortProcessor(),
                        .edge_property_ = active_indices.edge_property_->GetAbortProcessor(),
                        .vector_ = vector_index_.GetAbortProcessor(),
                        .vector_edge_ = vector_edge_index_.Analysis()};
}

void Indices::AbortProcessor::CollectOnEdgeRemoval(EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex,
                                                   Edge *edge) {
  edge_type_.CollectOnEdgeRemoval(edge_type, from_vertex, to_vertex, edge);
}

void Indices::AbortProcessor::CollectOnLabelRemoval(LabelId labelId, Vertex *vertex) {
  label_.CollectOnLabelRemoval(labelId, vertex);
  label_properties_.CollectOnLabelRemoval(labelId, vertex);
  vector_.CollectOnLabelRemoval(labelId, vertex);
}

void Indices::AbortProcessor::CollectOnLabelAddition(LabelId labelId, Vertex *vertex) {
  vector_.CollectOnLabelAddition(labelId, vertex);
}
void Indices::AbortProcessor::CollectOnPropertyChange(PropertyId propId, const PropertyValue &old_value,
                                                      Vertex *vertex) {
  label_properties_.CollectOnPropertyChange(propId, vertex);
  vector_.CollectOnPropertyChange(propId, old_value, vertex);
}

void Indices::AbortProcessor::CollectOnPropertyChange(EdgeTypeId edge_type, PropertyId property, Vertex *from_vertex,
                                                      Vertex *to_vertex, Edge *edge) {
  auto const etp_interesting = edge_type_property_.IsInteresting(edge_type, property);
  auto const ep_interesting = edge_property_.IsInteresting(property);
  if (etp_interesting || ep_interesting) {
    // extract
    auto value = edge->properties.GetProperty(property);
    if (value.IsNull()) return;

    if (etp_interesting) {
      edge_type_property_.CollectOnPropertyChange(edge_type, property, from_vertex, to_vertex, edge, value);
    }
    if (ep_interesting) {
      edge_property_.CollectOnPropertyChange(edge_type, property, from_vertex, to_vertex, edge, std::move(value));
    }
  }
}

bool Indices::AbortProcessor::IsInterestingEdgeProperty(PropertyId property) {
  return edge_type_property_.IsInteresting(property) || edge_property_.IsInteresting(property);
}

void Indices::AbortProcessor::Process(Indices &indices, ActiveIndices &active_indices, uint64_t start_timestamp,
                                      NameIdMapper *name_id_mapper) {
  active_indices.label_->AbortEntries(label_.cleanup_collection_, start_timestamp);
  active_indices.label_properties_->AbortEntries(label_properties_.cleanup_collection, start_timestamp);
  active_indices.edge_type_->AbortEntries(edge_type_.cleanup_collection_, start_timestamp);
  active_indices.edge_type_properties_->AbortEntries(edge_type_property_.cleanup_collection_, start_timestamp);
  active_indices.edge_property_->AbortEntries(edge_property_.cleanup_collection_, start_timestamp);
  indices.vector_index_.AbortEntries(name_id_mapper, vector_.cleanup_collection);
}
}  // namespace memgraph::storage
