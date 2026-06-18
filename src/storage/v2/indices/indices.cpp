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

#include "storage/v2/indices/indices.hpp"

#include "storage/v2/delta_container.hpp"
#include "storage/v2/disk/edge_property_index.hpp"
#include "storage/v2/disk/edge_type_index.hpp"
#include "storage/v2/disk/edge_type_property_index.hpp"
#include "storage/v2/disk/label_index.hpp"
#include "storage/v2/disk/label_property_index.hpp"
#include "storage/v2/disk/vertex_property_index.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indexed_property_decoder.hpp"
#include "storage/v2/inmemory/edge_property_index.hpp"
#include "storage/v2/inmemory/edge_type_index.hpp"
#include "storage/v2/inmemory/edge_type_property_index.hpp"
#include "storage/v2/inmemory/label_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/vertex_property_index.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/transaction.hpp"

namespace memgraph::storage {

uint64_t Indices::RemoveObsoleteVertexEntries(Storage *storage, uint64_t oldest_active_start_timestamp,
                                              std::stop_token token, IndexArming const &arming) const {
  auto swept = static_cast<InMemoryLabelIndex *>(label_index_.get())
                   ->RemoveObsoleteEntries(storage, oldest_active_start_timestamp, token, arming);
  swept += static_cast<InMemoryLabelPropertyIndex *>(label_property_index_.get())
               ->RemoveObsoleteEntries(storage, oldest_active_start_timestamp, token, arming);
  swept += static_cast<InMemoryVertexPropertyIndex *>(vertex_property_index_.get())
               ->RemoveObsoleteEntries(storage, oldest_active_start_timestamp, token, arming);
  return swept;
}

uint64_t Indices::RemoveObsoleteEdgeEntries(Storage *storage, uint64_t oldest_active_start_timestamp,
                                            std::stop_token token, IndexArming const &arming) const {
  auto swept = static_cast<InMemoryEdgeTypeIndex *>(edge_type_index_.get())
                   ->RemoveObsoleteEntries(storage, oldest_active_start_timestamp, token, arming);
  swept += static_cast<InMemoryEdgeTypePropertyIndex *>(edge_type_property_index_.get())
               ->RemoveObsoleteEntries(storage, oldest_active_start_timestamp, token, arming);
  swept += static_cast<InMemoryEdgePropertyIndex *>(edge_property_index_.get())
               ->RemoveObsoleteEntries(storage, oldest_active_start_timestamp, token, arming);
  return swept;
}

void Indices::RemoveVerticesFromVectorIndices(std::vector<Vertex *> const &vertices_to_remove) const {
  vector_index_.RemoveVertices(vertices_to_remove);
}

void Indices::RemoveEdgesFromVectorEdgeIndices(std::span<Edge *const> edges_to_remove) const {
  vector_edge_index_.RemoveEdges(edges_to_remove);
}

void Indices::DropGraphClearIndices() {
  static_cast<InMemoryLabelIndex *>(label_index_.get())->DropGraphClearIndices();
  static_cast<InMemoryLabelPropertyIndex *>(label_property_index_.get())->DropGraphClearIndices();
  static_cast<InMemoryEdgeTypeIndex *>(edge_type_index_.get())->DropGraphClearIndices();
  static_cast<InMemoryEdgeTypePropertyIndex *>(edge_type_property_index_.get())->DropGraphClearIndices();
  static_cast<InMemoryEdgePropertyIndex *>(edge_property_index_.get())->DropGraphClearIndices();
  static_cast<InMemoryVertexPropertyIndex *>(vertex_property_index_.get())->DropGraphClearIndices();
  // DropGraphClearIndices is only reachable from IN_MEMORY_ANALYTICAL DropGraph
  // where aborts can't happen, so flipping deferred_drop synchronously is safe.
  // If a future caller invokes Clear() from a transactional path, the flip
  // must instead be deferred to that txn's commit callback.
  for (auto &evicted : text_index_.Clear()) evicted->deferred_drop = true;
  for (auto &evicted : text_edge_index_.Clear()) evicted->deferred_drop = true;
  point_index_.Clear();
  vector_index_.Clear();
  vector_edge_index_.Clear();
  // Build the composite snapshot outside the outer lock to avoid the
  // outer->inner lock-order edge (see Indices::Indices ctor comment).
  auto snapshot = std::make_shared<ActiveIndices>(label_index_->GetActiveIndices(),
                                                  label_property_index_->GetActiveIndices(),
                                                  edge_type_index_->GetActiveIndices(),
                                                  edge_type_property_index_->GetActiveIndices(),
                                                  edge_property_index_->GetActiveIndices(),
                                                  vertex_property_index_->GetActiveIndices(),
                                                  text_index_.GetActiveIndices(),
                                                  text_edge_index_.GetActiveIndices(),
                                                  point_index_.GetActiveIndices(),
                                                  vector_index_.GetActiveIndices(),
                                                  vector_edge_index_.GetActiveIndices());
  active_indices_.WithLock([&](ActiveIndicesPtr &ci) { ci = std::move(snapshot); });
}

void Indices::UpdateOnAddLabel(LabelId label, Vertex *vertex, Transaction &tx, NameIdMapper *name_id_mapper) {
  tx.active_indices_->label_->UpdateOnAddLabel(label, vertex, tx);
  tx.active_indices_->label_properties_->UpdateOnAddLabel(label, vertex, tx);
  tx.active_indices_->text_->UpdateOnAddLabel(label, vertex, tx);
  vector_index_.UpdateOnAddLabel(
      label,
      vertex,
      IndexedPropertyDecoder<Vertex>{.indices = this, .name_id_mapper = name_id_mapper, .entity = vertex});
}

void Indices::UpdateOnRemoveLabel(LabelId label, Vertex *vertex, Transaction &tx, NameIdMapper *name_id_mapper) {
  tx.active_indices_->label_->UpdateOnRemoveLabel(label, vertex, tx);
  tx.active_indices_->label_properties_->UpdateOnRemoveLabel(label, vertex, tx);
  tx.active_indices_->text_->UpdateOnRemoveLabel(label, vertex, tx);
  vector_index_.UpdateOnRemoveLabel(
      label,
      vertex,
      IndexedPropertyDecoder<Vertex>{.indices = this, .name_id_mapper = name_id_mapper, .entity = vertex});
}

void Indices::UpdateOnSetProperty(PropertyId property, const PropertyValue &old_value, const PropertyValue &new_value,
                                  Vertex *vertex, Transaction &tx) {
  tx.active_indices_->label_properties_->UpdateOnSetProperty(property, old_value, new_value, vertex, tx);
  tx.active_indices_->vertex_property_->UpdateOnSetProperty(property, new_value, vertex, tx.start_timestamp);
  tx.active_indices_->text_->UpdateOnSetProperty(vertex, tx, property);
  vector_index_.UpdateOnSetProperty(property, new_value, vertex);
}

void Indices::UpdateOnSetProperty(EdgeTypeId edge_type, PropertyId property, const PropertyValue &value,
                                  Vertex *from_vertex, Vertex *to_vertex, Edge *edge, Transaction &tx) {
  tx.active_indices_->edge_type_properties_->UpdateOnSetProperty(
      from_vertex, to_vertex, edge, edge_type, property, value, tx.start_timestamp);
  tx.active_indices_->edge_property_->UpdateOnSetProperty(
      from_vertex, to_vertex, edge, edge_type, property, value, tx.start_timestamp);
  vector_edge_index_.UpdateOnSetProperty(from_vertex, to_vertex, edge, edge_type, property, value);
  tx.active_indices_->text_edge_->UpdateOnSetProperty(edge, from_vertex, to_vertex, edge_type, tx, property);
}

void Indices::UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref, EdgeTypeId edge_type,
                                   const Transaction &tx) {
  tx.active_indices_->edge_type_->UpdateOnEdgeCreation(from, to, edge_ref, edge_type, tx);
}

Indices::Indices(const Config &config, StorageMode storage_mode, utils::MemoryTracker *db_embedding_memory_tracker,
                 metrics::GaugeHandle active_label_indices, metrics::GaugeHandle active_label_property_indices,
                 metrics::GaugeHandle active_edge_type_indices, metrics::GaugeHandle active_edge_type_property_indices,
                 metrics::GaugeHandle active_edge_property_indices, metrics::GaugeHandle active_vertex_property_indices)
    : text_index_(config.durability.storage_directory),
      text_edge_index_(config.durability.storage_directory),
      vector_index_(db_embedding_memory_tracker),
      vector_edge_index_(db_embedding_memory_tracker) {
  std::invoke([this,
               config,
               storage_mode,
               active_label_indices,
               active_label_property_indices,
               active_edge_type_indices,
               active_edge_type_property_indices,
               active_edge_property_indices,
               active_vertex_property_indices]() {
    if (storage_mode == StorageMode::IN_MEMORY_TRANSACTIONAL || storage_mode == StorageMode::IN_MEMORY_ANALYTICAL) {
      label_index_ = std::make_unique<InMemoryLabelIndex>(active_label_indices);
      label_property_index_ = std::make_unique<InMemoryLabelPropertyIndex>(active_label_property_indices);
      edge_type_index_ = std::make_unique<InMemoryEdgeTypeIndex>(active_edge_type_indices);
      edge_type_property_index_ = std::make_unique<InMemoryEdgeTypePropertyIndex>(active_edge_type_property_indices);
      edge_property_index_ = std::make_unique<InMemoryEdgePropertyIndex>(active_edge_property_indices);
      vertex_property_index_ = std::make_unique<InMemoryVertexPropertyIndex>(active_vertex_property_indices);
    } else {
      label_index_ = std::make_unique<DiskLabelIndex>(config);
      label_property_index_ = std::make_unique<DiskLabelPropertyIndex>(config);
      edge_type_index_ = std::make_unique<DiskEdgeTypeIndex>();
      edge_type_property_index_ = std::make_unique<DiskEdgeTypePropertyIndex>();
      edge_property_index_ = std::make_unique<DiskEdgePropertyIndex>();
      vertex_property_index_ = std::make_unique<DiskVertexPropertyIndex>();
    }
  });
  // Build the composite snapshot outside the outer `active_indices_` lock so
  // we don't establish an outer→inner lock-order edge. Only the label /
  // label-property / edge-type / edge-type-property / edge-property sub-indices
  // take an inner read-lock inside GetActiveIndices(); text / point / vector /
  // vector-edge read their owner `index_` member directly (race-free by the
  // UNIQUE-access invariant, see each owner header). We still build outside
  // the outer lock uniformly so callers can't rely on the asymmetry.
  auto snapshot = std::make_shared<ActiveIndices>(label_index_->GetActiveIndices(),
                                                  label_property_index_->GetActiveIndices(),
                                                  edge_type_index_->GetActiveIndices(),
                                                  edge_type_property_index_->GetActiveIndices(),
                                                  edge_property_index_->GetActiveIndices(),
                                                  vertex_property_index_->GetActiveIndices(),
                                                  text_index_.GetActiveIndices(),
                                                  text_edge_index_.GetActiveIndices(),
                                                  point_index_.GetActiveIndices(),
                                                  vector_index_.GetActiveIndices(),
                                                  vector_edge_index_.GetActiveIndices());
  active_indices_.WithLock([&](ActiveIndicesPtr &ai) { ai = std::move(snapshot); });
}

void Indices::RebindMetricHandles(metrics::DatabaseMetricHandles const &handles) {
  static_cast<InMemoryLabelIndex *>(label_index_.get())->SetGauge(handles.active_label_indices);
  static_cast<InMemoryLabelPropertyIndex *>(label_property_index_.get())
      ->SetGauge(handles.active_label_property_indices);
  static_cast<InMemoryEdgeTypeIndex *>(edge_type_index_.get())->SetGauge(handles.active_edge_type_indices);
  static_cast<InMemoryEdgeTypePropertyIndex *>(edge_type_property_index_.get())
      ->SetGauge(handles.active_edge_type_property_indices);
  static_cast<InMemoryEdgePropertyIndex *>(edge_property_index_.get())->SetGauge(handles.active_edge_property_indices);
}

Indices::AbortProcessor Indices::GetAbortProcessor(ActiveIndices const &active_indices) const {
  return AbortProcessor{.label_ = active_indices.label_->GetAbortProcessor(),
                        .label_properties_ = active_indices.label_properties_->GetAbortProcessor(),
                        .edge_type_ = active_indices.edge_type_->GetAbortProcessor(),
                        .edge_type_property_ = active_indices.edge_type_properties_->GetAbortProcessor(),
                        .edge_property_ = active_indices.edge_property_->GetAbortProcessor(),
                        .vertex_property_ = active_indices.vertex_property_->GetAbortProcessor(),
                        .vector_ = vector_index_.GetAbortProcessor(),
                        .vector_edge_ = vector_edge_index_.GetAbortProcessor()};
}

void Indices::AbortProcessor::CollectOnEdgeRemoval(EdgeTypeId edge_type, Vertex *from_vertex, Vertex *to_vertex,
                                                   EdgeRef edge) {
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
  if (vertex_property_.IsInteresting(propId)) {
    auto value = vertex->properties.GetProperty(propId);
    if (!value.IsNull()) {
      vertex_property_.CollectOnPropertyChange(propId, vertex, std::move(value));
    }
  }
  vector_.CollectOnPropertyChange(propId, old_value, vertex);
}

void Indices::AbortProcessor::CollectOnEdgePropertyChange(PropertyId property, PropertyValue const &old_value,
                                                          Vertex *from_vertex, Edge *edge,
                                                          delta_container const &deltas) {
  if (!IsInterestingEdgeProperty(property)) return;

  auto link = std::optional<std::pair<EdgeTypeId, Vertex *>>{};
  for (auto const &[edge_type, to_vertex, edge_ref] : from_vertex->out_edges) {
    if (edge_ref.ptr != edge) continue;
    link = std::pair{edge_type, to_vertex};
    break;
  }

  // The link is gone if the same transaction deleted the edge, so fall back to the deltas that
  // would restore it. Both the link it removed and the one it added name the type. These deltas
  // belong to the aborting transaction alone, unlike the source vertex's own chain, which another
  // transaction may be writing under a lock an abort does not hold.
  if (!link.has_value()) {
    // Scanning the deltas costs nothing but time, and indexing them costs an entry per edge the
    // transaction linked, which for one miss in a large transaction is a lot of memory to find one
    // edge. So scan while misses are few, and only pay for the index once enough of them have
    // accumulated that scanning every time would be the greater cost.
    if (!out_edge_links_.has_value() && misses_ < kMissesBeforeIndexing) {
      ++misses_;
      for (auto const &delta : deltas) {
        if (delta.action != Delta::Action::ADD_OUT_EDGE && delta.action != Delta::Action::REMOVE_OUT_EDGE) continue;
        if (delta.vertex_edge.edge.ptr != edge) continue;
        link = std::pair{delta.vertex_edge.edge_type, delta.vertex_edge.vertex.Get()};
        break;
      }
    } else {
      if (!out_edge_links_.has_value()) {
        out_edge_links_.emplace();
        for (auto const &delta : deltas) {
          if (delta.action != Delta::Action::ADD_OUT_EDGE && delta.action != Delta::Action::REMOVE_OUT_EDGE) continue;
          out_edge_links_->emplace_back(
              delta.vertex_edge.edge.ptr, delta.vertex_edge.edge_type, delta.vertex_edge.vertex.Get());
        }
      }
      for (auto const &[linked_edge, edge_type, to_vertex] : *out_edge_links_) {
        if (linked_edge != edge) continue;
        link = std::pair{edge_type, to_vertex};
        break;
      }
    }
  }
  if (!link.has_value()) return;

  auto const [edge_type, to_vertex] = *link;
  CollectOnPropertyChange(edge_type, property, from_vertex, to_vertex, edge);
  vector_edge_.CollectOnPropertyChange(edge_type, property, old_value, from_vertex, to_vertex, edge);
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

bool Indices::AbortProcessor::IsInterestingEdgeProperty(PropertyId property) const {
  return edge_type_property_.IsInteresting(property) || edge_property_.IsInteresting(property) ||
         vector_edge_.IsInteresting(property);
}

void Indices::AbortProcessor::Process(Indices &indices, ActiveIndices const &active_indices, uint64_t start_timestamp,
                                      NameIdMapper *name_id_mapper) {
  active_indices.label_->AbortEntries(label_.cleanup_collection_, start_timestamp);
  active_indices.label_properties_->AbortEntries(label_properties_.cleanup_collection, start_timestamp);
  active_indices.edge_type_->AbortEntries(edge_type_.cleanup_collection_, start_timestamp);
  active_indices.edge_type_properties_->AbortEntries(edge_type_property_.cleanup_collection_, start_timestamp);
  active_indices.edge_property_->AbortEntries(edge_property_.cleanup_collection_, start_timestamp);
  active_indices.vertex_property_->AbortEntries(vertex_property_.cleanup_collection_, start_timestamp);
  indices.vector_index_.AbortEntries(&indices, name_id_mapper, vector_.cleanup_collection);
  indices.vector_edge_index_.AbortEntries(vector_edge_.cleanup_collection);
}
}  // namespace memgraph::storage
