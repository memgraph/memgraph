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

#include "storage/v2/inmemory/edge_type_property_index.hpp"

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/edge_info_helpers.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/indices_utils.hpp"
#include "utils/counter.hpp"

namespace {

using Delta = memgraph::storage::Delta;
using Vertex = memgraph::storage::Vertex;
using Edge = memgraph::storage::Edge;
using EdgeRef = memgraph::storage::EdgeRef;
using EdgeTypeId = memgraph::storage::EdgeTypeId;
using PropertyId = memgraph::storage::PropertyId;
using PropertyValue = memgraph::storage::PropertyValue;
using Transaction = memgraph::storage::Transaction;
using View = memgraph::storage::View;

using ReturnType = std::optional<std::tuple<EdgeTypeId, Vertex *, EdgeRef>>;
ReturnType VertexDeletedConnectedEdges(Vertex *vertex, Edge *edge, const Transaction *transaction, View view) {
  ReturnType link;
  Delta *delta = nullptr;
  {
    auto guard = std::shared_lock{vertex->lock};
    delta = vertex->delta;
  }
  ApplyDeltasForRead(transaction, delta, view, [&](const Delta &delta) {
    switch (delta.action) {
      case Delta::Action::ADD_LABEL:
      case Delta::Action::REMOVE_LABEL:
      case Delta::Action::SET_PROPERTY:
        break;
      case Delta::Action::ADD_IN_EDGE: {
        if (edge == delta.vertex_edge.edge.ptr) {
          link = {delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge};
          auto it = std::find(vertex->in_edges.begin(), vertex->in_edges.end(), link);
          MG_ASSERT(it == vertex->in_edges.end(), "Invalid database state!");
          break;
        }
      }
      case Delta::Action::ADD_OUT_EDGE: {
        if (edge == delta.vertex_edge.edge.ptr) {
          link = {delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge};
          auto it = std::find(vertex->out_edges.begin(), vertex->out_edges.end(), link);
          MG_ASSERT(it == vertex->out_edges.end(), "Invalid database state!");
          break;
        }
      }
      case Delta::Action::REMOVE_IN_EDGE:
      case Delta::Action::REMOVE_OUT_EDGE:
      case Delta::Action::RECREATE_OBJECT:
      case Delta::Action::DELETE_DESERIALIZED_OBJECT:
      case Delta::Action::DELETE_OBJECT:
        break;
    }
  });
  return link;
}
}  // namespace

namespace memgraph::storage {

bool InMemoryEdgeTypePropertyIndex::Entry::operator<(const Entry &rhs) const {
  if (edge->gid < rhs.edge->gid) {
    return true;
  }
  if (rhs.edge->gid < edge->gid) {
    return false;
  }
  return std::make_tuple(value, from_vertex, to_vertex, edge, timestamp) <
         std::make_tuple(rhs.value, rhs.from_vertex, rhs.to_vertex, rhs.edge, rhs.timestamp);
}

bool InMemoryEdgeTypePropertyIndex::Entry::operator==(const Entry &rhs) const {
  return std::make_tuple(value, from_vertex, to_vertex, edge, timestamp) ==
         std::make_tuple(rhs.value, rhs.from_vertex, rhs.to_vertex, rhs.edge, rhs.timestamp);
}

bool InMemoryEdgeTypePropertyIndex::Entry::operator<(const PropertyValue &rhs) const { return value < rhs; }

bool InMemoryEdgeTypePropertyIndex::Entry::operator==(const PropertyValue &rhs) const { return value == rhs; }

// Verifiy this
bool InMemoryEdgeTypePropertyIndex::CreateIndex(EdgeTypeId edge_type, PropertyId property,
                                                utils::SkipList<Vertex>::Accessor vertices) {
  auto [it, emplaced] = index_.try_emplace({edge_type, property});
  if (!emplaced) {
    return false;
  }

  utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  try {
    auto edge_acc = it->second.access();
    for (auto &from_vertex : vertices) {
      if (from_vertex.deleted) {
        continue;
      }

      for (auto &edge : from_vertex.out_edges) {
        const auto type = std::get<kEdgeTypeIdPos>(edge);
        if (type == edge_type) {
          auto *to_vertex = std::get<kVertexPos>(edge);
          if (to_vertex->deleted) {
            continue;
          }
          auto *edge_ptr = std::get<kEdgeRefPos>(edge).ptr;
          edge_acc.insert({edge_ptr->properties.GetProperty(property), &from_vertex, to_vertex, edge_ptr, 0});
        }
      }
    }
  } catch (const utils::OutOfMemoryException &) {
    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    index_.erase(it);
    throw;
  }

  return true;
}

bool InMemoryEdgeTypePropertyIndex::DropIndex(EdgeTypeId edge_type, PropertyId property) {
  return index_.erase({edge_type, property}) > 0;
}

bool InMemoryEdgeTypePropertyIndex::IndexExists(EdgeTypeId edge_type, PropertyId property) const {
  return index_.find({edge_type, property}) != index_.end();
}

std::vector<std::pair<EdgeTypeId, PropertyId>> InMemoryEdgeTypePropertyIndex::ListIndices() const {
  std::vector<std::pair<EdgeTypeId, PropertyId>> ret;
  ret.reserve(index_.size());
  for (const auto &item : index_) {
    ret.push_back({item.first.first, item.first.second});
  }
  return ret;
}

void InMemoryEdgeTypePropertyIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp,
                                                          std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter<2048>();

  for (auto &specific_index : index_) {
    if (token.stop_requested()) return;

    auto edges_acc = specific_index.second.access();
    for (auto it = edges_acc.begin(); it != edges_acc.end();) {
      if (maybe_stop() && token.stop_requested()) return;

      auto next_it = it;
      ++next_it;

      if (it->timestamp >= oldest_active_start_timestamp) {
        it = next_it;
        continue;
      }

      // It is possible that entries are identical except for the timestamp and maybe value.
      // We have to acount for that here. -> TODO
      const bool vertices_deleted = it->from_vertex->deleted || it->to_vertex->deleted;
      const bool has_next = next_it != edges_acc.end();

      // When we update specific entries in the index, we don't delete the previous entry.
      // The way they are removed from the index is through this check. The entries should
      // be right next to each other(in terms of iterator semantics) and the older one
      // should be removed here.
      bool redundant_duplicate = has_next && it->value == next_it->value && it->from_vertex == next_it->from_vertex &&
                                 it->to_vertex == next_it->to_vertex && it->edge == next_it->edge;
      // TODO inspect if this is actually correct
      if (redundant_duplicate || vertices_deleted ||
          !AnyVersionHasLabelProperty(*it->edge, specific_index.first.second, it->value,
                                      oldest_active_start_timestamp) ||
          !std::ranges::all_of(it->from_vertex->out_edges, [&](const auto &edge) {
            auto *to_vertex = std::get<InMemoryEdgeTypePropertyIndex::kVertexPos>(edge);
            return to_vertex != it->to_vertex;
          })) {
        edges_acc.remove(*it);
      }

      it = next_it;
    }
  }
}

void InMemoryEdgeTypePropertyIndex::UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge,
                                                        EdgeTypeId edge_type, PropertyId property, PropertyValue value,
                                                        uint64_t timestamp) {
  if (value.IsNull()) {
    return;
  }

  auto it = index_.find({edge_type, property});
  if (it == index_.end()) return;

  auto acc = it->second.access();

  acc.insert({value, from_vertex, to_vertex, edge, timestamp});
}

uint64_t InMemoryEdgeTypePropertyIndex::ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property) const {
  if (auto it = index_.find({edge_type, property}); it != index_.end()) {
    return it->second.size();
  }
  return 0;
}

// TODO Check if this should work without property and value
void InMemoryEdgeTypePropertyIndex::UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from,
                                                             Vertex *new_to, EdgeRef edge_ref, EdgeTypeId edge_type,
                                                             PropertyId property, PropertyValue value,
                                                             const Transaction &tx) {
  auto it = index_.find({edge_type, property});
  if (it == index_.end()) {
    return;
  }
  auto acc = it->second.access();

  auto entry_to_update = std::ranges::find_if(acc, [&](const auto &entry) {
    return entry.from_vertex == old_from && entry.to_vertex == old_to && entry.edge == edge_ref.ptr;
  });

  acc.remove(Entry{value, entry_to_update->from_vertex, entry_to_update->to_vertex, entry_to_update->edge,
                   entry_to_update->timestamp});
  acc.insert(Entry{value, new_from, new_to, edge_ref.ptr, tx.start_timestamp});
}

void InMemoryEdgeTypePropertyIndex::DropGraphClearIndices() { index_.clear(); }

InMemoryEdgeTypePropertyIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor, EdgeTypeId edge_type,
                                                  PropertyId property, View view, Storage *storage,
                                                  Transaction *transaction)
    : index_accessor_(std::move(index_accessor)),
      edge_type_(edge_type),
      property_(property),
      view_(view),
      storage_(storage),
      transaction_(transaction) {}

InMemoryEdgeTypePropertyIndex::Iterable::Iterator::Iterator(Iterable *self,
                                                            utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_edge_accessor_(EdgeRef{nullptr}, EdgeTypeId::FromInt(0), nullptr, nullptr, self_->storage_, nullptr),
      current_edge_(nullptr) {
  AdvanceUntilValid();
}

InMemoryEdgeTypePropertyIndex::Iterable::Iterator &InMemoryEdgeTypePropertyIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void InMemoryEdgeTypePropertyIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_accessor_.end(); ++index_iterator_) {
    auto *from_vertex = index_iterator_->from_vertex;
    auto *to_vertex = index_iterator_->to_vertex;

    if (!IsEdgeVisible(index_iterator_->edge, self_->transaction_, self_->view_) || from_vertex->deleted ||
        to_vertex->deleted) {
      continue;
    }

    const bool edge_was_deleted = index_iterator_->edge->deleted;
    auto [edge_ref, edge_type, deleted_from_vertex, deleted_to_vertex] = GetEdgeInfo();
    MG_ASSERT(edge_ref != EdgeRef(nullptr), "Invalid database state!");

    if (edge_was_deleted) {
      from_vertex = deleted_from_vertex;
      to_vertex = deleted_to_vertex;
    }

    auto accessor = EdgeAccessor{edge_ref, edge_type, from_vertex, to_vertex, self_->storage_, self_->transaction_};
    if (!accessor.IsVisible(self_->view_)) {
      continue;
    }

    current_edge_accessor_ = accessor;
    current_edge_ = edge_ref;
    break;
  }
}

std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *> InMemoryEdgeTypePropertyIndex::Iterable::Iterator::GetEdgeInfo() {
  auto *from_vertex = index_iterator_->from_vertex;
  auto *to_vertex = index_iterator_->to_vertex;

  if (index_iterator_->edge->deleted) {
    const auto missing_in_edge =
        VertexDeletedConnectedEdges(from_vertex, index_iterator_->edge, self_->transaction_, self_->view_);
    const auto missing_out_edge =
        VertexDeletedConnectedEdges(to_vertex, index_iterator_->edge, self_->transaction_, self_->view_);
    if (missing_in_edge && missing_out_edge &&
        std::get<kEdgeRefPos>(*missing_in_edge) == std::get<kEdgeRefPos>(*missing_out_edge)) {
      return std::make_tuple(std::get<kEdgeRefPos>(*missing_in_edge), std::get<kEdgeTypeIdPos>(*missing_in_edge),
                             to_vertex, from_vertex);
    }
  }

  const auto &from_edges = from_vertex->out_edges;
  const auto &to_edges = to_vertex->in_edges;

  auto it = std::find_if(from_edges.begin(), from_edges.end(), [&](const auto &from_entry) {
    const auto &from_edge = std::get<kEdgeRefPos>(from_entry);
    return std::any_of(to_edges.begin(), to_edges.end(), [&](const auto &to_entry) {
      const auto &to_edge = std::get<kEdgeRefPos>(to_entry);
      return index_iterator_->edge->gid == from_edge.ptr->gid && from_edge.ptr->gid == to_edge.ptr->gid;
    });
  });

  if (it != from_edges.end()) {
    const auto &from_edge = std::get<kEdgeRefPos>(*it);
    return std::make_tuple(from_edge, std::get<kEdgeTypeIdPos>(*it), from_vertex, to_vertex);
  }

  return {EdgeRef(nullptr), EdgeTypeId::FromUint(0U), nullptr, nullptr};
}

void InMemoryEdgeTypePropertyIndex::RunGC() {
  for (auto &index_entry : index_) {
    index_entry.second.run_gc();
  }
}

InMemoryEdgeTypePropertyIndex::Iterable InMemoryEdgeTypePropertyIndex::Edges(EdgeTypeId edge_type, PropertyId property,
                                                                             View view, Storage *storage,
                                                                             Transaction *transaction) {
  // TODO implement this correctly
  const auto it = index_.find({edge_type, property});
  MG_ASSERT(it != index_.end(), "Index for edge-type {} and property {} doesn't exist", edge_type.AsUint(),
            property.AsUint());
  return {it->second.access(), edge_type, property, view, storage, transaction};
}

}  // namespace memgraph::storage
