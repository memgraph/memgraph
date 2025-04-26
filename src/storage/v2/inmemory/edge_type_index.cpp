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

#include "storage/v2/inmemory/edge_type_index.hpp"

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/edge_info_helpers.hpp"
#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "utils/counter.hpp"

namespace memgraph::storage {

bool InMemoryEdgeTypeIndex::CreateIndex(EdgeTypeId edge_type, utils::SkipList<Vertex>::Accessor vertices,
                                        std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto [it, emplaced] = index_.try_emplace(edge_type);
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
          edge_acc.insert({&from_vertex, to_vertex, std::get<kEdgeRefPos>(edge).ptr, 0});
          if (snapshot_info) {
            snapshot_info->Update(UpdateType::EDGES);
          }
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

bool InMemoryEdgeTypeIndex::DropIndex(EdgeTypeId edge_type) { return index_.erase(edge_type) > 0; }

bool InMemoryEdgeTypeIndex::IndexExists(EdgeTypeId edge_type) const { return index_.find(edge_type) != index_.end(); }

std::vector<EdgeTypeId> InMemoryEdgeTypeIndex::ListIndices() const {
  std::vector<EdgeTypeId> ret;
  ret.reserve(index_.size());
  for (const auto &item : index_) {
    ret.push_back(item.first);
  }
  return ret;
}

void InMemoryEdgeTypeIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter(2048);

  for (auto &[_, et_index] : index_) {
    if (token.stop_requested()) return;

    auto edges_acc = et_index.access();
    for (auto it = edges_acc.begin(); it != edges_acc.end();) {
      if (maybe_stop() && token.stop_requested()) return;

      auto next_it = it;
      ++next_it;

      if (it->timestamp >= oldest_active_start_timestamp) {
        it = next_it;
        continue;
      }

      const bool vertices_deleted = it->from_vertex->deleted || it->to_vertex->deleted;
      const bool edge_deleted = it->edge->deleted;
      const bool has_next = next_it != edges_acc.end();

      // When we update specific entries in the index, we don't delete the previous entry.
      // The way they are removed from the index is through this check. The entries should
      // be right next to each other(in terms of iterator semantics) and the older one
      // should be removed here.
      const bool redundant_duplicate = has_next && it->from_vertex == next_it->from_vertex &&
                                       it->to_vertex == next_it->to_vertex && it->edge == next_it->edge;
      if (redundant_duplicate || vertices_deleted || edge_deleted) {
        edges_acc.remove(*it);
      }

      it = next_it;
    }
  }
}

uint64_t InMemoryEdgeTypeIndex::ApproximateEdgeCount(EdgeTypeId edge_type) const {
  if (auto it = index_.find(edge_type); it != index_.end()) {
    return it->second.size();
  }
  return 0;
}

void InMemoryEdgeTypeIndex::AbortEntries(EdgeTypeIndex::AbortableInfo const &info, uint64_t exact_start_timestamp) {
  for (auto const &[edge_type, edges] : info) {
    auto const it = index_.find(edge_type);
    DMG_ASSERT(it != index_.end());

    auto &index_storage = it->second;
    auto acc = index_storage.access();
    for (const auto &[from_vertex, to_vertex, edge] : edges) {
      acc.remove(Entry{from_vertex, to_vertex, edge, exact_start_timestamp});
    }
  }
}

void InMemoryEdgeTypeIndex::UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref, EdgeTypeId edge_type,
                                                 const Transaction &tx) {
  auto it = index_.find(edge_type);
  if (it == index_.end()) {
    return;
  }
  auto acc = it->second.access();
  acc.insert(Entry{from, to, edge_ref.ptr, tx.start_timestamp});
}

void InMemoryEdgeTypeIndex::UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from, Vertex *new_to,
                                                     EdgeRef edge_ref, EdgeTypeId edge_type, const Transaction &tx) {
  auto it = index_.find(edge_type);
  if (it == index_.end()) {
    return;
  }

  auto acc = it->second.access();
  acc.insert(Entry{new_from, new_to, edge_ref.ptr, tx.start_timestamp});
}

void InMemoryEdgeTypeIndex::DropGraphClearIndices() { index_.clear(); }

InMemoryEdgeTypeIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor,
                                          utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
                                          utils::SkipList<Edge>::ConstAccessor edge_accessor, EdgeTypeId edge_type,
                                          View view, Storage *storage, Transaction *transaction)
    : pin_accessor_edge_(std::move(edge_accessor)),
      pin_accessor_vertex_(std::move(vertex_accessor)),
      index_accessor_(std::move(index_accessor)),
      edge_type_(edge_type),
      view_(view),
      storage_(storage),
      transaction_(transaction) {}

InMemoryEdgeTypeIndex::Iterable::Iterator::Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_edge_(nullptr),
      current_accessor_(EdgeRef{nullptr}, EdgeTypeId::FromInt(0), nullptr, nullptr, self_->storage_, nullptr) {
  AdvanceUntilValid();
}

InMemoryEdgeTypeIndex::Iterable::Iterator &InMemoryEdgeTypeIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void InMemoryEdgeTypeIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_accessor_.end(); ++index_iterator_) {
    if (index_iterator_->edge == current_edge_.ptr) {
      continue;
    }

    if (!CanSeeEntityWithTimestamp(index_iterator_->timestamp, self_->transaction_, self_->view_)) {
      continue;
    }

    auto *from_vertex = index_iterator_->from_vertex;
    auto *to_vertex = index_iterator_->to_vertex;
    auto edge_ref = EdgeRef(index_iterator_->edge);

    auto accessor =
        EdgeAccessor{edge_ref, self_->edge_type_, from_vertex, to_vertex, self_->storage_, self_->transaction_};
    if (!accessor.IsVisible(self_->view_)) {
      continue;
    }

    current_edge_ = edge_ref;
    current_accessor_ = accessor;
    break;
  }
}

void InMemoryEdgeTypeIndex::RunGC() {
  for (auto &index_entry : index_) {
    index_entry.second.run_gc();
  }
}

InMemoryEdgeTypeIndex::Iterable InMemoryEdgeTypeIndex::Edges(EdgeTypeId edge_type, View view, Storage *storage,
                                                             Transaction *transaction) {
  const auto it = index_.find(edge_type);
  MG_ASSERT(it != index_.end(), "Index for edge-type {} doesn't exist", edge_type.AsUint());
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  auto edge_acc = static_cast<InMemoryStorage const *>(storage)->edges_.access();
  return {it->second.access(), std::move(vertex_acc), std::move(edge_acc), edge_type, view, storage, transaction};
}

EdgeTypeIndex::AbortProcessor InMemoryEdgeTypeIndex::GetAbortProcessor() const {
  std::vector<EdgeTypeId> res;
  res.reserve(index_.size());
  for (const auto &[edge_type, _] : index_) {
    res.emplace_back(edge_type);
  }
  return AbortProcessor{res};
}

}  // namespace memgraph::storage
