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

#include "storage/v2/inmemory/edge_type_index.hpp"

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/edge_info_helpers.hpp"
#include "storage/v2/indices/indices_utils.hpp"
#include "utils/counter.hpp"

namespace {

using Delta = memgraph::storage::Delta;
using Vertex = memgraph::storage::Vertex;
using Edge = memgraph::storage::Edge;
using EdgeRef = memgraph::storage::EdgeRef;
using EdgeTypeId = memgraph::storage::EdgeTypeId;
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

bool InMemoryEdgeTypeIndex::CreateIndex(EdgeTypeId edge_type, utils::SkipList<Vertex>::Accessor vertices) {
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
  auto maybe_stop = utils::ResettableCounter<2048>();

  for (auto &label_storage : index_) {
    if (token.stop_requested()) return;

    auto edges_acc = label_storage.second.access();
    for (auto it = edges_acc.begin(); it != edges_acc.end();) {
      if (maybe_stop() && token.stop_requested()) return;

      auto next_it = it;
      ++next_it;

      if (it->timestamp >= oldest_active_start_timestamp) {
        it = next_it;
        continue;
      }

      if (next_it != edges_acc.end() || it->from_vertex->deleted || it->to_vertex->deleted ||
          !std::ranges::all_of(it->from_vertex->out_edges, [&](const auto &edge) {
            auto *to_vertex = std::get<InMemoryEdgeTypeIndex::kVertexPos>(edge);
            return to_vertex != it->to_vertex;
          })) {
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

  auto entry_to_update = std::ranges::find_if(acc, [&](const auto &entry) {
    return entry.from_vertex == old_from && entry.to_vertex == old_to && entry.edge == edge_ref.ptr;
  });

  acc.remove(Entry{entry_to_update->from_vertex, entry_to_update->to_vertex, entry_to_update->edge,
                   entry_to_update->timestamp});
  acc.insert(Entry{new_from, new_to, edge_ref.ptr, tx.start_timestamp});
}

void InMemoryEdgeTypeIndex::DropGraphClearIndices() { index_.clear(); }

InMemoryEdgeTypeIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor, View view, Storage *storage,
                                          Transaction *transaction)
    : index_accessor_(std::move(index_accessor)), view_(view), storage_(storage), transaction_(transaction) {}

InMemoryEdgeTypeIndex::Iterable::Iterator::Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_edge_accessor_(EdgeRef{nullptr}, EdgeTypeId::FromInt(0), nullptr, nullptr, self_->storage_, nullptr),
      current_edge_(nullptr) {
  AdvanceUntilValid();
}

InMemoryEdgeTypeIndex::Iterable::Iterator &InMemoryEdgeTypeIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void InMemoryEdgeTypeIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_accessor_.end(); ++index_iterator_) {
    auto *from_vertex = index_iterator_->from_vertex;
    auto *to_vertex = index_iterator_->to_vertex;

    if (self_->transaction_->original_start_timestamp.has_value()) {
      if (index_iterator_->timestamp >= self_->transaction_->original_start_timestamp.value()) {
        continue;
      }
    }

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

std::tuple<EdgeRef, EdgeTypeId, Vertex *, Vertex *> InMemoryEdgeTypeIndex::Iterable::Iterator::GetEdgeInfo() {
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

void InMemoryEdgeTypeIndex::RunGC() {
  for (auto &index_entry : index_) {
    index_entry.second.run_gc();
  }
}

InMemoryEdgeTypeIndex::Iterable InMemoryEdgeTypeIndex::Edges(EdgeTypeId edge_type, View view, Storage *storage,
                                                             Transaction *transaction) {
  const auto it = index_.find(edge_type);
  MG_ASSERT(it != index_.end(), "Index for edge-type {} doesn't exist", edge_type.AsUint());
  return {it->second.access(), view, storage, transaction};
}

}  // namespace memgraph::storage
