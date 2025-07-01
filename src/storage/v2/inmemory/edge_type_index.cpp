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

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

namespace {
inline void TryInsertEdgeTypeIndex(Vertex &from_vertex, EdgeTypeId edge_type, auto &&index_accessor,
                                   std::optional<SnapshotObserverInfo> const &snapshot_info) {
  if (from_vertex.deleted) {
    return;
  }

  for (auto const &[type, to_vertex, edge_ref] : from_vertex.out_edges) {
    if (type != edge_type) continue;
    if (to_vertex->deleted) {
      continue;
    }
    index_accessor.insert({&from_vertex, to_vertex, edge_ref.ptr, 0});
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::EDGES);
    }
  }
}

inline void TryInsertEdgeTypeIndex(Vertex &from_vertex, EdgeTypeId edge_type, auto &&index_accessor,
                                   std::optional<SnapshotObserverInfo> const &snapshot_info, Transaction const &tx) {
  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  utils::small_vector<Vertex::EdgeTriple> edges;
  auto matches_edge_type = [edge_type](auto const &each) { return std::get<EdgeTypeId>(each) == edge_type; };
  {
    auto guard = std::shared_lock{from_vertex.lock};
    deleted = from_vertex.deleted;
    delta = from_vertex.delta;
    edges = from_vertex.out_edges | rv::filter(matches_edge_type) | r::to<utils::small_vector<Vertex::EdgeTriple>>;
  }
  // Create and drop index will always use snapshot isolation
  if (delta) {
    ApplyDeltasForRead(&tx, delta, View::OLD, [&](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Exists_ActionMethod(exists),
        Deleted_ActionMethod(deleted),
        Edges_ActionMethod<EdgeDirection::OUT>(edges, edge_type)
      });
      // clang-format on
    });
  }
  if (!exists || deleted || edges.empty()) {
    return;
  }

  for (auto const &[type, to_vertex, edge_ref] : edges) {
    index_accessor.insert({&from_vertex, to_vertex, edge_ref.ptr, tx.start_timestamp});
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::EDGES);
    }
  }
}

}  // namespace

bool InMemoryEdgeTypeIndex::CreateIndexOnePass(EdgeTypeId edge_type, utils::SkipList<Vertex>::Accessor vertices,
                                               std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto res = RegisterIndex(edge_type);
  if (!res) return false;
  auto res2 = PopulateIndex(edge_type, std::move(vertices), snapshot_info);
  if (res2.HasError()) {
    MG_ASSERT(false, "Index population can't fail, there was no cancellation callback.");
  }
  return PublishIndex(edge_type, 0);
}

struct PopulateCancel : std::exception {};

auto InMemoryEdgeTypeIndex::PopulateIndex(EdgeTypeId edge_type, utils::SkipList<Vertex>::Accessor vertices,
                                          std::optional<SnapshotObserverInfo> const &snapshot_info,
                                          Transaction const *tx, CheckCancelFunction cancel_check)
    -> utils::BasicResult<IndexPopulateError> {
  auto index = GetIndividualIndex(edge_type);
  if (!index) {
    MG_ASSERT(false, "It should not be possible to remove the index before populating it.");
  }

  try {
    auto const accessor_factory = [&] { return index->skip_list_.access(); };

    // TODO: ATM this is not parallel
    if (tx) {
      auto edge_acc = accessor_factory();
      for (auto &from_vertex : vertices) {
        if (cancel_check()) {
          throw PopulateCancel{};
        }
        TryInsertEdgeTypeIndex(from_vertex, edge_type, edge_acc, snapshot_info, *tx);
      }
    } else {
      auto edge_acc = accessor_factory();
      for (auto &from_vertex : vertices) {
        if (cancel_check()) {
          throw PopulateCancel{};
        }
        TryInsertEdgeTypeIndex(from_vertex, edge_type, edge_acc, snapshot_info);
      }
    }
  } catch (const PopulateCancel &) {
    DropIndex(edge_type);
    return IndexPopulateError::Cancellation;
  } catch (const utils::OutOfMemoryException &) {
    DropIndex(edge_type);
    throw;
  }
  return {};
}

bool InMemoryEdgeTypeIndex::RegisterIndex(EdgeTypeId edge_type) {
  return index_.WithLock([&](std::shared_ptr<IndicesContainer const> &indices_container) {
    auto const &indices = indices_container->indices_;
    auto it = indices.find(edge_type);
    if (it != indices.end()) return false;  // already exists

    utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
    // Register
    auto new_container = std::make_shared<IndicesContainer>(*indices_container);
    new_container->indices_.emplace(edge_type, std::make_shared<IndividualIndex>());
    indices_container = new_container;
    return true;
  });
}

bool InMemoryEdgeTypeIndex::PublishIndex(EdgeTypeId edge_type, uint64_t commit_timestamp) {
  auto index = GetIndividualIndex(edge_type);
  if (!index) return false;
  index->Publish(commit_timestamp);
  return true;
}

void InMemoryEdgeTypeIndex::IndividualIndex::Publish(uint64_t commit_timestamp) {
  status_.commit(commit_timestamp);
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveEdgeTypeIndices);
}

InMemoryEdgeTypeIndex::IndividualIndex::~IndividualIndex() {
  if (status_.is_ready()) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveEdgeTypeIndices);
  }
}

bool InMemoryEdgeTypeIndex::DropIndex(EdgeTypeId edge_type) {
  return index_.WithLock([&](std::shared_ptr<IndicesContainer const> &indices_container) {
    auto const it = indices_container->indices_.find(edge_type);
    if (it == indices_container->indices_.cend()) return false;

    auto new_container = std::make_shared<IndicesContainer>(*indices_container);
    new_container->indices_.erase(edge_type);
    indices_container = new_container;
    return true;
  });
}

bool InMemoryEdgeTypeIndex::ActiveIndices::IndexReady(memgraph::storage::EdgeTypeId edge_type) const {
  auto const &indices = ptr_->indices_;
  auto it = indices.find(edge_type);
  if (it == indices.end()) return false;
  return it->second->status_.is_ready();
}
bool InMemoryEdgeTypeIndex::ActiveIndices::IndexRegistered(EdgeTypeId edge_type) const {
  return ptr_->indices_.find(edge_type) != ptr_->indices_.end();
}

std::vector<EdgeTypeId> InMemoryEdgeTypeIndex::ActiveIndices::ListIndices(uint64_t start_timestamp) const {
  auto ret = std::vector<EdgeTypeId>{};
  ret.reserve(ptr_->indices_.size());
  for (auto const &[edge_type, index] : ptr_->indices_) {
    if (index->status_.is_visible(start_timestamp)) {
      ret.emplace_back(edge_type);
    }
  }
  return ret;
}

void InMemoryEdgeTypeIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter(2048);

  auto cpy = index_.WithReadLock(std::identity{});
  for (auto &[_, et_index] : cpy->indices_) {
    if (token.stop_requested()) return;

    auto edges_acc = et_index->skip_list_.access();
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

uint64_t InMemoryEdgeTypeIndex::ActiveIndices::ApproximateEdgeCount(EdgeTypeId edge_type) const {
  if (auto it = ptr_->indices_.find(edge_type); it != ptr_->indices_.end()) {
    return it->second->skip_list_.size();
  }
  return 0;
}

void InMemoryEdgeTypeIndex::ActiveIndices::AbortEntries(EdgeTypeIndex::AbortableInfo const &info,
                                                        uint64_t exact_start_timestamp) {
  for (auto const &[edge_type, edges] : info) {
    auto const it = ptr_->indices_.find(edge_type);
    DMG_ASSERT(it != ptr_->indices_.end());

    auto &index_storage = it->second;
    auto acc = index_storage->skip_list_.access();
    for (const auto &[from_vertex, to_vertex, edge] : edges) {
      acc.remove(Entry{from_vertex, to_vertex, edge, exact_start_timestamp});
    }
  }
}

void InMemoryEdgeTypeIndex::ActiveIndices::UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref,
                                                                EdgeTypeId edge_type, const Transaction &tx) {
  auto it = ptr_->indices_.find(edge_type);
  if (it == ptr_->indices_.end()) {
    return;
  }
  auto acc = it->second->skip_list_.access();
  acc.insert(Entry{from, to, edge_ref.ptr, tx.start_timestamp});
}

void InMemoryEdgeTypeIndex::ActiveIndices::UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from,
                                                                    Vertex *new_to, EdgeRef edge_ref,
                                                                    EdgeTypeId edge_type, const Transaction &tx) {
  auto it = ptr_->indices_.find(edge_type);
  if (it == ptr_->indices_.end()) {
    return;
  }

  auto acc = it->second->skip_list_.access();
  acc.insert(Entry{new_from, new_to, edge_ref.ptr, tx.start_timestamp});
}

void InMemoryEdgeTypeIndex::DropGraphClearIndices() {
  index_.WithLock([](std::shared_ptr<IndicesContainer const> &index) { index = std::make_shared<IndicesContainer>(); });
}

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
  auto cpy = index_.WithReadLock(std::identity{});
  for (auto &[edge_type, index] : cpy->indices_) {
    index->skip_list_.run_gc();
  }
}

InMemoryEdgeTypeIndex::Iterable InMemoryEdgeTypeIndex::ActiveIndices::Edges(EdgeTypeId edge_type, View view,
                                                                            Storage *storage,
                                                                            Transaction *transaction) {
  const auto it = ptr_->indices_.find(edge_type);
  MG_ASSERT(it != ptr_->indices_.end(), "Index for edge-type {} doesn't exist", edge_type.AsUint());
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  auto edge_acc = static_cast<InMemoryStorage const *>(storage)->edges_.access();
  return {it->second->skip_list_.access(),
          std::move(vertex_acc),
          std::move(edge_acc),
          edge_type,
          view,
          storage,
          transaction};
}

EdgeTypeIndex::AbortProcessor InMemoryEdgeTypeIndex::ActiveIndices::GetAbortProcessor() const {
  auto edge_type_filter = ptr_->indices_ | std::views::keys | ranges::to_vector;
  return AbortProcessor{std::move(edge_type_filter)};
}

auto InMemoryEdgeTypeIndex::GetActiveIndices() const -> std::unique_ptr<EdgeTypeIndex::ActiveIndices> {
  return std::make_unique<ActiveIndices>(index_.WithReadLock(std::identity{}));
}

auto InMemoryEdgeTypeIndex::GetIndividualIndex(EdgeTypeId edge_type) const -> std::shared_ptr<IndividualIndex> {
  return index_.WithReadLock(
      [&](std::shared_ptr<IndicesContainer const> const &index) -> std::shared_ptr<IndividualIndex> {
        auto it = index->indices_.find(edge_type);
        if (it == index->indices_.cend()) [[unlikely]]
          return {};
        return it->second;
      });
}

}  // namespace memgraph::storage
