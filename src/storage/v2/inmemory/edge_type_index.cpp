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

inline void AdvanceUntilValid_(auto &index_iterator, const auto &end_iterator, EdgeRef &current_edge_,
                               EdgeAccessor &current_accessor_, Transaction *transaction, View view,
                               EdgeTypeId edge_type, Storage *storage) {
  for (; index_iterator != end_iterator; ++index_iterator) {
    if (index_iterator->edge == current_edge_.ptr) {
      continue;
    }

    if (!CanSeeEntityWithTimestamp(index_iterator->timestamp, transaction, view)) {
      continue;
    }

    auto *from_vertex = index_iterator->from_vertex;
    auto *to_vertex = index_iterator->to_vertex;
    auto edge_ref = EdgeRef(index_iterator->edge);

    auto accessor = EdgeAccessor{edge_ref, edge_type, from_vertex, to_vertex, storage, transaction};
    if (!accessor.IsVisible(view)) {
      continue;
    }

    current_edge_ = edge_ref;
    current_accessor_ = accessor;
    break;
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
    if (tx) {
      // If we are in a transaction, we need to read the object with the correct MVCC snapshot isolation
      auto const insert_function = [&](Vertex &from_vertex, auto &index_accessor) {
        TryInsertEdgeTypeIndex(from_vertex, edge_type, index_accessor, snapshot_info, *tx);
      };
      PopulateIndexDispatch(vertices, accessor_factory, insert_function, std::move(cancel_check),
                            {} /*TODO: parallel*/);
    } else {
      // If we are not in a transaction, we need to read the object as it is. (post recovery)
      auto const insert_function = [&](Vertex &from_vertex, auto &index_accessor) {
        TryInsertEdgeTypeIndex(from_vertex, edge_type, index_accessor, snapshot_info);
      };
      PopulateIndexDispatch(vertices, accessor_factory, insert_function, std::move(cancel_check),
                            {} /*TODO: parallel*/);
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
    {
      auto it = indices.find(edge_type);
      if (it != indices.end()) return false;  // already exists
    }

    // Register
    auto new_container = std::make_shared<IndicesContainer>(*indices_container);
    auto [new_it, _] = new_container->indices_.emplace(edge_type, std::make_shared<IndividualIndex>());

    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
    // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
    all_indices_.WithLock([&](auto &all_indices) {
      auto new_all_indices = *all_indices;
      // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
      new_all_indices.emplace_back(new_it->second);
      all_indices = std::make_shared<std::vector<AllIndicesEntry>>(std::move(new_all_indices));
    });
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
  status_.Commit(commit_timestamp);
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveEdgeTypeIndices);
}

InMemoryEdgeTypeIndex::IndividualIndex::~IndividualIndex() {
  if (status_.IsReady()) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveEdgeTypeIndices);
  }
}

bool InMemoryEdgeTypeIndex::DropIndex(EdgeTypeId edge_type) {
  auto const result = index_.WithLock([&](std::shared_ptr<IndicesContainer const> &indices_container) {
    {
      auto const it = indices_container->indices_.find(edge_type);
      if (it == indices_container->indices_.cend()) return false;
    }

    auto new_container = std::make_shared<IndicesContainer>();
    for (auto const &[existing_edge_type, index] : indices_container->indices_) {
      if (existing_edge_type != edge_type) {
        new_container->indices_.emplace(existing_edge_type, index);
      }
    }
    indices_container = new_container;
    return true;
  });
  CleanupAllIndices();
  return result;
}

bool InMemoryEdgeTypeIndex::ActiveIndices::IndexReady(memgraph::storage::EdgeTypeId edge_type) const {
  auto const &indices = index_container_->indices_;
  auto it = indices.find(edge_type);
  if (it == indices.end()) return false;
  return it->second->status_.IsReady();
}

bool InMemoryEdgeTypeIndex::ActiveIndices::IndexRegistered(EdgeTypeId edge_type) const {
  return index_container_->indices_.find(edge_type) != index_container_->indices_.end();
}

std::vector<EdgeTypeId> InMemoryEdgeTypeIndex::ActiveIndices::ListIndices(uint64_t start_timestamp) const {
  auto ret = std::vector<EdgeTypeId>{};
  ret.reserve(index_container_->indices_.size());
  for (auto const &[edge_type, index] : index_container_->indices_) {
    if (index->status_.IsVisible(start_timestamp)) {
      ret.emplace_back(edge_type);
    }
  }
  return ret;
}

void InMemoryEdgeTypeIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter(2048);

  CleanupAllIndices();

  auto cpy = all_indices_.WithReadLock(std::identity{});
  for (auto &et_index : *cpy) {
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

      const bool has_next = next_it != edges_acc.end();

      // When we update specific entries in the index, we don't delete the previous entry.
      // The way they are removed from the index is through this check. The entries should
      // be right next to each other(in terms of iterator semantics) and the older one
      // should be removed here.
      const bool redundant_duplicate = has_next && it->from_vertex == next_it->from_vertex &&
                                       it->to_vertex == next_it->to_vertex && it->edge == next_it->edge;
      if (redundant_duplicate || !AnyVersionIsVisible(it->edge, oldest_active_start_timestamp)) {
        edges_acc.remove(*it);
      }

      it = next_it;
    }
  }
}

uint64_t InMemoryEdgeTypeIndex::ActiveIndices::ApproximateEdgeCount(EdgeTypeId edge_type) const {
  if (auto it = index_container_->indices_.find(edge_type); it != index_container_->indices_.end()) {
    return it->second->skip_list_.size();
  }
  return 0;
}

void InMemoryEdgeTypeIndex::ActiveIndices::AbortEntries(EdgeTypeIndex::AbortableInfo const &info,
                                                        uint64_t exact_start_timestamp) {
  for (auto const &[edge_type, edges] : info) {
    auto const it = index_container_->indices_.find(edge_type);
    DMG_ASSERT(it != index_container_->indices_.end());

    auto &index_storage = it->second;
    auto acc = index_storage->skip_list_.access();
    for (const auto &[from_vertex, to_vertex, edge] : edges) {
      acc.remove(Entry{from_vertex, to_vertex, edge, exact_start_timestamp});
    }
  }
}

void InMemoryEdgeTypeIndex::ActiveIndices::UpdateOnEdgeCreation(Vertex *from, Vertex *to, EdgeRef edge_ref,
                                                                EdgeTypeId edge_type, const Transaction &tx) {
  auto it = index_container_->indices_.find(edge_type);
  if (it == index_container_->indices_.end()) {
    return;
  }
  auto acc = it->second->skip_list_.access();
  acc.insert(Entry{from, to, edge_ref.ptr, tx.start_timestamp});
}

void InMemoryEdgeTypeIndex::DropGraphClearIndices() {
  index_.WithLock([](std::shared_ptr<IndicesContainer const> &index) { index = std::make_shared<IndicesContainer>(); });
  all_indices_.WithLock([](std::shared_ptr<std::vector<AllIndicesEntry> const> &all_indices) {
    all_indices = std::make_unique<std::vector<AllIndicesEntry>>();
  });
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
  AdvanceUntilValid_(index_iterator_, self_->index_accessor_.end(), current_edge_, current_accessor_,
                     self_->transaction_, self_->view_, self_->edge_type_, self_->storage_);
}

void InMemoryEdgeTypeIndex::RunGC() {
  // Remove indices that are not used by any txn
  CleanupAllIndices();

  // For each skip_list remaining, run GC
  auto cpy = all_indices_.WithReadLock(std::identity{});
  for (auto &index : *cpy) {
    index->skip_list_.run_gc();
  }
}

InMemoryEdgeTypeIndex::Iterable InMemoryEdgeTypeIndex::ActiveIndices::Edges(EdgeTypeId edge_type, View view,
                                                                            Storage *storage,
                                                                            Transaction *transaction) {
  const auto it = index_container_->indices_.find(edge_type);
  MG_ASSERT(it != index_container_->indices_.end(), "Index for edge-type {} doesn't exist", edge_type.AsUint());
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

InMemoryEdgeTypeIndex::ChunkedIterable InMemoryEdgeTypeIndex::ActiveIndices::ChunkedEdges(
    EdgeTypeId edge_type, utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
    utils::SkipList<Edge>::ConstAccessor edge_accessor, View view, Storage *storage, Transaction *transaction,
    size_t num_chunks) {
  const auto it = index_container_->indices_.find(edge_type);
  MG_ASSERT(it != index_container_->indices_.end(), "Index for edge-type {} doesn't exist", edge_type.AsUint());
  return {it->second->skip_list_.access(),
          std::move(vertex_accessor),
          std::move(edge_accessor),
          edge_type,
          view,
          storage,
          transaction,
          num_chunks};
}

EdgeTypeIndex::AbortProcessor InMemoryEdgeTypeIndex::ActiveIndices::GetAbortProcessor() const {
  auto edge_type_filter = index_container_->indices_ | std::views::keys | ranges::to_vector;
  return AbortProcessor{edge_type_filter};
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

void InMemoryEdgeTypeIndex::CleanupAllIndices() {
  all_indices_.WithLock([](std::shared_ptr<std::vector<AllIndicesEntry> const> &indices) {
    auto keep_condition = [](AllIndicesEntry const &entry) { return entry.use_count() != 1; };
    if (!r::all_of(*indices, keep_condition)) {
      indices = std::make_shared<std::vector<AllIndicesEntry>>(*indices | rv::filter(keep_condition) | r::to_vector);
    }
  });
}

InMemoryEdgeTypeIndex::ChunkedIterable::ChunkedIterable(utils::SkipList<Entry>::Accessor index_accessor,
                                                        utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
                                                        utils::SkipList<Edge>::ConstAccessor edge_accessor,
                                                        EdgeTypeId edge_type, View view, Storage *storage,
                                                        Transaction *transaction, size_t num_chunks)
    : pin_accessor_edge_(std::move(edge_accessor)),
      pin_accessor_vertex_(std::move(vertex_accessor)),
      index_accessor_(std::move(index_accessor)),
      edge_type_(edge_type),
      view_(view),
      storage_(storage),
      transaction_(transaction),
      chunks_{index_accessor_.create_chunks(num_chunks)} {
  // Index can have duplicate entries, we need to make sure each unique entry is inside a single chunk.
  RechunkIndex<utils::SkipList<Entry>>(chunks_, [](const auto &a, const auto &b) { return a.edge == b.edge; });
}

void InMemoryEdgeTypeIndex::ChunkedIterable::Iterator::AdvanceUntilValid() {
  // NOTE: Using the skiplist end here to not store the end iterator in the class
  // The higher level != end will still be correct
  AdvanceUntilValid_(index_iterator_, utils::SkipList<Entry>::ChunkedIterator{}, current_edge_, current_edge_accessor_,
                     self_->transaction_, self_->view_, self_->edge_type_, self_->storage_);
}

}  // namespace memgraph::storage
