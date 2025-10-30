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

#include "storage/v2/inmemory/edge_type_property_index.hpp"

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/edge_info_helpers.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_constants.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/property_value_utils.hpp"
#include "utils/counter.hpp"

namespace r = ranges;
namespace rv = r::views;
namespace memgraph::storage {

namespace {
inline void TryInsertEdgeTypePropertyIndex(Vertex &from_vertex, EdgeTypeId edge_type, PropertyId property,
                                           auto &&index_accessor,
                                           std::optional<SnapshotObserverInfo> const &snapshot_info) {
  if (from_vertex.deleted) {
    return;
  }

  for (auto const &[type, to_vertex, edge_ref] : from_vertex.out_edges) {
    if (type != edge_type || to_vertex->deleted) continue;
    auto property_value = edge_ref.ptr->properties.GetProperty(property);
    if (property_value.IsNull()) {
      continue;
    }

    index_accessor.insert({std::move(property_value), &from_vertex, to_vertex, edge_ref.ptr, 0});
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::EDGES);
    }
  }
}

inline void TryInsertEdgeTypePropertyIndex(Vertex &from_vertex, EdgeTypeId edge_type, PropertyId property,
                                           auto &&index_accessor,
                                           std::optional<SnapshotObserverInfo> const &snapshot_info,
                                           Transaction const &tx) {
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
        Edges_ActionMethod<EdgeDirection::OUT>(edges, edge_type),
      });
      // clang-format on
    });
  }
  if (!exists || deleted || edges.empty()) {
    return;
  }

  for (auto const &[type, to_vertex, edge_ref] : edges) {
    PropertyValue property_value;
    {
      auto guard = std::shared_lock{edge_ref.ptr->lock};
      exists = true;
      deleted = false;
      delta = edge_ref.ptr->delta;
      property_value = edge_ref.ptr->properties.GetProperty(property);
    }
    if (delta) {
      // Edge type is immutable so we don't need to check it
      ApplyDeltasForRead(&tx, delta, View::OLD, [&](const Delta &delta) {
        // clang-format off
        DeltaDispatch(delta, utils::ChainedOverloaded{
          Exists_ActionMethod(exists),
          Deleted_ActionMethod(deleted),
          PropertyValue_ActionMethod(property_value, property),
        });
        // clang-format on
      });
    }

    if (!exists || deleted || property_value.IsNull()) {
      continue;
    }

    index_accessor.insert({std::move(property_value), &from_vertex, to_vertex, edge_ref.ptr, tx.start_timestamp});
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::EDGES);
    }
  }
}

// Free function which advances the given iterator until the next valid entry is found.
void AdvanceUntilValid_(auto &index_iterator, const auto &end, EdgeRef &current_edge, EdgeAccessor &current_accessor,
                        PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                        const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
                        Transaction *transaction, EdgeTypeId edge_type) {
  for (; index_iterator != end; ++index_iterator) {
    if (index_iterator->edge == current_edge.ptr) {
      continue;
    }

    if (!CanSeeEntityWithTimestamp(index_iterator->timestamp, transaction, view)) {
      continue;
    }

    if (!IsValueIncludedByLowerBound(index_iterator->value, lower_bound)) continue;
    if (!IsValueIncludedByUpperBound(index_iterator->value, upper_bound)) {
      index_iterator = end;
      break;
    }

    if (!CurrentEdgeVersionHasProperty(*index_iterator->edge, property, index_iterator->value, transaction, view)) {
      continue;
    }

    auto *from_vertex = index_iterator->from_vertex;
    auto *to_vertex = index_iterator->to_vertex;
    auto edge_ref = EdgeRef(index_iterator->edge);

    auto accessor = EdgeAccessor{edge_ref, edge_type, from_vertex, to_vertex, storage, transaction};
    // TODO: Do we even need this since we performed CurrentVersionHasProperty?
    if (!accessor.IsVisible(view)) {
      continue;
    }

    current_edge = edge_ref;
    current_accessor = accessor;
    break;
  }
}
}  // namespace

bool InMemoryEdgeTypePropertyIndex::Entry::operator<(const PropertyValue &rhs) const { return value < rhs; }

bool InMemoryEdgeTypePropertyIndex::Entry::operator==(const PropertyValue &rhs) const { return value == rhs; }

bool InMemoryEdgeTypePropertyIndex::RegisterIndex(EdgeTypeId edge_type, PropertyId property) {
  return index_.WithLock([&](std::shared_ptr<IndexContainer const> &index_container) {
    {
      auto it = index_container->find({edge_type, property});
      if (it != index_container->end()) return false;
    }

    auto new_container = std::make_shared<IndexContainer>(*index_container);
    auto [new_it, _] = new_container->emplace(std::make_pair(edge_type, property), std::make_shared<IndividualIndex>());

    utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_blocker;
    // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
    all_indices_.WithLock([&](auto &all_indices) {
      auto new_all_indices = *all_indices;
      // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
      new_all_indices.emplace_back(new_it->second, property);
      all_indices = std::make_shared<std::vector<AllIndicesEntry>>(std::move(new_all_indices));
    });
    index_container = std::move(new_container);
    return true;
  });
}

bool InMemoryEdgeTypePropertyIndex::PublishIndex(EdgeTypeId edge_type, PropertyId property, uint64_t commit_timestamp) {
  auto index = GetIndividualIndex(edge_type, property);
  if (!index) return false;
  index->Publish(commit_timestamp);
  return true;
}

void InMemoryEdgeTypePropertyIndex::IndividualIndex::Publish(uint64_t commit_timestamp) {
  status.Commit(commit_timestamp);
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveEdgeTypePropertyIndices);
}

InMemoryEdgeTypePropertyIndex::IndividualIndex::~IndividualIndex() {
  if (status.IsReady()) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveEdgeTypePropertyIndices);
  }
}

bool InMemoryEdgeTypePropertyIndex::CreateIndexOnePass(EdgeTypeId edge_type, PropertyId property,
                                                       utils::SkipList<Vertex>::Accessor vertices,
                                                       std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto res = RegisterIndex(edge_type, property);
  if (!res) return false;
  auto res2 = PopulateIndex(edge_type, property, std::move(vertices), snapshot_info);
  if (res2.HasError()) {
    MG_ASSERT(false, "Index population can't fail, there was no cancellation callback.");
  }
  return PublishIndex(edge_type, property, 0);
}

bool InMemoryEdgeTypePropertyIndex::DropIndex(EdgeTypeId edge_type, PropertyId property) {
  auto result = index_.WithLock([&](std::shared_ptr<IndexContainer const> &index_container) {
    {
      auto it = index_container->find({edge_type, property});
      if (it == index_container->end()) [[unlikely]] {
        return false;
      }
    }

    auto new_container = std::make_shared<IndexContainer>(*index_container);
    new_container->erase({edge_type, property});
    index_container = std::move(new_container);
    return true;
  });
  CleanupAllIndices();
  return result;
}

auto InMemoryEdgeTypePropertyIndex::GetActiveIndices() const -> std::unique_ptr<EdgeTypePropertyIndex::ActiveIndices> {
  return std::make_unique<ActiveIndices>(index_.WithReadLock(std::identity{}));
}

auto InMemoryEdgeTypePropertyIndex::PopulateIndex(EdgeTypeId edge_type, PropertyId property,
                                                  utils::SkipList<Vertex>::Accessor vertices,
                                                  std::optional<SnapshotObserverInfo> const &snapshot_info,
                                                  Transaction const *tx, CheckCancelFunction cancel_check)
    -> utils::BasicResult<IndexPopulateError> {
  auto index = GetIndividualIndex(edge_type, property);
  if (!index) {
    MG_ASSERT(false, "It should not be possible to remove the index before populating it.");
  }

  try {
    auto const accessor_factory = [&] { return index->skiplist.access(); };
    if (tx) {
      // If we are in a transaction, we need to read the object with the correct MVCC snapshot isolation
      auto const insert_function = [&](Vertex &from_vertex, auto &index_accessor) {
        TryInsertEdgeTypePropertyIndex(from_vertex, edge_type, property, index_accessor, snapshot_info, *tx);
      };
      PopulateIndexDispatch(vertices, accessor_factory, insert_function, std::move(cancel_check),
                            {} /*TODO: parallel*/);
    } else {
      // If we are not in a transaction, we need to read the object as it is. (post recovery)
      auto const insert_function = [&](Vertex &from_vertex, auto &index_accessor) {
        TryInsertEdgeTypePropertyIndex(from_vertex, edge_type, property, index_accessor, snapshot_info);
      };
      PopulateIndexDispatch(vertices, accessor_factory, insert_function, std::move(cancel_check),
                            {} /*TODO: parallel*/);
    }
  } catch (const PopulateCancel &) {
    DropIndex(edge_type, property);
    return IndexPopulateError::Cancellation;
  } catch (const utils::OutOfMemoryException &) {
    DropIndex(edge_type, property);
    throw;
  }
  return {};
}

bool InMemoryEdgeTypePropertyIndex::ActiveIndices::IndexReady(EdgeTypeId edge_type, PropertyId property) const {
  auto it = index_container_->find(std::make_pair(edge_type, property));
  if (it == index_container_->cend()) [[unlikely]] {
    return false;
  }
  return it->second->status.IsReady();
}

std::vector<std::pair<EdgeTypeId, PropertyId>> InMemoryEdgeTypePropertyIndex::ActiveIndices::ListIndices(
    uint64_t start_timestamp) const {
  std::vector<std::pair<EdgeTypeId, PropertyId>> ret;
  ret.reserve(index_container_->size());
  for (const auto &item : *index_container_) {
    ret.emplace_back(item.first);
  }
  return ret;
}

void InMemoryEdgeTypePropertyIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp,
                                                          std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter(2048);
  CleanupAllIndices();
  auto all_indices = all_indices_.WithReadLock(std::identity{});

  for (auto &[index, property] : *all_indices) {
    if (token.stop_requested()) return;

    auto edges_acc = index->skiplist.access();
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
      const bool redundant_duplicate = has_next && it->value == next_it->value &&
                                       it->from_vertex == next_it->from_vertex && it->to_vertex == next_it->to_vertex &&
                                       it->edge == next_it->edge;
      if (redundant_duplicate ||
          !AnyVersionHasProperty(*it->edge, property, it->value, oldest_active_start_timestamp)) {
        edges_acc.remove(*it);
      }

      it = next_it;
    }
  }
}

void InMemoryEdgeTypePropertyIndex::ActiveIndices::AbortEntries(
    std::pair<EdgeTypeId, PropertyId> edge_type_property,
    std::span<std::tuple<Vertex *const, Vertex *const, Edge *const, PropertyValue> const> edges,
    uint64_t exact_start_timestamp) {
  auto it = index_container_->find(edge_type_property);
  if (it == index_container_->end()) [[unlikely]] {
    return;
  }

  auto acc = it->second->skiplist.access();
  for (const auto &[from_vertex, to_vertex, edge, value] : edges) {
    acc.remove(Entry{value, from_vertex, to_vertex, edge, exact_start_timestamp});
  }
}

void InMemoryEdgeTypePropertyIndex::ActiveIndices::UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex,
                                                                       Edge *edge, EdgeTypeId edge_type,
                                                                       PropertyId property, PropertyValue value,
                                                                       uint64_t timestamp) {
  if (value.IsNull()) {
    return;
  }

  auto it = index_container_->find({edge_type, property});
  if (it == index_container_->end()) [[unlikely]]
    return;

  auto acc = it->second->skiplist.access();
  acc.insert({value, from_vertex, to_vertex, edge, timestamp});
}

uint64_t InMemoryEdgeTypePropertyIndex::ActiveIndices::ApproximateEdgeCount(EdgeTypeId edge_type,
                                                                            PropertyId property) const {
  if (auto it = index_container_->find({edge_type, property}); it != index_container_->end()) [[likely]] {
    return it->second->skiplist.size();
  }
  return 0U;
}

uint64_t InMemoryEdgeTypePropertyIndex::ActiveIndices::ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                                                            const PropertyValue &value) const {
  auto it = index_container_->find({edge_type, property});
  MG_ASSERT(it != index_container_->end(), "Index for edge type {} and property {} doesn't exist", edge_type.AsUint(),
            property.AsUint());
  auto acc = it->second->skiplist.access();
  if (!value.IsNull()) {
    // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
    return acc.estimate_count(value, utils::SkipListLayerForCountEstimation(acc.size()));
  }
  // The value `Null` won't ever appear in the index because it indicates that
  // the property shouldn't exist. Instead, this value is used as an indicator
  // to estimate the average number of equal elements in the list (for any
  // given value).
  return acc.estimate_average_number_of_equals(
      [](const auto &first, const auto &second) { return first.value == second.value; },
      // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
      utils::SkipListLayerForAverageEqualsEstimation(acc.size()));
}

uint64_t InMemoryEdgeTypePropertyIndex::ActiveIndices::ApproximateEdgeCount(
    EdgeTypeId edge_type, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
    const std::optional<utils::Bound<PropertyValue>> &upper) const {
  auto it = index_container_->find({edge_type, property});
  MG_ASSERT(it != index_container_->end(), "Index for edge type {} and property {} doesn't exist", edge_type.AsUint(),
            property.AsUint());
  auto acc = it->second->skiplist.access();
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
  return acc.estimate_range_count(lower, upper, utils::SkipListLayerForCountEstimation(acc.size()));
}

void InMemoryEdgeTypePropertyIndex::DropGraphClearIndices() {
  index_.WithLock([](std::shared_ptr<IndexContainer const> &index) { index = std::make_shared<IndexContainer>(); });
  all_indices_.WithLock([](std::shared_ptr<std::vector<AllIndicesEntry> const> &all_indices) {
    all_indices = std::make_unique<std::vector<AllIndicesEntry>>();
  });
}

InMemoryEdgeTypePropertyIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor,
                                                  utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
                                                  utils::SkipList<Edge>::ConstAccessor edge_accessor,
                                                  EdgeTypeId edge_type, PropertyId property,
                                                  const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                  const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                  View view, Storage *storage, Transaction *transaction)
    : pin_accessor_edge_(std::move(edge_accessor)),
      pin_accessor_vertex_(std::move(vertex_accessor)),
      index_accessor_(std::move(index_accessor)),
      edge_type_(edge_type),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      bounds_valid_(ValidateBounds(lower_bound_, upper_bound_)),
      view_(view),
      storage_(storage),
      transaction_(transaction) {}

InMemoryEdgeTypePropertyIndex::Iterable::Iterator::Iterator(Iterable *self,
                                                            utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_edge_(nullptr),
      current_accessor_(EdgeRef{nullptr}, EdgeTypeId::FromInt(0), nullptr, nullptr, self_->storage_, nullptr) {
  AdvanceUntilValid();
}

InMemoryEdgeTypePropertyIndex::Iterable::Iterator &InMemoryEdgeTypePropertyIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void InMemoryEdgeTypePropertyIndex::Iterable::Iterator::AdvanceUntilValid() {
  AdvanceUntilValid_(index_iterator_, self_->index_accessor_.end(), current_edge_, current_accessor_, self_->property_,
                     self_->lower_bound_, self_->upper_bound_, self_->view_, self_->storage_, self_->transaction_,
                     self_->edge_type_);
}

void InMemoryEdgeTypePropertyIndex::RunGC() {
  // Remove indices that are not used by any txn
  CleanupAllIndices();

  // For each skip_list remaining, run GC
  auto cpy = all_indices_.WithReadLock(std::identity{});
  for (auto &entry : *cpy) {
    entry.index_->skiplist.run_gc();
  }
}

InMemoryEdgeTypePropertyIndex::Iterable InMemoryEdgeTypePropertyIndex::ActiveIndices::Edges(
    EdgeTypeId edge_type, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
    Transaction *transaction) {
  auto it = index_container_->find({edge_type, property});
  MG_ASSERT(it != index_container_->end(), "Index for edge type {} and property {} doesn't exist", edge_type.AsUint(),
            property.AsUint());
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  auto edge_acc = static_cast<InMemoryStorage const *>(storage)->edges_.access();
  return {it->second->skiplist.access(),
          std::move(vertex_acc),
          std::move(edge_acc),
          edge_type,
          property,
          lower_bound,
          upper_bound,
          view,
          storage,
          transaction};
}

InMemoryEdgeTypePropertyIndex::ChunkedIterable InMemoryEdgeTypePropertyIndex::ActiveIndices::ChunkedEdges(
    EdgeTypeId edge_type, PropertyId property, utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
    utils::SkipList<Edge>::ConstAccessor edge_accessor, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
    Transaction *transaction, size_t num_chunks) {
  auto it = index_container_->find({edge_type, property});
  MG_ASSERT(it != index_container_->end(), "Index for edge type {} and property {} doesn't exist", edge_type.AsUint(),
            property.AsUint());
  return {it->second->skiplist.access(),
          std::move(vertex_accessor),
          std::move(edge_accessor),
          edge_type,
          property,
          lower_bound,
          upper_bound,
          view,
          storage,
          transaction,
          num_chunks};
}

EdgeTypePropertyIndex::AbortProcessor InMemoryEdgeTypePropertyIndex::ActiveIndices::GetAbortProcessor() const {
  auto edge_type_property_filter = *index_container_ | std::views::keys | ranges::to_vector;
  return AbortProcessor{edge_type_property_filter};
}

void InMemoryEdgeTypePropertyIndex::ActiveIndices::AbortEntries(EdgeTypePropertyIndex::AbortableInfo const &info,
                                                                uint64_t start_timestamp) {
  for (auto const &[key, edges] : info) {
    auto it = index_container_->find(key);
    if (it == index_container_->end()) [[unlikely]] {
      return;
    }

    auto acc = it->second->skiplist.access();
    for (const auto &[from_vertex, to_vertex, edge, value] : edges) {
      acc.remove(Entry{std::move(value), from_vertex, to_vertex, edge, start_timestamp});
    }
  }
}

auto InMemoryEdgeTypePropertyIndex::GetIndividualIndex(EdgeTypeId edge_type, PropertyId property) const
    -> std::shared_ptr<IndividualIndex> {
  return index_.WithReadLock(
      [&](std::shared_ptr<IndexContainer const> const &index) -> std::shared_ptr<IndividualIndex> {
        auto it = index->find({edge_type, property});
        if (it == index->cend()) [[unlikely]]
          return {};
        return it->second;
      });
}

void InMemoryEdgeTypePropertyIndex::CleanupAllIndices() {
  all_indices_.WithLock([](std::shared_ptr<std::vector<AllIndicesEntry> const> &indices) {
    auto keep_condition = [](AllIndicesEntry const &entry) { return entry.index_.use_count() != 1; };
    if (!r::all_of(*indices, keep_condition)) {
      indices = std::make_shared<std::vector<AllIndicesEntry>>(*indices | rv::filter(keep_condition) | r::to_vector);
    }
  });
}

InMemoryEdgeTypePropertyIndex::ChunkedIterable::ChunkedIterable(
    utils::SkipList<Entry>::Accessor index_accessor, utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
    utils::SkipList<Edge>::ConstAccessor edge_accessor, EdgeTypeId edge_type, PropertyId property,
    const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
    Transaction *transaction, size_t num_chunks)
    : pin_accessor_edge_(std::move(edge_accessor)),
      pin_accessor_vertex_(std::move(vertex_accessor)),
      index_accessor_(std::move(index_accessor)),
      edge_type_(edge_type),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      bounds_valid_(ValidateBounds(lower_bound_, upper_bound_)),
      view_(view),
      storage_(storage),
      transaction_(transaction) {
  if (!bounds_valid_) return;

  // Chunk with bounds
  const auto lower_bound_pv = lower_bound_ ? std::optional<PropertyValue>{lower_bound_->value()} : std::nullopt;
  const auto upper_bound_pv = upper_bound_ ? std::optional<PropertyValue>{upper_bound_->value()} : std::nullopt;
  chunks_ = index_accessor_.create_chunks(num_chunks, lower_bound_pv, upper_bound_pv);

  // Index can have duplicate entries, we need to make sure each unique entry is inside a single chunk.
  RechunkIndex<utils::SkipList<Entry>>(
      chunks_, [](const auto &a, const auto &b) { return a.edge == b.edge && a.value == b.value; });
}

void InMemoryEdgeTypePropertyIndex::ChunkedIterable::Iterator::AdvanceUntilValid() {
  // NOTE: Using the skiplist end here to not store the end iterator in the class
  // The higher level != end will still be correct
  AdvanceUntilValid_(index_iterator_, utils::SkipList<Entry>::ChunkedIterator{}, current_edge_, current_edge_accessor_,
                     self_->property_, self_->lower_bound_, self_->upper_bound_, self_->view_, self_->storage_,
                     self_->transaction_, self_->edge_type_);
}

}  // namespace memgraph::storage
