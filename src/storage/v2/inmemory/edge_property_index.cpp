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

#include "storage/v2/inmemory/edge_property_index.hpp"

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/edge_info_helpers.hpp"
#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/property_value_utils.hpp"
#include "utils/counter.hpp"

namespace r = ranges;
namespace rv = r::views;

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

}  // namespace

namespace memgraph::storage {

namespace {
inline void TryInsertEdgePropertyIndex(Vertex &from_vertex, PropertyId property, auto &&index_accessor,
                                       std::optional<SnapshotObserverInfo> const &snapshot_info) {
  if (from_vertex.deleted) {
    return;
  }
  for (auto const &[edge_type, to_vertex, edge_ref] : from_vertex.out_edges) {
    if (to_vertex->deleted) {
      continue;
    }
    auto value = edge_ref.ptr->properties.GetProperty(property);
    if (value.IsNull()) {
      continue;
    }
    index_accessor.insert({std::move(value), &from_vertex, to_vertex, edge_ref.ptr, edge_type, 0});
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::EDGES);
    }
  }
}

inline void TryInsertEdgePropertyIndex(Vertex &from_vertex, PropertyId property, auto &&index_accessor,
                                       std::optional<SnapshotObserverInfo> const &snapshot_info,
                                       Transaction const &tx) {
  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  utils::small_vector<Vertex::EdgeTriple> edges;

  {
    auto guard = std::shared_lock{from_vertex.lock};
    deleted = from_vertex.deleted;
    delta = from_vertex.delta;
    edges = from_vertex.out_edges;
  }

  // Create and drop index will always use snapshot isolation
  if (delta) {
    ApplyDeltasForRead(&tx, delta, View::OLD, [&](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Exists_ActionMethod(exists),
        Deleted_ActionMethod(deleted),
        Edges_ActionMethod<EdgeDirection::OUT>(edges),
      });
      // clang-format on
    });
  }
  if (!exists || deleted || edges.empty()) {
    return;
  }

  for (auto const &[edge_type, to_vertex, edge_ref] : edges) {
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

    index_accessor.insert({property_value, &from_vertex, to_vertex, edge_ref.ptr, edge_type, tx.start_timestamp});
    if (snapshot_info) {
      snapshot_info->Update(UpdateType::EDGES);
    }
  }
}
}  // namespace

bool InMemoryEdgePropertyIndex::CreateIndexOnePass(PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                                                   std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto res = RegisterIndex(property);
  if (!res) return false;
  auto res2 = PopulateIndex(property, std::move(vertices), snapshot_info);
  if (res2.HasError()) {
    MG_ASSERT(false, "Index population can't fail, there was no cancellation callback.");
  }
  return PublishIndex(property, 0);
}

bool InMemoryEdgePropertyIndex::RegisterIndex(PropertyId property) {
  return index_.WithLock([&](std::shared_ptr<IndicesContainer const> &indices_container) {
    auto const &indices = indices_container->indices_;
    {
      auto it = indices.find(property);
      if (it != indices.end()) return false;  // already exists
    }

    utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
    // Register
    auto new_container = std::make_shared<IndicesContainer>(*indices_container);
    auto [new_it, _] = new_container->indices_.emplace(property, std::make_shared<IndividualIndex>());
    // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
    all_indices_.WithLock([&](auto &all_indices) {
      auto new_all_indices = *all_indices;
      // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
      new_all_indices.emplace_back(property, new_it->second);
      all_indices = std::make_shared<std::vector<AllIndicesEntry>>(std::move(new_all_indices));
    });
    indices_container = new_container;
    return true;
  });
}

auto InMemoryEdgePropertyIndex::PopulateIndex(PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                                              std::optional<SnapshotObserverInfo> const &snapshot_info,
                                              Transaction const *tx, CheckCancelFunction cancel_check)
    -> utils::BasicResult<IndexPopulateError> {
  auto index = GetIndividualIndex(property);
  if (!index) {
    MG_ASSERT(false, "It should not be possible to remove the index before populating it.");
  }

  try {
    auto const accessor_factory = [&] { return index->skip_list_.access(); };
    if (tx) {
      // If we are in a transaction, we need to read the object with the correct MVCC snapshot isolation
      auto const insert_function = [&](Vertex &from_vertex, auto &index_accessor) {
        TryInsertEdgePropertyIndex(from_vertex, property, index_accessor, snapshot_info, *tx);
      };
      PopulateIndexDispatch(vertices, accessor_factory, insert_function, std::move(cancel_check),
                            {} /*TODO: parallel*/);
    } else {
      // If we are not in a transaction, we need to read the object as it is. (post recovery)
      auto const insert_function = [&](Vertex &from_vertex, auto &index_accessor) {
        TryInsertEdgePropertyIndex(from_vertex, property, index_accessor, snapshot_info);
      };
      PopulateIndexDispatch(vertices, accessor_factory, insert_function, std::move(cancel_check),
                            {} /*TODO: parallel*/);
    }
  } catch (const PopulateCancel &) {
    DropIndex(property);
    return IndexPopulateError::Cancellation;
  } catch (const utils::OutOfMemoryException &) {
    DropIndex(property);
    throw;
  }
  return {};
}

bool InMemoryEdgePropertyIndex::PublishIndex(PropertyId property, uint64_t commit_timestamp) {
  auto index = GetIndividualIndex(property);
  if (!index) return false;
  index->Publish(commit_timestamp);
  return true;
}

void InMemoryEdgePropertyIndex::IndividualIndex::Publish(uint64_t commit_timestamp) {
  status_.Commit(commit_timestamp);
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveEdgePropertyIndices);
}

InMemoryEdgePropertyIndex::IndividualIndex::~IndividualIndex() {
  if (status_.IsReady()) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveEdgePropertyIndices);
  }
}

bool InMemoryEdgePropertyIndex::DropIndex(PropertyId property) {
  auto const result = index_.WithLock([&](std::shared_ptr<IndicesContainer const> &indices_container) {
    {
      auto const it = indices_container->indices_.find(property);
      if (it == indices_container->indices_.cend()) return false;
    }

    auto new_container = std::make_shared<IndicesContainer>();
    for (auto const &[existing_property, index] : indices_container->indices_) {
      if (existing_property != property) {
        new_container->indices_.emplace(existing_property, index);
      }
    }
    indices_container = new_container;
    return true;
  });
  CleanupAllIndicies();
  return result;
}

bool InMemoryEdgePropertyIndex::ActiveIndices::IndexExists(PropertyId property) const {
  auto const &indices = index_container_->indices_;
  return indices.find(property) != indices.end();
}

bool InMemoryEdgePropertyIndex::ActiveIndices::IndexReady(PropertyId property) const {
  auto const &indices = index_container_->indices_;
  auto it = indices.find(property);
  if (it == indices.end()) return false;
  return it->second->status_.IsReady();
}

std::vector<PropertyId> InMemoryEdgePropertyIndex::ActiveIndices::ListIndices(uint64_t start_timestamp) const {
  auto ret = std::vector<PropertyId>{};
  ret.reserve(index_container_->indices_.size());
  for (auto const &[property, index] : index_container_->indices_) {
    if (index->status_.IsVisible(start_timestamp)) {
      ret.emplace_back(property);
    }
  }
  return ret;
}

void InMemoryEdgePropertyIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter(2048);

  CleanupAllIndicies();

  auto cpy = all_indices_.WithReadLock(std::identity{});

  for (auto &[property_id, index] : *cpy) {
    if (token.stop_requested()) return;

    auto edges_acc = index->skip_list_.access();
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
      const bool redundant_duplicate = has_next && it->value == next_it->value &&
                                       it->from_vertex == next_it->from_vertex && it->to_vertex == next_it->to_vertex &&
                                       it->edge == next_it->edge;
      if (redundant_duplicate || vertices_deleted || edge_deleted ||
          !AnyVersionHasProperty(*it->edge, property_id, it->value, oldest_active_start_timestamp)) {
        edges_acc.remove(*it);
      }

      it = next_it;
    }
  }
}

void InMemoryEdgePropertyIndex::ActiveIndices::UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge,
                                                                   EdgeTypeId edge_type, PropertyId property,
                                                                   PropertyValue value, uint64_t timestamp) {
  if (value.IsNull()) {
    return;
  }

  auto it = index_container_->indices_.find(property);
  if (it == index_container_->indices_.end()) return;

  auto acc = it->second->skip_list_.access();
  acc.insert({value, from_vertex, to_vertex, edge, edge_type, timestamp});
}

uint64_t InMemoryEdgePropertyIndex::ActiveIndices::ApproximateEdgeCount(PropertyId property) const {
  if (auto it = index_container_->indices_.find(property); it != index_container_->indices_.end()) {
    return it->second->skip_list_.size();
  }

  return 0U;
}

uint64_t InMemoryEdgePropertyIndex::ActiveIndices::ApproximateEdgeCount(PropertyId property,
                                                                        const PropertyValue &value) const {
  auto it = index_container_->indices_.find(property);
  MG_ASSERT(it != index_container_->indices_.end(), "Index for edge property {} doesn't exist", property.AsUint());
  auto acc = it->second->skip_list_.access();
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

uint64_t InMemoryEdgePropertyIndex::ActiveIndices::ApproximateEdgeCount(
    PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
    const std::optional<utils::Bound<PropertyValue>> &upper) const {
  auto it = index_container_->indices_.find(property);
  MG_ASSERT(it != index_container_->indices_.end(), "Index for edge property {} doesn't exist", property.AsUint());
  auto acc = it->second->skip_list_.access();
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
  return acc.estimate_range_count(lower, upper, utils::SkipListLayerForCountEstimation(acc.size()));
}

void InMemoryEdgePropertyIndex::DropGraphClearIndices() {
  index_.WithLock([](std::shared_ptr<IndicesContainer const> &index) { index = std::make_shared<IndicesContainer>(); });
  all_indices_.WithLock([](std::shared_ptr<std::vector<AllIndicesEntry> const> &all_indices) {
    all_indices = std::make_unique<std::vector<AllIndicesEntry>>();
  });
}

InMemoryEdgePropertyIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor,
                                              utils::SkipList<Vertex>::ConstAccessor vertex_accessor,
                                              utils::SkipList<Edge>::ConstAccessor edge_accessor, PropertyId property,
                                              const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                              const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                                              Storage *storage, Transaction *transaction)
    : pin_accessor_edge_(std::move(edge_accessor)),
      pin_accessor_vertex_(std::move(vertex_accessor)),
      index_accessor_(std::move(index_accessor)),
      property_(property),
      lower_bound_(lower_bound),
      upper_bound_(upper_bound),
      view_(view),
      storage_(storage),
      transaction_(transaction) {
  // We have to fix the bounds that the user provided to us. If the user
  // provided only one bound we should make sure that only values of that type
  // are returned by the iterator. We ensure this by supplying either an
  // inclusive lower bound of the same type, or an exclusive upper bound of the
  // following type. If neither bound is set we yield all items in the index.

  // Remove any bounds that are set to `Null` because that isn't a valid value.
  if (lower_bound_ && lower_bound_->value().IsNull()) {
    lower_bound_ = std::nullopt;
  }
  if (upper_bound_ && upper_bound_->value().IsNull()) {
    upper_bound_ = std::nullopt;
  }

  // Check whether the bounds are of comparable types if both are supplied.
  if (lower_bound_ && upper_bound_ && !AreComparableTypes(lower_bound_->value().type(), upper_bound_->value().type())) {
    bounds_valid_ = false;
    return;
  }

  // Set missing bounds.
  if (lower_bound_ && !upper_bound_) {
    upper_bound_ = UpperBoundForType(lower_bound_->value().type());
  }
  if (upper_bound_ && !lower_bound_) {
    lower_bound_ = LowerBoundForType(upper_bound_->value().type());
  }
}

InMemoryEdgePropertyIndex::Iterable::Iterator::Iterator(Iterable *self, utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_edge_(nullptr),
      current_accessor_(EdgeRef{nullptr}, EdgeTypeId::FromInt(0), nullptr, nullptr, self_->storage_, nullptr) {
  AdvanceUntilValid();
}

InMemoryEdgePropertyIndex::Iterable::Iterator &InMemoryEdgePropertyIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void InMemoryEdgePropertyIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_accessor_.end(); ++index_iterator_) {
    if (index_iterator_->edge == current_edge_.ptr) {
      continue;
    }

    if (!CanSeeEntityWithTimestamp(index_iterator_->timestamp, self_->transaction_, self_->view_)) {
      continue;
    }

    if (!IsValueIncludedByLowerBound(index_iterator_->value, self_->lower_bound_)) continue;
    if (!IsValueIncludedByUpperBound(index_iterator_->value, self_->upper_bound_)) {
      index_iterator_ = self_->index_accessor_.end();
      break;
    }

    if (!CurrentEdgeVersionHasProperty(*index_iterator_->edge, self_->property_, index_iterator_->value,
                                       self_->transaction_, self_->view_)) {
      continue;
    }

    auto *from_vertex = index_iterator_->from_vertex;
    auto *to_vertex = index_iterator_->to_vertex;
    auto edge_ref = EdgeRef(index_iterator_->edge);
    auto edge_type = index_iterator_->edge_type;

    auto accessor = EdgeAccessor{edge_ref, edge_type, from_vertex, to_vertex, self_->storage_, self_->transaction_};
    // TODO: Do we even need this since we performed CurrentVersionHasProperty?
    if (!accessor.IsVisible(self_->view_)) {
      continue;
    }

    current_edge_ = edge_ref;
    current_accessor_ = accessor;
    break;
  }
}

void InMemoryEdgePropertyIndex::RunGC() {
  // Remove indicies that are not used by any txn
  CleanupAllIndicies();

  // For each skip_list remaining, run GC
  auto cpy = all_indices_.WithReadLock(std::identity{});
  for (auto &[_, index] : *cpy) {
    index->skip_list_.run_gc();
  }
}

InMemoryEdgePropertyIndex::Iterable InMemoryEdgePropertyIndex::ActiveIndices::Edges(
    PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
    Transaction *transaction) {
  auto it = index_container_->indices_.find(property);
  MG_ASSERT(it != index_container_->indices_.end(), "Index for edge property {} doesn't exist", property.AsUint());
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  auto edge_acc = static_cast<InMemoryStorage const *>(storage)->edges_.access();
  return {it->second->skip_list_.access(),
          std::move(vertex_acc),
          std::move(edge_acc),
          property,
          lower_bound,
          upper_bound,
          view,
          storage,
          transaction};
}

EdgePropertyIndex::AbortProcessor InMemoryEdgePropertyIndex::ActiveIndices::GetAbortProcessor() const {
  auto property_ids_filter = index_container_->indices_ | std::views::keys | ranges::to_vector;
  return AbortProcessor{property_ids_filter};
}

void InMemoryEdgePropertyIndex::ActiveIndices::AbortEntries(EdgePropertyIndex::AbortableInfo const &info,
                                                            uint64_t start_timestamp) {
  for (auto const &[property, edges] : info) {
    auto const it = index_container_->indices_.find(property);
    DMG_ASSERT(it != index_container_->indices_.end());

    auto &index_storage = it->second;
    auto acc = index_storage->skip_list_.access();
    for (const auto &[value, from_vertex, to_vertex, edge, type] : edges) {
      acc.remove(Entry{value, from_vertex, to_vertex, edge, type, start_timestamp});
    }
  }
}

std::unique_ptr<EdgePropertyIndex::ActiveIndices> InMemoryEdgePropertyIndex::GetActiveIndices() const {
  return std::make_unique<ActiveIndices>(index_.WithReadLock(std::identity{}));
}

auto InMemoryEdgePropertyIndex::GetIndividualIndex(PropertyId property) const -> std::shared_ptr<IndividualIndex> {
  return index_.WithReadLock(
      [&](std::shared_ptr<IndicesContainer const> const &index) -> std::shared_ptr<IndividualIndex> {
        auto it = index->indices_.find(property);
        if (it == index->indices_.cend()) [[unlikely]]
          return {};
        return it->second;
      });
}

void InMemoryEdgePropertyIndex::CleanupAllIndicies() {
  all_indices_.WithLock([](std::shared_ptr<std::vector<AllIndicesEntry> const> &indices) {
    auto keep_condition = [](AllIndicesEntry const &entry) { return entry.second.use_count() != 1; };
    if (!r::all_of(*indices, keep_condition)) {
      indices = std::make_shared<std::vector<AllIndicesEntry>>(*indices | rv::filter(keep_condition) | r::to_vector);
    }
  });
}

}  // namespace memgraph::storage
