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

bool InMemoryEdgePropertyIndex::Entry::operator<(const PropertyValue &rhs) const { return value < rhs; }

bool InMemoryEdgePropertyIndex::Entry::operator==(const PropertyValue &rhs) const { return value == rhs; }

bool InMemoryEdgePropertyIndex::CreateIndex(PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
                                            std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto [it, emplaced] = index_.try_emplace(property);
  if (!emplaced) {
    return false;
  }

  const utils::MemoryTracker::OutOfMemoryExceptionEnabler oom_exception;
  try {
    auto edge_acc = it->second.access();
    for (auto &from_vertex : vertices) {
      if (from_vertex.deleted) {
        continue;
      }

      for (auto &edge : from_vertex.out_edges) {
        const auto type = std::get<EdgeTypeId>(edge);
        auto *to_vertex = std::get<Vertex *>(edge);
        if (to_vertex->deleted) {
          continue;
        }
        auto *edge_ptr = std::get<EdgeRef>(edge).ptr;
        edge_acc.insert({edge_ptr->properties.GetProperty(property), &from_vertex, to_vertex, edge_ptr, type, 0});
        if (snapshot_info) {
          snapshot_info->Update(UpdateType::EDGES);
        }
      }
    }
  } catch (const utils::OutOfMemoryException &) {
    const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
    index_.erase(it);
    throw;
  }

  return true;
}

bool InMemoryEdgePropertyIndex::DropIndex(PropertyId property) { return index_.erase(property) > 0; }

bool InMemoryEdgePropertyIndex::IndexExists(PropertyId property) const { return index_.find(property) != index_.end(); }

std::vector<PropertyId> InMemoryEdgePropertyIndex::ListIndices() const {
  std::vector<PropertyId> ret;
  ret.reserve(index_.size());
  for (const auto &item : index_) {
    ret.emplace_back(item.first);
  }
  return ret;
}

void InMemoryEdgePropertyIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter(2048);

  for (auto &[property_id, index] : index_) {
    if (token.stop_requested()) return;

    auto edges_acc = index.access();
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

void InMemoryEdgePropertyIndex::AbortEntries(
    std::pair<EdgeTypeId, PropertyId> edge_property,
    std::span<std::tuple<Vertex *const, Vertex *const, Edge *const, PropertyValue> const> edges,
    uint64_t exact_start_timestamp) {
  auto it = index_.find(edge_property.second);
  if (it == index_.end()) {
    return;
  }

  auto acc = it->second.access();
  for (const auto &[from_vertex, to_vertex, edge, value] : edges) {
    acc.remove(Entry{value, from_vertex, to_vertex, edge, edge_property.first, exact_start_timestamp});
  }
}

void InMemoryEdgePropertyIndex::UpdateOnSetProperty(Vertex *from_vertex, Vertex *to_vertex, Edge *edge,
                                                    EdgeTypeId edge_type, PropertyId property, PropertyValue value,
                                                    uint64_t timestamp) {
  if (value.IsNull()) {
    return;
  }

  auto it = index_.find(property);
  if (it == index_.end()) return;

  auto acc = it->second.access();
  acc.insert({value, from_vertex, to_vertex, edge, edge_type, timestamp});
}

uint64_t InMemoryEdgePropertyIndex::ApproximateEdgeCount(PropertyId property) const {
  if (auto it = index_.find(property); it != index_.end()) {
    return it->second.size();
  }

  return 0U;
}

uint64_t InMemoryEdgePropertyIndex::ApproximateEdgeCount(PropertyId property, const PropertyValue &value) const {
  auto it = index_.find(property);
  MG_ASSERT(it != index_.end(), "Index for edge property {} doesn't exist", property.AsUint());
  auto acc = it->second.access();
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

uint64_t InMemoryEdgePropertyIndex::ApproximateEdgeCount(
    PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
    const std::optional<utils::Bound<PropertyValue>> &upper) const {
  auto it = index_.find(property);
  MG_ASSERT(it != index_.end(), "Index for edge property {} doesn't exist", property.AsUint());
  auto acc = it->second.access();
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
  return acc.estimate_range_count(lower, upper, utils::SkipListLayerForCountEstimation(acc.size()));
}

void InMemoryEdgePropertyIndex::UpdateOnEdgeModification(Vertex * /*old_from*/, Vertex * /*old_to*/, Vertex *new_from,
                                                         Vertex *new_to, EdgeRef edge_ref, EdgeTypeId edge_type,
                                                         PropertyId property, const PropertyValue &value,
                                                         const Transaction &tx) {
  auto it = index_.find(property);
  if (it == index_.end()) {
    return;
  }

  auto acc = it->second.access();
  acc.insert(Entry{value, new_from, new_to, edge_ref.ptr, edge_type, tx.start_timestamp});
}

void InMemoryEdgePropertyIndex::DropGraphClearIndices() { index_.clear(); }

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
  for (auto &index_entry : index_) {
    index_entry.second.run_gc();
  }
}

InMemoryEdgePropertyIndex::Iterable InMemoryEdgePropertyIndex::Edges(
    PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
    Transaction *transaction) {
  auto it = index_.find(property);
  MG_ASSERT(it != index_.end(), "Index for edge property {} doesn't exist", property.AsUint());
  auto vertex_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  auto edge_acc = static_cast<InMemoryStorage const *>(storage)->edges_.access();
  return {it->second.access(),
          std::move(vertex_acc),
          std::move(edge_acc),
          property,
          lower_bound,
          upper_bound,
          view,
          storage,
          transaction};
}

}  // namespace memgraph::storage
