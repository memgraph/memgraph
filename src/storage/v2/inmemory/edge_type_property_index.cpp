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
#include "storage/v2/inmemory/property_constants.hpp"
#include "storage/v2/property_value.hpp"
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
  ApplyDeltasForRead(transaction, delta, view, [&edge, &vertex, &link](const Delta &delta) {
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
        }
        break;
      }
      case Delta::Action::ADD_OUT_EDGE: {
        if (edge == delta.vertex_edge.edge.ptr) {
          link = {delta.vertex_edge.edge_type, delta.vertex_edge.vertex, delta.vertex_edge.edge};
          auto it = std::find(vertex->out_edges.begin(), vertex->out_edges.end(), link);
          MG_ASSERT(it == vertex->out_edges.end(), "Invalid database state!");
        }
        break;
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
  return std::tie(value, edge->gid, from_vertex->gid, to_vertex->gid, timestamp) <
         std::tie(rhs.value, rhs.edge->gid, rhs.from_vertex->gid, rhs.to_vertex->gid, rhs.timestamp);
}

bool InMemoryEdgeTypePropertyIndex::Entry::operator==(const Entry &rhs) const {
  return std::tie(value, edge, from_vertex, to_vertex, timestamp) ==
         std::tie(rhs.value, rhs.edge, rhs.from_vertex, rhs.to_vertex, rhs.timestamp);
}

bool InMemoryEdgeTypePropertyIndex::Entry::operator<(const PropertyValue &rhs) const { return value < rhs; }

bool InMemoryEdgeTypePropertyIndex::Entry::operator==(const PropertyValue &rhs) const { return value == rhs; }

bool InMemoryEdgeTypePropertyIndex::CreateIndex(EdgeTypeId edge_type, PropertyId property,
                                                utils::SkipList<Vertex>::Accessor vertices) {
  auto [it, emplaced] = index_.try_emplace({edge_type, property});
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
        const auto type = std::get<kEdgeTypeIdPos>(edge);
        if (type != edge_type) {
          continue;
        }
        auto *to_vertex = std::get<kVertexPos>(edge);
        if (to_vertex->deleted) {
          continue;
        }
        auto *edge_ptr = std::get<kEdgeRefPos>(edge).ptr;
        edge_acc.insert({edge_ptr->properties.GetProperty(property), &from_vertex, to_vertex, edge_ptr, 0});
      }
    }
  } catch (const utils::OutOfMemoryException &) {
    const utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
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
    ret.emplace_back(item.first.first, item.first.second);
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

      const bool vertices_deleted = it->from_vertex->deleted || it->to_vertex->deleted;
      const bool has_next = next_it != edges_acc.end();

      // When we update specific entries in the index, we don't delete the previous entry.
      // The way they are removed from the index is through this check. The entries should
      // be right next to each other(in terms of iterator semantics) and the older one
      // should be removed here.
      const bool redundant_duplicate = has_next && it->value == next_it->value &&
                                       it->from_vertex == next_it->from_vertex && it->to_vertex == next_it->to_vertex &&
                                       it->edge == next_it->edge;
      if (redundant_duplicate || vertices_deleted ||
          !AnyVersionHasLabelProperty(*it->edge, specific_index.first.second, it->value,
                                      oldest_active_start_timestamp)) {
        edges_acc.remove(*it);
      }

      it = next_it;
    }
  }
}

void InMemoryEdgeTypePropertyIndex::AbortEntries(
    std::pair<EdgeTypeId, PropertyId> edge_type_property,
    std::span<std::tuple<Vertex *const, Vertex *const, Edge *const, PropertyValue> const> edges,
    uint64_t exact_start_timestamp) {
  auto it = index_.find(edge_type_property);
  if (it == index_.end()) {
    return;
  }

  auto acc = it->second.access();
  for (const auto &[from_vertex, to_vertex, edge, value] : edges) {
    acc.remove(Entry{value, from_vertex, to_vertex, edge, exact_start_timestamp});
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

  return 0U;
}

uint64_t InMemoryEdgeTypePropertyIndex::ApproximateEdgeCount(EdgeTypeId edge_type, PropertyId property,
                                                             const PropertyValue &value) const {
  auto it = index_.find({edge_type, property});
  MG_ASSERT(it != index_.end(), "Index for edge type {} and property {} doesn't exist", edge_type.AsUint(),
            property.AsUint());
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

uint64_t InMemoryEdgeTypePropertyIndex::ApproximateEdgeCount(
    EdgeTypeId edge_type, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
    const std::optional<utils::Bound<PropertyValue>> &upper) const {
  auto it = index_.find({edge_type, property});
  MG_ASSERT(it != index_.end(), "Index for edge type {} and property {} doesn't exist", edge_type.AsUint(),
            property.AsUint());
  auto acc = it->second.access();
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
  return acc.estimate_range_count(lower, upper, utils::SkipListLayerForCountEstimation(acc.size()));
}

void InMemoryEdgeTypePropertyIndex::UpdateOnEdgeModification(Vertex *old_from, Vertex *old_to, Vertex *new_from,
                                                             Vertex *new_to, EdgeRef edge_ref, EdgeTypeId edge_type,
                                                             PropertyId property, const PropertyValue &value,
                                                             const Transaction &tx) {
  auto it = index_.find({edge_type, property});
  if (it == index_.end()) {
    return;
  }

  auto acc = it->second.access();
  acc.insert(Entry{value, new_from, new_to, edge_ref.ptr, tx.start_timestamp});
}

void InMemoryEdgeTypePropertyIndex::DropGraphClearIndices() { index_.clear(); }

InMemoryEdgeTypePropertyIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor, EdgeTypeId edge_type,
                                                  PropertyId property,
                                                  const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                                  const std::optional<utils::Bound<PropertyValue>> &upper_bound,
                                                  View view, Storage *storage, Transaction *transaction)
    : index_accessor_(std::move(index_accessor)),
      edge_type_(edge_type),
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
    // Here we need to supply an upper bound. The upper bound is set to an
    // exclusive lower bound of the following type.
    switch (lower_bound_->value().type()) {
      case PropertyValue::Type::Null:
        // This shouldn't happen because of the nullopt-ing above.
        LOG_FATAL("Invalid database state!");
        break;
      case PropertyValue::Type::Bool:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestNumber);
        break;
      case PropertyValue::Type::Int:
      case PropertyValue::Type::Double:
        // Both integers and doubles are treated as the same type in
        // `PropertyValue` and they are interleaved when sorted.
        upper_bound_ = utils::MakeBoundExclusive(kSmallestString);
        break;
      case PropertyValue::Type::String:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestList);
        break;
      case PropertyValue::Type::List:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestMap);
        break;
      case PropertyValue::Type::Map:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestTemporalData);
        break;
      case PropertyValue::Type::TemporalData:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestZonedTemporalData);
        break;
      case PropertyValue::Type::ZonedTemporalData:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestEnum);
        break;
      case PropertyValue::Type::Enum:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestPoint2d);
        break;
      case PropertyValue::Type::Point2d:
        upper_bound_ = utils::MakeBoundExclusive(kSmallestPoint3d);
        break;
      case PropertyValue::Type::Point3d:
        // This is the last type in the order so we leave the upper bound empty.
        break;
    }
  }
  if (upper_bound_ && !lower_bound_) {
    // Here we need to supply a lower bound. The lower bound is set to an
    // inclusive lower bound of the current type.
    switch (upper_bound_->value().type()) {
      case PropertyValue::Type::Null:
        // This shouldn't happen because of the nullopt-ing above.
        LOG_FATAL("Invalid database state!");
        break;
      case PropertyValue::Type::Bool:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestBool);
        break;
      case PropertyValue::Type::Int:
      case PropertyValue::Type::Double:
        // Both integers and doubles are treated as the same type in
        // `PropertyValue` and they are interleaved when sorted.
        lower_bound_ = utils::MakeBoundInclusive(kSmallestNumber);
        break;
      case PropertyValue::Type::String:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestString);
        break;
      case PropertyValue::Type::List:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestList);
        break;
      case PropertyValue::Type::Map:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestMap);
        break;
      case PropertyValue::Type::TemporalData:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestTemporalData);
        break;
      case PropertyValue::Type::ZonedTemporalData:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestZonedTemporalData);
        break;
      case PropertyValue::Type::Enum:
        lower_bound_ = utils::MakeBoundInclusive(kSmallestEnum);
        break;
      case PropertyValue::Type::Point2d:
        lower_bound_ = utils::MakeBoundExclusive(kSmallestPoint2d);
        break;
      case PropertyValue::Type::Point3d:
        lower_bound_ = utils::MakeBoundExclusive(kSmallestPoint3d);
        break;
    }
  }
}

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
  for (; index_iterator_ != self_->index_accessor_.end(); ++index_iterator_) {
    if (index_iterator_->edge == current_edge_.ptr) {
      continue;
    }

    if (!CanSeeEntityWithTimestamp(index_iterator_->timestamp, self_->transaction_)) {
      continue;
    }

    if (self_->lower_bound_) {
      if (index_iterator_->value < self_->lower_bound_->value()) {
        continue;
      }
      if (!self_->lower_bound_->IsInclusive() && index_iterator_->value == self_->lower_bound_->value()) {
        continue;
      }
    }
    if (self_->upper_bound_) {
      if (self_->upper_bound_->value() < index_iterator_->value) {
        index_iterator_ = self_->index_accessor_.end();
        break;
      }
      if (!self_->upper_bound_->IsInclusive() && index_iterator_->value == self_->upper_bound_->value()) {
        index_iterator_ = self_->index_accessor_.end();
        break;
      }
    }

    if (!CurrentEdgeVersionHasProperty(*index_iterator_->edge, self_->property_, index_iterator_->value,
                                       self_->transaction_, self_->view_)) {
      continue;
    }

    auto *from_vertex = index_iterator_->from_vertex;
    auto *to_vertex = index_iterator_->to_vertex;
    auto edge_ref = EdgeRef(index_iterator_->edge);

    auto accessor =
        EdgeAccessor{edge_ref, self_->edge_type_, from_vertex, to_vertex, self_->storage_, self_->transaction_};
    // TODO: Do we even need this since we performed CurrentVersionHasProperty?
    if (!accessor.IsVisible(self_->view_)) {
      continue;
    }

    current_edge_ = edge_ref;
    current_accessor_ = accessor;
    break;
  }
}

void InMemoryEdgeTypePropertyIndex::RunGC() {
  for (auto &index_entry : index_) {
    index_entry.second.run_gc();
  }
}

InMemoryEdgeTypePropertyIndex::Iterable InMemoryEdgeTypePropertyIndex::Edges(
    EdgeTypeId edge_type, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
    Transaction *transaction) {
  auto it = index_.find({edge_type, property});
  MG_ASSERT(it != index_.end(), "Index for edge type {} and property {} doesn't exist", edge_type.AsUint(),
            property.AsUint());
  return {it->second.access(), edge_type, property, lower_bound, upper_bound, view, storage, transaction};
}

}  // namespace memgraph::storage
