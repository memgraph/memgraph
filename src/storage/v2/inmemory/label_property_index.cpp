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

#include <chrono>
#include <cstdint>
#include <limits>

#include "storage/v2/constraints/constraints.hpp"
#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/property_constants.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/temporal.hpp"
#include "utils/bound.hpp"
#include "utils/counter.hpp"
#include "utils/logging.hpp"
#include "utils/temporal.hpp"

namespace memgraph::storage {

bool InMemoryLabelPropertyIndex::Entry::operator<(const Entry &rhs) const {
  if (value < rhs.value) {
    return true;
  }
  if (rhs.value < value) {
    return false;
  }
  return std::make_tuple(vertex, timestamp) < std::make_tuple(rhs.vertex, rhs.timestamp);
}

bool InMemoryLabelPropertyIndex::Entry::operator==(const Entry &rhs) const {
  return value == rhs.value && vertex == rhs.vertex && timestamp == rhs.timestamp;
}

bool InMemoryLabelPropertyIndex::Entry::operator<(const PropertyValue &rhs) const { return value < rhs; }

bool InMemoryLabelPropertyIndex::Entry::operator==(const PropertyValue &rhs) const { return value == rhs; }

bool InMemoryLabelPropertyIndex::CreateIndex(
    LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor vertices,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info) {
  spdlog::trace("Vertices size when creating index: {}", vertices.size());
  auto create_index_seq = [this](LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor &vertices,
                                 std::map<std::pair<LabelId, PropertyId>, utils::SkipList<Entry>>::iterator it) {
    using IndexAccessor = decltype(it->second.access());

    CreateIndexOnSingleThread(vertices, it, index_, std::make_pair(label, property),
                              [](Vertex &vertex, std::pair<LabelId, PropertyId> key, IndexAccessor &index_accessor) {
                                TryInsertLabelPropertyIndex(vertex, key, index_accessor);
                              });

    return true;
  };

  auto create_index_par =
      [this](LabelId label, PropertyId property, utils::SkipList<Vertex>::Accessor &vertices,
             std::map<std::pair<LabelId, PropertyId>, utils::SkipList<Entry>>::iterator label_property_it,
             const durability::ParallelizedSchemaCreationInfo &parallel_exec_info) {
        using IndexAccessor = decltype(label_property_it->second.access());

        CreateIndexOnMultipleThreads(
            vertices, label_property_it, index_, std::make_pair(label, property), parallel_exec_info,
            [](Vertex &vertex, std::pair<LabelId, PropertyId> key, IndexAccessor &index_accessor) {
              TryInsertLabelPropertyIndex(vertex, key, index_accessor);
            });

        return true;
      };

  auto [it, emplaced] =
      index_.emplace(std::piecewise_construct, std::forward_as_tuple(label, property), std::forward_as_tuple());

  indices_by_property_[property].insert({label, &it->second});

  if (!emplaced) {
    // Index already exists.
    return false;
  }

  if (parallel_exec_info) {
    return create_index_par(label, property, vertices, it, *parallel_exec_info);
  }

  return create_index_seq(label, property, vertices, it);
}

void InMemoryLabelPropertyIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update,
                                                  const Transaction &tx) {
  for (auto &[label_prop, storage] : index_) {
    if (label_prop.first != added_label) {
      continue;
    }
    auto prop_value = vertex_after_update->properties.GetProperty(label_prop.second);
    if (!prop_value.IsNull()) {
      auto acc = storage.access();
      acc.insert(Entry{std::move(prop_value), vertex_after_update, tx.start_timestamp});
    }
  }
}

void InMemoryLabelPropertyIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                                     const Transaction &tx) {
  if (value.IsNull()) {
    return;
  }

  auto index = indices_by_property_.find(property);
  if (index == indices_by_property_.end()) {
    return;
  }

  for (const auto &[label, storage] : index->second) {
    if (!utils::Contains(vertex->labels, label)) continue;
    auto acc = storage->access();
    acc.insert(Entry{value, vertex, tx.start_timestamp});
  }
}

bool InMemoryLabelPropertyIndex::DropIndex(LabelId label, PropertyId property) {
  if (indices_by_property_.find(property) != indices_by_property_.end()) {
    indices_by_property_.at(property).erase(label);

    if (indices_by_property_.at(property).empty()) {
      indices_by_property_.erase(property);
    }
  }

  return index_.erase({label, property}) > 0;
}

bool InMemoryLabelPropertyIndex::IndexExists(LabelId label, PropertyId property) const {
  return index_.find({label, property}) != index_.end();
}

std::vector<std::pair<LabelId, PropertyId>> InMemoryLabelPropertyIndex::ListIndices() const {
  std::vector<std::pair<LabelId, PropertyId>> ret;
  ret.reserve(index_.size());
  for (const auto &item : index_) {
    ret.push_back(item.first);
  }
  return ret;
}

void InMemoryLabelPropertyIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter<2048>();

  for (auto &[label_property, index] : index_) {
    auto [label_id, prop_id] = label_property;
    // before starting index, check if stop_requested
    if (token.stop_requested()) return;

    auto index_acc = index.access();
    auto it = index_acc.begin();
    auto end_it = index_acc.end();
    if (it == end_it) continue;
    while (true) {
      // Hot loop, don't check stop_requested every time
      if (maybe_stop() && token.stop_requested()) return;

      auto next_it = it;
      ++next_it;

      bool has_next = next_it != end_it;
      if (it->timestamp < oldest_active_start_timestamp) {
        bool redundant_duplicate = has_next && it->vertex == next_it->vertex && it->value == next_it->value;
        if (redundant_duplicate ||
            !AnyVersionHasLabelProperty(*it->vertex, label_id, prop_id, it->value, oldest_active_start_timestamp)) {
          index_acc.remove(*it);
        }
      }
      if (!has_next) break;
      it = next_it;
    }
  }
}

InMemoryLabelPropertyIndex::Iterable::Iterator::Iterator(Iterable *self,
                                                         utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, self_->storage_, nullptr),
      current_vertex_(nullptr) {
  AdvanceUntilValid();
}

InMemoryLabelPropertyIndex::Iterable::Iterator &InMemoryLabelPropertyIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void InMemoryLabelPropertyIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_accessor_.end(); ++index_iterator_) {
    if (index_iterator_->vertex == current_vertex_) {
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

    if (CurrentVersionHasLabelProperty(*index_iterator_->vertex, self_->label_, self_->property_,
                                       index_iterator_->value, self_->transaction_, self_->view_)) {
      current_vertex_ = index_iterator_->vertex;
      current_vertex_accessor_ = VertexAccessor(current_vertex_, self_->storage_, self_->transaction_);
      break;
    }
  }
}

InMemoryLabelPropertyIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor,
                                               utils::SkipList<Vertex>::ConstAccessor vertices_accessor, LabelId label,
                                               PropertyId property,
                                               const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                               const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                                               Storage *storage, Transaction *transaction)
    : pin_accessor_(std::move(vertices_accessor)),
      index_accessor_(std::move(index_accessor)),
      label_(label),
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

InMemoryLabelPropertyIndex::Iterable::Iterator InMemoryLabelPropertyIndex::Iterable::begin() {
  // If the bounds are set and don't have comparable types we don't yield any
  // items from the index.
  if (!bounds_valid_) return {this, index_accessor_.end()};
  auto index_iterator = index_accessor_.begin();
  if (lower_bound_) {
    index_iterator = index_accessor_.find_equal_or_greater(lower_bound_->value());
  }
  return {this, index_iterator};
}

InMemoryLabelPropertyIndex::Iterable::Iterator InMemoryLabelPropertyIndex::Iterable::end() {
  return {this, index_accessor_.end()};
}

uint64_t InMemoryLabelPropertyIndex::ApproximateVertexCount(LabelId label, PropertyId property) const {
  auto it = index_.find({label, property});
  MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(), property.AsUint());
  return it->second.size();
}

uint64_t InMemoryLabelPropertyIndex::ApproximateVertexCount(LabelId label, PropertyId property,
                                                            const PropertyValue &value) const {
  auto it = index_.find({label, property});
  MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(), property.AsUint());
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

uint64_t InMemoryLabelPropertyIndex::ApproximateVertexCount(
    LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower,
    const std::optional<utils::Bound<PropertyValue>> &upper) const {
  auto it = index_.find({label, property});
  MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(), property.AsUint());
  auto acc = it->second.access();
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
  return acc.estimate_range_count(lower, upper, utils::SkipListLayerForCountEstimation(acc.size()));
}

std::vector<std::pair<LabelId, PropertyId>> InMemoryLabelPropertyIndex::ClearIndexStats() {
  std::vector<std::pair<LabelId, PropertyId>> deleted_indexes;
  auto locked_stats = stats_.Lock();
  deleted_indexes.reserve(locked_stats->size());
  std::transform(locked_stats->begin(), locked_stats->end(), std::back_inserter(deleted_indexes),
                 [](const auto &elem) { return elem.first; });
  locked_stats->clear();
  return deleted_indexes;
}

// stats_ is a map where the key is a pair of label and property, so for one label many pairs can be deleted
std::vector<std::pair<LabelId, PropertyId>> InMemoryLabelPropertyIndex::DeleteIndexStats(
    const storage::LabelId &label) {
  std::vector<std::pair<LabelId, PropertyId>> deleted_indexes;
  auto locked_stats = stats_.Lock();
  for (auto it = locked_stats->cbegin(); it != locked_stats->cend();) {
    if (it->first.first == label) {
      deleted_indexes.push_back(it->first);
      it = locked_stats->erase(it);
    } else {
      ++it;
    }
  }
  return deleted_indexes;
}

void InMemoryLabelPropertyIndex::SetIndexStats(const std::pair<storage::LabelId, storage::PropertyId> &key,
                                               const LabelPropertyIndexStats &stats) {
  auto locked_stats = stats_.Lock();
  locked_stats->insert_or_assign(key, stats);
}

std::optional<LabelPropertyIndexStats> InMemoryLabelPropertyIndex::GetIndexStats(
    const std::pair<storage::LabelId, storage::PropertyId> &key) const {
  auto locked_stats = stats_.ReadLock();
  if (auto it = locked_stats->find(key); it != locked_stats->end()) {
    return it->second;
  }
  return {};
}

void InMemoryLabelPropertyIndex::RunGC() {
  for (auto &index_entry : index_) {
    index_entry.second.run_gc();
  }
}

InMemoryLabelPropertyIndex::Iterable InMemoryLabelPropertyIndex::Vertices(
    LabelId label, PropertyId property, const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
    Transaction *transaction) {
  DMG_ASSERT(storage->storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL ||
                 storage->storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL,
             "PropertyLabel index trying to access InMemory vertices from OnDisk!");
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  auto it = index_.find({label, property});
  MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(), property.AsUint());
  return {it->second.access(), std::move(vertices_acc), label, property, lower_bound, upper_bound, view, storage,
          transaction};
}

InMemoryLabelPropertyIndex::Iterable InMemoryLabelPropertyIndex::Vertices(
    LabelId label, PropertyId property,
    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc,
    const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
    Transaction *transaction) {
  auto it = index_.find({label, property});
  MG_ASSERT(it != index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(), property.AsUint());
  return {it->second.access(), std::move(vertices_acc), label, property, lower_bound, upper_bound, view, storage,
          transaction};
}

void InMemoryLabelPropertyIndex::AbortEntries(PropertyId property,
                                              std::span<std::pair<PropertyValue, Vertex *> const> vertices,
                                              uint64_t exact_start_timestamp) {
  auto const it = indices_by_property_.find(property);
  if (it == indices_by_property_.end()) return;

  auto &indices = it->second;
  for (const auto &[_, index] : indices) {
    auto index_acc = index->access();
    for (auto const &[value, vertex] : vertices) {
      index_acc.remove(Entry{value, vertex, exact_start_timestamp});
    }
  }
}

void InMemoryLabelPropertyIndex::AbortEntries(LabelId label,
                                              std::span<std::pair<PropertyValue, Vertex *> const> vertices,
                                              uint64_t exact_start_timestamp) {
  for (auto &[label_prop, storage] : index_) {
    if (label_prop.first != label) {
      continue;
    }

    auto index_acc = storage.access();
    for (const auto &[property, vertex] : vertices) {
      if (!property.IsNull()) {
        index_acc.remove(Entry{property, vertex, exact_start_timestamp});
      }
    }
  }
}

void InMemoryLabelPropertyIndex::DropGraphClearIndices() {
  index_.clear();
  indices_by_property_.clear();
  stats_->clear();
}

}  // namespace memgraph::storage
