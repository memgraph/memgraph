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

#include <cstdint>

#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/inmemory/label_property_composite_index.hpp"
#include "storage/v2/inmemory/property_constants.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/bound.hpp"
#include "utils/counter.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

using LabelPropertyCompositeIndexKey = InMemoryLabelPropertyCompositeIndex::LabelPropertyCompositeIndexKey;

bool InMemoryLabelPropertyCompositeIndex::Entry::operator<(const Entry &rhs) const {
  if (value < rhs.value) {
    return true;
  }
  if (rhs.value < value) {
    return false;
  }
  return std::make_tuple(vertex, timestamp) < std::make_tuple(rhs.vertex, rhs.timestamp);
}

bool InMemoryLabelPropertyCompositeIndex::Entry::operator==(const Entry &rhs) const {
  return value == rhs.value && vertex == rhs.vertex && timestamp == rhs.timestamp;
}

bool InMemoryLabelPropertyCompositeIndex::Entry::operator<(const std::vector<PropertyValue> &rhs) const {
  return value < rhs;
}

bool InMemoryLabelPropertyCompositeIndex::Entry::operator==(const std::vector<PropertyValue> &rhs) const {
  return value == rhs;
}

bool InMemoryLabelPropertyCompositeIndex::CreateIndex(
    LabelId label, const std::vector<PropertyId> &properties, utils::SkipList<Vertex>::Accessor vertices,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info) {
  spdlog::trace("Vertices size when creating index: {}", vertices.size());
  auto create_index_seq = [this](LabelId label, const std::vector<PropertyId> &properties,
                                 utils::SkipList<Vertex>::Accessor &vertices,
                                 std::map<LabelPropertyCompositeIndexKey, utils::SkipList<Entry>>::iterator it) {
    using IndexAccessor = decltype(it->second.access());

    CreateIndexOnSingleThread(vertices, it, index_, std::make_pair(label, properties),
                              [](Vertex &vertex, LabelPropertyCompositeIndexKey key, IndexAccessor &index_accessor) {
                                TryInsertLabelPropertyCompositeIndex(vertex, key, index_accessor);
                              });

    return true;
  };

  auto create_index_par = [this](LabelId label, const std::vector<PropertyId> &properties,
                                 utils::SkipList<Vertex>::Accessor &vertices,
                                 std::map<LabelPropertyCompositeIndexKey, utils::SkipList<Entry>>::iterator it,
                                 const durability::ParallelizedSchemaCreationInfo &parallel_exec_info) {
    using IndexAccessor = decltype(it->second.access());

    CreateIndexOnMultipleThreads(vertices, it, index_, std::make_pair(label, properties), parallel_exec_info,
                                 [](Vertex &vertex, LabelPropertyCompositeIndexKey key, IndexAccessor &index_accessor) {
                                   TryInsertLabelPropertyCompositeIndex(vertex, key, index_accessor);
                                 });

    return true;
  };

  auto [it, emplaced] =
      index_.emplace(std::piecewise_construct, std::forward_as_tuple(label, properties), std::forward_as_tuple());

  for (const auto &property : properties) {
    indices_by_property_[property].insert({std::make_pair(label, properties), &it->second});
  }

  if (!emplaced) {
    // Index already exists.
    return false;
  }

  if (parallel_exec_info) {
    return create_index_par(label, properties, vertices, it, *parallel_exec_info);
  }

  return create_index_seq(label, properties, vertices, it);
}

void InMemoryLabelPropertyCompositeIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update,
                                                           const Transaction &tx) {
  for (auto &[label_props, storage] : index_) {
    if (label_props.first != added_label) {
      continue;
    }

    auto props_values =
        vertex_after_update->properties.ExtractPropertyValues({label_props.second.begin(), label_props.second.end()});
    if (!props_values) {
      return;
    }
    auto acc = storage.access();
    acc.insert(Entry{std::move(*props_values), vertex_after_update, tx.start_timestamp});
  }
}

void InMemoryLabelPropertyCompositeIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value,
                                                              Vertex *vertex, const Transaction &tx) {
  if (value.IsNull()) {
    return;
  }

  auto it = indices_by_property_.find(property);
  if (it == indices_by_property_.end()) {
    return;
  }

  for (const auto &[key, storage] : it->second) {
    if (!utils::Contains(vertex->labels, key.first)) continue;
    auto props_values = vertex->properties.ExtractPropertyValues({key.second.begin(), key.second.end()});
    if (!props_values) {
      return;
    }

    auto acc = storage->access();
    acc.insert(Entry{std::move(*props_values), vertex, tx.start_timestamp});
  }
}

bool InMemoryLabelPropertyCompositeIndex::DropIndex(LabelId label, const std::vector<PropertyId> &properties) {
  for (const auto &property : properties) {
    auto it = indices_by_property_.find(property);
    if (it != indices_by_property_.end()) {
      it->second.erase({label, properties});

      if (it->second.empty()) {
        indices_by_property_.erase(it);
      }
    }
  }

  return index_.erase({label, properties}) > 0;
}

bool InMemoryLabelPropertyCompositeIndex::IndexExists(LabelId label, const std::vector<PropertyId> &properties) const {
  return index_.find({label, properties}) != index_.end();
}

std::vector<LabelPropertyCompositeIndexKey> InMemoryLabelPropertyCompositeIndex::ListIndices() const {
  std::vector<LabelPropertyCompositeIndexKey> ret;
  ret.reserve(index_.size());
  for (const auto &item : index_) {
    ret.push_back(item.first);
  }
  return ret;
}

void InMemoryLabelPropertyCompositeIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp,
                                                                std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter<2048>();

  for (auto &[label_properties, index] : index_) {
    auto [label_id, prop_ids] = label_properties;
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
            !AnyVersionHasLabelProperties(*it->vertex, label_id, prop_ids, it->value, oldest_active_start_timestamp)) {
          index_acc.remove(*it);
        }
      }
      if (!has_next) break;
      it = next_it;
    }
  }
}

InMemoryLabelPropertyCompositeIndex::Iterable::Iterator::Iterator(Iterable *self,
                                                                  utils::SkipList<Entry>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, self_->storage_, nullptr),
      current_vertex_(nullptr) {
  AdvanceUntilValid();
}

InMemoryLabelPropertyCompositeIndex::Iterable::Iterator &
InMemoryLabelPropertyCompositeIndex::Iterable::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

void InMemoryLabelPropertyCompositeIndex::Iterable::Iterator::AdvanceUntilValid() {
  for (; index_iterator_ != self_->index_accessor_.end(); ++index_iterator_) {
    if (index_iterator_->vertex == current_vertex_) {
      continue;
    }

    if (!CanSeeEntityWithTimestamp(index_iterator_->timestamp, self_->transaction_)) {
      continue;
    }

    if (index_iterator_->value < self_->lower_bound_) {
      continue;
    }

    if (index_iterator_->value > self_->upper_bound_) {
      index_iterator_ = self_->index_accessor_.end();
      break;
    }

    bool should_continue = false;
    auto bound_size = self_->lower_bound_.size();

    for (uint64_t i = 0; i < bound_size; i++) {
      if (index_iterator_->value[i] < self_->lower_bound_[i]) {
        should_continue = true;
        continue;
      }
      if (index_iterator_->value[i] > self_->upper_bound_[i]) {
        should_continue = true;
        continue;
      }

      if (self_->lower_bounds_[i]->IsExclusive() && index_iterator_->value[i] == self_->lower_bound_[i]) {
        should_continue = true;
        break;
      }
      if (self_->upper_bounds_[i]->IsExclusive() && index_iterator_->value[i] == self_->upper_bound_[i]) {
        should_continue = true;
        break;
      }
    }

    if (should_continue) {
      continue;
    }

    if (CurrentVersionHasLabelProperties(*index_iterator_->vertex, self_->label_, self_->properties_,
                                         index_iterator_->value, self_->transaction_, self_->view_)) {
      current_vertex_ = index_iterator_->vertex;
      current_vertex_accessor_ = VertexAccessor(current_vertex_, self_->storage_, self_->transaction_);
      break;
    }
  }
}

InMemoryLabelPropertyCompositeIndex::Iterable::Bounds InMemoryLabelPropertyCompositeIndex::Iterable::MakeBounds(
    const std::vector<std::optional<utils::Bound<PropertyValue>>> &lower_bounds,
    const std::vector<std::optional<utils::Bound<PropertyValue>>> &upper_bounds) {
  std::vector<PropertyValue> lower_bound_values;
  std::vector<PropertyValue> upper_bound_values;
  lower_bound_values.reserve(lower_bounds.size());
  upper_bound_values.reserve(upper_bounds.size());

  for (uint64_t i = 0; i < lower_bounds.size(); i++) {
    auto lower_bound = lower_bounds[i];
    auto upper_bound = upper_bounds[i];
    // Remove any bounds that are set to `Null` because that isn't a valid value.
    if (lower_bound && lower_bound->value().IsNull()) {
      lower_bound = std::nullopt;
    }
    if (upper_bound && upper_bound->value().IsNull()) {
      upper_bound = std::nullopt;
    }

    // Check whether the bounds are of comparable types if both are supplied.
    if (lower_bound && upper_bound && !AreComparableTypes(lower_bound->value().type(), upper_bound->value().type())) {
      bounds_valid_ = false;
      return {};
    }

    // Set missing bounds.
    if (lower_bound && !upper_bound) {
      // Here we need to supply an upper bound. The upper bound is set to an
      // exclusive lower bound of the following type.
      switch (lower_bound->value().type()) {
        case PropertyValue::Type::Null:
          // This shouldn't happen because of the nullopt-ing above.
          LOG_FATAL("Invalid database state!");
          break;
        case PropertyValue::Type::Bool:
          upper_bound = utils::MakeBoundExclusive(kSmallestNumber);
          break;
        case PropertyValue::Type::Int:
        case PropertyValue::Type::Double:
          // Both integers and doubles are treated as the same type in
          // `PropertyValue` and they are interleaved when sorted.
          upper_bound = utils::MakeBoundExclusive(kSmallestString);
          break;
        case PropertyValue::Type::String:
          upper_bound = utils::MakeBoundExclusive(kSmallestList);
          break;
        case PropertyValue::Type::List:
          upper_bound = utils::MakeBoundExclusive(kSmallestMap);
          break;
        case PropertyValue::Type::Map:
          upper_bound = utils::MakeBoundExclusive(kSmallestTemporalData);
          break;
        case PropertyValue::Type::TemporalData:
          upper_bound = utils::MakeBoundExclusive(kSmallestZonedTemporalData);
          break;
        case PropertyValue::Type::ZonedTemporalData:
          upper_bound = utils::MakeBoundExclusive(kSmallestEnum);
          break;
        case PropertyValue::Type::Enum:
          upper_bound = utils::MakeBoundExclusive(kSmallestPoint2d);
          break;
        case PropertyValue::Type::Point2d:
          upper_bound = utils::MakeBoundExclusive(kSmallestPoint3d);
          break;
        case PropertyValue::Type::Point3d:
          // This is the last type in the order so we leave the upper bound empty.
          break;
      }
    }
    if (upper_bound && !lower_bound) {
      // Here we need to supply a lower bound. The lower bound is set to an
      // inclusive lower bound of the current type.
      switch (upper_bound->value().type()) {
        case PropertyValue::Type::Null:
          // This shouldn't happen because of the nullopt-ing above.
          LOG_FATAL("Invalid database state!");
          break;
        case PropertyValue::Type::Bool:
          lower_bound = utils::MakeBoundInclusive(kSmallestBool);
          break;
        case PropertyValue::Type::Int:
        case PropertyValue::Type::Double:
          // Both integers and doubles are treated as the same type in
          // `PropertyValue` and they are interleaved when sorted.
          lower_bound = utils::MakeBoundInclusive(kSmallestNumber);
          break;
        case PropertyValue::Type::String:
          lower_bound = utils::MakeBoundInclusive(kSmallestString);
          break;
        case PropertyValue::Type::List:
          lower_bound = utils::MakeBoundInclusive(kSmallestList);
          break;
        case PropertyValue::Type::Map:
          lower_bound = utils::MakeBoundInclusive(kSmallestMap);
          break;
        case PropertyValue::Type::TemporalData:
          lower_bound = utils::MakeBoundInclusive(kSmallestTemporalData);
          break;
        case PropertyValue::Type::ZonedTemporalData:
          lower_bound = utils::MakeBoundInclusive(kSmallestZonedTemporalData);
          break;
        case PropertyValue::Type::Enum:
          lower_bound = utils::MakeBoundInclusive(kSmallestEnum);
          break;
        case PropertyValue::Type::Point2d:
          lower_bound = utils::MakeBoundExclusive(kSmallestPoint2d);
          break;
        case PropertyValue::Type::Point3d:
          lower_bound = utils::MakeBoundExclusive(kSmallestPoint3d);
          break;
      }
    }

    lower_bound_values.push_back(lower_bound->value());
    upper_bound_values.push_back(upper_bound->value());
  }

  return {.lower_bound = std::move(lower_bound_values), .upper_bound = std::move(upper_bound_values)};
}

InMemoryLabelPropertyCompositeIndex::Iterable::Iterable(
    utils::SkipList<Entry>::Accessor index_accessor, utils::SkipList<Vertex>::ConstAccessor vertices_accessor,
    LabelId label, const std::vector<PropertyId> &properties,
    const std::vector<std::optional<utils::Bound<PropertyValue>>> &lower_bounds,
    const std::vector<std::optional<utils::Bound<PropertyValue>>> &upper_bounds, View view, Storage *storage,
    Transaction *transaction)
    : pin_accessor_(std::move(vertices_accessor)),
      index_accessor_(std::move(index_accessor)),
      label_(label),
      properties_(properties),
      lower_bounds_(lower_bounds),
      upper_bounds_(upper_bounds),
      view_(view),
      storage_(storage),
      transaction_(transaction) {
  // We have to fix the bounds that the user provided to us. If the user
  // provided only one bound we should make sure that only values of that type
  // are returned by the iterator. We ensure this by supplying either an
  // inclusive lower bound of the same type, or an exclusive upper bound of the
  // following type. If neither bound is set we yield all items in the index.
  auto bounds = MakeBounds(lower_bounds, upper_bounds);
  lower_bound_ = std::move(bounds.lower_bound);
  upper_bound_ = std::move(bounds.upper_bound);
}

InMemoryLabelPropertyCompositeIndex::Iterable::Iterator InMemoryLabelPropertyCompositeIndex::Iterable::begin() {
  // If the bounds are set and don't have comparable types we don't yield any
  // items from the index.
  if (!bounds_valid_) return {this, index_accessor_.end()};
  auto index_iterator = index_accessor_.find_equal_or_greater(lower_bound_);
  return {this, index_iterator};
}

InMemoryLabelPropertyCompositeIndex::Iterable::Iterator InMemoryLabelPropertyCompositeIndex::Iterable::end() {
  return {this, index_accessor_.end()};
}

uint64_t InMemoryLabelPropertyCompositeIndex::ApproximateVertexCount(LabelId label,
                                                                     const std::vector<PropertyId> &properties) const {
  auto it = index_.find({label, properties});
  MG_ASSERT(it != index_.end(), "Composite index for label {} does not exist!", label.AsUint());

  return it->second.size();
}

uint64_t InMemoryLabelPropertyCompositeIndex::ApproximateVertexCount(
    LabelId label, const std::vector<PropertyId> &properties,
    const std::vector<std::optional<utils::Bound<PropertyValue>>> &lower,
    const std::vector<std::optional<utils::Bound<PropertyValue>>> &upper) const {
  auto it = index_.find({label, properties});
  MG_ASSERT(it != index_.end(), "Composite index for label {} does not exist!", label.AsUint());

  auto acc = it->second.access();
  return 0;
  // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
  // return acc.estimate_range_count(lower, upper, utils::SkipListLayerForCountEstimation(acc.size()));
}

std::vector<LabelPropertyCompositeIndexKey> InMemoryLabelPropertyCompositeIndex::ClearIndexStats() {
  std::vector<LabelPropertyCompositeIndexKey> deleted_indexes;
  auto locked_stats = stats_.Lock();
  deleted_indexes.reserve(locked_stats->size());
  std::transform(locked_stats->begin(), locked_stats->end(), std::back_inserter(deleted_indexes),
                 [](const auto &elem) { return elem.first; });
  locked_stats->clear();
  return deleted_indexes;
}

// stats_ is a map where the key is a pair of label and property, so for one label many pairs can be deleted
std::vector<LabelPropertyCompositeIndexKey> InMemoryLabelPropertyCompositeIndex::DeleteIndexStats(
    const storage::LabelId &label) {
  std::vector<LabelPropertyCompositeIndexKey> deleted_indexes;
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

void InMemoryLabelPropertyCompositeIndex::SetIndexStats(const LabelPropertyCompositeIndexKey &key,
                                                        const LabelPropertyCompositeIndexStats &stats) {
  auto locked_stats = stats_.Lock();
  locked_stats->insert_or_assign(key, stats);
}

std::optional<LabelPropertyCompositeIndexStats> InMemoryLabelPropertyCompositeIndex::GetIndexStats(
    const LabelPropertyCompositeIndexKey &key) const {
  auto locked_stats = stats_.ReadLock();
  if (auto it = locked_stats->find(key); it != locked_stats->end()) {
    return it->second;
  }
  return {};
}

void InMemoryLabelPropertyCompositeIndex::RunGC() {
  for (auto &index_entry : index_) {
    index_entry.second.run_gc();
  }
}

InMemoryLabelPropertyCompositeIndex::Iterable InMemoryLabelPropertyCompositeIndex::Vertices(
    LabelId label, const std::vector<PropertyId> &properties,
    const std::vector<std::optional<utils::Bound<PropertyValue>>> &lower_bound,
    const std::vector<std::optional<utils::Bound<PropertyValue>>> &upper_bound, View view, Storage *storage,
    Transaction *transaction) {
  DMG_ASSERT(storage->storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL ||
                 storage->storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL,
             "PropertyLabelComposite index trying to access InMemory vertices from OnDisk!");
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  auto it = index_.find({label, properties});
  MG_ASSERT(it != index_.end(), "Composite index for label {} does not exist!", label.AsUint());

  return {it->second.access(), std::move(vertices_acc), label, properties, lower_bound, upper_bound, view, storage,
          transaction};
}

InMemoryLabelPropertyCompositeIndex::Iterable InMemoryLabelPropertyCompositeIndex::Vertices(
    LabelId label, const std::vector<PropertyId> &properties,
    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc,
    const std::vector<std::optional<utils::Bound<PropertyValue>>> &lower_bound,
    const std::vector<std::optional<utils::Bound<PropertyValue>>> &upper_bound, View view, Storage *storage,
    Transaction *transaction) {
  auto it = index_.find({label, properties});
  MG_ASSERT(it != index_.end(), "Composite index for label {} does not exist!", label.AsUint());

  return {it->second.access(), std::move(vertices_acc), label, properties, lower_bound, upper_bound, view, storage,
          transaction};
}

void InMemoryLabelPropertyCompositeIndex::AbortEntries(PropertyId property,
                                                       std::span<std::pair<PropertyValue, Vertex *> const> vertices,
                                                       uint64_t exact_start_timestamp) {
  // TODO: Figure out
  // auto const it = indices_by_property_.find(property);
  // if (it == indices_by_property_.end()) return;

  // auto &indices = it->second;
  // for (const auto &[_, index] : indices) {
  //   auto index_acc = index->access();
  //   for (auto const &[value, vertex] : vertices) {
  //     index_acc.remove(Entry{value, vertex, exact_start_timestamp});
  //   }
  // }
  throw utils::NotYetImplemented(
      "Label-property composite index related operations are not yet supported using in-memory storage mode.");
}

void InMemoryLabelPropertyCompositeIndex::AbortEntries(LabelId label,
                                                       std::span<std::pair<PropertyValue, Vertex *> const> vertices,
                                                       uint64_t exact_start_timestamp) {
  // TODO: Figure out
  // for (auto &[label_prop, storage] : index_) {
  //   if (label_prop.first != label) {
  //     continue;
  //   }

  //   auto index_acc = storage.access();
  //   for (const auto &[property, vertex] : vertices) {
  //     if (!property.IsNull()) {
  //       index_acc.remove(Entry{property, vertex, exact_start_timestamp});
  //     }
  //   }
  // }
  throw utils::NotYetImplemented(
      "Label-property composite index related operations are not yet supported using in-memory storage mode.");
}

void InMemoryLabelPropertyCompositeIndex::DropGraphClearIndices() {
  index_.clear();
  indices_by_property_.clear();
  stats_->clear();
}

}  // namespace memgraph::storage
