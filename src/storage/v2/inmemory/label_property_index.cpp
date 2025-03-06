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

#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_constants.hpp"
#include "storage/v2/property_value.hpp"
#include "utils/bound.hpp"
#include "utils/counter.hpp"
#include "utils/logging.hpp"

namespace memgraph::storage {

bool InMemoryLabelPropertyIndex::Entry::operator<(const Entry &rhs) const {
  return std::tie(value, vertex, timestamp) < std::tie(rhs.value, rhs.vertex, rhs.timestamp);
}

bool InMemoryLabelPropertyIndex::Entry::operator==(const Entry &rhs) const {
  return std::tie(value, vertex, timestamp) == std::tie(rhs.value, rhs.vertex, rhs.timestamp);
}

bool InMemoryLabelPropertyIndex::Entry::operator<(const PropertyValue &rhs) const { return value < rhs; }

bool InMemoryLabelPropertyIndex::Entry::operator==(const PropertyValue &rhs) const { return value == rhs; }

bool InMemoryLabelPropertyIndex::NewEntry::operator<(const NewEntry &rhs) const {
  return std::tie(values, vertex, timestamp) < std::tie(rhs.values, rhs.vertex, rhs.timestamp);
}

bool InMemoryLabelPropertyIndex::NewEntry::operator==(const NewEntry &rhs) const {
  return std::tie(values, vertex, timestamp) == std::tie(rhs.values, rhs.vertex, rhs.timestamp);
}

bool InMemoryLabelPropertyIndex::NewEntry::operator<(std::vector<PropertyValue> const &rhs) const {
  return values < rhs;
}

bool InMemoryLabelPropertyIndex::NewEntry::operator==(std::vector<PropertyValue> const &rhs) const {
  return values == rhs;
}

// TODO(composite_index): move to somewhere more generic
template <int N>
inline constexpr auto project = [](auto const &value) -> decltype(auto) { return (std::get<N>(value)); };

InMemoryLabelPropertyIndex::PropertiesPermutationHelper::PropertiesPermutationHelper(
    std::span<PropertyId const> properties)
    : inverse_permutation_{std::ranges::views::iota(size_t{}, properties.size()) | ranges::to_vector},
      sorted_properties_(properties.begin(), properties.end()) {
  ranges::sort(ranges::views::zip(inverse_permutation_, sorted_properties_), std::less{}, project<1>);
}

inline void TryInsertLabelPropertiesIndex(Vertex &vertex, LabelId label,
                                          InMemoryLabelPropertyIndex::PropertiesPermutationHelper const &props,
                                          auto &&index_accessor) {
  if (vertex.deleted || !utils::Contains(vertex.labels, label)) {
    return;
  }

  auto values = vertex.properties.ExtractPropertyValuesMissingAsNull(props.sorted_properties_);
  if (std::ranges::all_of(values, [](auto const &each) { return each.IsNull(); })) {
    return;
  }

  auto inverse_permutation = props.inverse_permutation_;
  ranges::sort(ranges::views::zip(inverse_permutation, values), std::less<>{}, project<0>);

  // Using 0 as a timestamp is fine because the index is created at timestamp x
  // and any query using the index will be > x.
  index_accessor.insert({std::move(values), &vertex, 0});
}

bool InMemoryLabelPropertyIndex::CreateIndex(
    LabelId label, std::vector<PropertyId> const &properties, utils::SkipList<Vertex>::Accessor vertices,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
    std::optional<SnapshotObserverInfo> const &snapshot_info) {
  spdlog::trace("Vertices size when creating index: {}", vertices.size());

  {
    // OLD approach
    auto [it, emplaced] =
        index_.emplace(std::piecewise_construct, std::forward_as_tuple(label, properties[0]), std::forward_as_tuple());

    if (!emplaced) return false;

    indices_by_property_[properties[0]].insert({label, &it->second});

    auto const func = [&](Vertex &vertex, auto &index_accessor) {
      TryInsertLabelPropertyIndex(vertex, std::tuple(label, properties[0]), index_accessor);
    };

    if (parallel_exec_info) {
      CreateIndexOnMultipleThreads(vertices, it, index_, *parallel_exec_info, func, snapshot_info);
    } else {
      CreateIndexOnSingleThread(vertices, it, index_, func, snapshot_info);
    }
  }

  {
    // NEW approach

    auto [it1, _] = new_index_.try_emplace(label);
    auto &properties_map = it1->second;
    PropertiesPermutationHelper helper{properties};
    auto [it2, emplaced] = properties_map.try_emplace(properties, std::move(helper));
    if (!emplaced) {
      // Index already exists.
      return false;
    }

    auto &properties_key = it2->first;
    auto &index = it2->second;

    auto de = EntryDetail{&properties_key, &index};
    for (auto prop : properties) {
      new_indices_by_property_[prop].insert({label, de});
    }

    try {
      auto accessor_factory = [&] { return it2->second.skiplist.access(); };
      auto &props_permutation_helper = it2->second.permutations_helper;
      auto const try_insert_into_index = [&](Vertex &vertex, auto &index_accessor) {
        TryInsertLabelPropertiesIndex(vertex, label, props_permutation_helper, index_accessor);
      };
      PopulateIndex(vertices, accessor_factory, try_insert_into_index, parallel_exec_info, snapshot_info);
    } catch (const utils::OutOfMemoryException &) {
      utils::MemoryTracker::OutOfMemoryExceptionBlocker oom_exception_blocker;
      properties_map.erase(it2);
      throw;
    }
  }

  return true;
}

void InMemoryLabelPropertyIndex::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update,
                                                  const Transaction &tx) {
  // OLD
  for (auto &[label_prop, storage] : index_) {
    if (std::get<LabelId>(label_prop) != added_label) {
      continue;
    }
    auto prop_value = vertex_after_update->properties.GetProperty(std::get<PropertyId>(label_prop));
    if (!prop_value.IsNull()) {
      auto acc = storage.access();
      acc.insert(Entry{std::move(prop_value), vertex_after_update, tx.start_timestamp});
    }
  }

  // NEW
  auto const it = new_index_.find(added_label);
  if (it == new_index_.end()) {
    return;
  }

  for (auto &indices : it->second | std::ranges::views::filter([&](auto &each) {
                         auto &[index_props, _] = each;
                         return std::ranges::any_of(index_props, [&](PropertyId prop_id) {
                           return vertex_after_update->properties.HasProperty(prop_id);
                         });
                       })) {
    auto &[props, index] = indices;
    auto values = vertex_after_update->properties.ExtractPropertyValuesMissingAsNull(
        index.permutations_helper.sorted_properties_);
    auto inverse_permutation = index.permutations_helper.inverse_permutation_;
    ranges::sort(ranges::views::zip(inverse_permutation, values), std::less<>{}, project<0>);
    auto acc = index.skiplist.access();
    acc.insert({std::move(values), vertex_after_update, tx.start_timestamp});
  }
}

void InMemoryLabelPropertyIndex::UpdateOnSetProperty(PropertyId property, const PropertyValue &value, Vertex *vertex,
                                                     const Transaction &tx) {
  if (value.IsNull()) {
    return;
  }

  // OLD
  auto index = indices_by_property_.find(property);
  if (index != indices_by_property_.end()) {
    for (const auto &[label, storage] : index->second) {
      if (!utils::Contains(vertex->labels, label)) continue;
      auto acc = storage->access();
      acc.insert(Entry{value, vertex, tx.start_timestamp});
    }
  }

  // NEW
  auto const it = new_indices_by_property_.find(property);
  if (it == new_indices_by_property_.end()) {
    return;
  }

  for (auto &lookup : it->second | std::ranges::views::filter([&](auto &&each) {
                        return std::ranges::find(vertex->labels, each.first) != vertex->labels.cend();
                      }) | std::ranges::views::filter([&](auto &&each) {
                        PropertiesIds const &ids = *std::get<0>(each.second);
                        return std::ranges::find(ids, property) != ids.cend();
                      })) {
    auto &[property_ids, index] = lookup.second;

    auto values = vertex->properties.ExtractPropertyValuesMissingAsNull(index->permutations_helper.sorted_properties_);

    auto inverse_permutation = index->permutations_helper.inverse_permutation_;
    ranges::sort(ranges::views::zip(inverse_permutation, values), std::less<>{}, project<0>);

    auto acc = index->skiplist.access();
    acc.insert({std::move(values), vertex, tx.start_timestamp});
  }
}

bool InMemoryLabelPropertyIndex::DropIndex(LabelId label, std::vector<PropertyId> const &properties) {
  bool res;

  {
    // OLD approach
    // NOTE: this ATM does not remove stats
    if (indices_by_property_.contains(properties[0])) {
      indices_by_property_.at(properties[0]).erase(label);

      if (indices_by_property_.at(properties[0]).empty()) {
        indices_by_property_.erase(properties[0]);
      }
    }

    res = index_.erase({label, properties[0]}) > 0;
  }

  {
    // NEW approach

    // find the primary index
    auto it1 = new_index_.find(label);
    if (it1 == new_index_.end()) {
      return res /*false*/;
    }

    auto &properties_map = it1->second;
    auto it2 = properties_map.find(properties);
    if (it2 == properties_map.end()) {
      return res /*false*/;
    }

    // cleanup the auxiliary indexes
    // MUST be done before removal of primary index entries
    for (auto prop : properties) {
      auto it3 = new_indices_by_property_.find(prop);
      if (it3 == new_indices_by_property_.end()) continue;

      auto &label_map = it3->second;
      auto [b, e] = label_map.equal_range(label);
      // TODO(composite_index): replace linear search with logn
      while (b != e) {
        auto const &[props_key_ptr, _] = b->second;
        if (props_key_ptr == &it2->first) {
          b = label_map.erase(b);
        } else {
          ++b;
        }
      }
      if (label_map.empty()) {
        new_indices_by_property_.erase(it3);
      }
    }

    // Cleanup stats (the stats may not have been generated)
    std::invoke([&] {
      auto stats_ptr = new_stats_.Lock();
      auto it1 = stats_ptr->find(label);
      if (it1 == stats_ptr->end()) {
        return;
      }

      auto &properties_map = it1->second;
      auto it2 = properties_map.find(properties);
      if (it2 == properties_map.end()) {
        return;
      }
      properties_map.erase(it2);
      if (properties_map.empty()) {
        stats_ptr->erase(it1);
      }
    });

    // Do the actual removal from the primary index
    properties_map.erase(it2);
    if (properties_map.empty()) {
      new_index_.erase(it1);
    }

    return res /*true*/;
  }
}

bool InMemoryLabelPropertyIndex::IndexExists(LabelId label, PropertyId property) const {
  return index_.contains({label, property});
}

std::vector<std::pair<LabelId, PropertyId>> InMemoryLabelPropertyIndex::ListIndices() const {
  std::vector<std::pair<LabelId, PropertyId>> ret;
  ret.reserve(index_.size());
  for (auto const &[label, property] : index_ | std::views::keys) {
    ret.emplace_back(label, property);
  }
  return ret;
}

std::vector<std::pair<LabelId, std::vector<PropertyId>>> InMemoryLabelPropertyIndex::ListIndicesNew() const {
  std::vector<std::pair<LabelId, std::vector<PropertyId>>> ret;
  ret.reserve(index_.size());
  for (auto const &[label, indices] : new_index_) {
    for (auto const &props : indices | std::views::keys) {
      ret.emplace_back(label, props);
    }
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

    if (!CanSeeEntityWithTimestamp(index_iterator_->timestamp, self_->transaction_, self_->view_)) {
      continue;
    }

    if (!IsLowerBound(index_iterator_->value, self_->lower_bound_)) continue;
    if (!IsUpperBound(index_iterator_->value, self_->upper_bound_)) {
      index_iterator_ = self_->index_accessor_.end();
      break;
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
    upper_bound_ = MaxBound(lower_bound_->value().type());
  }
  if (upper_bound_ && !lower_bound_) {
    lower_bound_ = MinBound(upper_bound_->value().type());
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

void InMemoryLabelPropertyIndex::SetIndexStats(const std::pair<storage::LabelId, std::vector<storage::PropertyId>> &key,
                                               const LabelPropertyIndexStats &stats) {
  auto locked_stats = stats_.Lock();
  // locked_stats->insert_or_assign(key, stats);
}

std::optional<LabelPropertyIndexStats> InMemoryLabelPropertyIndex::GetIndexStats(
    const std::pair<storage::LabelId, storage::PropertyId> &key) const {
  auto locked_stats = stats_.ReadLock();
  if (auto it = locked_stats->find(key); it != locked_stats->end()) {
    return it->second;
  }
  return {};
}

std::optional<storage::LabelPropertyIndexStats> InMemoryLabelPropertyIndex::GetIndexStats(
    const std::pair<storage::LabelId, std::vector<storage::PropertyId>> &key) const {
  return {};
}

void InMemoryLabelPropertyIndex::RunGC() {
  for (auto &coll : index_ | std::views::values) {
    coll.run_gc();
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
  for (auto const &index : indices | std::views::values) {
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
    if (std::get<LabelId>(label_prop) != label) {
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

LabelPropertyIndex::IndexStats InMemoryLabelPropertyIndex::Analysis() const {
  IndexStats res{};
  for (const auto &[lp, _] : index_) {
    const auto &[label, property] = lp;
    res.l2p[label].emplace_back(property);
    res.p2l[property].emplace_back(label);
  }
  return res;
}

}  // namespace memgraph::storage
