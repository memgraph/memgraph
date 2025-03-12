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

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

namespace {

auto build_permutation_cycles(std::span<std::size_t const> permutation_index)
    -> PropertiesPermutationHelper::permutation_cycles {
  auto const n = permutation_index.size();

  auto visited = std::vector(n, false);
  auto cycle = std::vector<std::size_t>{};
  auto cycles = PropertiesPermutationHelper::permutation_cycles{};

  for (auto i = std::size_t{}; i != n; ++i) {
    if (visited[i]) [[unlikely]] {
      // already part of a cycle
      continue;
    }

    // build a cycle
    cycle.clear();
    auto current = i;
    do {
      visited[current] = true;
      cycle.push_back(current);
      current = permutation_index[current];
    } while (current != i);

    if (cycle.size() == 1) {
      // Ignore self-mapped elements
      continue;
    }
    cycles.emplace_back(std::move(cycle));
  }
  return cycles;
}

}  // namespace

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

PropertiesPermutationHelper::PropertiesPermutationHelper(std::span<PropertyId const> properties)
    : sorted_properties_(properties.begin(), properties.end()) {
  auto inverse_permutation = rv::iota(size_t{}, properties.size()) | r::to_vector;
  r::sort(rv::zip(inverse_permutation, sorted_properties_), std::less{},
          [](auto const &value) -> decltype(auto) { return (std::get<1>(value)); });
  cycles_ = build_permutation_cycles(inverse_permutation);
}

inline void TryInsertLabelPropertiesIndex(Vertex &vertex, LabelId label, PropertiesPermutationHelper const &props,
                                          auto &&index_accessor) {
  if (vertex.deleted || !utils::Contains(vertex.labels, label)) {
    return;
  }

  auto values = props.extract(vertex.properties);
  if (r::all_of(values, [](auto const &each) { return each.IsNull(); })) {
    return;
  }
  props.apply_permutation(values);

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
      CreateIndexOnMultipleThreads(vertices, it, index_, *parallel_exec_info, func,
                                   std::nullopt /*leave it to the new approach to observe*/);
    } else {
      CreateIndexOnSingleThread(vertices, it, index_, func, std::nullopt /*leave it to the new approach to observe*/);
    }
  }

  {
    // NEW approach

    auto [it1, _] = new_index_.try_emplace(label);
    auto &properties_map = it1->second;
    auto helper = PropertiesPermutationHelper{properties};
    auto [it2, emplaced] = properties_map.try_emplace(properties, std::move(helper));
    if (!emplaced) {
      // Index already exists.
      return false;
    }

    auto const &properties_key = it2->first;
    auto &index = it2->second;

    auto de = EntryDetail{&properties_key, &index};
    for (auto prop : properties) {
      new_indices_by_property_[prop].insert({label, de});
    }

    try {
      auto &index_skip_list = it2->second.skiplist;
      auto accessor_factory = [&] { return index_skip_list.access(); };
      auto &props_permutation_helper = it2->second.permutations_helper;
      auto const try_insert_into_index = [&](Vertex &vertex, auto &index_accessor) {
        TryInsertLabelPropertiesIndex(vertex, label, props_permutation_helper, index_accessor);
      };
      PopulateIndex(vertices, accessor_factory, try_insert_into_index, parallel_exec_info, snapshot_info);
    } catch (const utils::OutOfMemoryException &) {
      utils::MemoryTracker::OutOfMemoryExceptionBlocker const oom_exception_blocker;
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

  auto const prop_ids = vertex_after_update->properties.ExtractPropertyIds();

  auto const relevant_index = [&](auto &&each) {
    auto &[index_props, _] = each;
    auto vector_has_property = [&](auto &&index_prop) { return r::binary_search(prop_ids, index_prop); };
    return r::any_of(index_props, vector_has_property);
  };

  for (auto &indices : it->second | rv::filter(relevant_index)) {
    auto &[props, index] = indices;
    auto values = index.permutations_helper.extract(vertex_after_update->properties);
    index.permutations_helper.apply_permutation(values);
    DMG_ASSERT(r::any_of(values, [](auto &&val) { return !val.IsNull(); }), "At least one value should be non-null");
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
  {
    auto index = indices_by_property_.find(property);
    if (index != indices_by_property_.end()) {
      for (const auto &[label, storage] : index->second) {
        if (!utils::Contains(vertex->labels, label)) continue;
        auto acc = storage->access();
        acc.insert(Entry{value, vertex, tx.start_timestamp});
      }
    }
  }

  // NEW
  {
    auto const it = new_indices_by_property_.find(property);
    if (it == new_indices_by_property_.end()) {
      return;
    }

    auto const has_label = [&](auto &&each) { return r::find(vertex->labels, each.first) != vertex->labels.cend(); };
    auto const has_property = [&](auto &&each) {
      auto &ids = *std::get<PropertiesIds const *>(each.second);
      return r::find(ids, property) != ids.cend();
    };
    auto const relevant_index = [&](auto &&each) { return has_label(each) && has_property(each); };

    for (auto &lookup : it->second | rv::filter(relevant_index)) {
      auto &[property_ids, index] = lookup.second;

      auto values = index->permutations_helper.extract(vertex->properties);
      index->permutations_helper.apply_permutation(values);

      auto acc = index->skiplist.access();
      acc.insert({std::move(values), vertex, tx.start_timestamp});
    }
  }
}

bool InMemoryLabelPropertyIndex::DropIndex(LabelId label, std::vector<PropertyId> const &properties) {
  bool res = false;

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

bool InMemoryLabelPropertyIndex::IndexExists(LabelId label, std::span<PropertyId const> properties) const {
  auto it = new_index_.find(label);
  if (it != new_index_.end()) {
    return it->second.contains(properties);
  }

  return false;
}

auto InMemoryLabelPropertyIndex::RelevantLabelPropertiesIndicesInfo(std::span<LabelId const> labels,
                                                                    std::span<PropertyId const> properties) const
    -> std::vector<LabelPropertiesIndicesInfo> {
  auto res = std::vector<LabelPropertiesIndicesInfo>{};
  auto ppos_indices = rv::iota(size_t{}, properties.size()) | r::to_vector;
  auto properties_vec = properties | ranges::to_vector;

  // Given new_index_ of:
  // :L1 a, b, c
  // :L1 b, c, d

  // When:
  // labels = :L1 (0)
  // properties = b c e

  // Expect:
  // [-1, 0, 1]
  // [0, 1, -1]

  // When:
  // properties = c b
  // Expect:
  // [-1, 1, 0]
  // [1, 0, -1]

  r::sort(rv::zip(properties_vec, ppos_indices), std::less{},
          [](auto const &val) -> PropertyId const & { return std::get<0>(val); });

  for (auto [l_pos, label] : ranges::views::enumerate(labels)) {
    auto it = new_index_.find(label);
    if (it == new_index_.end()) continue;

    for (auto const &props : it->second | std::ranges::views::keys) {
      bool is_meaningful = false;
      auto positions = std::vector<int64_t>();
      for (auto prop : props) {
        auto it = r::lower_bound(properties_vec, prop);
        if (it != properties_vec.end() && *it == prop) {
          auto distance = std::distance(properties_vec.begin(), it);
          positions.emplace_back(static_cast<int64_t>(ppos_indices[distance]));
          is_meaningful = true;
        } else {
          positions.emplace_back(-1);
        }
      }

      if (is_meaningful) {
        res.emplace_back(l_pos, std::move(positions), label, props);
      }
    }
  }

  return res;
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

  auto const num_indexes =
      std::accumulate(new_index_.cbegin(), new_index_.cend(), size_t{},
                      [](auto sum, auto const &label_map) { return sum + label_map.second.size(); });

  ret.reserve(num_indexes);
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
                                                         utils::SkipList<NewEntry>::Iterator index_iterator)
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

    if (self_->lower_bound_) {
      if (index_iterator_->values[0] /*TODO*/ < self_->lower_bound_->value()) {
        continue;
      }
      if (!self_->lower_bound_->IsInclusive() && index_iterator_->values[0] /*TODO*/ == self_->lower_bound_->value()) {
        continue;
      }
    }
    if (self_->upper_bound_) {
      if (self_->upper_bound_->value() < index_iterator_->values[0] /*TODO*/) {
        index_iterator_ = self_->index_accessor_.end();
        break;
      }
      if (!self_->upper_bound_->IsInclusive() && index_iterator_->values[0] /*TODO*/ == self_->upper_bound_->value()) {
        index_iterator_ = self_->index_accessor_.end();
        break;
      }
    }

    if (CurrentVersionHasLabelProperty(*index_iterator_->vertex, self_->label_, self_->properties_,
                                       index_iterator_->values[0] /*TODO*/, self_->transaction_, self_->view_)) {
      current_vertex_ = index_iterator_->vertex;
      current_vertex_accessor_ = VertexAccessor(current_vertex_, self_->storage_, self_->transaction_);
      break;
    }
  }
}

InMemoryLabelPropertyIndex::Iterable::Iterable(utils::SkipList<NewEntry>::Accessor index_accessor,
                                               utils::SkipList<Vertex>::ConstAccessor vertices_accessor, LabelId label,
                                               std::span<PropertyId const> properties,
                                               const std::optional<utils::Bound<PropertyValue>> &lower_bound,
                                               const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view,
                                               Storage *storage, Transaction *transaction)
    : pin_accessor_(std::move(vertices_accessor)),
      index_accessor_(std::move(index_accessor)),
      label_(label),
      properties_(properties.begin(), properties.end()),
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
    // TODO: the line bellow need to be re-added
    //     index_iterator = index_accessor_.find_equal_or_greater(lower_bound_->value());
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

uint64_t InMemoryLabelPropertyIndex::ApproximateVertexCount(LabelId label,
                                                            std::span<PropertyId const> properties) const {
  auto it = new_index_.find(label);
  MG_ASSERT(it != new_index_.end(), "Index for label {} doesn't exist", label.AsUint());
  auto it2 = it->second.find(properties);
  MG_ASSERT(it2 != it->second.end(), "Index for label {} doesn't exist", label.AsUint());
  return it2->second.skiplist.size();
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

uint64_t InMemoryLabelPropertyIndex::ApproximateVertexCount(LabelId label,
                                                            std::vector<PropertyId> const &properties) const {
  auto const it = new_index_.find(label);
  MG_ASSERT(it != new_index_.end(), "Index for label {} doesn't exist", label.AsUint());

  auto const it2 = it->second.find(properties);
  MG_ASSERT(it2 != it->second.end(), "Index for label {} and properties doesn't exist", label.AsUint());

  return it2->second.skiplist.size();
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

std::vector<std::pair<LabelId, std::vector<PropertyId>>> InMemoryLabelPropertyIndex::ClearIndexStatsNew() {
  std::vector<std::pair<LabelId, std::vector<PropertyId>>> deleted_indexes;
  auto locked_stats = new_stats_.Lock();

  auto const num_stats = std::accumulate(locked_stats->cbegin(), locked_stats->cend(), size_t{},
                                         [](auto sum, auto const &label_map) { return sum + label_map.second.size(); });

  deleted_indexes.reserve(num_stats);
  for (auto &[label, properties_indices_stats] : *locked_stats) {
    for (auto const &properties : properties_indices_stats | rv::keys) {
      deleted_indexes.emplace_back(label, properties);
    }
  }

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

// TODO(composite-index) Seems suspicious to me that this deletes all stats
// based on the just the label. Why do properties not matter?
std::vector<std::pair<LabelId, std::vector<PropertyId>>> InMemoryLabelPropertyIndex::DeleteIndexStatsNew(
    const storage::LabelId &label) {
  std::vector<std::pair<LabelId, std::vector<PropertyId>>> deleted_indexes;
  auto locked_stats = new_stats_.Lock();

  auto const it = locked_stats->find(label);
  if (it != locked_stats->cend()) {
    return {};
  }
  for (auto const &properties : it->second | rv::keys) {
    deleted_indexes.emplace_back(label, properties);
  }
  locked_stats->erase(it);
  return deleted_indexes;
}

void InMemoryLabelPropertyIndex::SetIndexStats(const std::pair<storage::LabelId, storage::PropertyId> &key,
                                               const LabelPropertyIndexStats &stats) {
  auto locked_stats = stats_.Lock();
  locked_stats->insert_or_assign(key, stats);
}

void InMemoryLabelPropertyIndex::SetIndexStats(storage::LabelId label, std::span<storage::PropertyId const> properties,
                                               std::size_t prefix_level,
                                               storage::LabelPropertyIndexStats const &stats) {
  auto locked_stats = new_stats_.Lock();
  auto &inner_map = (*locked_stats)[label];
  auto it = inner_map.find(properties);
  if (it == inner_map.end()) {
    auto [it2, _] =
        inner_map.emplace(std::vector(properties.begin(), properties.end()), StatsByPrefix(properties.size()));
    it = it2;
  }
  it->second[prefix_level] = stats;
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
    const std::pair<storage::LabelId, std::span<storage::PropertyId const>> &key, std::size_t prefix_level) const {
  auto locked_stats = new_stats_.ReadLock();
  if (auto it = locked_stats->find(key.first); it != locked_stats->end()) {
    if (auto it2 = it->second.find(key.second); it2 != it->second.end()) {
      return it2->second[prefix_level];
    }
  }
  return std::nullopt;
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
  auto it = new_index_.find(label);
  MG_ASSERT(it != new_index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(),
            property.AsUint() /*TODO: correct the error msg*/);
  auto it2 = it->second.find(std::array{property});
  MG_ASSERT(it2 != it->second.end(), "Index for label {} and property {} doesn't exist", label.AsUint(),
            property.AsUint() /*TODO: correct the error msg*/);

  return {it2->second.skiplist.access(),
          std::move(vertices_acc),
          label,
          std::array{property},
          lower_bound,
          upper_bound,
          view,
          storage,
          transaction};
}

InMemoryLabelPropertyIndex::Iterable InMemoryLabelPropertyIndex::Vertices(
    LabelId label, std::span<PropertyId const> properties,
    const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
    Transaction *transaction) {
  DMG_ASSERT(storage->storage_mode_ == StorageMode::IN_MEMORY_TRANSACTIONAL ||
                 storage->storage_mode_ == StorageMode::IN_MEMORY_ANALYTICAL,
             "PropertyLabel index trying to access InMemory vertices from OnDisk!");
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  auto it = new_index_.find(label);
  MG_ASSERT(it != new_index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(),
            properties[0].AsUint() /*TODO: correct the error msg*/);
  auto it2 = it->second.find(properties);
  MG_ASSERT(it2 != it->second.end(), "Index for label {} and property {} doesn't exist", label.AsUint(),
            properties[0].AsUint() /*TODO: correct the error msg*/);

  return {it2->second.skiplist.access(),
          std::move(vertices_acc),
          label,
          properties,
          lower_bound,
          upper_bound,
          view,
          storage,
          transaction};
}

InMemoryLabelPropertyIndex::Iterable InMemoryLabelPropertyIndex::Vertices(
    LabelId label, PropertyId property,
    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc,
    const std::optional<utils::Bound<PropertyValue>> &lower_bound,
    const std::optional<utils::Bound<PropertyValue>> &upper_bound, View view, Storage *storage,
    Transaction *transaction) {
  auto it = new_index_.find(label);
  MG_ASSERT(it != new_index_.end(), "Index for label {} and property {} doesn't exist", label.AsUint(),
            property.AsUint() /*TODO: correct the error msg*/);
  auto it2 = it->second.find(std::array{property});
  MG_ASSERT(it2 != it->second.end(), "Index for label {} and property {} doesn't exist", label.AsUint(),
            property.AsUint() /*TODO: correct the error msg*/);

  return {it2->second.skiplist.access(),
          std::move(vertices_acc),
          label,
          std::array{property},
          lower_bound,
          upper_bound,
          view,
          storage,
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

  new_index_.clear();
  new_indices_by_property_.clear();
  new_stats_->clear();
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

auto PropertiesPermutationHelper::extract(PropertyStore const &properties) const -> std::vector<PropertyValue> {
  return properties.ExtractPropertyValuesMissingAsNull(sorted_properties_);
}

}  // namespace memgraph::storage
