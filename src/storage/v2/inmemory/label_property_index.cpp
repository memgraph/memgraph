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
#include <range/v3/algorithm/find.hpp>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_constants.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/property_value_utils.hpp"
#include "utils/bound.hpp"
#include "utils/counter.hpp"
#include "utils/fnv.hpp"
#include "utils/logging.hpp"

namespace r = ranges;
namespace rv = r::views;

namespace memgraph::storage {

namespace {

auto PropertyValueMatch_ActionMethod(std::vector<bool> &match, PropertiesPermutationHelper const &helper,
                                     IndexOrderedPropertyValues const &cmp_values) {
  using enum Delta::Action;
  return ActionMethod<SET_PROPERTY>([&](Delta const &delta) {
    for (auto &&[pos, matches] : helper.MatchesValue(delta.property.key, *delta.property.value, cmp_values)) {
      match[pos] = matches;
    }
  });
}

auto PropertyValuesUpdate_ActionMethod(PropertiesPermutationHelper const &helper, std::vector<PropertyValue> &values) {
  using enum Delta::Action;
  return ActionMethod<SET_PROPERTY>(
      [&](Delta const &delta) { helper.Update(delta.property.key, *delta.property.value, values); });
}

/** Converts a span of `PropertyPaths` into a comma-separated string.
 */
[[maybe_unused]] // Currently only used in DMG_ASSERT, maybe_unused to get rid of warning
auto JoinPropertiesAsString(std::span<PropertyPath const> properties) -> std::string {
  auto const make_nested = [](std::span<PropertyId const> path) {
    return utils::Join(path | ranges::views::transform(&PropertyId::AsUint) |
                           ranges::views::transform([](uint64_t id) { return std::to_string(id); }),
                       ".");
  };

  return utils::Join(properties | rv::transform([&](auto &&path) { return make_nested(path); }), ", ");
}

// Helper function for iterating through label-property index. Returns true if
// this transaction can see the given vertex, and the visible version has the
// given label and properties.
bool CurrentVersionHasLabelProperties(const Vertex &vertex, LabelId label, PropertiesPermutationHelper const &helper,
                                      IndexOrderedPropertyValues const &values, Transaction *transaction, View view) {
  bool exists = true;
  bool deleted = false;
  bool has_label = false;
  auto current_values_equal_to_value = std::vector<bool>{};
  const Delta *delta = nullptr;
  {
    auto const guard = std::shared_lock{vertex.lock};
    delta = vertex.delta;
    deleted = vertex.deleted;
    if (!delta && deleted) return false;
    has_label = utils::Contains(vertex.labels, label);
    if (!delta && !has_label) return false;
    current_values_equal_to_value = helper.MatchesValues(vertex.properties, values);
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = transaction->isolation_level == IsolationLevel::SNAPSHOT_ISOLATION;
    if (useCache) {
      auto const &cache = transaction->manyDeltasCache;
      if (auto resError = HasError(view, cache, &vertex, false); resError) return false;
      auto resLabel = cache.GetHasLabel(view, &vertex, label);
      if (resLabel && *resLabel) {
        bool all_matched = true;
        bool all_exist = true;

        for (auto &&[_, property_path, value] : helper.WithPropertyId(values)) {
          auto resProp = cache.GetProperty(view, &vertex, property_path.get()[0]);
          if (resProp) {
            // Only test if so far everything has already matched
            if (all_matched) {
              auto const *nested_value_ptr = ReadNestedPropertyValue(*resProp, property_path.get() | rv::drop(1));
              if ((nested_value_ptr && *nested_value_ptr != value.get()) ||
                  (!nested_value_ptr && !value.get().IsNull())) {
                all_matched = false;
              }
            }
          } else {
            // We can only use the cache as a result if we can validate all properties
            all_exist = false;
            break;
          }
        }
        if (all_exist) return all_matched;
      }
    }

    auto const n_processed = ApplyDeltasForRead(transaction, delta, view, [&](const Delta &delta) {
      // clang-format off
          DeltaDispatch(delta, utils::ChainedOverloaded{
            Deleted_ActionMethod(deleted),
            Exists_ActionMethod(exists),
            HasLabel_ActionMethod(has_label, label),
            PropertyValueMatch_ActionMethod(current_values_equal_to_value, helper, values)
          });
      // clang-format on
    });

    if (useCache && n_processed >= FLAGS_delta_chain_cache_threshold) {
      auto &cache = transaction->manyDeltasCache;
      cache.StoreExists(view, &vertex, exists);
      cache.StoreDeleted(view, &vertex, deleted);
      cache.StoreHasLabel(view, &vertex, label, has_label);

      // Caching does not work with nested property indices, because at
      // this point we've discarded the map except for the bottom-most
      // value. We cannot cache the map `a` if all we have is the value
      // `a.b.c`.
      for (auto &&[pos, property_path, value] : helper.WithPropertyId(values)) {
        if (property_path.get().size() == 1 && current_values_equal_to_value[pos]) {
          cache.StoreProperty(view, &vertex, property_path.get()[0], value.get());
        }
      }
    }
  }

  return exists && !deleted && has_label && std::ranges::all_of(current_values_equal_to_value, std::identity{});
}

/// Helper function for label-properties index garbage collection. Returns true if
/// there is a reachable version of the vertex that has the given label and
/// properties values.
inline bool AnyVersionHasLabelProperties(const Vertex &vertex, LabelId label, std::span<PropertyPath const> key,
                                         PropertiesPermutationHelper const &helper,
                                         IndexOrderedPropertyValues const &values, uint64_t timestamp) {
  Delta const *delta;
  bool exists = true;
  bool deleted;
  bool has_label;
  auto current_values_equal_to_value = std::vector<bool>{};
  {
    auto guard = std::shared_lock{vertex.lock};
    delta = vertex.delta;
    deleted = vertex.deleted;
    if (delta == nullptr && deleted) return false;
    has_label = utils::Contains(vertex.labels, label);
    if (delta == nullptr && !has_label) return false;
    current_values_equal_to_value = helper.MatchesValues(vertex.properties, values);
  }

  if (exists && !deleted && has_label && std::ranges::all_of(current_values_equal_to_value, std::identity{})) {
    return true;
  }

  constexpr auto interesting =
      details::ActionSet<Delta::Action::ADD_LABEL, Delta::Action::REMOVE_LABEL, Delta::Action::SET_PROPERTY,
                         Delta::Action::RECREATE_OBJECT, Delta::Action::DELETE_DESERIALIZED_OBJECT,
                         Delta::Action::DELETE_OBJECT>{};
  return details::AnyVersionSatisfiesPredicate<interesting>(timestamp, delta, [&](const Delta &delta) {
    // clang-format off
    DeltaDispatch(delta, utils::ChainedOverloaded{
                             Deleted_ActionMethod(deleted),
                             Exists_ActionMethod(exists),
                             HasLabel_ActionMethod(has_label, label),
                             PropertyValueMatch_ActionMethod(current_values_equal_to_value, helper, values)
                         });
    // clang-format on
    return exists && !deleted && has_label && std::ranges::all_of(current_values_equal_to_value, std::identity{});
  });
}

}  // namespace

bool InMemoryLabelPropertyIndex::Entry::operator<(std::vector<PropertyValue> const &rhs) const {
  return std::ranges::lexicographical_compare(
      std::span{values.values_.begin(), std::min(rhs.size(), values.values_.size())}, rhs);
}

bool InMemoryLabelPropertyIndex::Entry::operator==(std::vector<PropertyValue> const &rhs) const {
  return std::ranges::equal(std::span{values.values_.begin(), std::min(rhs.size(), values.values_.size())}, rhs);
}

inline void TryInsertLabelPropertiesIndex(Vertex &vertex, LabelId label, PropertiesPermutationHelper const &props,
                                          auto &&index_accessor,
                                          std::optional<SnapshotObserverInfo> const &snapshot_info) {
  // observe regardless
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::VERTICES);
  }

  if (vertex.deleted || !utils::Contains(vertex.labels, label)) {
    return;
  }

  auto values = props.Extract(vertex.properties);
  if (r::all_of(values, [](auto const &each) { return each.IsNull(); })) {
    return;
  }

  // Using 0 as a timestamp is fine because the index is created at timestamp x
  // and any query using the index will be > x.
  index_accessor.insert({props.ApplyPermutation(std::move(values)), &vertex, 0});
}

inline void TryInsertLabelPropertiesIndex(Vertex &vertex, LabelId label, PropertiesPermutationHelper const &props,
                                          auto &&index_accessor,
                                          std::optional<SnapshotObserverInfo> const &snapshot_info,
                                          Transaction const &tx) {
  // observe regardless
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::VERTICES);
  }

  bool exists = true;
  bool deleted = false;
  Delta *delta = nullptr;
  bool has_label = false;
  std::vector<PropertyValue> properties;
  {
    auto guard = std::shared_lock{vertex.lock};
    deleted = vertex.deleted;
    delta = vertex.delta;
    has_label = utils::Contains(vertex.labels, label);
    properties = props.Extract(vertex.properties);
  }
  // Create and drop index will always use snapshot isolation
  if (delta) {
    ApplyDeltasForRead(&tx, delta, View::OLD, [&](const Delta &delta) {
      // clang-format off
      DeltaDispatch(delta, utils::ChainedOverloaded{
        Exists_ActionMethod(exists),
        Deleted_ActionMethod(deleted),
        HasLabel_ActionMethod(has_label, label),
        PropertyValuesUpdate_ActionMethod(props, properties)
      });
      // clang-format on
    });
  }
  if (!exists || deleted || !has_label) {
    return;
  }

  // If extracted values are all null, then no index entry required
  if (r::all_of(properties, [](auto const &each) { return each.IsNull(); })) {
    return;
  }

  index_accessor.insert({props.ApplyPermutation(std::move(properties)), &vertex, tx.start_timestamp});
}

bool InMemoryLabelPropertyIndex::CreateIndexOnePass(
    LabelId label, PropertiesPaths const &properties, utils::SkipList<Vertex>::Accessor vertices,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
    std::optional<SnapshotObserverInfo> const &snapshot_info) {
  auto res = RegisterIndex(label, properties);
  if (!res) return false;
  auto res2 = PopulateIndex(label, properties, std::move(vertices), parallel_exec_info, snapshot_info);
  if (res2.HasError()) {
    MG_ASSERT(false, "Index population can't fail, there was no cancellation callback.");
  }
  return PublishIndex(label, properties, 0);
}

bool InMemoryLabelPropertyIndex::RegisterIndex(LabelId label, PropertiesPaths const &properties) {
  return index_.WithLock([&](std::shared_ptr<const IndexContainer> &index) {
    auto new_index = std::make_shared<IndexContainer>(*index);
    auto [it1, _] = new_index->indices_.try_emplace(label);
    auto &properties_map = it1->second;
    auto it2 = properties_map.find(properties);
    if (it2 != properties_map.end()) {
      // Index already exists.
      return false;
    }
    auto helper = PropertiesPermutationHelper{properties};
    auto [it3, _2] = properties_map.emplace(properties, std::make_shared<IndividualIndex>(std::move(helper)));
    all_indexes_.WithLock([&](auto &all_indexes) {
      auto new_all_indexes = *all_indexes;
      new_all_indexes.emplace_back(it3->second, label, properties);
      all_indexes = std::make_shared<std::vector<AllIndicesEntry>>(std::move(new_all_indexes));
    });

    // Add entries into the reverse lookup
    auto de = EntryDetail{&it3->first, it3->second.get()};
    for (auto &&property_path : properties) {
      new_index->reverse_lookup_[property_path[0]].insert({label, de});
    }
    index = std::move(new_index);
    return true;
  });
}

auto InMemoryLabelPropertyIndex::PopulateIndex(
    LabelId label, PropertiesPaths const &properties, utils::SkipList<Vertex>::Accessor vertices,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
    std::optional<SnapshotObserverInfo> const &snapshot_info, Transaction const *tx, CheckCancelFunction cancel_check)
    -> utils::BasicResult<IndexPopulateError> {
  auto index = GetIndividualIndex(label, properties);
  if (!index) {
    MG_ASSERT(false, "It should not be possible to remove the index before populating it.");
  }

  spdlog::trace("Vertices size when creating index: {}", vertices.size());

  try {
    auto const accessor_factory = [&] { return index->skiplist.access(); };

    if (tx) {
      // If we are in a transaction, we need to read the object with the correct MVCC snapshot isolation
      auto const insert_function = [&](Vertex &vertex, auto &index_accessor) {
        TryInsertLabelPropertiesIndex(vertex, label, index->permutations_helper, index_accessor, snapshot_info, *tx);
      };
      PopulateIndexDispatch(vertices, accessor_factory, insert_function, std::move(cancel_check), parallel_exec_info);
    } else {
      // If we are not in a transaction, we need to read the object as it is. (post recovery)
      auto const insert_function = [&](Vertex &vertex, auto &index_accessor) {
        TryInsertLabelPropertiesIndex(vertex, label, index->permutations_helper, index_accessor, snapshot_info);
      };
      PopulateIndexDispatch(vertices, accessor_factory, insert_function, std::move(cancel_check), parallel_exec_info);
    }
  } catch (const PopulateCancel &) {
    RemoveIndividualIndex(label, properties);
    return IndexPopulateError::Cancellation;
  } catch (const utils::OutOfMemoryException &) {
    RemoveIndividualIndex(label, properties);
    throw;
  }

  return {};
}

bool InMemoryLabelPropertyIndex::PublishIndex(LabelId label, PropertiesPaths const &properties,
                                              uint64_t commit_timestamp) {
  auto index = GetIndividualIndex(label, properties);
  if (!index) return false;
  index->Publish(commit_timestamp);
  return true;
}

void InMemoryLabelPropertyIndex::IndividualIndex::Publish(uint64_t commit_timestamp) {
  status.Commit(commit_timestamp);
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);
}

InMemoryLabelPropertyIndex::IndividualIndex::~IndividualIndex() {
  if (status.IsReady()) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);
  }
}

auto InMemoryLabelPropertyIndex::GetIndividualIndex(LabelId const &label, PropertiesPaths const &properties) const
    -> std::shared_ptr<IndividualIndex> {
  return index_.WithReadLock(
      [&](std::shared_ptr<IndexContainer const> const &index) -> std::shared_ptr<IndividualIndex> {
        auto it1 = index->indices_.find(label);
        if (it1 == index->indices_.cend()) [[unlikely]]
          return {};
        auto &properties_map = it1->second;
        auto it2 = properties_map.find(properties);
        if (it2 == properties_map.cend()) [[unlikely]]
          return {};
        return it2->second;
      });
}

bool InMemoryLabelPropertyIndex::RemoveIndividualIndex(LabelId const &label, PropertiesPaths const &properties) {
  auto result = index_.WithLock([&](std::shared_ptr<IndexContainer const> &index) {
    {
      auto it = index->indices_.find(label);
      if (it == index->indices_.cend()) [[unlikely]]
        return false;
      auto &properties_map = it->second;
      auto it2 = properties_map.find(properties);
      if (it2 == properties_map.cend()) [[unlikely]] {
        return false;
      }
    }
    auto new_index = std::make_shared<IndexContainer>(*index);
    auto it1 = new_index->indices_.find(label);
    DMG_ASSERT(it1 != new_index->indices_.cend(), "Index should exist");
    auto &properties_map = it1->second;
    auto it2 = properties_map.find(properties);
    DMG_ASSERT(it2 != properties_map.cend(), "Index should exist");

    // Erase the reverse lookup before removing the index entry
    for (auto const &prop_selector : properties) {
      auto it3 = new_index->reverse_lookup_.find(prop_selector[0]);
      if (it3 == new_index->reverse_lookup_.cend()) continue;
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
        new_index->reverse_lookup_.erase(it3);
      }
    }

    auto const make_props_subspan = [&](std::size_t length) {
      return std::span{properties.cbegin(), properties.cbegin() + length + 1};
    };

    // For each prefix of properties, compute the number of indices which have the
    // same label and properties prefix. For example, for :L1(a, b, c), we count
    // other indices for :L1(a, b, ...) and :L1(a, ...). Because stats are shared
    // between indices, we can only remove the stats if no other indices are using
    // them.
    auto const index_prefix_usage = std::invoke([&] {
      std::vector<std::size_t> use_count(properties.size(), 0);
      for (std::size_t i = 0; i < use_count.size(); ++i) {
        auto const prefix = make_props_subspan(i);

        use_count[i] = ranges::count_if(properties_map, [&](auto &&each) {
          auto &&[index_properties, _] = each;
          return ranges::starts_with(index_properties, prefix);
        });
      }

      return use_count;
    });

    // Cleanup stats (the stats may not have been generated)
    std::invoke([&] {
      auto stats_ptr = stats_.Lock();
      auto it1 = stats_ptr->find(label);
      if (it1 == stats_ptr->end()) {
        return;
      }

      auto &properties_map = it1->second;

      for (auto &&[prefix_len, use_count] : ranges::views::enumerate(index_prefix_usage)) {
        if (use_count != 1) {
          // Unless this is the only index using the stat, we shouldn't delete
          // it.
          continue;
        }

        auto it2 = properties_map.find(make_props_subspan(prefix_len));
        if (it2 == properties_map.end()) {
          continue;
        }
        properties_map.erase(it2);
        if (properties_map.empty()) {
          stats_ptr->erase(it1);
        }
      }
    });

    // new erase the index
    properties_map.erase(it2);
    if (properties_map.empty()) {
      new_index->indices_.erase(it1);
    }

    index = std::move(new_index);
    return true;
  });
  CleanupAllIndicies();
  return result;
}

void InMemoryLabelPropertyIndex::ActiveIndices::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update,
                                                                 const Transaction &tx) {
  auto const it = index_container_->indices_.find(added_label);
  if (it == index_container_->indices_.cend()) {
    return;
  }

  auto const prop_ids = vertex_after_update->properties.ExtractPropertyIds();

  auto const relevant_index = [&](auto &&each) {
    auto &[index_props, _] = each;
    auto vector_has_property = [&](auto &&index_prop) { return r::binary_search(prop_ids, index_prop); };
    return r::any_of(index_props[0], vector_has_property);
  };

  for (auto &[props, index] : it->second | rv::filter(relevant_index)) {
    auto &[permutations_helper, skiplist, status] = *index;
    auto values = permutations_helper.Extract(vertex_after_update->properties);
    if (r::any_of(values, [](auto &&val) { return !val.IsNull(); })) {
      auto acc = skiplist.access();
      acc.insert({permutations_helper.ApplyPermutation(std::move(values)), vertex_after_update, tx.start_timestamp});
    }
  }
}

void InMemoryLabelPropertyIndex::ActiveIndices::UpdateOnSetProperty(PropertyId property, const PropertyValue &value,
                                                                    Vertex *vertex, const Transaction &tx) {
  auto const it = index_container_->reverse_lookup_.find(property);
  if (it == index_container_->reverse_lookup_.end()) {
    return;
  }

  auto const has_label = [&](auto &&each) { return r::find(vertex->labels, each.first) != vertex->labels.cend(); };
  auto const has_property = [&](auto &&each) {
    auto &ids = *std::get<PropertiesPaths const *>(each.second);
    return r::find_if(ids, [&](auto &&path) { return path[0] == property; }) != ids.cend();
  };
  auto const relevant_index = [&](auto &&each) { return has_label(each) && has_property(each); };

  for (auto &lookup : it->second | rv::filter(relevant_index)) {
    auto &[property_ids, index] = lookup.second;

    auto values = index->permutations_helper.Extract(vertex->properties);
    if (r::any_of(values, [](auto &&value) { return !value.IsNull(); })) {
      auto acc = index->skiplist.access();
      acc.insert({index->permutations_helper.ApplyPermutation(std::move(values)), vertex, tx.start_timestamp});
    }
  }
}

bool InMemoryLabelPropertyIndex::DropIndex(LabelId label, std::vector<PropertyPath> const &properties) {
  return RemoveIndividualIndex(label, properties);
}

bool InMemoryLabelPropertyIndex::ActiveIndices::IndexReady(LabelId label,
                                                           std::span<PropertyPath const> properties) const {
  auto it = index_container_->indices_.find(label);
  if (it != index_container_->indices_.end()) {
    auto it2 = it->second.find(properties);
    if (it2 != it->second.end()) {
      return it2->second->status.IsReady();
    }
  }

  return false;
}

auto InMemoryLabelPropertyIndex::ActiveIndices::RelevantLabelPropertiesIndicesInfo(
    std::span<LabelId const> labels, std::span<PropertyPath const> properties) const
    -> std::vector<LabelPropertiesIndicesInfo> {
  auto res = std::vector<LabelPropertiesIndicesInfo>{};
  auto ppos_indices = rv::iota(size_t{}, properties.size()) | r::to_vector;
  auto properties_vec = properties | ranges::to_vector;

  // For each index with a matching label, this computes the position of the
  // index's composite property keys within the given properties, where
  // -1 is used as a sentinel to indicate that the property isn't found in the
  // given properties vector.
  //
  // For example, having the following two indices:
  //   - :L1(a, b, c) and :L1(b, c, d)
  //
  // Scenario 1:
  //   Input:
  //     - labels = [:L1] (at position 0)
  //     - properties = [b, c, e]
  //   Expected output (property position vectors):
  //     - For properties (a, b, c): [-1, 0, 1]   // a not found, b at pos 0, c at pos 1
  //     - For properties (b, c, d): [0, 1, -1]   // b at pos 0, c at pos 1, d not found
  //
  // Scenario 2:
  //   Input:
  //     - properties = [c, b]
  //   Expected output (property position vectors):
  //     - For properties (a, b, c): [-1, 1, 0]   // a not found, b at pos 1, c at pos 0
  //     - For properties (b, c, d): [1, 0, -1]   // b at pos 1, c at pos 0, d not found

  r::sort(rv::zip(properties_vec, ppos_indices), std::less{},
          [](auto const &val) -> PropertyPath const & { return std::get<0>(val); });

  for (auto [l_pos, label] : ranges::views::enumerate(labels)) {
    auto it = index_container_->indices_.find(label);
    if (it == index_container_->indices_.end()) continue;

    for (const auto &[nested_props, index] : it->second) {
      // Skip indexes which are not ready, they are never relevant for planning
      if (!index->status.IsReady()) continue;

      bool has_matching_property = false;
      auto positions = std::vector<int64_t>();
      for (auto const &prop_path : nested_props) {
        auto it = r::lower_bound(properties_vec, prop_path);
        if (it != properties_vec.end() && *it == prop_path) {
          auto distance = std::distance(properties_vec.begin(), it);
          positions.emplace_back(static_cast<int64_t>(ppos_indices[distance]));
          has_matching_property = true;
        } else {
          positions.emplace_back(-1);
        }
      }
      if (has_matching_property) {
        res.emplace_back(l_pos, std::move(positions), label, nested_props);
      }
    }
  }

  return res;
}

auto InMemoryLabelPropertyIndex::ActiveIndices::ListIndices(uint64_t start_timestamp) const
    -> std::vector<std::pair<LabelId, std::vector<PropertyPath>>> {
  std::vector<std::pair<LabelId, std::vector<PropertyPath>>> ret;

  auto const num_indexes = r::accumulate(index_container_->indices_, size_t{},
                                         [](auto sum, auto const &label_map) { return sum + label_map.second.size(); });

  ret.reserve(num_indexes);
  for (auto const &[label, indices] : index_container_->indices_) {
    for (auto const &[props, index] : indices) {
      if (index->status.IsVisible(start_timestamp)) {
        ret.emplace_back(label, props);
      }
    }
  }
  return ret;
}

void InMemoryLabelPropertyIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter(2048);

  CleanupAllIndicies();

  auto cpy = all_indexes_.WithReadLock(std::identity{});

  for (auto &[index, label_id, property_paths] : *cpy) {
    // before starting index, check if stop_requested
    if (token.stop_requested()) return;

    auto const &permutationHelper = index->permutations_helper;
    auto index_acc = index->skiplist.access();
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
        bool redundant_duplicate = has_next && it->vertex == next_it->vertex && it->values == next_it->values;
        if (redundant_duplicate ||
            !AnyVersionHasLabelProperties(*it->vertex, label_id, property_paths, permutationHelper, it->values,
                                          oldest_active_start_timestamp)) {
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

    // Check the prefix has at least one non-null value
    if (!self_->lower_bound_.empty()) {
      auto const prefix_values_only = index_iterator_->values.values_ | ranges::views::take(self_->lower_bound_.size());
      auto const all_null = ranges::all_of(prefix_values_only, [](PropertyValue const &pv) { return pv.IsNull(); });
      if (all_null) continue;
    }

    enum class InBoundResult { UNDER, IN_BOUNDS, IN_BOUNDS_AT_UB, OVER };

    auto const value_within_upper_bounds = [](std::optional<utils::Bound<PropertyValue>> const &ub,
                                              PropertyValue const &cmp_value) -> InBoundResult {
      if (ub) {
        auto ub_cmp_res = cmp_value <=> ub->value();
        if (is_gt(ub_cmp_res)) {
          return InBoundResult::OVER;
        }
        if (is_eq(ub_cmp_res)) {
          return ub->IsExclusive() ? InBoundResult::OVER : InBoundResult::IN_BOUNDS_AT_UB;
        }
      }
      return InBoundResult::IN_BOUNDS;
    };

    auto const value_within_bounds = [&](std::optional<utils::Bound<PropertyValue>> const &lb,
                                         std::optional<utils::Bound<PropertyValue>> const &ub,
                                         PropertyValue const &cmp_value) -> InBoundResult {
      if (lb) {
        auto lb_cmp_res = cmp_value <=> lb->value();
        if (is_lt(lb_cmp_res) || (lb->IsExclusive() && is_eq(lb_cmp_res))) {
          return InBoundResult::UNDER;
        }
      }
      return value_within_upper_bounds(ub, cmp_value);
    };

    enum class Result { Skip, NoMoreValidEntries, WithAllBounds };

    auto bounds_checker = [&]() {
      auto at_boundary_counter = 0;
      // level 0
      if (skip_lower_bound_check_) {
        switch (value_within_upper_bounds(self_->upper_bound_[0], index_iterator_->values.values_[0])) {
          case InBoundResult::UNDER:
            DMG_ASSERT(false, "this can't happen");
            break;
          case InBoundResult::IN_BOUNDS:
            // This property value is within the boundary, proceed onto the next member of the prefix level
            break;
          case InBoundResult::IN_BOUNDS_AT_UB: {
            // This property value is within the boundary, proceed onto the next member of the prefix level
            // But also this is the boundary of this given prefix level
            // We must track if all preceeding prefix levels of are at the boundary to be able to exit scan as
            // early as possible
            ++at_boundary_counter;
            break;
          }
          case InBoundResult::OVER: {
            // This property value is over the boundary
            // We are at level 0, hence no preceeding prefix levels, we can safely know that there are no more
            // entries that would be within any of the preceeding boundaries.
            return Result::NoMoreValidEntries;
          }
        };
      }
      // rest of the levels
      for (auto level = skip_lower_bound_check_ ? 1 : 0; level < self_->lower_bound_.size(); ++level) {
        switch (value_within_bounds(self_->lower_bound_[level], self_->upper_bound_[level],
                                    index_iterator_->values.values_[level])) {
          case InBoundResult::UNDER: {
            // This property value is under the boundary, hence we need to skip
            return Result::Skip;
          }
          case InBoundResult::IN_BOUNDS: {
            // This property value is within the boundary, proceed onto the next member of the prefix level
            break;
          }
          case InBoundResult::IN_BOUNDS_AT_UB: {
            // This property value is within the boundary, proceed onto the next member of the prefix level
            // But also this is the boundary of this given prefix level
            // We must track if all preceeding prefix levels of are at the boundary to be able to exit scan as
            // early as possible
            ++at_boundary_counter;
            break;
          }
          case InBoundResult::OVER: {
            // This property value is over the boundary
            // If all preceeding prefix levels are at the boundary, we can safely know that there are no more
            // entries that would be within any of the preceeding boundaries.
            // otherwise we skip
            auto const all_preceeding_levels_at_boundary = at_boundary_counter == level;
            return all_preceeding_levels_at_boundary ? Result::NoMoreValidEntries : Result::Skip;
          }
        }
      }
      return Result::WithAllBounds;
    };

    auto const res = bounds_checker();
    skip_lower_bound_check_ = false;
    if (res == Result::Skip) {
      continue;
    } else if (res == Result::NoMoreValidEntries) {
      index_iterator_ = self_->index_accessor_.end();
      break;
    }

    if (CurrentVersionHasLabelProperties(*index_iterator_->vertex, self_->label_, *self_->permutation_helper_,
                                         index_iterator_->values, self_->transaction_, self_->view_)) {
      current_vertex_ = index_iterator_->vertex;
      current_vertex_accessor_ = VertexAccessor(current_vertex_, self_->storage_, self_->transaction_);
      break;
    }
  }
}

InMemoryLabelPropertyIndex::Iterable::Iterable(utils::SkipList<Entry>::Accessor index_accessor,
                                               utils::SkipList<Vertex>::ConstAccessor vertices_accessor, LabelId label,
                                               PropertiesPaths const *properties,
                                               PropertiesPermutationHelper const *permutation_helper,
                                               std::span<PropertyValueRange const> ranges, View view, Storage *storage,
                                               Transaction *transaction)
    : pin_accessor_(std::move(vertices_accessor)),
      index_accessor_(std::move(index_accessor)),
      label_(label),
      properties_(properties),
      permutation_helper_{permutation_helper},
      view_(view),
      storage_(storage),
      transaction_(transaction) {
  using LowerAndUpperBounds =
      std::tuple<std::optional<utils::Bound<PropertyValue>>, std::optional<utils::Bound<PropertyValue>>, bool>;

  auto make_bounds_for_range = [](PropertyValueRange const &range) -> LowerAndUpperBounds {
    std::optional<utils::Bound<PropertyValue>> lower_bound;
    std::optional<utils::Bound<PropertyValue>> upper_bound;

    if (range.type_ == PropertyRangeType::INVALID) {
      return {std::nullopt, std::nullopt, false};
    } else if (range.type_ == PropertyRangeType::IS_NOT_NULL) {
      lower_bound = LowerBoundForType(PropertyValueType::Bool);
    } else if (range.type_ == PropertyRangeType::BOUNDED) {
      // We have to fix the bounds that the user provided to us. If the user
      // provided only one bound we should make sure that only values of that type
      // are returned by the iterator. We ensure this by supplying either an
      // inclusive lower bound of the same type, or an exclusive upper bound of the
      // following type. If neither bound is set we yield all items in the index.
      lower_bound = std::move(range.lower_);
      upper_bound = std::move(range.upper_);

      // Remove any bounds that are set to `Null` because that isn't a valid value.
      if (lower_bound && lower_bound->value().IsNull()) {
        lower_bound = std::nullopt;
      }
      if (upper_bound && upper_bound->value().IsNull()) {
        upper_bound = std::nullopt;
      }

      auto const are_comparable_ranges = [](auto const &lower_bound, auto const &upper_bound) {
        if (AreComparableTypes(lower_bound.value().type(), upper_bound.value().type())) {
          return true;
        } else if (upper_bound.IsInclusive()) {
          return false;
        } else {
          auto const upper_bound_for_lower_bound_type = storage::UpperBoundForType(lower_bound.value().type());
          return upper_bound_for_lower_bound_type && upper_bound.value() == upper_bound_for_lower_bound_type->value();
        };
      };

      // If both bounds are set, but are incomparable types, then this is an
      // invalid range and will yield an empty result set.
      if (lower_bound && upper_bound && !are_comparable_ranges(*lower_bound, *upper_bound)) {
        return {std::nullopt, std::nullopt, false};
      }

      // Set missing bounds.
      if (lower_bound && !upper_bound) {
        // Here we need to supply an upper bound. The upper bound is set to an
        // exclusive lower bound of the following type.
        upper_bound = UpperBoundForType(lower_bound->value().type());
      }

      if (upper_bound && !lower_bound) {
        // Here we need to supply a lower bound. The lower bound is set to an
        // inclusive lower bound of the current type.
        lower_bound = LowerBoundForType(upper_bound->value().type());
      }
    }

    return {std::move(lower_bound), std::move(upper_bound), true};
  };

  lower_bound_.reserve(ranges.size());
  upper_bound_.reserve(ranges.size());

  for (auto &&range : ranges) {
    auto [lb, ub, valid] = make_bounds_for_range(range);
    if (!valid) {
      bounds_valid_ = false;
      lower_bound_.clear();
      upper_bound_.clear();
      break;
    }
    lower_bound_.emplace_back(std::move(lb));
    upper_bound_.emplace_back(std::move(ub));
  }
}

InMemoryLabelPropertyIndex::Iterable::Iterator InMemoryLabelPropertyIndex::Iterable::begin() {
  // If the bounds are set and don't have comparable types we don't yield any
  // items from the index.
  if (!bounds_valid_) return {this, index_accessor_.end()};
  auto index_iterator = index_accessor_.begin();
  if (ranges::any_of(lower_bound_, [](auto &&lb) { return lb.has_value(); })) {
    auto lower_bound = lower_bound_ | ranges::views::transform([](auto &&range) -> storage::PropertyValue {
                         if (range.has_value()) {
                           return range.value().value();
                         } else {
                           return kSmallestProperty;
                         }
                       }) |
                       ranges::to_vector;
    index_iterator = index_accessor_.find_equal_or_greater(lower_bound);
  }
  return {this, index_iterator};
}

InMemoryLabelPropertyIndex::Iterable::Iterator InMemoryLabelPropertyIndex::Iterable::end() {
  return {this, index_accessor_.end()};
}

uint64_t InMemoryLabelPropertyIndex::ActiveIndices::ApproximateVertexCount(
    LabelId label, std::span<PropertyPath const> properties) const {
  auto it = index_container_->indices_.find(label);
  DMG_ASSERT(it != index_container_->indices_.end(), "Index for label {} and properties {} doesn't exist",
             label.AsUint(), JoinPropertiesAsString(properties));
  auto it2 = it->second.find(properties);
  DMG_ASSERT(it2 != it->second.end(), "Index for label {} and properties {} doesn't exist", label.AsUint(),
             JoinPropertiesAsString(properties));
  return it2->second->skiplist.size();
}

uint64_t InMemoryLabelPropertyIndex::ActiveIndices::ApproximateVertexCount(
    LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValue const> values) const {
  auto const it = index_container_->indices_.find(label);
  DMG_ASSERT(it != index_container_->indices_.end(), "Index for label {} and properties {} doesn't exist",
             label.AsUint(), JoinPropertiesAsString(properties));

  auto const it2 = it->second.find(properties);
  DMG_ASSERT(it2 != it->second.end(), "Index for label {} and properties {} doesn't exist", label.AsUint(),
             JoinPropertiesAsString(properties));

  auto acc = it2->second->skiplist.access();
  if (!ranges::all_of(values, [](auto &&prop) { return prop.IsNull(); })) {
    // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
    std::vector v(values.begin(), values.end());
    return acc.estimate_count(v, utils::SkipListLayerForCountEstimation(acc.size()));
  }

  // An entry with all values being `Null` won't ever appear in the index,
  // because it indicates that the properties shouldn't exist. Instead, this
  // is used as an indicator to estimate the average number of equal elements in
  // the list (for any given value).
  return acc.estimate_average_number_of_equals(
      [](const auto &first, const auto &second) { return first.values == second.values; },
      // NOLINTNEXTLINE(bugprone-narrowing-conversions,cppcoreguidelines-narrowing-conversions)
      utils::SkipListLayerForAverageEqualsEstimation(acc.size()));
}

uint64_t InMemoryLabelPropertyIndex::ActiveIndices::ApproximateVertexCount(
    LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> bounds) const {
  auto const it = index_container_->indices_.find(label);
  DMG_ASSERT(it != index_container_->indices_.end(), "Index for label {} and properties {} doesn't exist",
             label.AsUint(), JoinPropertiesAsString(properties));

  auto const it2 = it->second.find(properties);
  DMG_ASSERT(it2 != it->second.end(), "Index for label {} and properties {} doesn't exist", label.AsUint(),
             JoinPropertiesAsString(properties));

  auto acc = it2->second->skiplist.access();

  auto in_bounds_for_all_prefix = [&](Entry const &entry) {
    constexpr auto within_bounds = [](PropertyValue const &value, PropertyValueRange const &bounds) -> bool {
      return bounds.IsValueInRange(value);
    };
    auto value_within_bounds = [&](auto &&p) { return std::apply(within_bounds, p); };
    return ranges::all_of(ranges::views::zip(entry.values.values_, bounds), value_within_bounds);
  };
  return ranges::count_if(acc.sampling_range(), in_bounds_for_all_prefix);
}

std::vector<std::pair<LabelId, std::vector<PropertyPath>>> InMemoryLabelPropertyIndex::ClearIndexStats() {
  std::vector<std::pair<LabelId, std::vector<PropertyPath>>> deleted_indexes;
  auto locked_stats = stats_.Lock();

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

// TODO(composite-index) Seems suspicious to me that this deletes all stats
// based on the just the label. Why do properties not matter?
std::vector<std::pair<LabelId, std::vector<PropertyPath>>> InMemoryLabelPropertyIndex::DeleteIndexStats(
    const storage::LabelId &label) {
  std::vector<std::pair<LabelId, std::vector<PropertyPath>>> deleted_indexes;
  auto locked_stats = stats_.Lock();

  auto const it = locked_stats->find(label);
  if (it == locked_stats->cend()) {
    return {};
  }
  for (auto const &properties : it->second | rv::keys) {
    deleted_indexes.emplace_back(label, properties);
  }
  locked_stats->erase(it);
  return deleted_indexes;
}

void InMemoryLabelPropertyIndex::SetIndexStats(storage::LabelId label,
                                               std::span<storage::PropertyPath const> properties,
                                               storage::LabelPropertyIndexStats const &stats) {
  auto locked_stats = stats_.Lock();
  auto &inner_map = (*locked_stats)[label];
  auto it = inner_map.find(properties);
  if (it == inner_map.end()) {
    auto [it2, _] = inner_map.emplace(std::vector(properties.begin(), properties.end()), LabelPropertyIndexStats{});
    it = it2;
  }
  it->second = stats;
}

std::optional<storage::LabelPropertyIndexStats> InMemoryLabelPropertyIndex::GetIndexStats(
    const std::pair<storage::LabelId, std::span<storage::PropertyPath const>> &key) const {
  auto locked_stats = stats_.ReadLock();
  if (auto it = locked_stats->find(key.first); it != locked_stats->end()) {
    if (auto it2 = it->second.find(key.second); it2 != it->second.end()) {
      return it2->second;
    }
  }
  return std::nullopt;
}

void InMemoryLabelPropertyIndex::RunGC() {
  // Remove indicies that are not used by any txn
  CleanupAllIndicies();

  auto cpy = all_indexes_.WithReadLock(std::identity{});
  for (auto &[index, _1, _2] : *cpy) {
    index->skiplist.run_gc();
  }
}

InMemoryLabelPropertyIndex::Iterable InMemoryLabelPropertyIndex::ActiveIndices::Vertices(
    LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> values, View view,
    Storage *storage, Transaction *transaction) {
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  auto it = index_container_->indices_.find(label);
  DMG_ASSERT(it != index_container_->indices_.end(), "Index for label {} and properties {} doesn't exist",
             label.AsUint(), JoinPropertiesAsString(properties));
  auto it2 = it->second.find(properties);
  DMG_ASSERT(it2 != it->second.end(), "Index for label {} and properties {} doesn't exist", label.AsUint(),
             JoinPropertiesAsString(properties));
  return {it2->second->skiplist.access(),
          std::move(vertices_acc),
          label,
          &it2->first,
          &it2->second->permutations_helper,
          values,
          view,
          storage,
          transaction};
}

InMemoryLabelPropertyIndex::Iterable InMemoryLabelPropertyIndex::ActiveIndices::Vertices(
    LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc, View view, Storage *storage,
    Transaction *transaction) {
  auto it = index_container_->indices_.find(label);
  DMG_ASSERT(it != index_container_->indices_.end(), "Index for label {} and properties {} doesn't exist",
             label.AsUint(), JoinPropertiesAsString(properties));
  auto it2 = it->second.find(properties);
  DMG_ASSERT(it2 != it->second.end(), "Index for label {} and properties {} doesn't exist", label.AsUint(),
             JoinPropertiesAsString(properties));

  return {it2->second->skiplist.access(),
          std::move(vertices_acc),
          label,
          &it2->first,
          &it2->second->permutations_helper,
          std::move(range),
          view,
          storage,
          transaction};
}

void InMemoryLabelPropertyIndex::DropGraphClearIndices() {
  index_.WithLock([](auto &idx) { idx = std::make_shared<IndexContainer>(); });
  stats_->clear();
  CleanupAllIndicies();
}

auto InMemoryLabelPropertyIndex::ActiveIndices::GetAbortProcessor() const -> LabelPropertyIndex::AbortProcessor {
  AbortProcessor res{};
  for (const auto &[label, per_properties] : index_container_->indices_) {
    for (auto const &[props, index] : per_properties) {
      // Root properties may be duplicated in nested indices, such
      // as having a.b, a.c, and a.d. In that case, we only need to build
      // an abort processor for a single `a`.
      auto const unique_props = std::invoke(
          [](auto props) {
            auto root_props = props | rv::transform([](auto &&el) { return el[0]; }) | r::to_vector;
            r::sort(root_props);
            return rv::unique(root_props) | r::to_vector;
          },
          props);

      for (auto const &root_prop : unique_props) {
        res.l2p[label][root_prop].emplace_back(&props, &index->permutations_helper);
        res.p2l[root_prop][label].emplace_back(&props, &index->permutations_helper);
      }
    }
  }
  return res;
}

auto InMemoryLabelPropertyIndex::GetActiveIndices() const -> std::unique_ptr<LabelPropertyIndex::ActiveIndices> {
  return std::make_unique<ActiveIndices>(index_.WithReadLock(std::identity{}));
}

void InMemoryLabelPropertyIndex::ActiveIndices::AbortEntries(AbortableInfo const &info, uint64_t start_timestamp) {
  for (auto const &[label, by_properties] : info) {
    auto it = index_container_->indices_.find(label);
    DMG_ASSERT(it != index_container_->indices_.end());
    for (auto const &[prop, to_remove] : by_properties) {
      auto it2 = it->second.find(*prop);
      DMG_ASSERT(it2 != it->second.end());
      auto acc = it2->second->skiplist.access();
      for (auto &[values, vertex] : to_remove) {
        acc.remove(Entry{std::move(values), vertex, start_timestamp});
      }
    }
  }
}

void InMemoryLabelPropertyIndex::CleanupAllIndicies() {
  // By cleanup, we mean just cleanup of the all_indexes_
  // If all_indexes_ is the only thing holding onto an IndividualIndex, we remove it
  all_indexes_.WithLock([](std::shared_ptr<std::vector<AllIndicesEntry> const> &indices) {
    auto keep_condition = [](AllIndicesEntry const &entry) { return entry.index_.use_count() != 1; };
    if (!r::all_of(*indices, keep_condition)) {
      indices = std::make_shared<std::vector<AllIndicesEntry>>(*indices | rv::filter(keep_condition) | r::to_vector);
    }
  });
}

}  // namespace memgraph::storage
