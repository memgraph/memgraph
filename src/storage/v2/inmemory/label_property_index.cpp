// Copyright 2026 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <concepts>
#include <cstdint>
#include <range/v3/all.hpp>

#include "storage/v2/id_types.hpp"
#include "storage/v2/indices/active_indices_updater.hpp"
#include "storage/v2/indices/indices_utils.hpp"
#include "storage/v2/indices/label_property_index.hpp"
#include "storage/v2/inmemory/label_property_index.hpp"
#include "storage/v2/inmemory/storage.hpp"
#include "storage/v2/property_constants.hpp"
#include "storage/v2/property_value.hpp"
#include "storage/v2/property_value_utils.hpp"
#include "utils/bound.hpp"
#include "utils/counter.hpp"
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
                                      IndexOrderedPropertyValues const &values, Transaction *transaction, View view,
                                      bool use_cache = true) {
  bool exists = true;
  bool deleted = false;
  bool has_label = false;
  auto current_values_equal_to_value = std::vector<bool>{};
  const Delta *delta = nullptr;
  auto guard = std::shared_lock{vertex.lock};
  delta = vertex.delta();
  deleted = vertex.deleted();
  if (!delta && deleted) return false;
  has_label = std::ranges::contains(vertex.labels, label);
  if (!delta && !has_label) return false;
  current_values_equal_to_value = helper.MatchesValues(vertex.properties, values);

  // If vertex has non-sequential deltas, hold lock while applying them
  if (!vertex.has_uncommitted_non_sequential_deltas()) {
    guard.unlock();
  }

  // Checking cache has a cost, only do it if we have any deltas
  // if we have no deltas then what we already have from the vertex is correct.
  if (delta && transaction->isolation_level != IsolationLevel::READ_UNCOMMITTED) {
    // IsolationLevel::READ_COMMITTED would be tricky to propagate invalidation to
    // so for now only cache for IsolationLevel::SNAPSHOT_ISOLATION
    auto const useCache = use_cache && transaction->UseCache();
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

    // Unlock if we still hold the lock (i.e., vertex had non-sequential deltas)
    if (guard.owns_lock()) {
      guard.unlock();
    }

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
    delta = vertex.delta();
    deleted = vertex.deleted();
    if (delta == nullptr && deleted) return false;
    has_label = std::ranges::contains(vertex.labels, label);
    if (delta == nullptr && !has_label) return false;
    current_values_equal_to_value = helper.MatchesValues(vertex.properties, values);
  }

  if (exists && !deleted && has_label && std::ranges::all_of(current_values_equal_to_value, std::identity{})) {
    return true;
  }

  constexpr auto interesting = details::ActionSet<Delta::Action::ADD_LABEL,
                                                  Delta::Action::REMOVE_LABEL,
                                                  Delta::Action::SET_PROPERTY,
                                                  Delta::Action::RECREATE_OBJECT,
                                                  Delta::Action::DELETE_DESERIALIZED_OBJECT,
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

// Advances the index iterator to the next valid entry within [lower_bound, upper_bound].
// Bounds always refer to *value* ordering (lower_bound <= upper_bound), regardless of
// iteration direction. For DESC indices, the caller sets reverse_iteration=true which
// flips the UNDER/OVER early-termination semantics: in ASC iteration values increase so
// UNDER means "skip, will reach range" and OVER means "stop"; in DESC iteration values
// decrease so UNDER means "stop, past range" and OVER means "skip, will reach range".
void AdvanceUntilValid_(auto &index_iterator, const auto &end, auto *&current_vertex, auto &current_vertex_accessor,
                        auto *storage, auto *transaction, auto view, auto label, const auto &lower_bound,
                        const auto &upper_bound, bool &skip_lower_bound_check, auto &permutation_helper,
                        bool use_cache = true, bool reverse_iteration = false) {
  for (; index_iterator != end; ++index_iterator) {
    if (index_iterator->vertex == current_vertex) {
      continue;
    }

    if (!CanSeeEntityWithTimestamp(index_iterator->timestamp, transaction, view)) {
      continue;
    }

    // Check the prefix has at least one non-null value
    if (!lower_bound.empty()) {
      auto const prefix_values_only = index_iterator->values.values_ | ranges::views::take(lower_bound.size());
      auto const all_null = ranges::all_of(prefix_values_only, [](PropertyValue const &pv) { return pv.IsNull(); });
      if (all_null) continue;
    }

    enum class InBoundResult : uint8_t { UNDER, IN_BOUNDS, IN_BOUNDS_AT_UB, OVER };

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

    enum class Result : uint8_t { Skip, NoMoreValidEntries, WithAllBounds };

    // In ASC iteration: values increase, so UNDER → skip (will reach range), OVER → stop (past range).
    // In DESC iteration: values decrease, so UNDER → stop (past range), OVER → skip (will reach range).
    auto const out_of_range_below = reverse_iteration ? Result::NoMoreValidEntries : Result::Skip;
    auto const out_of_range_above_all_at_boundary = reverse_iteration ? Result::Skip : Result::NoMoreValidEntries;
    auto const out_of_range_above_not_at_boundary = Result::Skip;

    auto bounds_checker = [&]() {
      auto at_boundary_counter = 0;
      // level 0
      if (skip_lower_bound_check) {
        switch (value_within_upper_bounds(upper_bound[0], index_iterator->values.values_[0])) {
          case InBoundResult::UNDER:
            DMG_ASSERT(false, "this can't happen");
            break;
          case InBoundResult::IN_BOUNDS:
            break;
          case InBoundResult::IN_BOUNDS_AT_UB: {
            ++at_boundary_counter;
            break;
          }
          case InBoundResult::OVER: {
            return out_of_range_above_all_at_boundary;
          }
        };
      }
      // rest of the levels
      for (auto level = skip_lower_bound_check ? 1 : 0; level < lower_bound.size(); ++level) {
        switch (value_within_bounds(lower_bound[level], upper_bound[level], index_iterator->values.values_[level])) {
          case InBoundResult::UNDER: {
            // At level 0, out_of_range_below is direction-dependent: ASC→Skip, DESC→NoMoreValidEntries.
            // At level >= 1, a secondary property below range doesn't mean all remaining entries are invalid
            // (the next entry may have a different primary value), so always Skip.
            return level == 0 ? out_of_range_below : Result::Skip;
          }
          case InBoundResult::IN_BOUNDS: {
            break;
          }
          case InBoundResult::IN_BOUNDS_AT_UB: {
            ++at_boundary_counter;
            break;
          }
          case InBoundResult::OVER: {
            auto const all_preceeding_levels_at_boundary = at_boundary_counter == level;
            return all_preceeding_levels_at_boundary ? out_of_range_above_all_at_boundary
                                                     : out_of_range_above_not_at_boundary;
          }
        }
      }
      return Result::WithAllBounds;
    };

    auto const res = bounds_checker();
    skip_lower_bound_check = false;
    if (res == Result::Skip) {
      continue;
    }
    if (res == Result::NoMoreValidEntries) {
      index_iterator = end;
      break;
    }

    if (CurrentVersionHasLabelProperties(*index_iterator->vertex,
                                         label,
                                         *permutation_helper,
                                         index_iterator->values,
                                         transaction,
                                         view,
                                         use_cache)) {
      current_vertex = index_iterator->vertex;
      current_vertex_accessor = VertexAccessor(current_vertex, storage, transaction);
      break;
    }
  }
}

}  // namespace

template <bool Reverse>
bool InMemoryLabelPropertyIndex::BasicEntry<Reverse>::operator<(std::vector<PropertyValue> const &rhs) const {
  auto span = std::span{values.values_.begin(), std::min(rhs.size(), values.values_.size())};
  // In DESC, "less than" in skip-list terms means "greater than" in value terms.
  if constexpr (Reverse) {
    return std::ranges::lexicographical_compare(rhs, span);
  } else {
    return std::ranges::lexicographical_compare(span, rhs);
  }
}

template <bool Reverse>
bool InMemoryLabelPropertyIndex::BasicEntry<Reverse>::operator==(std::vector<PropertyValue> const &rhs) const {
  return std::ranges::equal(std::span{values.values_.begin(), std::min(rhs.size(), values.values_.size())}, rhs);
}

template <bool Reverse>
bool InMemoryLabelPropertyIndex::BasicEntry<Reverse>::operator<=(std::vector<PropertyValue> const &rhs) const {
  return *this < rhs || *this == rhs;
}

inline void TryInsertLabelPropertiesIndex(Vertex &vertex, LabelId label, PropertiesPermutationHelper const &props,
                                          auto &&index_accessor,
                                          std::optional<SnapshotObserverInfo> const &snapshot_info) {
  // observe regardless
  if (snapshot_info) {
    snapshot_info->Update(UpdateType::VERTICES);
  }

  if (vertex.deleted() || !std::ranges::contains(vertex.labels, label)) {
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
    deleted = vertex.deleted();
    delta = vertex.delta();
    has_label = std::ranges::contains(vertex.labels, label);
    properties = props.Extract(vertex.properties);

    // If vertex has non-sequential deltas, hold lock while applying them
    if (!vertex.has_uncommitted_non_sequential_deltas()) {
      guard.unlock();
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
    ActiveIndicesUpdater const &updater, std::optional<SnapshotObserverInfo> const &snapshot_info, IndexOrder order) {
  auto res = RegisterIndex(label, properties, updater, order);
  if (!res) return false;
  auto res2 = PopulateIndex(label, properties, std::move(vertices), parallel_exec_info, updater, snapshot_info, order);
  if (!res2) {
    MG_ASSERT(false, "Index population can't fail, there was no cancellation callback.");
  }
  return PublishIndex(label, properties, 0, order);
}

namespace {
// Inserts a new index into indices_map, updates the all_indices tracking list,
// and populates the reverse lookup. Returns false if the index already exists.
// Caller must hold the all_indices_ lock when passing all_indexes.
template <typename IndicesMap, typename AllIndicesVec, typename ReverseLookup>
bool RegisterIntoIndicesMap(IndicesMap &indices_map, AllIndicesVec &all_indexes, ReverseLookup &reverse_lookup,
                            LabelId label, PropertiesPaths const &properties) {
  auto [it1, _] = indices_map.try_emplace(label);
  auto &properties_map = it1->second;
  if (properties_map.find(properties) != properties_map.end()) {
    return false;
  }
  auto helper = PropertiesPermutationHelper{properties};
  using IndexPtr = typename std::decay_t<decltype(properties_map)>::mapped_type;
  using IndexT = typename IndexPtr::element_type;
  auto [it3, _2] = properties_map.emplace(properties, std::make_shared<IndexT>(std::move(helper)));
  auto new_all_indexes = *all_indexes;
  new_all_indexes.emplace_back(it3->second, label, properties);
  using Vec = std::decay_t<decltype(*all_indexes)>;
  all_indexes = std::make_shared<Vec>(std::move(new_all_indexes));
  using EntryDetail = std::tuple<PropertiesPaths const *, IndexT *>;
  auto de = EntryDetail{&it3->first, it3->second.get()};
  for (auto &&property_path : properties) {
    reverse_lookup[property_path[0]].insert({label, de});
  }
  return true;
}
}  // namespace

bool InMemoryLabelPropertyIndex::RegisterIndex(LabelId label, PropertiesPaths const &properties,
                                               ActiveIndicesUpdater const &updater, IndexOrder order) {
  return index_.WithLock([&](std::shared_ptr<const IndexContainer> &index) {
    auto new_index = std::make_shared<IndexContainer>(*index);

    bool registered = all_indices_.WithLock([&](AllIndicesData &data) {
      if (order == IndexOrder::ASC)
        return RegisterIntoIndicesMap(
            new_index->asc_indices_, data.asc, new_index->asc_reverse_lookup_, label, properties);
      return RegisterIntoIndicesMap(
          new_index->desc_indices_, data.desc, new_index->desc_reverse_lookup_, label, properties);
    });
    if (!registered) return false;

    index = std::move(new_index);
    updater(std::make_shared<ActiveIndices>(index));
    return true;
  });
}

auto InMemoryLabelPropertyIndex::PopulateIndex(
    LabelId label, PropertiesPaths const &properties, utils::SkipList<Vertex>::Accessor vertices,
    const std::optional<durability::ParallelizedSchemaCreationInfo> &parallel_exec_info,
    ActiveIndicesUpdater const &updater, std::optional<SnapshotObserverInfo> const &snapshot_info, IndexOrder order,
    Transaction const *tx, CheckCancelFunction cancel_check) -> std::expected<void, IndexPopulateError> {
  auto populate = [&](auto index) {
    if (!index) {
      MG_ASSERT(false, "It should not be possible to remove the index before populating it.");
    }

    spdlog::trace("Vertices size when creating index: {}", vertices.size());

    auto const accessor_factory = [&] { return index->skiplist.access(); };

    if (tx) {
      auto const insert_function = [&](Vertex &vertex, auto &index_accessor) {
        TryInsertLabelPropertiesIndex(vertex, label, index->permutations_helper, index_accessor, snapshot_info, *tx);
      };
      PopulateIndexDispatch(vertices, accessor_factory, insert_function, std::move(cancel_check), parallel_exec_info);
    } else {
      auto const insert_function = [&](Vertex &vertex, auto &index_accessor) {
        TryInsertLabelPropertiesIndex(vertex, label, index->permutations_helper, index_accessor, snapshot_info);
      };
      PopulateIndexDispatch(vertices, accessor_factory, insert_function, std::move(cancel_check), parallel_exec_info);
    }
  };

  try {
    (order == IndexOrder::ASC) ? populate(GetIndividualIndex<Entry>(label, properties))
                               : populate(GetIndividualIndex<DescEntry>(label, properties));
  } catch (const PopulateCancel &) {
    DropSingleOrder(label, properties, updater, order);
    return std::unexpected{IndexPopulateError::Cancellation};
  } catch (const utils::OutOfMemoryException &) {
    DropSingleOrder(label, properties, updater, order);
    throw;
  }

  return {};
}

bool InMemoryLabelPropertyIndex::PublishIndex(LabelId label, PropertiesPaths const &properties,
                                              uint64_t commit_timestamp, IndexOrder order) {
  auto publish = [&](auto &&index) {
    if (!index) return false;
    index->Publish(commit_timestamp);
    return true;
  };
  return (order == IndexOrder::ASC) ? publish(GetIndividualIndex<Entry>(label, properties))
                                    : publish(GetIndividualIndex<DescEntry>(label, properties));
}

template <typename EntryT>
void InMemoryLabelPropertyIndex::IndividualIndex<EntryT>::Publish(uint64_t commit_timestamp) {
  status.Commit(commit_timestamp);
  memgraph::metrics::IncrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);
}

template <typename EntryT>
InMemoryLabelPropertyIndex::IndividualIndex<EntryT>::~IndividualIndex() {
  if (status.IsReady()) {
    memgraph::metrics::DecrementCounter(memgraph::metrics::ActiveLabelPropertyIndices);
  }
}

template <typename EntryT>
auto InMemoryLabelPropertyIndex::GetIndividualIndex(LabelId const &label, PropertiesPaths const &properties) const
    -> std::shared_ptr<IndividualIndex<EntryT>> {
  return index_.WithReadLock(
      [&](std::shared_ptr<IndexContainer const> const &index) -> std::shared_ptr<IndividualIndex<EntryT>> {
        auto const &indices_map = [&]() -> auto const & {
          if constexpr (std::same_as<EntryT, DescEntry>) {
            return index->desc_indices_;
          } else {
            return index->asc_indices_;
          }
        }();
        auto it1 = indices_map.find(label);
        if (it1 == indices_map.cend()) [[unlikely]]
          return {};
        auto &properties_map = it1->second;
        auto it2 = properties_map.find(properties);
        if (it2 == properties_map.cend()) [[unlikely]]
          return {};
        return it2->second;
      });
}

void InMemoryLabelPropertyIndex::ActiveIndices::UpdateOnAddLabel(LabelId added_label, Vertex *vertex_after_update,
                                                                 const Transaction &tx) {
  auto const prop_ids = vertex_after_update->properties.ExtractPropertyIds();

  auto const relevant_index = [&](auto &&each) {
    auto &[index_props, _] = each;
    auto vector_has_property = [&](auto &&index_prop) { return r::binary_search(prop_ids, index_prop); };
    return r::any_of(index_props[0], vector_has_property);
  };

  auto const insert_into = [&](auto &indices_map) {
    auto const it = indices_map.find(added_label);
    if (it == indices_map.cend()) return;
    for (auto &[props, index] : it->second | rv::filter(relevant_index)) {
      auto &[permutations_helper, skiplist, status] = *index;
      auto values = permutations_helper.Extract(vertex_after_update->properties);
      if (r::any_of(values, [](auto &&val) { return !val.IsNull(); })) {
        auto acc = skiplist.access();
        acc.insert({permutations_helper.ApplyPermutation(std::move(values)), vertex_after_update, tx.start_timestamp});
      }
    }
  };

  index_container_->ForEachIndicesMap(insert_into);
}

void InMemoryLabelPropertyIndex::ActiveIndices::UpdateOnSetProperty(PropertyId property, const PropertyValue &value,
                                                                    Vertex *vertex, const Transaction &tx) {
  auto const has_label = [&](auto &&each) { return r::contains(vertex->labels, each.first); };

  auto const update_from_reverse_lookup = [&](auto &reverse_lookup) {
    auto const it = reverse_lookup.find(property);
    if (it == reverse_lookup.end()) return;

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
  };

  index_container_->ForEachReverseLookup(update_from_reverse_lookup);
}

namespace {
// Helper to drop an index from a specific indices map and reverse lookup.
// Returns true if the index was found and dropped.
template <typename IndicesMap, typename ReverseLookup>
bool DropFromIndicesMap(IndicesMap &indices_map_for_label, ReverseLookup &reverse_lookup,
                        std::vector<PropertyPath> const &properties, LabelId label) {
  auto it2 = indices_map_for_label.find(properties);
  if (it2 == indices_map_for_label.end()) return false;

  // Erase the reverse lookup before removing the index entry
  for (auto const &prop_selector : properties) {
    auto it3 = reverse_lookup.find(prop_selector[0]);
    if (it3 == reverse_lookup.cend()) continue;
    auto &label_map = it3->second;
    auto [b, e] = label_map.equal_range(label);
    while (b != e) {
      auto const &[props_key_ptr, _] = b->second;
      if (props_key_ptr == &it2->first) {
        b = label_map.erase(b);
      } else {
        ++b;
      }
    }
    if (label_map.empty()) {
      reverse_lookup.erase(it3);
    }
  }

  indices_map_for_label.erase(it2);
  return true;
}
}  // namespace

namespace {
// Drops the index for (label, properties) from indices_map + reverse_lookup.
// Also erases the label key from indices_map if it becomes empty.
// Returns true if the index was found and dropped.
template <typename IndicesMap, typename ReverseLookup>
bool DropFromOrder(IndicesMap &indices_map, ReverseLookup &reverse_lookup, LabelId label,
                   std::vector<PropertyPath> const &properties) {
  auto it1 = indices_map.find(label);
  if (it1 == indices_map.end()) return false;
  auto &properties_map = it1->second;
  bool ok = DropFromIndicesMap(properties_map, reverse_lookup, properties, label);
  if (ok && properties_map.empty()) {
    indices_map.erase(it1);
  }
  return ok;
}
}  // namespace

void InMemoryLabelPropertyIndex::CleanupStatsForDrop(IndexContainer const &new_index, LabelId label,
                                                     std::vector<PropertyPath> const &properties) {
  auto const make_props_subspan = [&](std::size_t length) {
    return std::span{properties.cbegin(), properties.cbegin() + length + 1};
  };
  auto const count_prefix_usage = [&](auto &label_indices, std::size_t prefix_len) -> std::size_t {
    auto const prefix = make_props_subspan(prefix_len);
    return ranges::count_if(label_indices, [&](auto &&each) {
      auto &&[index_properties, _] = each;
      return ranges::starts_with(index_properties, prefix);
    });
  };

  auto stats_ptr = stats_.Lock();
  auto it1 = stats_ptr->find(label);
  if (it1 == stats_ptr->end()) return;

  auto &stats_properties_map = it1->second;
  for (std::size_t prefix_len = 0; prefix_len < properties.size(); ++prefix_len) {
    std::size_t total_usage = 0;
    if (auto ait = new_index.asc_indices_.find(label); ait != new_index.asc_indices_.end()) {
      total_usage += count_prefix_usage(ait->second, prefix_len);
    }
    if (auto dit = new_index.desc_indices_.find(label); dit != new_index.desc_indices_.end()) {
      total_usage += count_prefix_usage(dit->second, prefix_len);
    }
    if (total_usage != 0) continue;

    auto it2 = stats_properties_map.find(make_props_subspan(prefix_len));
    if (it2 == stats_properties_map.end()) continue;
    stats_properties_map.erase(it2);
    if (stats_properties_map.empty()) {
      stats_ptr->erase(it1);
      break;
    }
  }
}

LabelPropertyIndex::DropResult InMemoryLabelPropertyIndex::DropIndex(LabelId label,
                                                                     std::vector<PropertyPath> const &properties,
                                                                     ActiveIndicesUpdater const &updater) {
  // Single lock+copy: drop from both ASC and DESC in one atomic update
  auto result = index_.WithLock([&](std::shared_ptr<IndexContainer const> &index) -> DropResult {
    auto new_index = std::make_shared<IndexContainer>(*index);
    bool dropped_asc = DropFromOrder(new_index->asc_indices_, new_index->asc_reverse_lookup_, label, properties);
    bool dropped_desc = DropFromOrder(new_index->desc_indices_, new_index->desc_reverse_lookup_, label, properties);
    if (!dropped_asc && !dropped_desc) return {};

    CleanupStatsForDrop(*new_index, label, properties);
    index = std::move(new_index);
    updater(std::make_shared<ActiveIndices>(index));
    return {dropped_asc, dropped_desc};
  });
  CleanupAllIndices();
  return result;
}

void InMemoryLabelPropertyIndex::DropSingleOrder(LabelId label, std::vector<PropertyPath> const &properties,
                                                 ActiveIndicesUpdater const &updater, IndexOrder order) {
  index_.WithLock([&](std::shared_ptr<IndexContainer const> &index) {
    auto new_index = std::make_shared<IndexContainer>(*index);
    bool dropped = (order == IndexOrder::ASC)
                       ? DropFromOrder(new_index->asc_indices_, new_index->asc_reverse_lookup_, label, properties)
                       : DropFromOrder(new_index->desc_indices_, new_index->desc_reverse_lookup_, label, properties);
    if (!dropped) return;
    CleanupStatsForDrop(*new_index, label, properties);
    index = std::move(new_index);
    updater(std::make_shared<ActiveIndices>(index));
  });
  CleanupAllIndices();
}

bool InMemoryLabelPropertyIndex::ActiveIndices::IndexExists(LabelId label,
                                                            std::span<PropertyPath const> properties) const {
  auto check = [&](auto &indices_map) {
    auto it = indices_map.find(label);
    if (it != indices_map.end()) {
      auto it2 = it->second.find(properties);
      return it2 != it->second.end();
    }
    return false;
  };
  return check(index_container_->asc_indices_) || check(index_container_->desc_indices_);
}

bool InMemoryLabelPropertyIndex::ActiveIndices::IndexReady(LabelId label,
                                                           std::span<PropertyPath const> properties) const {
  auto check = [&](auto &indices_map) {
    auto it = indices_map.find(label);
    if (it != indices_map.end()) {
      auto it2 = it->second.find(properties);
      if (it2 != it->second.end()) {
        return it2->second->status.IsReady();
      }
    }
    return false;
  };
  return check(index_container_->asc_indices_) || check(index_container_->desc_indices_);
}

auto InMemoryLabelPropertyIndex::ActiveIndices::RelevantLabelPropertiesIndicesInfo(
    std::span<LabelId const> labels, std::span<PropertyPath const> properties) const
    -> std::vector<LabelPropertiesIndicesInfo> {
  auto res = std::vector<LabelPropertiesIndicesInfo>{};
  auto ppos_indices = rv::iota(size_t{}, properties.size()) | r::to<std::vector>();
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

  r::sort(rv::zip(properties_vec, ppos_indices), std::less{}, [](auto const &val) -> PropertyPath const & {
    return std::get<0>(val);
  });

  auto const collect_from = [&](auto &indices_map, IndexOrder order) {
    for (auto [l_pos, label] : ranges::views::enumerate(labels)) {
      auto it = indices_map.find(label);
      if (it == indices_map.end()) continue;

      for (const auto &[nested_props, index] : it->second) {
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
          auto &info = res.emplace_back(l_pos, std::move(positions), label, nested_props);
          info.order_ = order;
        }
      }
    }
  };

  collect_from(index_container_->asc_indices_, IndexOrder::ASC);
  collect_from(index_container_->desc_indices_, IndexOrder::DESC);

  return res;
}

auto InMemoryLabelPropertyIndex::ActiveIndices::ListIndices(uint64_t start_timestamp) const
    -> std::vector<std::pair<LabelId, std::vector<PropertyPath>>> {
  std::vector<std::pair<LabelId, std::vector<PropertyPath>>> ret;

  auto const count_from = [](auto &indices_map) {
    return r::fold_left(
        indices_map, size_t{}, [](auto sum, auto const &label_map) { return sum + label_map.second.size(); });
  };
  ret.reserve(count_from(index_container_->asc_indices_) + count_from(index_container_->desc_indices_));

  auto const collect_from = [&](auto &indices_map) {
    for (auto const &[label, indices] : indices_map) {
      for (auto const &[props, index] : indices) {
        if (index->status.IsVisible(start_timestamp)) {
          ret.emplace_back(label, props);
        }
      }
    }
  };

  index_container_->ForEachIndicesMap(collect_from);
  return ret;
}

auto InMemoryLabelPropertyIndex::ActiveIndices::ListIndices(uint64_t start_timestamp, IndexOrder order) const
    -> std::vector<std::pair<LabelId, std::vector<PropertyPath>>> {
  std::vector<std::pair<LabelId, std::vector<PropertyPath>>> ret;
  auto const collect_from = [&](auto const &indices_map) {
    for (auto const &[label, indices] : indices_map) {
      for (auto const &[props, index] : indices) {
        if (index->status.IsVisible(start_timestamp)) {
          ret.emplace_back(label, props);
        }
      }
    }
  };
  if (order == IndexOrder::ASC) {
    collect_from(index_container_->asc_indices_);
  } else {
    collect_from(index_container_->desc_indices_);
  }
  return ret;
}

void InMemoryLabelPropertyIndex::RemoveObsoleteEntries(uint64_t oldest_active_start_timestamp, std::stop_token token) {
  auto maybe_stop = utils::ResettableCounter(2048);

  CleanupAllIndices();

  auto const remove_from = [&](auto const &all_indexes) {
    for (auto &[index, label_id, property_paths] : *all_indexes) {
      if (token.stop_requested()) return;

      auto const &permutationHelper = index->permutations_helper;
      auto index_acc = index->skiplist.access();
      auto it = index_acc.begin();
      auto end_it = index_acc.end();
      if (it == end_it) continue;
      while (true) {
        if (maybe_stop() && token.stop_requested()) return;

        auto next_it = it;
        ++next_it;

        bool has_next = next_it != end_it;
        if (it->timestamp < oldest_active_start_timestamp) {
          bool redundant_duplicate = has_next && it->vertex == next_it->vertex && it->values == next_it->values;
          if (redundant_duplicate || !AnyVersionHasLabelProperties(*it->vertex,
                                                                   label_id,
                                                                   property_paths,
                                                                   permutationHelper,
                                                                   it->values,
                                                                   oldest_active_start_timestamp)) {
            index_acc.remove(*it);
          }
        }
        if (!has_next) break;
        it = next_it;
      }
    }
  };

  auto data = all_indices_.ReadCopy();
  remove_from(data.asc);
  remove_from(data.desc);
}

template <typename EntryT>
InMemoryLabelPropertyIndex::Iterable<EntryT>::Iterator::Iterator(
    Iterable *self, typename utils::SkipList<EntryT>::Iterator index_iterator)
    : self_(self),
      index_iterator_(index_iterator),
      current_vertex_accessor_(nullptr, self_->storage_, nullptr),
      current_vertex_(nullptr) {
  AdvanceUntilValid();
}

template <typename EntryT>
typename InMemoryLabelPropertyIndex::Iterable<EntryT>::Iterator &
InMemoryLabelPropertyIndex::Iterable<EntryT>::Iterator::operator++() {
  ++index_iterator_;
  AdvanceUntilValid();
  return *this;
}

template <typename EntryT>
void InMemoryLabelPropertyIndex::Iterable<EntryT>::Iterator::AdvanceUntilValid() {
  constexpr bool is_desc = std::same_as<EntryT, DescEntry>;
  AdvanceUntilValid_(index_iterator_,
                     self_->index_accessor_.end(),
                     current_vertex_,
                     current_vertex_accessor_,
                     self_->storage_,
                     self_->transaction_,
                     self_->view_,
                     self_->label_,
                     self_->lower_bound_,
                     self_->upper_bound_,
                     skip_lower_bound_check_,
                     self_->permutation_helper_,
                     /*use_cache=*/true,
                     /*reverse_iteration=*/is_desc);
}

template <typename EntryT>
InMemoryLabelPropertyIndex::Iterable<EntryT>::Iterable(typename utils::SkipList<EntryT>::Accessor index_accessor,
                                                       utils::SkipList<Vertex>::ConstAccessor vertices_accessor,
                                                       LabelId label, PropertiesPaths const *properties,
                                                       PropertiesPermutationHelper const *permutation_helper,
                                                       std::span<PropertyValueRange const> ranges, View view,
                                                       Storage *storage, Transaction *transaction)
    : pin_accessor_(std::move(vertices_accessor)),
      index_accessor_(std::move(index_accessor)),
      label_(label),
      properties_(properties),
      permutation_helper_{permutation_helper},
      view_(view),
      storage_(storage),
      transaction_(transaction) {
  bounds_valid_ = ValidateBounds(ranges, lower_bound_, upper_bound_);  // NOLINT
}

template <typename EntryT>
typename InMemoryLabelPropertyIndex::Iterable<EntryT>::Iterator InMemoryLabelPropertyIndex::Iterable<EntryT>::begin() {
  if (!bounds_valid_) return {this, index_accessor_.end()};
  auto index_iterator = index_accessor_.begin();
  if constexpr (std::same_as<EntryT, DescEntry>) {
    // For DESC index, we seek to the upper bound (highest value), because forward
    // iteration in DESC goes from high to low values.
    if (const auto upper_bound = GenerateBounds(upper_bound_, kLargestProperty); upper_bound) {
      index_iterator = index_accessor_.find_equal_or_greater(*upper_bound);
    }
  } else {
    if (const auto lower_bound = GenerateBounds(lower_bound_, kSmallestProperty); lower_bound) {
      index_iterator = index_accessor_.find_equal_or_greater(*lower_bound);
    }
  }
  return {this, index_iterator};
}

template <typename EntryT>
typename InMemoryLabelPropertyIndex::Iterable<EntryT>::Iterator InMemoryLabelPropertyIndex::Iterable<EntryT>::end() {
  return {this, index_accessor_.end()};
}

uint64_t InMemoryLabelPropertyIndex::ActiveIndices::ApproximateVertexCount(LabelId label,
                                                                           std::span<PropertyPath const> properties,
                                                                           std::optional<IndexOrder> order) const {
  auto result = WithFoundIndex(label, properties, [](auto &index) -> uint64_t { return index.skiplist.size(); }, order);
  DMG_ASSERT(
      result, "Index for label {} and properties {} doesn't exist", label.AsUint(), JoinPropertiesAsString(properties));
  return *result;
}

uint64_t InMemoryLabelPropertyIndex::ActiveIndices::ApproximateVertexCount(LabelId label,
                                                                           std::span<PropertyPath const> properties,
                                                                           std::span<PropertyValue const> values,
                                                                           std::optional<IndexOrder> order) const {
  auto result = WithFoundIndex(
      label,
      properties,
      [&](auto &index) -> uint64_t {
        auto acc = index.skiplist.access();
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
      },
      order);
  DMG_ASSERT(
      result, "Index for label {} and properties {} doesn't exist", label.AsUint(), JoinPropertiesAsString(properties));
  return *result;
}

uint64_t InMemoryLabelPropertyIndex::ActiveIndices::ApproximateVertexCount(LabelId label,
                                                                           std::span<PropertyPath const> properties,
                                                                           std::span<PropertyValueRange const> bounds,
                                                                           std::optional<IndexOrder> order) const {
  auto result = WithFoundIndex(
      label,
      properties,
      [&](auto &index) -> uint64_t {
        auto acc = index.skiplist.access();
        auto in_bounds_for_all_prefix = [&](auto const &entry) {
          constexpr auto within_bounds = [](PropertyValue const &value, PropertyValueRange const &bounds) -> bool {
            return bounds.IsValueInRange(value);
          };
          auto value_within_bounds = [&](auto &&p) { return std::apply(within_bounds, p); };
          return std::ranges::all_of(std::ranges::views::zip(entry.values.values_, bounds), value_within_bounds);
        };
        return std::ranges::count_if(acc.sampling_range(), in_bounds_for_all_prefix);
      },
      order);
  DMG_ASSERT(
      result, "Index for label {} and properties {} doesn't exist", label.AsUint(), JoinPropertiesAsString(properties));
  return *result;
}

std::vector<std::pair<LabelId, std::vector<PropertyPath>>> InMemoryLabelPropertyIndex::ClearIndexStats() {
  std::vector<std::pair<LabelId, std::vector<PropertyPath>>> deleted_indexes;
  auto locked_stats = stats_.Lock();

  auto const num_stats =
      std::accumulate(locked_stats->cbegin(), locked_stats->cend(), size_t{}, [](auto sum, auto const &label_map) {
        return sum + label_map.second.size();
      });

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
  CleanupAllIndices();

  auto const run_gc = [](auto const &all_indexes) {
    for (auto &[index, _1, _2] : *all_indexes) {
      index->skiplist.run_gc();
    }
  };
  auto data = all_indices_.ReadCopy();
  run_gc(data.asc);
  run_gc(data.desc);
}

template <typename EntryT>
auto InMemoryLabelPropertyIndex::ActiveIndices::Vertices(LabelId label, std::span<PropertyPath const> properties,
                                                         std::span<PropertyValueRange const> values, View view,
                                                         Storage *storage, Transaction *transaction)
    -> Iterable<EntryT> {
  auto vertices_acc = static_cast<InMemoryStorage const *>(storage)->vertices_.access();
  return Vertices<EntryT>(label, properties, values, std::move(vertices_acc), view, storage, transaction);
}

template <typename EntryT>
auto InMemoryLabelPropertyIndex::ActiveIndices::Vertices(
    LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc, View view, Storage *storage,
    Transaction *transaction) -> Iterable<EntryT> {
  auto const &indices_map = IndicesMap<EntryT>();
  auto it = indices_map.find(label);
  DMG_ASSERT(it != indices_map.end(),
             "Index for label {} and properties {} doesn't exist",
             label.AsUint(),
             JoinPropertiesAsString(properties));
  auto it2 = it->second.find(properties);
  DMG_ASSERT(it2 != it->second.end(),
             "Index for label {} and properties {} doesn't exist",
             label.AsUint(),
             JoinPropertiesAsString(properties));

  return {it2->second->skiplist.access(),
          std::move(vertices_acc),
          label,
          &it2->first,
          &it2->second->permutations_helper,
          range,
          view,
          storage,
          transaction};
}

template <typename EntryT>
InMemoryLabelPropertyIndex::ChunkedIterable<EntryT> InMemoryLabelPropertyIndex::ActiveIndices::ChunkedVertices(
    LabelId label, std::span<PropertyPath const> properties, std::span<PropertyValueRange const> range,
    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor vertices_acc, View view, Storage *storage,
    Transaction *transaction, size_t num_chunks) {
  auto const &indices_map = IndicesMap<EntryT>();
  auto it = indices_map.find(label);
  DMG_ASSERT(it != indices_map.end(),
             "Index for label {} and properties {} doesn't exist",
             label.AsUint(),
             JoinPropertiesAsString(properties));
  auto it2 = it->second.find(properties);
  DMG_ASSERT(it2 != it->second.end(),
             "Index for label {} and properties {} doesn't exist",
             label.AsUint(),
             JoinPropertiesAsString(properties));

  return {it2->second->skiplist.access(),
          std::move(vertices_acc),
          label,
          &it2->first,
          &it2->second->permutations_helper,
          range,
          view,
          storage,
          transaction,
          num_chunks};
}

void InMemoryLabelPropertyIndex::DropGraphClearIndices() {
  index_.WithLock([](auto &idx) { idx = std::make_shared<IndexContainer>(); });
  stats_->clear();
  all_indices_.WithLock([](AllIndicesData &data) {
    data.asc = std::make_shared<std::vector<AscAllIndicesEntry> const>();
    data.desc = std::make_shared<std::vector<DescAllIndicesEntry> const>();
  });
}

auto InMemoryLabelPropertyIndex::ActiveIndices::GetAbortProcessor() const -> LabelPropertyIndex::AbortProcessor {
  AbortProcessor res{};

  auto const collect_from = [&](auto &indices_map) {
    for (const auto &[label, per_properties] : indices_map) {
      for (auto const &[props, index] : per_properties) {
        auto const unique_props = std::invoke(
            [](auto props) {
              auto root_props = props | rv::transform([](auto &&el) { return el[0]; }) | r::to<std::vector>();
              r::sort(root_props);
              return rv::unique(root_props) | r::to<std::vector>();
            },
            props);

        for (auto const &root_prop : unique_props) {
          res.l2p[label][root_prop].emplace_back(&props, &index->permutations_helper);
          res.p2l[root_prop][label].emplace_back(&props, &index->permutations_helper);
        }
      }
    }
  };

  index_container_->ForEachIndicesMap(collect_from);
  return res;
}

auto InMemoryLabelPropertyIndex::GetActiveIndices() const -> std::shared_ptr<LabelPropertyIndex::ActiveIndices> {
  return std::make_shared<ActiveIndices>(index_.ReadCopy());
}

void InMemoryLabelPropertyIndex::ActiveIndices::AbortEntries(AbortableInfo const &info, uint64_t start_timestamp) {
  // AbortableInfo is keyed by label+properties and spans both index orders.
  // An entry to abort may only exist in ASC, DESC, or both — soft lookup is intentional.
  auto const abort_from = [&]<bool Move>(auto &indices_map, std::bool_constant<Move>) {
    using EntryT = typename std::decay_t<decltype(indices_map)>::mapped_type::mapped_type::element_type::EntryType;
    for (auto const &[label, by_properties] : info) {
      auto it = indices_map.find(label);
      if (it == indices_map.end()) continue;
      for (auto const &[prop, to_remove] : by_properties) {
        auto it2 = it->second.find(*prop);
        if (it2 == it->second.end()) continue;
        auto acc = it2->second->skiplist.access();
        for (auto &[values, vertex] : to_remove) {
          if constexpr (Move) {
            acc.remove(EntryT{std::move(values), vertex, start_timestamp});
          } else {
            acc.remove(EntryT{values, vertex, start_timestamp});
          }
        }
      }
    }
  };
  abort_from(index_container_->asc_indices_, std::false_type{});
  abort_from(index_container_->desc_indices_, std::true_type{});
}

void InMemoryLabelPropertyIndex::CleanupAllIndices() {
  auto const cleanup = [](auto &indices) {
    auto keep_condition = [](auto const &entry) { return entry.index_.use_count() != 1; };
    if (!r::all_of(*indices, keep_condition)) {
      using Vec = std::decay_t<decltype(*indices)>;
      indices = std::make_shared<Vec>(*indices | rv::filter(keep_condition) | r::to<std::vector>());
    }
  };
  all_indices_.WithLock([&](AllIndicesData &data) {
    cleanup(data.asc);
    cleanup(data.desc);
  });
}

template <typename EntryT>
void InMemoryLabelPropertyIndex::ChunkedIterable<EntryT>::Iterator::AdvanceUntilValid() {
  constexpr bool is_desc = std::same_as<EntryT, DescEntry>;
  AdvanceUntilValid_(index_iterator_,
                     typename utils::SkipList<EntryT>::ChunkedIterator{},
                     current_vertex_,
                     current_vertex_accessor_,
                     self_->storage_,
                     self_->transaction_,
                     self_->view_,
                     self_->label_,
                     self_->lower_bound_,
                     self_->upper_bound_,
                     skip_lower_bound_check_,
                     self_->permutation_helper_,
                     /*use_cache=*/false,
                     /*reverse_iteration=*/is_desc);
}

template <typename EntryT>
InMemoryLabelPropertyIndex::ChunkedIterable<EntryT>::ChunkedIterable(
    typename utils::SkipList<EntryT>::Accessor index_accessor, utils::SkipList<Vertex>::ConstAccessor vertices_accessor,
    LabelId label, PropertiesPaths const *properties, PropertiesPermutationHelper const *permutation_helper,
    std::span<PropertyValueRange const> ranges, View view, Storage *storage, Transaction *transaction,
    size_t num_chunks)
    : pin_accessor_(std::move(vertices_accessor)),
      index_accessor_(std::move(index_accessor)),
      label_(label),
      properties_(properties),
      permutation_helper_(permutation_helper),
      view_(view),
      storage_(storage),
      transaction_(transaction) {
  bounds_valid_ = ValidateBounds(ranges, lower_bound_, upper_bound_);  // NOLINT
  if (!bounds_valid_) return;

  if constexpr (std::same_as<EntryT, DescEntry>) {
    chunks_ = index_accessor_.create_chunks(
        num_chunks, GenerateBounds(upper_bound_, kLargestProperty), GenerateBounds(lower_bound_, kSmallestProperty));
  } else {
    chunks_ = index_accessor_.create_chunks(
        num_chunks, GenerateBounds(lower_bound_, kSmallestProperty), GenerateBounds(upper_bound_, kLargestProperty));
  }
  RechunkIndex<utils::SkipList<EntryT>>(
      chunks_, [](const auto &a, const auto &b) { return a.vertex == b.vertex && a.values == b.values; });
}

// Explicit template instantiations
template struct InMemoryLabelPropertyIndex::BasicEntry<false>;
template struct InMemoryLabelPropertyIndex::BasicEntry<true>;
template struct InMemoryLabelPropertyIndex::IndividualIndex<InMemoryLabelPropertyIndex::Entry>;
template struct InMemoryLabelPropertyIndex::IndividualIndex<InMemoryLabelPropertyIndex::DescEntry>;
template class InMemoryLabelPropertyIndex::Iterable<InMemoryLabelPropertyIndex::Entry>;
template class InMemoryLabelPropertyIndex::Iterable<InMemoryLabelPropertyIndex::DescEntry>;
template class InMemoryLabelPropertyIndex::ChunkedIterable<InMemoryLabelPropertyIndex::Entry>;
template class InMemoryLabelPropertyIndex::ChunkedIterable<InMemoryLabelPropertyIndex::DescEntry>;
template auto InMemoryLabelPropertyIndex::GetIndividualIndex<InMemoryLabelPropertyIndex::Entry>(
    LabelId const &, PropertiesPaths const &) const -> std::shared_ptr<IndividualIndex<Entry>>;
template auto InMemoryLabelPropertyIndex::GetIndividualIndex<InMemoryLabelPropertyIndex::DescEntry>(
    LabelId const &, PropertiesPaths const &) const -> std::shared_ptr<IndividualIndex<DescEntry>>;
template auto InMemoryLabelPropertyIndex::ActiveIndices::Vertices<InMemoryLabelPropertyIndex::Entry>(
    LabelId, std::span<PropertyPath const>, std::span<PropertyValueRange const>, View, Storage *, Transaction *)
    -> Iterable<Entry>;
template auto InMemoryLabelPropertyIndex::ActiveIndices::Vertices<InMemoryLabelPropertyIndex::DescEntry>(
    LabelId, std::span<PropertyPath const>, std::span<PropertyValueRange const>, View, Storage *, Transaction *)
    -> Iterable<DescEntry>;
template auto InMemoryLabelPropertyIndex::ActiveIndices::Vertices<InMemoryLabelPropertyIndex::Entry>(
    LabelId, std::span<PropertyPath const>, std::span<PropertyValueRange const>,
    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor, View, Storage *, Transaction *)
    -> Iterable<Entry>;
template auto InMemoryLabelPropertyIndex::ActiveIndices::Vertices<InMemoryLabelPropertyIndex::DescEntry>(
    LabelId, std::span<PropertyPath const>, std::span<PropertyValueRange const>,
    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor, View, Storage *, Transaction *)
    -> Iterable<DescEntry>;
template InMemoryLabelPropertyIndex::ChunkedIterable<InMemoryLabelPropertyIndex::Entry>
InMemoryLabelPropertyIndex::ActiveIndices::ChunkedVertices<InMemoryLabelPropertyIndex::Entry>(
    LabelId, std::span<PropertyPath const>, std::span<PropertyValueRange const>,
    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor, View, Storage *, Transaction *, size_t);
template InMemoryLabelPropertyIndex::ChunkedIterable<InMemoryLabelPropertyIndex::DescEntry>
InMemoryLabelPropertyIndex::ActiveIndices::ChunkedVertices<InMemoryLabelPropertyIndex::DescEntry>(
    LabelId, std::span<PropertyPath const>, std::span<PropertyValueRange const>,
    memgraph::utils::SkipList<memgraph::storage::Vertex>::ConstAccessor, View, Storage *, Transaction *, size_t);

}  // namespace memgraph::storage
