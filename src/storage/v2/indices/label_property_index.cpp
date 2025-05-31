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

#include "label_property_index.hpp"
#include "storage/v2/transaction.hpp"

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
}  // end namespace

PropertiesPermutationHelper::PropertiesPermutationHelper(std::span<PropertyId const> properties)
    : sorted_properties_(properties.begin(), properties.end()) {
  auto inverse_permutation = rv::iota(size_t{}, properties.size()) | r::to_vector;
  r::sort(rv::zip(inverse_permutation, sorted_properties_), std::less{},
          [](auto const &value) -> decltype(auto) { return (std::get<1>(value)); });
  position_lookup_ = std::move(inverse_permutation);
  cycles_ = build_permutation_cycles(position_lookup_);
}

auto PropertiesPermutationHelper::Extract(PropertyStore const &properties) const -> std::vector<PropertyValue> {
  return properties.ExtractPropertyValuesMissingAsNull(sorted_properties_);
}

auto PropertiesPermutationHelper::ApplyPermutation(std::vector<PropertyValue> values) const
    -> IndexOrderedPropertyValues {
  for (const auto &cycle : cycles_) {
    auto tmp = std::move(values[cycle.front()]);
    for (auto pos : std::span{cycle}.subspan<1>()) {
      tmp = std::exchange(values[pos], tmp);
    }
    values[cycle.front()] = std::move(tmp);
  }

  return {std::move(values)};
}

auto PropertiesPermutationHelper::ApplyPermutation(std::map<PropertyId, PropertyValue> &&values) const
    -> IndexOrderedPropertyValues {
  // TODO: name is not correct, does more than ApplyPermutation
  std::vector<PropertyValue> values_vec;
  values_vec.reserve(sorted_properties_.size());
  for (auto id : sorted_properties_) {
    auto const it = values.find(id);
    if (it == values.end()) {
      values_vec.emplace_back();
    } else {
      values_vec.emplace_back(std::move(it->second));
    }
  }
  return ApplyPermutation(std::move(values_vec));
}

auto PropertiesPermutationHelper::MatchesValue(PropertyId property_id, PropertyValue const &value,
                                               IndexOrderedPropertyValues const &values) const
    -> std::optional<std::pair<std::ptrdiff_t, bool>> {
  auto it = std::ranges::find(sorted_properties_, property_id);
  if (it == sorted_properties_.end()) return std::nullopt;

  auto pos = std::distance(sorted_properties_.begin(), it);
  return std::pair{pos, values.values_[position_lookup_[pos]] == value};
}

auto PropertiesPermutationHelper::MatchesValues(PropertyStore const &properties,
                                                IndexOrderedPropertyValues const &values) const -> std::vector<bool> {
  return properties.ArePropertiesEqual(sorted_properties_, values.values_, position_lookup_);
}

size_t PropertyValueRange::hash() const noexcept {
  auto const lower{lower_ ? lower_->value() : PropertyValue{}};
  auto const upper{upper_ ? upper_->value() : PropertyValue{}};
  auto const prop_value_hash = std::hash<PropertyValue>{};

  std::size_t seed = 0;
  boost::hash_combine(seed, static_cast<size_t>(type_));
  boost::hash_combine(seed, prop_value_hash(lower));
  boost::hash_combine(seed, prop_value_hash(upper));
  return seed;
}

void LabelPropertyIndex::AbortProcessor::process(LabelPropertyIndex &index, Transaction &tx) {
  tx.active_indices_->AbortEntries(cleanup_collection, tx.start_timestamp);
}

}  // namespace memgraph::storage
