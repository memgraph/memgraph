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

#include <span>

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

PropertiesPermutationHelper::PropertiesPermutationHelper(std::span<PropertyPath const> properties)
    : sorted_properties_(properties.begin(), properties.end()) {
  auto inverse_permutation = rv::iota(size_t{}, properties.size()) | r::to_vector;
  r::sort(rv::zip(inverse_permutation, sorted_properties_), std::less{},
          [](auto const &value) -> decltype(auto) { return std::get<1>(value); });
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

auto PropertiesPermutationHelper::MatchesValue(PropertyId outer_prop_id, PropertyValue const &value,
                                               IndexOrderedPropertyValues const &values) const
    -> std::vector<std::pair<std::ptrdiff_t, bool>> {
  auto enum_properties = rv::enumerate(sorted_properties_);
  auto relevant_paths =
      r::equal_range(enum_properties, outer_prop_id, {}, [&](auto &&el) { return std::get<1>(el)[0]; });

  auto is_match = [&](auto &&el) -> std::pair<std::ptrdiff_t, bool> {
    auto &&[index, path] = el;
    auto const &cmp_value = values.values_[position_lookup_[index]];
    // Outer property was already read to get `value`, strip that off of the path
    DMG_ASSERT(!path.empty(), "PropertyPath should be at least 1");
    auto const *nested_value = ReadNestedPropertyValue(value, path | rv::drop(1));
    return {index, nested_value && *nested_value == cmp_value};
  };
  return relevant_paths | rv::transform(is_match) | r::to_vector;
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

}  // namespace memgraph::storage
