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
          [](auto const &value) -> decltype(auto) { return std::get<1>(value)[0]; });
  position_lookup_ = std::move(inverse_permutation);
  cycles_ = build_permutation_cycles(position_lookup_);

  sorted_properties_roots_ = sorted_properties_ | rv::transform([](auto &&path) { return path[0]; }) | r::to_vector;
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

auto PropertiesPermutationHelper::MatchesValue(PropertyId property_id, PropertyValue const &value,
                                               IndexOrderedPropertyValues const &values) const
    -> std::vector<std::pair<std::ptrdiff_t, bool>> {
  auto const compare_nested_value = [](PropertyValue const &outer, PropertyValue const &value,
                                       PropertyPath const &path) {
    PropertyValue const *value_ptr = &outer;
    auto path_it = std::next(path.cbegin());

    // Traverse the nested map of property values until we have found the single
    // nested property we compare against.
    while (std::distance(path_it, path.cend()) != 0) {
      if (!value_ptr->IsMap()) {
        return false;
      }

      auto &map = value_ptr->ValueMap();
      auto map_it = map.find(*path_it++);
      if (map_it == map.end()) {
        // subkey doesn't exist
        return false;
      }

      value_ptr = &map_it->second;
    }

    return *value_ptr == value;
  };

  return rv::enumerate(sorted_properties_) | rv::filter([&](auto &&el) { return std::get<1>(el)[0] == property_id; }) |
         rv::transform([&](auto &&el) -> std::pair<std::ptrdiff_t, bool> {
           auto &&[index, path] = el;
           std::size_t const pos{position_lookup_[index]};
           return {pos, compare_nested_value(value, values.values_[position_lookup_[pos]], path)};
         }) |
         r::to_vector;
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
