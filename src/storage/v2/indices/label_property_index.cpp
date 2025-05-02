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

PropertiesPermutationHelper::PropertiesPermutationHelper(std::span<PropertyId const> properties)
    : PropertiesPermutationHelper{
          properties | rv::transform([](auto &&property_id) { return PropertyPath{property_id}; }) | r::to_vector} {}

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
  auto top_level_values = properties.ExtractPropertyValuesMissingAsNull(sorted_properties_roots_);

  // Assumes `value` is a map, if the key exists, this will extract the nested
  // `PropertyValue` with the given key. Otherwise, a `null` `PropertyValue`
  // is returned.
  // @TODO very poor performance, as we must instantiate all levels
  // of the nested property map as `PropertyValues`. Would it be better to
  // either do this in a single pass, or update the property store reader
  // to allow us to read nested properties directly without needing to
  // instantiate the intermediate values.
  // @TODO We are creating `PropertyValue`s here without using the allocators.
  auto extract_from_map = [](PropertyValue &value, PropertyId key) {
    DMG_ASSERT(value.IsMap());
    auto &&as_map = value.ValueMap();
    auto it = as_map.find(key);
    if (it != as_map.end()) {
      return it->second;
    } else {
      return PropertyValue{};
    }
  };

  // @TODO currently parsing all to extract nested properties. As an optimisation,
  // we can detect whether the index needs the additional parsing, and if not,
  // just return the `top_level_values`.
  auto values = rv::zip(sorted_properties_, top_level_values) | rv::transform([&](auto &&paths_and_values) {
                  auto &&[path, value] = paths_and_values;
                  for (auto &&key : path | rv::drop(1)) {
                    if (value.IsMap()) {
                      value = extract_from_map(value, key);
                    } else {
                      return PropertyValue{};
                    }
                  }
                  return value;
                }) |
                r::to_vector;

  return top_level_values;
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
    -> std::optional<std::pair<std::ptrdiff_t, bool>> {
  // @TODO account for nesting here by using sorted_properties_ rather
  // than just the roots
  auto it = std::ranges::find(sorted_properties_roots_, property_id);
  if (it == sorted_properties_roots_.end()) return std::nullopt;

  auto pos = std::distance(sorted_properties_roots_.begin(), it);
  return std::pair{pos, values.values_[position_lookup_[pos]] == value};
}

auto PropertiesPermutationHelper::MatchesValues(PropertyStore const &properties,
                                                IndexOrderedPropertyValues const &values) const -> std::vector<bool> {
  // @TODO account for nesting here by using sorted_properties, rather than
  // just the roots of the nested values.
  return properties.ArePropertiesEqual(sorted_properties_roots_, values.values_, position_lookup_);
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
