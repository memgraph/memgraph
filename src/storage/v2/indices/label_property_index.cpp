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

#include "label_property_index.hpp"

#include <range/v3/all.hpp>
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

/// Which of `properties` end in the name of a field a value carries. Asked as "which id is
/// this name" rather than the other way about: asking for the name behind an id the mapper
/// does not know is fatal, and asking for the id behind an unused name would mint one.
auto PropertiesPermutationHelper::GatherFields(std::span<PropertyPath const> properties, NameIdMapper &name_id_mapper)
    -> std::vector<PropertiesPermutationHelper::FieldsOf> {
  auto field_ids = std::vector<std::pair<PropertyId, ValueField>>{};
  for (auto const &[name, field] : FieldNames()) {
    if (auto const id = name_id_mapper.NameToIdIfExists(name)) {
      field_ids.emplace_back(PropertyId::FromUint(*id), field);
    }
  }

  // Gathered under the property each path starts at. Paths are given in their own order, so
  // ones sharing a property arrive together and a group is closed by the next property.
  auto fields = std::vector<PropertiesPermutationHelper::FieldsOf>{};
  for (auto const &[pos, path] : ranges::views::enumerate(properties)) {
    if (path.size() == 1) continue;
    for (auto const &[id, field] : field_ids) {
      if (id != path.back()) continue;
      if (fields.empty() || fields.back().outer != path[0]) {
        fields.push_back({.outer = path[0], .named = {}});
      }
      fields.back().named.emplace_back(pos, field);
      break;
    }
  }
  return fields;
}

void PropertiesPermutationHelper::OrderPaths(std::span<PropertyPath const> properties) {
  sorted_properties_.assign(properties.begin(), properties.end());
  auto inverse_permutation = rv::iota(size_t{}, properties.size()) | r::to_vector;
  r::sort(rv::zip(inverse_permutation, sorted_properties_), std::less{}, [](auto const &value) -> decltype(auto) {
    return std::get<1>(value);
  });
  position_lookup_ = std::move(inverse_permutation);
  cycles_ = build_permutation_cycles(position_lookup_);
  for (auto const &[pos, path] : ranges::views::enumerate(sorted_properties_)) {
    auto const outer_prop_id = path[0];
    grouped_by_outer_prop_id_[outer_prop_id].emplace_back(pos);
  }
}

PropertiesPermutationHelper::PropertiesPermutationHelper(std::span<PropertyPath const> properties,
                                                         NameIdMapper &name_id_mapper) {
  OrderPaths(properties);
  // Worked out once here rather than for every record read, because resolving a name is a
  // lookup and these paths are read for every record.
  fields_ = GatherFields(sorted_properties_, name_id_mapper);
}

PropertiesPermutationHelper::PropertiesPermutationHelper(std::span<PropertyPath const> properties, WithoutNames) {
  OrderPaths(properties);
  // Not an assertion that compiles away: a caller reaching here with a nested path would
  // otherwise have that path read as naming no field, silently.
  MG_ASSERT(r::none_of(sorted_properties_, [](auto const &path) { return path.size() > 1; }),
            "Reading a key inside a value takes the names behind the ids");
}

auto PropertiesPermutationHelper::FieldAt(std::size_t pos) const -> std::optional<ValueField> {
  // Searched rather than indexed because only the paths that name a field are held, which is
  // usually none of them.
  for (auto const &group : fields_) {
    for (auto const &[at, field] : group.named) {
      if (at == pos) return field;
    }
  }
  return std::nullopt;
}

auto PropertiesPermutationHelper::ReadPath(std::size_t pos, PropertyValue const &value) const
    -> std::optional<PropertyValue> {
  return ReadPropertyPath(value, sorted_properties_[pos].as_span(), FieldAt(pos));
}

auto PropertiesPermutationHelper::Extract(PropertyStore const &properties) const -> std::vector<PropertyValue> {
  auto values = properties.ExtractPropertyValuesMissingAsNull(sorted_properties_);
  ReadFieldsInto(properties, values);
  return values;
}

void PropertiesPermutationHelper::ExtractInto(PropertyStore const &properties, std::span<PropertyValue> out) const {
  properties.ExtractPropertyValuesMissingAsNull(sorted_properties_, out);
  ReadFieldsInto(properties, out);
}

void PropertiesPermutationHelper::ReadFieldsInto(PropertyStore const &properties, std::span<PropertyValue> out) const {
  for (auto const &group : fields_) {
    auto const held = properties.GetProperty(group.outer);
    for (auto const &[pos, field] : group.named) {
      auto read = ReadPropertyPath(held, sorted_properties_[pos].as_span(), field);
      out[pos] = read ? *std::move(read) : PropertyValue{};
    }
  }
}

void PropertiesPermutationHelper::Update(PropertyId outer_prop_id, PropertyValue const &value,
                                         std::vector<PropertyValue> &extracted_values) const {
  auto it = grouped_by_outer_prop_id_.find(outer_prop_id);
  if (it == grouped_by_outer_prop_id_.cend()) return;  // outer_prop_id is irrelevant to this index
  auto const &sorted_positions = it->second;
  for (auto const &pos : sorted_positions) {
    auto read = ReadPath(pos, value);
    // The path need not resolve: `value` may not be a map, or may be one without the property.
    // No value at the path means a null index key.
    extracted_values[pos] = read ? *std::move(read) : PropertyValue{};
  }
}

void PropertiesPermutationHelper::ApplyPermutationInPlace(std::span<PropertyValue> values) const {
  for (const auto &cycle : cycles_) {
    auto tmp = std::move(values[cycle.front()]);
    for (auto pos : std::span{cycle}.subspan<1>()) {
      tmp = std::exchange(values[pos], std::move(tmp));
    }
    values[cycle.front()] = std::move(tmp);
  }
}

auto PropertiesPermutationHelper::ApplyPermutation(std::vector<PropertyValue> values) const
    -> IndexOrderedValuesVector {
  ApplyPermutationInPlace(std::span{values});
  return {std::move(values)};
}

auto PropertiesPermutationHelper::MatchesValue(PropertyId outer_prop_id, PropertyValue const &value,
                                               IndexOrderedValuesView cmp_values) const
    -> std::vector<std::pair<std::ptrdiff_t, bool>> {
  auto enum_properties = rv::enumerate(sorted_properties_);
  auto relevant_paths =
      r::equal_range(enum_properties, outer_prop_id, {}, [&](auto &&el) { return std::get<1>(el)[0]; });

  auto is_match = [&](auto &&el) -> std::pair<std::ptrdiff_t, bool> {
    auto &&[index, path] = el;
    auto const &cmp_value = cmp_values[position_lookup_[index]];
    DMG_ASSERT(!path.empty(), "PropertyPath should be at least 1");
    auto const read = ReadPath(index, value);
    return {index, read ? *read == cmp_value : cmp_value.IsNull()};
  };
  return relevant_paths | rv::transform(is_match) | r::to_vector;
}

void PropertiesPermutationHelper::MatchesValues(PropertyStore const &properties, IndexOrderedValuesView values,
                                                std::vector<bool> &out) const {
  properties.ArePropertiesEqual(sorted_properties_, values, position_lookup_, out);
  for (auto const &group : fields_) {
    auto const held = properties.GetProperty(group.outer);
    for (auto const &[pos, field] : group.named) {
      auto const read = ReadPropertyPath(held, sorted_properties_[pos].as_span(), field);
      auto const &cmp_value = values[position_lookup_[pos]];
      out[pos] = read ? *read == cmp_value : cmp_value.IsNull();
    }
  }
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

void LabelPropertyIndexAbortProcessor::CollectOnPropertyChange(PropertyId propId, Vertex *vertex) {
  const auto &it = lookup->p2l.find(propId);
  if (it == lookup->p2l.end()) return;

  for (auto const &[label, index_info] : it->second) {
    if (!std::ranges::contains(vertex->labels, label)) continue;
    for (auto const &[properties, helper] : index_info) {
      auto current_values = helper->Extract(vertex->properties);
      // Only if current_values has at least one non-null value do we need to cleanup its index entry
      if (ranges::any_of(current_values, [](PropertyValue const &val) { return !val.IsNull(); })) {
        cleanup_collection[label][properties].emplace_back(helper->ApplyPermutation(std::move(current_values)).values_,
                                                           vertex);
      }
    }
  }
}

void LabelPropertyIndexAbortProcessor::CollectOnLabelRemoval(LabelId label, Vertex *vertex) {
  const auto &it = lookup->l2p.find(label);
  if (it == lookup->l2p.end()) return;

  auto dedup = std::set<LabelPropertyIndex::IndexInfo>{};
  for (const auto &[property, index_info] : it->second) {
    for (auto const &info : index_info) {
      dedup.insert(info);
    }
  }
  for (auto const &[properties, helper] : dedup) {
    auto current_values = helper->Extract(vertex->properties);
    // Only if current_values has at least one non-null value do we need to cleanup its index entry
    if (ranges::any_of(current_values, [](PropertyValue const &val) { return !val.IsNull(); })) {
      cleanup_collection[label][properties].emplace_back(helper->ApplyPermutation(std::move(current_values)).values_,
                                                         vertex);
    }
  }
}
}  // namespace memgraph::storage
