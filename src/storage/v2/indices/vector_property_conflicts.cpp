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

#include "storage/v2/indices/vector_property_conflicts.hpp"

#include <algorithm>
#include <cctype>
#include <ranges>
#include <string_view>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::storage {

namespace {

std::string Name(NameIdMapper &mapper, auto id) { return mapper.IdToName(id.AsUint()); }

// Backtick-quotes a name that is not a plain identifier so the suggested DROP statement parses.
std::string QuoteName(std::string_view name) {
  auto const is_plain =
      !name.empty() && std::isdigit(static_cast<unsigned char>(name.front())) == 0 &&
      std::ranges::all_of(name, [](char c) { return std::isalnum(static_cast<unsigned char>(c)) || c == '_'; });
  if (is_plain) return std::string{name};
  std::string quoted{"`"};
  for (auto const c : name) quoted += c == '`' ? std::string_view{"``"} : std::string_view{&c, 1};
  return quoted + '`';
}

std::string JoinPaths(NameIdMapper &mapper, std::vector<PropertyPath> const &paths) {
  std::vector<std::string> joined;
  for (auto const &path : paths) {
    auto const names = path | std::views::transform([&](PropertyId p) { return Name(mapper, p); });
    joined.push_back(fmt::format("{}", fmt::join(names, ".")));
  }
  return fmt::format("{}", fmt::join(joined, ", "));
}

}  // namespace

std::vector<VectorPropertyConflict> FindVectorPropertyConflicts(
    IndicesInfo const &indices, std::span<std::pair<LabelId, std::set<PropertyId>> const> unique_constraints,
    NameIdMapper &name_id_mapper) {
  std::vector<VectorPropertyConflict> conflicts;

  auto add = [&](std::string vector_index, std::string const &index_name, PropertyId property, std::string other) {
    conflicts.push_back({.vector_index = std::move(vector_index),
                         .vector_index_name = QuoteName(index_name),
                         .property = Name(name_id_mapper, property),
                         .other = std::move(other)});
  };

  for (auto const &spec : indices.vector_indices_spec) {
    auto const vector_index = fmt::format("vector index {}", spec.index_name);
    for (auto const &entry : indices.label_properties) {
      if (std::ranges::none_of(entry.properties, [&](auto const &path) { return path.front() == spec.property; })) {
        continue;
      }
      add(vector_index,
          spec.index_name,
          spec.property,
          fmt::format("label+property index :{}({})",
                      Name(name_id_mapper, entry.label),
                      JoinPaths(name_id_mapper, entry.properties)));
    }
    for (auto const property : indices.vertex_property) {
      if (property != spec.property) continue;
      add(vector_index,
          spec.index_name,
          spec.property,
          fmt::format("global vertex property index :({})", Name(name_id_mapper, property)));
    }
    for (auto const &[label, properties] : unique_constraints) {
      if (!properties.contains(spec.property)) continue;
      auto const names = properties | std::views::transform([&](PropertyId p) { return Name(name_id_mapper, p); });
      add(vector_index,
          spec.index_name,
          spec.property,
          fmt::format("unique constraint :{}({})", Name(name_id_mapper, label), fmt::join(names, ", ")));
    }
  }

  for (auto const &spec : indices.vector_edge_indices_spec) {
    auto const vector_index = fmt::format("vector edge index {}", spec.index_name);
    for (auto const &[edge_type, property] : indices.edge_type_property) {
      if (property != spec.property) continue;
      add(vector_index,
          spec.index_name,
          spec.property,
          fmt::format(
              "edge-type+property index :{}({})", Name(name_id_mapper, edge_type), Name(name_id_mapper, property)));
    }
    for (auto const property : indices.edge_property) {
      if (property != spec.property) continue;
      add(vector_index,
          spec.index_name,
          spec.property,
          fmt::format("global edge property index :({})", Name(name_id_mapper, property)));
    }
  }

  return conflicts;
}

std::string OrdinaryIndexOnVectorPropertyError(VectorPropertyConflict const &conflict) {
  return fmt::format(
      "Cannot create {}: property {} is already indexed by {}. A property index or unique constraint on a "
      "vector-indexed property returns wrong results. Drop the vector index first (DROP VECTOR INDEX {};) or use a "
      "property that is not vector-indexed.",
      conflict.other,
      conflict.property,
      conflict.vector_index,
      conflict.vector_index_name);
}

std::string VectorIndexOnIndexedPropertyError(VectorPropertyConflict const &conflict) {
  return fmt::format(
      "Cannot create {0}: property {1} is already covered by {2}. A property index or unique constraint on a "
      "vector-indexed property returns wrong results. Drop the {2} first or store the vectors in a property that is "
      "not indexed.",
      conflict.vector_index,
      conflict.property,
      conflict.other);
}

std::string VectorPropertyConflictWarning(VectorPropertyConflict const &conflict) {
  auto capitalised = conflict.vector_index;
  if (!capitalised.empty())
    capitalised[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(capitalised[0])));
  return fmt::format(
      "{0} and {1} both cover property {2}, so queries using the {1} can return wrong results. Drop one of them, for "
      "example DROP VECTOR INDEX {3};",
      capitalised,
      conflict.other,
      conflict.property,
      conflict.vector_index_name);
}

}  // namespace memgraph::storage
