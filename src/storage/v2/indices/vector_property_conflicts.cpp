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

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "storage/v2/name_id_mapper.hpp"
#include "storage/v2/storage.hpp"

namespace memgraph::storage {

namespace {

std::string PropertyName(NameIdMapper &mapper, PropertyId id) { return mapper.IdToName(id.AsUint()); }

std::string LabelName(NameIdMapper &mapper, LabelId id) { return mapper.IdToName(id.AsUint()); }

std::string EdgeTypeName(NameIdMapper &mapper, EdgeTypeId id) { return mapper.IdToName(id.AsUint()); }

std::string JoinPaths(NameIdMapper &mapper, std::vector<PropertyPath> const &paths) {
  std::string out;
  for (auto const &path : paths) {
    if (!out.empty()) out += ", ";
    bool first = true;
    for (auto const prop : path) {
      if (!first) out += ".";
      first = false;
      out += PropertyName(mapper, prop);
    }
  }
  return out;
}

}  // namespace

std::vector<VectorPropertyConflict> FindVectorPropertyConflicts(
    IndicesInfo const &indices, std::span<std::pair<LabelId, std::set<PropertyId>> const> unique_constraints,
    NameIdMapper &name_id_mapper) {
  std::vector<VectorPropertyConflict> conflicts;

  auto add = [&](std::string vector_index, std::string const &index_name, PropertyId property, std::string other) {
    conflicts.push_back({.vector_index = std::move(vector_index),
                         .vector_index_name = index_name,
                         .property = PropertyName(name_id_mapper, property),
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
                      LabelName(name_id_mapper, entry.label),
                      JoinPaths(name_id_mapper, entry.properties)));
    }
    for (auto const property : indices.vertex_property) {
      if (property != spec.property) continue;
      add(vector_index,
          spec.index_name,
          spec.property,
          fmt::format("global vertex property index :({})", PropertyName(name_id_mapper, property)));
    }
    for (auto const &[label, properties] : unique_constraints) {
      if (!properties.contains(spec.property)) continue;
      auto const names =
          properties | std::views::transform([&](PropertyId p) { return PropertyName(name_id_mapper, p); });
      add(vector_index,
          spec.index_name,
          spec.property,
          fmt::format("unique constraint :{}({})", LabelName(name_id_mapper, label), fmt::join(names, ", ")));
    }
  }

  for (auto const &spec : indices.vector_edge_indices_spec) {
    auto const vector_index = fmt::format("vector edge index {}", spec.index_name);
    for (auto const &[edge_type, property] : indices.edge_type_property) {
      if (property != spec.property) continue;
      add(vector_index,
          spec.index_name,
          spec.property,
          fmt::format("edge-type+property index :{}({})",
                      EdgeTypeName(name_id_mapper, edge_type),
                      PropertyName(name_id_mapper, property)));
    }
    for (auto const property : indices.edge_property) {
      if (property != spec.property) continue;
      add(vector_index,
          spec.index_name,
          spec.property,
          fmt::format("global edge property index :({})", PropertyName(name_id_mapper, property)));
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
      "Cannot create {}: property {} is already covered by {}. A property index or unique constraint on a "
      "vector-indexed property returns wrong results. Drop the {} first or store the vectors in a property that is "
      "not indexed.",
      conflict.vector_index,
      conflict.property,
      conflict.other,
      conflict.other);
}

std::string VectorPropertyConflictWarning(VectorPropertyConflict const &conflict) {
  auto capitalised = conflict.vector_index;
  if (!capitalised.empty())
    capitalised[0] = static_cast<char>(std::toupper(static_cast<unsigned char>(capitalised[0])));
  return fmt::format(
      "{} and {} both cover property {}, so queries using the {} can return wrong results. Drop one of them, for "
      "example DROP VECTOR INDEX {};",
      capitalised,
      conflict.other,
      conflict.property,
      conflict.other,
      conflict.vector_index_name);
}

}  // namespace memgraph::storage
