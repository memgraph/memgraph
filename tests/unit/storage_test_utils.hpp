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

#pragma once

#include <nlohmann/json_fwd.hpp>

#include "storage/v2/id_types.hpp"
#include "storage/v2/storage.hpp"
#include "storage/v2/view.hpp"

size_t CountVertices(memgraph::storage::Storage::Accessor &storage_accessor, memgraph::storage::View view);

inline constexpr std::array storage_modes{memgraph::storage::StorageMode::IN_MEMORY_ANALYTICAL,
                                          memgraph::storage::StorageMode::IN_MEMORY_TRANSACTIONAL};

inline auto &FindProp(auto &in, std::string_view key) {
  auto &props = in["properties"];
  auto itr = std::find_if(props.begin(), props.end(), [&](const auto &in) { return in["key"] == key; });
  if (itr == props.end()) throw std::exception();
  return *itr;
}

bool ConfrontJSON(const nlohmann::json &lhs, const nlohmann::json &rhs);

/** Test helper to validate the properties of any vertices within the given
 * property ranges.
 * @return number of vertices matching the property ranges.
 */
template <typename Accessor>
std::size_t CheckVertexProperties(std::unique_ptr<Accessor> acc, memgraph::storage::LabelId label,
                                  std::span<memgraph::storage::PropertyPath const> props,
                                  std::span<memgraph::storage::PropertyValueRange const> ranges,
                                  auto &&props_validator) {
  auto iterable = acc->Vertices(label, props, ranges, memgraph::storage::View::OLD);
  size_t found_vertices = 0;
  for (auto it = iterable.begin(); it != iterable.end(); ++it) {
    auto vertex = *it;
    auto results = props | ranges::views::transform([&](auto &&prop) {
                     auto result = vertex.GetProperty(prop[0], memgraph::storage::View::OLD);
                     if (!result) {
                       return memgraph::storage::PropertyValue{};
                     }
                     auto value = ReadNestedPropertyValue(*result, prop | ranges::views::drop(1));
                     return value ? *value : memgraph::storage::PropertyValue{};
                   }) |
                   ranges::to_vector;
    props_validator(results);
    ++found_vertices;
  }
  return found_vertices;
};
