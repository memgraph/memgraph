// Copyright 2024 Memgraph Ltd.
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

inline bool ConfrontJSON(const nlohmann::json &lhs, const nlohmann::json &rhs) {
  if (lhs.type() == rhs.type()) {
    if (lhs.type() == nlohmann::detail::value_t::array) {
      // Comparing two arrays (NO NESTED ARRAYS)
      const auto &lhs_array = lhs.get_ref<const nlohmann::json::array_t &>();
      const auto &rhs_array = rhs.get_ref<const nlohmann::json::array_t &>();
      return std::is_permutation(lhs_array.begin(), lhs_array.end(), rhs_array.begin(), rhs_array.end(), ConfrontJSON);
    }
    if (lhs.type() == nlohmann::detail::value_t::object) {
      const auto &lhs_object = lhs.get_ref<const nlohmann::json::object_t &>();
      const auto &rhs_object = rhs.get_ref<const nlohmann::json::object_t &>();
      if (lhs_object.size() != rhs_object.size()) return false;
      for (const auto &[key, val] : lhs_object) {
        try {
          if (!ConfrontJSON(val, rhs_object.at(key))) return false;
        } catch (std::range_error &) {
          return false;
        }
      }
      return true;
    }
  }
  return lhs == rhs;
}
