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

#include "storage/v2/enum.hpp"
#include "storage/v2/name_id_mapper.hpp"
#include "utils/result.hpp"

#include <strong_type/strong_type.hpp>

#include <algorithm>
#include <cstdint>
#include <map>
#include <ranges>
#include <string>

namespace memgraph::storage {

enum struct EnumStorageError : uint8_t { EnumExists };

struct EnumStore {
  auto register_enum(std::string enum_name, std::vector<std::string> enum_values)
      -> memgraph::utils::BasicResult<EnumStorageError> {
    auto new_pos = next_enum_id_;
    auto new_id = EnumTypeId{new_pos};

    // Add name to id mapping
    auto [it1, inserted1] = name_to_id_.try_emplace(enum_name, new_id);
    if (!inserted1) return EnumStorageError::EnumExists;
    ++next_enum_id_;
    id_to_name_.resize(next_enum_id_);
    id_to_name_[new_pos] = std::move(enum_name);

    // insert values for the given type
    auto [it2, inserted2] = enum_values_.try_emplace(new_id, std::move(enum_values));
    if (!inserted2) {
      DMG_ASSERT(false, "logical bug");
      name_to_id_.erase(it1);
      --next_enum_id_;
      return EnumStorageError::EnumExists;
    }

    return {};
  }

  std::optional<EnumTypeId> enum_id_of(std::string_view enum_name) {
    auto it = name_to_id_.find(enum_name);
    if (it == name_to_id_.cend()) return std::nullopt;
    return it->second;
  }

  std::optional<EnumValueId> enum_value_id_of(EnumTypeId enum_id, std::string_view enum_member) {
    auto it = enum_values_.find(enum_id);
    if (it == enum_values_.cend()) return std::nullopt;
    auto const &[_, details] = *it;

    auto match = [&](auto &val) { return val == enum_member; };
    auto match_it = std::ranges::find_if(details, match);
    if (match_it == details.cend()) return std::nullopt;

    auto n = static_cast<uint64_t>(std::distance(details.cbegin(), match_it));
    return EnumValueId{n};
  }

  auto enum_name(EnumTypeId id) -> std::optional<std::string> {
    // TODO: maybe guarentee lifetime (shared_ptr?)
    if (id_to_name_.size() <= id.value_of()) return std::nullopt;
    return id_to_name_[id.value_of()];
  }

  auto enum_value(EnumTypeId id, EnumValueId valId) -> std::optional<std::string> {
    auto it = enum_values_.find(id);
    if (it == enum_values_.cend()) return std::nullopt;
    auto &values = it->second;
    if (values.size() <= valId.value_of()) return std::nullopt;
    return values[valId.value_of()];
  }

 private:
  uint64_t next_enum_id_{};
  std::map<std::string, EnumTypeId, std::less<>> name_to_id_;
  std::vector<std::string> id_to_name_;

  std::map<EnumTypeId, std::vector<std::string>> enum_values_;
};

}  // namespace memgraph::storage
