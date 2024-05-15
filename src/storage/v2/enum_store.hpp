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
  auto register_enum(std::string type_str, std::vector<std::string> enum_value_strs)
      -> memgraph::utils::BasicResult<EnumStorageError> {
    auto new_pos = next_enum_type_id_;
    auto new_id = EnumTypeId{new_pos};

    // Add name to id mapping
    auto [it1, inserted1] = name_to_etype_.try_emplace(type_str, new_id);
    if (!inserted1) return EnumStorageError::EnumExists;
    ++next_enum_type_id_;
    etype_to_name_.resize(next_enum_type_id_);
    etype_to_name_[new_pos] = std::move(type_str);

    // insert values for the given type
    auto [it2, inserted2] = enum_values_.try_emplace(new_id, std::move(enum_value_strs));
    if (!inserted2) {
      DMG_ASSERT(false, "logical bug");
      name_to_etype_.erase(it1);
      --next_enum_type_id_;
      return EnumStorageError::EnumExists;
    }

    return {};
  }

  auto to_enum_type(std::string_view type_str) const -> std::optional<EnumTypeId> {
    auto it = name_to_etype_.find(type_str);
    if (it == name_to_etype_.cend()) return std::nullopt;
    return it->second;
  }

  auto to_enum_value(std::string_view type_str, std::string_view value_str) const -> std::optional<EnumValueId> {
    auto e_type = to_enum_type(type_str);
    if (!e_type) return std::nullopt;
    auto it = enum_values_.find(*e_type);
    if (it == enum_values_.cend()) return std::nullopt;
    auto const &[_, details] = *it;

    auto match = [&](auto &val) { return val == value_str; };
    auto match_it = std::ranges::find_if(details, match);
    if (match_it == details.cend()) return std::nullopt;

    auto n = static_cast<uint64_t>(std::distance(details.cbegin(), match_it));
    return EnumValueId{n};
  }

  auto to_enum_value(EnumTypeId e_type, std::string_view value_str) const -> std::optional<EnumValueId> {
    auto it = enum_values_.find(e_type);
    if (it == enum_values_.cend()) return std::nullopt;
    auto const &[_, details] = *it;

    auto match = [&](auto &val) { return val == value_str; };
    auto match_it = std::ranges::find_if(details, match);
    if (match_it == details.cend()) return std::nullopt;

    auto n = static_cast<uint64_t>(std::distance(details.cbegin(), match_it));
    return EnumValueId{n};
  }

  auto to_enum(std::string_view type_str, std::string_view value_str) const -> std::optional<Enum> {
    auto e_type = to_enum_type(type_str);
    if (!e_type) return std::nullopt;
    auto e_value = to_enum_value(*e_type, value_str);
    if (!e_value) return std::nullopt;
    return Enum{*e_type, *e_value};
  }

  auto to_type_string(EnumTypeId id) const -> std::optional<std::string> {
    // TODO: maybe guarentee lifetime (shared_ptr?)
    if (etype_to_name_.size() <= id.value_of()) return std::nullopt;
    return etype_to_name_[id.value_of()];
  }

  auto to_value_string(EnumTypeId e_type, EnumValueId e_value) const -> std::optional<std::string> {
    auto it = enum_values_.find(e_type);
    if (it == enum_values_.cend()) return std::nullopt;
    auto &values = it->second;
    if (values.size() <= e_value.value_of()) return std::nullopt;
    return values[e_value.value_of()];
  }

  auto to_string(Enum val) const -> std::optional<std::string> {
    auto type_str = to_type_string(val.type_id());
    if (!type_str) return std::nullopt;
    auto value_str = to_value_string(val.type_id(), val.value_id());
    if (!value_str) return std::nullopt;

    return std::format("{}::{}", *type_str, *value_str);
  }

 private:
  uint64_t next_enum_type_id_{};
  std::map<std::string, EnumTypeId, std::less<>> name_to_etype_;
  std::vector<std::string> etype_to_name_;

  std::map<EnumTypeId, std::vector<std::string>> enum_values_;
};

}  // namespace memgraph::storage
