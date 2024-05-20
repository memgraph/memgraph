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

#include "absl/container/flat_hash_map.h"
#include "range/v3/all.hpp"
#include "strong_type/strong_type.hpp"

#include <algorithm>
#include <cstdint>
#include <map>
#include <ranges>
#include <string>

namespace memgraph::storage {

enum struct EnumStorageError : uint8_t { EnumExists, InvalidValues };

struct EnumStore {
  auto register_enum(std::string type_str, std::vector<std::string> enum_value_strs)
      -> memgraph::utils::BasicResult<EnumStorageError, EnumTypeId> {
    namespace rv = ranges::views;

    auto new_pos = etype_strs_.size();
    auto new_id = EnumTypeId{new_pos};

    auto [it_type, inserted] = etype_lookup_.try_emplace(type_str, new_id);
    if (!inserted) [[unlikely]] {
      return EnumStorageError::EnumExists;
    }

    auto values_lookup = decltype(evalue_lookups_)::value_type{};
    for (auto const &[pos, value] : rv::enumerate(enum_value_strs)) {
      auto [it_values, inserted] = values_lookup.try_emplace(value, EnumValueId{pos});
      if (!inserted) [[unlikely]] {
        etype_lookup_.erase(it_type);
        return EnumStorageError::InvalidValues;
      }
    }
    try {
      evalue_lookups_.emplace_back(std::move(values_lookup));
      etype_strs_.emplace_back(std::move(type_str));
      evalue_strs_.emplace_back(std::move(enum_value_strs));
    } catch (...) {
      etype_lookup_.erase(it_type);
      if (etype_lookup_.size() < evalue_lookups_.size()) evalue_lookups_.pop_back();
      if (etype_lookup_.size() < etype_strs_.size()) etype_strs_.pop_back();
      throw;
    }

    DMG_ASSERT(etype_strs_.size() == evalue_strs_.size());
    DMG_ASSERT(etype_strs_.size() == etype_lookup_.size());
    DMG_ASSERT(etype_strs_.size() == evalue_lookups_.size());

    return {new_id};
  }

  // used by recovery in the event of failure
  void clear() {
    etype_strs_.clear();
    evalue_strs_.clear();
    etype_lookup_.clear();
    evalue_lookups_.clear();
  }

  auto to_enum_type(std::string_view type_str) const -> std::optional<EnumTypeId> {
    auto it = etype_lookup_.find(type_str);
    if (it == etype_lookup_.cend()) return std::nullopt;
    return it->second;
  }

  auto to_enum_value(std::string_view type_str, std::string_view value_str) const -> std::optional<EnumValueId> {
    auto e_type = to_enum_type(type_str);
    if (!e_type) return std::nullopt;
    return to_enum_value(*e_type, value_str);
  }

  auto to_enum_value(EnumTypeId e_type, std::string_view value_str) const -> std::optional<EnumValueId> {
    if (evalue_lookups_.size() <= e_type.value_of()) return std::nullopt;
    auto const &evalue_lookup = evalue_lookups_[e_type.value_of()];
    auto it = evalue_lookup.find(value_str);
    if (it == evalue_lookup.cend()) return std::nullopt;
    return it->second;
  }

  auto to_enum(std::string_view type_str, std::string_view value_str) const -> std::optional<Enum> {
    auto e_type = to_enum_type(type_str);
    if (!e_type) return std::nullopt;
    auto e_value = to_enum_value(*e_type, value_str);
    if (!e_value) return std::nullopt;
    return Enum{*e_type, *e_value};
  }

  auto to_type_string(EnumTypeId id) const -> std::optional<std::string> {
    if (etype_strs_.size() <= id.value_of()) return std::nullopt;
    return etype_strs_[id.value_of()];
  }

  auto to_values_strings(EnumTypeId e_type) const -> std::vector<std::string> const * {
    if (evalue_strs_.size() <= e_type.value_of()) return nullptr;
    return std::addressof(evalue_strs_[e_type.value_of()]);
  }

  auto to_value_string(EnumTypeId e_type, EnumValueId e_value) const -> std::optional<std::string> {
    if (evalue_strs_.size() <= e_type.value_of()) return std::nullopt;
    auto const &values = evalue_strs_[e_type.value_of()];
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

  auto all_registered() const { return ranges::views::zip(etype_strs_, evalue_strs_); }

 private:
  std::vector<std::string> etype_strs_;
  std::vector<std::vector<std::string>> evalue_strs_;

  absl::flat_hash_map<std::string, EnumTypeId> etype_lookup_;
  std::vector<absl::flat_hash_map<std::string, EnumValueId>> evalue_lookups_;
};

}  // namespace memgraph::storage
