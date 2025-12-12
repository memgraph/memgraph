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

#pragma once

#include "absl/container/flat_hash_map.h"
#include "storage/v2/enum.hpp"
#include "utils/result.hpp"

namespace r = std::ranges;
namespace rv = r::views;

namespace memgraph::storage {

enum struct EnumStorageError : uint8_t { EnumExists, InvalidValue, UnknownEnumType, UnknownEnumValue, ParseError };

struct EnumStore {
  auto RegisterEnum(std::string_view type_str, std::span<std::string const> enum_value_strs) {
    return RegisterEnum(std::string{type_str}, std::vector(enum_value_strs.begin(), enum_value_strs.end()));
  }

  auto RegisterEnum(std::string type_str, std::vector<std::string> enum_value_strs)
      -> memgraph::utils::BasicResult<EnumStorageError, EnumTypeId> {
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
        return EnumStorageError::InvalidValue;
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

  auto UpdateValue(std::string_view e_type, std::string_view old_value, std::string_view new_value)
      -> memgraph::utils::BasicResult<EnumStorageError, Enum> {
    auto etype = ToEnumType(e_type);
    if (etype.HasError()) return etype.GetError();
    return UpdateValue(*etype, old_value, new_value);
  }

  auto UpdateValue(EnumTypeId e_type, std::string_view old_value, std::string_view new_value)
      -> memgraph::utils::BasicResult<EnumStorageError, Enum> {
    auto e_value = ToEnumValue(e_type, old_value);
    if (e_value.HasError()) return e_value.GetError();

    if (old_value == new_value) return EnumStorageError::InvalidValue;

    auto &lookup = evalue_lookups_[e_type.value_of()];
    auto &strings = evalue_strs_[e_type.value_of()];

    if (lookup.contains(new_value)) return EnumStorageError::InvalidValue;

    lookup.try_emplace(new_value, *e_value);
    strings[e_value->value_of()] = new_value;
    lookup.erase(old_value);
    MG_ASSERT(strings.size() == lookup.size());
    return Enum{e_type, *e_value};
  }

  auto AddValue(std::string_view e_type, std::string_view new_value)
      -> memgraph::utils::BasicResult<EnumStorageError, Enum> {
    auto etype = ToEnumType(e_type);
    if (etype.HasError()) return etype.GetError();
    return AddValue(*etype, new_value);
  }

  auto AddValue(EnumTypeId e_type, std::string_view new_value) -> memgraph::utils::BasicResult<EnumStorageError, Enum> {
    if (evalue_lookups_.size() <= e_type.value_of()) return EnumStorageError::UnknownEnumType;

    auto &lookup = evalue_lookups_[e_type.value_of()];
    auto &strings = evalue_strs_[e_type.value_of()];

    if (lookup.contains(new_value)) return EnumStorageError::InvalidValue;

    auto e_value = EnumValueId{strings.size()};
    strings.emplace_back(new_value);
    try {
      lookup.try_emplace(new_value, e_value);
    } catch (...) {
      strings.pop_back();
      throw;
    }
    MG_ASSERT(strings.size() == lookup.size());
    return Enum{e_type, e_value};
  }

  // used by recovery in the event of failure
  void clear() {
    etype_strs_.clear();
    evalue_strs_.clear();
    etype_lookup_.clear();
    evalue_lookups_.clear();
  }

  auto ToEnumType(std::string_view type_str) const -> memgraph::utils::BasicResult<EnumStorageError, EnumTypeId> {
    auto it = etype_lookup_.find(type_str);
    if (it == etype_lookup_.cend()) return EnumStorageError::UnknownEnumType;
    return it->second;
  }

  auto ToEnumValue(std::string_view type_str, std::string_view value_str) const
      -> memgraph::utils::BasicResult<EnumStorageError, EnumValueId> {
    auto e_type = ToEnumType(type_str);
    if (e_type.HasError()) return e_type.GetError();
    return ToEnumValue(*e_type, value_str);
  }

  auto ToEnumValue(EnumTypeId e_type, std::string_view value_str) const
      -> memgraph::utils::BasicResult<EnumStorageError, EnumValueId> {
    if (evalue_lookups_.size() <= e_type.value_of()) return EnumStorageError::UnknownEnumType;
    auto const &evalue_lookup = evalue_lookups_[e_type.value_of()];
    auto it = evalue_lookup.find(value_str);
    if (it == evalue_lookup.cend()) return EnumStorageError::UnknownEnumValue;
    return it->second;
  }

  auto ToEnum(std::string_view enum_str) const -> memgraph::utils::BasicResult<EnumStorageError, Enum> {
    auto pos = enum_str.find("::");
    if (pos == std::string_view::npos) return EnumStorageError::ParseError;
    auto etype = enum_str.substr(0, pos);
    auto evalue = enum_str.substr(pos + 2);
    return ToEnum(etype, evalue);
  }

  auto ToEnum(std::string_view type_str, std::string_view value_str) const
      -> memgraph::utils::BasicResult<EnumStorageError, Enum> {
    auto e_type = ToEnumType(type_str);
    if (e_type.HasError()) return e_type.GetError();
    auto e_value = ToEnumValue(*e_type, value_str);
    if (e_value.HasError()) return e_value.GetError();
    return Enum{*e_type, *e_value};
  }

  auto ToTypeString(EnumTypeId id) const -> memgraph::utils::BasicResult<EnumStorageError, std::string> {
    if (etype_strs_.size() <= id.value_of()) return EnumStorageError::UnknownEnumType;
    return etype_strs_[id.value_of()];
  }

  auto ToValuesStrings(EnumTypeId e_type) const -> std::vector<std::string> const * {
    if (evalue_strs_.size() <= e_type.value_of()) return nullptr;
    return std::addressof(evalue_strs_[e_type.value_of()]);
  }

  auto ToValueString(EnumTypeId e_type, EnumValueId e_value) const
      -> memgraph::utils::BasicResult<EnumStorageError, std::string> {
    if (evalue_strs_.size() <= e_type.value_of()) return EnumStorageError::UnknownEnumType;
    auto const &values = evalue_strs_[e_type.value_of()];
    if (values.size() <= e_value.value_of()) return EnumStorageError::UnknownEnumValue;
    return values[e_value.value_of()];
  }

  auto ToString(Enum val) const -> memgraph::utils::BasicResult<EnumStorageError, std::string> {
    auto type_str = ToTypeString(val.type_id());
    if (type_str.HasError()) return type_str;
    auto value_str = ToValueString(val.type_id(), val.value_id());
    if (value_str.HasError()) return value_str;

    return std::format("{}::{}", *type_str, *value_str);
  }

  auto AllRegistered() const { return rv::zip(etype_strs_, evalue_strs_); }

 private:
  std::vector<std::string> etype_strs_;
  std::vector<std::vector<std::string>> evalue_strs_;

  absl::flat_hash_map<std::string, EnumTypeId> etype_lookup_;
  std::vector<absl::flat_hash_map<std::string, EnumValueId>> evalue_lookups_;
};

}  // namespace memgraph::storage
