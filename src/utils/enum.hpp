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

#include <expected>
#include <optional>
#include <string>

#include "utils/string.hpp"

namespace memgraph::utils {
enum class ValidationError : uint8_t { EmptyValue, InvalidValue };

// Returns joined string representations for every enum in the mapping.
auto GetAllowedEnumValuesString(const auto &mappings) -> std::string {
  std::vector<std::string> allowed_values;
  allowed_values.reserve(mappings.size());
  std::transform(mappings.begin(), mappings.end(), std::back_inserter(allowed_values),
                 [](const auto &mapping) { return std::string(mapping.first); });
  return Join(allowed_values, ", ");
}

// Checks if the string value can be represented as an enum.
// If not, the BasicResult will contain an error.
auto IsValidEnumValueString(const auto &value, const auto &mappings) -> std::expected<void, ValidationError> {
  if (value.empty()) {
    return std::unexpected{ValidationError::EmptyValue};
  }

  if (std::find_if(mappings.begin(), mappings.end(), [&](const auto &mapping) { return mapping.first == value; }) ==
      mappings.cend()) {
    return std::unexpected{ValidationError::InvalidValue};
  }

  return {};
}
// Tries to convert a string into enum, which would then contain a value if the conversion
// has been successful.
template <typename Enum>
auto StringToEnum(const auto &value, const auto &mappings) -> std::optional<Enum> {
  const auto mapping_iter =
      std::find_if(mappings.begin(), mappings.end(), [&](const auto &mapping) { return mapping.first == value; });
  if (mapping_iter == mappings.cend()) {
    return std::nullopt;
  }

  return mapping_iter->second;
}

// Tries to convert a enum into string, which would then contain a value if the conversion
// has been successful.
template <typename Enum>
requires std::is_enum_v<Enum>
auto EnumToString(const auto &value, const auto &mappings) -> std::optional<std::string_view> {
  const auto mapping_iter =
      std::find_if(mappings.begin(), mappings.end(), [&](const auto &mapping) { return mapping.second == value; });
  if (mapping_iter == mappings.cend()) [[unlikely]] {
    return std::nullopt;
  }
  return mapping_iter->first;
}

template <typename Enum>
requires std::is_enum_v<Enum> && requires { Enum::N; }
bool NumToEnum(std::underlying_type_t<Enum> input, Enum &res) {
  if (input >= std::to_underlying(Enum::N)) return false;
  res = static_cast<Enum>(input);
  return true;
}
}  // namespace memgraph::utils
