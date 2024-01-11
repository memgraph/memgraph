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

#include <optional>
#include <string>

#include "utils/result.hpp"
#include "utils/string.hpp"

namespace memgraph::utils {
enum class ValidationError : uint8_t { EmptyValue, InvalidValue };

// Returns joined string representations for every enum in the mapping.
std::string GetAllowedEnumValuesString(const auto &mappings) {
  std::vector<std::string> allowed_values;
  allowed_values.reserve(mappings.size());
  std::transform(mappings.begin(), mappings.end(), std::back_inserter(allowed_values),
                 [](const auto &mapping) { return std::string(mapping.first); });
  return memgraph::utils::Join(allowed_values, ", ");
}

// Checks if the string value can be represented as an enum.
// If not, the BasicResult will contain an error.
memgraph::utils::BasicResult<ValidationError> IsValidEnumValueString(const auto &value, const auto &mappings) {
  if (value.empty()) {
    return ValidationError::EmptyValue;
  }

  if (std::find_if(mappings.begin(), mappings.end(), [&](const auto &mapping) { return mapping.first == value; }) ==
      mappings.cend()) {
    return ValidationError::InvalidValue;
  }

  return {};
}
// Tries to convert a string into enum, which would then contain a value if the conversion
// has been successful.
template <typename Enum>
std::optional<Enum> StringToEnum(const auto &value, const auto &mappings) {
  const auto mapping_iter =
      std::find_if(mappings.begin(), mappings.end(), [&](const auto &mapping) { return mapping.first == value; });
  if (mapping_iter == mappings.cend()) {
    return std::nullopt;
  }

  return mapping_iter->second;
}

template <typename T, typename Enum>
requires std::integral<T>
inline T EnumToNum(Enum res) {
  static_assert(std::numeric_limits<T>::max() >= static_cast<size_t>(Enum::N));
  return static_cast<T>(res);
}

template <typename T, typename Enum>
requires std::integral<T>
inline bool NumToEnum(T input, Enum &res) {
  if (input >= EnumToNum<T>(Enum::N)) return false;
  res = static_cast<Enum>(input);
  return true;
}

template <size_t Num, typename T, typename Enum>
requires std::integral<T>
inline T EnumToNum(Enum res) {
  static_assert(std::numeric_limits<T>::max() >= Num);
  return static_cast<T>(res);
}

template <size_t Num, typename T, typename Enum>
requires std::integral<T>
inline bool NumToEnum(T input, Enum &res) {
  if (input >= Num) return false;
  res = static_cast<Enum>(input);
  return true;
}
}  // namespace memgraph::utils
