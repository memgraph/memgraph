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

#include "communication/bolt/v1/mg_types.hpp"
namespace memgraph::communication::bolt {

auto BoltMapToMgTypeInfo(map_t const &value) -> std::optional<mg_type_info> {
  auto type_selector = value.find(kMgTypeType);
  if (type_selector == value.cend()) return std::nullopt;
  auto value_selector = value.find(kMgTypeValue);
  if (value_selector == value.cend()) return std::nullopt;
  if (!type_selector->second.IsString()) return std::nullopt;
  auto mg_type = std::string_view{type_selector->second.ValueString()};
  auto const &value_val = value_selector->second;
  if (!value_val.IsString()) return std::nullopt;
  auto mg_value = std::string_view{value_val.ValueString()};
  if (mg_type == kMgTypeEnum) {
    return mg_type_info{MgType::Enum, kMgTypeEnum, mg_value};
  }
  return std::nullopt;
}

}  // namespace memgraph::communication::bolt
