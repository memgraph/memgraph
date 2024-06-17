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

#include <string_view>
#include "communication/bolt/v1/value.hpp"

namespace memgraph::communication::bolt {

enum class MgType : uint8_t {
  Enum,
};

struct mg_type_info {
  MgType type;
  std::string_view type_str;
  std::string_view value_str;
};

constexpr std::string_view kMgTypeEnum = "mg_enum";
constexpr std::string_view kMgTypeType = "__type";
constexpr std::string_view kMgTypeValue = "__value";

auto BoltMapToMgTypeInfo(map_t const &value) -> std::optional<mg_type_info>;
}  // namespace memgraph::communication::bolt
