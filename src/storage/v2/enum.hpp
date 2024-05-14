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

#include <concepts>
#include <cstdint>

#include "strong_type/strong_type.hpp"

namespace memgraph::storage {

using EnumTypeId = strong::type<uint64_t, struct EnumId_, strong::regular, strong::partially_ordered>;
using EnumValueId = strong::type<uint64_t, struct EnumValueId_, strong::regular, strong::partially_ordered>;

struct Enum {
  Enum() = delete;

  Enum(EnumTypeId type, EnumValueId value) : type_id_{type}, value_id_{value} {}

  //  friend auto operator==(Enum const &, Enum const &) -> bool = default;
  //  friend auto operator<(Enum const &, Enum const &) -> bool = default;
  friend auto operator<=>(Enum const &, Enum const &) -> std::partial_ordering = default;

  EnumTypeId type_id_;
  EnumValueId value_id_;
};

}  // namespace memgraph::storage
