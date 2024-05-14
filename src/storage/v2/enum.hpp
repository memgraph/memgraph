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

  friend auto as_tuple(const Enum &obj) { return std::tie(obj.type_id_, obj.value_id_); }

 private:
  EnumTypeId type_id_;
  EnumValueId value_id_;
};

template <std::size_t N>
decltype(auto) get(const Enum &obj) {
  return std::get<N>(as_tuple(obj));
}

}  // namespace memgraph::storage

namespace std {
template <>
struct tuple_size<memgraph::storage::Enum> : std::integral_constant<std::size_t, 2> {};

template <size_t N>
struct tuple_element<N, memgraph::storage::Enum> {
  using type = std::tuple_element_t<N, decltype(as_tuple(std::declval<memgraph::storage::Enum>()))>;
};
}  // namespace std
