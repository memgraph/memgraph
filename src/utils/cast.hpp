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

#include <cstdint>
#include <cstring>
#include <type_traits>

namespace memgraph::utils {

template <typename T>
constexpr std::underlying_type_t<T> UnderlyingCast(T e) {
  return static_cast<std::underlying_type_t<T>>(e);
}

/**
 * uint to int conversion in C++ is a bit tricky. Take a look here
 * https://stackoverflow.com/questions/14623266/why-cant-i-reinterpret-cast-uint-to-int
 * for more details.
 *
 * @tparam TDest Returned datatype.
 * @tparam TSrc Input datatype.
 *
 * @return "copy casted" value.
 */
template <typename TDest, typename TSrc>
TDest MemcpyCast(TSrc src) {
  TDest dest;
  static_assert(sizeof(dest) == sizeof(src), "MemcpyCast expects source and destination to be of same size");
  static_assert(std::is_arithmetic_v<TSrc>, "MemcpyCast expects source is an arithmetic type");
  static_assert(std::is_arithmetic_v<TDest>, "MemcypCast expects destination is an arithmetic type");
  std::memcpy(&dest, &src, sizeof(src));
  return dest;
}

}  // namespace memgraph::utils
