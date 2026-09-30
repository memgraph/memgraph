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

#include <tuple>
#include <type_traits>

namespace memgraph::utils {
template <typename>
struct function_traits;

template <typename Function>
struct function_traits : function_traits<decltype(&std::remove_reference_t<Function>::operator())> {};

template <typename ReturnType, typename... Args>
struct function_traits<ReturnType (*)(Args...)> {
  template <std::size_t Index>
  using argument = std::remove_cvref_t<std::tuple_element_t<Index, std::tuple<Args...>>>;

  static constexpr std::size_t arity{sizeof...(Args)};
};

}  // namespace memgraph::utils
