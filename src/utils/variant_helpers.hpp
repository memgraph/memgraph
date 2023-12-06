// Copyright 2023 Memgraph Ltd.
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

#include <functional>
#include <type_traits>

namespace memgraph::utils {
template <class... Ts>
struct Overloaded : Ts... {
  using Ts::operator()...;
};

template <class... Ts>
Overloaded(Ts...) -> Overloaded<Ts...>;

template <typename... Ts>
struct ChainedOverloaded : Ts... {
  template <typename... Us>
  explicit ChainedOverloaded(Us &&...ts) : Ts(std::forward<Us>(ts))... {}

  template <typename... Args>
  auto operator()(Args &&...args) {
    auto conditional_invoke = [&]<typename Base>(Base *self) {
      if constexpr (std::is_invocable_v<Base, Args...>) {
        std::invoke(*self, args...);
      }
    };
    (conditional_invoke(static_cast<Ts *>(this)), ...);
  }
};

template <typename... Ts>
ChainedOverloaded(Ts...) -> ChainedOverloaded<Ts...>;

}  // namespace memgraph::utils
