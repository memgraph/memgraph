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

#include <concepts>
#include <functional>

namespace memgraph::planner::core {
/// Concept: Symbol must be hashable, trivially copyable, and equality comparable
template <typename T>
concept ENodeSymbol = requires(T a, T b) {
  { std::hash<T>{}(a) } -> std::convertible_to<std::size_t>;
  { a == b } -> std::convertible_to<bool>;
}
&&std::is_trivially_copyable_v<T>;

}  // namespace memgraph::planner::core
