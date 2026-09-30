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

namespace memgraph::utils {

template <typename T>
struct tag_type {
  using type = T;
};

template <auto V>
struct tag_value {
  static constexpr auto value = V;
};

template <typename T>
auto tag_t = tag_type<T>{};

template <auto V>
auto tag_v = tag_value<V>{};

}  // namespace memgraph::utils
